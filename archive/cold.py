"""Cold storage: per-batch ZIP packages and the background cooling worker.

A cooling job moves a *frozen* list of registered originals into a single
ZIP package that supports random per-member reads. The state machine is

    queued -> compressing -> verifying -> switching -> cleaning
           \-> failed (retried automatically, hot originals always preserved)

Safety rules implemented here:

* the member list (manifest) is frozen in the catalog when the job is first
  accepted; later uploads never alter it and are never deleted by the job;
* packages are built as unique ``.tmp`` files and atomically renamed, so a
  half-written package can never be opened or served;
* every member is re-hashed out of the finished ZIP (receipts and bytes are
  re-checked against the catalog before switching the retrieval location);
* hot originals are deleted only after the package is persisted AND verified
  AND the catalog flip to ``storage='cold'`` has committed, and only when the
  bytes on disk still match the frozen digest;
* a crash at any point is resumable: before the flip the hot originals are
  authoritative and the package is simply rebuilt, afterwards the verified
  package is authoritative and cleanup continues.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import zipfile
from pathlib import Path

CHUNK = 64 * 1024


def sha256_bytes_iter(chunks) -> tuple[str, int]:
    hasher = hashlib.sha256()
    total = 0
    for chunk in chunks:
        hasher.update(chunk)
        total += len(chunk)
    return hasher.hexdigest(), total


def sha256_file(path: Path) -> tuple[str, int]:
    def _iter():
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(CHUNK)
                if not chunk:
                    break
                yield chunk
    return sha256_bytes_iter(_iter())


class ColdPackageError(RuntimeError):
    """A cold package is missing or damaged."""


class ColdJobError(RuntimeError):
    """A cooling job failed; it has already been recorded in the catalog."""


class ColdStorage:
    """Filesystem layout for cold ZIP packages (one ZIP per batch)."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def package_path(self, batch_id: str) -> Path:
        return self.root / f"{batch_id}.zip"

    def _tmp_path(self, batch_id: str) -> Path:
        return self.root / f".{batch_id}.{os.getpid()}.{threading.get_ident()}.tmp"

    def cleanup_stale_tmp(self) -> list[str]:
        removed = []
        for path in self.root.glob(".*.tmp"):
            try:
                path.unlink()
                removed.append(path.name)
            except OSError:
                pass
        return removed

    def build_package(self, batch_id: str, manifest: list[dict],
                      resolve_bytes, progress_cb=None) -> Path:
        """Compress the frozen originals into a new temporary package.

        ``resolve_bytes(batch_id, entry)`` returns the exact original bytes
        for a manifest member, reading them from the hot original or, for a
        re-cooling whose hot copy was already removed, from the previously
        persisted package. Returns the temporary path; the caller verifies
        and atomically publishes it. Nothing here touches hot originals or
        the catalog.
        """
        tmp = self._tmp_path(batch_id)
        try:
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED,
                                 compresslevel=6) as zf:
                for idx, entry in enumerate(manifest):
                    content = resolve_bytes(batch_id, entry)
                    # arcname is the bare validated filename: no paths,
                    # no traversal, one member per frozen file.
                    zf.writestr(entry["filename"], content)
                    if progress_cb is not None:
                        progress_cb(idx + 1)
            return tmp
        except BaseException:
            # A failed build must never leave a half package behind.
            try:
                tmp.unlink()
            except OSError:
                pass
            raise

    def publish(self, tmp: Path, final: Path) -> None:
        """Atomic package publication (same directory -> same filesystem)."""
        os.replace(tmp, final)

    def package_exists(self, batch_id: str) -> bool:
        return self.package_path(batch_id).is_file()

    def verify_package(self, batch_id: str,
                       manifest: list[dict]) -> tuple[bool, list[str], list[str]]:
        """Re-hash every member out of the persisted ZIP.

        Returns ``(ok, errors, extra_members)`` where ``extra_members`` are
        ZIP members that are not part of the frozen manifest (an anomaly even
        when every manifest member itself is fine).
        """
        path = self.package_path(batch_id)
        errors: list[str] = []
        if not path.is_file():
            return False, [f"cold package missing: {path.name}"], []
        try:
            with zipfile.ZipFile(path) as zf:
                bad = zf.testzip()
                if bad is not None:
                    errors.append(f"zip CRC/header failure on member: {bad}")
                names = set(zf.namelist())
                expected_names = {e["filename"] for e in manifest}
                extra = sorted(names - expected_names)
                for entry in manifest:
                    name = entry["filename"]
                    if name not in names:
                        errors.append(f"member missing from package: {name}")
                        continue
                    try:
                        digest, length = sha256_bytes_iter(
                            self._iter_member(zf, name))
                    except (zipfile.BadZipFile, OSError) as exc:
                        errors.append(f"member unreadable {name}: {exc}")
                        continue
                    if length != entry["length"] or digest != entry["sha256"]:
                        errors.append(
                            f"member mismatch {name}: "
                            f"got len={length} sha={digest}, "
                            f"expected len={entry['length']} "
                            f"sha={entry['sha256']}")
        except zipfile.BadZipFile as exc:
            return False, [f"cold package is not a valid zip: {exc}"], []
        return not errors, errors, extra

    @staticmethod
    def _iter_member(zf: zipfile.ZipFile, name: str):
        with zf.open(name) as member:
            while True:
                chunk = member.read(CHUNK)
                if not chunk:
                    break
                yield chunk

    def read_member(self, batch_id: str, filename: str) -> bytes:
        """Read one member out of the package (single-item random read)."""
        path = self.package_path(batch_id)
        if not path.is_file():
            raise ColdPackageError(f"cold package missing: {path.name}")
        try:
            with zipfile.ZipFile(path) as zf:
                names = set(zf.namelist())
                if filename not in names:
                    raise ColdPackageError(
                        f"member {filename} absent from package {path.name}")
                return zf.read(filename)  # CRC checked by zipfile on read
        except zipfile.BadZipFile as exc:
            raise ColdPackageError(f"cold package damaged: {exc}") from exc

    def has_member(self, batch_id: str, filename: str) -> bool:
        path = self.package_path(batch_id)
        if not path.is_file():
            return False
        try:
            with zipfile.ZipFile(path) as zf:
                return filename in zf.namelist()
        except zipfile.BadZipFile:
            return False

    def list_members(self, batch_id: str) -> list[str] | None:
        """Member names, or ``None`` when the package is missing/broken."""
        path = self.package_path(batch_id)
        if not path.is_file():
            return None
        try:
            with zipfile.ZipFile(path) as zf:
                if zf.testzip() is not None:
                    return None
                return sorted(zf.namelist())
        except zipfile.BadZipFile:
            return None

    def remove_package(self, batch_id: str) -> None:
        """Only safe before the catalog flip: hot files remain authoritative."""
        path = self.package_path(batch_id)
        try:
            path.unlink()
        except FileNotFoundError:
            pass


class ColdWorker:
    """Background daemon that drives cooling jobs without blocking the API.

    The worker only ever holds the per-batch lock (never a global one), so
    slow compression or deletion of one batch cannot stall uploads, downloads
    or status queries of any batch.
    """

    def __init__(self, ctx):
        self._ctx = ctx
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="cold-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def kick(self) -> None:
        """Wake the worker immediately after a new job was enqueued."""
        self._wake.set()

    def _loop(self) -> None:
        ctx = self._ctx
        # Stale .tmp files from a crash mid-build are never published.
        ctx.cold.cleanup_stale_tmp()
        while not self._stop.is_set():
            self._wake.wait(timeout=1.0)
            self._wake.clear()
            try:
                jobs = ctx.db.list_unfinished_cold_jobs()
            except Exception as exc:  # pragma: no cover - keep the thread alive
                print(f"[cold-worker] cannot list jobs: {exc}", flush=True)
                continue
            for job in jobs:
                if self._stop.is_set():
                    break
                try:
                    self._run_job(job)
                except ColdJobError as exc:
                    # Already persisted as 'failed' by _fail(); the worker
                    # keeps going with the next batch.
                    print(f"[cold-worker] {job['batch_id']} failed: {exc}",
                          flush=True)
                except Exception as exc:  # safety net
                    try:
                        ctx.db.update_cold_job(
                            job["batch_id"], state="failed",
                            error=f"unexpected worker error: {exc}")
                    except Exception:  # pragma: no cover
                        pass
                    print(f"[cold-worker] loop error: {exc}", flush=True)

    # ------------------------------------------------------------------ #
    def _run_job(self, job) -> None:
        ctx = self._ctx
        batch_id = job["batch_id"]
        manifest = ctx.load_manifest(job)
        with ctx.lock(batch_id):
            current = ctx.db.get_cold_job(batch_id)
            if current is None or current["state"] == "completed":
                return
            state = current["state"]
            if state in ("queued", "failed", "compressing", "verifying"):
                # Build/verify from the frozen hot originals and publish the
                # package, then fall through to the switch+cleanup stage.
                self._stage_build_and_verify(batch_id, manifest)
            self._stage_switch_and_clean(batch_id, manifest)

    def _maybe_crash(self, point: str) -> None:
        marker = self._ctx.crash_marker
        if marker is not None and marker.exists():
            # The marker is intentionally NOT consumed here so the self test
            # can decide which crash point to stop at (it removes the marker
            # itself when it wants the next attempt to run through).
            print(f"[fault-injection] cold worker exiting at {point}",
                  flush=True)
            os._exit(1)

    def _sleep(self) -> None:
        if self._ctx.step_sleep:
            time.sleep(self._ctx.step_sleep)

    def _stage_build_and_verify(self, batch_id: str,
                                manifest: list[dict]) -> None:
        """queued/failed -> compressing -> verifying -> switching.

        Hot originals are the preferred source in this stage; members whose
        hot copy was already removed by an earlier, completed cooling run are
        recovered from the previously persisted package. Any existing package
        is rebuilt from scratch, so a crashed partial attempt can never be
        mistaken for a finished one.
        """
        ctx = self._ctx
        # 1. preconditions: every frozen member is a registered, witnessed
        #    file whose exact frozen bytes are still reachable.
        for entry in manifest:
            row = ctx.db.get_file(batch_id, entry["filename"])
            if row is None or row["status"] != "registered":
                raise self._fail(batch_id,
                    f"manifest member not registered: {entry['filename']}")
            row_data = dict(row)
            if row_data.get("receipt"):
                row_data["receipt"] = json.loads(row_data["receipt"])
            ok, err = ctx.verify_receipt(row_data)
            if not ok:
                raise self._fail(batch_id,
                    f"receipt check failed for {entry['filename']}: {err}")
            hot = ctx.hot_path(batch_id, entry["filename"])
            if hot.is_file():
                digest, length = sha256_file(hot)
                if digest != entry["sha256"] or length != entry["length"]:
                    raise self._fail(batch_id,
                        f"hot original changed since freeze: "
                        f"{entry['filename']}")
            elif not ctx.cold.has_member(batch_id, entry["filename"]):
                raise self._fail(batch_id,
                    f"original unreachable in neither hot nor cold storage: "
                    f"{entry['filename']}")

        def _resolve(bid: str, entry: dict) -> bytes:
            """Return the frozen member's exact bytes, hash-verified."""
            hot = ctx.hot_path(bid, entry["filename"])
            if hot.is_file():
                content = hot.read_bytes()
            else:
                try:
                    content = ctx.cold.read_member(bid, entry["filename"])
                except ColdPackageError as exc:
                    raise self._fail(
                        bid, f"cannot recover member {entry['filename']} "
                             f"from existing package: {exc}") from exc
            digest = hashlib.sha256(content).hexdigest()
            if digest != entry["sha256"] or len(content) != entry["length"]:
                raise self._fail(
                    bid, f"resolved bytes do not match frozen manifest: "
                         f"{entry['filename']}")
            return content

        ctx.db.update_cold_job(batch_id, state="compressing",
                               processed=0, clear_error=True)
        # Build the replacement as a unique .tmp first; publication below
        # atomically overwrites any package from an earlier attempt. The old
        # package must stay in place until then because re-cooling may need it
        # as the byte source for members whose hot copy was already removed.
        tmp = ctx.cold.build_package(
            batch_id, manifest, _resolve,
            progress_cb=lambda n: ctx.db.update_cold_job(
                batch_id, processed=n))
        self._sleep()
        self._maybe_crash("after_build_before_publish")
        final = ctx.cold.package_path(batch_id)
        ctx.cold.publish(tmp, final)

        ctx.db.update_cold_job(batch_id, state="verifying")
        ok, errors, extra = ctx.cold.verify_package(batch_id, manifest)
        if extra:
            errors.append("undeclared members in package: "
                          + ", ".join(extra))
        if not ok:
            # Keep the bad package on disk for forensics; the independent
            # verification page will flag it as damaged. Hot originals are
            # never deleted past this point, so the frozen bytes survive.
            raise self._fail(batch_id,
                "cold package verification failed: " + "; ".join(errors))
        self._maybe_crash("after_verify_before_switch")

    def _stage_switch_and_clean(self, batch_id: str,
                                manifest: list[dict]) -> None:
        """switching -> cleaning -> completed.

        The package is re-verified before any hot byte is deleted, the
        retrieval location flips atomically in the catalog, and hot files are
        removed one by one only while their bytes still match the freeze.
        """
        ctx = self._ctx
        ok, errors, extra = ctx.cold.verify_package(batch_id, manifest)
        if extra:
            errors.append("undeclared members in package: "
                          + ", ".join(extra))
        if not ok:
            raise self._fail(batch_id,
                "cold package not trustworthy, hot files preserved: "
                + "; ".join(errors))

        job = ctx.db.get_cold_job(batch_id)
        if job["state"] != "cleaning":
            # Atomic catalog switch: readers never see a half flip.
            ctx.db.mark_storage_cold(
                batch_id, [e["filename"] for e in manifest])
            ctx.db.update_cold_job(batch_id, state="cleaning", processed=0)
        removed = 0
        for entry in manifest:
            hot = ctx.hot_path(batch_id, entry["filename"])
            if hot.is_file():
                self._maybe_crash("before_hot_delete")
                digest, _length = sha256_file(hot)
                if digest != entry["sha256"]:
                    # Never delete bytes we cannot account for: a later upload
                    # or tampering must be preserved for investigation.
                    raise self._fail(batch_id,
                        f"hot file diverged from frozen manifest, kept: "
                        f"{entry['filename']}")
                hot.unlink()
            removed += 1
            ctx.db.update_cold_job(batch_id, processed=removed)
            self._sleep()

        # Remove the now-empty hot batch directory (ignore non-empty: later
        # uploads may legitimately still live there).
        hot_dir = ctx.hot_dir(batch_id)
        try:
            hot_dir.rmdir()
        except OSError:
            pass
        ctx.db.update_cold_job(batch_id, state="completed",
                               processed=len(manifest), clear_error=True)

    def _fail(self, batch_id: str, error: str) -> "ColdJobError":
        """Persist the failure and raise for the worker loop."""
        self._ctx.db.update_cold_job(batch_id, state="failed", error=error)
        return ColdJobError(error)
