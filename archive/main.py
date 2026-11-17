"""Research data archiving web service.

The archiver uploads originals to an independent witness, verifies every
Ed25519 receipt with a fixed public key, and only reports success when the
local original, the catalog entry and the verified receipt are all present.

Long-term retention batches can be *cooled*: the registered file list is
frozen, the hot originals are compressed into a per-batch ZIP that supports
single-member reads, and hot bytes are deleted only after the package is
persisted, independently re-verified and the catalog has atomically switched
its retrieval location to cold. All cooling work runs in a background worker
and never blocks uploads, downloads or status queries.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import gc
import threading
import uuid
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates

from archive.cold import ColdPackageError, ColdStorage, ColdWorker
from archive.db import COLD_ACTIVE_STATES, Database
from archive.witness_client import (
    WitnessClient,
    WitnessConflict,
    WitnessUnreachable,
)
from common.receipt import load_public_key, verify_envelope

MAX_UPLOAD_BYTES = int(os.getenv("ARCHIVE_MAX_UPLOAD_BYTES", 2 * 1024 * 1024))
DB_PATH = os.getenv("ARCHIVE_DB", "/data/archive.db")
STORAGE_DIR = Path(os.getenv("ARCHIVE_STORAGE", "/data/originals"))
# Default cold storage to a sibling of the catalog DB so it is writable in
# every deployment layout (compose pins all three under the /data volume).
COLD_DIR = Path(os.getenv("ARCHIVE_COLD_DIR",
                          str(Path(DB_PATH).parent / "cold")))
PUBKEY_PATH = os.getenv("ARCHIVE_PUBLIC_KEY",
                        "/keys/witness_ed25519.pub.pem")
WITNESS_URL = os.getenv("WITNESS_URL", "http://witness:8001")
CRASH_MARKER = Path(os.getenv("ARCHIVE_CRASH_MARKER",
                              "/data/crash_after_witness.flag"))
# Default to a sibling of the catalog DB so the marker is writable wherever
# the persistent volume is mounted (compose overrides this to /data anyway).
COLD_CRASH_MARKER = Path(os.getenv(
    "ARCHIVE_COLD_CRASH_MARKER",
    str(Path(DB_PATH).parent / "crash_during_cooling.flag")))
# Test/knob: artificial per-step delay so the self test can prove that slow
# compression and deletion never block same-batch API traffic.
COLD_STEP_SLEEP = float(os.getenv("ARCHIVE_COLD_STEP_SLEEP", "0"))

KEY_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")

db: Database
witness: WitnessClient
cold: ColdStorage
worker: ColdWorker
public_key = None
templates = Jinja2Templates(directory=os.path.join(
    os.path.dirname(__file__), "templates"))

# One lock per batch: concurrent retries of the same batch are serialized so
# the same original can never be archived twice, and cooling shares the lock.
_batch_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def batch_lock(batch_id: str) -> threading.Lock:
    with _locks_guard:
        lock = _batch_locks.get(batch_id)
        if lock is None:
            lock = threading.Lock()
            _batch_locks[batch_id] = lock
        return lock


def validate_keys(batch_id: str, filename: str) -> None:
    if not KEY_RE.fullmatch(batch_id):
        raise HTTPException(400, "invalid batch_id")
    if not KEY_RE.fullmatch(filename) or filename in (".", ".."):
        raise HTTPException(400, "invalid filename")


def storage_path(batch_id: str, filename: str) -> Path:
    return STORAGE_DIR / batch_id / filename


def sha256_file(path: Path) -> tuple[str, int]:
    hasher = hashlib.sha256()
    length = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(64 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
            length += len(chunk)
    return hasher.hexdigest(), length


def sha256_file_handle(fh) -> tuple[str, int]:
    """Hash an open file and rewind it so it can be streamed afterwards."""
    fh.seek(0)
    hasher = hashlib.sha256()
    length = 0
    while True:
        chunk = fh.read(64 * 1024)
        if not chunk:
            break
        hasher.update(chunk)
        length += len(chunk)
    fh.seek(0)
    return hasher.hexdigest(), length


def row_to_dict(row) -> dict:
    data = dict(row)
    if data.get("receipt"):
        data["receipt"] = json.loads(data["receipt"])
    return data


def verify_receipt(file_row: dict) -> tuple[bool, str | None]:
    if not file_row.get("receipt"):
        return False, "no receipt in catalog"
    expected = {
        "batch_id": file_row["batch_id"],
        "filename": file_row["filename"],
        "length": file_row["length"],
        "sha256": file_row["sha256"],
    }
    return verify_envelope(public_key, file_row["receipt"], expected)


def original_available(batch_id: str, row: dict) -> bool:
    """Whether the bytes registered in the catalog are currently readable."""
    if row["storage"] == "cold":
        return cold.has_member(batch_id, row["filename"])
    return storage_path(batch_id, row["filename"]).is_file()


def batch_status(batch_id: str) -> dict:
    rows = [row_to_dict(r) for r in db.list_batch(batch_id)]
    registered = [r for r in rows if r["status"] == "registered"]
    ok = bool(rows) and len(registered) == len(rows)
    # Success requires local originals (hot or cold) AND catalog AND verified
    # receipts.
    complete = False
    if ok:
        complete = all(
            original_available(batch_id, r) and verify_receipt(r)
            for r in registered
        )
    job_row = db.get_cold_job(batch_id)
    cold_job = job_to_dict(job_row) if job_row is not None else None
    return {
        "batch_id": batch_id,
        "status": "complete" if complete else (
            "pending" if any(r["status"] == "pending" for r in rows)
            else "incomplete"),
        "file_count": len(rows),
        "registered_count": len(registered),
        "cold_count": sum(1 for r in rows if r["storage"] == "cold"),
        "cold_job": cold_job,
        "files": rows,
    }


def job_to_dict(row) -> dict:
    data = dict(row)
    data["manifest"] = json.loads(data["manifest"])
    return data


class ColdContext:
    """Everything the background cooling worker needs, injected explicitly."""

    def __init__(self):
        self.db = db
        self.cold = cold
        self.crash_marker = COLD_CRASH_MARKER
        self.step_sleep = COLD_STEP_SLEEP

    def lock(self, batch_id: str) -> threading.Lock:
        return batch_lock(batch_id)

    def hot_path(self, batch_id: str, filename: str) -> Path:
        return storage_path(batch_id, filename)

    def hot_dir(self, batch_id: str) -> Path:
        return STORAGE_DIR / batch_id

    def load_manifest(self, job) -> list[dict]:
        return json.loads(job["manifest"])

    def verify_receipt(self, row: dict) -> tuple[bool, str | None]:
        return verify_receipt(row)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, witness, cold, worker, public_key
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    COLD_DIR.mkdir(parents=True, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    db = Database(DB_PATH)
    witness = WitnessClient(WITNESS_URL)
    cold = ColdStorage(COLD_DIR)
    public_key = load_public_key(PUBKEY_PATH)
    # Grab the running uvicorn server for the test-only shutdown endpoint.
    import uvicorn.server
    for obj in gc.get_objects():
        if isinstance(obj, uvicorn.server.Server):
            app.state.server = obj
            break
    # Background cooling: resumes any job left unfinished by a restart.
    worker = ColdWorker(ColdContext())
    worker.start()
    # After a restart, find and continue every batch left pending instead of
    # re-submitting anything blindly. Also recovers originals written before a
    # crash that happened ahead of local catalog registration.
    try:
        reconcile_pending()
    except Exception as exc:  # pragma: no cover - startup must not crash
        print(f"[startup] reconcile deferred: {exc}")
    yield
    worker.stop()
    db.close()


app = FastAPI(title="Research Data Archive", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Pages
# --------------------------------------------------------------------------- #
@app.get("/", response_class=HTMLResponse)
def page_index(request: Request):
    batches = [dict(r) for r in db.list_batches()]
    jobs = [job_to_dict(r) for r in db.list_cold_jobs()]
    return templates.TemplateResponse(
        request,
        "index.html",
        {"batches": batches, "jobs": jobs, "witness_url": WITNESS_URL},
    )


@app.get("/batches/{batch_id}", response_class=HTMLResponse)
def page_batch(request: Request, batch_id: str):
    if not KEY_RE.fullmatch(batch_id):
        raise HTTPException(400, "invalid batch_id")
    return templates.TemplateResponse(
        request,
        "batch.html",
        {"batch_id": batch_id},
    )


@app.get("/verify", response_class=HTMLResponse)
def page_verify(request: Request):
    return templates.TemplateResponse(request, "verify.html", {})


# --------------------------------------------------------------------------- #
# Upload / download
# --------------------------------------------------------------------------- #
@app.post("/api/batches/{batch_id}/files")
def upload_file(batch_id: str,
                filename: str = Form(...),
                file: UploadFile = File(...)):
    # Sync endpoint on purpose: FastAPI runs it in a threadpool, so blocking
    # file IO and the synchronous witness call do not stall the event loop
    # and uploads of different batches truly run concurrently.
    validate_keys(batch_id, filename)

    # Read with the 2 MiB hard limit.
    hasher = hashlib.sha256()
    chunks: list[bytes] = []
    total = 0
    src = file.file  # underlying sync spooled file object
    while True:
        chunk = src.read(64 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_UPLOAD_BYTES:
            raise HTTPException(
                413,
                f"file exceeds archive limit of {MAX_UPLOAD_BYTES} bytes",
            )
        hasher.update(chunk)
        chunks.append(chunk)
    if total == 0:
        raise HTTPException(400, "empty file is not accepted")
    content = b"".join(chunks)
    digest = hasher.hexdigest()

    with batch_lock(batch_id):
        existing = db.get_file(batch_id, filename)
        if existing is not None:
            if existing["status"] == "registered" \
                    and existing["sha256"] == digest:
                # Idempotent re-upload of identical content: nothing to do.
                # This never rewrites a hot original nor flips a cold file
                # back to hot.
                return {"result": "already_registered",
                        "storage": existing["storage"],
                        "batch": batch_status(batch_id)}
            if existing["sha256"] != digest:
                raise HTTPException(
                    409,
                    "different content already exists for this "
                    "batch_id/filename; the witnessed original is preserved",
                )
            # Same content still pending: fall through to reconciliation.

        target = storage_path(batch_id, filename)
        # Never overwrite a stored original, even on failure/cleanup paths.
        # New uploads always land in hot storage, even for cooled batches.
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            # Unique temp name: concurrent uploads must not share a .part file.
            tmp = target.with_name(
                f".{target.name}.{uuid.uuid4().hex}.part")
            with open(tmp, "wb") as fh:
                fh.write(content)
            os.replace(tmp, target)

        try:
            response = witness.submit(batch_id, filename, content, digest)
        except WitnessUnreachable as exc:
            # Not yet known to be accepted: mark PENDING for verification;
            # must never be treated as "not accepted" and re-submitted blind.
            db.upsert_pending(batch_id, filename, total, digest,
                              f"witness unreachable: {exc}")
            return JSONResponse(
                status_code=202,
                content={"result": "pending",
                         "reason": "witness_unreachable",
                         "batch": batch_status(batch_id)},
            )
        except WitnessConflict as exc:
            db.upsert_pending(batch_id, filename, total, digest,
                              f"content conflict: {exc}")
            raise HTTPException(409, exc.detail)

        # Demo fault: the process dies after the witness accepted the original
        # but before the local catalog records the receipt. A marker file in
        # the persistent volume guarantees the crash happens only once.
        if CRASH_MARKER.exists():
            try:
                CRASH_MARKER.unlink()
            except OSError:
                pass
            print("[fault-injection] crashing after witness acceptance, "
                  "before local registration", flush=True)
            os._exit(1)

        envelope = response["receipt"]
        ok, err = verify_envelope(
            public_key, envelope,
            expected={"batch_id": batch_id, "filename": filename,
                      "length": total, "sha256": digest},
        )
        if not ok:
            db.upsert_pending(batch_id, filename, total, digest,
                              f"receipt verification failed: {err}")
            raise HTTPException(502, f"receipt verification failed: {err}")

        db.mark_registered(batch_id, filename, total, digest,
                           json.dumps(envelope, sort_keys=True))
        return {"result": "registered", "reused": response.get("reused"),
                "storage": "hot", "batch": batch_status(batch_id)}


@app.get("/api/batches")
def list_batches():
    return {"batches": [dict(r) for r in db.list_batches()]}


@app.get("/api/batches/{batch_id}")
def get_batch(batch_id: str):
    if not KEY_RE.fullmatch(batch_id):
        raise HTTPException(400, "invalid batch_id")
    rows = db.list_batch(batch_id)
    if not rows:
        raise HTTPException(404, "batch not found in local catalog")
    return batch_status(batch_id)


@app.get("/api/batches/{batch_id}/download/{filename}")
def download(batch_id: str, filename: str):
    validate_keys(batch_id, filename)
    row = db.get_file(batch_id, filename)
    if row is None or row["status"] != "registered":
        raise HTTPException(404, "file is not registered; download closed")

    if row["storage"] == "cold":
        # Single-member random read straight out of the persisted ZIP.
        try:
            content = cold.read_member(batch_id, filename)
        except ColdPackageError as exc:
            # A damaged/missing cold package is an anomaly, never silently
            # masked by whatever hot bytes may linger on disk.
            raise HTTPException(
                503, f"cold package unavailable: {exc}") from exc
        digest = hashlib.sha256(content).hexdigest()
        if digest != row["sha256"] or len(content) != row["length"]:
            raise HTTPException(409, "cold member fails integrity check; "
                                     "download refused")
        receipt = json.loads(row["receipt"])
        ok, err = verify_envelope(
            public_key, receipt,
            expected={"batch_id": batch_id, "filename": filename,
                      "length": len(content), "sha256": digest},
        )
        if not ok:
            raise HTTPException(409, f"receipt invalid: {err}")
        return Response(
            content=content,
            media_type="application/octet-stream",
            headers={"Content-Disposition":
                     f'attachment; filename="{filename}"'})

    # Hot download. Open the file *before* any check so that a cooling job
    # deleting the hot path after this download started cannot truncate it:
    # the open file descriptor keeps the inode alive until the last byte is
    # sent even if the directory entry is removed mid-transfer.
    path = storage_path(batch_id, filename)
    if not path.is_file():
        raise HTTPException(410, "registered original is missing on disk")
    fh = open(path, "rb")
    try:
        digest, length = sha256_file_handle(fh)
        if digest != row["sha256"] or length != row["length"]:
            raise HTTPException(409, "stored original fails integrity check; "
                                     "download refused")
        receipt = json.loads(row["receipt"])
        ok, err = verify_envelope(
            public_key, receipt,
            expected={"batch_id": batch_id, "filename": filename,
                      "length": length, "sha256": digest},
        )
        if not ok:
            raise HTTPException(409, f"receipt invalid: {err}")

        def _iter_chunks():
            try:
                while True:
                    chunk = fh.read(64 * 1024)
                    if not chunk:
                        break
                    yield chunk
            finally:
                # Closed here rather than in the endpoint: the response is
                # streamed after this function returns, and the fd must stay
                # open (keeping a possibly-unlinked inode readable) until the
                # final chunk is sent.
                fh.close()

        return StreamingResponse(
            _iter_chunks(),
            media_type="application/octet-stream",
            headers={"Content-Disposition":
                     f'attachment; filename="{filename}"',
                     "Content-Length": str(length)})
    except Exception:
        fh.close()
        raise


# --------------------------------------------------------------------------- #
# Cold storage (batch cooling)
# --------------------------------------------------------------------------- #
@app.post("/api/batches/{batch_id}/cold")
def enqueue_cooling(batch_id: str):
    """Accept a batch for cooling and freeze its registered file list.

    The first acceptance snapshots the currently registered files into the
    job manifest. Repeated cooling keeps that original frozen list; later
    uploads stay hot and are never silently added or deleted.
    """
    if not KEY_RE.fullmatch(batch_id):
        raise HTTPException(400, "invalid batch_id")
    with batch_lock(batch_id):
        rows = db.list_batch(batch_id)
        if not rows:
            raise HTTPException(404, "batch not found in local catalog")
        if any(r["status"] != "registered" for r in rows):
            raise HTTPException(
                409, "batch still has pending (unwitnessed) files; cooling "
                     "is only accepted after every file is registered")
        existing = db.get_cold_job(batch_id)
        if existing is not None and existing["state"] in COLD_ACTIVE_STATES:
            return JSONResponse(
                status_code=409,
                content={"result": "already_running",
                         "job": job_to_dict(existing)})
        if existing is None:
            # Freeze the registered list exactly as it is at first acceptance.
            manifest = [
                {"filename": r["filename"], "length": r["length"],
                 "sha256": r["sha256"]}
                for r in rows if r["status"] == "registered"
            ]
            db.insert_cold_job(batch_id, manifest)
        else:
            # Repeated cooling (completed/failed): keep the ORIGINAL frozen
            # manifest, just re-queue the work.
            db.requeue_cold_job(batch_id)
        job = job_to_dict(db.get_cold_job(batch_id))
    worker.kick()
    return {"result": "queued", "job": job}


@app.get("/api/batches/{batch_id}/cold")
def get_cooling(batch_id: str):
    if not KEY_RE.fullmatch(batch_id):
        raise HTTPException(400, "invalid batch_id")
    row = db.get_cold_job(batch_id)
    if row is None:
        raise HTTPException(404, "no cooling job for this batch")
    return {"job": job_to_dict(row)}


@app.get("/api/cold-jobs")
def list_cooling_jobs():
    return {"jobs": [job_to_dict(r) for r in db.list_cold_jobs()]}


# --------------------------------------------------------------------------- #
# Reconciliation after disconnect / restart
# --------------------------------------------------------------------------- #
def discover_orphans() -> list[dict]:
    """Find stored originals (hot OR cold) that have no catalog row.

    This covers the crash window after the witness accepted an original but
    before local registration: the file is on disk, the catalog has nothing.
    Cold packages are scanned too so a catalog row dropped after cooling can
    still be recovered from the ZIP member rather than declared lost.
    """
    orphans: list[dict] = []
    seen: set[tuple[str, str]] = set()
    if STORAGE_DIR.is_dir():
        for batch_dir in sorted(STORAGE_DIR.iterdir()):
            if not batch_dir.is_dir() or not KEY_RE.fullmatch(batch_dir.name):
                continue
            batch_id = batch_dir.name
            for path in sorted(batch_dir.iterdir()):
                if not path.is_file() or path.name.endswith(".part") \
                        or not KEY_RE.fullmatch(path.name):
                    continue
                if db.get_file(batch_id, path.name) is not None:
                    continue  # catalog already knows this file
                digest, length = sha256_file(path)
                key = (batch_id, path.name)
                seen.add(key)
                orphans.append({"batch_id": batch_id, "filename": path.name,
                                "length": length, "sha256": digest})

    # Catalog-less originals that live only inside a cold package.
    if COLD_DIR.is_dir():
        for pkg in sorted(COLD_DIR.glob("*.zip")):
            batch_id = pkg.stem
            if not KEY_RE.fullmatch(batch_id):
                continue
            members = cold.list_members(batch_id)
            if members is None:
                continue  # damaged package: the verify page reports it
            for name in members:
                if not KEY_RE.fullmatch(name):
                    continue
                if db.get_file(batch_id, name) is not None:
                    continue
                if (batch_id, name) in seen:
                    continue
                content, err = _read_cold_member(batch_id, name)
                if content is None:
                    continue
                digest = hashlib.sha256(content).hexdigest()
                seen.add((batch_id, name))
                orphans.append({"batch_id": batch_id, "filename": name,
                                "length": len(content), "sha256": digest})
    return orphans


def reconcile_one(target: dict) -> dict:
    """Check one pending/orphan target against the witness and continue it.

    ``target`` always carries batch_id, filename, length, sha256. The witness
    is queried first: an existing receipt is reused, so a disconnect can never
    cause a blind re-submission.
    """
    batch_id, filename = target["batch_id"], target["filename"]
    result = {"batch_id": batch_id, "filename": filename}

    # Make sure the catalog has a pending row so the work is visible even if
    # the witness is currently unreachable.
    if db.get_file(batch_id, filename) is None:
        db.upsert_pending(batch_id, filename, target["length"],
                          target["sha256"], "recovered orphan: pending")

    try:
        info = witness.get_batch(batch_id)
    except WitnessUnreachable as exc:
        # Still unknown: keep pending, show "to be verified", never resubmit.
        db.set_error(batch_id, filename, f"witness unreachable: {exc}")
        result.update(state="pending", reason="witness_unreachable")
        return result

    match = next((it for it in info["items"]
                  if it["filename"] == filename), None)
    if match is None:
        # Witness explicitly says it never accepted this file. It is safe to
        # (re)submit exactly the locally stored original.
        path = storage_path(batch_id, filename)
        if not path.is_file():
            db.set_error(batch_id, filename,
                         "witness 404 and local original missing")
            result.update(state="pending",
                          reason="witness_404_and_local_missing")
            return result
        content = path.read_bytes()
        try:
            response = witness.submit(batch_id, filename, content,
                                      target["sha256"])
        except WitnessUnreachable as exc:
            db.set_error(batch_id, filename, f"witness unreachable: {exc}")
            result.update(state="pending", reason="witness_unreachable")
            return result
        envelope = response["receipt"]
    else:
        envelope = match["receipt"]

    ok, err = verify_envelope(
        public_key, envelope,
        expected={"batch_id": batch_id, "filename": filename,
                  "length": target["length"], "sha256": target["sha256"]},
    )
    if not ok:
        db.set_error(batch_id, filename, f"receipt verification failed: {err}")
        result.update(state="pending", reason=f"bad_receipt: {err}")
        return result

    db.mark_registered(batch_id, filename, target["length"],
                       target["sha256"],
                       json.dumps(envelope, sort_keys=True))
    result.update(state="registered",
                  reused=match is not None)
    return result


def reconcile_pending() -> dict:
    # Merge catalog-pending rows with on-disk orphans (crash before register),
    # keyed by (batch_id, filename) so nothing is processed twice.
    targets: dict[tuple[str, str], dict] = {}
    for row in db.list_pending():
        targets[(row["batch_id"], row["filename"])] = dict(row)
    for orphan in discover_orphans():
        targets.setdefault((orphan["batch_id"], orphan["filename"]), orphan)

    results = []
    for target in targets.values():
        with batch_lock(target["batch_id"]):
            # Re-read inside the lock: a concurrent upload may have finished.
            current = db.get_file(target["batch_id"], target["filename"])
            if current is not None and current["status"] == "registered":
                results.append({"batch_id": target["batch_id"],
                                "filename": target["filename"],
                                "state": "already_registered"})
                continue
            results.append(reconcile_one(target))
    return {"checked": len(results), "results": results}


@app.post("/api/reconcile")
def reconcile_endpoint():
    return reconcile_pending()


# --------------------------------------------------------------------------- #
# Independent verification against the witness's issued-receipt list
# --------------------------------------------------------------------------- #
def _read_cold_member(batch_id: str, filename: str):
    """Return (content, error) for a cold member; never raises."""
    try:
        return cold.read_member(batch_id, filename), None
    except ColdPackageError as exc:
        return None, str(exc)


@app.get("/api/verify")
def verify_against_witness():
    try:
        listing = witness.list_receipts()
    except WitnessUnreachable as exc:
        # Witness unreachable: verification must NEVER be reported as passed.
        return JSONResponse(
            status_code=503,
            content={"passed": False, "witness_reachable": False,
                     "error": f"witness unreachable: {exc}", "batches": []},
        )

    # Expected witnessed members per batch, driving every re-hash below.
    expected: dict[str, set[str]] = {}
    batches: dict[str, dict] = {}
    for item in listing["items"]:
        expected.setdefault(item["batch_id"], set()).add(item["filename"])

    # Package-level audit FIRST: a missing/damaged cold package or an
    # undeclared member is an anomaly on its own, and must be surfaced even
    # when hot leftovers happen to remain readable — a bad package must never
    # be masked by a residual hot copy.
    package_anomalies = _scan_packages(expected)

    for item in listing["items"]:
        batch_id, filename = item["batch_id"], item["filename"]
        problems: list[dict] = []

        # The full signed envelope travels with the listing; verify it
        # against the fixed public key (no extra witness round-trips).
        envelope_resp = item.get("receipt")
        sig_ok, sig_err = (False, "no receipt in witness listing")
        if envelope_resp is not None:
            sig_ok, sig_err = verify_envelope(public_key, envelope_resp)
        if not sig_ok:
            problems.append({"type": "receipt_signature_invalid",
                             "detail": sig_err})

        catalog = db.get_file(batch_id, filename)
        storage = catalog["storage"] if catalog is not None else "unknown"
        hot_path = storage_path(batch_id, filename)
        local_present = False
        local_digest = local_length = None
        source = "missing"

        if storage == "cold":
            # The cold package is authoritative once the catalog switched.
            # Its health must be reported even if a hot copy still lingers.
            content, cold_err = _read_cold_member(batch_id, filename)
            if content is None:
                if not cold.package_exists(batch_id):
                    problems.append({
                        "type": "cold_package_missing",
                        "detail": "catalog points at cold storage but the "
                                  f"ZIP package is absent ({cold_err})",
                    })
                else:
                    problems.append({
                        "type": "cold_package_damaged",
                        "detail": "ZIP package cannot be read or member "
                                  f"failed CRC: {cold_err}",
                    })
            else:
                local_present = True
                source = "cold"
                local_digest = hashlib.sha256(content).hexdigest()
                local_length = len(content)
        else:
            # Hot originals (including catalog-missing witnesses: the hot
            # directory is still searched so 目录漏登 stays discoverable).
            if hot_path.is_file():
                local_digest, local_length = sha256_file(hot_path)
                local_present = True
                source = "hot"
            else:
                # Maybe it was cooled without a catalog row to say so: find
                # the bytes in the cold package rather than declaring loss.
                content, cold_err = _read_cold_member(batch_id, filename)
                if content is not None:
                    local_present = True
                    source = "cold"
                    local_digest = hashlib.sha256(content).hexdigest()
                    local_length = len(content)

        if not local_present:
            problems.append({"type": "missing_original",
                             "detail": "witnessed file absent from both hot "
                                       "storage and cold packages"})
        elif local_digest != item["sha256"] \
                or local_length != item["length"]:
            problems.append({
                "type": "tampered_original",
                "detail": f"{source} SHA-256/length differs from receipt",
                "local_sha256": local_digest,
                "receipt_sha256": item["sha256"],
            })

        if catalog is None:
            problems.append({"type": "catalog_missing",
                             "detail": "not registered in local catalog"})
        elif catalog["status"] != "registered":
            problems.append({"type": "catalog_pending",
                             "detail": f"catalog status is {catalog['status']}"})

        bucket = batches.setdefault(batch_id, {"batch_id": batch_id,
                                               "files": [], "passed": True,
                                               "package_problems": []})
        bucket["files"].append({
            "filename": filename,
            "receipt": {"length": item["length"], "sha256": item["sha256"],
                        "issued_at": item["issued_at"]},
            "local": {"present": local_present, "source": source,
                      "length": local_length, "sha256": local_digest},
            "catalogued": catalog is not None,
            "storage": storage,
            "receipt_signature_ok": sig_ok,
            "problems": problems,
        })
        if problems:
            bucket["passed"] = False

    # Fold the package-level anomalies into the affected batches so they are
    # visible both per batch and (below) as explicit package_problems rows.
    member_level = {"cold_package_missing", "cold_package_damaged"}
    for batch_id, anomalies in package_anomalies.items():
        bucket = batches.setdefault(batch_id, {"batch_id": batch_id,
                                               "files": [], "passed": True,
                                               "package_problems": []})
        bucket["passed"] = False
        for anomaly in anomalies:
            bucket["package_problems"].append(anomaly)
            # Mirror member-related anomalies onto the batch's file rows so
            # the per-file table shows them too, deduplicated against the
            # problems already recorded while reading each member.
            if anomaly["type"] not in member_level:
                continue
            for f in bucket["files"]:
                if any(p["type"] == anomaly["type"] for p in f["problems"]):
                    continue
                f["problems"].append(anomaly)

    # An empty witness list is reported explicitly, not silently "passed":
    # there is nothing to verify, and callers must not read it as a clean bill.
    return {
        "passed": all(b["passed"] for b in batches.values())
        and bool(batches),
        "empty": not batches,
        "witness_reachable": True,
        "receipt_count": listing["count"],
        "batches": sorted(batches.values(), key=lambda b: b["batch_id"]),
    }


def _scan_packages(expected: dict[str, set[str]]) -> dict[str, list[dict]]:
    """Audit every cold package on disk against the witness lists."""
    anomalies: dict[str, list[dict]] = {}
    if not COLD_DIR.is_dir():
        return anomalies
    for path in sorted(COLD_DIR.glob("*.zip")):
        batch_id = path.stem
        if not KEY_RE.fullmatch(batch_id):
            continue
        wanted = expected.get(batch_id, set())
        try:
            with zipfile.ZipFile(path) as zf:
                bad = zf.testzip()
                if bad is not None:
                    anomalies.setdefault(batch_id, []).append({
                        "type": "cold_package_damaged",
                        "detail": f"CRC/header failure on member {bad} "
                                  f"in {path.name}",
                    })
                    continue
                members = set(zf.namelist())
        except Exception as exc:
            anomalies.setdefault(batch_id, []).append({
                "type": "cold_package_damaged",
                "detail": f"{path.name} is unreadable: {exc}",
            })
            continue
        undeclared = sorted(members - wanted)
        for name in undeclared:
            anomalies.setdefault(batch_id, []).append({
                "type": "cold_undeclared_member",
                "detail": f"package {path.name} contains member not on the "
                          f"witness list: {name}",
            })
    return anomalies


# --------------------------------------------------------------------------- #
# Fault injection for the disconnect / cooling demos
# --------------------------------------------------------------------------- #
@app.post("/api/admin/arm-crash")
def arm_crash():
    CRASH_MARKER.parent.mkdir(parents=True, exist_ok=True)
    CRASH_MARKER.touch()
    return {"armed": True, "marker": str(CRASH_MARKER)}


@app.post("/api/admin/arm-cold-crash")
def arm_cold_crash():
    COLD_CRASH_MARKER.parent.mkdir(parents=True, exist_ok=True)
    COLD_CRASH_MARKER.touch()
    return {"armed": True, "marker": str(COLD_CRASH_MARKER)}


@app.post("/api/admin/clear-cold-crash")
def clear_cold_crash():
    try:
        COLD_CRASH_MARKER.unlink()
        cleared = True
    except FileNotFoundError:
        cleared = False
    return {"cleared": cleared, "marker": str(COLD_CRASH_MARKER)}


@app.post("/__selftest_shutdown")
def selftest_shutdown():
    """Test-only graceful shutdown (no signal permissions in CI sandbox)."""
    def _stop():
        app.state.server.should_exit = True
    threading.Thread(target=_stop, daemon=True).start()
    return {"shutting_down": True}
