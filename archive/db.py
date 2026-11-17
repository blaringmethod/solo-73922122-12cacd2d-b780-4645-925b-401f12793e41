"""SQLite persistence for the archiver (the local catalog).

Each operation opens its own connection: with a single shared connection,
closing a read cursor rolls back any open write transaction on that same
connection. WAL mode keeps concurrent readers/writers working correctly.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id      TEXT NOT NULL,
    filename      TEXT NOT NULL,
    length        INTEGER NOT NULL,
    sha256        TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('pending','registered')),
    storage       TEXT NOT NULL DEFAULT 'hot',
    receipt       TEXT,
    last_error    TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(batch_id, filename)
);
CREATE INDEX IF NOT EXISTS idx_files_batch ON files(batch_id);
CREATE TABLE IF NOT EXISTS cold_jobs (
    batch_id     TEXT PRIMARY KEY,
    state        TEXT NOT NULL,
    manifest     TEXT NOT NULL,
    total        INTEGER NOT NULL,
    processed    INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# Cold-job state machine.
COLD_ACTIVE_STATES = ("queued", "compressing", "verifying",
                      "switching", "cleaning")
COLD_TERMINAL_STATES = ("completed", "failed")


class Database:
    def __init__(self, path: str):
        self._path = path
        with self.cursor() as cur:
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA busy_timeout=5000")
            cur.executescript(SCHEMA)
            self._migrate(cur)

    def _migrate(self, cur: sqlite3.Cursor) -> None:
        """Upgrade catalogs created before cold storage existed."""
        cols = {r[1] for r in cur.execute("PRAGMA table_info(files)")}
        if "storage" not in cols:
            # Every pre-existing original is a hot file by definition.
            cur.execute(
                "ALTER TABLE files ADD COLUMN storage TEXT NOT NULL "
                "DEFAULT 'hot'")

    @contextmanager
    def cursor(self):
        conn = sqlite3.connect(self._path, timeout=10)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def get_file(self, batch_id: str, filename: str) -> sqlite3.Row | None:
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM files WHERE batch_id=? AND filename=?",
                (batch_id, filename),
            )
            return cur.fetchone()

    def list_batch(self, batch_id: str) -> list[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM files WHERE batch_id=? ORDER BY filename",
                (batch_id,),
            )
            return cur.fetchall()

    def list_batches(self) -> list[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute(
                """SELECT batch_id,
                          COUNT(*) AS file_count,
                          SUM(CASE WHEN status='registered' THEN 1 ELSE 0 END)
                              AS registered_count,
                          SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END)
                              AS pending_count,
                          SUM(CASE WHEN storage='cold' THEN 1 ELSE 0 END)
                              AS cold_count,
                          MIN(created_at) AS first_seen
                   FROM files GROUP BY batch_id
                   ORDER BY MIN(created_at) DESC"""
            )
            return cur.fetchall()

    def list_pending(self) -> list[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM files WHERE status='pending' "
                "ORDER BY batch_id, filename"
            )
            return cur.fetchall()

    def upsert_pending(self, batch_id: str, filename: str,
                       length: int, sha256: str, error: str | None) -> None:
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO files (batch_id, filename, length, sha256,
                                      status, last_error)
                   VALUES (?, ?, ?, ?, 'pending', ?)
                   ON CONFLICT(batch_id, filename) DO UPDATE SET
                       length=excluded.length, sha256=excluded.sha256,
                       last_error=excluded.last_error,
                       updated_at=datetime('now')""",
                (batch_id, filename, length, sha256, error),
            )

    def mark_registered(self, batch_id: str, filename: str,
                        length: int, sha256: str, receipt: str) -> None:
        # Upsert, not a bare UPDATE: a fresh upload may never have had a
        # pending row, and an UPDATE matching zero rows would silently drop
        # the registration. New registrations always start as hot originals.
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO files (batch_id, filename, length, sha256,
                                      status, storage, receipt, last_error)
                   VALUES (?, ?, ?, ?, 'registered', 'hot', ?, NULL)
                   ON CONFLICT(batch_id, filename) DO UPDATE SET
                       length=excluded.length, sha256=excluded.sha256,
                       status='registered', receipt=excluded.receipt,
                       last_error=NULL, updated_at=datetime('now')""",
                (batch_id, filename, length, sha256, receipt),
            )

    def set_error(self, batch_id: str, filename: str,
                  error: str) -> None:
        with self.cursor() as cur:
            cur.execute(
                """UPDATE files SET last_error=?,
                       updated_at=datetime('now')
                   WHERE batch_id=? AND filename=?""",
                (error, batch_id, filename),
            )

    # ------------------------------------------------------------------ #
    # Cold storage jobs
    # ------------------------------------------------------------------ #
    def get_cold_job(self, batch_id: str) -> sqlite3.Row | None:
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM cold_jobs WHERE batch_id=?", (batch_id,))
            return cur.fetchone()

    def list_cold_jobs(self) -> list[sqlite3.Row]:
        with self.cursor() as cur:
            cur.execute(
                "SELECT * FROM cold_jobs ORDER BY updated_at DESC")
            return cur.fetchall()

    def insert_cold_job(self, batch_id: str, manifest: list[dict]) -> None:
        """Freeze the registered file list for this batch."""
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO cold_jobs (batch_id, state, manifest, total,
                                          processed, last_error)
                   VALUES (?, 'queued', ?, ?, 0, NULL)
                   ON CONFLICT(batch_id) DO UPDATE SET
                       state='queued', manifest=excluded.manifest,
                       total=excluded.total, processed=0, last_error=NULL,
                       updated_at=datetime('now')""",
                (batch_id, json.dumps(manifest, sort_keys=True),
                 len(manifest)),
            )

    def requeue_cold_job(self, batch_id: str) -> None:
        """Re-queue a terminal job while KEEPING its original frozen manifest."""
        with self.cursor() as cur:
            cur.execute(
                """UPDATE cold_jobs SET state='queued', processed=0,
                       last_error=NULL, updated_at=datetime('now')
                   WHERE batch_id=?""",
                (batch_id,))

    def update_cold_job(self, batch_id: str, *, state: str | None = None,
                        processed: int | None = None,
                        error: str | None = None,
                        clear_error: bool = False) -> None:
        fields = ["updated_at=datetime('now')"]
        params: list = []
        if state is not None:
            fields.append("state=?")
            params.append(state)
        if processed is not None:
            fields.append("processed=?")
            params.append(processed)
        if error is not None:
            fields.append("last_error=?")
            params.append(error)
        if clear_error:
            fields.append("last_error=NULL")
        params.append(batch_id)
        with self.cursor() as cur:
            cur.execute(
                f"UPDATE cold_jobs SET {', '.join(fields)} WHERE batch_id=?",
                params)

    def mark_storage_cold(self, batch_id: str,
                          filenames: list[str]) -> None:
        """Flip every frozen member to cold in one transaction.

        The switch of the retrieval location is therefore atomic across the
        catalog: a reader never sees a half-switched batch.
        """
        with self.cursor() as cur:
            cur.executemany(
                """UPDATE files SET storage='cold',
                       updated_at=datetime('now')
                   WHERE batch_id=? AND filename=?""",
                [(batch_id, name) for name in filenames],
            )

    def list_unfinished_cold_jobs(self) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in COLD_ACTIVE_STATES + ("failed",))
        states = (*COLD_ACTIVE_STATES, "failed")
        with self.cursor() as cur:
            cur.execute(
                f"SELECT * FROM cold_jobs WHERE state IN ({placeholders}) "
                "ORDER BY batch_id", states)
            return cur.fetchall()

    def close(self) -> None:
        # Connections are per-operation; nothing persistent to close.
        return None
