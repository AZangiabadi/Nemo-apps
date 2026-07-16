from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from nemo_app.config import AppConfig

from .models import Job


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, (set, tuple)):
        return list(value)
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def _json_dumps(value: object) -> str:
    return json.dumps(value, default=_json_default, separators=(",", ":"))


class SecretCipher:
    def __init__(self, *, key: str = "", key_path: Path | None = None):
        if key:
            key_bytes = key.encode()
        elif key_path and key_path.exists():
            key_bytes = key_path.read_bytes().strip()
        elif key_path:
            key_path.parent.mkdir(parents=True, exist_ok=True)
            key_bytes = Fernet.generate_key()
            try:
                descriptor = os.open(
                    key_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                key_bytes = key_path.read_bytes().strip()
            else:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(key_bytes)
        else:
            key_bytes = Fernet.generate_key()
        try:
            self._fernet = Fernet(key_bytes)
        except (ValueError, TypeError) as exc:
            raise ValueError("NEMO_JOB_SECRET_KEY is not a valid Fernet key") from exc

    def encrypt(self, secrets: dict[str, str]) -> bytes:
        return self._fernet.encrypt(_json_dumps(secrets).encode())

    def decrypt(self, value: bytes | None) -> dict[str, str]:
        if not value:
            return {}
        try:
            payload = json.loads(self._fernet.decrypt(value).decode())
        except (InvalidToken, json.JSONDecodeError) as exc:
            raise ValueError("Unable to decrypt job secrets with the configured key") from exc
        return {str(key): str(item) for key, item in payload.items()}


class JobStore:
    def __init__(self, path: Path, *, cipher: SecretCipher):
        self.path = path
        self.cipher = cipher
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL DEFAULT '',
                    current INTEGER NOT NULL DEFAULT 0,
                    total INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    secrets_encrypted BLOB,
                    result_json TEXT NOT NULL DEFAULT '{}',
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT '',
                    worker_id TEXT NOT NULL DEFAULT '',
                    heartbeat_at TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS jobs_status_created
                    ON jobs(status, created_at);
                CREATE TABLE IF NOT EXISTS job_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    created_at TEXT NOT NULL,
                    message TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS job_logs_job ON job_logs(job_id, id);
                """
            )
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(jobs)")}
            if "heartbeat_at" not in columns:
                connection.execute(
                    "ALTER TABLE jobs ADD COLUMN heartbeat_at TEXT NOT NULL DEFAULT ''"
                )

    def enqueue(
        self,
        kind: str,
        *,
        payload: dict[str, Any],
        secrets: dict[str, str] | None = None,
        title: str | None = None,
        job_id: str | None = None,
    ) -> str:
        identifier = job_id or str(uuid.uuid4())
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO jobs (
                    id, kind, status, title, payload_json, secrets_encrypted, created_at
                ) VALUES (?, ?, 'pending', ?, ?, ?, ?)
                """,
                (
                    identifier,
                    kind,
                    title or kind.replace("_", " ").title(),
                    _json_dumps(payload),
                    self.cipher.encrypt(secrets or {}),
                    _now(),
                ),
            )
            self._insert_log(connection, identifier, "Job queued")
        return identifier

    @staticmethod
    def _insert_log(
        connection: sqlite3.Connection,
        job_id: str,
        message: str,
        *,
        created_at: str | None = None,
    ) -> str:
        timestamp = created_at or _now()
        connection.execute(
            "INSERT INTO job_logs(job_id, created_at, message) VALUES (?, ?, ?)",
            (job_id, timestamp, str(message)),
        )
        return timestamp

    def _job(self, row: sqlite3.Row, *, secrets: dict[str, str] | None = None) -> Job:
        return Job(
            id=row["id"],
            kind=row["kind"],
            status=row["status"],
            title=row["title"],
            summary=row["summary"],
            current=row["current"],
            total=row["total"],
            payload=json.loads(row["payload_json"]),
            result=json.loads(row["result_json"]),
            error=row["error"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            worker_id=row["worker_id"],
            secrets=secrets or {},
        )

    def get(self, job_id: str) -> Job | None:
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._job(row) if row else None

    def claim_next(self, worker_id: str) -> Job | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM jobs WHERE status = 'pending' ORDER BY created_at LIMIT 1"
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            started = _now()
            connection.execute(
                """
                UPDATE jobs SET status='running', started_at=?, heartbeat_at=?, worker_id=?
                WHERE id=? AND status='pending'
                """,
                (started, started, worker_id, row["id"]),
            )
            updated = connection.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
            connection.execute("COMMIT")
            return self._job(updated, secrets=self.cipher.decrypt(updated["secrets_encrypted"]))
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def append_log(self, job_id: str, message: str) -> None:
        with self._connection() as connection:
            created_at = self._insert_log(connection, job_id, message)
            connection.execute(
                "UPDATE jobs SET heartbeat_at=? WHERE id=? AND status='running'",
                (created_at, job_id),
            )

    def logs(self, job_id: str, *, limit: int = 400) -> list[str]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT message FROM job_logs WHERE job_id=? ORDER BY id DESC LIMIT ?",
                (job_id, limit),
            ).fetchall()
        return [row["message"] for row in reversed(rows)]

    def progress(self, job_id: str, current: int, total: int, summary: str) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE jobs SET current=?, total=?, summary=?, heartbeat_at=?
                WHERE id=? AND status='running'
                """,
                (max(0, current), max(0, total), summary, _now(), job_id),
            )

    def complete(self, job_id: str, *, summary: str, result: dict[str, Any]) -> None:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE jobs SET status='completed', summary=?, result_json=?,
                    secrets_encrypted=NULL, finished_at=?
                WHERE id=? AND status='running'
                """,
                (summary, _json_dumps(result), _now(), job_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Job {job_id!r} is not running")
            self._insert_log(connection, job_id, summary)

    def fail(self, job_id: str, *, error: str, log_message: str | None = None) -> None:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE jobs SET status='failed', summary='Job failed', error=?,
                    secrets_encrypted=NULL, finished_at=?
                WHERE id=? AND status='running'
                """,
                (error, _now(), job_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Job {job_id!r} is not running")
            self._insert_log(connection, job_id, log_message or error)

    def recover_stale_jobs(self, stale_seconds: int) -> int:
        cutoff = (dt.datetime.now(dt.UTC) - dt.timedelta(seconds=max(60, stale_seconds))).isoformat(
            timespec="seconds"
        )
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs SET status='pending', worker_id='', started_at='',
                    heartbeat_at='', summary='Recovered after a worker interruption'
                WHERE status='running' AND (heartbeat_at='' OR heartbeat_at < ?)
                """,
                (cutoff,),
            )
            return cursor.rowcount

    def expire_before(self, cutoff: dt.datetime) -> list[str]:
        cutoff_text = cutoff.astimezone(dt.UTC).isoformat(timespec="seconds")
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT id FROM jobs WHERE status IN ('completed','failed') AND finished_at < ?",
                (cutoff_text,),
            ).fetchall()
            identifiers = [row["id"] for row in rows]
            connection.executemany(
                "UPDATE jobs SET status='expired', result_json='{}' WHERE id=?",
                [(identifier,) for identifier in identifiers],
            )
        return identifiers


def create_job_store(config: AppConfig) -> JobStore:
    config.ensure_directories()
    cipher = SecretCipher(
        key=config.job_secret_key,
        key_path=None if config.job_secret_key else config.data_dir / "job_secret.key",
    )
    return JobStore(config.database_path, cipher=cipher)
