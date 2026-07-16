from __future__ import annotations

import datetime as dt
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from nemo_app.jobs.store import JobStore, SecretCipher


class JobStoreTests(unittest.TestCase):
    def test_job_is_durable_and_secrets_are_not_plaintext(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cipher = SecretCipher(key_path=root / "secret.key")
            store = JobStore(root / "jobs.sqlite3", cipher=cipher)
            job_id = store.enqueue(
                "invoice",
                payload={"source": "input.csv"},
                secrets={"api_token": "very-secret-token"},
            )
            raw_database = (root / "jobs.sqlite3").read_bytes()
            self.assertNotIn(b"very-secret-token", raw_database)

            claimed = store.claim_next("test-worker")
            self.assertIsNotNone(claimed)
            assert claimed is not None
            self.assertEqual(claimed.id, job_id)
            self.assertEqual(claimed.secrets["api_token"], "very-secret-token")

            store.complete(job_id, summary="Done", result={"files": ["one.xlsx"]})
            reopened = JobStore(root / "jobs.sqlite3", cipher=cipher)
            completed = reopened.get(job_id)
            self.assertIsNotNone(completed)
            assert completed is not None
            self.assertEqual(completed.status, "completed")
            self.assertEqual(completed.result["files"], ["one.xlsx"])

    def test_result_values_are_converted_to_portable_json(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            store = JobStore(
                root / "jobs.sqlite3",
                cipher=SecretCipher(key_path=root / "secret.key"),
            )
            job_id = store.enqueue("report", payload={})
            store.claim_next("worker")
            store.complete(
                job_id,
                summary="Done",
                result={
                    "files": [],
                    "data": {
                        "date": dt.date(2026, 7, 16),
                        "path": root / "report.xlsx",
                    },
                },
            )
            result = store.get(job_id)
            assert result is not None
            self.assertEqual(result.result["data"]["date"], "2026-07-16")
            self.assertEqual(result.result["data"]["path"], str(root / "report.xlsx"))

    def test_recovery_only_requeues_stale_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            store = JobStore(
                root / "jobs.sqlite3",
                cipher=SecretCipher(key_path=root / "secret.key"),
            )
            stale_id = store.enqueue("one", payload={})
            store.claim_next("worker-one")
            active_id = store.enqueue("two", payload={})
            store.claim_next("worker-two")
            with closing(sqlite3.connect(store.path)) as connection:
                connection.execute(
                    "UPDATE jobs SET heartbeat_at='2020-01-01T00:00:00+00:00' WHERE id=?",
                    (stale_id,),
                )
                connection.commit()
            self.assertEqual(store.recover_stale_jobs(3600), 1)
            stale = store.get(stale_id)
            active = store.get(active_id)
            assert stale is not None
            assert active is not None
            self.assertEqual(stale.status, "pending")
            self.assertEqual(active.status, "running")


if __name__ == "__main__":
    unittest.main()
