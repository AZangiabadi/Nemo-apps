from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from nemo_app.imports.qualification_importer import run_qualification_import
from nemo_app.nemo.client import NemoClient


class QualificationImporterTests(unittest.TestCase):
    def test_dry_run_creates_updates_and_skips_expected_rows(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "qualifications.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.append(["Notes", "Tool ID", "Qualification Date", "User Email"])
            sheet.append(["create", 10, dt.date(2026, 4, 1), "ada@example.edu"])
            sheet.append(["duplicate", 10, "2026-04-01", "ada@example.edu"])
            sheet.append(["update", 11, "04/02/2026", "ada@example.edu"])
            sheet.append(["unchanged", 10, "2026-03-01", "grace@example.edu"])
            sheet.append(["missing user", 10, "2026-04-03", "missing@example.edu"])
            sheet.append(["invalid tool", 999, "2026-04-04", "ada@example.edu"])
            sheet.append(["invalid date", 10, "not a date", "ada@example.edu"])
            workbook.save(path)
            workbook.close()

            client = NemoClient("test-token", dry_run=True)

            def fetch_all(endpoint: str) -> list[dict[str, object]]:
                if endpoint == "users/":
                    return [
                        {"id": 1, "email": "ada@example.edu"},
                        {"id": 2, "email": "grace@example.edu"},
                    ]
                if endpoint == "tools/":
                    return [{"id": 10}, {"id": 11}]
                if endpoint == "qualifications/":
                    return [
                        {"id": 100, "user": 1, "tool": 11, "qualified_on": "2025-01-01"},
                        {"id": 101, "user": 2, "tool": 10, "qualified_on": "2026-03-01"},
                    ]
                raise AssertionError(endpoint)

            client.fetch_all = fetch_all  # type: ignore[method-assign]
            progress: list[tuple[int, int, str]] = []
            logs: list[str] = []
            result = run_qualification_import(
                path,
                client=client,
                progress=lambda current, total, message: progress.append((current, total, message)),
                log=logs.append,
            )

            self.assertEqual(result.row_count, 6)
            self.assertEqual(result.created_count, 1)
            self.assertEqual(result.updated_count, 1)
            self.assertEqual(result.unchanged_count, 2)
            self.assertEqual(result.invalid_row_count, 1)
            self.assertEqual(result.missing_user_count, 1)
            self.assertEqual(result.invalid_tool_count, 1)
            self.assertTrue(result.dry_run)
            self.assertEqual([item[0] for item in progress], list(range(1, 7)))
            self.assertTrue(all(item[1] == 6 for item in progress))
            self.assertEqual(
                sum(action.startswith("POST qualifications/") for action in client.actions), 1
            )
            self.assertEqual(
                sum(action.startswith("PATCH qualifications/100/") for action in client.actions),
                1,
            )
            self.assertIn("1 created, 1 updated, 2 unchanged", logs[-1])


if __name__ == "__main__":
    unittest.main()
