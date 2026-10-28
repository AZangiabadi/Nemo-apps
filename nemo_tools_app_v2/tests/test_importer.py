from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import requests

from nemo_app.imports.user_importer import SpreadsheetRow, _create_user, run_import
from nemo_app.nemo.cache import JsonTTLCache
from nemo_app.nemo.client import NemoClient


class ImporterTests(unittest.TestCase):
    def test_invalid_email_stops_before_any_api_requests(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spreadsheet = root / "users.csv"
            spreadsheet.write_text(
                "Name,UNI,Email,PI,Account Type,Project Number\n"
                "Valid User,vu1,valid@example.edu,,Local,P100\n"
                "Invalid User,iu1,invalid@example,,Local,P100\n"
            )
            client = Mock()
            with self.assertRaisesRegex(ValueError, r"Row 3.*invalid@example"):
                run_import(spreadsheet, client=client, cache=JsonTTLCache(root / "cache"))
            self.assertEqual(client.mock_calls, [])

    def test_nemo_validation_error_reports_row_and_field(self) -> None:
        response = requests.Response()
        response.status_code = 400
        response._content = b'{"username": ["A user with that username already exists."]}'
        client = Mock()
        client.post.side_effect = requests.HTTPError(response=response)
        row = SpreadsheetRow(3, "Example User", "eu1", "user@example.edu", "", "local", "P100")
        with self.assertRaisesRegex(ValueError, r"Row 3 \(eu1\).*username.*already exists"):
            _create_user(client, row, projects=[100])

    def test_dry_run_finishes_all_phases_without_caching_synthetic_records(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spreadsheet = root / "users.csv"
            spreadsheet.write_text(
                "Name,UNI,Email,PI,Account Type,Project Number\n"
                "Ada PI,,ada.pi@example.edu,PI,Local,P100\n"
                "Ada PI,,ada.pi@example.edu,PI,Local,P200\n"
                "Grace User,,grace@example.edu,,Local,P100\n",
                encoding="utf-8",
            )
            client = NemoClient("test-token", dry_run=True)
            client.fetch_all = lambda _endpoint: []  # type: ignore[method-assign]
            cache = JsonTTLCache(root / "cache", 300)
            progress: list[tuple[int, int, str]] = []

            result = run_import(
                spreadsheet,
                client=client,
                cache=cache,
                progress=lambda current, total, message: progress.append((current, total, message)),
            )

            self.assertTrue(result.dry_run)
            self.assertEqual(result.row_count, 3)
            self.assertEqual(result.project_count, 2)
            self.assertEqual([value[0] for value in progress], list(range(1, 9)))
            self.assertTrue(all(value[1] == 8 for value in progress))
            cached = cache.get(f"{client.identity_hash}:import-records")
            self.assertEqual(cached["projects"], {})
            self.assertNotIn("-1", str(cached))


if __name__ == "__main__":
    unittest.main()
