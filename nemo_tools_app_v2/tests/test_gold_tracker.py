from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from nemo_app.config import AppConfig
from nemo_app.web.app import create_app


class GoldTrackerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.config = replace(
            AppConfig.from_env(base_dir=root),
            data_dir=root / "data",
            asset_dir=root,
            access_password="",
            job_secret_key="",
            angstrom_log_app_url="https://pi.example",
        )
        self.app = create_app(self.config)
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session["csrf_token"] = "test-csrf"

    def test_mirror_preserves_scripts_and_uses_only_local_api(self):
        source = (
            "<!doctype html><html><head><title>Pi Tracker</title></head><body>"
            '<script>fetch("/api/scans");const a="A & B";</script></body></html>'
        )
        upstream = Mock(status_code=200, text=source, headers={"Content-Type": "text/html"})
        with patch(
            "nemo_app.web.blueprints.gold_tracker.requests.get", return_value=upstream
        ) as fetch:
            response = self.client.get("/tools/gold-thickness-tracker")
        self.assertEqual(response.status_code, 200)
        self.assertIn('fetch("/api/scans");const a="A & B";', response.text)
        self.assertIn("X-CSRF-Token", response.text)
        self.assertIn("/tools/gold-thickness-tracker/api/scans", response.text)
        self.assertNotIn("pi.example", response.text)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertIn("connect-src 'self'", response.headers["Content-Security-Policy"])
        self.assertFalse(fetch.call_args.kwargs["allow_redirects"])

    def test_scans_require_csrf_and_validate_dates(self):
        endpoint = "/tools/gold-thickness-tracker/api/scans"
        with patch("nemo_app.web.blueprints.gold_tracker.requests.request") as upstream:
            self.assertEqual(
                self.client.post(
                    endpoint, json={"start": "2026-01-01", "end": "2026-01-02"}
                ).status_code,
                400,
            )
            for payload in (
                None,
                {},
                {"start": "2026-02-01", "end": "2026-01-01"},
                {"start": 5, "end": "2026-01-01"},
            ):
                response = self.client.post(
                    endpoint, json=payload, headers={"X-CSRF-Token": "test-csrf"}
                )
                self.assertEqual(response.status_code, 400)
            upstream.assert_not_called()

    def test_scan_creation_and_polling_relay_only_fixed_endpoints(self):
        upstream = Mock(status_code=200)
        upstream.json.return_value = {"job_id": "scan-1"}
        with patch(
            "nemo_app.web.blueprints.gold_tracker.requests.request", return_value=upstream
        ) as fetch:
            response = self.client.post(
                "/tools/gold-thickness-tracker/api/scans",
                json={"start": "2026-01-01", "end": "2026-01-02", "url": "https://evil.example"},
                headers={"X-CSRF-Token": "test-csrf", "Authorization": "secret-browser-credential"},
            )
            self.assertEqual(response.json, {"job_id": "scan-1"})
            self.assertEqual(fetch.call_args.args, ("POST", "https://pi.example/api/scans"))
            self.assertEqual(
                fetch.call_args.kwargs["json"], {"start": "2026-01-01", "end": "2026-01-02"}
            )
            self.assertEqual(fetch.call_args.kwargs["headers"], {"Accept": "application/json"})
            self.assertFalse(fetch.call_args.kwargs["allow_redirects"])
            upstream.json.return_value = {"state": "complete", "result": {"runs": []}}
            response = self.client.get("/tools/gold-thickness-tracker/api/scans/scan-1")
            self.assertEqual(response.json["state"], "complete")
            self.assertEqual(fetch.call_args.args, ("GET", "https://pi.example/api/scans/scan-1"))
            fetch.reset_mock()
            self.assertEqual(
                self.client.get("/tools/gold-thickness-tracker/api/scans/bad.id").status_code, 404
            )
            self.assertEqual(
                self.client.get("/tools/gold-thickness-tracker/api/scans/a/b").status_code, 404
            )
            fetch.assert_not_called()

    def test_upstream_errors_are_safe_and_do_not_redirect(self):
        with patch(
            "nemo_app.web.blueprints.gold_tracker.requests.get",
            side_effect=requests.ConnectionError("private backend error"),
        ):
            response = self.client.get("/tools/gold-thickness-tracker")
            self.assertEqual(response.status_code, 502)
            self.assertNotIn("private backend error", response.text)
        upstream = Mock(status_code=302)
        with patch("nemo_app.web.blueprints.gold_tracker.requests.request", return_value=upstream):
            response = self.client.get("/tools/gold-thickness-tracker/api/scans/id")
            self.assertEqual(response.status_code, 502)
            self.assertNotIn("Location", response.headers)

    def test_page_and_both_api_endpoints_require_login(self):
        protected = create_app(replace(self.config, access_password="secret")).test_client()
        for path in ("/tools/gold-thickness-tracker", "/tools/gold-thickness-tracker/api/scans/id"):
            self.assertEqual(protected.get(path).status_code, 302)
        self.assertEqual(
            protected.post("/tools/gold-thickness-tracker/api/scans", json={}).status_code, 302
        )
