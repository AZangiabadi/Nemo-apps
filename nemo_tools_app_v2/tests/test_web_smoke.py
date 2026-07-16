from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from io import BytesIO
from pathlib import Path

from nemo_app.config import AppConfig
from nemo_app.dashboard.service import DashboardReport
from nemo_app.web.app import create_app


class WebSmokeTests(unittest.TestCase):
    def _config(self, root: Path, **changes) -> AppConfig:
        values = {
            "data_dir": root / "data",
            "asset_dir": root,
            "access_password": "",
            "jumbotron_api_token": "",
            "job_secret_key": "",
            **changes,
        }
        return replace(AppConfig.from_env(base_dir=root), **values)

    def test_all_tool_pages_render(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(Path(folder))
            app = create_app(config)
            app.config.update(TESTING=True)
            client = app.test_client()
            for path in (
                "/",
                "/tools/user-import",
                "/tools/invoices",
                "/tools/reports",
                "/tools/excel-to-pdf",
                "/tools/replacement",
                "/jumbotron",
            ):
                with self.subTest(path=path):
                    self.assertEqual(client.get(path).status_code, 200)

    def test_upload_is_queued_and_token_is_encrypted(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(Path(folder))
            app = create_app(config)
            app.config.update(TESTING=True)
            client = app.test_client()
            client.get("/tools/invoices")
            with client.session_transaction() as session:
                csrf = session["csrf_token"]
            response = client.post(
                "/tools/invoices",
                data={
                    "csrf_token": csrf,
                    "api_token": "web-secret-token",
                    "csv_file": (BytesIO(b"Type,User\n"), "usage.csv"),
                    "generate_excel": "on",
                    "generate_pdf": "on",
                    "make_zip": "on",
                    "apply_hourly_caps": "on",
                },
                content_type="multipart/form-data",
            )
            self.assertEqual(response.status_code, 302)
            self.assertIn("/jobs/", response.headers["Location"])
            self.assertNotIn(b"web-secret-token", config.database_path.read_bytes())

    def test_login_rejects_external_next_url(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(Path(folder), access_password="correct-horse")
            app = create_app(config)
            app.config.update(TESTING=True)
            client = app.test_client()
            client.get("/login")
            with client.session_transaction() as session:
                csrf = session["csrf_token"]
            response = client.post(
                "/login?next=https://example.net/phishing",
                data={"csrf_token": csrf, "password": "correct-horse"},
            )
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/")

    def test_login_requires_csrf_even_with_the_correct_password(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(Path(folder), access_password="correct-horse")
            app = create_app(config)
            app.config.update(TESTING=True)
            response = app.test_client().post(
                "/login",
                data={"password": "correct-horse"},
            )
            self.assertEqual(response.status_code, 400)

    def test_invalid_production_configuration_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(Path(folder), environment="production")
            with self.assertRaisesRegex(ValueError, "Invalid production configuration"):
                create_app(config)

    def test_jumbotron_includes_configured_auto_scroll(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(
                Path(folder),
                jumbotron_api_token="dashboard-token",
                jumbotron_scroll_step_px=2,
                jumbotron_scroll_interval_ms=75,
            )
            app = create_app(config)
            app.config.update(TESTING=True)
            report = DashboardReport(
                generated_at="Thu, Jul 16, 2026 12:00 PM",
                current_usage=(("ada", "SEM", "12:00 PM"),),
                upcoming=(),
                cancellations=(),
            )
            app.extensions["dashboard_service"].report = lambda _client: report

            response = app.test_client().get("/jumbotron")
            page = response.get_data(as_text=True)

            self.assertEqual(response.status_code, 200)
            self.assertIn("function startAutoScroll()", page)
            self.assertIn("const scrollStepPx = 2;", page)
            self.assertIn("const scrollIntervalMs = 75;", page)
            self.assertIn('class="jumbotron-page"', page)
            self.assertNotIn('class="site-header"', page)
            self.assertNotIn('aria-label="Main navigation"', page)


if __name__ == "__main__":
    unittest.main()
