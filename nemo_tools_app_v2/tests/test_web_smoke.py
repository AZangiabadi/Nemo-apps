from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from io import BytesIO
from pathlib import Path

from nemo_app.config import AppConfig
from nemo_app.dashboard.service import DashboardReport
from nemo_app.jobs.handlers import JobContext
from nemo_app.web.app import create_app


class WebSmokeTests(unittest.TestCase):
    def _config(self, root: Path, **changes) -> AppConfig:
        values = {
            "data_dir": root / "data",
            "asset_dir": root,
            "access_password": "",
            "read_only_api_token": "",
            "jumbotron_api_token": "",
            "jumbotron_kiosk_token": "",
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
                "/tools/detailed-financials",
                "/tools/usage-caps",
                "/tools/user-pi",
                "/tools/excel-to-pdf",
                "/tools/missed-reservations",
                "/tools/active-users",
                "/tools/replacement",
                "/jumbotron",
                "/tools/reports",
            ):
                with self.subTest(path=path):
                    self.assertEqual(client.get(path).status_code, 200)

    def test_home_preserves_legacy_tool_navigation(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            app = create_app(self._config(Path(folder)))
            app.config.update(TESTING=True)

            page = app.test_client().get("/").get_data(as_text=True)
            titles = (
                "User/Account/Project Batch Import",
                "NEMO Invoice Generator",
                "Detailed Financials",
                "Usage Cap Analysis",
                "User PI Report",
                "Excel Invoice to PDF",
                "Missed Reservations",
                "Active Lab Users",
                "Replace Account/Project",
                "Jumbotron",
            )

            positions = [page.index(title) for title in titles]
            self.assertEqual(positions, sorted(positions))

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

    def test_server_read_only_token_removes_read_tool_token_fields(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            server_token = "server-read-only-token"
            config = self._config(Path(folder), read_only_api_token=server_token)
            app = create_app(config)
            app.config.update(TESTING=True)
            client = app.test_client()

            for path in (
                "/tools/invoices",
                "/tools/detailed-financials",
                "/tools/user-pi",
                "/tools/active-users",
            ):
                with self.subTest(path=path):
                    page = client.get(path).get_data(as_text=True)
                    self.assertNotIn('name="api_token"', page)
                    self.assertIn("protected NEMO service account", page)

            for path in ("/tools/user-import", "/tools/replacement"):
                with self.subTest(path=path):
                    page = client.get(path).get_data(as_text=True)
                    self.assertIn('name="api_token"', page)

            client.get("/tools/invoices")
            with client.session_transaction() as session:
                csrf = session["csrf_token"]
            response = client.post(
                "/tools/invoices",
                data={
                    "csrf_token": csrf,
                    "csv_file": (BytesIO(b"Type,User\n"), "usage.csv"),
                    "generate_excel": "on",
                },
                content_type="multipart/form-data",
            )

            self.assertEqual(response.status_code, 302)
            self.assertIn("/jobs/", response.headers["Location"])
            self.assertNotIn(server_token.encode(), config.database_path.read_bytes())
            claimed = app.extensions["job_store"].claim_next("test-worker")
            self.assertIsNotNone(claimed)
            assert claimed is not None
            self.assertEqual(claimed.secrets, {})
            context = JobContext(config, app.extensions["job_store"], claimed)
            self.assertEqual(context.read_only_client().token, server_token)
            with self.assertRaisesRegex(ValueError, "requires a NEMO API token"):
                context.client()

    def test_server_read_only_token_queues_all_read_only_reports(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(Path(folder), read_only_api_token="server-read-only-token")
            app = create_app(config)
            app.config.update(TESTING=True)
            client = app.test_client()
            client.get("/tools/invoices")
            with client.session_transaction() as session:
                csrf = session["csrf_token"]

            uploads = (
                ("/tools/reports/detailed-financials", "billing.csv"),
                ("/tools/reports/user-pi", "billing.csv"),
            )
            for path, filename in uploads:
                with self.subTest(path=path):
                    response = client.post(
                        path,
                        data={
                            "csrf_token": csrf,
                            "csv_file": (BytesIO(b"Type,User\n"), filename),
                        },
                        content_type="multipart/form-data",
                    )
                    self.assertEqual(response.status_code, 302)
                    self.assertIn("/jobs/", response.headers["Location"])

            response = client.post(
                "/tools/reports/active-users",
                data={"csrf_token": csrf},
            )
            self.assertEqual(response.status_code, 302)
            self.assertIn("/jobs/", response.headers["Location"])

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

    def test_kiosk_token_only_authenticates_jumbotron_routes(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(
                Path(folder),
                access_password="correct-horse",
                jumbotron_api_token="dashboard-token",
                jumbotron_kiosk_token="kiosk-secret-with-at-least-32-characters",
            )
            app = create_app(config)
            app.config.update(TESTING=True)
            report = DashboardReport(
                generated_at="Thu, Jul 16, 2026 12:00 PM",
                current_usage=(),
                upcoming=(),
                cancellations=(),
            )
            app.extensions["dashboard_service"].report = lambda _client: report
            client = app.test_client()

            response = client.get("/jumbotron?kiosk_token=kiosk-secret-with-at-least-32-characters")

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/jumbotron")
            self.assertNotIn("kiosk-secret", response.headers["Location"])
            self.assertEqual(client.get("/jumbotron").status_code, 200)
            self.assertEqual(client.get("/jumbotron/data").status_code, 200)
            denied = client.get("/tools/invoices")
            self.assertEqual(denied.status_code, 302)
            self.assertTrue(denied.headers["Location"].startswith("/login?next="))

    def test_invalid_kiosk_token_is_not_repeated_in_login_redirect(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            config = self._config(
                Path(folder),
                access_password="correct-horse",
                jumbotron_kiosk_token="kiosk-secret-with-at-least-32-characters",
            )
            app = create_app(config)
            app.config.update(TESTING=True)

            response = app.test_client().get("/jumbotron?kiosk_token=incorrect-secret")

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/login?next=/jumbotron")
            self.assertNotIn("incorrect-secret", response.headers["Location"])


if __name__ == "__main__":
    unittest.main()
