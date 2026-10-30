from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pandas as pd

from nemo_app.config import AppConfig
from nemo_app.jobs.handlers import JobContext, run_job
from nemo_app.nemo.billing import fetch_billing_dataframe
from nemo_app.web.app import create_app
from tests.fixtures import usage_frame
from tests.test_services import _Metadata

REPORTS = {
    "detailed-financials": "detailed_financials",
    "usage-caps": "usage_caps",
    "user-pi": "user_pi",
    "missed-reservations": "missed_reservations",
}


class ReportApiTests(unittest.TestCase):
    def test_missed_jobs_use_tables_and_charts_instead_of_raw_json(self):
        with tempfile.TemporaryDirectory() as folder:
            app = create_app(self.config(Path(folder)))
            store = app.extensions["job_store"]
            job_id = store.enqueue(
                "missed_reservations",
                title="Missed reservation report",
                payload={"source": "api", "billing_month": "2026-09"},
            )
            store.claim_next("test-worker")
            store.complete(
                job_id,
                summary="Ready",
                result={
                    "files": [],
                    "data": {
                        "users": [{"User": "Sample", "Missed Reservations": 7}],
                        "tools": [{"Tool": "SEM", "Missed Reservations": 7}],
                        "total_users": 3,
                    },
                },
            )
            client = app.test_client()
            response = client.get(f"/jobs/{job_id}")
            self.assertEqual(response.status_code, 200)
            page = response.get_data(as_text=True)
            for marker in (
                'id="users-chart"',
                'id="tools-chart"',
                'id="users-body"',
                'id="tools-body"',
                "Users with 5+ misses",
                "2026-09",
            ):
                self.assertIn(marker, page)
            self.assertIn("missed_reservations.js", page)
            self.assertNotIn("JSON.stringify", page)
            data = client.get(f"/jobs/{job_id}/status").get_json()["data"]
            self.assertEqual(data["total_users"], 3)
            self.assertEqual(data["users"][0]["Missed Reservations"], 7)
            other_id = store.enqueue("active_users", title="Active", payload={})
            other_page = client.get(f"/jobs/{other_id}").get_data(as_text=True)
            self.assertIn("JSON.stringify", other_page)
            self.assertNotIn("missed_reservations.js", other_page)

    def config(self, root, token="read-only-test"):
        return replace(
            AppConfig.from_env(base_dir=root),
            data_dir=root / "data",
            access_password="",
            read_only_api_token=token,
            job_secret_key="",
        )

    def test_all_reports_queue_month_without_upload_and_worker_exports_source(self):
        with tempfile.TemporaryDirectory() as folder:
            config = self.config(Path(folder))
            app = create_app(config)
            client = app.test_client()
            store = app.extensions["job_store"]
            for slug, kind in REPORTS.items():
                with self.subTest(report=slug):
                    page = client.get(f"/tools/{slug}").get_data(as_text=True)
                    self.assertIn('type="month"', page)
                    self.assertIn('value="api" checked', page)
                    self.assertNotIn('name="api_token"', page)
                    with client.session_transaction() as session:
                        csrf = session["csrf_token"]
                    response = client.post(
                        f"/tools/reports/{slug}",
                        data={
                            "csrf_token": csrf,
                            "source": "api",
                            "billing_month": "2026-04",
                        },
                    )
                    self.assertEqual(response.status_code, 302)
                    job = store.claim_next("test-worker")
                    self.assertEqual(job.kind, kind)
                    self.assertEqual(job.payload["stored_uploads"], [])
                    self.assertEqual(job.secrets, {})
                    with (
                        patch(
                            "nemo_app.jobs.handlers.fetch_billing_dataframe",
                            return_value=usage_frame(),
                        ) as fetch,
                        patch.object(JobContext, "metadata", return_value=_Metadata()),
                    ):
                        run_job(config, store, job)
                    self.assertEqual(fetch.call_args.args[1], "2026-04")
                    self.assertTrue(fetch.call_args.args[0].read_only)
                    self.assertEqual(
                        fetch.call_args.kwargs["include_no_charge_projects"],
                        kind == "missed_reservations",
                    )
                    result = store.get(job.id)
                    self.assertEqual(result.status, "completed")
                    self.assertIn("output/billing_source.csv", result.result["files"])
                    self.assertEqual(
                        len(result.result["files"]), 1 if kind == "missed_reservations" else 2
                    )
                    for path in result.result["files"]:
                        self.assertTrue((config.jobs_dir / job.id / path).is_file())

    def test_invalid_month_source_and_missing_token_do_not_enqueue(self):
        with tempfile.TemporaryDirectory() as folder:
            app = create_app(self.config(Path(folder), token=""))
            client = app.test_client()
            client.get("/tools/usage-caps")
            with client.session_transaction() as session:
                csrf = session["csrf_token"]
            for slug in REPORTS:
                for values in (
                    {"source": "api", "billing_month": "2026-13", "api_token": "test"},
                    {"source": "api", "billing_month": "2026-04"},
                    {"source": "other", "billing_month": "2026-04", "api_token": "test"},
                    {"source": "csv", "api_token": "test"},
                ):
                    response = client.post(
                        f"/tools/reports/{slug}",
                        data={
                            "csrf_token": csrf,
                            **values,
                        },
                    )
                    self.assertEqual(response.status_code, 400)
            self.assertIsNone(app.extensions["job_store"].claim_next("test"))
            for slug in REPORTS:
                self.assertEqual(
                    client.post(
                        f"/tools/reports/{slug}",
                        data={
                            "source": "api",
                            "billing_month": "2026-04",
                            "api_token": "test",
                        },
                    ).status_code,
                    400,
                )

    def test_csv_reports_remain_available_including_token_free_local_reports(self):
        with tempfile.TemporaryDirectory() as folder:
            app = create_app(self.config(Path(folder), token=""))
            client = app.test_client()
            client.get("/tools/usage-caps")
            with client.session_transaction() as session:
                csrf = session["csrf_token"]
            for slug, kind in REPORTS.items():
                token = (
                    {"api_token": "upload-test"}
                    if kind in {"detailed_financials", "user_pi"}
                    else {}
                )
                data = {
                    "csrf_token": csrf,
                    "source": "csv",
                    **token,
                    "usage_csvs" if kind == "usage_caps" else "csv_file": (
                        BytesIO(usage_frame().to_csv(index=False).encode()),
                        "April.csv",
                    ),
                }
                if kind == "usage_caps":
                    data["usage_csvs"] = [data["usage_csvs"], (BytesIO(b"x"), "May.csv")]
                self.assertEqual(client.post(f"/tools/reports/{slug}", data=data).status_code, 302)
                job = app.extensions["job_store"].claim_next("test")
                self.assertEqual(job.payload["source"], "csv")
                self.assertEqual(job.secrets, token)
                self.assertEqual(
                    len(job.payload["stored_uploads"]), 2 if kind == "usage_caps" else 1
                )

    def test_empty_missed_month_produces_empty_report_and_audit(self):
        with tempfile.TemporaryDirectory() as folder:
            config = self.config(Path(folder))
            app = create_app(config)
            store = app.extensions["job_store"]
            job_id = store.enqueue(
                "missed_reservations",
                title="Missed",
                payload={
                    "source": "api",
                    "billing_month": "2026-04",
                },
            )
            with patch(
                "nemo_app.jobs.handlers.fetch_billing_dataframe",
                return_value=usage_frame().iloc[:0],
            ):
                run_job(config, store, store.claim_next("test"))
            job = store.get(job_id)
            self.assertEqual(job.result["data"]["total_users"], 0)
            self.assertEqual(job.result["data"]["users"], [])

    def test_usage_cap_staff_audit_excludes_waived_charges(self):
        from nemo_app.reports.usage_caps import build_usage_cap_report

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            frame = usage_frame()
            frame["Waived"] = False
            extra = frame.iloc[[0]].copy()
            extra["Type"] = "staff_charge"
            extra["Application identifier"] = "Staff"
            extra["Cost"] = 999999
            extra["Waived"] = True
            source = root / "usage.csv"
            pd.concat([frame, extra], ignore_index=True).to_csv(source, index=False)
            build_usage_cap_report([(source, "Test")], root / "caps.xlsx")
            book = openpyxl.load_workbook(root / "caps.xlsx", data_only=True)
            try:
                self.assertNotIn(
                    999999, [cell.value for row in book["Staff Charge Detail"] for cell in row]
                )
            finally:
                book.close()

    def test_missed_api_query_can_include_no_charge_projects(self):
        from unittest.mock import Mock

        client = Mock()
        client.fetch_all.return_value = []
        frame = fetch_billing_dataframe(
            client,
            "2024-02",
            timezone=self.config(Path(tempfile.gettempdir())).timezone,
            allow_empty=True,
            include_no_charge_projects=True,
        )
        self.assertTrue(frame.empty)
        self.assertIn("end=2024-02-29", client.fetch_all.call_args.args[0])
        self.assertIn("include_no_charge_projects=true", client.fetch_all.call_args.args[0])
