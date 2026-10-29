from __future__ import annotations

import datetime as dt
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

from nemo_app.config import AppConfig
from nemo_app.invoices.service import InvoiceOptions, calculate_invoices, generate_invoices
from nemo_app.jobs.handlers import run_job
from nemo_app.jobs.store import create_job_store
from nemo_app.reports.revenue import summarize_revenue
from nemo_app.web.app import create_app
from tests.fixtures import PROJECT_MAP, usage_frame


class RevenueMetadata:
    def projects(self, **_options):
        return {**PROJECT_MAP, "No charge": {"id": 99, "no_charge": True}}

    def tools(self, **_options):
        return {26: "Oxford PECVD", 28: "Angstrom High Vacuum"}

    def tool_labs(self, **_options):
        return {"Oxford PECVD": "Cleanroom", "Angstrom High Vacuum": "Cleanroom"}

    def adjustments(self, **_options):
        return []

    def consumable_labs(self, **_options):
        return {}

    def usage_events_for_tools(self, *_args, **_options):
        return [
            {
                "id": 100,
                "tool": 28,
                "project": 1,
                "start": "2026-04-10T09:00:00-04:00",
                "end": "2026-04-10T10:00:00-04:00",
                "run_data": {
                    "angstrom_high_vacuum_depositions": {
                        "user_input": [
                            {
                                "deposited_material": "Au",
                                "deposited_thickness_nm": "25",
                            }
                        ]
                    }
                },
            }
        ]


def revenue_source():
    source = usage_frame()
    source.loc[0, "Cost"] = 3000.0
    gold = source.iloc[[0]].copy()
    gold["Item"] = "Angstrom High Vacuum"
    gold["Cost"], gold["Quantity"] = 100.0, 60.0
    gold["End time"] = "04/10/2026 @ 10:00 AM"
    waived = source.iloc[:2].copy()
    waived["Waived"] = True
    no_charge = source.iloc[[0]].copy()
    no_charge["Project"] = "No charge"
    return pd.concat([source, gold, waived, no_charge], ignore_index=True)


class RevenueTests(unittest.TestCase):
    def test_reconciles_caps_gold_fees_waivers_and_exported_invoices(self):
        for hourly_caps, expected in ((True, 1452.5), (False, 1652.5)):
            with self.subTest(hourly_caps=hourly_caps):
                options = InvoiceOptions(apply_hourly_caps=hourly_caps, generate_pdf=False)
                calculation = calculate_invoices(
                    revenue_source(),
                    metadata=RevenueMetadata(),
                    options=options,
                    timezone=ZoneInfo("America/New_York"),
                )
                report = summarize_revenue(calculation.documents, "2026-04")
                self.assertEqual(report["total"], expected)
                self.assertEqual(report["gold"], 27.5)
                self.assertEqual(report["fees"], 50.0)
                self.assertEqual(report["pi_count"], 1)
                self.assertEqual(report["pis"][0]["total"], expected)
                self.assertEqual(sum(row["total"] for row in report["daily"]), expected)
                self.assertEqual(sum(row["total"] for row in report["applications"]), expected)
                self.assertEqual(report["daily"][-1]["fees"], 50.0)
                with tempfile.TemporaryDirectory() as folder:
                    with patch("nemo_app.invoices.service.render_invoice_workbook") as renderer:
                        renderer.side_effect = lambda document, path: path
                        generate_invoices(
                            revenue_source(),
                            Path(folder),
                            metadata=RevenueMetadata(),
                            options=replace(options, make_zip=False),
                        )
                    self.assertEqual(
                        sum(call.args[0].invoice_total for call in renderer.call_args_list),
                        expected,
                    )

    def test_empty_and_other_month_reports_are_zero(self):
        empty = summarize_revenue((), "2024-02")
        self.assertEqual(len(empty["daily"]), 29)
        self.assertEqual(empty["total"], 0)
        self.assertEqual(empty["pis"], [])
        docs = calculate_invoices(
            revenue_source(),
            metadata=RevenueMetadata(),
            options=InvoiceOptions(),
        ).documents
        self.assertEqual(summarize_revenue(docs, "2026-05")["total"], 0)

    def test_multiple_project_types_and_pi_ranking(self):
        source = usage_frame().iloc[[0, 0]].copy().reset_index(drop=True)
        source.loc[1, "Project"] = "Project B PI2"
        source.loc[1, "Application identifier"] = "Industry"
        source.loc[1, "Cost"] = 500.0
        docs = calculate_invoices(
            source, metadata=RevenueMetadata(), options=InvoiceOptions()
        ).documents
        report = summarize_revenue(docs, "2026-04")
        self.assertEqual(report["pi_count"], 2)
        self.assertEqual([pi["total"] for pi in report["pis"]], [350.0, 90.0])
        self.assertEqual(
            {item["name"]: item["total"] for item in report["applications"]}["Industry"], 350
        )

    def test_revenue_job_route_worker_and_security(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = replace(
                AppConfig.from_env(base_dir=root),
                data_dir=root / "data",
                asset_dir=root,
                access_password="",
                read_only_api_token="test-token",
                job_secret_key="",
            )
            app = create_app(config)
            client = app.test_client()
            self.assertEqual(client.get("/tools/revenue").status_code, 200)
            self.assertEqual(client.post("/tools/revenue").status_code, 400)
            with client.session_transaction() as session:
                csrf = session["csrf_token"]
            invalid = client.post(
                "/tools/revenue", data={"csrf_token": csrf, "billing_month": "bad"}
            )
            self.assertEqual(invalid.status_code, 400)
            response = client.post(
                "/tools/revenue",
                data={
                    "csrf_token": csrf,
                    "billing_month": "2026-04",
                    "apply_hourly_caps": "on",
                },
            )
            self.assertEqual(response.status_code, 202)
            status_url = response.json["status_url"]
            store = app.extensions["job_store"]
            job = store.claim_next("revenue-test")
            with (
                patch(
                    "nemo_app.jobs.handlers.fetch_billing_dataframe", return_value=revenue_source()
                ),
                patch("nemo_app.jobs.handlers.JobContext.metadata", return_value=RevenueMetadata()),
            ):
                run_job(config, store, job)
            completed = client.get(status_url)
            self.assertEqual(completed.json["status"], "completed")
            self.assertEqual(completed.json["data"]["total"], 1452.5)
            self.assertEqual(completed.headers["Cache-Control"], "no-store")
            self.assertNotIn("test-token", completed.get_data(as_text=True))
            saved = client.post(
                "/tools/revenue",
                data={
                    "csrf_token": csrf,
                    "billing_month": "2026-04",
                    "apply_hourly_caps": "on",
                    "action": "show",
                },
            )
            self.assertEqual(saved.status_code, 200)
            self.assertEqual(saved.json["data"], completed.json["data"])
            self.assertTrue(saved.json["saved"])
            missing = client.post(
                "/tools/revenue",
                data={
                    "csrf_token": csrf,
                    "billing_month": "2026-04",
                    "action": "show",
                },
            )
            self.assertEqual(missing.status_code, 404)
            failed_id = store.enqueue(
                "revenue",
                title="Failed rerun",
                payload={"billing_month": "2026-04"},
            )
            store.claim_next("test-failure")
            store.fail(failed_id, error="API unavailable")
            self.assertEqual(
                store.revenue_snapshot("2026-04", hourly_caps=True), saved.json["data"]
            )
            # Simulate upgrading an installation with completed jobs but no snapshots.
            with sqlite3.connect(config.database_path) as connection:
                connection.execute("DELETE FROM revenue_snapshots")
            migrated = create_job_store(config)
            self.assertEqual(
                migrated.revenue_snapshot("2026-04", hourly_caps=True), saved.json["data"]
            )
            store.expire_before(dt.datetime.now(dt.UTC) + dt.timedelta(days=365))
            self.assertEqual(store.get(job.id).status, "expired")
            self.assertEqual(
                store.revenue_snapshot("2026-04", hourly_caps=True), saved.json["data"]
            )
            tokenless = create_app(replace(config, read_only_api_token="")).test_client()
            tokenless.get("/tools/revenue")
            with tokenless.session_transaction() as session:
                tokenless_csrf = session["csrf_token"]
            with patch("nemo_app.jobs.store.JobStore.enqueue") as enqueue:
                loaded = tokenless.post(
                    "/tools/revenue",
                    data={
                        "csrf_token": tokenless_csrf,
                        "billing_month": "2026-04",
                        "apply_hourly_caps": "on",
                        "action": "show",
                    },
                )
                self.assertEqual(loaded.status_code, 200)
                enqueue.assert_not_called()
            protected = create_app(replace(config, access_password="secret"))
            self.assertEqual(protected.test_client().get("/tools/revenue").status_code, 302)

    def test_older_concurrent_run_cannot_replace_newer_saved_analysis(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = replace(
                AppConfig.from_env(base_dir=root), data_dir=root / "data", job_secret_key=""
            )
            store = create_job_store(config)
            for identifier in ("a-first", "b-second"):
                store.enqueue("revenue", title="Analysis", payload={}, job_id=identifier)
                store.claim_next(identifier)
            for identifier, total in (("b-second", 200), ("a-first", 100)):
                store.complete(
                    identifier,
                    summary="Complete",
                    result={
                        "data": {
                            "period": "2026-04",
                            "apply_hourly_caps": False,
                            "generated_at": dt.datetime.now(dt.UTC).isoformat(),
                            "total": total,
                        }
                    },
                )
            self.assertEqual(store.revenue_snapshot("2026-04", hourly_caps=False)["total"], 200)
