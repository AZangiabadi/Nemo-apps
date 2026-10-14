from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from nemo_app.config import AppConfig
from nemo_app.invoices.service import InvoiceOptions, generate_invoices
from nemo_app.nemo.billing import (
    billing_month_dates,
    billing_records_dataframe,
    fetch_billing_dataframe,
)
from nemo_app.web.app import create_app
from tests.fixtures import PROJECT_MAP, usage_frame
from tests.test_services import _Metadata


class ApiBillingTests(unittest.TestCase):
    def test_api_fetch_uses_month_end_and_excludes_no_charge(self):
        client = Mock()
        client.fetch_all.return_value = [self.record()]
        frame = fetch_billing_dataframe(client, "2024-02", timezone=ZoneInfo("America/New_York"))
        self.assertEqual(len(frame), 1)
        endpoint = client.fetch_all.call_args.args[0]
        self.assertIn("end=2024-02-29", endpoint)
        self.assertIn("include_no_charge_projects=false", endpoint)
        self.assertEqual(frame.iloc[0]["Cost"], 23.08)
        self.assertEqual(frame.iloc[0]["Start time"], "09/30/2026 @ 08:27 PM")
        self.assertEqual(frame.iloc[0]["Quantity"], 81.43)

    def record(self):
        return {
            "item_type": "tool_usage",
            "user": "ss7755",
            "user_fullname": "Sample User",
            "name": "Raman",
            "project": "Sample project",
            "application": "Local",
            "start": "2026-10-01T00:27:36+00:00",
            "end": "2026-10-01T01:49:02+00:00",
            "quantity": "81.43",
            "rate": "USD 17.00/hr (8.50 minimum)",
            "amount": "23.08",
        }

    def test_bad_period_and_missing_charge_fields_fail(self):
        for period in ("", "2026-13", "2026-9", "2026-09-01"):
            with self.assertRaises(ValueError):
                billing_month_dates(period)
        record = self.record()
        del record["amount"]
        with self.assertRaises(ValueError):
            billing_records_dataframe([record], timezone=dt.UTC)

    def test_api_form_queues_without_csv_and_validates_month(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = replace(
                AppConfig.from_env(base_dir=root),
                data_dir=root / "data",
                read_only_api_token="test-read-only",
                access_password="",
                job_secret_key="",
            )
            app = create_app(config)
            app.config.update(TESTING=True)
            client = app.test_client()
            client.get("/tools/invoices")
            with client.session_transaction() as session:
                csrf = session["csrf_token"]
            data = {
                "csrf_token": csrf,
                "source": "api",
                "billing_month": "2026-09",
                "generate_excel": "on",
            }
            self.assertEqual(client.post("/tools/invoices", data=data).status_code, 302)
            job = app.extensions["job_store"].claim_next("test")
            self.assertEqual(job.payload["billing_month"], "2026-09")
            self.assertEqual(job.payload["source"], "api")
            self.assertEqual(job.payload["stored_uploads"], [])
            data["billing_month"] = "2026-13"
            self.assertEqual(client.post("/tools/invoices", data=data).status_code, 400)

    def test_dataframe_and_csv_invoices_match_and_no_charge_is_excluded(self):
        class Metadata(_Metadata):
            def projects(self, **options):
                projects = {key: dict(value) for key, value in PROJECT_MAP.items()}
                projects["No charge project"] = {"no_charge": True}
                return projects

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            frame = usage_frame()
            free_row = frame.iloc[[0]].copy()
            free_row["Project"] = "No charge project"
            import pandas as pd

            frame = pd.concat([frame, free_row], ignore_index=True)
            csv = root / "source.csv"
            frame.to_csv(csv, index=False)
            results = []
            for source, destination in ((frame, root / "api"), (csv, root / "csv")):
                results.append(
                    generate_invoices(
                        source,
                        destination,
                        metadata=Metadata(),
                        options=InvoiceOptions(generate_pdf=False, make_zip=False),
                    )
                )
            self.assertEqual(results[0].row_count, results[1].row_count)
            self.assertEqual(results[0].invoice_count, results[1].invoice_count)
            self.assertLess(results[0].row_count, len(frame))
