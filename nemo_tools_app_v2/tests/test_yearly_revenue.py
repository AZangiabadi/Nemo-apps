from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from nemo_app.config import AppConfig
from nemo_app.reports.yearly_revenue import yearly_revenue
from nemo_app.web.app import create_app


def saved(source="NEMO", total=110):
    return {
        "source": source,
        "total": total,
        "labs": [
            {"name": "Cleanroom", "total": 50},
            {"name": "SMCL", "total": 30},
            {"name": "Electron Microscopy Lab", "total": 20},
            {"name": "Consumable", "total": 5},
        ],
    }


class YearlyRevenueTests(unittest.TestCase):
    def test_stacks_three_labs_and_reconciles_other_charges_across_sources(self):
        months = {"2026-01": saved("Badger"), "2026-04": saved()}
        data = yearly_revenue(2026, today=dt.date(2026, 10, 29), monthly_report=months.get)
        self.assertEqual(len(data["months"]), 12)
        self.assertEqual(data["lab_total"], 200)
        self.assertEqual(data["total"], 220)
        self.assertEqual(data["other"], 20)
        self.assertEqual(data["saved_months"], 2)
        self.assertEqual(data["missing_months"], 8)
        self.assertEqual(data["months"][10]["status"], "future")
        self.assertEqual(data["months"][1]["labs"], None)
        self.assertEqual(data["months"][0]["source"], "Badger")
        self.assertEqual(set(data["months"][0]["labs"]), set(data["labs"]))

    def test_zero_missing_old_reports_and_estimates_are_distinct(self):
        zero = saved(total=0)
        zero["labs"] = [
            {"name": lab, "total": 0} for lab in ("Cleanroom", "SMCL", "Electron Microscopy Lab")
        ]
        estimated = saved("Badger")
        estimated["cap_adjusted_rows"] = 2
        months = {"2025-01": zero, "2025-02": {"total": 123}, "2025-03": estimated}
        data = yearly_revenue(2025, today=dt.date(2026, 10, 29), monthly_report=months.get)
        self.assertEqual(data["months"][0]["lab_total"], 0)
        self.assertEqual(data["months"][1]["status"], "no_lab_breakdown")
        self.assertEqual(data["months"][3]["total"], None)
        self.assertEqual(data["unclassified"], 123)
        self.assertEqual(data["estimated_months"], 1)
        self.assertEqual(data["lab_total"] + data["other"] + data["unclassified"], data["total"])

    def test_route_is_saved_only_and_validates_year_and_caps(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = replace(
                AppConfig.from_env(base_dir=root),
                data_dir=root / "data",
                asset_dir=root,
                access_password="",
                read_only_api_token="",
                job_secret_key="",
            )
            app = create_app(config)
            client = app.test_client()
            self.assertEqual(client.get("/tools/revenue/yearly").status_code, 200)
            with (
                patch("nemo_app.web.blueprints.revenue.BadgerHistory.report", return_value=None),
                patch(
                    "nemo_app.jobs.store.JobStore.revenue_snapshot", return_value=saved()
                ) as snapshot,
                patch("nemo_app.jobs.store.JobStore.enqueue") as enqueue,
            ):
                response = client.get("/tools/revenue/yearly/data?year=2025&hourly_caps=1")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json["lab_months"], 12)
                self.assertEqual(response.headers["Cache-Control"], "no-store")
                self.assertTrue(all(call.kwargs["hourly_caps"] for call in snapshot.call_args_list))
                enqueue.assert_not_called()
            for year in ("bad", "0", "9999"):
                self.assertEqual(
                    client.get(f"/tools/revenue/yearly/data?year={year}").status_code, 400
                )
            self.assertEqual(
                client.get("/tools/revenue/yearly/data?year=2025&hourly_caps=bad").status_code, 400
            )
            protected = create_app(replace(config, access_password="secret")).test_client()
            self.assertEqual(protected.get("/tools/revenue/yearly/data?year=2025").status_code, 302)
