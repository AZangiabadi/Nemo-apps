from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from nemo_app.config import AppConfig
from nemo_app.reports.badger import BadgerHistory, adjusted_cost, cap_rates, summarize_badger
from nemo_app.web.app import create_app


def charge(minutes, cost, *, raw=None, item="Oxford PECVD", day="2026-03-10"):
    return {
        "item": item,
        "bdate": day + " 12:00:00",
        "member": "user@example.edu",
        "project": "Project A",
        "account": "A",
        "pi": "pi@example.edu",
        "project_type": "cni-local",
        "type": "eq_activity",
        "amount": minutes,
        "rawcost": cost if raw is None else raw,
        "cost": cost,
        "source_file": "history.xlsx",
        "source_row": 2,
    }


class BadgerTests(unittest.TestCase):
    def test_preserves_existing_caps_and_adjusts_only_identifiable_linear_charges(self):
        reference = [charge(m, m * 0.5) for m in (60, 90, 120)]
        capped = charge(1000, 60, raw=120)
        uncapped = charge(1000, 250, raw=500)
        rates = cap_rates([*reference, capped, uncapped])
        self.assertEqual(adjusted_cost(capped, rates), (6000, False))
        self.assertEqual(adjusted_cost(uncapped, rates), (6000, True))
        unknown = charge(1000, 250, item="Unknown tool")
        self.assertEqual(adjusted_cost(unknown, rates), (25000, False))

    def test_reconciles_months_credits_mapping_and_ignored_rows(self):
        rows = [charge(60, 30), charge(60, -1), charge(60, -20), charge(60, 99, day="2026-02-01")]
        result = summarize_badger(rows, "2026-03", cap_rates(rows))
        self.assertEqual(result["total"], 10)
        self.assertEqual(result["ignored_rows"], 1)
        self.assertEqual(result["fees"], 0)
        self.assertEqual(result["gold"], 0)
        for key in ("daily", "applications", "labs", "pis"):
            self.assertEqual(sum(r["total"] for r in result[key]), 10)
        self.assertEqual(next(r["total"] for r in result["applications"] if r["name"] == "CDG"), 10)

    def test_import_is_idempotent_and_historical_routes_never_call_nemo(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "history.xlsx").touch()
            headers = (
                "item",
                "bdate",
                "member",
                "project",
                "account",
                "pi",
                "project_type",
                "type",
                "amount",
                "rawcost",
                "cost",
            )
            data = [
                charge(60, 30),
                charge(2000, 135, item="AJA Orion-8 Dielectrics Sputtering System"),
            ]
            values = [headers, *[tuple(row[k] for k in headers) for row in data]]
            book = Mock()
            sheet = Mock()
            sheet.values = values
            book.__iter__ = Mock(return_value=iter([sheet]))
            history = BadgerHistory(root / "data")
            for _ in range(2):
                book.__iter__ = Mock(return_value=iter([sheet]))
                with patch("nemo_app.reports.badger.load_workbook", return_value=book):
                    self.assertEqual(history.import_directory(root)["rows"], 2)
            config = replace(
                AppConfig.from_env(base_dir=root),
                data_dir=root / "data",
                asset_dir=root,
                access_password="",
                read_only_api_token="",
                job_secret_key="",
            )
            client = create_app(config).test_client()
            client.get("/tools/revenue")
            with client.session_transaction() as session:
                csrf = session["csrf_token"]
            with patch("nemo_app.jobs.store.JobStore.enqueue") as enqueue:
                for action in ("show", "analyze"):
                    response = client.post(
                        "/tools/revenue",
                        data={"csrf_token": csrf, "billing_month": "2026-03", "action": action},
                    )
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json["data"]["source"], "Badger")
                    self.assertEqual(response.json["data"]["total"], 165)
                enqueue.assert_not_called()
