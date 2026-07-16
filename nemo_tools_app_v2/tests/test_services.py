from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path
from zoneinfo import ZoneInfo

from nemo_app.billing.adjustments import apply_adjustment_requests
from nemo_app.dashboard.service import DashboardService
from nemo_app.invoices.service import InvoiceOptions, generate_invoices
from nemo_app.nemo.client import NemoClient
from nemo_app.replacements.service import replace_account_project
from nemo_app.reports.detailed_financials import build_detailed_financial_report
from nemo_app.reports.user_pi import build_user_pi_report
from tests.fixtures import PROJECT_MAP, usage_frame


class _Metadata:
    def projects(self, **_options):
        return PROJECT_MAP

    def tools(self, **_options):
        return {}

    def adjustments(self, **_options):
        return []

    def consumable_labs(self, **_options):
        return {"Sample holder": "Cleanroom"}

    def users(self, **_options):
        return [
            {
                "username": "al1",
                "email": "ada@example.edu",
                "first_name": "Ada",
                "last_name": "Lovelace",
            }
        ]


class _DashboardClient:
    identity_hash = "dashboard-test"

    def fetch_all(self, endpoint: str):
        if endpoint.startswith("usage_events/"):
            return [{"user": 1, "tool": 2, "start": "2026-07-16T09:00:00"}]
        return []

    def fetch_by_ids(self, endpoint: str, _identifiers):
        if endpoint == "users/":
            return {1: {"id": 1, "username": "ada"}}
        if endpoint == "tools/":
            return {2: {"id": 2, "name": "SEM"}}
        return {}


class ServiceTests(unittest.TestCase):
    def test_approved_adjustment_changes_time_cost_and_project(self) -> None:
        source = usage_frame().iloc[[0]].copy()
        adjusted = apply_adjustment_requests(
            source,
            [
                {
                    "status": 1,
                    "item_tool": 10,
                    "original_start": "2026-04-10T09:00:00",
                    "original_end": "2026-04-10T19:00:00",
                    "new_start": "2026-04-10T09:00:00",
                    "new_end": "2026-04-10T11:00:00",
                    "new_project": 3,
                }
            ],
            tools_by_id={10: "Oxford PECVD"},
            projects_by_name={
                **PROJECT_MAP,
                "Project C PI3": {
                    "id": 3,
                    "name": "Project C PI3",
                    "application_identifier": "CDG",
                },
            },
        )
        self.assertEqual(adjusted.iloc[0]["Quantity"], 120.0)
        self.assertEqual(adjusted.iloc[0]["Cost"], 20.0)
        self.assertEqual(adjusted.iloc[0]["Project"], "Project C PI3")
        self.assertEqual(adjusted.iloc[0]["Application identifier"], "CDG")

    def test_invoice_service_creates_a_complete_archive(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "usage.csv"
            usage_frame().to_csv(source, index=False)
            result = generate_invoices(
                source,
                root / "output",
                metadata=_Metadata(),  # type: ignore[arg-type]
                options=InvoiceOptions(make_zip=True),
            )
            self.assertEqual(result.invoice_count, 1)
            self.assertEqual(len(result.files), 1)
            with zipfile.ZipFile(result.files[0]) as archive:
                names = archive.namelist()
            self.assertTrue(any(name.endswith(".xlsx") for name in names))
            self.assertTrue(any(name.endswith(".pdf") for name in names))
            self.assertTrue(any("PI-Contacts" in name for name in names))

    def test_remaining_workbook_reports(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "usage.csv"
            usage_frame().to_csv(source, index=False)
            detailed = build_detailed_financial_report(
                source,
                root / "detailed.xlsx",
                metadata=_Metadata(),  # type: ignore[arg-type]
                use_cache=False,
            )
            user_pi = build_user_pi_report(
                source,
                root / "user-pi.xlsx",
                metadata=_Metadata(),  # type: ignore[arg-type]
                use_cache=False,
            )
            self.assertGreater(detailed.row_count, 0)
            self.assertGreater(user_pi.user_count, 0)
            self.assertTrue(detailed.output_path.is_file())
            self.assertTrue(user_pi.output_path.is_file())

    def test_replacement_dry_run_and_dashboard(self) -> None:
        client = NemoClient("token", dry_run=True)

        def fetch_all(endpoint: str):
            if endpoint == "accounts/":
                return [{"id": 1, "name": "OLD", "type": 1, "active": True}]
            if endpoint == "projects/":
                return [
                    {
                        "id": 2,
                        "name": "OLD",
                        "account": 1,
                        "users": [4],
                        "active": True,
                    }
                ]
            raise AssertionError(endpoint)

        client.fetch_all = fetch_all  # type: ignore[method-assign]
        result = replace_account_project(
            client=client,
            old_value="OLD",
            target_value="NEW",
            mode="new",
            deactivate_old=True,
        )
        self.assertTrue(result.dry_run)
        self.assertGreaterEqual(len(client.actions), 4)
        dashboard = DashboardService(timezone=ZoneInfo("America/New_York")).report(
            _DashboardClient()  # type: ignore[arg-type]
        )
        self.assertEqual(dashboard.current_usage[0][0:2], ("ada", "SEM"))


if __name__ == "__main__":
    unittest.main()
