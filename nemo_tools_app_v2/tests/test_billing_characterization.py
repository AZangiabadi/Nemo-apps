from __future__ import annotations

import unittest

import pandas as pd

from nemo_app.billing.caps import apply_max_session_charge_caps, apply_project_charge_caps
from nemo_app.billing.prepare import filter_invoice_quantity_rows, prepare_usage_dataframe
from tests.fixtures import usage_frame


class BillingCharacterizationTests(unittest.TestCase):
    def test_cross_month_usage_is_billed_in_completion_month(self) -> None:
        source = usage_frame().iloc[[0]].copy()
        source.loc[source.index[0], "Start time"] = "04/30/2026 @ 11:00 PM"
        source.loc[source.index[0], "End time"] = "05/01/2026 @ 01:00 AM"

        prepared = prepare_usage_dataframe(source, apply_caps=False)

        self.assertEqual(prepared.iloc[0]["Period"], "2026-05")

    def test_missing_or_invalid_end_time_falls_back_to_start_month(self) -> None:
        source = pd.concat([usage_frame().iloc[[0]]] * 2, ignore_index=True)
        source.loc[0, "End time"] = ""
        source.loc[1, "End time"] = "03/31/2026 @ 11:00 PM"

        prepared = prepare_usage_dataframe(source, apply_caps=False)

        self.assertEqual(prepared["Period"].tolist(), ["2026-04", "2026-04"])

    def test_hourly_cap_preserves_original_values(self) -> None:
        prepared = prepare_usage_dataframe(usage_frame(), apply_caps=False)
        capped = apply_max_session_charge_caps(prepared)
        pecvd = capped.loc[capped["Item_norm"].eq("Oxford PECVD")].iloc[0]
        self.assertEqual(pecvd["Original Quantity"], 600.0)
        self.assertEqual(pecvd["Quantity"], 240.0)
        self.assertEqual(pecvd["Original Cost"], 100.0)
        self.assertEqual(pecvd["Cost"], 40.0)

    def test_project_cap_allocates_exact_cents(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "Period": "2026-04",
                    "Billable User Key": "al1",
                    "Project": "P1",
                    "Application identifier": "Local",
                    "IsToolUsageCharge": True,
                    "IsStaffCharge": False,
                    "Cost": 1000.0,
                },
                {
                    "Period": "2026-04",
                    "Billable User Key": "al1",
                    "Project": "P1",
                    "Application identifier": "Local",
                    "IsToolUsageCharge": True,
                    "IsStaffCharge": False,
                    "Cost": 1000.0,
                },
            ]
        )
        capped = apply_project_charge_caps(frame)
        self.assertEqual(capped["Cost"].tolist(), [750.0, 750.0])
        self.assertAlmostEqual(capped["Project Cap Reduction"].sum(), 500.0)

    def test_staff_time_inherits_matching_tool_lab(self) -> None:
        prepared = prepare_usage_dataframe(usage_frame(), apply_caps=False)
        staff = prepared.loc[prepared["IsStaffCharge"]].iloc[0]
        self.assertEqual(staff["Lab"], "Cleanroom")

    def test_invoice_filter_removes_missed_and_one_minute_usage(self) -> None:
        prepared = prepare_usage_dataframe(usage_frame(), apply_caps=False)
        filtered = filter_invoice_quantity_rows(
            prepared.loc[~prepared["IsMissedReservation"]].copy()
        )
        self.assertEqual(len(filtered), 3)
        self.assertFalse(filtered["IsMissedReservation"].any())
        self.assertFalse(((filtered["Type"] == "tool_usage") & (filtered["Quantity"] <= 1)).any())


if __name__ == "__main__":
    unittest.main()
