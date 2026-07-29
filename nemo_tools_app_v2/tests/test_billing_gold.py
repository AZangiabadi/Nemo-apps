from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from nemo_app.billing.gold import add_gold_deposition_charges, gold_thickness_nm
from nemo_app.billing.prepare import prepare_usage_dataframe
from nemo_app.invoices.service import InvoiceOptions, generate_invoices


def _run_data(
    *entries: dict[str, str],
    group_name: str = "angstrom_metals_depositions",
) -> str:
    return json.dumps(
        {
            group_name: {
                "type": "group",
                "user_input": {str(index): entry for index, entry in enumerate(entries)},
            }
        }
    )


class GoldDepositionTests(unittest.TestCase):
    def test_parser_sums_only_positive_pure_gold_entries(self) -> None:
        payload = _run_data(
            {"deposited_material": "Au", "deposited_thickness_nm": "12.25"},
            {"deposited_material": "Ti", "deposited_thickness_nm": "50"},
            {"deposited_material": "Au/Pd", "deposited_thickness_nm": "100"},
            {"deposited_material": "Au", "deposited_thickness_nm": "2.75"},
            {"deposited_material": "Au", "deposited_thickness_nm": "-5"},
        )
        self.assertEqual(gold_thickness_nm(payload), Decimal("15.00"))
        high_vacuum_payload = _run_data(
            {"deposited_material": "Au", "deposited_thickness_nm": "85"},
            {"deposited_material": "Pt", "deposited_thickness_nm": "20"},
            group_name="angstrom_high_vacuum_depositions",
        )
        self.assertEqual(gold_thickness_nm(high_vacuum_payload), Decimal("85"))
        self.assertEqual(gold_thickness_nm("not json"), Decimal("0"))

    def test_gold_charge_uses_activating_project_and_does_not_count_as_tool_usage(self) -> None:
        source = pd.DataFrame(
            [
                {
                    "Type": "tool_usage",
                    "User": "Ada Lovelace",
                    "Username": "al1",
                    "Item": "Angstrom Metals Deposition System",
                    "Project": "Project 450",
                    "Application identifier": "Local",
                    "Start time": "07/27/2026 @ 07:54 AM",
                    "End time": "07/27/2026 @ 12:46 PM",
                    "Rate": "$20/hr",
                    "Cost": 100.0,
                    "Quantity": 292.0,
                }
            ]
        )
        prepared = prepare_usage_dataframe(source, apply_caps=False)
        result = add_gold_deposition_charges(
            prepared,
            source=source,
            usage_events=[
                {
                    "id": 6746,
                    "start": "2026-07-27T07:54:47.640514-04:00",
                    "end": "2026-07-27T12:46:48.439906-04:00",
                    "user": 624,
                    "project": 450,
                    "tool": 27,
                    "run_data": _run_data(
                        {"deposited_material": "Ti", "deposited_thickness_nm": "50"},
                        {"deposited_material": "Au", "deposited_thickness_nm": "20.25"},
                    ),
                }
            ],
            tools_by_id={27: "Angstrom Metals Deposition System"},
            projects_by_name={
                "Project 450": {
                    "id": 450,
                    "application_identifier": "Local",
                }
            },
        )

        self.assertEqual(len(result), 2)
        charge = result.loc[result["Type"].eq("gold_deposition")].iloc[0]
        self.assertEqual(charge["Project"], "Project 450")
        self.assertEqual(charge["Application identifier"], "Local")
        self.assertEqual(charge["Quantity"], 20.25)
        self.assertEqual(charge["Rate"], "$1.10/nm")
        self.assertEqual(charge["Cost"], 22.28)
        self.assertEqual(charge["Lab"], "Cleanroom")
        self.assertTrue(charge["IsConsumable"])
        self.assertFalse(charge["IsToolUsageCharge"])
        self.assertEqual(charge["Usage Event ID"], 6746)

    def test_unmatched_api_event_is_not_billed(self) -> None:
        source = pd.DataFrame(
            [
                {
                    "Type": "tool_usage",
                    "User": "Ada Lovelace",
                    "Item": "Angstrom High Vacuum",
                    "Project": "Uploaded Project",
                    "Application identifier": "Local",
                    "Start time": "07/27/2026 @ 07:54 AM",
                    "End time": "07/27/2026 @ 08:54 AM",
                    "Rate": "$20/hr",
                    "Cost": 20.0,
                    "Quantity": 60.0,
                }
            ]
        )
        prepared = prepare_usage_dataframe(source, apply_caps=False)
        result = add_gold_deposition_charges(
            prepared,
            source=source,
            usage_events=[
                {
                    "id": 1,
                    "start": "2026-07-27T07:54:00-04:00",
                    "project": 999,
                    "tool": 27,
                    "run_data": _run_data(
                        {"deposited_material": "Au", "deposited_thickness_nm": "10"}
                    ),
                }
            ],
            tools_by_id={27: "Angstrom High Vacuum"},
            projects_by_name={
                "Different Project": {
                    "id": 999,
                    "application_identifier": "Local",
                }
            },
        )
        self.assertEqual(len(result), 1)

    def test_staff_charge_invoice_includes_high_vacuum_gold_line(self) -> None:
        source_frame = pd.DataFrame(
            [
                {
                    "Type": "staff_charge",
                    "User": "George Magaud",
                    "Username": "gm3288",
                    "Item": "Angstrom High Vacuum",
                    "Project": "Project 183",
                    "Application identifier": "Industry",
                    "Start time": "07/10/2026 @ 11:27 AM",
                    "End time": "07/10/2026 @ 12:52 PM",
                    "Rate": "120.00/hr (60.00 minimum)",
                    "Cost": 170.64,
                    "Quantity": 85.31666666666668,
                }
            ]
        )

        class Metadata:
            def projects(self, **_options):
                return {
                    "Project 183": {
                        "id": 183,
                        "contact_name": "George Magaud",
                        "contact_email": "gmagaud@example.com",
                        "application_identifier": "Industry",
                    }
                }

            def tools(self, **_options):
                return {28: "Angstrom High Vacuum"}

            def adjustments(self, **_options):
                return []

            def consumable_labs(self, **_options):
                return {}

            def usage_events_for_tools(self, tool_ids, *, start, end):
                self.query = (tool_ids, start, end)
                return [
                    {
                        "id": 5836,
                        "start": "2026-07-10T11:27:39.152365-04:00",
                        "end": "2026-07-10T12:52:58.077677-04:00",
                        "user": 237,
                        "operator": 4,
                        "project": 183,
                        "tool": 28,
                        "run_data": _run_data(
                            {"deposited_material": "Au", "deposited_thickness_nm": "85"},
                            {"deposited_material": "Pt", "deposited_thickness_nm": "20"},
                            group_name="angstrom_high_vacuum_depositions",
                        ),
                    }
                ]

        metadata = Metadata()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            csv_path = root / "usage.csv"
            source_frame.to_csv(csv_path, index=False)
            result = generate_invoices(
                csv_path,
                root / "output",
                metadata=metadata,  # type: ignore[arg-type]
                options=InvoiceOptions(generate_pdf=False, make_zip=False),
            )
            invoice_path = next(
                path
                for path in result.files
                if path.suffix == ".xlsx" and "PI-Contacts" not in path.name
            )
            workbook = load_workbook(invoice_path, data_only=True)
            sheet = workbook["Invoice"]
            values = [
                cell.value for row in sheet.iter_rows() for cell in row if cell.value is not None
            ]
            summary_title_row = next(
                row
                for row in range(1, sheet.max_row + 1)
                if sheet.cell(row, 1).value == "Project fees summary"
            )
            summary_header_row = summary_title_row + 1
            summary_columns = {
                sheet.cell(summary_header_row, column).value: column
                for column in range(1, sheet.max_column + 1)
            }
            project_row = next(
                row
                for row in range(summary_header_row + 1, sheet.max_row + 1)
                if sheet.cell(row, summary_columns["Project"]).value == "Project 183"
            )
            project_summary = {
                heading: sheet.cell(project_row, column).value
                for heading, column in summary_columns.items()
            }
            workbook.close()

        self.assertEqual(metadata.query[0], {28})
        self.assertIn("Gold deposition (Au) - Angstrom High Vacuum", values)
        self.assertIn("$1.10/nm", values)
        self.assertIn(85.0, values)
        self.assertIn(93.5, values)
        self.assertEqual(project_summary["Cleanroom"], 170.64)
        self.assertEqual(project_summary["Consumable"], 93.5)
        self.assertEqual(project_summary["Project Total"], 414.14)


if __name__ == "__main__":
    unittest.main()
