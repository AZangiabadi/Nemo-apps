from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from dataclasses import asdict, replace
from io import BytesIO
from pathlib import Path

from nemo_app.billing.invoice_model import InvoiceDocument
from nemo_app.billing.prepare import prepare_usage_dataframe
from nemo_app.config import AppConfig
from nemo_app.invoices.customization import (
    InvoiceCustomization,
    parse_lab_sections,
)
from nemo_app.invoices.pdf_renderer import _invoice_details_markup, render_invoice_pdf
from nemo_app.invoices.service import make_invoice_number
from nemo_app.web.app import create_app
from tests.fixtures import usage_frame


class InvoiceCustomizationTests(unittest.TestCase):
    def document(
        self,
        application: str = "Local",
        *,
        access_fees: dict[str, float] | None = None,
    ) -> InvoiceDocument:
        frame = prepare_usage_dataframe(usage_frame(), apply_caps=False).iloc[:3].copy()
        frame["Application identifier"] = application
        return InvoiceDocument.from_frame(
            frame,
            pi_key="ada.pi@example.edu",
            pi_name="PI, Ada",
            pi_email="ada.pi@example.edu",
            period="2026-04",
            invoice_number="TEST-1",
            generated_at=dt.datetime(2026, 5, 1, 9, 30, tzinfo=dt.UTC),
            access_fee_by_application=access_fees,
        )

    def test_settings_round_trip_and_lab_renaming(self) -> None:
        settings = InvoiceCustomization(
            main_title="Example Nanofabrication Center",
            subtitle="Monthly User Invoice",
            lab_sections=parse_lab_sections("Cleanroom = Nanofab\nSMCL = Materials Lab"),
            include_access_fee=False,
            access_fees={"Local": 0.0},
            billing_fields=("pi", "invoice_number"),
            font_family="Times",
            font_size=9,
            color_theme="forest",
            detail_columns=("date", "description", "cost"),
            invoice_number_format="calendar",
            payment_instructions="",
        )

        restored = InvoiceCustomization.from_mapping(asdict(settings))

        self.assertEqual(restored, settings)
        self.assertEqual(restored.lab_sections[0], ("Cleanroom", "Nanofab"))

    def test_access_fee_can_be_changed_or_removed(self) -> None:
        custom_fee = self.document("Industry", access_fees={"Industry": 275.0})
        no_fee = self.document("Industry", access_fees={})

        self.assertEqual(custom_fee.access_fee, 275.0)
        self.assertEqual(custom_fee.invoice_total, custom_fee.usage_total + 275.0)
        self.assertEqual(no_fee.access_fee, 0.0)
        self.assertIsNone(no_fee.access_fee_project)

    def test_billing_fields_and_number_formats_are_selectable(self) -> None:
        settings = InvoiceCustomization(billing_fields=("pi", "invoice_number"))
        markup = _invoice_details_markup(self.document(), settings)
        generated_at = dt.datetime(2026, 7, 29, 3, 48, 13, tzinfo=dt.UTC)

        self.assertIn("PI: PI, Ada", markup)
        self.assertIn("Invoice #: TEST-1", markup)
        self.assertNotIn("Email:", markup)
        self.assertEqual(
            make_invoice_number("2026-04", 7, generated_at, "timestamped"),
            "CNI-2604-290348-007",
        )
        self.assertEqual(make_invoice_number("2026-04", 7, generated_at, "monthly"), "CNI-2604-007")
        self.assertEqual(
            make_invoice_number("2026-04", 7, generated_at, "calendar"), "INV-202604-0007"
        )
        self.assertEqual(
            make_invoice_number("2026-04", 7, generated_at, "yearly"), "INV-2026-00007"
        )

    def test_custom_pdf_renders_with_selected_style_and_columns(self) -> None:
        settings = InvoiceCustomization(
            main_title="Example Nanofabrication Center",
            subtitle="Monthly User Invoice",
            lab_sections=(("Cleanroom", "Nanofab"),),
            include_access_fee=False,
            access_fees={},
            billing_fields=("pi", "billing_month"),
            font_family="Courier",
            font_size=9,
            color_theme="ocean",
            detail_columns=("date", "description", "cost"),
            invoice_number_format="monthly",
            payment_instructions="",
        )
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "custom.pdf"
            render_invoice_pdf(self.document(access_fees={}), output, customization=settings)
            self.assertGreater(output.stat().st_size, 1000)

    def test_web_form_queues_uploaded_logo_and_custom_settings(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = replace(
                AppConfig.from_env(base_dir=root),
                data_dir=root / "data",
                asset_dir=root,
                read_only_api_token="server-token",
                access_password="",
                job_secret_key="",
            )
            app = create_app(config)
            app.config.update(TESTING=True)
            client = app.test_client()
            page = client.get("/tools/invoices")
            self.assertIn(b'name="main_title"', page.data)
            self.assertIn(b'name="detail_columns"', page.data)
            with client.session_transaction() as session:
                csrf = session["csrf_token"]

            response = client.post(
                "/tools/invoices",
                data={
                    "csrf_token": csrf,
                    "customization_present": "1",
                    "csv_file": (BytesIO(b"Type,User\n"), "usage.csv"),
                    "logo_file": (BytesIO(b"test-logo"), "logo.png"),
                    "generate_pdf": "on",
                    "main_title": "Example Lab",
                    "subtitle": "Usage Invoice",
                    "lab_sections": "Cleanroom = Nanofab",
                    "access_fee_local": "0",
                    "access_fee_cdg": "0",
                    "access_fee_external_academic": "100",
                    "access_fee_industry": "200",
                    "billing_fields": ["pi", "invoice_number"],
                    "font_family": "Times",
                    "font_size": "9",
                    "color_theme": "grayscale",
                    "detail_columns": ["date", "cost"],
                    "invoice_number_format": "monthly",
                    "payment_instructions": "Pay online.",
                },
                content_type="multipart/form-data",
            )

            self.assertEqual(response.status_code, 302)
            job = app.extensions["job_store"].claim_next("test-worker")
            self.assertIsNotNone(job)
            assert job is not None
            self.assertEqual(job.payload["logo_input"], "input_2.png")
            self.assertEqual(job.payload["customization"]["main_title"], "Example Lab")
            self.assertEqual(job.payload["customization"]["detail_columns"], ["date", "cost"])


if __name__ == "__main__":
    unittest.main()
