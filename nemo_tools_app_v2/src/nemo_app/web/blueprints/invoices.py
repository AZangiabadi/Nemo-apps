from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from flask import Blueprint, flash, render_template, request

from nemo_app.invoices.customization import (
    BILLING_FIELD_OPTIONS,
    COLOR_THEME_OPTIONS,
    DEFAULT_INVOICE_CUSTOMIZATION,
    DETAIL_COLUMN_OPTIONS,
    FONT_OPTIONS,
    FONT_SIZE_OPTIONS,
    INVOICE_NUMBER_OPTIONS,
    InvoiceCustomization,
    format_lab_sections,
    parse_lab_sections,
)
from nemo_app.invoices.service import InvoiceOptions

from ..common import checkbox, enqueue_upload_job, job_redirect, read_only_api_secrets

invoice_blueprint = Blueprint("invoices", __name__)

ACCESS_FEE_FIELDS = {
    "Local": "access_fee_local",
    "CDG": "access_fee_cdg",
    "External Academic": "access_fee_external_academic",
    "Industry": "access_fee_industry",
}


def _invoice_form_context() -> dict[str, object]:
    return {
        "defaults": DEFAULT_INVOICE_CUSTOMIZATION,
        "default_lab_sections": format_lab_sections(DEFAULT_INVOICE_CUSTOMIZATION.lab_sections),
        "billing_field_options": BILLING_FIELD_OPTIONS,
        "detail_column_options": DETAIL_COLUMN_OPTIONS,
        "font_options": FONT_OPTIONS,
        "font_size_options": FONT_SIZE_OPTIONS,
        "color_theme_options": COLOR_THEME_OPTIONS,
        "invoice_number_options": INVOICE_NUMBER_OPTIONS,
        "access_fee_fields": ACCESS_FEE_FIELDS,
    }


def _render_invoice_form(*, status: int = 200):
    return render_template("invoice_form.html", **_invoice_form_context()), status


def _customization_from_request() -> InvoiceCustomization:
    if request.form.get("customization_present") != "1":
        return DEFAULT_INVOICE_CUSTOMIZATION
    access_fees: dict[str, float] = {}
    for application, field_name in ACCESS_FEE_FIELDS.items():
        raw_amount = request.form.get(field_name, "0").strip()
        try:
            access_fees[application] = float(raw_amount or 0)
        except ValueError as exc:
            raise ValueError(f"Enter a valid access fee for {application}.") from exc
    return InvoiceCustomization(
        main_title=request.form.get("main_title", "").strip(),
        subtitle=request.form.get("subtitle", "").strip(),
        lab_sections=parse_lab_sections(request.form.get("lab_sections", "")),
        include_access_fee=checkbox("include_access_fee"),
        access_fees=access_fees,
        billing_fields=tuple(request.form.getlist("billing_fields")),
        font_family=request.form.get("font_family", ""),
        font_size=int(request.form.get("font_size", "0")),
        color_theme=request.form.get("color_theme", ""),
        detail_columns=tuple(request.form.getlist("detail_columns")),
        invoice_number_format=request.form.get("invoice_number_format", ""),
        payment_instructions=request.form.get("payment_instructions", "").strip(),
    )


@invoice_blueprint.get("/tools/invoices")
def invoice_form():
    return _render_invoice_form()


@invoice_blueprint.post("/tools/invoices")
def invoice_submit():
    upload = request.files.get("csv_file")
    api_secrets = read_only_api_secrets()
    if not upload or not upload.filename:
        flash("Choose a usage CSV.", "error")
        return _render_invoice_form(status=400)
    if api_secrets is None:
        flash("Enter your NEMO API token.", "error")
        return _render_invoice_form(status=400)
    options = InvoiceOptions(
        generate_excel=checkbox("generate_excel"),
        generate_pdf=checkbox("generate_pdf"),
        make_zip=checkbox("make_zip"),
        use_cache=not checkbox("bypass_cache"),
        apply_hourly_caps=checkbox("apply_hourly_caps"),
    )
    if not options.generate_excel and not options.generate_pdf:
        flash("Select Excel, PDF, or both.", "error")
        return _render_invoice_form(status=400)
    try:
        customization = _customization_from_request()
    except (TypeError, ValueError) as exc:
        flash(str(exc), "error")
        return _render_invoice_form(status=400)
    upload_specs = [(upload, {".csv"})]
    logo_input = None
    logo_upload = request.files.get("logo_file")
    if logo_upload and logo_upload.filename:
        logo_suffix = Path(logo_upload.filename).suffix.lower()
        if logo_suffix not in {".png", ".jpg", ".jpeg"}:
            flash("Upload a PNG or JPEG logo.", "error")
            return _render_invoice_form(status=400)
        upload_specs.append((logo_upload, {".png", ".jpg", ".jpeg"}))
        logo_input = f"input_2{logo_suffix}"
    job_id = enqueue_upload_job(
        "invoice",
        title="Invoice generation",
        upload_specs=upload_specs,
        payload={
            "input": "input_1.csv",
            "logo_input": logo_input,
            "options": asdict(options),
            "customization": asdict(customization),
        },
        secrets=api_secrets,
    )
    return job_redirect(job_id)


@invoice_blueprint.get("/tools/excel-to-pdf")
def excel_pdf_form():
    return render_template("excel_pdf_form.html")


@invoice_blueprint.post("/tools/excel-to-pdf")
def excel_pdf_submit():
    upload = request.files.get("invoice_excel")
    if not upload or not upload.filename:
        flash("Choose an invoice workbook.", "error")
        return render_template("excel_pdf_form.html"), 400
    suffix = ".xlsm" if upload.filename.lower().endswith(".xlsm") else ".xlsx"
    job_id = enqueue_upload_job(
        "excel_pdf",
        title="Excel invoice to PDF",
        upload_specs=[(upload, {".xlsx", ".xlsm"})],
        payload={"input": f"input_1{suffix}"},
    )
    return job_redirect(job_id)
