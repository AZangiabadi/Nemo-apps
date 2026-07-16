from __future__ import annotations

from dataclasses import asdict

from flask import Blueprint, flash, render_template, request

from nemo_app.invoices.service import InvoiceOptions

from ..common import checkbox, enqueue_upload_job, job_redirect

invoice_blueprint = Blueprint("invoices", __name__)


@invoice_blueprint.get("/tools/invoices")
def invoice_form():
    return render_template("invoice_form.html")


@invoice_blueprint.post("/tools/invoices")
def invoice_submit():
    upload = request.files.get("csv_file")
    token = request.form.get("api_token", "").strip()
    if not upload or not upload.filename or not token:
        flash("Choose a usage CSV and enter your NEMO API token.", "error")
        return render_template("invoice_form.html"), 400
    options = InvoiceOptions(
        generate_excel=checkbox("generate_excel"),
        generate_pdf=checkbox("generate_pdf"),
        make_zip=checkbox("make_zip"),
        use_cache=not checkbox("bypass_cache"),
        apply_hourly_caps=checkbox("apply_hourly_caps"),
    )
    if not options.generate_excel and not options.generate_pdf:
        flash("Select Excel, PDF, or both.", "error")
        return render_template("invoice_form.html"), 400
    job_id = enqueue_upload_job(
        "invoice",
        title="Invoice generation",
        upload_specs=[(upload, {".csv"})],
        payload={"input": "input_1.csv", "options": asdict(options)},
        secrets={"api_token": token},
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
