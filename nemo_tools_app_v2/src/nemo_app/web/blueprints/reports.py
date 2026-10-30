from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path

from flask import Blueprint, flash, render_template, request

from nemo_app.nemo.billing import billing_month_dates

from ..common import (
    checkbox,
    config,
    enqueue_upload_job,
    job_redirect,
    job_store,
    read_only_api_secrets,
)

reports_blueprint = Blueprint("reports", __name__)


def _report_form(template: str, status: int = 200):
    previous_month = (
        dt.datetime.now(config().timezone).date().replace(day=1) - dt.timedelta(days=1)
    ).strftime("%Y-%m")
    return render_template(template, default_billing_month=previous_month), status


@reports_blueprint.get("/tools/reports")
def reports_page():
    return render_template("reports.html")


@reports_blueprint.get("/tools/detailed-financials")
def detailed_page():
    return _report_form("detailed_financials_form.html")


@reports_blueprint.get("/tools/usage-caps")
def usage_caps_page():
    return _report_form("usage_caps_form.html")


@reports_blueprint.get("/tools/user-pi")
def user_pi_page():
    return _report_form("user_pi_form.html")


@reports_blueprint.get("/tools/missed-reservations")
def missed_page():
    return _report_form("missed_reservations_form.html")


@reports_blueprint.get("/tools/active-users")
def active_users_page():
    return render_template("active_users_form.html")


def _submit_report(
    kind: str, title: str, template: str, *, multiple: bool = False, metadata: bool = False
):
    source = request.form.get("source", "csv")
    month = request.form.get("billing_month", "").strip()
    uploads = [
        upload
        for upload in request.files.getlist("usage_csvs" if multiple else "csv_file")
        if upload and upload.filename
    ]
    try:
        if source not in {"api", "csv"}:
            raise ValueError("Choose API or CSV as the report source.")
        if source == "api":
            billing_month_dates(month)
        elif not uploads:
            raise ValueError("Choose at least one usage CSV." if multiple else "Choose a CSV.")
        api_secrets = read_only_api_secrets() if source == "api" or metadata else {}
        if api_secrets is None:
            raise ValueError("Enter your NEMO API token.")
    except ValueError as exc:
        flash(str(exc), "error")
        return _report_form(template, 400)
    uploads = uploads if source == "csv" else []
    job_id = enqueue_upload_job(
        kind,
        title=title,
        upload_specs=[(upload, {".csv"}) for upload in uploads],
        payload={
            "source": source,
            "billing_month": month if source == "api" else None,
            "input": "input_1.csv",
            "inputs": [
                {
                    "stored_name": f"input_{index}.csv",
                    "label": Path(upload.filename or f"Source {index}").stem,
                }
                for index, upload in enumerate(uploads, 1)
            ],
            "use_cache": not checkbox("bypass_cache"),
        },
        secrets=api_secrets,
    )
    return job_redirect(job_id)


@reports_blueprint.post("/tools/reports/detailed-financials")
def detailed_submit():
    return _submit_report(
        "detailed_financials",
        "Detailed financial report",
        "detailed_financials_form.html",
        metadata=True,
    )


@reports_blueprint.post("/tools/reports/user-pi")
def user_pi_submit():
    return _submit_report(
        "user_pi",
        "User PI report",
        "user_pi_form.html",
        metadata=True,
    )


@reports_blueprint.post("/tools/reports/usage-caps")
def usage_caps_submit():
    return _submit_report(
        "usage_caps",
        "Usage cap analysis",
        "usage_caps_form.html",
        multiple=True,
    )


@reports_blueprint.post("/tools/reports/missed-reservations")
def missed_submit():
    return _submit_report(
        "missed_reservations",
        "Missed reservation report",
        "missed_reservations_form.html",
    )


@reports_blueprint.post("/tools/reports/active-users")
def active_users_submit():
    api_secrets = read_only_api_secrets()
    if api_secrets is None:
        flash("Enter your NEMO API token.", "error")
        return render_template("active_users_form.html"), 400
    job_id = str(uuid.uuid4())
    job_store().enqueue(
        "active_users",
        title="Active lab users report",
        payload={},
        secrets=api_secrets,
        job_id=job_id,
    )
    return job_redirect(job_id)
