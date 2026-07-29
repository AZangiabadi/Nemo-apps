from __future__ import annotations

import uuid
from pathlib import Path

from flask import Blueprint, flash, render_template, request

from ..common import (
    checkbox,
    enqueue_upload_job,
    job_redirect,
    job_store,
    read_only_api_secrets,
)

reports_blueprint = Blueprint("reports", __name__)


@reports_blueprint.get("/tools/reports")
def reports_page():
    return render_template("reports.html")


@reports_blueprint.get("/tools/detailed-financials")
def detailed_page():
    return render_template("detailed_financials_form.html")


@reports_blueprint.get("/tools/usage-caps")
def usage_caps_page():
    return render_template("usage_caps_form.html")


@reports_blueprint.get("/tools/user-pi")
def user_pi_page():
    return render_template("user_pi_form.html")


@reports_blueprint.get("/tools/missed-reservations")
def missed_page():
    return render_template("missed_reservations_form.html")


@reports_blueprint.get("/tools/active-users")
def active_users_page():
    return render_template("active_users_form.html")


def _token_upload(kind: str, title: str):
    upload = request.files.get("csv_file")
    api_secrets = read_only_api_secrets()
    if not upload or not upload.filename:
        flash("Choose a CSV.", "error")
        return None
    if api_secrets is None:
        flash("Enter your NEMO API token.", "error")
        return None
    job_id = enqueue_upload_job(
        kind,
        title=title,
        upload_specs=[(upload, {".csv"})],
        payload={"input": "input_1.csv", "use_cache": not checkbox("bypass_cache")},
        secrets=api_secrets,
    )
    return job_redirect(job_id)


@reports_blueprint.post("/tools/reports/detailed-financials")
def detailed_submit():
    return _token_upload("detailed_financials", "Detailed financial report") or (
        render_template("detailed_financials_form.html"),
        400,
    )


@reports_blueprint.post("/tools/reports/user-pi")
def user_pi_submit():
    return _token_upload("user_pi", "User PI report") or (
        render_template("user_pi_form.html"),
        400,
    )


@reports_blueprint.post("/tools/reports/usage-caps")
def usage_caps_submit():
    uploads = [
        upload for upload in request.files.getlist("usage_csvs") if upload and upload.filename
    ]
    if not uploads:
        flash("Choose at least one usage CSV.", "error")
        return render_template("usage_caps_form.html"), 400
    inputs = [
        {
            "stored_name": f"input_{index}.csv",
            "label": Path(upload.filename or f"Source {index}").stem,
        }
        for index, upload in enumerate(uploads, 1)
    ]
    job_id = enqueue_upload_job(
        "usage_caps",
        title="Usage cap analysis",
        upload_specs=[(upload, {".csv"}) for upload in uploads],
        payload={"inputs": inputs},
    )
    return job_redirect(job_id)


@reports_blueprint.post("/tools/reports/missed-reservations")
def missed_submit():
    upload = request.files.get("csv_file")
    if not upload or not upload.filename:
        flash("Choose a usage CSV.", "error")
        return render_template("missed_reservations_form.html"), 400
    job_id = enqueue_upload_job(
        "missed_reservations",
        title="Missed reservation report",
        upload_specs=[(upload, {".csv"})],
        payload={"input": "input_1.csv"},
    )
    return job_redirect(job_id)


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
