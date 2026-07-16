from __future__ import annotations

import uuid

from flask import Blueprint, flash, render_template, request

from ..common import checkbox, enqueue_upload_job, job_redirect, job_store

administration_blueprint = Blueprint("administration", __name__)


@administration_blueprint.get("/tools/user-import")
def import_form():
    return render_template("user_import_form.html")


@administration_blueprint.post("/tools/user-import")
def import_submit():
    upload = request.files.get("spreadsheet")
    token = request.form.get("api_token", "").strip()
    if not upload or not upload.filename or not token:
        flash("Choose a spreadsheet and enter your NEMO API token.", "error")
        return render_template("user_import_form.html"), 400
    suffix = ".csv" if upload.filename.lower().endswith(".csv") else ".xlsx"
    job_id = enqueue_upload_job(
        "user_import",
        title="User/account/project import",
        upload_specs=[(upload, {".csv", ".xlsx"})],
        payload={
            "input": f"input_1{suffix}",
            "dry_run": checkbox("dry_run"),
            "use_cache": not checkbox("bypass_cache"),
        },
        secrets={"api_token": token},
    )
    return job_redirect(job_id)


@administration_blueprint.get("/tools/replacement")
def replacement_form():
    return render_template("replacement_form.html")


@administration_blueprint.post("/tools/replacement")
def replacement_submit():
    token = request.form.get("api_token", "").strip()
    old_value = request.form.get("old_value", "").strip()
    target_value = request.form.get("target_value", "").strip()
    if not token or not old_value or not target_value:
        flash("Token, old value, and target value are required.", "error")
        return render_template("replacement_form.html"), 400
    job_id = str(uuid.uuid4())
    job_store().enqueue(
        "replacement",
        title="Account/project replacement",
        payload={
            "old_value": old_value,
            "target_value": target_value,
            "mode": request.form.get("mode", "new"),
            "dry_run": checkbox("dry_run"),
            "deactivate_old": checkbox("deactivate_old"),
        },
        secrets={"api_token": token},
        job_id=job_id,
    )
    return job_redirect(job_id)
