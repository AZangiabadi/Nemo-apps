from __future__ import annotations

import datetime as dt

from flask import Blueprint, jsonify, render_template, request, url_for

from nemo_app.nemo.billing import billing_month_dates

from ..common import checkbox, config, job_store, read_only_api_secrets

revenue_blueprint = Blueprint("revenue", __name__)


@revenue_blueprint.get("/tools/revenue")
def page():
    today = dt.datetime.now(config().timezone).date()
    default = (today.replace(day=1) - dt.timedelta(days=1)).strftime("%Y-%m")
    return render_template("revenue.html", default_month=default, max_month=today.strftime("%Y-%m"))


@revenue_blueprint.post("/tools/revenue")
def analyze():
    period = request.form.get("billing_month", "")
    try:
        start, _ = billing_month_dates(period)
        if start > dt.datetime.now(config().timezone).date():
            raise ValueError("Choose the current month or an earlier month.")
        action = request.form.get("action", "analyze")
        if action not in {"show", "analyze"}:
            raise ValueError("Choose Show analysis or Re-analyze.")
        if action == "show":
            saved = job_store().revenue_snapshot(period, hourly_caps=checkbox("apply_hourly_caps"))
            if saved is None:
                response = jsonify(
                    error=(
                        "No saved analysis for this month and cap setting. "
                        "Select Re-analyze to create one."
                    )
                )
                response.status_code = 404
            else:
                response = jsonify(data=saved, saved=True)
            response.headers["Cache-Control"] = "no-store"
            return response
        secrets = read_only_api_secrets()
        if secrets is None:
            raise ValueError("Enter your NEMO API token.")
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    identifier = job_store().enqueue(
        "revenue",
        title=f"Revenue analysis {period}",
        payload={"billing_month": period, "apply_hourly_caps": checkbox("apply_hourly_caps")},
        secrets=secrets,
    )
    return jsonify(status_url=url_for("revenue.status", job_id=identifier)), 202


@revenue_blueprint.get("/tools/revenue/<job_id>/status")
def status(job_id: str):
    job = job_store().get(job_id)
    if not job or job.kind != "revenue":
        return jsonify(error="Revenue analysis not found."), 404
    response = jsonify(
        status=job.status,
        finished=job.finished,
        error=job.error,
        message=job.summary,
        data=job.result.get("data", {}) if job.status == "completed" else {},
    )
    response.headers["Cache-Control"] = "no-store"
    return response
