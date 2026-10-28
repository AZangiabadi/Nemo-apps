from __future__ import annotations

import datetime as dt

from flask import Blueprint, abort, flash, render_template, request

from nemo_app.mailchimp import MailchimpClient

from ..common import checkbox, config, job_redirect, job_store, read_only_api_secrets

newsletter_blueprint = Blueprint("newsletter", __name__)


@newsletter_blueprint.context_processor
def newsletter_settings():
    return {"mailchimp_key_configured": bool(config().mailchimp_api_key)}


@newsletter_blueprint.app_template_filter("newsletter_datetime")
def newsletter_datetime(value):
    if not value:
        return "Not recorded"
    try:
        timestamp = dt.datetime.fromisoformat(str(value))
    except ValueError:
        return "Not recorded"
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=config().timezone)
    return timestamp.astimezone(config().timezone).strftime("%b %d, %Y %I:%M %p %Z")


@newsletter_blueprint.get("/tools/newsletter")
def form():
    return render_template("newsletter_form.html")


@newsletter_blueprint.post("/tools/newsletter")
def compare():
    key = config().mailchimp_api_key or request.form.get("mailchimp_key", "").strip()
    audience = request.form.get("audience_id", "").strip()
    secrets = read_only_api_secrets()
    try:
        MailchimpClient(key, audience)
        if secrets is None:
            raise ValueError("Enter your NEMO API token.")
    except ValueError as exc:
        flash(str(exc), "error")
        return render_template("newsletter_form.html"), 400
    identifier = job_store().enqueue(
        "newsletter_compare",
        title="Newsletter comparison",
        payload={
            "audience_id": audience,
        },
        secrets={**secrets, "mailchimp_key": key},
    )
    return job_redirect(identifier)


def _comparison(job_id: str):
    job = job_store().get(job_id)
    if not job or job.kind != "newsletter_compare" or job.status != "completed":
        abort(404)
    return job


@newsletter_blueprint.get("/tools/newsletter/review/<job_id>")
def review(job_id: str):
    job = _comparison(job_id)
    return render_template("newsletter_review.html", job=job, data=job.result["data"])


@newsletter_blueprint.post("/tools/newsletter/review/<job_id>")
def submit(job_id: str):
    job = _comparison(job_id)
    data = job.result["data"]
    selected = set(request.form.getlist("emails"))
    candidates = {row["email"]: row for row in data["contacts"] if row["status"] == "missing"}
    mode = request.form.get("subscription_status", "pending")
    consent_note = request.form.get("consent_note", "").strip()
    valid_mode = mode in {"pending", "subscribed"} and not (
        mode == "subscribed" and data.get("double_optin")
    )
    if (
        not selected
        or not selected.issubset(candidates)
        or not valid_mode
        or not checkbox("consent_confirmed")
        or not 5 <= len(consent_note) <= 1000
    ):
        flash(
            "Select missing contacts, confirm their newsletter permission, and record its source.",
            "error",
        )
        return render_template("newsletter_review.html", job=job, data=data), 400
    identifier = job_store().enqueue(
        "newsletter_add",
        title="Approved newsletter additions",
        payload={
            "comparison_id": job.id,
            "audience_id": job.payload["audience_id"],
            "contacts": [candidates[email] for email in sorted(selected)],
            "subscription_status": mode,
            "consent_note": consent_note,
            "consent_confirmed": True,
            "approved_at": dt.datetime.now(dt.UTC).isoformat(),
        },
        secrets=job_store().get_secrets(job.id),
    )
    return job_redirect(identifier)
