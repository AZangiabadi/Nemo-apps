from __future__ import annotations

import json

from flask import Blueprint, current_app, jsonify, render_template

from nemo_app.nemo.client import NemoClient

dashboard_blueprint = Blueprint("dashboard", __name__)


def _report():
    config = current_app.extensions["nemo_config"]
    if not config.jumbotron_api_token:
        raise RuntimeError("Set NEMO_JUMBOTRON_API_TOKEN to use the jumbotron.")
    client = NemoClient(config.jumbotron_api_token, base_url=config.api_base_url)
    return current_app.extensions["dashboard_service"].report(client)


@dashboard_blueprint.get("/jumbotron")
def page():
    config = current_app.extensions["nemo_config"]
    try:
        report = _report()
        error = ""
    except Exception as exc:
        report = None
        error = str(exc)
    signature = json.dumps(report.as_dict(), sort_keys=True) if report else ""
    return render_template(
        "jumbotron.html",
        report=report,
        error=error,
        initial_signature=signature,
        refresh_ms=config.jumbotron_refresh_seconds * 1000,
        scroll_step_px=config.jumbotron_scroll_step_px,
        scroll_interval_ms=config.jumbotron_scroll_interval_ms,
    )


@dashboard_blueprint.get("/jumbotron/data")
def data():
    try:
        report = _report()
        html = render_template("_jumbotron_content.html", report=report)
        payload = report.as_dict()
        return jsonify(
            {"html": html, "signature": json.dumps(payload, sort_keys=True), "report": payload}
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503
