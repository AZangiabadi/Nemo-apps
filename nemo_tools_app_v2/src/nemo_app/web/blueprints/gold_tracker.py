from __future__ import annotations

import datetime as dt
import re
from html.parser import HTMLParser
from urllib.parse import urlsplit

import requests
from flask import Blueprint, Response, jsonify, render_template, request

from ..common import config

gold_tracker_blueprint = Blueprint("gold_tracker", __name__)
JOB_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


class _MirrorHTML(HTMLParser):
    """Preserve the existing dashboard while injecting same-origin API routing."""

    def __init__(self, bridge: str):
        super().__init__(convert_charrefs=False)
        self.parts: list[str] = []
        self.bridge = bridge
        self.injected = False

    def handle_starttag(self, tag, attrs):
        self.parts.append(self.get_starttag_text())
        if tag == "head" and not self.injected:
            self.parts.append(self.bridge)
            self.injected = True

    def handle_startendtag(self, tag, attrs):
        self.parts.append(self.get_starttag_text())

    def handle_endtag(self, tag):
        self.parts.append(f"</{tag}>")

    def handle_data(self, data):
        self.parts.append(data)

    def handle_entityref(self, name):
        self.parts.append(f"&{name};")

    def handle_charref(self, name):
        self.parts.append(f"&#{name};")

    def handle_comment(self, data):
        self.parts.append(f"<!--{data}-->")

    def handle_decl(self, decl):
        self.parts.append(f"<!{decl}>")


def _base_url() -> str | None:
    target = config().angstrom_log_app_url.strip().rstrip("/")
    try:
        parsed = urlsplit(target)
    except ValueError:
        return None
    return target if parsed.scheme in {"http", "https"} and parsed.netloc else None


def _no_store(response):
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@gold_tracker_blueprint.get("/tools/gold-thickness-tracker")
def page():
    base = _base_url()
    if base is None:
        return render_template(
            "error.html",
            title="Gold Thickness Tracker",
            message="The gold thickness tracker is not configured.",
        ), 503
    try:
        upstream = requests.get(base + "/", timeout=15, allow_redirects=False)
        if upstream.status_code != 200 or "text/html" not in upstream.headers.get(
            "Content-Type", ""
        ):
            raise ValueError("Unexpected tracker page response")
        parser = _MirrorHTML(render_template("_gold_tracker_bridge.html"))
        parser.feed(upstream.text)
        parser.close()
        if not parser.injected:
            raise ValueError("Tracker page is missing its head element")
    except (requests.RequestException, ValueError):
        return render_template(
            "error.html",
            title="Gold Thickness Tracker",
            message="The gold tracker is currently unavailable. Please try again.",
        ), 502
    response = _no_store(Response("".join(parser.parts), mimetype="text/html"))
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
        "connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'self'; "
        "form-action 'self'; frame-ancestors 'self'"
    )
    return response


def _relay_api(path: str, payload: dict | None = None):
    base = _base_url()
    if base is None:
        return jsonify(error="The gold thickness tracker is not configured."), 503
    try:
        # Only fixed tracker endpoints are relayed; browser cookies/credentials stay on NEMO Tools.
        upstream = requests.request(
            "POST" if payload is not None else "GET",
            base + path,
            json=payload,
            headers={"Accept": "application/json"},
            timeout=15,
            allow_redirects=False,
        )
        if upstream.status_code not in {200, 201, 202, 400, 404}:
            raise ValueError("Unexpected tracker API response")
        data = upstream.json()
        if not isinstance(data, dict):
            raise ValueError("Unexpected tracker API data")
        if payload is not None and upstream.status_code < 300:
            job_id = data.get("job_id")
            if not isinstance(job_id, str) or not JOB_ID.fullmatch(job_id):
                raise ValueError("Invalid tracker scan identifier")
    except (requests.RequestException, ValueError):
        return jsonify(error="The gold tracker is currently unavailable. Please try again."), 502
    response = jsonify(data)
    response.status_code = upstream.status_code
    return _no_store(response)


@gold_tracker_blueprint.post("/tools/gold-thickness-tracker/api/scans")
def start_scan():
    payload = request.get_json(silent=True)
    try:
        if not isinstance(payload, dict):
            raise ValueError
        start = dt.date.fromisoformat(payload.get("start", ""))
        end = dt.date.fromisoformat(payload.get("end", ""))
        if start > end:
            raise ValueError
    except (ValueError, TypeError):
        return jsonify(error="Choose valid start and end dates, with start on or before end."), 400
    return _relay_api("/api/scans", {"start": start.isoformat(), "end": end.isoformat()})


@gold_tracker_blueprint.get("/tools/gold-thickness-tracker/api/scans/<job_id>")
def scan_status(job_id: str):
    if not JOB_ID.fullmatch(job_id):
        return jsonify(error="Scan not found."), 404
    return _relay_api("/api/scans/" + job_id)
