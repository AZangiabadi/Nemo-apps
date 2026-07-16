from __future__ import annotations

import hmac
import secrets
from urllib.parse import urlsplit

from flask import (
    Blueprint,
    abort,
    current_app,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

auth_blueprint = Blueprint("auth", __name__)


def _safe_next_url(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or not value.startswith("/") or "\\" in value:
        return None
    return value


def csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def _access_password() -> str:
    return current_app.extensions["nemo_config"].access_password


def install_security(app) -> None:
    app.jinja_env.globals["csrf_token"] = csrf_token

    @app.before_request
    def authenticate_and_validate_csrf():
        if request.endpoint in {"auth.login", "core.health", "static"} or request.path.startswith(
            "/static/"
        ):
            return None
        password = _access_password()
        if password and not session.get("authenticated"):
            return redirect(url_for("auth.login", next=request.full_path))
        if request.method == "POST":
            supplied = request.form.get("csrf_token", "") or request.headers.get("X-CSRF-Token", "")
            expected = session.get("csrf_token", "")
            if not expected or not hmac.compare_digest(expected, supplied):
                abort(400, "Invalid or missing CSRF token")
        return None


@auth_blueprint.route("/login", methods=["GET", "POST"])
def login():
    error = ""
    if request.method == "POST":
        expected = _access_password()
        supplied = request.form.get("password", "")
        supplied_csrf = request.form.get("csrf_token", "")
        expected_csrf = session.get("csrf_token", "")
        if (
            not expected_csrf
            or not supplied_csrf
            or not hmac.compare_digest(expected_csrf, supplied_csrf)
        ):
            abort(400, "Invalid CSRF token")
        if expected and hmac.compare_digest(expected, supplied):
            session["authenticated"] = True
            return redirect(_safe_next_url(request.args.get("next")) or url_for("core.home"))
        error = "Incorrect access password."
    return render_template("login.html", error=error)


@auth_blueprint.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))
