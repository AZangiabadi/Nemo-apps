from __future__ import annotations

from flask import Blueprint, current_app, jsonify, render_template, send_file

core_blueprint = Blueprint("core", __name__)


@core_blueprint.get("/healthz")
def health():
    return jsonify({"status": "ok"})


@core_blueprint.get("/")
def home():
    return render_template("home.html")


@core_blueprint.get("/assets/<name>")
def asset(name: str):
    allowed = {
        "columbia-logo": "Columbia_logo.png",
        "nemo-logo": "nemo_logo.jpeg",
        "import-template": "Account-project-user-adding-Nemo.xlsx",
    }
    filename = allowed.get(name)
    if not filename:
        return "", 404
    path = current_app.extensions["nemo_config"].asset_dir / filename
    if not path.exists():
        return "", 404
    return send_file(
        path,
        as_attachment=name == "import-template",
        download_name=path.name,
    )
