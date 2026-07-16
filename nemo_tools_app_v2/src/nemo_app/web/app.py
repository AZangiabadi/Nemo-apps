from __future__ import annotations

from flask import Flask, render_template
from werkzeug.middleware.proxy_fix import ProxyFix

from nemo_app.config import AppConfig
from nemo_app.dashboard.service import DashboardService
from nemo_app.jobs.store import create_job_store

from .blueprints.administration import administration_blueprint
from .blueprints.core import core_blueprint
from .blueprints.dashboard import dashboard_blueprint
from .blueprints.invoices import invoice_blueprint
from .blueprints.jobs import jobs_blueprint
from .blueprints.reports import reports_blueprint
from .registry import TOOLS
from .security import auth_blueprint, install_security


def create_app(config: AppConfig | None = None) -> Flask:
    settings = config or AppConfig.from_env()
    settings.validate()
    settings.ensure_directories()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = settings.flask_secret_key
    app.config.update(
        MAX_CONTENT_LENGTH=32 * 1024 * 1024,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=not settings.debug,
    )
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.extensions["nemo_config"] = settings
    app.extensions["job_store"] = create_job_store(settings)
    app.extensions["dashboard_service"] = DashboardService(
        timezone=settings.timezone,
        cache_seconds=settings.jumbotron_cache_seconds,
    )
    app.jinja_env.globals["tools"] = TOOLS
    app.register_blueprint(auth_blueprint)
    app.register_blueprint(core_blueprint)
    app.register_blueprint(invoice_blueprint)
    app.register_blueprint(administration_blueprint)
    app.register_blueprint(reports_blueprint)
    app.register_blueprint(jobs_blueprint)
    app.register_blueprint(dashboard_blueprint)
    install_security(app)

    @app.errorhandler(404)
    def not_found(_error):
        return render_template(
            "error.html", title="Not Found", message="The requested page was not found."
        ), 404

    @app.errorhandler(413)
    def too_large(_error):
        return render_template(
            "error.html", title="Upload Too Large", message="Uploads are limited to 32 MB."
        ), 413

    return app


def main() -> None:
    config = AppConfig.from_env()
    app = create_app(config)
    print(f"NEMO Tools Hub starting on https://127.0.0.1:{config.port}")
    app.run(
        host="0.0.0.0",
        port=config.port,
        debug=config.debug,
        ssl_context="adhoc",
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
