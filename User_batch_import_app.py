import io
import os
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Callable

import requests
from flask import Flask, render_template, request


ImportFn = Callable[[str, str, bool], None]


def create_import_app(
    *,
    import_fn: ImportFn,
    template_name: str,
    allowed_suffixes: set[str],
    max_content_length: int = 16 * 1024 * 1024,
) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = max_content_length

    def allowed_filename(filename: str) -> bool:
        return Path(filename).suffix.lower() in allowed_suffixes

    @app.route("/", methods=["GET", "POST"])
    def index():
        result = None
        error = None
        status = None

        if request.method == "POST":
            token = request.form.get("token", "").strip()
            dry_run = request.form.get("dry_run") == "on"
            spreadsheet = request.files.get("spreadsheet")

            if not token:
                error = "Enter your NEMO API token."
            elif not spreadsheet or not spreadsheet.filename:
                error = "Choose a spreadsheet to upload."
            elif not allowed_filename(spreadsheet.filename):
                accepted = ", ".join(sorted(allowed_suffixes))
                error = f"Upload one of: {accepted}"
            else:
                suffix = Path(spreadsheet.filename).suffix.lower()
                temp_path = None
                output = io.StringIO()

                try:
                    with tempfile.NamedTemporaryFile(
                        suffix=suffix, delete=False
                    ) as handle:
                        spreadsheet.save(handle)
                        temp_path = Path(handle.name)

                    with redirect_stdout(output), redirect_stderr(output):
                        print(
                            "Run started via web app.\n"
                            f"Uploaded file: {spreadsheet.filename}\n"
                            f"Mode: {'Dry Run' if dry_run else 'Live Import'}\n"
                        )
                        import_fn(str(temp_path), token, dry_run)

                    status = "success"
                    result = output.getvalue().strip()
                except requests.HTTPError as error_obj:
                    response = error_obj.response
                    details = response.text if response is not None else str(error_obj)
                    error = f"API request failed.\n\n{details}"
                    status = "error"
                    result = output.getvalue().strip()
                except Exception as error_obj:
                    error = str(error_obj)
                    status = "error"
                    result = output.getvalue().strip()
                finally:
                    if temp_path and temp_path.exists():
                        temp_path.unlink()

        return render_template(
            template_name,
            error=error,
            result=result,
            status=status,
        )

    return app


def run_dev_server(app: Flask, *, default_port: int, label: str) -> None:
    port = int(os.environ.get("PORT", str(default_port)))
    print(f"{label} starting on http://127.0.0.1:{port}")
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=port)
