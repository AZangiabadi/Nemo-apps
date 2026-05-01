import json
import html
import io
import os
import shutil
import tempfile
import threading
import traceback
import uuid
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
from dotenv import load_dotenv
from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from werkzeug.middleware.proxy_fix import ProxyFix

import nemo_invoice_generator_with_pdf as invoice_logic
from excel_invoice_pdf_converter import convert_excel_invoice_to_pdf
from nemo_invoice_generator_with_pdf import NEMO_BASE_URL, create_invoice_zip
from nemo_user_importer import BASE_URL as NEMO_API_BASE_URL, NemoClient, run_import


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DEFAULT_PORT = int(os.environ.get("PORT", "8000"))
DEBUG_MODE = os.environ.get("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
JUMBOTRON_API_TOKEN_ENV = "NEMO_JUMBOTRON_API_TOKEN"
JUMBOTRON_TIMEZONE_NAME = os.environ.get(
    "NEMO_JUMBOTRON_TIMEZONE", "America/New_York"
).strip() or "America/New_York"
JUMBOTRON_REFRESH_SECONDS = int(os.environ.get("NEMO_JUMBOTRON_REFRESH_SECONDS", "15"))
JUMBOTRON_CACHE_SECONDS = max(
    0, int(os.environ.get("NEMO_JUMBOTRON_CACHE_SECONDS", "15"))
)
JUMBOTRON_SCROLL_STEP_PX = int(os.environ.get("NEMO_JUMBOTRON_SCROLL_STEP_PX", "1"))
JUMBOTRON_SCROLL_INTERVAL_MS = int(
    os.environ.get("NEMO_JUMBOTRON_SCROLL_INTERVAL_MS", "50")
)
ALLOWED_IMPORT_SUFFIXES = {".xlsx", ".csv"}
ALLOWED_INVOICE_SUFFIXES = {".csv"}
ALLOWED_EXCEL_INVOICE_SUFFIXES = {".xlsx", ".xlsm"}
ALLOWED_MISSED_RESERVATION_SUFFIXES = {".csv"}
GENERATED_INVOICES_DIR = BASE_DIR / "generated_invoices"
JOB_STATE_DIR = BASE_DIR / ".job_state"
INVOICE_RUN_LOG_PATH = BASE_DIR / "invoice_generator_runs.log"
INVOICE_RETENTION_DAYS = 14
JOBS: dict[str, dict[str, object]] = {}
JOBS_LOCK = threading.Lock()
JUMBOTRON_CACHE_LOCK = threading.Lock()
JUMBOTRON_CACHE: dict[str, object] = {
    "report": None,
    "token": None,
    "expires_at": None,
}

try:
    JUMBOTRON_TIMEZONE = ZoneInfo(JUMBOTRON_TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    JUMBOTRON_TIMEZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class AppDefinition:
    slug: str
    title: str
    summary: str
    accent: str
    details: str


APP_DEFINITIONS: list[AppDefinition] = [
    AppDefinition(
        slug="user-batch-import",
        title="User/Account/Project Batch Import From Excel",
        summary="Upload an Excel or CSV file and create NEMO accounts, projects, PIs, and users.",
        accent="#0f766e",
        details="Good for onboarding users in bulk with optional dry-run mode.",
    ),
    AppDefinition(
        slug="nemo-invoice-generator",
        title="NEMO Invoice Generator",
        summary="Upload a usage CSV and generate invoice ZIP files with Excel and optional PDF output.",
        accent="#9a3412",
        details="Uses your existing invoice logic and keeps the ZIP ready for download.",
    ),
    AppDefinition(
        slug="excel-invoice-to-pdf",
        title="Excel Invoice to PDF",
        summary="Upload an edited NEMO invoice workbook and generate a matching PDF invoice.",
        accent="#475569",
        details="Useful when you adjust the invoice Excel file and need a fresh PDF in the same format.",
    ),
    AppDefinition(
        slug="missed-reservation-report",
        title="Missed Reservation Report",
        summary="Upload a usage CSV and list users with 5 or more missed reservations.",
        accent="#be123c",
        details="Counts missed-reservation charge rows by user for quick follow-up.",
    ),
    AppDefinition(
        slug="account-project-replacement",
        title="Account/Project Replacement",
        summary="Clone an old account/project to a new number, then deactivate the old records.",
        accent="#6d28d9",
        details="Uses the NEMO API and runs in dry-run mode by default.",
    ),
    AppDefinition(
        slug="jumbotron",
        title="Jumbotron",
        summary="Show live tool usage, upcoming reservations for today and tomorrow, and today's cancellations.",
        accent="#1d4ed8",
        details="Built on top of the NEMO jumbotron idea with reservation-focused tables.",
    ),
]


def register_app(definition: AppDefinition) -> None:
    APP_DEFINITIONS.append(definition)


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024



def render_page(title: str, body: str) -> str:
    return render_template("base.html", title=title, body=body)


def get_app(slug: str) -> AppDefinition:
    for definition in APP_DEFINITIONS:
        if definition.slug == slug:
            return definition
    raise KeyError(slug)


def save_upload(upload, *, allowed_suffixes: set[str], folder: str) -> Path:
    suffix = Path(upload.filename or "").suffix.lower()
    if suffix not in allowed_suffixes:
        accepted = ", ".join(sorted(allowed_suffixes))
        raise ValueError(f"Upload one of: {accepted}")

    destination = Path(folder) / f"{uuid.uuid4().hex}{suffix}"
    upload.save(destination)
    return destination


def iso_timestamp(value: Optional[datetime] = None) -> str:
    current = value or datetime.now().astimezone()
    if current.tzinfo is None:
        current = current.astimezone()
    return current.isoformat(timespec="seconds")


def find_website_logo_path() -> Optional[str]:
    for candidate in (
        "Columbia_logo.png",
        "columbia_logo.png",
        "Columbia_logo.jpg",
        "columbia_logo.jpg",
        "Columbia_logo.jpeg",
        "columbia_logo.jpeg",
        "CNI_logo.png",
        "cni_logo.png",
        "CNI-logo.png",
        "cni-logo.png",
    ):
        path = BASE_DIR / candidate
        if path.exists():
            return str(path)
    return None


def find_pdf_logo_path() -> Optional[str]:
    for candidate in (
        "Columbia_logo.png",
        "columbia_logo.png",
        "columbia_logo.jpg",
        "Columbia_logo.jpg",
        "columbia_logo.jpeg",
        "Columbia_logo.jpeg",
    ):
        path = BASE_DIR / candidate
        if path.exists():
            return str(path)
    return None


def find_nemo_logo_path() -> Optional[str]:
    for candidate in (
        "nemo_logo.png",
        "Nemo_logo.png",
        "nemo-logo.png",
        "Nemo-logo.png",
        "cni_nemo_logo.png",
        "CNI_NEMO_logo.png",
        "nemo_logo.jpg",
        "nemo_logo.jpeg",
    ):
        path = BASE_DIR / candidate
        if path.exists():
            return str(path)
    return None


def find_batch_import_template_path() -> Optional[str]:
    template_path = BASE_DIR / "Account-project-user-adding-Nemo.xlsx"
    if template_path.exists():
        return str(template_path)
    return None


def ensure_job_state_dir() -> Path:
    JOB_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return JOB_STATE_DIR


def job_state_path(job_id: str) -> Path:
    return ensure_job_state_dir() / f"{job_id}.json"


def write_job_state(job_id: str, job: dict[str, object]) -> None:
    path = job_state_path(job_id)
    payload = dict(job)
    payload["files"] = list(job.get("files", []))
    payload["file_downloads"] = list(job.get("file_downloads", []))
    payload["log_lines"] = list(job.get("log_lines", []))
    payload["job_id"] = job_id
    temp_path = path.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    temp_path.replace(path)


def load_job_state(job_id: str) -> Optional[dict[str, object]]:
    path = job_state_path(job_id)
    if not path.exists():
        return None
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    job["files"] = list(job.get("files", []))
    job["file_downloads"] = list(job.get("file_downloads", []))
    job["log_lines"] = list(job.get("log_lines", []))
    if "log" not in job:
        job["log"] = "\n".join(str(line) for line in job["log_lines"])
    return job


def set_job(job_id: str, job: dict[str, object]) -> None:
    job_copy = dict(job)
    job_copy["files"] = list(job.get("files", []))
    job_copy["file_downloads"] = list(job.get("file_downloads", []))
    job_copy["log_lines"] = list(job.get("log_lines", []))
    with JOBS_LOCK:
        JOBS[job_id] = job_copy
    write_job_state(job_id, job_copy)


def update_job(job_id: str, **changes: object) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            job = load_job_state(job_id)
            if job is None:
                return
            JOBS[job_id] = job
        job.update(changes)
        write_job_state(job_id, job)


def get_job(job_id: str) -> Optional[dict[str, object]]:
    with JOBS_LOCK:
        loaded_job = load_job_state(job_id)
        if loaded_job is not None:
            JOBS[job_id] = loaded_job
            job = loaded_job
        else:
            job = JOBS.get(job_id)
            if not job:
                return None
        copied = dict(job)
        copied["files"] = list(job.get("files", []))
        copied["file_downloads"] = list(job.get("file_downloads", []))
        copied["log_lines"] = list(job.get("log_lines", []))
        return copied


def append_job_log(job_id: str, message: str) -> None:
    print(f"[job {job_id}] {message}", flush=True)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            job = load_job_state(job_id)
            if job is None:
                return
            JOBS[job_id] = job
        lines = list(job.get("log_lines", []))
        lines.append(str(message))
        if len(lines) > 400:
            lines = lines[-400:]
        job["log_lines"] = lines
        job["log"] = "\n".join(job["log_lines"])
        write_job_state(job_id, job)


def ensure_generated_invoices_dir() -> Path:
    GENERATED_INVOICES_DIR.mkdir(parents=True, exist_ok=True)
    return GENERATED_INVOICES_DIR


def cleanup_old_generated_jobs(retention_days: int = INVOICE_RETENTION_DAYS) -> None:
    root = ensure_generated_invoices_dir()
    cutoff = datetime.now() - timedelta(days=retention_days)

    for day_dir in root.iterdir():
        if not day_dir.is_dir():
            continue
        for job_dir in day_dir.iterdir():
            if not job_dir.is_dir():
                continue

            metadata_path = job_dir / "metadata.json"
            created_at = None
            if metadata_path.exists():
                try:
                    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                    raw_created_at = metadata.get("created_at")
                    if raw_created_at:
                        created_at = datetime.fromisoformat(str(raw_created_at))
                except Exception:
                    created_at = None

            if created_at is None:
                created_at = datetime.fromtimestamp(job_dir.stat().st_mtime)

            if created_at < cutoff:
                shutil.rmtree(job_dir, ignore_errors=True)

        if not any(day_dir.iterdir()):
            day_dir.rmdir()


def _resolve_existing_output_path(
    source_path: Optional[str], fallback_dir: Optional[Path] = None
) -> Optional[Path]:
    if source_path:
        candidate = Path(source_path)
        if candidate.exists():
            return candidate

    if fallback_dir and fallback_dir.exists() and source_path:
        fallback_candidate = fallback_dir / Path(source_path).name
        if fallback_candidate.exists():
            return fallback_candidate

    return None


def _collect_existing_generated_outputs(
    generated_paths: list[str], fallback_dir: Optional[Path] = None
) -> list[Path]:
    resolved_paths: list[Path] = []
    seen: set[Path] = set()

    for source_path in generated_paths:
        resolved = _resolve_existing_output_path(source_path, fallback_dir)
        if resolved and resolved not in seen:
            resolved_paths.append(resolved)
            seen.add(resolved)

    if resolved_paths or not fallback_dir or not fallback_dir.exists():
        return resolved_paths

    for pattern in ("*.xlsx", "*.pdf"):
        for candidate in sorted(fallback_dir.glob(pattern)):
            if candidate not in seen:
                resolved_paths.append(candidate)
                seen.add(candidate)

    return resolved_paths


def resolve_invoice_outputs_for_download(
    *,
    make_zip: bool,
    zip_path: Optional[str],
    generated_paths: list[str],
) -> tuple[Optional[str], list[str]]:
    fallback_dir = Path(generated_paths[0]).parent if generated_paths else None

    if make_zip:
        if not zip_path:
            raise ValueError("ZIP output was requested, but no ZIP file was created.")
        resolved_zip_path = _resolve_existing_output_path(zip_path, fallback_dir)
        if not resolved_zip_path:
            raise FileNotFoundError(
                "ZIP file was created but could not be found in the temporary output folder."
            )
        return str(resolved_zip_path), []

    existing_outputs = _collect_existing_generated_outputs(generated_paths, fallback_dir)
    if not existing_outputs:
        raise FileNotFoundError(
            "Invoice files were created but could not be found in the temporary output folder."
        )
    return None, [str(path) for path in existing_outputs]


def append_invoice_run_log(
    *,
    job_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    summary: str,
    workdir: str,
    generate_excel: bool,
    generate_pdf: bool,
    make_zip: bool,
    xlsx_created: int = 0,
    pdf_created: int = 0,
    error: Optional[str] = None,
) -> None:
    entry = {
        "job_id": job_id,
        "started_at": iso_timestamp(started_at),
        "finished_at": iso_timestamp(finished_at),
        "status": status,
        "summary": summary,
        "workdir": workdir,
        "generate_excel": generate_excel,
        "generate_pdf": generate_pdf,
        "make_zip": make_zip,
        "xlsx_created": xlsx_created,
        "pdf_created": pdf_created,
    }
    if error:
        entry["error"] = error
    with INVOICE_RUN_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=True) + "\n")


def persist_invoice_outputs(
    *,
    job_id: str,
    created_at: datetime,
    make_zip: bool,
    zip_path: Optional[str],
    generated_paths: list[str],
    selected_options: dict[str, object],
) -> tuple[Path, Optional[str], list[str], Path]:
    base_output_dir = ensure_generated_invoices_dir()
    job_output_dir = base_output_dir / created_at.strftime("%Y-%m-%d") / job_id
    job_output_dir.mkdir(parents=True, exist_ok=True)
    fallback_dir = Path(generated_paths[0]).parent if generated_paths else None

    persisted_zip_path: Optional[str] = None
    persisted_files: list[str] = []

    if make_zip:
        if not zip_path:
            raise ValueError("ZIP output was requested, but no ZIP file was created.")
        source_zip_path = _resolve_existing_output_path(zip_path, fallback_dir)
        if not source_zip_path:
            raise FileNotFoundError(
                "ZIP file was created but could not be found for persistence. "
                f"zip_path={zip_path!r}, fallback_dir={str(fallback_dir)!r}, "
                f"job_output_dir={str(job_output_dir)!r}"
            )
        destination = job_output_dir / source_zip_path.name
        shutil.move(str(source_zip_path), destination)
        persisted_zip_path = str(destination)
    else:
        existing_outputs = _collect_existing_generated_outputs(generated_paths, fallback_dir)
        if not existing_outputs:
            raise FileNotFoundError(
                "Invoice files were created but could not be found for persistence. "
                f"generated_paths={generated_paths!r}, fallback_dir={str(fallback_dir)!r}, "
                f"job_output_dir={str(job_output_dir)!r}"
            )
        for source_path in existing_outputs:
            destination = job_output_dir / source_path.name
            shutil.move(str(source_path), destination)
            persisted_files.append(str(destination))

    metadata = {
        "job_id": job_id,
        "created_at": iso_timestamp(created_at),
        "selected_options": selected_options,
        "output_file_paths": persisted_files,
        "zip_file_path": persisted_zip_path,
    }
    metadata_path = job_output_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    return job_output_dir, persisted_zip_path, persisted_files, metadata_path


def verify_persisted_invoice_outputs(
    *,
    make_zip: bool,
    persisted_zip_path: Optional[str],
    persisted_files: list[str],
    metadata_path: Path,
) -> None:
    if make_zip:
        if not persisted_zip_path or not Path(persisted_zip_path).exists():
            raise FileNotFoundError(
                "ZIP file was not found in persistent storage after moving it."
            )
    else:
        if not persisted_files:
            raise FileNotFoundError(
                "No generated invoice files were found in persistent storage."
            )
        missing_paths = [path for path in persisted_files if not Path(path).exists()]
        if missing_paths:
            raise FileNotFoundError(
                "Some generated invoice files were not found in persistent storage: "
                + ", ".join(missing_paths)
            )

    if not metadata_path.exists():
        raise FileNotFoundError("Invoice job metadata was not written to persistent storage.")


@app.get("/assets/columbia-logo")
def columbia_logo():
    logo_path = find_website_logo_path()
    if not logo_path:
        return "", 404
    return send_file(logo_path)


@app.get("/assets/nemo-logo")
def nemo_logo():
    logo_path = find_nemo_logo_path()
    if not logo_path:
        return "", 404
    return send_file(logo_path)


@app.get("/downloads/account-project-user-template")
def account_project_user_template():
    template_path = find_batch_import_template_path()
    if not template_path:
        return (
            render_page(
                "Not Found",
                '<section class="panel"><h2>Template file not found.</h2></section>',
            ),
            404,
        )
    return send_file(
        template_path,
        as_attachment=True,
        download_name=Path(template_path).name,
    )


def build_homepage() -> str:
    cards = []
    for definition in APP_DEFINITIONS:
        cards.append(
            f"""
            <article class="card" style="--accent:{html.escape(definition.accent)};">
              <h2>{html.escape(definition.title)}</h2>
              <p>{html.escape(definition.summary)}</p>
              <p class="footer-note">{html.escape(definition.details)}</p>
              <p><a class="button" href="/apps/{html.escape(definition.slug)}">Open App</a></p>
            </article>
            """
        )

    body = f"""
    <section class="grid">
      {''.join(cards)}
    </section>
    """
    return render_page("NEMO Tools Hub", body)


def build_import_page(
    *,
    error: str | None = None,
    result: str | None = None,
    status: str | None = None,
) -> str:
    template_available = find_batch_import_template_path() is not None
    template_note = (
        '<p class="help">Need the Excel template? '
        '<a href="/downloads/account-project-user-template">Download it here</a>.</p>'
        if template_available
        else '<p class="help">The Excel template file is not available in this project folder.</p>'
    )
    message = ""
    if error:
        message += f'<div class="status error"><strong>Error</strong><pre>{html.escape(error)}</pre></div>'
    if result:
        status_class = "success" if status == "success" else "info"
        label = "Completed" if status == "success" else "Run Output"
        message += f'<div class="status {status_class}"><strong>{label}</strong><pre>{html.escape(result)}</pre></div>'

    body = f"""
    <section class="panel accented" style="--accent:#0f766e;">
      <h2>User/Account/Project Batch Import From Excel</h2>
      <p>Upload an Excel or CSV file, provide your NEMO API token.</p>
      {template_note}
      <form action="/apps/user-batch-import/run" method="post" enctype="multipart/form-data">
        <div>
          <label for="token">NEMO API Token</label>
          <input id="token" type="password" name="token" placeholder="Enter API token" required>
        </div>
        <div>
          <label for="spreadsheet">Spreadsheet</label>
          <input id="spreadsheet" type="file" name="spreadsheet" accept=".xlsx,.csv" required>
          <div class="help">Accepted formats: .xlsx and .csv</div>
          <p>First run in dry-run mode, if successful, uncheck both dry-run and bypass cache.</p>
        </div>
        <div>
          <label><input type="checkbox" name="dry_run" checked> Dry run only</label>
          <div class="help">Keep this checked if you want to preview changes before sending them to NEMO.</div>
        </div>
        <div>
          <label><input type="checkbox" name="bypass_cache" unchecked> Bypass cache and use live API data</label>
          <div class="help">Check this if you made changes to users/accounts in the past 10 minutes.</div>
        </div>
        <div class="actions">
          <button type="submit">Run Batch Import</button>
          <a class="button secondary" href="/">Back Home</a>
        </div>
      </form>
      {message}
    </section>
    """
    return render_page("User/Account/Project Batch Import", body)


def build_import_job_page(job_id: str) -> str:
    body = f"""
    <section class="panel accented" style="--accent:#0f766e;">
      <h2>User/Account/Project Batch Import In Progress</h2>
      <p>The batch import is running in the background. This page updates automatically while NEMO accounts, projects, and users are processed.</p>
      <div class="state-row" aria-label="Job state">
        <span id="job-state-pill" class="state-pill running">Running</span>
      </div>
      <div id="job-status" class="status info">
        <strong id="job-title">Starting...</strong>
        <div id="job-summary" class="help">Preparing batch import job.</div>
        <div id="job-timer" class="help">Elapsed time: 00:00</div>
        <div class="progress-shell"><div id="job-progress-bar" class="progress-bar"></div></div>
        <pre id="job-log" class="job-log">Waiting for first update...</pre>
      </div>
      <div class="actions">
        <a class="button secondary" href="/apps/user-batch-import/jobs/{html.escape(job_id)}/status" target="_blank" rel="noopener noreferrer">Open Status API</a>
        <a class="button secondary" href="/apps/user-batch-import">Start Another Run</a>
        <a class="button secondary" href="/">Back Home</a>
      </div>
    </section>
    <script>
      const jobId = {json.dumps(job_id)};
      const statusEl = document.getElementById("job-status");
      const titleEl = document.getElementById("job-title");
      const summaryEl = document.getElementById("job-summary");
      const timerEl = document.getElementById("job-timer");
      const logEl = document.getElementById("job-log");
      const barEl = document.getElementById("job-progress-bar");
      const statePillEl = document.getElementById("job-state-pill");
      let startedAtMs = null;
      let timerHandle = null;

      function formatElapsed(ms) {{
        const totalSeconds = Math.max(0, Math.floor(ms / 1000));
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;

        if (hours > 0) {{
          return `${{String(hours).padStart(2, "0")}}:${{String(minutes).padStart(2, "0")}}:${{String(seconds).padStart(2, "0")}}`;
        }}
        return `${{String(minutes).padStart(2, "0")}}:${{String(seconds).padStart(2, "0")}}`;
      }}

      function renderTimer() {{
        if (startedAtMs === null) {{
          timerEl.textContent = "Elapsed time: 00:00";
          return;
        }}
        timerEl.textContent = `Elapsed time: ${{formatElapsed(Date.now() - startedAtMs)}}`;
      }}

      timerHandle = window.setInterval(renderTimer, 1000);

      async function poll() {{
        const response = await fetch(`/apps/user-batch-import/jobs/${{jobId}}/status`);
        if (!response.ok) {{
          titleEl.textContent = "Job not found";
          summaryEl.textContent = "The job data is no longer available.";
          statusEl.className = "status error";
          statePillEl.className = "state-pill failed";
          statePillEl.textContent = "Failed";
          return;
        }}

        const data = await response.json();
        const total = data.total || 0;
        const current = data.current || 0;
        const percent = total > 0 ? Math.round((current / total) * 100) : 0;

        titleEl.textContent = data.title || "Batch Import";
        summaryEl.textContent = data.summary || "";
        if (data.started_at) {{
          const parsed = Date.parse(data.started_at);
          if (!Number.isNaN(parsed)) {{
            startedAtMs = parsed;
          }}
        }}
        renderTimer();
        logEl.textContent = data.log || "";
        barEl.style.width = `${{percent}}%`;
        statusEl.className = `status ${{data.status_class || "info"}}`;
        if (data.status === "completed") {{
          statePillEl.className = "state-pill complete";
          statePillEl.textContent = "Complete";
        }} else if (data.status === "error") {{
          statePillEl.className = "state-pill failed";
          statePillEl.textContent = "Failed";
        }} else {{
          statePillEl.className = "state-pill running";
          statePillEl.textContent = "Running";
        }}

        if (data.finished) {{
          if (timerHandle !== null) {{
            window.clearInterval(timerHandle);
            timerHandle = null;
          }}
          return;
        }}
        window.setTimeout(poll, 1200);
      }}

      poll();
    </script>
    """
    return render_page("Batch Import Status", body)


def build_invoice_page(
    *,
    error: str | None = None,
    result: str | None = None,
    download_url: str | None = None,
) -> str:
    pdf_available = invoice_logic._pdf_available()
    pdf_note = (
        "PDF generation is available."
        if pdf_available
        else "PDF generation is currently unavailable because reportlab is not installed in this Python environment."
    )

    message = ""
    if error:
        message += f'<div class="status error"><strong>Error</strong><pre>{html.escape(error)}</pre></div>'
    if result:
        extra = (
            f'<p><a class="button" href="{html.escape(download_url)}">Download Invoice ZIP</a></p>'
            if download_url
            else ""
        )
        message += f'<div class="status success"><strong>Done</strong><pre>{html.escape(result)}</pre>{extra}</div>'

    pdf_checked_attr = "checked" if pdf_available else ""
    pdf_disabled_attr = "" if pdf_available else "disabled"
    body = f"""
    <section class="panel accented" style="--accent:#9a3412;">
      <h2>NEMO Invoice Generator</h2>
      <p>Upload a NEMO usage CSV, choose the file types you want, and start a background job that reports invoice-by-invoice progress.</p>
      <form action="/apps/nemo-invoice-generator/run" method="post" enctype="multipart/form-data">
        <div>
          <label for="api_token">NEMO API Token</label>
          <input id="api_token" type="password" name="api_token" placeholder="Enter API token" required>
        </div>
        <div>
          <label for="csv_file">Usage CSV</label>
          <input id="csv_file" type="file" name="csv_file" accept=".csv" required>
        </div>
        <fieldset>
          <legend>Output Options</legend>
          <div class="choice-row">
            <label><input type="checkbox" name="generate_excel" checked> Excel</label>
            <label><input type="checkbox" name="generate_pdf" {pdf_checked_attr} {pdf_disabled_attr}> PDF</label>
            <label><input type="checkbox" name="make_zip" checked> Make ZIP file</label>
          </div>
          <div class="help">{html.escape(pdf_note)}</div>
          <div class="help">If ZIP is unchecked, the finished page will show individual file download links instead.</div>
        </fieldset>
        <div>
          <label><input type="checkbox" name="bypass_cache" checked> Bypass cache and use live API data</label>
          <div class="help">Check this to force fresh NEMO project and consumable metadata instead of recent cached API results.</div>
        </div>
        <div class="actions">
          <button type="submit">Generate Invoices</button>
          <a class="button secondary" href="/">Back Home</a>
        </div>
      </form>
      {message}
    </section>
    """
    return render_page("NEMO Invoice Generator", body)


def build_excel_invoice_pdf_page(
    *,
    error: str | None = None,
    result: str | None = None,
    download_url: str | None = None,
) -> str:
    pdf_available = invoice_logic._pdf_available()
    message = ""
    if error:
        message += f'<div class="status error"><strong>Error</strong><pre>{html.escape(error)}</pre></div>'
    if result:
        extra = (
            f'<p><a class="button" href="{html.escape(download_url)}">Download PDF</a></p>'
            if download_url
            else ""
        )
        message += f'<div class="status success"><strong>Done</strong><pre>{html.escape(result)}</pre>{extra}</div>'

    disabled_attr = "" if pdf_available else "disabled"
    pdf_note = (
        "PDF generation is available."
        if pdf_available
        else "PDF generation is currently unavailable because reportlab is not installed in this Python environment."
    )

    body = f"""
    <section class="panel accented" style="--accent:#475569;">
      <h2>Excel Invoice to PDF</h2>
      <p>Upload an edited Excel invoice generated by the NEMO Invoice Generator and create a new PDF with the same invoice layout.</p>
      <form action="/apps/excel-invoice-to-pdf/run" method="post" enctype="multipart/form-data">
        <div>
          <label for="invoice_excel">Edited Invoice Excel File</label>
          <input id="invoice_excel" type="file" name="invoice_excel" accept=".xlsx,.xlsm" required>
          <div class="help">Use the workbook that contains the generated “Invoice” sheet.</div>
        </div>
        <div class="help">{html.escape(pdf_note)}</div>
        <div class="actions">
          <button type="submit" {disabled_attr}>Generate PDF</button>
          <a class="button secondary" href="/">Back Home</a>
        </div>
      </form>
      {message}
    </section>
    """
    return render_page("Excel Invoice to PDF", body)


def missed_reservation_mask(df: pd.DataFrame) -> pd.Series:
    candidate_columns = [
        column
        for column in ("Type", "Item", "Description", "Name", "Details")
        if column in df.columns
    ]
    if not candidate_columns:
        raise ValueError(
            "CSV must include at least one column that can identify missed reservations, such as Type or Item."
        )

    mask = pd.Series(False, index=df.index)
    for column in candidate_columns:
        text = df[column].fillna("").astype(str).str.lower()
        mask = mask | text.str.contains(r"missed\s+reservation", regex=True)
        mask = mask | (text.str.contains("missed") & text.str.contains("reservation"))
    return mask


def missed_reservation_tool_column(df: pd.DataFrame) -> Optional[str]:
    for column in ("Tool", "Item", "Description", "Name"):
        if column in df.columns:
            return column
    return None


def format_top_missed_tools(
    missed: pd.DataFrame,
    group_columns: list[str],
) -> pd.DataFrame:
    tool_column = missed_reservation_tool_column(missed)
    if not tool_column:
        return pd.DataFrame(columns=group_columns + ["Top Missed Tools"])

    tool_counts_source = missed.copy()
    tool_counts_source["Missed Tool"] = (
        tool_counts_source[tool_column].fillna("").astype(str).str.strip()
    )
    tool_counts_source = tool_counts_source[tool_counts_source["Missed Tool"] != ""]
    if tool_counts_source.empty:
        return pd.DataFrame(columns=group_columns + ["Top Missed Tools"])

    tool_counts = (
        tool_counts_source.groupby(group_columns + ["Missed Tool"], dropna=False)
        .size()
        .reset_index(name="Tool Misses")
        .sort_values(
            group_columns + ["Tool Misses", "Missed Tool"],
            ascending=[True] * len(group_columns) + [False, True],
        )
    )

    rows: list[dict[str, object]] = []
    for group_key, group in tool_counts.groupby(group_columns, dropna=False, sort=False):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        row = dict(zip(group_columns, group_key))
        row["Top Missed Tools"] = ", ".join(
            f"{record['Missed Tool']} ({int(record['Tool Misses'])})"
            for record in group.head(3).to_dict("records")
        )
        rows.append(row)
    return pd.DataFrame(rows)


def top_missed_tools_report(missed: pd.DataFrame, *, limit: int = 10) -> pd.DataFrame:
    tool_column = missed_reservation_tool_column(missed)
    if not tool_column:
        return pd.DataFrame(columns=["Tool", "Missed Reservations"])

    tool_rows = missed.copy()
    tool_rows["Tool"] = tool_rows[tool_column].fillna("").astype(str).str.strip()
    tool_rows = tool_rows[tool_rows["Tool"] != ""]
    if tool_rows.empty:
        return pd.DataFrame(columns=["Tool", "Missed Reservations"])

    return (
        tool_rows.groupby("Tool", dropna=False)
        .size()
        .reset_index(name="Missed Reservations")
        .sort_values(["Missed Reservations", "Tool"], ascending=[False, True])
        .head(limit)
        .reset_index(drop=True)
    )


def build_missed_reservation_reports(
    csv_path: Path, *, threshold: int = 5
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    df = pd.read_csv(csv_path)
    if "User" not in df.columns:
        raise ValueError("CSV must include a User column.")

    missed = df[missed_reservation_mask(df)].copy()
    empty_user_report = pd.DataFrame(
        columns=["User", "Username", "Missed Reservations", "Top Missed Tools"]
    )
    empty_tool_report = pd.DataFrame(columns=["Tool", "Missed Reservations"])
    if missed.empty:
        return empty_user_report, empty_tool_report, 0

    missed["User"] = missed["User"].fillna("").astype(str).str.strip()
    missed = missed[missed["User"] != ""]
    if missed.empty:
        return empty_user_report, empty_tool_report, 0

    group_columns = ["User"]
    has_username = "Username" in missed.columns
    if has_username:
        missed["Username"] = missed["Username"].fillna("").astype(str).str.strip()
        group_columns.append("Username")

    report = (
        missed.groupby(group_columns, dropna=False)
        .size()
        .reset_index(name="Missed Reservations")
        .sort_values(["Missed Reservations", "User"], ascending=[False, True])
    )
    top_tools = format_top_missed_tools(missed, group_columns)
    if not top_tools.empty:
        report = report.merge(top_tools, on=group_columns, how="left")
    else:
        report["Top Missed Tools"] = ""
    report["Top Missed Tools"] = report["Top Missed Tools"].fillna("")
    report = report[report["Missed Reservations"] >= threshold]
    if not has_username:
        report.insert(1, "Username", "")
    user_report = report.loc[
        :, ["User", "Username", "Missed Reservations", "Top Missed Tools"]
    ].reset_index(drop=True)
    return user_report, top_missed_tools_report(missed), len(report)


def build_missed_reservation_report(csv_path: Path, *, threshold: int = 5) -> pd.DataFrame:
    user_report, _tool_report, _total_missed_users = build_missed_reservation_reports(
        csv_path, threshold=threshold
    )
    return user_report


def build_missed_reservation_page(
    *,
    error: str | None = None,
    report: pd.DataFrame | None = None,
    tool_report: pd.DataFrame | None = None,
    total_missed_users: int | None = None,
) -> str:
    message = ""
    if error:
        message = f'<div class="status error"><strong>Error</strong><pre>{html.escape(error)}</pre></div>'

    report_html = ""
    if report is not None:
        if report.empty:
            report_html = """
            <div class="status success">
              <strong>No users at threshold</strong>
              <div>No users had 5 or more missed reservations in this CSV.</div>
            </div>
            """
        else:
            rows = "\n".join(
                f"""
                <tr>
                  <td>{html.escape(str(row["User"]))}</td>
                  <td>{html.escape(str(row["Username"]))}</td>
                  <td>{int(row["Missed Reservations"])}</td>
                  <td>{html.escape(str(row["Top Missed Tools"]))}</td>
                </tr>
                """
                for row in report.to_dict("records")
            )
            total_text = (
                f"{total_missed_users} user(s) had at least one missed reservation. "
                if total_missed_users is not None
                else ""
            )
            report_html = f"""
            <div class="status success">
              <strong>Report ready</strong>
              <div>{html.escape(total_text)}{len(report)} user(s) had 5 or more missed reservations.</div>
            </div>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>User</th>
                    <th>Username</th>
                    <th>Missed Reservations</th>
                    <th>Top Missed Tools</th>
                  </tr>
                </thead>
                <tbody>{rows}</tbody>
              </table>
            </div>
            """

    tool_report_html = ""
    if tool_report is not None and not tool_report.empty:
        tool_rows = "\n".join(
            f"""
            <tr>
              <td>{html.escape(str(row["Tool"]))}</td>
              <td>{int(row["Missed Reservations"])}</td>
            </tr>
            """
            for row in tool_report.to_dict("records")
        )
        tool_report_html = f"""
        <h2>Top Missed Tools</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Tool</th>
                <th>Missed Reservations</th>
              </tr>
            </thead>
            <tbody>{tool_rows}</tbody>
          </table>
        </div>
        """

    body = f"""
    <section class="panel accented" style="--accent:#be123c;">
      <h2>Missed Reservation Report</h2>
      <p>Upload a NEMO usage CSV and list users with 5 or more missed reservation rows.</p>
      <form action="/apps/missed-reservation-report/run" method="post" enctype="multipart/form-data">
        <div>
          <label for="usage_csv">Usage CSV</label>
          <input id="usage_csv" type="file" name="usage_csv" accept=".csv" required>
          <div class="help">Rows are counted when common usage-export fields contain “missed reservation”.</div>
        </div>
        <div class="actions">
          <button type="submit">Build Report</button>
          <a class="button secondary" href="/">Back Home</a>
        </div>
      </form>
      {message}
      {report_html}
      {tool_report_html}
    </section>
    """
    return render_page("Missed Reservation Report", body)


ACCOUNT_CLONE_FIELDS = ("note", "type")
PROJECT_CLONE_FIELDS = (
    "principal_investigators",
    "users",
    "application_identifier",
    "allow_consumable_withdrawals",
    "allow_staff_charges",
    "discipline",
    "project_types",
    "only_allow_tools",
    "project_name",
    "contact_name",
    "contact_phone",
    "contact_email",
    "expires_on",
    "addressee",
    "comments",
    "no_charge",
    "no_tax",
    "no_cap",
    "category",
    "institution",
    "department",
    "staff_host",
)


def _find_nemo_record(records: list[dict], value: str, label: str) -> dict:
    lookup = value.strip()
    if not lookup:
        raise ValueError(f"Enter the {label}.")

    if lookup.isdigit():
        record_id = int(lookup)
        id_matches = [record for record in records if record.get("id") == record_id]
        if len(id_matches) == 1:
            return id_matches[0]

    name_matches = [
        record
        for record in records
        if str(record.get("name", "")).strip() == lookup
    ]
    if len(name_matches) == 1:
        return name_matches[0]
    if len(name_matches) > 1:
        raise ValueError(f"Multiple {label} records matched {lookup!r}; use the API id instead.")
    raise ValueError(f"No {label} record found for {lookup!r}.")


def _find_account_for_project(accounts: list[dict], project: dict) -> dict:
    project_name = str(project.get("name", "")).strip()
    name_matches = [
        account
        for account in accounts
        if str(account.get("name", "")).strip() == project_name
    ]
    if len(name_matches) == 1:
        return name_matches[0]
    if len(name_matches) > 1:
        raise ValueError(
            f"Multiple account records matched project name {project_name!r}; "
            "clean up the duplicate account names before replacement."
        )

    linked_account_id = project.get("account")
    if linked_account_id is not None:
        id_matches = [
            account for account in accounts if account.get("id") == linked_account_id
        ]
        if len(id_matches) == 1:
            return id_matches[0]

    raise ValueError(
        f"No account found with the same name as project {project_name!r}."
    )


def _record_name_exists(records: list[dict], name: str) -> bool:
    return any(str(record.get("name", "")).strip() == name.strip() for record in records)


def clone_account_project(
    *,
    token: str,
    old_number: str,
    new_number: str,
    dry_run: bool,
) -> list[str]:
    client = NemoClient(token=token, base_url=NEMO_API_BASE_URL, dry_run=dry_run)
    today = datetime.now(JUMBOTRON_TIMEZONE).date().isoformat()
    old_number = old_number.strip()
    new_number = new_number.strip()
    if not token.strip():
        raise ValueError("NEMO API token is required.")
    if not old_number:
        raise ValueError("Old account/project number is required.")
    if not new_number:
        raise ValueError("New account/project number is required.")
    if old_number == new_number:
        raise ValueError("The old and new account/project numbers must be different.")

    accounts = client.fetch_all("accounts/")
    projects = client.fetch_all("projects/")
    old_project = _find_nemo_record(projects, old_number, "old project")
    old_account = _find_account_for_project(accounts, old_project)

    if _record_name_exists(accounts, new_number):
        raise ValueError(f"An account named {new_number!r} already exists.")
    if _record_name_exists(projects, new_number):
        raise ValueError(f"A project named {new_number!r} already exists.")

    account_payload = {
        field: old_account.get(field)
        for field in ACCOUNT_CLONE_FIELDS
        if field in old_account
    }
    account_payload.update(
        {
            "name": new_number,
            "start_date": today,
            "active": True,
        }
    )
    new_account = client.post("accounts/", account_payload)
    new_account_id = int(new_account["id"])

    project_payload = {
        field: old_project.get(field)
        for field in PROJECT_CLONE_FIELDS
        if field in old_project
    }
    project_payload.update(
        {
            "name": new_number,
            "start_date": today,
            "active": True,
            "account": new_account_id,
        }
    )
    new_project = client.post("projects/", project_payload)

    client.patch(f"projects/{old_project['id']}/", {"active": False})
    client.patch(f"accounts/{old_account['id']}/", {"active": False})

    mode = "DRY RUN" if dry_run else "LIVE RUN"
    return [
        f"{mode} completed.",
        f"Old account: {old_account.get('name')} (id {old_account.get('id')})",
        f"Old project: {old_project.get('name')} (id {old_project.get('id')})",
        f"New account: {new_account.get('name')} (id {new_account.get('id')})",
        f"New project: {new_project.get('name')} (id {new_project.get('id')})",
        f"New start_date: {today}",
        "Old project active=false",
        "Old account active=false",
    ]


def build_account_project_replacement_page(
    *,
    error: str | None = None,
    result: str | None = None,
) -> str:
    message = ""
    if error:
        message = f'<div class="status error"><strong>Error</strong><pre>{html.escape(error)}</pre></div>'
    if result:
        message = f'<div class="status success"><strong>Completed</strong><pre>{html.escape(result)}</pre></div>'

    body = f"""
    <section class="panel accented" style="--accent:#6d28d9;">
      <h2>Account/Project Replacement</h2>
      <p>Clone an existing NEMO account and project to a new account/project number, then deactivate the old account and project.</p>
      <form action="/apps/account-project-replacement/run" method="post">
        <div>
          <label for="token">NEMO API Token</label>
          <input id="token" type="password" name="token" placeholder="Enter API token" required>
        </div>
        <div>
          <label for="old_number">Exact Account Name or Project ID</label>
          <input id="old_number" type="text" name="old_number" placeholder="Exact account name or project ID" required>
          <div class="help">Enter the old account name or the old project API id. The app finds the matching old account by the project name.</div>
        </div>
        <div>
          <label for="new_number">New Account/Project Number</label>
          <input id="new_number" type="text" name="new_number" placeholder="Exact new account/project name" required>
          <div class="help">The new account and project inherit old metadata, use this value as their name, and get today as start_date.</div>
        </div>
        <div>
          <label><input type="checkbox" name="dry_run" checked> Dry run only</label>
          <div class="help">Keep dry run checked first. Uncheck only when the preview looks right.</div>
        </div>
        <div class="actions">
          <button type="submit">Replace Account/Project</button>
          <a class="button secondary" href="/">Back Home</a>
        </div>
      </form>
      {message}
    </section>
    """
    return render_page("Account/Project Replacement", body)


def build_invoice_job_page(job_id: str) -> str:
    body = f"""
    <section class="panel accented" style="--accent:#9a3412;">
      <h2>Invoice Generation In Progress</h2>
      <p>The job is running in the background. This page updates automatically while files are being created.</p>
      <div class="state-row" aria-label="Job state">
        <span id="job-state-pill" class="state-pill running">Running</span>
        <span id="job-download-pill" class="state-pill">Downloads Pending</span>
      </div>
      <div id="job-status" class="status info">
        <strong id="job-title">Starting…</strong>
        <div id="job-summary" class="help">Preparing invoice job.</div>
        <div id="job-timer" class="help">Elapsed time: 00:00</div>
        <div class="progress-shell"><div id="job-progress-bar" class="progress-bar"></div></div>
        <pre id="job-log" class="job-log">Waiting for first update…</pre>
      </div>
      <div id="job-downloads" class="download-list"></div>
      <div class="actions">
        <a class="button secondary" href="/apps/nemo-invoice-generator/jobs/{html.escape(job_id)}/status" target="_blank" rel="noopener noreferrer">Open Status API</a>
        <a class="button secondary" href="/apps/nemo-invoice-generator">Start Another Job</a>
        <a class="button secondary" href="/">Back Home</a>
      </div>
    </section>
    <script>
      const jobId = {json.dumps(job_id)};
      const statusEl = document.getElementById("job-status");
      const titleEl = document.getElementById("job-title");
      const summaryEl = document.getElementById("job-summary");
      const timerEl = document.getElementById("job-timer");
      const logEl = document.getElementById("job-log");
      const barEl = document.getElementById("job-progress-bar");
      const downloadsEl = document.getElementById("job-downloads");
      const statePillEl = document.getElementById("job-state-pill");
      const downloadPillEl = document.getElementById("job-download-pill");
      let timerStartedAtMs = null;
      let timerFinishedAtMs = null;
      let timerHandle = null;

      function formatElapsed(ms) {{
        const totalSeconds = Math.max(0, Math.floor(ms / 1000));
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;

        if (hours > 0) {{
          return `${{String(hours).padStart(2, "0")}}:${{String(minutes).padStart(2, "0")}}:${{String(seconds).padStart(2, "0")}}`;
        }}
        return `${{String(minutes).padStart(2, "0")}}:${{String(seconds).padStart(2, "0")}}`;
      }}

      function renderTimer() {{
        if (timerStartedAtMs === null) {{
          timerEl.textContent = "Elapsed time: waiting for API work to begin";
          return;
        }}
        const endMs = timerFinishedAtMs === null ? Date.now() : timerFinishedAtMs;
        timerEl.textContent = `Elapsed time: ${{formatElapsed(endMs - timerStartedAtMs)}}`;
      }}

      timerHandle = window.setInterval(renderTimer, 1000);

      function renderDownloads(data) {{
        if (!data.finished || data.status !== "completed") {{
          downloadsEl.replaceChildren();
          downloadPillEl.className = "state-pill";
          downloadPillEl.textContent = "Downloads Pending";
          return;
        }}
        const links = [];
        if (data.zip_download_url) {{
          const zipLink = document.createElement("a");
          zipLink.className = "button";
          zipLink.href = data.zip_download_url;
          zipLink.textContent = "Download ZIP";
          links.push(zipLink);
        }}
        if (Array.isArray(data.file_downloads)) {{
          for (const item of data.file_downloads) {{
            const fileLink = document.createElement("a");
            fileLink.className = "button secondary";
            fileLink.href = item.url;
            fileLink.textContent = item.label || "Download file";
            links.push(fileLink);
          }}
        }}
        downloadsEl.replaceChildren(...links);
        downloadPillEl.className = "state-pill downloadable";
        downloadPillEl.textContent = "Downloadable";
      }}

      async function poll() {{
        try {{
          const response = await fetch(`/apps/nemo-invoice-generator/jobs/${{jobId}}/status`, {{
            cache: "no-store",
          }});
          if (!response.ok) {{
            titleEl.textContent = "Job not found";
            summaryEl.textContent = "The job data is no longer available.";
            statusEl.className = "status error";
            statePillEl.className = "state-pill failed";
            statePillEl.textContent = "Failed";
            return;
          }}

          const data = await response.json();
          const total = data.total || 0;
          const current = data.current || 0;
          const percent = total > 0 ? Math.round((current / total) * 100) : 0;

          titleEl.textContent = data.title || "Invoice Job";
          summaryEl.textContent = data.summary || "";
          if (data.timer_started_at) {{
            const parsed = Date.parse(data.timer_started_at);
            if (!Number.isNaN(parsed)) {{
              timerStartedAtMs = parsed;
            }}
          }}
          if (data.links_ready_at) {{
            const parsedFinished = Date.parse(data.links_ready_at);
            if (!Number.isNaN(parsedFinished)) {{
              timerFinishedAtMs = parsedFinished;
            }}
          }}
          renderTimer();
          logEl.textContent = data.log || "";
          logEl.scrollTop = 0;
          barEl.style.width = `${{percent}}%`;
          statusEl.className = `status ${{data.status_class || "info"}}`;
          if (data.status === "completed") {{
            statePillEl.className = "state-pill complete";
            statePillEl.textContent = "Complete";
          }} else if (data.status === "error") {{
            statePillEl.className = "state-pill failed";
            statePillEl.textContent = "Failed";
          }} else {{
            statePillEl.className = "state-pill running";
            statePillEl.textContent = "Running";
          }}
          renderDownloads(data);

          if (data.finished) {{
            if (timerFinishedAtMs === null) {{
              timerFinishedAtMs = Date.now();
              renderTimer();
            }}
            if (timerHandle !== null) {{
              window.clearInterval(timerHandle);
              timerHandle = null;
            }}
            return;
          }}
        }} catch (error) {{
          console.error("Invoice job polling failed", error);
          summaryEl.textContent = "Connection hiccup while checking job status. Retrying...";
        }}
        window.setTimeout(poll, 1200);
      }}

      poll();
    </script>
    """
    return render_page("Invoice Job Status", body)


def parse_api_datetime(value: object) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def format_dashboard_datetime(value: object) -> str:
    parsed = parse_api_datetime(value)
    if not parsed:
        return "—"
    local_value = parsed.astimezone(JUMBOTRON_TIMEZONE)
    return local_value.strftime("%a, %b %d, %Y %I:%M %p")


def get_jumbotron_token() -> str:
    token = os.environ.get(JUMBOTRON_API_TOKEN_ENV, "").strip()
    if token:
        return token
    raise RuntimeError(
        f"Set the {JUMBOTRON_API_TOKEN_ENV} environment variable before opening the jumbotron page."
    )


def fetch_lookup_map(
    client: NemoClient, endpoint: str, ids: set[int]
) -> dict[int, dict[str, object]]:
    if not ids:
        return {}

    records: dict[int, dict[str, object]] = {}
    sorted_ids = sorted(ids)
    for start_index in range(0, len(sorted_ids), 100):
        chunk = sorted_ids[start_index : start_index + 100]
        chunk_text = ",".join(str(item_id) for item_id in chunk)
        for record in client.fetch_all(f"{endpoint}?id__in={chunk_text}"):
            record_id = record.get("id")
            if isinstance(record_id, int):
                records[record_id] = record
    return records


def username_for_id(users_by_id: dict[int, dict[str, object]], user_id: object) -> str:
    if not isinstance(user_id, int):
        return "—"
    user = users_by_id.get(user_id, {})
    username = str(user.get("username", "") or "").strip()
    if username:
        return username
    return f"User {user_id}"


def tool_name_for_id(tools_by_id: dict[int, dict[str, object]], tool_id: object) -> str:
    if not isinstance(tool_id, int):
        return "—"
    tool = tools_by_id.get(tool_id, {})
    name = str(tool.get("name", "") or "").strip()
    if name:
        return name
    return f"Tool {tool_id}"


def build_dashboard_table(
    title: str, subtitle: str, columns: list[str], rows: list[list[str]]
) -> str:
    if not rows:
        table_markup = '<div class="status info">No matching records.</div>'
    else:
        head = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
        body_rows = []
        for row in rows:
            cells = "".join(f"<td>{html.escape(cell)}</td>" for cell in row)
            body_rows.append(f"<tr>{cells}</tr>")
        table_markup = (
            '<div class="table-wrap"><table><thead><tr>'
            + head
            + "</tr></thead><tbody>"
            + "".join(body_rows)
            + "</tbody></table></div>"
        )

    return f"""
    <section class="panel">
      <h2>{html.escape(title)}</h2>
      <p>{html.escape(subtitle)}</p>
      {table_markup}
    </section>
    """


def build_jumbotron_report(token: str) -> dict[str, object]:
    client = NemoClient(token, base_url=NEMO_API_BASE_URL)
    now = datetime.now(JUMBOTRON_TIMEZONE)
    today_start = datetime.combine(now.date(), time.min, tzinfo=now.tzinfo)
    tomorrow_start = today_start + timedelta(days=1)
    day_after_tomorrow_start = today_start + timedelta(days=2)

    usage_events = client.fetch_all("usage_events/?end__isnull=true")
    reservation_query = urlencode(
        {
            "start__gte": today_start.isoformat(),
            "start__lt": day_after_tomorrow_start.isoformat(),
        }
    )
    reservations = client.fetch_all(
        f"reservations/?{reservation_query}"
    )

    user_ids: set[int] = set()
    tool_ids: set[int] = set()

    for event in usage_events:
        for key in ("user", "operator"):
            value = event.get(key)
            if isinstance(value, int):
                user_ids.add(value)
        tool_value = event.get("tool")
        if isinstance(tool_value, int):
            tool_ids.add(tool_value)

    for reservation in reservations:
        for key in ("user", "creator", "cancelled_by"):
            value = reservation.get(key)
            if isinstance(value, int):
                user_ids.add(value)
        tool_value = reservation.get("tool")
        if isinstance(tool_value, int):
            tool_ids.add(tool_value)

    users_by_id = fetch_lookup_map(client, "users/", user_ids)
    tools_by_id = fetch_lookup_map(client, "tools/", tool_ids)

    current_usage = sorted(
        usage_events,
        key=lambda event: (
            parse_api_datetime(event.get("start"))
            or datetime.min.replace(tzinfo=now.tzinfo)
        ),
    )

    upcoming_reservations = []
    todays_cancellations = []
    for reservation in reservations:
        start_time = parse_api_datetime(reservation.get("start"))
        if not start_time:
            continue

        is_cancelled = bool(reservation.get("cancelled"))
        if not is_cancelled and start_time >= now:
            if start_time < tomorrow_start:
                day_label = "Today"
            elif start_time < day_after_tomorrow_start:
                day_label = "Tomorrow"
            else:
                continue
            upcoming_reservations.append((day_label, reservation))

        if is_cancelled and start_time < tomorrow_start:
            todays_cancellations.append(reservation)

    upcoming_reservations.sort(
        key=lambda item: parse_api_datetime(item[1].get("start")) or now
    )
    todays_cancellations.sort(
        key=lambda reservation: parse_api_datetime(reservation.get("cancellation_time"))
        or parse_api_datetime(reservation.get("start"))
        or now,
        reverse=True,
    )

    current_usage_rows = []
    for event in current_usage:
        user_id = (
            event.get("user")
            if isinstance(event.get("user"), int)
            else event.get("operator")
        )
        current_usage_rows.append(
            [
                username_for_id(users_by_id, user_id),
                tool_name_for_id(tools_by_id, event.get("tool")),
                format_dashboard_datetime(event.get("start")),
            ]
        )

    upcoming_rows = []
    for day_label, reservation in upcoming_reservations:
        upcoming_rows.append(
            [
                day_label,
                username_for_id(users_by_id, reservation.get("user")),
                tool_name_for_id(tools_by_id, reservation.get("tool")),
                format_dashboard_datetime(reservation.get("start")),
                format_dashboard_datetime(reservation.get("end")),
            ]
        )

    cancellation_rows = []
    for reservation in todays_cancellations:
        cancellation_rows.append(
            [
                username_for_id(users_by_id, reservation.get("user")),
                tool_name_for_id(tools_by_id, reservation.get("tool")),
                "Missed reservation" if reservation.get("missed") else "User Cancelled",
                format_dashboard_datetime(reservation.get("start")),
                format_dashboard_datetime(reservation.get("cancellation_time")),
            ]
        )

    return {
        "generated_at": now.strftime("%a, %b %d, %Y %I:%M %p"),
        "current_usage_rows": current_usage_rows,
        "upcoming_rows": upcoming_rows,
        "cancellation_rows": cancellation_rows,
    }


def get_jumbotron_report(token: str) -> dict[str, object]:
    now = datetime.now(JUMBOTRON_TIMEZONE)
    with JUMBOTRON_CACHE_LOCK:
        cached_report = JUMBOTRON_CACHE.get("report")
        cached_token = JUMBOTRON_CACHE.get("token")
        expires_at = JUMBOTRON_CACHE.get("expires_at")
        if (
            JUMBOTRON_CACHE_SECONDS > 0
            and isinstance(cached_report, dict)
            and cached_token == token
            and isinstance(expires_at, datetime)
            and now < expires_at
        ):
            return cached_report

    report = build_jumbotron_report(token)

    if JUMBOTRON_CACHE_SECONDS > 0:
        with JUMBOTRON_CACHE_LOCK:
            JUMBOTRON_CACHE["report"] = report
            JUMBOTRON_CACHE["token"] = token
            JUMBOTRON_CACHE["expires_at"] = (
                datetime.now(JUMBOTRON_TIMEZONE)
                + timedelta(seconds=JUMBOTRON_CACHE_SECONDS)
            )

    return report


def build_jumbotron_content(report: dict[str, object]) -> str:
    generated_at = str(report.get("generated_at", "") or "")
    current_usage_rows = report.get("current_usage_rows", [])
    upcoming_rows = report.get("upcoming_rows", [])
    cancellation_rows = report.get("cancellation_rows", [])

    stats = f"""
    <section class="stat-grid">
      <article class="stat-card">
        <p class="stat-label">In Use Now</p>
        <p class="stat-value">{len(current_usage_rows)}</p>
      </article>
      <article class="stat-card">
        <p class="stat-label">Upcoming</p>
        <p class="stat-value">{len(upcoming_rows)}</p>
      </article>
      <article class="stat-card">
        <p class="stat-label">Today's Cancellations</p>
        <p class="stat-value">{len(cancellation_rows)}</p>
      </article>
    </section>
    """

    return f"""
    <section class="panel accented" style="--accent:#1d4ed8;">
      <h2>Live NEMO Activity</h2>
      <p>Data pulled from the NEMO API at {html.escape(generated_at)}.</p>
      {stats}
    </section>
    <div class="section-stack">
      {build_dashboard_table(
          "Currently In Use",
          "",
          ["Username", "Tool", "Started"],
          current_usage_rows,
      )}
      {build_dashboard_table(
          "Upcoming Reservations",
          "Upcoming reservations scheduled for the rest of today and all of tomorrow.",
          ["Day", "Username", "Tool", "Start", "End"],
          upcoming_rows,
      )}
      {build_dashboard_table(
          "Today's Cancellations",
          "Cancelled reservations scheduled for today, including auto-cancelled missed reservations.",
          ["Username", "Tool", "Type", "Reservation Start", "Cancelled At"],
          cancellation_rows,
      )}
    </div>
    """


def build_jumbotron_page(
    *,
    error: str | None = None,
    report: Optional[dict[str, object]] = None,
) -> str:
    content = ""
    if report:
        content = build_jumbotron_content(report)
    elif error:
        content = f'<div class="status error"><strong>Error</strong><pre>{html.escape(error)}</pre></div>'
    else:
        content = '<div class="status info">Loading jumbotron…</div>'

    signature = json.dumps(report or {"error": error}, sort_keys=True)
    return render_template(
        "jumbotron.html",
        content=content,
        initial_signature=signature,
        refresh_ms=JUMBOTRON_REFRESH_SECONDS * 1000,
        scroll_step_px=JUMBOTRON_SCROLL_STEP_PX,
        scroll_interval_ms=JUMBOTRON_SCROLL_INTERVAL_MS,
    )


@app.get("/")
def home() -> str:
    return build_homepage()


@app.get("/apps")
def apps_redirect() -> Response:
    return redirect("/")


@app.get("/apps/<slug>")
def app_page(slug: str) -> str:
    try:
        get_app(slug)
    except KeyError:
        return (
            render_page(
                "Not Found", '<section class="panel"><h2>App not found</h2></section>'
            ),
            404,
        )

    if slug == "user-batch-import":
        return build_import_page()
    if slug == "nemo-invoice-generator":
        return build_invoice_page()
    if slug == "excel-invoice-to-pdf":
        return build_excel_invoice_pdf_page()
    if slug == "missed-reservation-report":
        return build_missed_reservation_page()
    if slug == "account-project-replacement":
        return build_account_project_replacement_page()
    if slug == "jumbotron":
        try:
            report = get_jumbotron_report(get_jumbotron_token())
        except Exception as exc:
            return build_jumbotron_page(error=str(exc))
        return build_jumbotron_page(report=report)
    return (
        render_page(
            "Coming Soon",
            '<section class="panel"><h2>App page is not implemented yet.</h2></section>',
        ),
        404,
    )


@app.post("/apps/user-batch-import/run")
def run_user_batch_import() -> str:
    token = request.form.get("token", "").strip()
    dry_run = request.form.get("dry_run") == "on"
    bypass_cache = request.form.get("bypass_cache") == "on"
    spreadsheet = request.files.get("spreadsheet")

    if not token:
        return build_import_page(error="Enter your NEMO API token.", status="error")
    if not spreadsheet or not spreadsheet.filename:
        return build_import_page(
            error="Choose a spreadsheet to upload.", status="error"
        )

    job_id = str(uuid.uuid4())
    temp_dir = tempfile.mkdtemp(prefix=f"nemo_import_{job_id}_")

    try:
        saved_path = save_upload(
            spreadsheet,
            allowed_suffixes=ALLOWED_IMPORT_SUFFIXES,
            folder=temp_dir,
        )
    except Exception as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        return build_import_page(error=str(exc), status="error")

    set_job(
        job_id,
        {
            "status": "running",
            "title": "Batch import running",
            "summary": "Preparing import job.",
            "current": 0,
            "total": 7,
            "log": "Preparing batch import job...",
            "log_lines": ["Preparing batch import job..."],
            "mode": "Dry Run" if dry_run else "Live Import",
            "cache_mode": "Live API" if bypass_cache else "Cached API",
            "started_at": iso_timestamp(),
        },
    )

    def worker() -> None:
        output = io.StringIO()

        def on_status(message: str) -> None:
            update_job(job_id, summary=message)
            append_job_log(job_id, message)

        def on_progress(done: int, total: int, label: str) -> None:
            update_job(job_id, current=done, total=total, summary=label)

        try:
            append_job_log(
                job_id,
                f"Starting {'dry run' if dry_run else 'live import'} for {spreadsheet.filename}",
            )
            append_job_log(
                job_id,
                f"Using {'live API data' if bypass_cache else 'cached API data when available'}",
            )
            with redirect_stdout(output), redirect_stderr(output):
                print(
                    "Run started via web app.\n"
                    f"Uploaded file: {spreadsheet.filename}\n"
                    f"Mode: {'Dry Run' if dry_run else 'Live Import'}\n"
                    f"API source: {'Live API' if bypass_cache else 'Cached API allowed'}\n"
                )
                run_import(
                    str(saved_path),
                    token,
                    dry_run=dry_run,
                    use_cache=not bypass_cache,
                    status_callback=on_status,
                    progress_callback=on_progress,
                )

            update_job(
                job_id,
                status="completed",
                title="Batch import complete",
                summary=(
                    "Dry run finished. No changes were sent to NEMO."
                    if dry_run
                    else "Import finished successfully."
                ),
                current=7,
                total=7,
            )
        except Exception as exc:
            error_text = str(exc)
            append_job_log(job_id, error_text)
            update_job(
                job_id,
                status="error",
                title="Batch import failed",
                summary=error_text,
            )
        finally:
            if saved_path.exists():
                saved_path.unlink(missing_ok=True)
            shutil.rmtree(temp_dir, ignore_errors=True)

    threading.Thread(target=worker, daemon=True).start()
    return build_import_job_page(job_id)


@app.get("/apps/user-batch-import/jobs/<job_id>/status")
def user_batch_import_job_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    status = str(job.get("status", "running"))
    status_class = "info"
    if status == "completed":
        status_class = "success"
    elif status == "error":
        status_class = "error"

    return jsonify(
        {
            "title": job.get("title", "Batch import"),
            "summary": job.get("summary", ""),
            "log": job.get("log", ""),
            "status": status,
            "status_class": status_class,
            "finished": status in {"completed", "error"},
            "current": job.get("current", 0),
            "total": job.get("total", 0),
            "mode": job.get("mode", ""),
            "started_at": job.get("started_at", ""),
        }
    )


@app.post("/apps/nemo-invoice-generator/run")
def run_invoice_generator() -> str:
    csv_file = request.files.get("csv_file")
    api_token = request.form.get("api_token", "").strip()
    nemo_base = NEMO_BASE_URL
    generate_excel = request.form.get("generate_excel") == "on"
    generate_pdf = request.form.get("generate_pdf") == "on"
    make_zip = request.form.get("make_zip") == "on"
    bypass_cache = request.form.get("bypass_cache") == "on"

    if not api_token:
        return build_invoice_page(error="API token is required.")
    if not csv_file or not csv_file.filename:
        return build_invoice_page(error="Choose a usage CSV to upload.")
    if not generate_excel and not generate_pdf:
        return build_invoice_page(error="Select at least one output format.")
    if generate_pdf and not invoice_logic._pdf_available():
        return build_invoice_page(
            error="PDF output was selected, but reportlab is not installed in this Python environment."
        )

    job_id = str(uuid.uuid4())
    created_at = datetime.now().astimezone()
    workdir = tempfile.mkdtemp(prefix=f"invoice_{job_id}_")
    try:
        csv_path = save_upload(
            csv_file,
            allowed_suffixes=ALLOWED_INVOICE_SUFFIXES,
            folder=workdir,
        )
    except Exception as exc:
        shutil.rmtree(workdir, ignore_errors=True)
        return build_invoice_page(error=str(exc))

    set_job(
        job_id,
        {
            "status": "running",
            "title": "Starting invoice generation",
            "summary": "Preparing uploaded CSV.",
            "current": 0,
            "total": 0,
            "log": "Preparing invoice job…",
            "log_lines": ["Preparing invoice job…"],
            "zip_path": None,
            "files": [],
            "workdir": workdir,
            "file_downloads": [],
            "zip_download_url": None,
            "started_at": iso_timestamp(created_at),
            "timer_started_at": None,
            "links_ready_at": None,
            "cache_mode": "Live API" if bypass_cache else "Cached API",
        },
    )

    def worker() -> None:
        try:
            outdir = Path(workdir) / "invoices"
            outdir.mkdir(parents=True, exist_ok=True)
            logo_path = find_pdf_logo_path() if generate_pdf else None
            update_job(
                job_id,
                timer_started_at=iso_timestamp(),
            )

            def on_status(message: str) -> None:
                append_job_log(job_id, message)

            def on_progress(done: int, total: int, label: str) -> None:
                if total <= 0:
                    append_job_log(job_id, label)
                    update_job(
                        job_id,
                        title=label,
                        summary="Preparing invoice data before file generation starts.",
                        current=0,
                        total=0,
                    )
                    return

                if done == 0:
                    append_job_log(job_id, label)
                    update_job(
                        job_id,
                        title=f"Ready to create {total} invoice(s)",
                        summary=f"Prepared invoice groups. About to start file generation for {total} invoice(s).",
                        current=0,
                        total=total,
                    )
                    return

                append_job_log(job_id, f"Completed {done} of {total}: {label}")
                update_job(
                    job_id,
                    title=f"{done} done out of {total}",
                    summary=f"Creating invoice {done} of {total}",
                    current=done,
                    total=total,
                )

            xlsx_created, pdf_created, df, generated_paths = (
                invoice_logic.generate_invoices(
                    csv_path=str(csv_path),
                    outdir=str(outdir),
                    nemo_base=nemo_base.rstrip("/"),
                    api_token=api_token,
                    generate_excel=generate_excel,
                    generate_pdf=generate_pdf,
                    logo_path=logo_path,
                    use_cache=not bypass_cache,
                    progress_callback=on_progress,
                    status_callback=on_status,
                )
            )

            zip_path = None
            zip_download_url = None
            if make_zip:
                append_job_log(job_id, "Creating ZIP archive")
                update_job(
                    job_id,
                    title="Creating ZIP file",
                    summary="Combining generated files into one archive.",
                )
                zip_path = create_invoice_zip(
                    str(outdir),
                    df,
                    remove_members=True,
                )
                if not zip_path:
                    raise ValueError("ZIP file creation failed.")

            result_lines = []
            if generate_excel:
                result_lines.append(f"Created {xlsx_created} Excel invoice(s).")
            if generate_pdf:
                result_lines.append(f"Created {pdf_created} PDF invoice(s).")
            if make_zip:
                result_lines.append("ZIP file is ready to download.")
            else:
                result_lines.append("Files are ready to download individually.")

            append_job_log(job_id, "Verifying temporary output files")
            append_job_log(
                job_id,
                f"Temporary output inputs: zip={'yes' if zip_path else 'no'}, generated_files={len(generated_paths)}",
            )
            resolved_zip_path, resolved_files = resolve_invoice_outputs_for_download(
                make_zip=make_zip,
                zip_path=zip_path,
                generated_paths=generated_paths,
            )
            links_ready_at = iso_timestamp()
            append_job_log(job_id, "Temporary output verified. Download links are ready.")

            file_downloads = []
            if not make_zip:
                file_downloads = [
                    {
                        "label": Path(path).name,
                        "url": f"/download/{job_id}/files/{index}",
                    }
                    for index, path in enumerate(resolved_files)
                ]
            if resolved_zip_path:
                zip_download_url = f"/download/{job_id}"

            update_job(
                job_id,
                status="completed",
                title="Invoice generation completed",
                summary="All requested files have been created.",
                current=(
                    max(int(job.get("total", 0)), int(job.get("current", 0)))
                    if (job := get_job(job_id))
                    else 0
                ),
                zip_path=resolved_zip_path,
                files=resolved_files,
                file_downloads=file_downloads,
                zip_download_url=zip_download_url,
                links_ready_at=links_ready_at,
            )
            for line in result_lines:
                append_job_log(job_id, line)
            append_invoice_run_log(
                job_id=job_id,
                started_at=created_at,
                finished_at=datetime.now(),
                status="success",
                summary="Invoice generation completed successfully.",
                workdir=workdir,
                generate_excel=generate_excel,
                generate_pdf=generate_pdf,
                make_zip=make_zip,
                xlsx_created=xlsx_created,
                pdf_created=pdf_created,
            )
        except Exception as exc:
            error_text = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            append_job_log(job_id, error_text)
            for line in traceback.format_exc().strip().splitlines():
                append_job_log(job_id, line)
            update_job(
                job_id,
                status="error",
                title="Invoice generation failed",
                summary="The job stopped before completion.",
            )
            append_invoice_run_log(
                job_id=job_id,
                started_at=created_at,
                finished_at=datetime.now(),
                status="failed",
                summary="Invoice generation failed.",
                workdir=workdir,
                generate_excel=generate_excel,
                generate_pdf=generate_pdf,
                make_zip=make_zip,
                error=error_text,
            )
            shutil.rmtree(workdir, ignore_errors=True)

    threading.Thread(target=worker, daemon=True).start()
    return redirect(url_for("invoice_job_page", job_id=job_id))


@app.post("/apps/excel-invoice-to-pdf/run")
def run_excel_invoice_to_pdf() -> str:
    invoice_excel = request.files.get("invoice_excel")

    if not invoice_logic._pdf_available():
        return build_excel_invoice_pdf_page(
            error="PDF output is unavailable because reportlab is not installed in this Python environment."
        )
    if not invoice_excel or not invoice_excel.filename:
        return build_excel_invoice_pdf_page(error="Choose an Excel invoice file to upload.")

    job_id = str(uuid.uuid4())
    created_at = datetime.now().astimezone()
    workdir = tempfile.mkdtemp(prefix=f"excel_invoice_pdf_{job_id}_")
    try:
        saved_path = save_upload(
            invoice_excel,
            allowed_suffixes=ALLOWED_EXCEL_INVOICE_SUFFIXES,
            folder=workdir,
        )
        output_dir = ensure_generated_invoices_dir() / created_at.strftime("%Y-%m-%d") / job_id
        output_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = convert_excel_invoice_to_pdf(
            saved_path,
            output_dir,
            logo_path=find_pdf_logo_path(),
        )
        metadata_path = output_dir / "metadata.json"
        metadata_path.write_text(
            json.dumps(
                {
                    "job_id": job_id,
                    "created_at": iso_timestamp(created_at),
                    "source_filename": invoice_excel.filename,
                    "output_file_paths": [pdf_path],
                    "selected_options": {"source": "edited_excel_invoice"},
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        set_job(
            job_id,
            {
                "status": "completed",
                "title": "Excel invoice PDF ready",
                "summary": "The edited Excel invoice was converted to PDF.",
                "current": 1,
                "total": 1,
                "log": "Excel invoice PDF generated.",
                "log_lines": ["Excel invoice PDF generated."],
                "zip_path": None,
                "files": [pdf_path],
                "workdir": str(output_dir),
                "file_downloads": [
                    {"label": Path(pdf_path).name, "url": f"/download/{job_id}/files/0"}
                ],
                "zip_download_url": None,
                "started_at": iso_timestamp(created_at),
                "timer_started_at": iso_timestamp(created_at),
                "links_ready_at": iso_timestamp(),
            },
        )
    except Exception as exc:
        shutil.rmtree(workdir, ignore_errors=True)
        return build_excel_invoice_pdf_page(error=str(exc))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    return build_excel_invoice_pdf_page(
        result=f"Created {Path(pdf_path).name}.",
        download_url=f"/download/{job_id}/files/0",
    )


@app.post("/apps/missed-reservation-report/run")
def run_missed_reservation_report() -> str:
    usage_csv = request.files.get("usage_csv")

    if not usage_csv or not usage_csv.filename:
        return build_missed_reservation_page(error="Choose a usage CSV to upload.")

    workdir = tempfile.mkdtemp(prefix="missed_reservations_")
    try:
        csv_path = save_upload(
            usage_csv,
            allowed_suffixes=ALLOWED_MISSED_RESERVATION_SUFFIXES,
            folder=workdir,
        )
        report, tool_report, total_missed_users = build_missed_reservation_reports(
            csv_path, threshold=5
        )
        return build_missed_reservation_page(
            report=report,
            tool_report=tool_report,
            total_missed_users=total_missed_users,
        )
    except Exception as exc:
        return build_missed_reservation_page(error=str(exc))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


@app.post("/apps/account-project-replacement/run")
def run_account_project_replacement() -> str:
    token = request.form.get("token", "").strip()
    old_number = request.form.get("old_number", "").strip()
    new_number = request.form.get("new_number", "").strip()
    dry_run = request.form.get("dry_run") == "on"

    try:
        result_lines = clone_account_project(
            token=token,
            old_number=old_number,
            new_number=new_number,
            dry_run=dry_run,
        )
    except Exception as exc:
        error_text = "".join(traceback.format_exception_only(type(exc), exc)).strip()
        return build_account_project_replacement_page(error=error_text)

    return build_account_project_replacement_page(result="\n".join(result_lines))


@app.get("/apps/jumbotron/data")
def jumbotron_data():
    try:
        report = get_jumbotron_report(get_jumbotron_token())
        payload = {
            "html": build_jumbotron_content(report),
            "signature": json.dumps(report, sort_keys=True),
            "generated_at": report.get("generated_at", ""),
        }
        return jsonify(payload)
    except Exception as exc:
        error_html = f'<div class="status error"><strong>Error</strong><pre>{html.escape(str(exc))}</pre></div>'
        return (
            jsonify(
                {
                    "html": error_html,
                    "signature": json.dumps({"error": str(exc)}, sort_keys=True),
                    "error": str(exc),
                }
            ),
            500,
        )


@app.get("/apps/nemo-invoice-generator/jobs/<job_id>")
def invoice_job_page(job_id: str) -> str:
    if not get_job(job_id):
        return (
            render_page(
                "Not Found", '<section class="panel"><h2>Job not found.</h2></section>'
            ),
            404,
        )
    return build_invoice_job_page(job_id)


@app.get("/apps/nemo-invoice-generator/jobs/<job_id>/status")
def invoice_job_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404

    status = str(job.get("status", "running"))
    status_class = "info"
    if status == "completed":
        status_class = "success"
    elif status == "error":
        status_class = "error"

    return jsonify(
        {
            "title": job.get("title", "Invoice job"),
            "summary": job.get("summary", ""),
            "current": job.get("current", 0),
            "total": job.get("total", 0),
            "log": "\n".join(job.get("log_lines", [])),
            "status": status,
            "status_class": status_class,
            "finished": status in {"completed", "error"},
            "zip_download_url": job.get("zip_download_url"),
            "file_downloads": job.get("file_downloads", []),
            "started_at": job.get("started_at", ""),
            "timer_started_at": job.get("timer_started_at", ""),
            "links_ready_at": job.get("links_ready_at", ""),
        }
    )


@app.get("/download/<job_id>")
def download(job_id: str):
    job = get_job(job_id)
    if not job:
        return (
            render_page(
                "Not Found", '<section class="panel"><h2>Job not found.</h2></section>'
            ),
            404,
        )
    zip_path = job.get("zip_path")
    if not zip_path:
        return (
            render_page(
                "Not Found",
                '<section class="panel"><h2>ZIP file not found.</h2></section>',
            ),
            404,
        )
    return send_file(
        zip_path, as_attachment=True, download_name=Path(str(zip_path)).name
    )


@app.get("/download/<job_id>/files/<int:file_index>")
def download_generated_file(job_id: str, file_index: int):
    job = get_job(job_id)
    if not job:
        return (
            render_page(
                "Not Found", '<section class="panel"><h2>Job not found.</h2></section>'
            ),
            404,
        )

    files = list(job.get("files", []))
    if file_index < 0 or file_index >= len(files):
        return (
            render_page(
                "Not Found",
                '<section class="panel"><h2>Generated file not found.</h2></section>',
            ),
            404,
        )

    file_path = str(files[file_index])
    return send_file(
        file_path,
        as_attachment=True,
        download_name=Path(file_path).name,
    )


def main() -> None:
    cleanup_old_generated_jobs()
    print(f"NEMO Tools Hub starting on https://127.0.0.1:{DEFAULT_PORT}")
    app.run(
        host="0.0.0.0",
        port=DEFAULT_PORT,
        debug=DEBUG_MODE,
        ssl_context="adhoc",
    )


if __name__ == "__main__":
    main()
