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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_file,
    url_for,
)

import nemo_invoice_generator_with_pdf as invoice_logic
from nemo_invoice_generator_with_pdf import NEMO_BASE_URL, create_invoice_zip
from nemo_user_importer import run_import


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PORT = int(os.environ.get("PORT", "8000"))
ALLOWED_IMPORT_SUFFIXES = {".xlsx", ".csv"}
ALLOWED_INVOICE_SUFFIXES = {".csv"}
GENERATED_INVOICES_DIR = BASE_DIR / "generated_invoices"
INVOICE_RETENTION_DAYS = 14
JOBS: dict[str, dict[str, object]] = {}
JOBS_LOCK = threading.Lock()


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
        title="User Batch Import From Excel",
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
]


def register_app(definition: AppDefinition) -> None:
    APP_DEFINITIONS.append(definition)


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024


PAGE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title }}</title>
  <style>
    :root {
      --ink: #12233f;
      --paper: #f5f3ed;
      --panel: rgba(233, 241, 249, 0.34);
      --line: rgba(20, 35, 63, 0.12);
      --muted: #5e6b82;
      --hero-start: #0c3b60;
      --hero-mid: #165b78;
      --hero-end: #08304b;
      --gold: #d6b36a;
      --shadow: 0 24px 60px rgba(12, 29, 57, 0.18);
      --soft-shadow: 0 16px 34px rgba(12, 29, 57, 0.10);
      --radius: 28px;
    }
    * { box-sizing: border-box; }
    body {
      position: relative;
      margin: 0;
      font-family: "Palatino Linotype", "Book Antiqua", Georgia, serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(0, 94, 184, 0.08), transparent 28%),
        radial-gradient(circle at bottom right, rgba(214, 179, 106, 0.10), transparent 25%),
        var(--paper);
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      background-image: url("/assets/nemo-logo");
      background-repeat: no-repeat;
      background-position: center center;
      background-size: min(72vw, 980px);
      opacity: 0.05;
      pointer-events: none;
      z-index: 0;
    }
    a { color: inherit; }
    code, pre { font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace; }
    .shell {
      position: relative;
      z-index: 1;
      max-width: 1220px;
      margin: 0 auto;
      padding: 30px 20px 64px;
    }
    .hero {
      position: relative;
      overflow: hidden;
      background:
        linear-gradient(135deg, rgba(255, 255, 255, 0.06), transparent 28%),
        radial-gradient(circle at top right, rgba(214, 179, 106, 0.24), transparent 26%),
        linear-gradient(135deg, var(--hero-start) 0%, var(--hero-mid) 52%, var(--hero-end) 100%);
      color: white;
      border-radius: 36px;
      padding: 34px 38px 38px;
      box-shadow: var(--shadow);
      isolation: isolate;
      min-height: 420px;
    }
    .hero::after {
      content: "";
      position: absolute;
      inset: auto auto -120px -80px;
      width: 360px;
      height: 360px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(214, 179, 106, 0.18), transparent 68%);
      z-index: 0;
      pointer-events: none;
    }
    .hero-copy {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      position: relative;
      z-index: 1;
      max-width: 760px;
    }
    .brand-lockup {
      display: inline-flex;
      align-items: center;
      margin-bottom: 26px;
      padding: 16px 20px;
      border: 1px solid rgba(255, 255, 255, 0.14);
      border-radius: 24px;
      background: rgba(255, 255, 255, 0.08);
      backdrop-filter: blur(10px);
    }
    .hero-logo {
      max-width: 360px;
      width: min(360px, 52vw);
      height: auto;
      display: block;
      filter: drop-shadow(0 12px 24px rgba(0, 0, 0, 0.18));
    }
    .hero h1 {
      margin: 0 0 14px;
      font-size: clamp(3rem, 6vw, 5rem);
      line-height: 0.96;
      letter-spacing: -0.03em;
      text-wrap: balance;
    }
    .hero p {
      margin: 0;
      max-width: 720px;
      line-height: 1.62;
      font-size: clamp(1.08rem, 2vw, 1.42rem);
      color: rgba(255, 255, 255, 0.90);
    }
    .nav {
      margin: 18px 0 0;
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
    }
    .nav a, .button, button {
      display: inline-block;
      border: 0;
      text-decoration: none;
      cursor: pointer;
      border-radius: 999px;
      padding: 13px 20px;
      font-size: 1rem;
      transition: transform 160ms ease, background 160ms ease, box-shadow 160ms ease;
    }
    .nav a {
      background: rgba(255, 255, 255, 0.14);
      color: white;
      border: 1px solid rgba(255, 255, 255, 0.14);
      backdrop-filter: blur(10px);
    }
    .button, button {
      background: #13264b;
      color: white;
      box-shadow: var(--soft-shadow);
    }
    .button.secondary {
      background: transparent;
      color: var(--ink);
      border: 1px solid var(--line);
    }
    .nav a:hover, .button:hover, button:hover {
      transform: translateY(-1px);
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 26px;
      margin-top: 30px;
    }
    .card, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--soft-shadow);
      backdrop-filter: blur(30px);
      -webkit-backdrop-filter: blur(30px);
    }
    .card {
      background: linear-gradient(
        180deg,
        rgba(245, 249, 253, 0.30) 0%,
        rgba(218, 230, 242, 0.22) 100%
      );
      border: 1px solid rgba(255, 255, 255, 0.30);
      padding: 28px;
    }
    .card h2, .panel h2 {
      margin-top: 0;
      margin-bottom: 12px;
      font-size: clamp(2rem, 3vw, 2.8rem);
      line-height: 0.96;
      letter-spacing: -0.03em;
    }
    .eyebrow {
      display: inline-block;
      margin-bottom: 16px;
      padding: 8px 14px;
      border-radius: 999px;
      font-family: "Helvetica Neue", Arial, sans-serif;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      background: rgba(214, 179, 106, 0.20);
    }
    .panel {
      margin-top: 30px;
      padding: 30px;
    }
    form {
      display: grid;
      gap: 18px;
    }
    label {
      display: block;
      font-weight: 700;
      margin-bottom: 6px;
    }
    .help {
      margin-top: 4px;
      color: var(--muted);
      font-size: 0.93rem;
      line-height: 1.45;
    }
    input[type="text"],
    input[type="password"],
    input[type="file"] {
      width: 100%;
      padding: 13px 15px;
      border: 1px solid rgba(18, 35, 63, 0.16);
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.94);
      font: inherit;
    }
    input[type="checkbox"] {
      transform: translateY(1px);
      margin-right: 8px;
    }
    fieldset {
      margin: 0;
      padding: 16px 18px;
      border: 1px solid rgba(18, 35, 63, 0.12);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.42);
    }
    legend {
      padding: 0 8px;
      font-weight: 700;
    }
    .choice-row {
      display: flex;
      gap: 18px;
      flex-wrap: wrap;
    }
    .choice-row label {
      margin-bottom: 0;
      font-weight: 600;
    }
    .actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      align-items: center;
    }
    .status {
      margin-top: 22px;
      padding: 18px 20px;
      border-radius: 22px;
      white-space: pre-wrap;
      line-height: 1.5;
    }
    .status.success {
      background: #dff5ea;
      border: 1px solid #96d5b4;
    }
    .status.error {
      background: #fde8e2;
      border: 1px solid #efb6a8;
    }
    .status.info {
      background: #edf3ff;
      border: 1px solid #b8caef;
    }
    .progress-shell {
      margin-top: 14px;
      width: 100%;
      height: 14px;
      border-radius: 999px;
      overflow: hidden;
      background: rgba(18, 35, 63, 0.10);
    }
    .progress-bar {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, #1a8f6c, #55c38f);
      transition: width 220ms ease;
    }
    .download-list {
      margin-top: 14px;
      display: grid;
      gap: 10px;
    }
    .download-list a {
      text-decoration: none;
    }
    pre {
      margin: 10px 0 0;
      padding: 14px;
      overflow-x: auto;
      background: rgba(20, 33, 61, 0.05);
      border-radius: 14px;
      border: 1px solid rgba(20, 33, 61, 0.08);
      font-size: 0.92rem;
    }
    .job-log {
      max-height: 320px;
      overflow-y: auto;
      white-space: pre-wrap;
    }
    .footer-note {
      margin-top: 18px;
      color: var(--muted);
      line-height: 1.55;
    }
    @media (max-width: 700px) {
      .hero, .panel, .card { padding: 22px; }
      .hero-logo {
        max-width: 240px;
        width: min(240px, 78vw);
      }
      body::before {
        background-size: min(110vw, 720px);
        background-position: center top 180px;
      }
      .brand-lockup {
        padding: 12px 14px;
      }
      .hero h1 {
        font-size: clamp(2.4rem, 12vw, 3.5rem);
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-copy">
        <div class="brand-lockup">
          <img class="hero-logo" src="/assets/columbia-logo" alt="Columbia Nano Initiative logo">
        </div>
        <h1>NEMO Tools Hub</h1>
        <p>A shared Columbia Nano Initiative workspace for NEMO operations.</p>
        <div class="nav">
          <a href="/">Home</a>
          <a href="/apps/user-batch-import">User Batch Import</a>
          <a href="/apps/nemo-invoice-generator">Invoice Generator</a>
        </div>
      </div>
    </section>
    {{ body|safe }}
  </div>
</body>
</html>
"""


def render_page(title: str, body: str) -> str:
    return render_template_string(PAGE_TEMPLATE, title=title, body=body)


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


def find_logo_path() -> Optional[str]:
    for candidate in (
        "CNI_logo.png",
        "cni_logo.png",
        "CNI-logo.png",
        "cni-logo.png",
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


def update_job(job_id: str, **changes: object) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job.update(changes)


def get_job(job_id: str) -> Optional[dict[str, object]]:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None
        copied = dict(job)
        copied["files"] = list(job.get("files", []))
        return copied


def append_job_log(job_id: str, message: str) -> None:
    print(f"[job {job_id}] {message}", flush=True)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        lines = list(job.get("log_lines", []))
        lines.append(str(message))
        job["log_lines"] = lines
        job["log"] = "\n".join(job["log_lines"])


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

    persisted_zip_path: Optional[str] = None
    persisted_files: list[str] = []

    if make_zip:
        if not zip_path:
            raise ValueError("ZIP output was requested, but no ZIP file was created.")
        destination = job_output_dir / Path(zip_path).name
        shutil.move(zip_path, destination)
        persisted_zip_path = str(destination)
    else:
        for source_path in generated_paths:
            destination = job_output_dir / Path(source_path).name
            shutil.move(source_path, destination)
            persisted_files.append(str(destination))

    metadata = {
        "job_id": job_id,
        "created_at": created_at.isoformat(timespec="seconds"),
        "selected_options": selected_options,
        "output_file_paths": persisted_files,
        "zip_file_path": persisted_zip_path,
    }
    metadata_path = job_output_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    return job_output_dir, persisted_zip_path, persisted_files, metadata_path


@app.get("/assets/columbia-logo")
def columbia_logo():
    logo_path = find_logo_path()
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
            <article class="card">
              <div class="eyebrow" style="color:{html.escape(definition.accent)};">App</div>
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
    <section class="panel">
      <div class="eyebrow">App 1</div>
      <h2>User Batch Import From Excel</h2>
      <p>Upload an Excel or CSV file, provide your NEMO API token, and choose whether the run should stay in dry-run mode.</p>
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
        </div>
        <div>
          <label><input type="checkbox" name="dry_run" checked> Dry run only</label>
          <div class="help">Keep this checked if you want to preview changes before sending them to NEMO.</div>
        </div>
        <div class="actions">
          <button type="submit">Run Batch Import</button>
          <a class="button secondary" href="/">Back Home</a>
        </div>
      </form>
      {message}
    </section>
    """
    return render_page("User Batch Import", body)


def build_import_job_page(job_id: str) -> str:
    body = f"""
    <section class="panel">
      <div class="eyebrow">App 1</div>
      <h2>User Batch Import In Progress</h2>
      <p>The batch import is running in the background. This page updates automatically while NEMO accounts, projects, and users are processed.</p>
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
    <section class="panel">
      <div class="eyebrow">App 2</div>
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
        <div class="actions">
          <button type="submit">Generate Invoices</button>
          <a class="button secondary" href="/">Back Home</a>
        </div>
      </form>
      {message}
    </section>
    """
    return render_page("NEMO Invoice Generator", body)


def build_invoice_job_page(job_id: str) -> str:
    body = f"""
    <section class="panel">
      <div class="eyebrow">App 2</div>
      <h2>Invoice Generation In Progress</h2>
      <p>The job is running in the background. This page updates automatically while files are being created.</p>
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

      function renderDownloads(data) {{
        const links = [];
        if (data.zip_download_url) {{
          links.push(`<a class="button" href="${{data.zip_download_url}}">Download ZIP</a>`);
        }}
        if (Array.isArray(data.file_downloads)) {{
          for (const item of data.file_downloads) {{
            links.push(`<a class="button secondary" href="${{item.url}}">${{item.label}}</a>`);
          }}
        }}
        downloadsEl.innerHTML = links.join("");
      }}

      async function poll() {{
        const response = await fetch(`/apps/nemo-invoice-generator/jobs/${{jobId}}/status`);
        if (!response.ok) {{
          titleEl.textContent = "Job not found";
          summaryEl.textContent = "The job data is no longer available.";
          statusEl.className = "status error";
          return;
        }}

        const data = await response.json();
        const total = data.total || 0;
        const current = data.current || 0;
        const percent = total > 0 ? Math.round((current / total) * 100) : 0;

        titleEl.textContent = data.title || "Invoice Job";
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
        renderDownloads(data);

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
    return render_page("Invoice Job Status", body)


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

    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "running",
            "title": "Batch import running",
            "summary": "Preparing import job.",
            "current": 0,
            "total": 7,
            "log": "Preparing batch import job...",
            "log_lines": ["Preparing batch import job..."],
            "mode": "Dry Run" if dry_run else "Live Import",
            "started_at": datetime.now().isoformat(timespec="seconds"),
        }

    def worker() -> None:
        output = io.StringIO()

        def on_status(message: str) -> None:
            update_job(job_id, summary=message)
            append_job_log(job_id, message)

        def on_progress(done: int, total: int, label: str) -> None:
            update_job(job_id, current=done, total=total, summary=label)

        try:
            with redirect_stdout(output), redirect_stderr(output):
                print(
                    "Run started via web app.\n"
                    f"Uploaded file: {spreadsheet.filename}\n"
                    f"Mode: {'Dry Run' if dry_run else 'Live Import'}\n"
                )
                run_import(
                    str(saved_path),
                    token,
                    dry_run=dry_run,
                    status_callback=on_status,
                    progress_callback=on_progress,
                )

            combined_log = output.getvalue().strip()
            for line in combined_log.splitlines():
                if line.strip():
                    append_job_log(job_id, line)

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
            details = output.getvalue().strip()
            if details:
                for line in details.splitlines():
                    if line.strip():
                        append_job_log(job_id, line)
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

    cleanup_old_generated_jobs()
    job_id = str(uuid.uuid4())
    created_at = datetime.now()
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

    with JOBS_LOCK:
        JOBS[job_id] = {
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
            "job_output_dir": None,
            "metadata_path": None,
            "file_downloads": [],
            "zip_download_url": None,
            "started_at": created_at.isoformat(timespec="seconds"),
        }

    def worker() -> None:
        try:
            outdir = Path(workdir) / "invoices"
            outdir.mkdir(parents=True, exist_ok=True)
            logo_path = find_logo_path() if generate_pdf else None

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

            xlsx_created, pdf_created, df, generated_paths = invoice_logic.generate_invoices(
                csv_path=str(csv_path),
                outdir=str(outdir),
                nemo_base=nemo_base.rstrip("/"),
                api_token=api_token,
                generate_excel=generate_excel,
                generate_pdf=generate_pdf,
                logo_path=logo_path,
                progress_callback=on_progress,
                status_callback=on_status,
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

            append_job_log(job_id, "Moving finished output into persistent storage")
            selected_options = {
                "generate_excel": generate_excel,
                "generate_pdf": generate_pdf,
                "make_zip": make_zip,
            }
            job_output_dir, persisted_zip_path, persisted_files, metadata_path = (
                persist_invoice_outputs(
                    job_id=job_id,
                    created_at=created_at,
                    make_zip=make_zip,
                    zip_path=zip_path,
                    generated_paths=generated_paths,
                    selected_options=selected_options,
                )
            )

            file_downloads = []
            if not make_zip:
                file_downloads = [
                    {
                        "label": Path(path).name,
                        "url": f"/download/{job_id}/files/{index}",
                    }
                    for index, path in enumerate(persisted_files)
                ]
            if persisted_zip_path:
                zip_download_url = f"/download/{job_id}"

            update_job(
                job_id,
                status="completed",
                title="Invoice generation completed",
                summary="All requested files have been created.",
                current=max(int(job.get("total", 0)), int(job.get("current", 0)))
                if (job := get_job(job_id))
                else 0,
                zip_path=persisted_zip_path,
                files=persisted_files,
                file_downloads=file_downloads,
                zip_download_url=zip_download_url,
                job_output_dir=str(job_output_dir),
                metadata_path=str(metadata_path),
            )
            for line in result_lines:
                append_job_log(job_id, line)
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception as exc:
            error_text = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            append_job_log(job_id, error_text)
            update_job(
                job_id,
                status="error",
                title="Invoice generation failed",
                summary="The job stopped before completion.",
            )
            shutil.rmtree(workdir, ignore_errors=True)

    threading.Thread(target=worker, daemon=True).start()
    return redirect(url_for("invoice_job_page", job_id=job_id))


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
            "log": job.get("log", ""),
            "status": status,
            "status_class": status_class,
            "finished": status in {"completed", "error"},
            "zip_download_url": job.get("zip_download_url"),
            "file_downloads": job.get("file_downloads", []),
            "started_at": job.get("started_at", ""),
        }
    )


@app.get("/download/<job_id>")
def download(job_id: str):
    job = JOBS.get(job_id)
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
                "Not Found", '<section class="panel"><h2>ZIP file not found.</h2></section>'
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
                "Not Found", '<section class="panel"><h2>Generated file not found.</h2></section>'
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
    print(f"NEMO Tools Hub starting on http://127.0.0.1:{DEFAULT_PORT}")
    app.run(host="0.0.0.0", port=DEFAULT_PORT, debug=True)


if __name__ == "__main__":
    main()
