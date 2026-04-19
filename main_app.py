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

from dotenv import load_dotenv
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
from werkzeug.middleware.proxy_fix import ProxyFix

import nemo_invoice_generator_with_pdf as invoice_logic
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
    .table-wrap {
      overflow-x: auto;
      border-radius: 22px;
      border: 1px solid rgba(18, 35, 63, 0.10);
      background: rgba(255, 255, 255, 0.55);
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 14px 16px;
      text-align: left;
      vertical-align: top;
      border-bottom: 1px solid rgba(18, 35, 63, 0.08);
    }
    th {
      font-family: "Helvetica Neue", Arial, sans-serif;
      font-size: 0.84rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      background: rgba(237, 243, 255, 0.88);
    }
    tbody tr:last-child td {
      border-bottom: 0;
    }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 16px;
      margin-top: 22px;
      margin-bottom: 24px;
    }
    .stat-card {
      padding: 18px 20px;
      border-radius: 22px;
      background: rgba(255, 255, 255, 0.58);
      border: 1px solid rgba(18, 35, 63, 0.10);
      box-shadow: var(--soft-shadow);
    }
    .stat-label {
      margin: 0 0 8px;
      color: var(--muted);
      font-family: "Helvetica Neue", Arial, sans-serif;
      font-size: 0.82rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .stat-value {
      margin: 0;
      font-size: clamp(2rem, 4vw, 2.8rem);
      line-height: 1;
    }
    .section-stack {
      display: grid;
      gap: 24px;
      margin-top: 30px;
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
          <a href="/apps/jumbotron">Jumbotron</a>
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


def iso_timestamp(value: Optional[datetime] = None) -> str:
    current = value or datetime.now().astimezone()
    if current.tzinfo is None:
        current = current.astimezone()
    return current.isoformat(timespec="seconds")


def find_website_logo_path() -> Optional[str]:
    for candidate in (
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
          downloadsEl.innerHTML = "";
          return;
        }}
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
        try {{
          const response = await fetch(`/apps/nemo-invoice-generator/jobs/${{jobId}}/status`, {{
            cache: "no-store",
          }});
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
          barEl.style.width = `${{percent}}%`;
          statusEl.className = `status ${{data.status_class || "info"}}`;
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
    <section class="panel">
      <div class="eyebrow">Live Snapshot</div>
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

    template = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Jumbotron</title>
  <style>
    :root {
      --ink: #12233f;
      --paper: #f4f0e8;
      --panel: rgba(244, 248, 252, 0.82);
      --line: rgba(20, 35, 63, 0.12);
      --muted: #5e6b82;
      --hero-start: #0c3b60;
      --hero-mid: #165b78;
      --hero-end: #08304b;
      --shadow: 0 24px 60px rgba(12, 29, 57, 0.16);
      --soft-shadow: 0 16px 34px rgba(12, 29, 57, 0.10);
      --radius: 28px;
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: auto; }
    body {
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
      opacity: 0.04;
      pointer-events: none;
      z-index: 0;
    }
    .shell {
      position: relative;
      z-index: 1;
      max-width: 1380px;
      margin: 0 auto;
      padding: 28px 24px 48px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--soft-shadow);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      padding: 28px;
    }
    h2 {
      margin: 0 0 12px;
      font-size: clamp(2rem, 3vw, 2.8rem);
      line-height: 0.96;
      letter-spacing: -0.03em;
    }
    p {
      line-height: 1.55;
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
      color: #1d4ed8;
      background: rgba(29, 78, 216, 0.12);
    }
    .status {
      margin-top: 22px;
      padding: 18px 20px;
      border-radius: 22px;
      white-space: pre-wrap;
      line-height: 1.5;
    }
    .status.error {
      background: #fde8e2;
      border: 1px solid #efb6a8;
    }
    .status.info {
      background: #edf3ff;
      border: 1px solid #b8caef;
    }
    .table-wrap {
      overflow-x: auto;
      border-radius: 22px;
      border: 1px solid rgba(18, 35, 63, 0.10);
      background: rgba(255, 255, 255, 0.68);
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 16px 18px;
      text-align: left;
      vertical-align: top;
      border-bottom: 1px solid rgba(18, 35, 63, 0.08);
      font-size: 1.04rem;
    }
    th {
      position: sticky;
      top: 0;
      z-index: 1;
      font-family: "Helvetica Neue", Arial, sans-serif;
      font-size: 0.88rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      background: rgba(237, 243, 255, 0.96);
    }
    tbody tr:last-child td {
      border-bottom: 0;
    }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 16px;
      margin-top: 22px;
      margin-bottom: 24px;
    }
    .stat-card {
      padding: 18px 20px;
      border-radius: 22px;
      background: rgba(255, 255, 255, 0.62);
      border: 1px solid rgba(18, 35, 63, 0.10);
      box-shadow: var(--soft-shadow);
    }
    .stat-label {
      margin: 0 0 8px;
      color: var(--muted);
      font-family: "Helvetica Neue", Arial, sans-serif;
      font-size: 0.82rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .stat-value {
      margin: 0;
      font-size: clamp(2.2rem, 5vw, 3.2rem);
      line-height: 1;
    }
    .section-stack {
      display: grid;
      gap: 24px;
      margin-top: 24px;
    }
    @media (max-width: 700px) {
      .shell, .panel { padding: 18px; }
      th, td { padding: 12px 14px; font-size: 0.96rem; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <div id="jumbotron-content">{{ content|safe }}</div>
  </main>
  <script>
    const contentEl = document.getElementById("jumbotron-content");
    const refreshUrl = "/apps/jumbotron/data";
    const refreshMs = {{ refresh_ms }};
    const scrollStepPx = {{ scroll_step_px }};
    const scrollIntervalMs = {{ scroll_interval_ms }};
    let lastSignature = {{ initial_signature|tojson }};
    let scrollHandle = null;
    let pauseUntil = 0;

    function startAutoScroll() {
      if (scrollHandle !== null) window.clearInterval(scrollHandle);
      scrollHandle = window.setInterval(() => {
        if (Date.now() < pauseUntil) return;
        const maxScrollTop = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
        if (maxScrollTop <= 0) return;
        const nearBottom = window.scrollY >= maxScrollTop - scrollStepPx - 2;
        if (nearBottom) {
          pauseUntil = Date.now() + 2500;
          window.scrollTo({ top: 0, behavior: "smooth" });
          return;
        }
        window.scrollBy(0, scrollStepPx);
      }, scrollIntervalMs);
    }

    async function refreshIfChanged() {
      try {
        const response = await fetch(refreshUrl, { cache: "no-store" });
        if (!response.ok) return;
        const data = await response.json();
        if (data.signature !== lastSignature) {
          lastSignature = data.signature;
          contentEl.innerHTML = data.html;
          pauseUntil = Date.now() + 1200;
          window.scrollTo({ top: 0, behavior: "smooth" });
        }
      } catch (error) {
        console.error("Jumbotron refresh failed", error);
      }
    }

    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) {
        refreshIfChanged();
      }
    });

    startAutoScroll();
    window.setInterval(refreshIfChanged, refreshMs);
  </script>
</body>
</html>
    """
    signature = json.dumps(report or {"error": error}, sort_keys=True)
    return render_template_string(
        template,
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
            "log": "\n".join(job.get("log_lines", [])[-80:]),
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
