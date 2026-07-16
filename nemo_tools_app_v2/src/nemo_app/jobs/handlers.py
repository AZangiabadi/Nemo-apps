from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

from nemo_app.config import AppConfig
from nemo_app.imports.user_importer import run_import
from nemo_app.invoices.excel_parser import convert_excel_to_pdf
from nemo_app.invoices.service import InvoiceOptions, generate_invoices
from nemo_app.nemo.cache import JsonTTLCache
from nemo_app.nemo.client import NemoClient
from nemo_app.nemo.metadata import MetadataRepository
from nemo_app.replacements.service import replace_account_project
from nemo_app.reports.active_lab_users import build_active_lab_users_report
from nemo_app.reports.detailed_financials import build_detailed_financial_report
from nemo_app.reports.missed_reservations import build_missed_reservation_reports
from nemo_app.reports.usage_caps import build_usage_cap_report
from nemo_app.reports.user_pi import build_user_pi_report

from .models import Job
from .store import JobStore


def _result_data(
    result: object, *, omit: tuple[str, ...] = ("files", "output_path")
) -> dict[str, Any]:
    data = asdict(result)
    for field in omit:
        data.pop(field, None)
    return data


class JobContext:
    def __init__(self, config: AppConfig, store: JobStore, job: Job):
        self.config = config
        self.store = store
        self.job = job
        self.job_dir = config.jobs_dir / job.id
        self.output_dir = self.job_dir / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def input(self, name: str) -> Path:
        path = (self.job_dir / "input" / name).resolve()
        if not path.is_relative_to((self.job_dir / "input").resolve()) or not path.exists():
            raise FileNotFoundError(f"Job input {name!r} was not found")
        return path

    def log(self, message: str) -> None:
        self.store.append_log(self.job.id, message)

    def progress(self, current: int, total: int, message: str) -> None:
        self.store.progress(self.job.id, current, total, message)

    def client(self, *, dry_run: bool = False) -> NemoClient:
        token = self.job.secrets.get("api_token", "")
        if not token:
            raise ValueError("This job requires a NEMO API token")
        return NemoClient(token, base_url=self.config.api_base_url, dry_run=dry_run)

    def metadata(self) -> MetadataRepository:
        return MetadataRepository(
            self.client(),
            JsonTTLCache(self.config.cache_dir / "metadata", self.config.metadata_cache_seconds),
        )

    def files_result(self, files: list[Path], **data: Any) -> dict[str, Any]:
        return {
            "files": [str(path.relative_to(self.job_dir)) for path in files],
            "data": data,
        }


def _invoice(context: JobContext) -> tuple[str, dict[str, Any]]:
    options = InvoiceOptions(**context.job.payload["options"])
    result = generate_invoices(
        context.input(context.job.payload["input"]),
        context.output_dir,
        metadata=context.metadata(),
        options=options,
        logo_path=_asset(context.config, "Columbia_logo.png"),
        progress=context.progress,
    )
    return (
        f"Created {result.invoice_count} invoice(s)",
        context.files_result(list(result.files), **_result_data(result)),
    )


def _user_import(context: JobContext) -> tuple[str, dict[str, Any]]:
    dry_run = bool(context.job.payload.get("dry_run", True))
    result = run_import(
        context.input(context.job.payload["input"]),
        client=context.client(dry_run=dry_run),
        cache=JsonTTLCache(context.config.cache_dir / "imports", 300),
        use_cache=bool(context.job.payload.get("use_cache", True)),
        progress=context.progress,
        log=context.log,
    )
    return (
        "Dry run complete" if dry_run else "Import complete",
        {"files": [], "data": _result_data(result, omit=())},
    )


def _excel_pdf(context: JobContext) -> tuple[str, dict[str, Any]]:
    path = convert_excel_to_pdf(
        context.input(context.job.payload["input"]),
        context.output_dir,
        logo_path=_asset(context.config, "Columbia_logo.png"),
    )
    return "PDF created", context.files_result([path])


def _detailed(context: JobContext) -> tuple[str, dict[str, Any]]:
    path = context.output_dir / "detailed_financials.xlsx"
    result = build_detailed_financial_report(
        context.input(context.job.payload["input"]),
        path,
        metadata=context.metadata(),
        use_cache=bool(context.job.payload.get("use_cache", True)),
    )
    return "Detailed financial report ready", context.files_result([path], **_result_data(result))


def _usage_caps(context: JobContext) -> tuple[str, dict[str, Any]]:
    sources = [
        (context.input(item["stored_name"]), item["label"])
        for item in context.job.payload["inputs"]
    ]
    path = context.output_dir / "usage_cap_analysis.xlsx"
    result = build_usage_cap_report(sources, path)
    return "Usage cap report ready", context.files_result([path], **_result_data(result))


def _user_pi(context: JobContext) -> tuple[str, dict[str, Any]]:
    path = context.output_dir / "user_pi_report.xlsx"
    result = build_user_pi_report(
        context.input(context.job.payload["input"]),
        path,
        metadata=context.metadata(),
        use_cache=bool(context.job.payload.get("use_cache", True)),
    )
    return "User PI report ready", context.files_result([path], **_result_data(result))


def _active_users(context: JobContext) -> tuple[str, dict[str, Any]]:
    path = context.output_dir / "active_lab_users.xlsx"
    result = build_active_lab_users_report(path, client=context.client())
    return "Active lab users report ready", context.files_result([path], **_result_data(result))


def _missed(context: JobContext) -> tuple[str, dict[str, Any]]:
    users, tools, total = build_missed_reservation_reports(
        context.input(context.job.payload["input"]), threshold=5
    )
    data = {
        "users": users.to_dict("records"),
        "tools": tools.to_dict("records"),
        "total_users": total,
    }
    return "Missed reservation report ready", {"files": [], "data": data}


def _replacement(context: JobContext) -> tuple[str, dict[str, Any]]:
    result = replace_account_project(
        client=context.client(dry_run=bool(context.job.payload.get("dry_run", True))),
        old_value=context.job.payload["old_value"],
        target_value=context.job.payload["target_value"],
        mode=context.job.payload["mode"],
        deactivate_old=bool(context.job.payload.get("deactivate_old", True)),
    )
    return "Replacement complete", {"files": [], "data": _result_data(result, omit=())}


def _asset(config: AppConfig, filename: str) -> Path | None:
    path = config.asset_dir / filename
    return path if path.exists() else None


HANDLERS: dict[str, Callable[[JobContext], tuple[str, dict[str, Any]]]] = {
    "invoice": _invoice,
    "user_import": _user_import,
    "excel_pdf": _excel_pdf,
    "detailed_financials": _detailed,
    "usage_caps": _usage_caps,
    "user_pi": _user_pi,
    "active_users": _active_users,
    "missed_reservations": _missed,
    "replacement": _replacement,
}


def run_job(config: AppConfig, store: JobStore, job: Job) -> None:
    handler = HANDLERS.get(job.kind)
    if not handler:
        raise ValueError(f"Unknown job type {job.kind!r}")
    context = JobContext(config, store, job)
    context.log(f"Starting {job.title}")
    summary, result = handler(context)
    # Verify every declared output before marking the job complete.
    for relative in result.get("files", []):
        path = (context.job_dir / relative).resolve()
        if not path.is_relative_to(context.job_dir.resolve()) or not path.is_file():
            raise FileNotFoundError(f"Expected output {relative!r} was not created")
    store.complete(job.id, summary=summary, result=result)
