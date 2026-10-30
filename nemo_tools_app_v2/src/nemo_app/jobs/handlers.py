from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

import requests

from nemo_app.angstrom.client import AngstromLogClient
from nemo_app.config import AppConfig
from nemo_app.imports.qualification_importer import run_qualification_import
from nemo_app.imports.user_importer import run_import
from nemo_app.invoices.customization import InvoiceCustomization
from nemo_app.invoices.excel_parser import convert_excel_to_pdf
from nemo_app.invoices.service import (
    InvoiceOptions,
    NoInvoiceableRows,
    calculate_invoices,
    generate_invoices,
)
from nemo_app.mailchimp import MailchimpClient
from nemo_app.nemo.billing import fetch_billing_dataframe
from nemo_app.nemo.cache import JsonTTLCache
from nemo_app.nemo.client import NemoClient
from nemo_app.nemo.metadata import MetadataRepository
from nemo_app.newsletter import active_contacts, compare_newsletter
from nemo_app.replacements.service import replace_account_project
from nemo_app.reports.active_lab_users import build_active_lab_users_report
from nemo_app.reports.detailed_financials import build_detailed_financial_report
from nemo_app.reports.missed_reservations import build_missed_reservation_reports
from nemo_app.reports.revenue import summarize_revenue
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

    def read_only_client(self) -> NemoClient:
        token = self.config.read_only_api_token or self.job.secrets.get("api_token", "")
        if not token:
            raise ValueError("This job requires the server read-only NEMO API token")
        return NemoClient(token, base_url=self.config.api_base_url, read_only=True)

    def metadata(self) -> MetadataRepository:
        return MetadataRepository(
            self.read_only_client(),
            JsonTTLCache(self.config.cache_dir / "metadata", self.config.metadata_cache_seconds),
        )

    def files_result(self, files: list[Path], **data: Any) -> dict[str, Any]:
        return {
            "files": [str(path.relative_to(self.job_dir)) for path in files],
            "data": data,
        }


def _invoice(context: JobContext) -> tuple[str, dict[str, Any]]:
    options = InvoiceOptions(**context.job.payload["options"])
    customization = InvoiceCustomization.from_mapping(context.job.payload.get("customization"))
    logo_input = context.job.payload.get("logo_input")
    logo_path = (
        context.input(str(logo_input))
        if logo_input
        else _asset(context.config, "Columbia_logo.png")
    )
    gold_log_client = (
        AngstromLogClient(context.config.angstrom_log_app_url)
        if context.config.angstrom_log_app_url
        else None
    )
    if context.job.payload.get("source") == "api":
        period = context.job.payload["billing_month"]
        context.log(f"Fetching {period} billing data from NEMO; no-charge projects excluded")
        source = fetch_billing_dataframe(
            context.read_only_client(), period, timezone=context.config.timezone
        )
        context.log(f"Loaded {len(source)} billing records")
        source.to_csv(context.output_dir / "billing_source.csv", index=False)
    else:
        source = context.input(context.job.payload["input"])
    result = generate_invoices(
        source,
        context.output_dir,
        metadata=context.metadata(),
        options=options,
        logo_path=logo_path,
        progress=context.progress,
        timezone=context.config.timezone,
        gold_log_client=gold_log_client,
        customization=customization,
    )
    result_files = list(result.files)
    if context.job.payload.get("source") == "api":
        result_files.append(context.output_dir / "billing_source.csv")
    return (
        f"Created {result.invoice_count} invoice(s)",
        context.files_result(result_files, **_result_data(result)),
    )


def _revenue(context: JobContext) -> tuple[str, dict[str, Any]]:
    period = context.job.payload["billing_month"]
    context.progress(0, 3, "Fetching monthly billing")
    source = fetch_billing_dataframe(
        context.read_only_client(),
        period,
        timezone=context.config.timezone,
        allow_empty=True,
    )
    documents = ()
    hourly_caps = bool(context.job.payload.get("apply_hourly_caps", False))
    if not source.empty:
        context.progress(1, 3, "Calculating final invoices")
        try:
            calculation = calculate_invoices(
                source,
                metadata=context.metadata(),
                options=InvoiceOptions(apply_hourly_caps=hourly_caps, use_cache=False),
                timezone=context.config.timezone,
                gold_log_client=(
                    AngstromLogClient(context.config.angstrom_log_app_url)
                    if context.config.angstrom_log_app_url
                    else None
                ),
            )
            documents = calculation.documents
        except NoInvoiceableRows:
            pass
    report = summarize_revenue(documents, period)
    report["apply_hourly_caps"] = hourly_caps
    report["generated_at"] = dt.datetime.now(context.config.timezone).isoformat()
    context.progress(3, 3, "Revenue analysis complete")
    return "Revenue analysis complete", {"files": [], "data": report}


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


def _qualification_import(context: JobContext) -> tuple[str, dict[str, Any]]:
    dry_run = bool(context.job.payload.get("dry_run", True))
    result = run_qualification_import(
        context.input(context.job.payload["input"]),
        client=context.client(dry_run=dry_run),
        progress=context.progress,
        log=context.log,
    )
    action = "Dry run complete" if dry_run else "Qualification import complete"
    return action, {"files": [], "data": _result_data(result, omit=())}


def _excel_pdf(context: JobContext) -> tuple[str, dict[str, Any]]:
    path = convert_excel_to_pdf(
        context.input(context.job.payload["input"]),
        context.output_dir,
        logo_path=_asset(context.config, "Columbia_logo.png"),
    )
    return "PDF created", context.files_result([path])


def _report_source(context: JobContext) -> tuple[Path, list[Path]]:
    if context.job.payload.get("source") != "api":
        return context.input(context.job.payload["input"]), []
    period = context.job.payload["billing_month"]
    context.log(f"Fetching {period} report data from NEMO")
    frame = fetch_billing_dataframe(
        context.read_only_client(),
        period,
        timezone=context.config.timezone,
        allow_empty=context.job.kind == "missed_reservations",
        include_no_charge_projects=context.job.kind == "missed_reservations",
    )
    path = context.output_dir / "billing_source.csv"
    frame.to_csv(path, index=False)
    context.log(f"Loaded {len(frame)} billing records")
    return path, [path]


def _detailed(context: JobContext) -> tuple[str, dict[str, Any]]:
    source, audit_files = _report_source(context)
    path = context.output_dir / "detailed_financials.xlsx"
    result = build_detailed_financial_report(
        source,
        path,
        metadata=context.metadata(),
        use_cache=bool(context.job.payload.get("use_cache", True)),
    )
    return "Detailed financial report ready", context.files_result(
        [path, *audit_files], **_result_data(result)
    )


def _usage_caps(context: JobContext) -> tuple[str, dict[str, Any]]:
    audit_files = []
    if context.job.payload.get("source") == "api":
        source, audit_files = _report_source(context)
        sources = [(source, f"NEMO {context.job.payload['billing_month']}")]
    else:
        sources = [
            (context.input(item["stored_name"]), item["label"])
            for item in context.job.payload["inputs"]
        ]
    path = context.output_dir / "usage_cap_analysis.xlsx"
    result = build_usage_cap_report(
        sources,
        path,
        metadata=context.metadata() if context.job.payload.get("source") == "api" else None,
        use_cache=bool(context.job.payload.get("use_cache", True)),
    )
    return "Usage cap report ready", context.files_result(
        [path, *audit_files], **_result_data(result)
    )


def _user_pi(context: JobContext) -> tuple[str, dict[str, Any]]:
    source, audit_files = _report_source(context)
    path = context.output_dir / "user_pi_report.xlsx"
    result = build_user_pi_report(
        source,
        path,
        metadata=context.metadata(),
        use_cache=bool(context.job.payload.get("use_cache", True)),
    )
    return "User PI report ready", context.files_result(
        [path, *audit_files], **_result_data(result)
    )


def _active_users(context: JobContext) -> tuple[str, dict[str, Any]]:
    path = context.output_dir / "active_lab_users.xlsx"
    result = build_active_lab_users_report(path, client=context.read_only_client())
    return "Active lab users report ready", context.files_result([path], **_result_data(result))


def _missed(context: JobContext) -> tuple[str, dict[str, Any]]:
    source, audit_files = _report_source(context)
    users, tools, total = build_missed_reservation_reports(source, threshold=5)
    data = {
        "users": users.to_dict("records"),
        "tools": tools.to_dict("records"),
        "total_users": total,
    }
    return "Missed reservation report ready", context.files_result(audit_files, **data)


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


def _newsletter_compare(context: JobContext) -> tuple[str, dict[str, Any]]:
    mailchimp = MailchimpClient(
        context.job.secrets["mailchimp_key"], context.job.payload["audience_id"]
    )
    client = context.read_only_client()
    users = client.fetch_all("users/")
    context.log("Reading tool-use history for newsletter activity columns")
    try:
        events = client.fetch_all("usage_events/?fields=user,start,end&limit=1000")
    except (requests.RequestException, ValueError):
        events = None
        context.log("Tool history could not be read. Last tool use will show Unavailable.")
    result = compare_newsletter(
        users, mailchimp, usage_events=events, timezone=context.config.timezone
    )
    context.log("Comparison complete. No contacts were added and no emails were sent.")
    return "Comparison ready — review missing contacts", {"files": [], "data": result}


def _newsletter_add(context: JobContext) -> tuple[str, dict[str, Any]]:
    payload = context.job.payload
    if (
        not payload.get("consent_confirmed")
        or not payload.get("consent_note")
        or payload.get("subscription_status")
        not in {
            "pending",
            "subscribed",
        }
    ):
        raise ValueError("Newsletter permission and a valid subscription action are required.")
    mailchimp = MailchimpClient(context.job.secrets["mailchimp_key"], payload["audience_id"])
    if payload["subscription_status"] == "subscribed" and mailchimp.audience_info().get(
        "double_optin"
    ):
        raise ValueError(
            "This audience requires confirmation. Run a new comparison and use pending."
        )
    current, _invalid = active_contacts(context.read_only_client().fetch_all("users/"))
    active = {contact["email"] for contact in current}
    outcomes = []
    for number, contact in enumerate(payload["contacts"], 1):
        outcome = (
            mailchimp.add_new(contact, payload["subscription_status"])
            if contact["email"] in active
            else "skipped_inactive"
        )
        outcomes.append({"email": contact["email"], "result": outcome})
        context.log(f"{contact['email']}: {outcome}")
        context.progress(number, len(payload["contacts"]), "Processing approved contacts")
    return "Newsletter additions processed", {
        "files": [],
        "data": {
            "outcomes": outcomes,
            "consent_source": payload["consent_note"],
            "approved_at": payload["approved_at"],
        },
    }


HANDLERS: dict[str, Callable[[JobContext], tuple[str, dict[str, Any]]]] = {
    "revenue": _revenue,
    "newsletter_compare": _newsletter_compare,
    "newsletter_add": _newsletter_add,
    "invoice": _invoice,
    "user_import": _user_import,
    "qualification_import": _qualification_import,
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
