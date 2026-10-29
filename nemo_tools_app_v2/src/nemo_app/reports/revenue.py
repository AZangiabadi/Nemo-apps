from __future__ import annotations

import datetime as dt
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import pandas as pd

from nemo_app.billing.constants import INVOICE_APPLICATION_IDENTIFIERS
from nemo_app.billing.invoice_model import InvoiceDocument
from nemo_app.nemo.billing import billing_month_dates


def _cents(value: object) -> int:
    return int((Decimal(str(value)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def summarize_revenue(documents: tuple[InvoiceDocument, ...], period: str) -> dict[str, Any]:
    start, end = billing_month_dates(period)
    days = {
        (start + dt.timedelta(days=index)).isoformat(): {"charges": 0, "fees": 0, "gold": 0}
        for index in range(end.day)
    }
    applications = dict.fromkeys(INVOICE_APPLICATION_IDENTIFIERS, 0)
    pis: dict[str, dict[str, Any]] = {}
    total = gold = fees = invoice_count = 0
    projects: set[str] = set()
    for document in documents:
        if document.period != period:
            continue
        invoice_count += 1
        amount = _cents(document.invoice_total)
        total += amount
        fee = _cents(document.access_fee)
        fees += fee
        days[end.isoformat()]["fees"] += fee
        document_gold = 0
        line_total = 0
        for _, row in document.lines.iterrows():
            cost = _cents(row["Cost"])
            line_total += cost
            timestamp = row.get("End_dt")
            if pd.isna(timestamp) or timestamp is None:
                timestamp = row.get("Start_dt")
            if pd.notna(timestamp) and timestamp.tzinfo is not None:
                timestamp = timestamp.astimezone(document.generated_at.tzinfo)
            date = timestamp.date().isoformat() if pd.notna(timestamp) else end.isoformat()
            # Monthly invoice allocation wins over raw timestamps for generated charges.
            date = date if date in days else end.isoformat()
            days[date]["charges"] += cost
            if str(row.get("Type")).lower() == "gold_deposition":
                days[date]["gold"] += cost
                document_gold += cost
        days[end.isoformat()]["charges"] += _cents(document.usage_total) - line_total
        gold += document_gold
        grouped: dict[str, int] = defaultdict(int)
        for project in document.projects:
            projects.add(project.project)
            grouped[project.application] += _cents(project.total)
        if grouped:
            grouped[max(grouped, key=grouped.get)] += amount - sum(grouped.values())
        for application, value in grouped.items():
            applications[application] = applications.get(application, 0) + value
        pi = pis.setdefault(
            document.pi_key,
            {
                "name": document.pi_name,
                "email": document.pi_email,
                "total": 0,
                "gold": 0,
                "projects": set(),
            },
        )
        pi["total"] += amount
        pi["gold"] += document_gold
        pi["projects"].update(project.project for project in document.projects)
    return {
        "period": period,
        "label": start.strftime("%B %Y"),
        "total": total / 100,
        "gold": gold / 100,
        "fees": fees / 100,
        "other": (total - gold - fees) / 100,
        "invoice_count": invoice_count,
        "project_count": len(projects),
        "pi_count": len(pis),
        "daily": [
            {
                "date": date,
                **{key: value / 100 for key, value in values.items()},
                "total": (values["charges"] + values["fees"]) / 100,
            }
            for date, values in days.items()
        ],
        "applications": [
            {"name": name, "total": value / 100} for name, value in applications.items()
        ],
        "pis": [
            {
                "name": pi["name"],
                "email": pi["email"],
                "total": pi["total"] / 100,
                "gold": pi["gold"] / 100,
                "projects": len(pi["projects"]),
            }
            for pi in sorted(pis.values(), key=lambda pi: (-pi["total"], pi["name"]))
        ],
    }
