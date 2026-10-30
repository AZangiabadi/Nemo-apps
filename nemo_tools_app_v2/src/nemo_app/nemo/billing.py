from __future__ import annotations

import calendar
import datetime as dt
import math
import re
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests

from .client import NemoClient


def billing_month_dates(period: str) -> tuple[dt.date, dt.date]:
    if not re.fullmatch(r"\d{4}-\d{2}", period):
        raise ValueError("Choose a billing month and year.")
    try:
        start = dt.date.fromisoformat(period + "-01")
    except ValueError as exc:
        raise ValueError("Choose a valid billing month and year.") from exc
    return start, start.replace(day=calendar.monthrange(start.year, start.month)[1])


def _timestamp(value: object, timezone: dt.tzinfo) -> str:
    if not value:
        return ""
    parsed = dt.datetime.fromisoformat(str(value))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone)
    return parsed.strftime("%m/%d/%Y @ %I:%M %p")


def billing_records_dataframe(
    records: list[dict[str, Any]], *, timezone: dt.tzinfo
) -> pd.DataFrame:
    """Adapt NEMO's calculated billing data to the existing invoice input schema."""
    columns = [
        "Type",
        "User",
        "Username",
        "Item",
        "Account",
        "Project",
        "Application identifier",
        "Start time",
        "End time",
        "Quantity",
        "Note",
        "Rate",
        "Cost",
        "Waived",
    ]
    rows = []
    required = {
        "item_type",
        "user_fullname",
        "name",
        "project",
        "application",
        "start",
        "end",
        "quantity",
        "rate",
        "amount",
    }
    for record in records:
        if required - record.keys():
            raise ValueError("NEMO billing data is missing required invoice fields.")
        cost = float(record["amount"] or 0)
        quantity = float(record["quantity"] or 0)
        if not math.isfinite(cost) or not math.isfinite(quantity):
            raise ValueError("NEMO returned an invalid billing amount or quantity.")
        rows.append(
            [
                record["item_type"],
                record["user_fullname"],
                record.get("user", ""),
                record["name"],
                record.get("account", ""),
                record["project"],
                record["application"],
                _timestamp(record["start"], timezone),
                _timestamp(record["end"], timezone),
                quantity,
                record.get("note", ""),
                record["rate"],
                cost,
                record.get("waived", False),
            ]
        )
    return pd.DataFrame(rows, columns=columns)


def fetch_billing_dataframe(
    client: NemoClient,
    period: str,
    *,
    timezone: dt.tzinfo,
    allow_empty: bool = False,
    include_no_charge_projects: bool = False,
) -> pd.DataFrame:
    start, end = billing_month_dates(period)
    query = urlencode(
        {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "include_no_charge_projects": str(include_no_charge_projects).lower(),
        }
    )
    try:
        records = client.fetch_all(f"billing/billing_data/?{query}")
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 403:
            raise ValueError("The NEMO token requires the use_billing_api permission.") from exc
        if status == 400:
            raise ValueError(
                "NEMO could not calculate this month's billing data. Check its billing "
                "rates and project configuration, or use CSV upload."
            ) from exc
        raise
    if not records and not allow_empty:
        raise ValueError("NEMO returned no billable records for the selected month.")
    return billing_records_dataframe(records, timezone=timezone)
