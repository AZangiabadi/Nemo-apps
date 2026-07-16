from __future__ import annotations

import datetime as dt
import math
import re

import pandas as pd


def parse_nemo_datetime(value: object) -> dt.datetime | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    for format_string in ("%m/%d/%Y @ %I:%M %p", "%m/%d/%Y@%I:%M %p"):
        try:
            return dt.datetime.strptime(text, format_string)
        except ValueError:
            continue
    return None


def parse_iso_datetime(value: object) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).strip())
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def normalize_item(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return re.sub(r"\s*\((Individual|Group)\)\s*$", "", str(value).strip())


def normalize_tool_key(value: object) -> str:
    text = normalize_item(value).lower().replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "", text)


def parse_tool_id(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        numeric = float(value)
        return int(numeric) if numeric.is_integer() else None
    except (TypeError, ValueError):
        return None


def parse_hourly_rate(rate: object) -> float | None:
    match = re.search(
        r"\$?\s*(\d+(?:\.\d+)?)\s*/\s*hr\b",
        str(rate or "").strip(),
        flags=re.IGNORECASE,
    )
    return float(match.group(1)) if match else None


def parse_minimum_charge(rate: object) -> float | None:
    text = str(rate or "").strip()
    for pattern in (
        r"\$?\s*(\d+(?:\.\d+)?)\s*minimum",
        r"minimum(?:\s+charge)?[^$0-9]*\$?\s*(\d+(?:\.\d+)?)",
    ):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return float(match.group(1))
    return None


def period_for_datetime(value: dt.datetime | None) -> str:
    return value.strftime("%Y-%m") if value else "Unknown"


def month_label(period: str) -> str:
    try:
        year, month = period.split("-")
        return f"{dt.date(int(year), int(month), 1).strftime('%b').upper()}{year}"
    except (ValueError, TypeError):
        return period


def safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^\w\s,\-]+", "", (value or "").strip())
    return re.sub(r"\s{2,}", " ", cleaned).strip() or "UNKNOWN_PI"
