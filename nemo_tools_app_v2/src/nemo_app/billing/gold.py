from __future__ import annotations

import datetime as dt
import json
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

import pandas as pd

from .caps import billable_user_key
from .constants import INVOICE_APPLICATION_IDENTIFIERS
from .text import normalize_item, normalize_tool_key, parse_iso_datetime, parse_nemo_datetime

ANGSTROM_TOOL_NAMES = frozenset(
    {
        "Angstrom High Vacuum",
        "Angstrom Metals Deposition System",
    }
)
ANGSTROM_TOOL_KEYS = frozenset(normalize_tool_key(name) for name in ANGSTROM_TOOL_NAMES)
ANGSTROM_CHARGE_TYPES = frozenset({"tool_usage", "staff_charge"})
ANGSTROM_RUN_DATA_GROUPS = (
    "angstrom_metals_depositions",
    "angstrom_high_vacuum_depositions",
)
GOLD_MATERIAL = "Au"
GOLD_RATE_PER_NM = Decimal("1.10")
GOLD_RATE_LABEL = "$1.10/nm"


def gold_thickness_nm(run_data: object) -> Decimal:
    """Return the total valid, pure-gold thickness recorded in a usage event."""
    if not run_data:
        return Decimal("0")
    try:
        payload = json.loads(run_data) if isinstance(run_data, str) else run_data
    except (json.JSONDecodeError, TypeError):
        return Decimal("0")
    if not isinstance(payload, dict):
        return Decimal("0")

    entries: list[object] = []
    for group_name in ANGSTROM_RUN_DATA_GROUPS:
        group = payload.get(group_name)
        if not isinstance(group, dict):
            continue
        user_input = group.get("user_input")
        if isinstance(user_input, dict):
            entries.extend(user_input.values())
        elif isinstance(user_input, (list, tuple)):
            entries.extend(user_input)

    total = Decimal("0")
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or str(entry.get("deposited_material", "")).strip() != GOLD_MATERIAL
        ):
            continue
        try:
            thickness = Decimal(str(entry.get("deposited_thickness_nm", "")).strip())
        except (InvalidOperation, ValueError):
            continue
        if thickness.is_finite() and thickness > 0:
            total += thickness
    return total


def angstrom_tool_ids(tools_by_id: dict[int, str]) -> set[int]:
    return {
        tool_id
        for tool_id, name in tools_by_id.items()
        if normalize_tool_key(name) in ANGSTROM_TOOL_KEYS
    }


def angstrom_source_window(source: pd.DataFrame) -> tuple[dt.datetime, dt.datetime] | None:
    """Return a tight API query window around Angstrom rows in the uploaded CSV."""
    if source.empty or not {"Type", "Item", "Start time"}.issubset(source.columns):
        return None
    charge_type = source["Type"].astype(str).str.strip().str.lower()
    invoiceable_charge = charge_type.isin(ANGSTROM_CHARGE_TYPES)
    angstrom = source["Item"].apply(normalize_tool_key).isin(ANGSTROM_TOOL_KEYS)
    starts = (
        source.loc[invoiceable_charge & angstrom, "Start time"].apply(parse_nemo_datetime).dropna()
    )
    if starts.empty:
        return None
    return starts.min() - dt.timedelta(minutes=1), starts.max() + dt.timedelta(minutes=2)


def _project_lookup(projects_by_name: dict[str, dict[str, Any]]) -> dict[int, tuple[str, str]]:
    result: dict[int, tuple[str, str]] = {}
    for name, project in projects_by_name.items():
        project_id = project.get("id")
        application = str(project.get("application_identifier") or "").strip()
        if isinstance(project_id, int) and application in INVOICE_APPLICATION_IDENTIFIERS:
            result[project_id] = (str(name), application)
    return result


def _source_matches(source: pd.DataFrame) -> dict[tuple[str, str, dt.datetime], pd.Series]:
    matches: dict[tuple[str, str, dt.datetime], pd.Series] = {}
    for _, row in source.iterrows():
        if str(row.get("Type") or "").strip().lower() not in ANGSTROM_CHARGE_TYPES:
            continue
        tool_key = normalize_tool_key(row.get("Item"))
        start = parse_nemo_datetime(row.get("Start time"))
        project = str(row.get("Project") or "").strip()
        if tool_key in ANGSTROM_TOOL_KEYS and start and project:
            matches.setdefault((tool_key, project, start.replace(second=0, microsecond=0)), row)
    return matches


def add_gold_deposition_charges(
    prepared: pd.DataFrame,
    *,
    source: pd.DataFrame,
    usage_events: list[dict[str, Any]],
    tools_by_id: dict[int, str],
    projects_by_name: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """
    Add a Cleanroom consumable-style charge for matched Angstrom usage events.

    An API event must match an uploaded tool-usage or staff-charge row by tool, activating
    project, and start minute. This prevents a partial CSV export from billing
    unrelated Angstrom sessions returned by the API query.
    """
    if prepared.empty or not usage_events:
        return prepared.copy()
    project_by_id = _project_lookup(projects_by_name)
    source_by_key = _source_matches(source)
    additions: list[dict[str, Any]] = []
    seen_event_ids: set[object] = set()

    for event in usage_events:
        event_id = event.get("id")
        if event_id in seen_event_ids:
            continue
        seen_event_ids.add(event_id)
        tool_name = tools_by_id.get(event.get("tool"))
        tool_key = normalize_tool_key(tool_name)
        project = project_by_id.get(event.get("project"))
        start = parse_iso_datetime(event.get("start"))
        thickness = gold_thickness_nm(event.get("run_data"))
        if tool_key not in ANGSTROM_TOOL_KEYS or not project or not start or thickness <= 0:
            continue
        project_name, application = project
        source_row = source_by_key.get(
            (tool_key, project_name, start.replace(second=0, microsecond=0))
        )
        if source_row is None:
            continue

        cost = (thickness * GOLD_RATE_PER_NM).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        row = {column: None for column in prepared.columns}
        row.update(
            {
                "Type": "gold_deposition",
                "User": source_row.get("User", ""),
                "Username": source_row.get("Username", ""),
                "Item": f"Gold deposition (Au) - {normalize_item(tool_name)}",
                "Item_norm": f"Gold deposition (Au) - {normalize_item(tool_name)}",
                "Project": project_name,
                "Application identifier": application,
                "Start time": source_row.get("Start time"),
                "End time": source_row.get("End time"),
                "Start_dt": start,
                "End_dt": parse_iso_datetime(event.get("end")),
                "Rate": GOLD_RATE_LABEL,
                "Cost": float(cost),
                "Quantity": float(thickness),
                "IsConsumable": True,
                "IsMissedReservation": False,
                "IsStaffCharge": False,
                "IsToolUsageCharge": False,
                "Lab": "Cleanroom",
                "Period": start.strftime("%Y-%m"),
                "Subsidy": 0.0,
                "Usage Event ID": event_id,
            }
        )
        row["Billable User Key"] = billable_user_key(pd.Series(row))
        additions.append(row)

    if not additions:
        return prepared.copy()
    addition_frame = pd.DataFrame(additions)
    columns = [*prepared.columns, *addition_frame.columns.difference(prepared.columns)]
    result = pd.concat(
        [
            prepared.dropna(axis=1, how="all"),
            addition_frame.dropna(axis=1, how="all"),
        ],
        ignore_index=True,
        sort=False,
    )
    return result.reindex(columns=columns)
