from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

from .text import (
    normalize_tool_key,
    parse_hourly_rate,
    parse_iso_datetime,
    parse_minimum_charge,
    parse_nemo_datetime,
    parse_tool_id,
)


def _matching_datetime(value: dt.datetime | None) -> dt.datetime | None:
    return value.replace(second=0, microsecond=0, tzinfo=None) if value else None


def _adjusted_cost(rate: object, quantity_minutes: float, original_cost: float) -> float:
    hourly_rate = parse_hourly_rate(rate)
    if hourly_rate is None:
        return original_cost
    cost = hourly_rate * quantity_minutes / 60.0
    minimum = parse_minimum_charge(rate)
    return round(max(cost, minimum or 0.0), 2)


def apply_adjustment_requests(
    frame: pd.DataFrame,
    adjustments: list[dict[str, Any]],
    *,
    tools_by_id: dict[int, str],
    projects_by_name: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Apply approved NEMO adjustment requests to matching usage-export rows."""
    if frame.empty or not adjustments or not tools_by_id:
        return frame.copy()

    project_names: dict[int, str] = {}
    project_applications: dict[int, str] = {}
    for project in projects_by_name.values():
        project_id = project.get("id")
        if not isinstance(project_id, int):
            continue
        project_names[project_id] = str(project.get("name") or "").strip()
        application = str(
            project.get("application_identifier") or project.get("account_type") or ""
        ).strip()
        if application:
            project_applications[project_id] = application

    result = frame.copy()
    result["_source_index"] = result.index
    result["_tool_key"] = result["Item"].apply(normalize_tool_key)
    result["_start_match"] = (
        result["Start time"].apply(parse_nemo_datetime).apply(_matching_datetime)
    )
    result["_end_match"] = (
        result.get("End time", pd.Series(None, index=result.index))
        .apply(parse_nemo_datetime)
        .apply(_matching_datetime)
    )

    used_sources: set[object] = set()
    rows_to_drop: set[object] = set()
    for adjustment in adjustments:
        if adjustment.get("status") != 1 or adjustment.get("deleted"):
            continue
        tool_id = parse_tool_id(adjustment.get("item_tool"))
        tool_name = tools_by_id.get(tool_id) if tool_id is not None else None
        original_start = _matching_datetime(parse_iso_datetime(adjustment.get("original_start")))
        original_end = _matching_datetime(parse_iso_datetime(adjustment.get("original_end")))
        if not tool_name or not original_start or not original_end:
            continue

        matches = result[
            result["_tool_key"].eq(normalize_tool_key(tool_name))
            & result["_start_match"].eq(original_start)
            & result["_end_match"].eq(original_end)
            & ~result["_source_index"].isin(used_sources)
        ]
        if matches.empty:
            continue
        if len(matches) > 1:
            original_project_id = parse_tool_id(adjustment.get("original_project"))
            project_name = project_names.get(original_project_id or -1)
            if project_name:
                narrowed = matches[matches["Project"].eq(project_name)]
                if not narrowed.empty:
                    matches = narrowed
            matches = matches.sort_values("_source_index", kind="stable").iloc[:1]

        index = matches.index[0]
        used_sources.add(matches.iloc[0]["_source_index"])
        new_start = parse_iso_datetime(adjustment.get("new_start"))
        new_end = parse_iso_datetime(adjustment.get("new_end"))
        if adjustment.get("waive") or (new_start is not None and new_start == new_end):
            rows_to_drop.add(index)
            continue
        if new_start and new_end and new_end >= new_start:
            minutes = (new_end - new_start).total_seconds() / 60.0
            result.at[index, "Start time"] = new_start.strftime("%m/%d/%Y @ %I:%M %p")
            result.at[index, "End time"] = new_end.strftime("%m/%d/%Y @ %I:%M %p")
            result.at[index, "Quantity"] = minutes
            result.at[index, "Cost"] = _adjusted_cost(
                result.at[index, "Rate"], minutes, float(result.at[index, "Cost"] or 0)
            )

        new_project_id = parse_tool_id(adjustment.get("new_project"))
        if new_project_id is not None:
            if project_names.get(new_project_id):
                result.at[index, "Project"] = project_names[new_project_id]
            if project_applications.get(new_project_id):
                result.at[index, "Application identifier"] = project_applications[new_project_id]

    if rows_to_drop:
        result = result.drop(index=list(rows_to_drop))
    return result.drop(
        columns=["_source_index", "_tool_key", "_start_match", "_end_match"],
        errors="ignore",
    ).reset_index(drop=True)
