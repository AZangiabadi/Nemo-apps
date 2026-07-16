from __future__ import annotations

import math

import pandas as pd

from .constants import (
    FORCED_MAX_HOURS_WITHOUT_HOURLY_CAPS,
    PROJECT_CAP_BY_APPLICATION,
    TOOL_MAX_HOURS_ALIASES,
    TOOL_MAX_HOURS_BY_NAME,
    TOOL_MAX_HOURS_BY_TOOL_ID,
)
from .text import normalize_tool_key, parse_tool_id


def billable_user_key(row: pd.Series) -> str:
    username = str(row.get("Username") or "").strip()
    return username or str(row.get("User") or "").strip()


def resolve_max_billable_hours(row: pd.Series) -> float | None:
    for column in ("Tool ID", "Tool Id", "ToolID", "Tool"):
        if column not in row.index:
            continue
        tool_id = parse_tool_id(row[column])
        if tool_id in TOOL_MAX_HOURS_BY_TOOL_ID:
            return TOOL_MAX_HOURS_BY_TOOL_ID[tool_id]
    key = normalize_tool_key(row.get("Item"))
    return TOOL_MAX_HOURS_BY_NAME.get(TOOL_MAX_HOURS_ALIASES.get(key, key))


def _apply_session_caps(frame: pd.DataFrame, maximum_hours: pd.Series) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    maximum_hours = pd.to_numeric(maximum_hours, errors="coerce")
    quantity = pd.to_numeric(result["Quantity"], errors="coerce")
    consumable = result.get("IsConsumable", pd.Series(False, index=result.index)).fillna(False)
    capped = (
        (~consumable) & maximum_hours.notna() & quantity.notna() & (quantity / 60.0 > maximum_hours)
    )
    if not capped.any():
        return result

    old_quantity = quantity.loc[capped].astype(float)
    old_cost = pd.to_numeric(result.loc[capped, "Cost"], errors="coerce").fillna(0.0)
    new_quantity = maximum_hours.loc[capped].astype(float) * 60.0
    result.loc[capped, "Original Quantity"] = old_quantity
    result.loc[capped, "Original Cost"] = old_cost
    result.loc[capped, "Quantity"] = new_quantity
    new_cost = old_cost.copy()
    positive = old_quantity > 0
    new_cost.loc[positive] = (
        old_cost.loc[positive] * new_quantity.loc[positive] / old_quantity.loc[positive]
    )
    result.loc[capped, "Cost"] = new_cost.round(2)
    return result


def apply_max_session_charge_caps(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result["Max Billable Hours"] = result.apply(resolve_max_billable_hours, axis=1)
    return _apply_session_caps(result, result["Max Billable Hours"])


def apply_forced_caps_when_hourly_caps_ignored(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    keys = (
        result["Item"]
        .apply(normalize_tool_key)
        .map(lambda key: TOOL_MAX_HOURS_ALIASES.get(key, key))
    )
    forced = keys.map(FORCED_MAX_HOURS_WITHOUT_HOURLY_CAPS)
    result["Max Billable Hours"] = pd.NA
    result.loc[forced.notna(), "Max Billable Hours"] = forced.loc[forced.notna()]
    return _apply_session_caps(result, forced)


def scale_costs_to_target(costs: pd.Series, target_total: float) -> pd.Series:
    """Scale positive costs to an exact cent target using largest remainders."""
    if costs.empty:
        return costs.copy()
    numeric = pd.to_numeric(costs, errors="coerce").fillna(0.0).astype(float)
    positive = numeric > 0
    result = pd.Series(0.0, index=numeric.index, dtype=float)
    if not positive.any():
        return result
    positive_costs = numeric.loc[positive]
    total = float(positive_costs.sum())
    if total <= 0:
        return result
    target_cents = max(0, int(round(target_total * 100)))
    raw_cents = positive_costs * target_cents / total
    floor_cents = raw_cents.apply(math.floor).astype(int)
    missing_cents = target_cents - int(floor_cents.sum())
    if missing_cents:
        for index in (raw_cents - floor_cents).sort_values(ascending=False).index[:missing_cents]:
            floor_cents.loc[index] += 1
    result.loc[positive] = floor_cents / 100.0
    return result


def apply_project_charge_caps(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "Period" not in frame.columns:
        return frame.copy()
    result = frame.copy()
    if "Billable User Key" not in result.columns:
        result["Billable User Key"] = result.apply(billable_user_key, axis=1)

    changed: list[object] = []
    group_columns = [
        "Period",
        "Billable User Key",
        "Project",
        "Application identifier",
    ]
    for (_, _, _, application), indexes in result.groupby(
        group_columns, dropna=False
    ).groups.items():
        cap = PROJECT_CAP_BY_APPLICATION.get(str(application))
        if cap is None:
            continue
        group_index = pd.Index(indexes)
        billable_index = group_index[
            result.loc[group_index, "IsToolUsageCharge"].fillna(False).astype(bool)
            & ~result.loc[group_index, "IsStaffCharge"].fillna(False).astype(bool)
        ]
        costs = pd.to_numeric(result.loc[billable_index, "Cost"], errors="coerce").fillna(0.0)
        if costs.empty or float(costs.sum()) <= cap:
            continue
        scaled = scale_costs_to_target(costs, cap)
        capped = costs > scaled
        capped_indexes = billable_index[capped]
        if capped_indexes.empty:
            continue
        changed.extend(capped_indexes.tolist())
        result.loc[capped_indexes, "Original Project Cost"] = costs.loc[capped]
        result.loc[capped_indexes, "Project Cap Applied"] = cap
        result.loc[capped_indexes, "Cost"] = scaled.loc[capped]
    if changed:
        result.loc[changed, "Project Cap Reduction"] = (
            result.loc[changed, "Original Project Cost"] - result.loc[changed, "Cost"]
        ).round(2)
    return result
