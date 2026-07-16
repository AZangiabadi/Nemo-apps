from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from .adjustments import apply_adjustment_requests
from .caps import (
    apply_forced_caps_when_hourly_caps_ignored,
    apply_max_session_charge_caps,
    apply_project_charge_caps,
    billable_user_key,
)
from .constants import INVOICE_APPLICATION_IDENTIFIERS, TOOL_TO_LAB
from .text import normalize_item, parse_minimum_charge, parse_nemo_datetime, period_for_datetime

REQUIRED_USAGE_COLUMNS = {
    "Type",
    "User",
    "Item",
    "Project",
    "Application identifier",
    "Start time",
    "Rate",
    "Cost",
    "Quantity",
}


def is_consumable_type(value: object) -> bool:
    return "consum" in str(value or "").strip().lower()


def is_missed_reservation(row: pd.Series) -> bool:
    for column in ("Type", "Item", "Description", "Name", "Details"):
        if column not in row.index:
            continue
        text = str(row.get(column) or "").strip().lower()
        if re.search(r"missed\s+reservation", text) or ("missed" in text and "reservation" in text):
            return True
    return False


def is_staff_charge(row: pd.Series) -> bool:
    return (
        normalize_item(row.get("Item")).lower() == "staff time"
        or str(row.get("Type") or "").strip().lower() == "staff_charge"
    )


def _classify_usage(
    frame: pd.DataFrame, consumable_labs: dict[str, str] | None = None
) -> pd.DataFrame:
    result = frame.copy()
    result["Start_dt"] = result["Start time"].apply(parse_nemo_datetime)
    result["End_dt"] = result.get("End time", pd.Series(None, index=result.index)).apply(
        parse_nemo_datetime
    )
    result["Item_norm"] = result["Item"].apply(normalize_item)
    result["IsConsumable"] = result["Type"].apply(is_consumable_type)
    result["IsMissedReservation"] = result.apply(is_missed_reservation, axis=1)
    result["IsStaffCharge"] = result.apply(is_staff_charge, axis=1)
    result["IsToolUsageCharge"] = ~result["IsConsumable"] & ~result["IsMissedReservation"]
    result["Lab"] = result["Item_norm"].map(TOOL_TO_LAB)
    if consumable_labs:
        result["Lab"] = result["Lab"].fillna(result["Item_norm"].map(consumable_labs))
    result["Lab"] = result["Lab"].fillna("Consumable")
    result.loc[result["IsConsumable"], "Lab"] = "Consumable"
    return associate_staff_time_labs(result)


def associate_staff_time_labs(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    staff = result["Item_norm"].str.lower().eq("staff time")
    result.loc[staff, "Lab"] = "Staff time"
    candidates = (
        ~staff
        & result["IsToolUsageCharge"].fillna(False)
        & result["Start_dt"].notna()
        & result["End_dt"].notna()
    )
    matches: dict[tuple[object, object, object], str] = {}
    for _, row in result.loc[candidates].sort_index(kind="stable").iterrows():
        key = (row["User"], row["Project"], row["Start_dt"])
        matches.setdefault(key, str(row.get("Lab") or ""))
    for index, row in result.loc[staff].iterrows():
        lab = matches.get((row["User"], row["Project"], row["Start_dt"]))
        if lab:
            result.at[index, "Lab"] = lab
    return result


def filter_invoice_quantity_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    quantity = pd.to_numeric(frame["Quantity"], errors="coerce")
    short_usage = (
        frame["Type"].astype(str).str.strip().str.lower().eq("tool_usage")
        & quantity.notna()
        & quantity.le(1)
        & ~frame["Item_norm"].astype(str).str.lower().eq("litho hood 2")
    )
    return frame.loc[~short_usage].copy()


def prepare_usage_dataframe(
    source: pd.DataFrame,
    *,
    consumable_labs: dict[str, str] | None = None,
    tools_by_id: dict[int, str] | None = None,
    project_map: dict[str, dict[str, Any]] | None = None,
    adjustment_requests: list[dict[str, Any]] | None = None,
    filter_applications: bool = True,
    filter_invoice_quantities: bool = False,
    apply_hourly_caps: bool = True,
    apply_caps: bool = True,
) -> pd.DataFrame:
    missing = REQUIRED_USAGE_COLUMNS - set(source.columns)
    if missing:
        raise ValueError(f"CSV missing expected columns: {sorted(missing)}")

    result = source.copy()
    result["Application identifier"] = result["Application identifier"].astype(str).str.strip()
    if filter_applications:
        result = result[
            result["Application identifier"].isin(INVOICE_APPLICATION_IDENTIFIERS)
        ].copy()
    result["Cost"] = pd.to_numeric(result["Cost"], errors="coerce").fillna(0.0)
    result["Quantity"] = pd.to_numeric(result["Quantity"], errors="coerce")
    result = _classify_usage(result, consumable_labs)

    if tools_by_id and adjustment_requests:
        result = apply_adjustment_requests(
            result,
            adjustment_requests,
            tools_by_id=tools_by_id,
            projects_by_name=project_map or {},
        )
        result = _classify_usage(result, consumable_labs)

    if filter_invoice_quantities:
        result = filter_invoice_quantity_rows(result)
    result["Period"] = result["Start_dt"].apply(period_for_datetime)
    result["Billable User Key"] = result.apply(billable_user_key, axis=1)

    if apply_caps:
        result = (
            apply_max_session_charge_caps(result)
            if apply_hourly_caps
            else apply_forced_caps_when_hourly_caps_ignored(result)
        )
        result = apply_project_charge_caps(result)

    result["Subsidy"] = 0.0
    cdg = result["Application identifier"].str.upper().eq("CDG")
    result.loc[cdg, "Subsidy"] = result.loc[cdg, "Cost"] / 9.0
    if cdg.any():
        minimum = result.loc[cdg, "Rate"].apply(parse_minimum_charge)
        at_minimum = minimum.notna() & (result.loc[cdg, "Cost"].sub(minimum).abs() < 0.005)
        result.loc[result.loc[cdg].index[at_minimum], "Subsidy"] = 0.0
    return result.reset_index(drop=True)


def load_usage_csv(path: str | Path, **options: Any) -> pd.DataFrame:
    return prepare_usage_dataframe(pd.read_csv(path), **options)


def sort_detail_rows(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["_staff_order"] = result["Item_norm"].str.lower().eq("staff time").astype(int)
    result["_source_order"] = range(len(result))
    return result.sort_values(
        [
            "Start_dt",
            "User",
            "Project",
            "_staff_order",
            "End_dt",
            "Item_norm",
            "_source_order",
        ],
        kind="stable",
    ).drop(columns=["_staff_order", "_source_order"])
