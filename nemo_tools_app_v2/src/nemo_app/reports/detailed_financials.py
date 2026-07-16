from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from nemo_app.billing.prepare import load_usage_csv
from nemo_app.nemo.metadata import MetadataRepository, project_pi

from .excel import save_frames


@dataclass(frozen=True, slots=True)
class DetailedFinancialResult:
    output_path: Path
    row_count: int
    member_count: int
    project_count: int
    total_after_caps: float


def _lookup_users(users: list[dict]) -> dict[str, str]:
    result: dict[str, str] = {}
    for user in users:
        email = str(user.get("email") or "").strip()
        if not email:
            continue
        candidates = [user.get("username"), user.get("name")]
        candidates.append(f"{user.get('first_name', '')} {user.get('last_name', '')}")
        for value in candidates:
            key = " ".join(str(value or "").lower().split())
            if key:
                result.setdefault(key, email)
    return result


def _before_caps(row: pd.Series) -> float:
    for name in ("Original Cost", "Original Project Cost", "Cost"):
        value = pd.to_numeric(pd.Series([row.get(name)]), errors="coerce").iloc[0]
        if pd.notna(value):
            return float(value)
    return 0.0


def build_detailed_financial_report(
    csv_path: Path,
    output_path: Path,
    *,
    metadata: MetadataRepository,
    use_cache: bool,
) -> DetailedFinancialResult:
    projects = metadata.projects(use_cache=use_cache)
    tools = metadata.tools(use_cache=use_cache)
    adjustments = metadata.adjustments(use_cache=use_cache)
    consumables = metadata.consumable_labs(use_cache=use_cache)
    user_lookup = _lookup_users(metadata.users(use_cache=use_cache))
    frame = load_usage_csv(
        csv_path,
        project_map=projects,
        tools_by_id=tools,
        adjustment_requests=adjustments,
        consumable_labs=consumables,
        filter_applications=False,
    )
    records: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        project = str(row.get("Project") or "").strip()
        pi = project_pi(project, projects)
        member = str(row.get("Email") or row.get("User email") or "").strip()
        if not member:
            key = " ".join(str(row.get("Username") or row.get("User") or "").lower().split())
            member = user_lookup.get(key, str(row.get("Username") or row.get("User") or "").strip())
        amount = row.get("Original Quantity")
        if pd.isna(amount) if amount is not None else True:
            amount = row.get("Quantity", 0)
        charge_type = (
            "Missed reservation"
            if row.get("IsMissedReservation")
            else "Staff charge"
            if row.get("IsStaffCharge")
            else "Consumable"
            if row.get("IsConsumable")
            else "Tools usage"
        )
        records.append(
            {
                "Item": str(row.get("Item") or row.get("Item_norm") or ""),
                "Date & Time": row.get("Start_dt"),
                "Member Name": str(row.get("User") or ""),
                "Member": member,
                "Project": project,
                "PI Email": pi.email,
                "Project Type": str(row.get("Application identifier") or ""),
                "Charge Type": charge_type,
                "Amount": float(amount or 0),
                "Cost Before Caps": _before_caps(row),
                "Cost After Caps": float(row.get("Cost") or 0),
            }
        )
    details = pd.DataFrame(records).sort_values(
        ["Item", "Date & Time", "Member Name", "Project"], kind="stable"
    )
    if details.empty:
        raise ValueError("No financial rows could be created.")
    summary = pd.DataFrame(
        [
            {"Metric": "Rows", "Value": len(details)},
            {"Metric": "Unique Members", "Value": details["Member"].nunique()},
            {"Metric": "Unique Projects", "Value": details["Project"].nunique()},
            {"Metric": "Total Cost Before Caps", "Value": details["Cost Before Caps"].sum()},
            {"Metric": "Total Cost After Caps", "Value": details["Cost After Caps"].sum()},
            {
                "Metric": "Cap Savings",
                "Value": details["Cost Before Caps"].sum() - details["Cost After Caps"].sum(),
            },
        ]
    )
    aggregations = {
        "Rows": ("Member", "size"),
        "Members": ("Member", "nunique"),
        "Projects": ("Project", "nunique"),
        "Amount": ("Amount", "sum"),
        "Cost Before Caps": ("Cost Before Caps", "sum"),
        "Cost After Caps": ("Cost After Caps", "sum"),
    }
    by_project = details.groupby("Project Type", dropna=False).agg(**aggregations).reset_index()
    by_charge = details.groupby("Charge Type", dropna=False).agg(**aggregations).reset_index()
    money_columns = {"Cost Before Caps", "Cost After Caps"}
    save_frames(
        output_path,
        [
            ("Detailed Financials", details, money_columns, {"Date & Time"}),
            ("Summary", summary, set(), set()),
            ("By Project Type", by_project, money_columns, set()),
            ("By Charge Type", by_charge, money_columns, set()),
        ],
    )
    return DetailedFinancialResult(
        output_path,
        len(details),
        details["Member"].nunique(),
        details["Project"].nunique(),
        float(details["Cost After Caps"].sum()),
    )
