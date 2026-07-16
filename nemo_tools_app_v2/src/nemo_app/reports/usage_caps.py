from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from nemo_app.billing.caps import apply_max_session_charge_caps, apply_project_charge_caps
from nemo_app.billing.constants import ACCESS_FEE_BY_APPLICATION, PROJECT_CAP_BY_APPLICATION
from nemo_app.billing.invoice_model import InvoiceDocument
from nemo_app.billing.prepare import is_missed_reservation, is_staff_charge, prepare_usage_dataframe
from nemo_app.billing.text import normalize_item

from .excel import save_frames

MONEY_COLUMNS = {
    "No Cap Usage Charges",
    "Hourly-Capped Usage Charges",
    "Final Usage Charges",
    "Project-Only Usage Charges",
    "Access Fees",
    "Staff Charges Already Included",
    "Additional Staff-Application Charges",
    "No Cap Total Income",
    "Final Total Income",
    "Final Total Income Including Staff Application",
    "Hourly Cap Reduction",
    "Project Cap Reduction",
    "Total Cap Reduction",
    "Usage Before Project Cap",
    "Capped Billable Usage",
    "Project Cap Savings",
    "Original Cost",
    "Hourly-Capped Cost",
    "Reduction",
    "Cost",
}


@dataclass(frozen=True, slots=True)
class UsageCapResult:
    output_path: Path
    periods: tuple[str, ...]
    source_count: int
    row_count: int
    final_income: float
    final_income_with_staff: float
    hourly_reduction: float
    project_reduction: float


def _period(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(
        series.astype(str).str.replace(" @ ", " ", regex=False),
        format="%m/%d/%Y %I:%M %p",
        errors="coerce",
    )
    return parsed.dt.strftime("%Y-%m").fillna("Unknown")


def _prepare_sources(sources: list[tuple[Path, str]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    base_frames: list[pd.DataFrame] = []
    raw_frames: list[pd.DataFrame] = []
    for path, label in sources:
        raw = pd.read_csv(path)
        raw["Source File"] = label
        raw["Cost"] = pd.to_numeric(raw["Cost"], errors="coerce").fillna(0.0)
        raw["Period"] = _period(raw["Start time"])
        raw["Item_norm"] = raw["Item"].apply(normalize_item)
        raw["IsStaffCharge"] = raw.apply(is_staff_charge, axis=1)
        raw["IsMissedReservation"] = raw.apply(is_missed_reservation, axis=1)
        raw_frames.append(raw)

        base = prepare_usage_dataframe(raw, apply_caps=False)
        base["Source File"] = label
        base["PI_key"] = base["Project"].astype(str).str.split().str[-1]
        base_frames.append(base.loc[~base["IsMissedReservation"].fillna(False)])
    if not base_frames:
        raise ValueError("Choose at least one usage CSV.")
    return pd.concat(base_frames, ignore_index=True), pd.concat(raw_frames, ignore_index=True)


def _access_fees(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (period, pi_key), group in frame.groupby(["Period", "PI_key"], dropna=False):
        fee = InvoiceDocument.from_frame(
            group,
            pi_key=str(pi_key),
            pi_name=str(pi_key),
            pi_email="",
            period=str(period),
            invoice_number="analysis",
        ).access_fee
        if fee:
            rows.append({"Period": period, "PI/User Group": pi_key, "Access Fees": fee})
    return pd.DataFrame(rows, columns=["Period", "PI/User Group", "Access Fees"])


def _project_hits(frame: pd.DataFrame, *, prefix: str = "") -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    group_columns = ["Period", "Billable User Key", "Project", "Application identifier"]
    for (period, user_key, project, application), group in frame.groupby(
        group_columns, dropna=False
    ):
        cap = PROJECT_CAP_BY_APPLICATION.get(str(application))
        if cap is None:
            continue
        tools = group.loc[
            group["IsToolUsageCharge"].fillna(False) & ~group["IsStaffCharge"].fillna(False)
        ]
        original = float(tools["Cost"].fillna(0).sum())
        if original <= cap:
            continue
        top = tools.groupby("Item_norm")["Cost"].sum().sort_values(ascending=False).head(3)
        rows.append(
            {
                "Period": period,
                "User Key": user_key,
                "User": ", ".join(sorted(set(tools["User"].dropna().astype(str)))),
                "Project": project,
                "Application identifier": application,
                f"{prefix}Project Cap": cap,
                f"{prefix}Usage Before Project Cap": original,
                f"{prefix}Capped Billable Usage": cap,
                f"{prefix}Project Cap Savings": original - cap,
                "Tool Usage Rows": len(tools),
                "Top Instruments": ", ".join(top.index.astype(str)),
            }
        )
    return pd.DataFrame(rows)


def _staff_summary(raw: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    included = base.loc[base["IsStaffCharge"].fillna(False)].groupby("Period")["Cost"].sum()
    additional = (
        raw.loc[
            raw["Application identifier"].astype(str).str.strip().eq("Staff")
            & ~raw["IsMissedReservation"].fillna(False)
        ]
        .groupby("Period")["Cost"]
        .sum()
    )
    result = pd.concat(
        [
            included.rename("Staff Charges Already Included"),
            additional.rename("Additional Staff-Application Charges"),
        ],
        axis=1,
    ).fillna(0.0)
    result["Total Visible Staff-Related Charges"] = result.sum(axis=1)
    return result.reset_index()


def _staff_details(raw: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    additional = raw.loc[
        raw["Application identifier"].astype(str).str.strip().eq("Staff")
        & ~raw["IsMissedReservation"].fillna(False)
    ].copy()
    additional["Staff Bucket"] = "Additional Staff-Application Charges"

    included = base.loc[base["IsStaffCharge"].fillna(False)].copy()
    included["Staff Bucket"] = "Staff Charges Already Included"
    columns = [
        "Staff Bucket",
        "Period",
        "Source File",
        "Type",
        "User",
        "Username",
        "Item",
        "Application identifier",
        "Project",
        "Start time",
        "End time",
        "Quantity",
        "Rate",
        "Cost",
    ]
    combined = pd.concat(
        [included.reindex(columns=columns), additional.reindex(columns=columns)],
        ignore_index=True,
    )
    return combined.sort_values(
        ["Staff Bucket", "Period", "Cost"],
        ascending=[True, True, False],
        kind="stable",
    )


def _scenario_summary(
    base: pd.DataFrame,
    hourly: pd.DataFrame,
    final: pd.DataFrame,
    project_only: pd.DataFrame,
    staff: pd.DataFrame,
) -> pd.DataFrame:
    fee_by_period = _access_fees(base).groupby("Period")["Access Fees"].sum()
    staff_index = staff.set_index("Period")
    rows: list[dict[str, object]] = []
    for period in sorted(base["Period"].dropna().unique()):
        no_cap = float(base.loc[base["Period"].eq(period), "Cost"].sum())
        hourly_total = float(hourly.loc[hourly["Period"].eq(period), "Cost"].sum())
        final_total = float(final.loc[final["Period"].eq(period), "Cost"].sum())
        project_total = float(project_only.loc[project_only["Period"].eq(period), "Cost"].sum())
        fee = float(fee_by_period.get(period, 0.0))
        included = float(staff_index["Staff Charges Already Included"].get(period, 0.0))
        additional = float(staff_index["Additional Staff-Application Charges"].get(period, 0.0))
        rows.append(
            {
                "Period": period,
                "No Cap Usage Charges": no_cap,
                "Hourly-Capped Usage Charges": hourly_total,
                "Final Usage Charges": final_total,
                "Project-Only Usage Charges": project_total,
                "Access Fees": fee,
                "Staff Charges Already Included": included,
                "Additional Staff-Application Charges": additional,
                "No Cap Total Income": no_cap + fee,
                "Final Total Income": final_total + fee,
                "Final Total Income Including Staff Application": final_total + fee + additional,
                "Hourly Cap Reduction": no_cap - hourly_total,
                "Project Cap Reduction": hourly_total - final_total,
                "Total Cap Reduction": no_cap - final_total,
            }
        )
    summary = pd.DataFrame(rows)
    total = {"Period": "Total"}
    total.update(
        {column: summary[column].sum() for column in summary.columns if column != "Period"}
    )
    return pd.concat([summary, pd.DataFrame([total])], ignore_index=True)


def _hourly_details(hourly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    hits = hourly.loc[
        hourly.get("Original Cost", pd.Series(index=hourly.index, dtype=float)).notna()
    ].copy()
    if hits.empty:
        return pd.DataFrame(), pd.DataFrame()
    hits["Original Hours"] = hits["Original Quantity"] / 60.0
    hits["Billable Hours After Cap"] = hits["Quantity"] / 60.0
    hits["Reduction"] = hits["Original Cost"] - hits["Cost"]
    hits = hits.rename(columns={"Item_norm": "Instrument", "Cost": "Hourly-Capped Cost"})
    detail_columns = [
        "Period",
        "User",
        "Username",
        "Instrument",
        "Project",
        "Application identifier",
        "Start time",
        "End time",
        "Max Billable Hours",
        "Original Hours",
        "Billable Hours After Cap",
        "Original Cost",
        "Hourly-Capped Cost",
        "Reduction",
    ]
    details = hits[detail_columns].sort_values("Reduction", ascending=False)
    top = (
        hits.groupby(["User", "Username", "Instrument", "Application identifier"], dropna=False)
        .agg(
            **{
                "Capped Sessions": ("Reduction", "size"),
                "Original Hours": ("Original Hours", "sum"),
                "Billable Hours After Cap": ("Billable Hours After Cap", "sum"),
                "Original Cost": ("Original Cost", "sum"),
                "Hourly-Capped Cost": ("Hourly-Capped Cost", "sum"),
                "Reduction": ("Reduction", "sum"),
            }
        )
        .reset_index()
        .sort_values(["Reduction", "Capped Sessions"], ascending=False)
        .head(30)
    )
    return top, details


def build_usage_cap_report(sources: list[tuple[Path, str]], output_path: Path) -> UsageCapResult:
    base, raw = _prepare_sources(sources)
    if base.empty:
        raise ValueError("No invoice-compatible rows were found.")
    hourly = apply_max_session_charge_caps(base)
    final = apply_project_charge_caps(hourly)
    project_only = apply_project_charge_caps(base)
    staff = _staff_summary(raw, base)
    summary = _scenario_summary(base, hourly, final, project_only, staff)
    top_hourly, hourly_details = _hourly_details(hourly)
    project_hits = _project_hits(hourly)
    project_only_hits = _project_hits(base, prefix="No-Hourly ")
    lab_summary = pd.concat(
        [
            frame.groupby(["Period", "Lab"], dropna=False)["Cost"]
            .sum()
            .reset_index()
            .assign(Scenario=name)
            for name, frame in (
                ("No Caps", base),
                ("Hourly Cap Only", hourly),
                ("Hourly + Project Caps", final),
                ("Project Cap Only", project_only),
            )
        ],
        ignore_index=True,
    )[["Scenario", "Period", "Lab", "Cost"]]
    source_audit = (
        base.groupby(["Source File", "Period"], dropna=False)
        .agg(Rows=("Cost", "size"), Cost=("Cost", "sum"))
        .reset_index()
    )
    assumptions = pd.DataFrame(
        [["Source file", label, path.name] for path, label in sources]
        + [
            [
                "Application filter",
                "Local, CDG, Industry, External Academic",
                "Invoice-compatible rows",
            ],
            ["Excluded rows", "Missed reservations", "Removed before scenario totals"],
            ["Cap order", "Hourly/session caps, then project caps", "Matches invoice generation"],
            [
                "Access fee",
                "Highest configured application fee per PI/month",
                str(ACCESS_FEE_BY_APPLICATION),
            ],
        ],
        columns=["Topic", "Value", "Notes"],
    )
    money = MONEY_COLUMNS
    save_frames(
        output_path,
        [
            ("Executive Summary", summary, money, set()),
            ("Staff Charges", staff, money, set()),
            ("Staff Charge Detail", _staff_details(raw, base), money, set()),
            ("Monthly Lab Summary", lab_summary, {"Cost"}, set()),
            ("Top 30 Hourly Cap", top_hourly, money, set()),
            ("Hourly Cap Details", hourly_details, money, set()),
            ("Project Cap Hits", project_hits, money, set()),
            ("No Hourly Project Cap", project_only_hits, money, set()),
            ("Access Fees", _access_fees(base), money, set()),
            ("Source Period Audit", source_audit, {"Cost"}, set()),
            ("Assumptions", assumptions, set(), set()),
        ],
    )
    total = summary.loc[summary["Period"].eq("Total")].iloc[0]
    periods = tuple(value for value in summary["Period"].astype(str) if value != "Total")
    return UsageCapResult(
        output_path,
        periods,
        len(sources),
        len(base),
        float(total["Final Total Income"]),
        float(total["Final Total Income Including Staff Application"]),
        float(total["Hourly Cap Reduction"]),
        float(total["Project Cap Reduction"]),
    )
