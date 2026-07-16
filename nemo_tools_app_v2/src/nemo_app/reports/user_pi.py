from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from nemo_app.billing.prepare import load_usage_csv
from nemo_app.nemo.metadata import MetadataRepository, project_pi

from .excel import save_frames


@dataclass(frozen=True, slots=True)
class UserPIReportResult:
    output_path: Path
    row_count: int
    user_count: int
    project_count: int
    pi_count: int


def build_user_pi_report(
    csv_path: Path,
    output_path: Path,
    *,
    metadata: MetadataRepository,
    use_cache: bool,
) -> UserPIReportResult:
    projects = metadata.projects(use_cache=use_cache)
    frame = load_usage_csv(csv_path, project_map=projects)
    frame = frame.loc[~frame["IsMissedReservation"].fillna(False)].copy()
    if frame.empty:
        raise ValueError("No billing rows remain after filtering the CSV.")
    records: list[dict[str, object]] = []
    totals: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        project = str(row.get("Project") or "").strip()
        if not project:
            continue
        pi = project_pi(project, projects)
        user = str(row.get("User") or row.get("Username") or "").strip()
        record = {
            "User": user,
            "Username": str(row.get("Username") or "").strip(),
            "Project Number": project,
            "Project Type": str(row.get("Application identifier") or "").strip(),
            "PI Name": pi.display_name,
            "PI Email": pi.email,
            "Billing Period": str(row.get("Period") or ""),
        }
        records.append(record)
        totals.append(
            {
                "PI Name": pi.display_name,
                "PI Email": pi.email,
                "Project Number": project,
                "Project Type": record["Project Type"],
                "Total Amount": float(row.get("Cost") or 0),
            }
        )
    report = (
        pd.DataFrame(records)
        .drop_duplicates()
        .sort_values(["PI Name", "Project Number", "User", "Billing Period"], kind="stable")
    )
    project_totals = (
        pd.DataFrame(totals)
        .groupby(
            ["PI Name", "PI Email", "Project Number", "Project Type"], as_index=False, dropna=False
        )["Total Amount"]
        .sum()
        .sort_values(["PI Name", "Project Number"], kind="stable")
    )
    summary = pd.DataFrame(
        [
            {"Metric": "Rows", "Value": len(report)},
            {"Metric": "Unique Users", "Value": report["User"].nunique()},
            {"Metric": "Unique Projects", "Value": report["Project Number"].nunique()},
            {"Metric": "Unique PIs", "Value": report["PI Name"].nunique()},
        ]
    )
    save_frames(
        output_path,
        [
            ("Users and PIs", report, set(), set()),
            ("PI Project Totals", project_totals, {"Total Amount"}, set()),
            ("Summary", summary, set(), set()),
        ],
    )
    return UserPIReportResult(
        output_path,
        len(report),
        report["User"].nunique(),
        report["Project Number"].nunique(),
        report["PI Name"].nunique(),
    )
