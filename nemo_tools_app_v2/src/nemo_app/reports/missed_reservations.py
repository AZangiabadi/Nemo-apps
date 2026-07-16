from __future__ import annotations

from pathlib import Path

import pandas as pd

from nemo_app.billing.prepare import is_missed_reservation


def build_missed_reservation_reports(
    csv_path: Path, *, threshold: int = 5, tool_limit: int = 10
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    frame = pd.read_csv(csv_path)
    if "User" not in frame.columns:
        raise ValueError("CSV must include a User column.")
    missed = frame.loc[frame.apply(is_missed_reservation, axis=1)].copy()
    user_columns = ["User", "Username", "Missed Reservations", "Top Missed Tools"]
    if missed.empty:
        return (
            pd.DataFrame(columns=user_columns),
            pd.DataFrame(columns=["Tool", "Missed Reservations"]),
            0,
        )
    missed["User"] = missed["User"].fillna("").astype(str).str.strip()
    missed = missed.loc[missed["User"].ne("")]
    if "Username" not in missed.columns:
        missed["Username"] = ""
    else:
        missed["Username"] = missed["Username"].fillna("").astype(str).str.strip()
    tool_column = next(
        (name for name in ("Tool", "Item", "Description", "Name") if name in missed.columns),
        None,
    )
    missed["Tool"] = missed[tool_column].fillna("").astype(str).str.strip() if tool_column else ""
    counts = (
        missed.groupby(["User", "Username"], dropna=False)
        .size()
        .reset_index(name="Missed Reservations")
    )
    top_by_user: dict[tuple[str, str], str] = {}
    for key, group in missed.loc[missed["Tool"].ne("")].groupby(["User", "Username"], dropna=False):
        tool_counts = group["Tool"].value_counts().rename_axis("Tool").reset_index(name="Count")
        top_by_user[key] = ", ".join(
            f"{row.Tool} ({int(row.Count)})" for row in tool_counts.head(3).itertuples()
        )
    counts["Top Missed Tools"] = [
        top_by_user.get((row.User, row.Username), "") for row in counts.itertuples()
    ]
    total_users = len(counts)
    user_report = counts.loc[counts["Missed Reservations"].ge(threshold), user_columns].sort_values(
        ["Missed Reservations", "User"], ascending=[False, True], kind="stable"
    )
    tool_report = (
        missed.loc[missed["Tool"].ne(""), "Tool"]
        .value_counts()
        .head(tool_limit)
        .rename_axis("Tool")
        .reset_index(name="Missed Reservations")
    )
    return user_report.reset_index(drop=True), tool_report, total_users
