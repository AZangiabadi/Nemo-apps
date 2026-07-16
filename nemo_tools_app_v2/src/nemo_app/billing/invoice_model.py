from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd

from .constants import ACCESS_FEE_BY_APPLICATION, DESIRED_LAB_ORDER
from .prepare import sort_detail_rows


@dataclass(frozen=True, slots=True)
class PIInfo:
    key: str
    display_name: str
    email: str = ""


@dataclass(frozen=True, slots=True)
class ProjectSummary:
    project: str
    application: str
    lab_totals: dict[str, float]
    staff_time: float
    access_fee: float

    @property
    def usage_total(self) -> float:
        return sum(self.lab_totals.values()) + self.staff_time

    @property
    def total(self) -> float:
        return self.usage_total + self.access_fee


@dataclass(frozen=True)
class InvoiceDocument:
    lines: pd.DataFrame
    pi_key: str
    pi_name: str
    pi_email: str
    period: str
    invoice_number: str
    access_fee: float
    access_fee_project: tuple[str, str] | None
    lab_totals: dict[str, float]
    projects: tuple[ProjectSummary, ...]
    generated_at: dt.datetime

    @property
    def usage_total(self) -> float:
        return round(sum(self.lab_totals.values()), 2)

    @property
    def invoice_total(self) -> float:
        return round(self.usage_total + self.access_fee, 2)

    @property
    def show_subsidy(self) -> bool:
        return self.lines["Application identifier"].astype(str).str.upper().eq("CDG").any()

    def lines_for_lab(self, lab: str) -> pd.DataFrame:
        return sort_detail_rows(self.lines.loc[self.lines["Lab"].eq(lab)].copy())

    @classmethod
    def from_frame(
        cls,
        frame: pd.DataFrame,
        *,
        pi_key: str,
        pi_name: str,
        pi_email: str,
        period: str,
        invoice_number: str,
        generated_at: dt.datetime | None = None,
        access_fee_override: float | None = None,
    ) -> InvoiceDocument:
        lines = frame.copy().reset_index(drop=True)
        lab_totals = {
            str(lab): round(float(cost), 2)
            for lab, cost in lines.groupby("Lab", dropna=False)["Cost"].sum().items()
        }
        access_fee = (
            float(access_fee_override) if access_fee_override is not None else _access_fee(lines)
        )
        fee_project = _access_fee_project(lines)
        projects = tuple(_project_summaries(lines, access_fee, fee_project))
        return cls(
            lines=lines,
            pi_key=pi_key,
            pi_name=pi_name,
            pi_email=pi_email,
            period=period,
            invoice_number=invoice_number,
            access_fee=access_fee,
            access_fee_project=fee_project,
            lab_totals=lab_totals,
            projects=projects,
            generated_at=generated_at or dt.datetime.now().astimezone(),
        )


def _access_fee(frame: pd.DataFrame) -> float:
    real_usage = frame.loc[frame["IsToolUsageCharge"].fillna(False)]
    if real_usage.empty:
        return 0.0
    return max(
        (
            ACCESS_FEE_BY_APPLICATION.get(str(value), 0.0)
            for value in real_usage["Application identifier"]
        ),
        default=0.0,
    )


def _access_fee_project(frame: pd.DataFrame) -> tuple[str, str] | None:
    real_usage = frame.loc[frame["IsToolUsageCharge"].fillna(False)]
    if real_usage.empty:
        return None
    totals = (
        real_usage.groupby(["Project", "Application identifier"], dropna=False)["Cost"]
        .sum()
        .reset_index()
        .sort_values(["Cost", "Project"], ascending=[False, True], kind="stable")
    )
    first = totals.iloc[0]
    return str(first["Project"]), str(first["Application identifier"])


def _project_summaries(
    frame: pd.DataFrame,
    access_fee: float,
    fee_project: tuple[str, str] | None,
) -> Iterable[ProjectSummary]:
    group_columns = ["Project", "Application identifier"]
    for (project, application), group in frame.groupby(group_columns, dropna=False):
        staff = group["Item_norm"].astype(str).str.lower().eq("staff time")
        lab_totals = {
            lab: round(float(group.loc[group["Lab"].eq(lab) & ~staff, "Cost"].sum()), 2)
            for lab in DESIRED_LAB_ORDER
        }
        project_key = (str(project), str(application))
        yield ProjectSummary(
            project=str(project),
            application=str(application),
            lab_totals=lab_totals,
            staff_time=round(float(group.loc[staff, "Cost"].sum()), 2),
            access_fee=access_fee if project_key == fee_project else 0.0,
        )
