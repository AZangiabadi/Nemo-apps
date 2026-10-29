from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd

from .gold import ANGSTROM_CHARGE_TYPES, ANGSTROM_TOOL_KEYS, gold_thickness_nm
from .text import normalize_item, normalize_tool_key, parse_iso_datetime, parse_nemo_datetime

TOOL_TO_LOG_SYSTEM = {
    normalize_tool_key("Angstrom High Vacuum"): "Angstrom UHV",
    normalize_tool_key("Angstrom Metals Deposition System"): "Angstrom Classic",
}
DEFAULT_MATCH_GRACE_MINUTES = 30
DEFAULT_ABSOLUTE_TOLERANCE_NM = 1.0
DEFAULT_RELATIVE_TOLERANCE = 0.05


@dataclass(frozen=True, slots=True)
class ReportedGoldUsage:
    event_id: object
    system: str
    instrument: str
    user: str
    project: str
    start: dt.datetime
    end: dt.datetime
    reported_nm: float


@dataclass(frozen=True, slots=True)
class LoggedGoldRun:
    system: str
    start: dt.datetime
    thickness_nm: float
    run_id: str
    source: str
    source_file: str
    warning: str


@dataclass(frozen=True, slots=True)
class GoldAuditRow:
    event_id: object | None
    system: str
    instrument: str
    user: str
    project: str
    usage_start: dt.datetime | None
    usage_end: dt.datetime | None
    reported_nm: float | None
    logged_nm: float | None
    closest_log_time: dt.datetime | None
    time_difference_minutes: float | None
    log_run_ids: str
    log_sources: str
    notes: str
    initial_status: str


@dataclass(frozen=True, slots=True)
class GoldAuditReport:
    rows: tuple[GoldAuditRow, ...]
    match_grace_minutes: int
    absolute_tolerance_nm: float
    relative_tolerance: float
    source_status: str

    @property
    def exception_count(self) -> int:
        return sum(row.initial_status != "Match" for row in self.rows)


def _id(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _project_names(projects_by_name: dict[str, dict[str, Any]]) -> dict[int, str]:
    return {
        project_id: str(name)
        for name, project in projects_by_name.items()
        if (project_id := _id(project.get("id"))) is not None
    }


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


def reported_gold_usage(
    source: pd.DataFrame,
    usage_events: list[dict[str, Any]],
    *,
    tools_by_id: dict[int, str],
    projects_by_name: dict[str, dict[str, Any]],
) -> list[ReportedGoldUsage]:
    source_by_key = _source_matches(source)
    project_names = _project_names(projects_by_name)
    records: list[ReportedGoldUsage] = []
    seen: set[object] = set()
    for event in usage_events:
        event_id = event.get("id")
        if event_id in seen:
            continue
        seen.add(event_id)
        tool_name = tools_by_id.get(_id(event.get("tool")) or -1, "")
        tool_key = normalize_tool_key(tool_name)
        project_name = project_names.get(_id(event.get("project")) or -1, "")
        start = parse_iso_datetime(event.get("start"))
        if tool_key not in TOOL_TO_LOG_SYSTEM or not project_name or not start:
            continue
        source_row = source_by_key.get(
            (tool_key, project_name, start.replace(second=0, microsecond=0))
        )
        if source_row is None:
            continue
        end = parse_iso_datetime(event.get("end")) or start
        records.append(
            ReportedGoldUsage(
                event_id=event_id,
                system=TOOL_TO_LOG_SYSTEM[tool_key],
                instrument=normalize_item(tool_name),
                user=str(source_row.get("User") or "").strip(),
                project=project_name,
                start=start,
                end=max(start, end),
                reported_nm=float(gold_thickness_nm(event.get("run_data"))),
            )
        )
    return sorted(records, key=lambda record: (record.start, record.system, str(record.event_id)))


def logged_gold_runs(payload: list[dict[str, Any]]) -> list[LoggedGoldRun]:
    records: list[LoggedGoldRun] = []
    for row in payload:
        system = str(row.get("system") or "").strip()
        start = parse_iso_datetime(row.get("start_time"))
        try:
            thickness = Decimal(str(row.get("thickness_nm")))
        except (InvalidOperation, ValueError):
            continue
        if system not in TOOL_TO_LOG_SYSTEM.values() or not start:
            continue
        if not thickness.is_finite() or thickness <= 0:
            continue
        records.append(
            LoggedGoldRun(
                system=system,
                start=start,
                thickness_nm=float(thickness),
                run_id=str(row.get("run_id") or "").strip(),
                source=str(row.get("source") or "").strip(),
                source_file=str(row.get("source_file") or "").strip(),
                warning=str(row.get("warning") or "").strip(),
            )
        )
    return sorted(records, key=lambda record: (record.start, record.system, record.run_id))


def _distance_to_usage(log_time: dt.datetime, usage: ReportedGoldUsage) -> float:
    if usage.start <= log_time <= usage.end:
        return 0.0
    return min(
        abs((log_time - usage.start).total_seconds()), abs((log_time - usage.end).total_seconds())
    )


def _status(reported: float | None, logged: float | None, absolute: float, relative: float) -> str:
    if reported is None:
        return "No user report"
    if logged is None:
        return "No log match"
    tolerance = max(absolute, relative * abs(logged))
    return "Match" if abs(reported - logged) <= tolerance else "Mismatch"


def build_gold_audit_report(
    usages: list[ReportedGoldUsage],
    logs: list[LoggedGoldRun],
    *,
    source_status: str = "Connected to Angstrom log application",
    match_grace_minutes: int = DEFAULT_MATCH_GRACE_MINUTES,
    absolute_tolerance_nm: float = DEFAULT_ABSOLUTE_TOLERANCE_NM,
    relative_tolerance: float = DEFAULT_RELATIVE_TOLERANCE,
) -> GoldAuditReport:
    grace = dt.timedelta(minutes=match_grace_minutes)
    assigned: dict[int, list[LoggedGoldRun]] = {index: [] for index in range(len(usages))}
    unassigned: list[LoggedGoldRun] = []
    ambiguous: set[int] = set()
    for log in logs:
        candidates = [
            (index, usage)
            for index, usage in enumerate(usages)
            if usage.system == log.system and usage.start - grace <= log.start <= usage.end + grace
        ]
        if not candidates:
            unassigned.append(log)
            continue
        index, _ = min(
            candidates,
            key=lambda candidate: (
                _distance_to_usage(log.start, candidate[1]),
                abs((log.start - candidate[1].start).total_seconds()),
                candidate[0],
            ),
        )
        score = (
            _distance_to_usage(log.start, usages[index]),
            abs((log.start - usages[index].start).total_seconds()),
        )
        tied = [
            i
            for i, usage in candidates
            if (
                _distance_to_usage(log.start, usage),
                abs((log.start - usage.start).total_seconds()),
            )
            == score
        ]
        if len(tied) > 1:
            ambiguous.update(tied)
        assigned[index].append(log)

    rows: list[GoldAuditRow] = []
    for index, usage in enumerate(usages):
        matched = assigned[index]
        logged_nm = sum(log.thickness_nm for log in matched) if matched else None
        if usage.reported_nm <= 0 and logged_nm is None:
            continue
        closest = (
            min(matched, key=lambda log: abs((log.start - usage.start).total_seconds()))
            if matched
            else None
        )
        notes = "; ".join(filter(None, (log.warning for log in matched)))
        if index in ambiguous:
            notes = "; ".join(filter(None, (notes, "Ambiguous tool-log session match")))
        rows.append(
            GoldAuditRow(
                event_id=usage.event_id,
                system=usage.system,
                instrument=usage.instrument,
                user=usage.user,
                project=usage.project,
                usage_start=usage.start,
                usage_end=usage.end,
                reported_nm=usage.reported_nm,
                logged_nm=logged_nm,
                closest_log_time=closest.start if closest else None,
                time_difference_minutes=(
                    abs((closest.start - usage.start).total_seconds()) / 60 if closest else None
                ),
                log_run_ids="; ".join(log.run_id for log in matched if log.run_id),
                log_sources="; ".join(
                    filter(None, (f"{log.source}: {log.source_file}" for log in matched))
                ),
                notes=notes,
                initial_status=_status(
                    usage.reported_nm,
                    logged_nm,
                    absolute_tolerance_nm,
                    relative_tolerance,
                ),
            )
        )

    instrument_by_system = {value: key for key, value in TOOL_TO_LOG_SYSTEM.items()}
    instrument_names = {
        normalize_tool_key("Angstrom High Vacuum"): "Angstrom High Vacuum",
        normalize_tool_key(
            "Angstrom Metals Deposition System"
        ): "Angstrom Metals Deposition System",
    }
    for log in unassigned:
        tool_key = instrument_by_system[log.system]
        rows.append(
            GoldAuditRow(
                event_id=None,
                system=log.system,
                instrument=instrument_names[tool_key],
                user="",
                project="",
                usage_start=None,
                usage_end=None,
                reported_nm=None,
                logged_nm=log.thickness_nm,
                closest_log_time=log.start,
                time_difference_minutes=None,
                log_run_ids=log.run_id,
                log_sources=f"{log.source}: {log.source_file}".strip(": "),
                notes=log.warning,
                initial_status="No user report",
            )
        )
    rows.sort(
        key=lambda row: (
            row.usage_start or row.closest_log_time or dt.datetime.max,
            row.system,
            str(row.event_id or ""),
        )
    )
    return GoldAuditReport(
        rows=tuple(rows),
        match_grace_minutes=match_grace_minutes,
        absolute_tolerance_nm=absolute_tolerance_nm,
        relative_tolerance=relative_tolerance,
        source_status=source_status,
    )


def billable_logged_thickness(report: GoldAuditReport) -> dict[object, float]:
    thicknesses = {}
    for row in report.rows:
        if row.event_id is None:
            continue
        if row.notes or row.logged_nm is None:
            continue
        thicknesses[row.event_id] = row.logged_nm
    return thicknesses
