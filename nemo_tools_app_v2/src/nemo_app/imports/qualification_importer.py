from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import openpyxl

from nemo_app.nemo.client import NemoClient

QUALIFICATION_LEVEL = 1
TOOL_COLUMN_INDEX = 1
DATE_COLUMN_INDEX = 2
EMAIL_COLUMN_INDEX = 3

Progress = Callable[[int, int, str], None]
Log = Callable[[str], None]


@dataclass(frozen=True, slots=True)
class QualificationRow:
    number: int
    tool_id: int
    qualified_on: str
    email: str


@dataclass(frozen=True, slots=True)
class QualificationImportResult:
    row_count: int
    created_count: int
    updated_count: int
    unchanged_count: int
    invalid_row_count: int
    missing_user_count: int
    invalid_tool_count: int
    dry_run: bool
    issues: tuple[str, ...]


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _email(value: object) -> str:
    return _text(value).lower()


def _identifier(value: object) -> int | None:
    if isinstance(value, dict):
        value = value.get("id")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _tool_id(value: object) -> int | None:
    text = _text(value)
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return int(number) if number.is_integer() else None


def _qualified_on(value: object) -> str | None:
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    text = _text(value)
    if not text:
        return None
    for parser in (
        dt.date.fromisoformat,
        lambda item: dt.datetime.strptime(item, "%m/%d/%Y").date(),
        lambda item: dt.datetime.strptime(item, "%m/%d/%y").date(),
    ):
        try:
            return parser(text).isoformat()
        except ValueError:
            continue
    return None


def _looks_like_header(tool: object, date: object, email: object) -> bool:
    combined = " ".join(_text(value).lower() for value in (tool, date, email))
    return "tool" in combined and "email" in combined


def load_qualification_rows(path: Path) -> tuple[list[QualificationRow], list[str]]:
    workbook = openpyxl.load_workbook(path, data_only=True, read_only=True)
    try:
        values = list(workbook.active.iter_rows(values_only=True))
    finally:
        workbook.close()

    rows: list[QualificationRow] = []
    issues: list[str] = []
    for number, values_row in enumerate(values, 1):
        values_row = tuple(values_row)
        if len(values_row) <= EMAIL_COLUMN_INDEX:
            if any(_text(value) for value in values_row):
                issues.append(f"Row {number}: columns B, C, and D are required")
            continue
        tool_value = values_row[TOOL_COLUMN_INDEX]
        date_value = values_row[DATE_COLUMN_INDEX]
        email_value = values_row[EMAIL_COLUMN_INDEX]
        if _looks_like_header(tool_value, date_value, email_value):
            continue

        tool_id = _tool_id(tool_value)
        qualified_on = _qualified_on(date_value)
        email = _email(email_value)
        if tool_id is None and qualified_on is None and not email:
            continue
        if tool_id is None or qualified_on is None or not email:
            issues.append(f"Row {number}: tool ID, qualification date, or email is invalid")
            continue
        rows.append(QualificationRow(number, tool_id, qualified_on, email))

    if not rows:
        detail = f" {issues[0]}" if issues else ""
        raise ValueError(f"No valid qualification rows were found.{detail}")
    return rows, issues


def _user_lookup(users: list[dict[str, Any]]) -> dict[str, int]:
    lookup: dict[str, int] = {}
    for user in users:
        email = _email(user.get("email"))
        user_id = _identifier(user.get("id"))
        if email and user_id is not None:
            lookup[email] = user_id
    return lookup


def _tool_ids(tools: list[dict[str, Any]]) -> set[int]:
    return {tool_id for tool in tools if (tool_id := _identifier(tool.get("id"))) is not None}


def _qualification_lookup(
    qualifications: list[dict[str, Any]],
) -> dict[tuple[int, int], dict[str, Any]]:
    lookup: dict[tuple[int, int], dict[str, Any]] = {}
    for qualification in qualifications:
        user_id = _identifier(qualification.get("user"))
        tool_id = _identifier(qualification.get("tool"))
        if user_id is not None and tool_id is not None:
            lookup[(user_id, tool_id)] = qualification
    return lookup


def run_qualification_import(
    spreadsheet_path: Path,
    *,
    client: NemoClient,
    progress: Progress | None = None,
    log: Log | None = None,
) -> QualificationImportResult:
    write_log = log or (lambda _message: None)
    rows, parse_issues = load_qualification_rows(spreadsheet_path)
    write_log(f"Loaded {len(rows)} valid qualification rows")
    for issue in parse_issues:
        write_log(issue)

    users = _user_lookup(client.fetch_all("users/"))
    tools = _tool_ids(client.fetch_all("tools/"))
    qualifications = _qualification_lookup(client.fetch_all("qualifications/"))
    write_log(
        f"Loaded {len(users)} users, {len(tools)} tools, and "
        f"{len(qualifications)} existing qualifications"
    )

    created = updated = unchanged = missing_users = invalid_tools = 0
    issues = list(parse_issues)
    total = len(rows)
    for current, row in enumerate(rows, 1):
        user_id = users.get(row.email)
        if user_id is None:
            missing_users += 1
            issue = f"Row {row.number}: no NEMO user found for {row.email}"
            issues.append(issue)
            write_log(issue)
        elif row.tool_id not in tools:
            invalid_tools += 1
            issue = f"Row {row.number}: tool ID {row.tool_id} was not found in NEMO"
            issues.append(issue)
            write_log(issue)
        else:
            key = (user_id, row.tool_id)
            existing = qualifications.get(key)
            payload = {
                "qualified_on": row.qualified_on,
                "user": user_id,
                "tool": row.tool_id,
                "qualification_level": QUALIFICATION_LEVEL,
            }
            if existing is None:
                result = client.post("qualifications/", payload)
                qualifications[key] = result
                created += 1
                write_log(
                    f"Row {row.number}: created qualification for user {user_id}, "
                    f"tool {row.tool_id}, dated {row.qualified_on}"
                )
            elif _text(existing.get("qualified_on")) != row.qualified_on:
                qualification_id = _identifier(existing.get("id"))
                if qualification_id is None:
                    raise ValueError(
                        f"Existing qualification for user {user_id} and tool {row.tool_id} "
                        "does not include an ID"
                    )
                result = client.patch(f"qualifications/{qualification_id}/", payload)
                qualifications[key] = {**existing, **result}
                updated += 1
                write_log(
                    f"Row {row.number}: updated qualification {qualification_id} "
                    f"to {row.qualified_on}"
                )
            else:
                unchanged += 1
                write_log(f"Row {row.number}: qualification already has date {row.qualified_on}")
        if progress:
            progress(current, total, f"Qualification row {row.number}")

    write_log(
        f"Summary: {created} created, {updated} updated, {unchanged} unchanged, "
        f"{len(parse_issues)} invalid rows, {missing_users} missing users, "
        f"{invalid_tools} invalid tools"
    )
    return QualificationImportResult(
        row_count=len(rows),
        created_count=created,
        updated_count=updated,
        unchanged_count=unchanged,
        invalid_row_count=len(parse_issues),
        missing_user_count=missing_users,
        invalid_tool_count=invalid_tools,
        dry_run=client.dry_run,
        issues=tuple(issues),
    )
