from __future__ import annotations

import csv
import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import openpyxl

from nemo_app.nemo.cache import JsonTTLCache
from nemo_app.nemo.client import NemoClient

ACCOUNT_TYPES = {"cdg": 1, "industry": 2, "external academic": 3, "local": 4}
USER_TYPES = {"local": 1, "cdg": 1, "external academic": 2, "industry": 4}
PROJECT_TYPES = {"cdg": 2, "local": 3, "external academic": 4, "industry": 5}
PROJECT_CATEGORIES = {"cdg": 6, "external academic": 1, "industry": 2, "local": 3}
EXPECTED_HEADERS = {"name", "uni", "email", "pi", "account type", "project number"}

Progress = Callable[[int, int, str], None]
Log = Callable[[str], None]


def _text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _account_type(value: object) -> str:
    return " ".join(_text(value).lower().split())


def _ids(values: object) -> list[int]:
    if not isinstance(values, list):
        return []
    result: set[int] = set()
    for value in values:
        if isinstance(value, dict):
            value = value.get("id")
        try:
            result.add(int(value))
        except (TypeError, ValueError):
            continue
    return sorted(result)


@dataclass(slots=True)
class SpreadsheetRow:
    number: int
    name: str
    uni: str
    email: str
    pi_value: str
    account_type: str
    project_number: str

    @property
    def is_pi(self) -> bool:
        return self.pi_value.lower() == "pi"


@dataclass(slots=True)
class ExistingRecords:
    accounts: dict[str, dict[str, Any]]
    users_by_email: dict[str, dict[str, Any]]
    users_by_username: dict[str, dict[str, Any]]
    projects: dict[str, dict[str, Any]]

    def serialize(self) -> dict[str, object]:
        return {
            "accounts": self.accounts,
            "users_by_email": self.users_by_email,
            "users_by_username": self.users_by_username,
            "projects": self.projects,
        }

    @classmethod
    def deserialize(cls, value: dict[str, object]) -> ExistingRecords:
        return cls(
            accounts=dict(value.get("accounts", {})),
            users_by_email=dict(value.get("users_by_email", {})),
            users_by_username=dict(value.get("users_by_username", {})),
            projects=dict(value.get("projects", {})),
        )


@dataclass(frozen=True, slots=True)
class ImportResult:
    row_count: int
    project_count: int
    user_count: int
    pi_count: int
    dry_run: bool
    actions: tuple[str, ...]


def _spreadsheet_values(path: Path) -> list[list[object]]:
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as handle:
            return [list(row) for row in csv.reader(handle)]
    workbook = openpyxl.load_workbook(path, data_only=True, read_only=True)
    try:
        return [list(row) for row in workbook.active.iter_rows(values_only=True)]
    finally:
        workbook.close()


def load_spreadsheet(path: Path) -> list[SpreadsheetRow]:
    values = _spreadsheet_values(path)
    header_row = None
    header_indexes: dict[str, int] = {}
    for row_index, row in enumerate(values):
        indexes = {_text(value).lower(): index for index, value in enumerate(row) if _text(value)}
        if EXPECTED_HEADERS.issubset(indexes):
            header_row, header_indexes = row_index, indexes
            break
    if header_row is None:
        raise ValueError("Could not find the required spreadsheet headers.")
    rows: list[SpreadsheetRow] = []
    for number, row in enumerate(values[header_row + 1 :], start=header_row + 2):
        fields: list[str] = []
        for name in (
            "name",
            "uni",
            "email",
            "pi",
            "account type",
            "project number",
        ):
            index = header_indexes[name]
            fields.append(_text(row[index] if index < len(row) else ""))
        if not any(fields):
            continue
        rows.append(
            SpreadsheetRow(
                number,
                fields[0],
                fields[1],
                fields[2].lower(),
                fields[3],
                _account_type(fields[4]),
                fields[5],
            )
        )
    if not rows:
        raise ValueError("No usable spreadsheet rows were found.")
    errors: list[str] = []
    for row in rows:
        if not row.name and not row.uni:
            errors.append(f"Row {row.number}: missing Name and UNI")
        if not row.email and not row.uni:
            errors.append(f"Row {row.number}: missing Email and UNI")
        if not row.project_number:
            errors.append(f"Row {row.number}: missing Project Number")
        if row.account_type not in ACCOUNT_TYPES:
            errors.append(f"Row {row.number}: unsupported Account type {row.account_type!r}")
    if errors:
        raise ValueError("\n".join(errors[:20]))
    return rows


def _split_name(value: str) -> tuple[str, str]:
    parts = value.split()
    return (parts[0], " ".join(parts[1:])) if parts else ("", "")


def _generated_uni(row: SpreadsheetRow, used: set[str]) -> str:
    first, last = _split_name(row.name)
    initials = f"{first[:1] or 'x'}{last[:1] or first[:1] or 'x'}".lower()
    digest = int(hashlib.sha256(f"{row.email}|{row.name}".encode()).hexdigest()[:8], 16)
    for offset in range(90):
        candidate = f"xx{initials}{10 + ((digest + offset) % 90)}"
        if candidate not in used:
            used.add(candidate)
            return candidate
    raise ValueError(f"Could not generate a unique UNI for row {row.number}")


def _load_existing(client: NemoClient, cache: JsonTTLCache, *, use_cache: bool) -> ExistingRecords:
    key = f"{client.identity_hash}:import-records"
    cached = cache.get(key) if use_cache else None
    if isinstance(cached, dict):
        return ExistingRecords.deserialize(cached)
    accounts = client.fetch_all("accounts/")
    users = client.fetch_all("users/")
    projects = client.fetch_all("projects/")
    records = ExistingRecords(
        accounts={
            str(value.get("name") or "").strip().lower(): value
            for value in accounts
            if value.get("name")
        },
        users_by_email={
            str(value.get("email") or "").strip().lower(): value
            for value in users
            if value.get("email")
        },
        users_by_username={
            str(value.get("username") or "").strip().lower(): value
            for value in users
            if value.get("username")
        },
        projects={
            str(value.get("name") or "").strip().lower(): value
            for value in projects
            if value.get("name")
        },
    )
    cache.set(key, records.serialize())
    return records


def _existing_user(row: SpreadsheetRow, records: ExistingRecords) -> dict[str, Any] | None:
    return records.users_by_email.get(row.email) or records.users_by_username.get(row.uni.lower())


def _fill_from_existing(row: SpreadsheetRow, user: dict[str, Any]) -> None:
    row.email = row.email or str(user.get("email") or "").strip().lower()
    row.uni = row.uni or str(user.get("username") or "").strip()
    row.name = row.name or " ".join(
        value
        for value in (
            str(user.get("first_name") or "").strip(),
            str(user.get("last_name") or "").strip(),
        )
        if value
    )


def _account_payload(row: SpreadsheetRow) -> dict[str, Any]:
    return {
        "name": row.project_number,
        "note": "",
        "start_date": date.today().isoformat(),
        "active": True,
        "type": ACCOUNT_TYPES[row.account_type],
    }


def _user_payload(
    row: SpreadsheetRow,
    *,
    projects: list[int] | None = None,
    managed_projects: list[int] | None = None,
    managed_accounts: list[int] | None = None,
) -> dict[str, Any]:
    first, last = _split_name(row.name)
    return {
        "username": row.uni,
        "first_name": first,
        "last_name": last,
        "email": row.email,
        "domain": "",
        "notes": "",
        "badge_number": None,
        "access_expiration": None,
        "is_active": True,
        "is_staff": False,
        "training_required": False,
        "date_joined": datetime.now().astimezone().isoformat(timespec="seconds"),
        "type": USER_TYPES[row.account_type],
        "projects": sorted(set(projects or [])),
        "managed_projects": sorted(set(managed_projects or [])),
        "managed_accounts": sorted(set(managed_accounts or [])),
        "managed_users": [],
        "emergency_contact": "",
    }


def _project_payload(row: SpreadsheetRow, account_id: int, pi_id: int) -> dict[str, Any]:
    return {
        "principal_investigators": [pi_id],
        "users": [pi_id],
        "name": row.project_number,
        "application_identifier": row.account_type,
        "start_date": date.today().isoformat(),
        "active": True,
        "allow_consumable_withdrawals": True,
        "allow_staff_charges": True,
        "account": account_id,
        "project_types": [PROJECT_TYPES[row.account_type]],
        "only_allow_tools": [],
        "contact_name": row.name,
        "contact_email": row.email,
        "addressee": f"{row.name}\r\n{row.email}",
        "comments": "",
        "no_charge": False,
        "no_tax": False,
        "no_cap": False,
        "category": PROJECT_CATEGORIES[row.account_type],
    }


def _patch_relationships(
    client: NemoClient, user: dict[str, Any], **relationships: list[int]
) -> dict[str, Any]:
    changes = {
        key: sorted(set(_ids(user.get(key)) + values))
        for key, values in relationships.items()
        if values is not None
    }
    changes = {key: value for key, value in changes.items() if _ids(user.get(key)) != value}
    return {**user, **(client.patch(f"users/{user['id']}/", changes) if changes else {})}


def run_import(
    spreadsheet_path: Path,
    *,
    client: NemoClient,
    cache: JsonTTLCache,
    use_cache: bool = True,
    progress: Progress | None = None,
    log: Log | None = None,
) -> ImportResult:
    total_steps = 8
    current = 0

    def advance(message: str) -> None:
        nonlocal current
        current += 1
        if log:
            log(message)
        if progress:
            progress(current, total_steps, message)

    rows = load_spreadsheet(spreadsheet_path)
    advance("Spreadsheet loaded and validated")
    records = _load_existing(client, cache, use_cache=use_cache)
    advance("Current accounts, users, and projects loaded")
    used = set(records.users_by_username)
    generated_by_email: dict[str, str] = {}
    for row in rows:
        existing = _existing_user(row, records)
        if existing:
            _fill_from_existing(row, existing)
        if not row.uni:
            generated = generated_by_email.get(row.email)
            if generated is None:
                generated = _generated_uni(row, used)
                generated_by_email[row.email] = generated
            row.uni = generated

    account_ids: dict[str, int] = {}
    for row in rows:
        key = row.project_number.lower()
        if key in account_ids:
            continue
        account = records.accounts.get(key) or client.post("accounts/", _account_payload(row))
        records.accounts[key] = account
        account_ids[key] = int(account["id"])
    advance("Accounts processed")

    for row in (value for value in rows if value.is_pi):
        user = _existing_user(row, records)
        if user:
            user = _patch_relationships(
                client, user, managed_accounts=[account_ids[row.project_number.lower()]]
            )
        else:
            if not row.email:
                raise ValueError(f"Row {row.number}: PI email is required for a new user")
            user = client.post(
                "users/",
                _user_payload(row, managed_accounts=[account_ids[row.project_number.lower()]]),
            )
        records.users_by_email[row.email] = user
        records.users_by_username[row.uni.lower()] = user
    advance("Principal investigators processed")

    for row in (value for value in rows if value.is_pi):
        key = row.project_number.lower()
        if key in records.projects:
            continue
        pi = records.users_by_email.get(row.email)
        if not pi:
            raise ValueError(f"PI {row.email!r} was not found after user processing")
        records.projects[key] = client.post(
            "projects/", _project_payload(row, account_ids[key], int(pi["id"]))
        )
    advance("Projects processed")

    for row in (value for value in rows if value.is_pi):
        user = records.users_by_email[row.email]
        project = records.projects[row.project_number.lower()]
        user = _patch_relationships(
            client,
            user,
            projects=[int(project["id"])],
            managed_projects=[int(project["id"])],
            managed_accounts=[account_ids[row.project_number.lower()]],
        )
        records.users_by_email[row.email] = user
    advance("PI relationships processed")

    for row in (value for value in rows if not value.is_pi):
        project = records.projects.get(row.project_number.lower())
        if not project:
            raise ValueError(f"Project {row.project_number!r} was not found")
        user = _existing_user(row, records)
        if user:
            user = _patch_relationships(client, user, projects=[int(project["id"])])
        else:
            if not row.email:
                raise ValueError(f"Row {row.number}: email is required for a new user")
            user = client.post("users/", _user_payload(row, projects=[int(project["id"])]))
        records.users_by_email[row.email] = user
        records.users_by_username[row.uni.lower()] = user
    advance("Other users processed")
    # A dry run contains synthetic negative IDs. Never let those records become
    # the starting point for a later live import.
    if not client.dry_run:
        cache.set(f"{client.identity_hash}:import-records", records.serialize())
    advance("Dry run complete" if client.dry_run else "Import complete")
    return ImportResult(
        len(rows),
        len({row.project_number.lower() for row in rows}),
        len({row.email or row.uni for row in rows}),
        sum(row.is_pi for row in rows),
        client.dry_run,
        tuple(client.actions),
    )
