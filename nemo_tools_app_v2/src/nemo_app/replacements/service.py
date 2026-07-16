from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from nemo_app.nemo.client import NemoClient

ACCOUNT_COPY_FIELDS = ("note", "type")
PROJECT_COPY_FIELDS = (
    "principal_investigators",
    "users",
    "application_identifier",
    "allow_consumable_withdrawals",
    "allow_staff_charges",
    "discipline",
    "project_types",
    "only_allow_tools",
    "project_name",
    "contact_name",
    "contact_phone",
    "contact_email",
    "expires_on",
    "addressee",
    "comments",
    "no_charge",
    "no_tax",
    "no_cap",
    "category",
    "institution",
    "department",
    "staff_host",
)


@dataclass(frozen=True, slots=True)
class ReplacementResult:
    mode: str
    dry_run: bool
    lines: tuple[str, ...]


def _record(records: list[dict[str, Any]], value: str, label: str) -> dict[str, Any]:
    lookup = value.strip()
    if not lookup:
        raise ValueError(f"Enter the {label}.")
    matches = []
    if lookup.isdigit():
        matches = [record for record in records if record.get("id") == int(lookup)]
    if not matches:
        matches = [record for record in records if str(record.get("name") or "").strip() == lookup]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"Multiple {label} records matched {lookup!r}; use the API id.")
    raise ValueError(f"No {label} record matched {lookup!r}.")


def _account_for_project(accounts: list[dict[str, Any]], project: dict[str, Any]) -> dict[str, Any]:
    name = str(project.get("name") or "").strip()
    named = [account for account in accounts if str(account.get("name") or "").strip() == name]
    if len(named) == 1:
        return named[0]
    linked = [account for account in accounts if account.get("id") == project.get("account")]
    if len(linked) == 1:
        return linked[0]
    raise ValueError(f"Could not uniquely resolve the account for project {name!r}.")


def _ids(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    result: set[int] = set()
    for item in value:
        if isinstance(item, dict):
            item = item.get("id")
        try:
            result.add(int(item))
        except (TypeError, ValueError):
            continue
    return sorted(result)


def _deactivate(client: NemoClient, project: dict[str, Any], account: dict[str, Any]) -> list[str]:
    client.patch(f"projects/{project['id']}/", {"active": False})
    client.patch(f"accounts/{account['id']}/", {"active": False})
    return ["Old project active=false", "Old account active=false"]


def replace_account_project(
    *,
    client: NemoClient,
    old_value: str,
    target_value: str,
    mode: str,
    deactivate_old: bool,
) -> ReplacementResult:
    if mode not in {"new", "existing"}:
        raise ValueError("Replacement mode must be 'new' or 'existing'.")
    if old_value.strip() == target_value.strip():
        raise ValueError("Old and target account/project values must differ.")
    accounts = client.fetch_all("accounts/")
    projects = client.fetch_all("projects/")
    old_project = _record(projects, old_value, "old project")
    old_account = _account_for_project(accounts, old_project)

    if mode == "new":
        target = target_value.strip()
        if any(str(value.get("name") or "").strip() == target for value in accounts):
            raise ValueError(f"An account named {target!r} already exists.")
        if any(str(value.get("name") or "").strip() == target for value in projects):
            raise ValueError(f"A project named {target!r} already exists.")
        today = dt.date.today().isoformat()
        account_payload = {
            field: old_account.get(field) for field in ACCOUNT_COPY_FIELDS if field in old_account
        }
        account_payload.update({"name": target, "start_date": today, "active": True})
        new_account = client.post("accounts/", account_payload)
        project_payload = {
            field: old_project.get(field) for field in PROJECT_COPY_FIELDS if field in old_project
        }
        project_payload.update(
            {"name": target, "start_date": today, "active": True, "account": int(new_account["id"])}
        )
        new_project = client.post("projects/", project_payload)
        lines = [
            "Replacement option: create a new project/account",
            f"Old account: {old_account.get('name')} (id {old_account.get('id')})",
            f"Old project: {old_project.get('name')} (id {old_project.get('id')})",
            f"New account: {new_account.get('name')} (id {new_account.get('id')})",
            f"New project: {new_project.get('name')} (id {new_project.get('id')})",
            f"New start_date: {today}",
        ]
    else:
        target_project = _record(projects, target_value, "existing project")
        target_account = _account_for_project(accounts, target_project)
        old_users = set(_ids(old_project.get("users")))
        target_users = set(_ids(target_project.get("users")))
        users_to_add = sorted(old_users - target_users)
        if users_to_add:
            client.patch(
                f"projects/{target_project['id']}/", {"users": sorted(old_users | target_users)}
            )
            users = {
                int(user["id"]): user
                for user in client.fetch_all("users/")
                if user.get("id") is not None
            }
            missing = [user_id for user_id in users_to_add if user_id not in users]
            if missing:
                raise ValueError("Missing copied users: " + ", ".join(map(str, missing)))
            for user_id in users_to_add:
                project_ids = _ids(users[user_id].get("projects"))
                target_id = int(target_project["id"])
                if target_id not in project_ids:
                    client.patch(
                        f"users/{user_id}/", {"projects": sorted(project_ids + [target_id])}
                    )
        lines = [
            "Replacement option: use an existing project/account",
            f"Old account: {old_account.get('name')} (id {old_account.get('id')})",
            f"Old project: {old_project.get('name')} (id {old_project.get('id')})",
            f"Existing account: {target_account.get('name')} (id {target_account.get('id')})",
            f"Existing project: {target_project.get('name')} (id {target_project.get('id')})",
            f"Users added: {len(users_to_add)}",
        ]
    lines.extend(
        _deactivate(client, old_project, old_account)
        if deactivate_old
        else ["Old project/account left active"]
    )
    return ReplacementResult(mode, client.dry_run, tuple(lines))
