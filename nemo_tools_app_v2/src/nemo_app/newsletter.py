from __future__ import annotations

import datetime as dt
import re
from collections import Counter
from typing import Any

from nemo_app.mailchimp import MailchimpClient


def _timestamp(value: object, timezone: dt.tzinfo) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone) if parsed.tzinfo is None else parsed


def contact_activity(
    users: list[dict[str, Any]], events: list[dict[str, Any]], *, timezone: dt.tzinfo
) -> dict[str, dict[str, Any]]:
    """Aggregate login and attributed tool usage across accounts sharing an email."""
    activity: dict[str, dict[str, Any]] = {}
    emails_by_id: dict[str, str] = {}
    for user in users:
        if user.get("is_active") is not True:
            continue
        email = str(user.get("email") or "").strip().lower()
        entry = activity.setdefault(
            email, {"login": None, "tool": None, "in_use": False, "is_pi": False}
        )
        managed_projects = user.get("managed_projects")
        entry["is_pi"] = entry["is_pi"] or (
            isinstance(managed_projects, list) and bool(managed_projects)
        )
        if user.get("id") is not None:
            emails_by_id[str(user["id"])] = email
        login = _timestamp(user.get("last_login"), timezone)
        if login and (entry["login"] is None or login > entry["login"]):
            entry["login"] = login
    for event in events:
        identifier = event.get("user")
        if isinstance(identifier, dict):
            identifier = identifier.get("id")
        email = emails_by_id.get(str(identifier))
        if email is None:
            continue
        start = _timestamp(event.get("start"), timezone)
        end = _timestamp(event.get("end"), timezone)
        latest = end if end and (start is None or end >= start) else start
        entry = activity[email]
        if latest and (entry["tool"] is None or latest > entry["tool"]):
            entry["tool"] = latest
        if start and event.get("end") is None:
            entry["in_use"] = True
    return {
        email: {
            "last_login": value["login"].isoformat() if value["login"] else None,
            "last_tool_use": value["tool"].isoformat() if value["tool"] else None,
            "tool_in_use": value["in_use"],
            "is_pi": value["is_pi"],
        }
        for email, value in activity.items()
    }


def active_contacts(users: list[dict[str, Any]]) -> tuple[list[dict[str, str]], int]:
    contacts: dict[str, dict[str, str]] = {}
    invalid = 0
    for user in users:
        if user.get("is_active") is not True:
            continue
        email = str(user.get("email") or "").strip().lower()
        if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
            invalid += 1
            continue
        contacts.setdefault(
            email,
            {
                "email": email,
                "first_name": str(user.get("first_name") or ""),
                "last_name": str(user.get("last_name") or ""),
            },
        )
    return sorted(contacts.values(), key=lambda c: c["email"]), invalid


def compare_newsletter(
    users: list[dict[str, Any]],
    mailchimp: MailchimpClient,
    *,
    usage_events: list[dict[str, Any]] | None = None,
    timezone: dt.tzinfo = dt.UTC,
) -> dict[str, Any]:
    info = mailchimp.audience_info()
    members = mailchimp.members()
    contacts, invalid = active_contacts(users)
    activity = contact_activity(users, usage_events or [], timezone=timezone)
    rows = [
        {
            **contact,
            **activity[contact["email"]],
            "status": members.get(contact["email"], "missing"),
        }
        for contact in contacts
    ]
    return {
        "audience_name": str(info.get("name") or ""),
        "double_optin": bool(info.get("double_optin")),
        "counts": dict(Counter(row["status"] for row in rows)),
        "active_unique_emails": len(rows),
        "invalid_or_missing_emails": invalid,
        "contacts": rows,
        "tool_activity_available": usage_events is not None,
    }
