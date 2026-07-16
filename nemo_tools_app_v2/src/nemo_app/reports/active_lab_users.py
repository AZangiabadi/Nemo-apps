from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from nemo_app.nemo.client import NemoClient

from .excel import save_frames

LABS = {
    "clean-room": ("Clean Room", ("clean room", "cleanroom")),
    "smcl": (
        "SMCL",
        (
            "smcl",
            "shared materials characterization lab",
            "soft materials characterization lab",
            "surface materials characterization lab",
        ),
    ),
    "electron-microscopy": (
        "Electron Microscopy",
        ("electron microscopy", "electron microscope", "electron microscopy lab"),
    ),
}


@dataclass(frozen=True, slots=True)
class ActiveLabUsersResult:
    output_path: Path
    cutoff_date: dt.date
    combined_user_count: int
    summary: tuple[dict[str, object], ...]


def _search_text(value: object) -> str:
    if isinstance(value, dict):
        return " ".join(_search_text(item) for item in value.values())
    if isinstance(value, list):
        return " ".join(_search_text(item) for item in value)
    return " ".join(re.sub(r"[_\-/\\()\[\]{},;:]", " ", str(value or "").lower()).split())


def _date(value: object) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _user_name(user: dict[str, Any]) -> str:
    return str(
        user.get("name") or f"{user.get('first_name', '')} {user.get('last_name', '')}"
    ).strip()


def build_active_lab_users_report(
    output_path: Path,
    *,
    client: NemoClient,
    selected_labs: tuple[str, ...] = tuple(LABS),
    today: dt.date | None = None,
) -> ActiveLabUsersResult:
    unknown = set(selected_labs) - set(LABS)
    if unknown:
        raise ValueError(f"Unknown labs: {', '.join(sorted(unknown))}")
    cutoff = (today or dt.date.today()) - dt.timedelta(days=365)
    tools = client.fetch_all("tools/")
    tools_by_id = {record["id"]: record for record in tools if isinstance(record.get("id"), int)}
    tool_ids: dict[str, set[int]] = {lab: set() for lab in selected_labs}
    for tool_id, tool in tools_by_id.items():
        text = _search_text(tool)
        for lab in selected_labs:
            if any(keyword in text for keyword in LABS[lab][1]):
                tool_ids[lab].add(tool_id)
    try:
        qualifications = client.fetch_all(f"qualifications/?qualified_on__gte={cutoff.isoformat()}")
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code not in {400, 404}:
            raise
        qualifications = client.fetch_all("qualifications/")

    matched: dict[str, dict[int, dict[str, object]]] = {lab: {} for lab in selected_labs}
    user_ids: set[int] = set()
    for qualification in qualifications:
        qualified_on = _date(qualification.get("qualified_on"))
        user_id = qualification.get("user")
        tool_id = qualification.get("tool")
        if (
            not qualified_on
            or qualified_on < cutoff
            or not isinstance(user_id, int)
            or not isinstance(tool_id, int)
        ):
            continue
        for lab in selected_labs:
            if tool_id not in tool_ids[lab]:
                continue
            user_ids.add(user_id)
            entry = matched[lab].setdefault(user_id, {"date": qualified_on, "tools": set()})
            entry["date"] = max(entry["date"], qualified_on)
            entry["tools"].add(tool_id)
    users = client.fetch_by_ids("users/", user_ids)
    frames: dict[str, pd.DataFrame] = {}
    combined: dict[int, dict[str, object]] = {}
    summary: list[dict[str, object]] = []
    for lab in selected_labs:
        records: list[dict[str, object]] = []
        inactive = 0
        label = LABS[lab][0]
        for user_id, qualification in matched[lab].items():
            user = users.get(user_id, {})
            if not bool(user.get("is_active", user.get("active", True))):
                inactive += 1
                continue
            tool_names = ", ".join(
                str(tools_by_id[tool_id].get("name") or f"Tool {tool_id}")
                for tool_id in sorted(qualification["tools"])
            )
            record = {
                "Email": str(user.get("email") or ""),
                "Username": str(user.get("username") or ""),
                "Name": _user_name(user),
                "User ID": user_id,
                "Last Qualified On": qualification["date"].isoformat(),
                "Qualified Tools": tool_names,
            }
            records.append(record)
            entry = combined.setdefault(user_id, {**record, "Labs": set(), "Tools": {}})
            entry["Labs"].add(label)
            entry["Tools"][label] = tool_names
            entry["Last Qualified On"] = max(
                entry["Last Qualified On"], record["Last Qualified On"]
            )
        frames[lab] = pd.DataFrame(
            records,
            columns=[
                "Email",
                "Username",
                "Name",
                "User ID",
                "Last Qualified On",
                "Qualified Tools",
            ],
        ).sort_values(["Email", "Username"], kind="stable")
        summary.append(
            {
                "Lab": label,
                "Matched Tools": len(tool_ids[lab]),
                "Active Users": len(records),
                "Inactive Users Skipped": inactive,
            }
        )
    combined_records = [
        {
            "Email": entry["Email"],
            "Username": entry["Username"],
            "Name": entry["Name"],
            "User ID": user_id,
            "Labs": ", ".join(sorted(entry["Labs"])),
            "Last Qualified On": entry["Last Qualified On"],
            "Qualified Tools By Lab": "; ".join(
                f"{lab}: {entry['Tools'][lab]}" for lab in sorted(entry["Tools"])
            ),
        }
        for user_id, entry in combined.items()
    ]
    combined_frame = pd.DataFrame(
        combined_records,
        columns=[
            "Email",
            "Username",
            "Name",
            "User ID",
            "Labs",
            "Last Qualified On",
            "Qualified Tools By Lab",
        ],
    )
    sheets = [
        ("Summary", pd.DataFrame(summary), set(), set()),
        ("All Labs", combined_frame, set(), set()),
    ]
    sheets.extend((LABS[lab][0], frames[lab], set(), set()) for lab in selected_labs)
    save_frames(output_path, sheets)
    return ActiveLabUsersResult(output_path, cutoff, len(combined_frame), tuple(summary))
