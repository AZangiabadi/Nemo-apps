from __future__ import annotations

from typing import Any


def named_ids(records: list[dict[str, Any]], *, source: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for record in records:
        name = " ".join(str(record.get("name") or "").lower().split())
        identifier = record.get("id")
        if not name or not isinstance(identifier, int):
            continue
        if name in result and result[name] != identifier:
            raise ValueError(f"Ambiguous NEMO name {name!r} in {source}")
        result[name] = identifier
    return result


def required_id(lookup: dict[str, int], name: str, *, source: str) -> int:
    key = " ".join(name.lower().split())
    if key not in lookup:
        raise ValueError(f"NEMO {source} is missing required name {name!r}")
    return lookup[key]


def tool_lab(record: dict[str, Any]) -> str | None:
    category = str(record.get("_category") or "").split("/", 1)[0].strip().lower()
    return {
        "cleanroom": "Cleanroom",
        "smcl": "SMCL",
        "electron microscopy": "Electron Microscopy Lab",
    }.get(category)
