from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import statistics
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from openpyxl import load_workbook

from nemo_app.billing.constants import (
    DESIRED_LAB_ORDER,
    INVOICE_APPLICATION_IDENTIFIERS,
    TOOL_MAX_HOURS_ALIASES,
    TOOL_MAX_HOURS_BY_NAME,
    TOOL_TO_LAB,
)
from nemo_app.billing.text import normalize_tool_key
from nemo_app.config import AppConfig
from nemo_app.nemo.billing import billing_month_dates

APPLICATIONS = {
    "local": "Local",
    "cni-local": "CDG",
    "other academic": "External Academic",
    "industrial": "Industry",
}
ALIASES = {
    "autofinder": "autofinder1",
    "balteccriticalpointdryer": "balteccpd",
    "malvernnanozszetapotentialanalyzer": "malvernzetasizernanozs",
    normalize_tool_key("Renishaw inVia Raman microscope"): "renishawinviamicroraman",
    "ecosecriuvgpc": "tosohecosecriuvgpc",
    "agilentgpc": "agilent1260infinitygpc",
    "horibaxplora": "horibaxploramicroraman",
    "klatencorsurfaceprofilometer": "klap17profiler",
}
HISTORICAL_LABS = {
    "acidhood": "Cleanroom",
    "dektakprofilometer": "Cleanroom",
    "feiebeamwriter": "Cleanroom",
    "heidelberg1umlaserwriter": "Cleanroom",
    "hitachi4700sem": "Cleanroom",
    "laurellspinner2": "Cleanroom",
    "laurellspinner3": "Cleanroom",
    "nanobeam": "Cleanroom",
    "oxfordicprie": "Cleanroom",
    "polisher": "Cleanroom",
    "semicoreebeamevaporator": "Cleanroom",
    "technicsrie": "Cleanroom",
    "westbondaluminumwirebonder": "Cleanroom",
    "westbondgoldwirebonder": "Cleanroom",
    "signatonetransferstation": "Cleanroom",
    "lithohood1": "Cleanroom",
    "cryogenicr700xsquidmagnetometer": "SMCL",
    "ecosechtgpc": "SMCL",
    "jeol100cxtem": "Electron Microscopy Lab",
    "sampleprep": "Electron Microscopy Lab",
}
REQUIRED = {
    "item",
    "bdate",
    "member",
    "project",
    "account",
    "pi",
    "project_type",
    "type",
    "amount",
    "rawcost",
    "cost",
}


def _key(item: str) -> str:
    key = normalize_tool_key(item)
    key = ALIASES.get(key, key)
    return TOOL_MAX_HOURS_ALIASES.get(key, key)


LABS_BY_TOOL = {_key(name): lab for name, lab in TOOL_TO_LAB.items()}


def _cap(item: str) -> float | None:
    key = _key(item)
    return TOOL_MAX_HOURS_BY_NAME.get(TOOL_MAX_HOURS_ALIASES.get(key, key))


def _lab(row: dict) -> str:
    if row["type"] == "inven_activity":
        return "Consumable"
    key = _key(row["item"])
    if key.startswith("expertech"):
        return "Cleanroom"
    return LABS_BY_TOOL.get(key, HISTORICAL_LABS.get(key, "Unassigned"))


def _cents(value: object) -> int:
    return int((Decimal(str(value)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def cap_rates(rows: list[dict]) -> dict[tuple, float]:
    """Infer linear prices only from comparable positive equipment charges in the same year/tier."""
    groups = defaultdict(list)
    for row in rows:
        cap = _cap(row["item"])
        if (
            row["type"] == "eq_activity"
            and cap
            and row["amount"] > 0
            and row["rawcost"] > 0
            and row["cost"] > 0
            and row["project_type"] in APPLICATIONS
        ):
            groups[(row["bdate"][:4], _key(row["item"]), row["project_type"])].append(row)
    rates = {}
    for group, entries in groups.items():
        cap_minutes = _cap(entries[0]["item"]) * 60
        within = [
            r["rawcost"] / r["amount"]
            for r in entries
            if max(60, cap_minutes * 0.2) <= r["amount"] <= cap_minutes
        ]
        if len(within) >= 3:
            rates[group] = statistics.median(within)
            continue
        over = [r for r in entries if r["amount"] > cap_minutes]
        if len({r["amount"] for r in over}) >= 2:
            observed = [r["rawcost"] / r["amount"] for r in over]
            median = statistics.median(observed)
            if max(observed) - min(observed) <= median * 0.015:
                rates[group] = median
    return rates


def adjusted_cost(row: dict, rates: dict) -> tuple[int, bool]:
    cost = _cents(row["cost"])
    cap = _cap(row["item"])
    if (
        row["type"] != "eq_activity"
        or not cap
        or cost <= 0
        or row["rawcost"] <= 0
        or row["amount"] <= cap * 60
    ):
        return cost, False
    rate = rates.get((row["bdate"][:4], _key(row["item"]), row["project_type"]))
    if not rate:
        return cost, False
    raw = row["rawcost"]
    capped_price = rate * cap * 60
    if raw <= capped_price + max(0.05, capped_price * 0.015):
        return cost, False
    if abs(raw - rate * row["amount"]) > max(0.05, raw * 0.015):
        return cost, False
    # Scale the final charge, retaining Badger's existing discounts and monthly adjustments.
    adjusted = int(
        (Decimal(cost) * Decimal(str(cap * 60)) / Decimal(str(row["amount"]))).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )
    return min(cost, adjusted), adjusted < cost


def summarize_badger(rows: list[dict], period: str, rates: dict) -> dict:
    start, end = billing_month_dates(period)
    days = {(start + dt.timedelta(days=i)).isoformat(): 0 for i in range(end.day)}
    applications = dict.fromkeys(INVOICE_APPLICATION_IDENTIFIERS, 0)
    labs = dict.fromkeys(DESIRED_LAB_ORDER, 0)
    pis = {}
    projects = set()
    total = recorded = ignored = adjusted = charge_rows = unassigned = 0
    changes = []
    sources = set()
    for row in rows:
        if row["bdate"][:7] != period:
            continue
        sources.add(row["source_file"])
        if row["cost"] == -1 or row["rawcost"] == -1:
            ignored += 1
            continue
        application = APPLICATIONS.get(row["project_type"])
        if not application:
            continue
        original = _cents(row["cost"])
        value, changed = adjusted_cost(row, rates)
        charge_rows += 1
        recorded += original
        total += value
        adjusted += int(changed)
        lab = _lab(row)
        unassigned += int(lab == "Unassigned")
        labs[lab] = labs.get(lab, 0) + value
        days[row["bdate"][:10]] += value
        applications[application] += value
        if changed:
            changes.append(
                {
                    "item": row["item"],
                    "date": row["bdate"],
                    "source_file": row["source_file"],
                    "source_row": row["source_row"],
                    "recorded": original / 100,
                    "adjusted": value / 100,
                    "cap_hours": _cap(row["item"]),
                }
            )
        if value:
            project = row["project"]
            projects.add(project)
            email = row["pi"] or "Unknown PI"
            pi = pis.setdefault(
                email, {"name": email.split("@")[0], "email": email, "total": 0, "projects": set()}
            )
            pi["total"] += value
            pi["projects"].add(project)
    return {
        "period": period,
        "label": start.strftime("%B %Y"),
        "source": "Badger",
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "apply_hourly_caps": True,
        "total": total / 100,
        "recorded_total": recorded / 100,
        "cap_adjustment": (recorded - total) / 100,
        "cap_adjusted_rows": adjusted,
        "ignored_rows": ignored,
        "unassigned_lab_rows": unassigned,
        "cap_changes": changes,
        "source_files": sorted(sources),
        "record_count": charge_rows,
        "basis": "Historical cap-adjusted estimate" if adjusted else "Recorded Badger charges",
        "gold": 0,
        "fees": 0,
        "other": total / 100,
        "invoice_count": None,
        "pi_count": len(pis),
        "project_count": len(projects),
        "daily": [
            {"date": date, "charges": cost / 100, "fees": 0, "gold": 0, "total": cost / 100}
            for date, cost in days.items()
        ],
        "applications": [
            {"name": name, "total": cost / 100} for name, cost in applications.items()
        ],
        "labs": [{"name": name, "total": cost / 100} for name, cost in labs.items()],
        "pis": [
            {
                "name": pi["name"],
                "email": pi["email"],
                "total": pi["total"] / 100,
                "gold": 0,
                "projects": len(pi["projects"]),
            }
            for pi in sorted(pis.values(), key=lambda p: (-p["total"], p["name"]))
        ],
    }


class BadgerHistory:
    def __init__(self, data_dir: Path):
        self.path = data_dir / "badger.sqlite3"

    def report(self, period: str, *, recalculate: bool = False) -> dict | None:
        if not self.path.exists():
            return None
        with sqlite3.connect(self.path) as connection:
            row = connection.execute(
                "SELECT report_json FROM reports WHERE period=?", (period,)
            ).fetchone()
            if row is None:
                return None
            if not recalculate:
                result = json.loads(row[0])
                result["historical_range"] = list(
                    connection.execute("SELECT MIN(period), MAX(period) FROM reports").fetchone()
                )
                return result
            rows = [
                json.loads(row[0])
                for row in connection.execute(
                    "SELECT payload_json FROM charges WHERE period BETWEEN ? AND ?",
                    (period[:4] + "-01", period[:4] + "-12"),
                )
            ]
            result = summarize_badger(rows, period, cap_rates(rows))
            result["historical_range"] = list(
                connection.execute("SELECT MIN(period), MAX(period) FROM reports").fetchone()
            )
            connection.execute(
                "UPDATE reports SET report_json=? WHERE period=?", (json.dumps(result), period)
            )
            return result

    def import_directory(self, directory: Path) -> dict:
        files = sorted(directory.glob("*.xlsx"))
        if not files:
            raise ValueError("No Badger .xlsx files found")
        rows = []
        seen = set()
        event_sources = {}
        for path in files:
            book = load_workbook(path, read_only=True, data_only=True)
            try:
                occurrences = defaultdict(int)
                for sheet in book:
                    values = iter(sheet.values)
                    header = next(values)
                    if REQUIRED - set(header):
                        raise ValueError(f"{path.name}: unsupported Badger columns")
                    for number, values_row in enumerate(values, 2):
                        row = {
                            key: value for key, value in zip(header, values_row, strict=True) if key
                        }
                        if not row.get("item"):
                            continue
                        stamp = dt.datetime.fromisoformat(str(row["bdate"]))
                        row["bdate"] = stamp.isoformat(sep=" ")
                        for key in ("amount", "rawcost", "cost"):
                            row[key] = float(row[key])
                            if not Decimal(str(row[key])).is_finite():
                                raise ValueError(f"{path.name} row {number}: non-finite {key}")
                        row["project_type"] = str(row["project_type"]).strip().lower()
                        if row["project_type"] not in {*APPLICATIONS, "no charge"}:
                            raise ValueError(f"{path.name} row {number}: unknown project type")
                        event_key = tuple(
                            row[key]
                            for key in (
                                "item",
                                "bdate",
                                "member",
                                "project",
                                "account",
                                "pi",
                                "project_type",
                                "type",
                                "amount",
                            )
                        )
                        previous = event_sources.get(event_key)
                        prices = (row["rawcost"], row["cost"])
                        if previous and previous[0] != path.name and previous[1] != prices:
                            raise ValueError(
                                f"Conflicting charges in {previous[0]} and {path.name} row {number}"
                            )
                        event_sources.setdefault(event_key, (path.name, prices))
                        digest = hashlib.sha256(
                            json.dumps(row, sort_keys=True).encode()
                        ).hexdigest()
                        occurrences[digest] += 1
                        identity = (digest, occurrences[digest])
                        if identity in seen:
                            continue
                        seen.add(identity)
                        row.update(source_file=path.name, source_row=number)
                        rows.append(row)
            finally:
                book.close()
        periods = sorted({row["bdate"][:7] for row in rows})
        rates = cap_rates(rows)
        reports = [summarize_badger(rows, period, rates) for period in periods]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path, timeout=30) as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS charges (
                    period TEXT NOT NULL, payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS charge_period ON charges(period);
                CREATE TABLE IF NOT EXISTS reports (
                    period TEXT PRIMARY KEY, report_json TEXT NOT NULL
                );
            """)
            connection.execute("DELETE FROM charges")
            connection.execute("DELETE FROM reports")
            connection.executemany(
                "INSERT INTO charges VALUES (?,?)",
                [(row["bdate"][:7], json.dumps(row)) for row in rows],
            )
            connection.executemany(
                "INSERT INTO reports VALUES (?,?)",
                [(report["period"], json.dumps(report)) for report in reports],
            )
        return {
            "files": len(files),
            "rows": len(rows),
            "months": len(periods),
            "first_month": periods[0],
            "last_month": periods[-1],
            "ignored_rows": sum(r["ignored_rows"] for r in reports),
            "adjusted_rows": sum(r["cap_adjusted_rows"] for r in reports),
            "cap_adjustment": round(sum(r["cap_adjustment"] for r in reports), 2),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import Badger history into the private app database"
    )
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            BadgerHistory(AppConfig.from_env().data_dir).import_directory(args.directory), indent=2
        )
    )
