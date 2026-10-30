from __future__ import annotations

import calendar
import datetime as dt
from collections.abc import Callable
from decimal import ROUND_HALF_UP, Decimal

LABS = ("Cleanroom", "SMCL", "Electron Microscopy Lab")


def cents(value: float) -> int:
    return int((Decimal(str(value)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def yearly_revenue(
    year: int,
    *,
    today: dt.date,
    monthly_report: Callable[[str], dict | None],
) -> dict:
    if not 1 <= year <= today.year:
        raise ValueError("Choose the current year or an earlier year.")
    months = []
    lab_totals = dict.fromkeys(LABS, 0)
    revenue = other = unclassified = saved_count = lab_count = estimated_count = 0
    for month in range(1, 13):
        period = f"{year:04d}-{month:02d}"
        future = year == today.year and month > today.month
        report = None if future else monthly_report(period)
        row = {
            "period": period,
            "label": calendar.month_abbr[month],
            "source": "",
            "status": "future" if future else "not_saved",
            "labs": None,
            "lab_total": None,
            "other": None,
            "total": None,
            "estimated": False,
        }
        if report is not None:
            total = cents(report["total"])
            revenue += total
            saved_count += 1
            estimated = bool(report.get("cap_adjusted_rows"))
            estimated_count += int(estimated)
            row.update(
                source=report.get("source", "NEMO"),
                total=total / 100,
                estimated=estimated,
                status="no_lab_breakdown",
            )
            if isinstance(report.get("labs"), list):
                lookup = {item["name"]: cents(item["total"]) for item in report["labs"]}
                selected = {lab: lookup.get(lab, 0) for lab in LABS}
                subtotal = sum(selected.values())
                outside = total - subtotal
                other += outside
                lab_count += 1
                for lab, value in selected.items():
                    lab_totals[lab] += value
                row.update(
                    status="available",
                    labs={lab: value / 100 for lab, value in selected.items()},
                    lab_total=subtotal / 100,
                    other=outside / 100,
                )
            else:
                unclassified += total
        months.append(row)
    return {
        "year": year,
        "months": months,
        "labs": list(LABS),
        "lab_totals": {lab: value / 100 for lab, value in lab_totals.items()},
        "lab_total": sum(lab_totals.values()) / 100,
        "total": revenue / 100,
        "other": other / 100,
        "unclassified": unclassified / 100,
        "saved_months": saved_count,
        "lab_months": lab_count,
        "estimated_months": estimated_count,
        "missing_months": sum(row["status"] == "not_saved" for row in months),
    }
