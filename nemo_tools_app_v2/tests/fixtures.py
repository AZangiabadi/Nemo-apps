from __future__ import annotations

import pandas as pd


def usage_frame() -> pd.DataFrame:
    rows = [
        {
            "Type": "tool_usage",
            "User": "Ada Lovelace",
            "Username": "al1",
            "Item": "Oxford PECVD",
            "Project": "Project A PI1",
            "Application identifier": "Local",
            "Start time": "04/10/2026 @ 09:00 AM",
            "End time": "04/10/2026 @ 07:00 PM",
            "Rate": "$10/hr",
            "Cost": 100.0,
            "Quantity": 600.0,
        },
        {
            "Type": "staff_charge",
            "User": "Ada Lovelace",
            "Username": "al1",
            "Item": "Staff Time",
            "Project": "Project A PI1",
            "Application identifier": "Local",
            "Start time": "04/10/2026 @ 09:00 AM",
            "End time": "04/10/2026 @ 10:00 AM",
            "Rate": "$50/hr",
            "Cost": 50.0,
            "Quantity": 60.0,
        },
        {
            "Type": "consumable_withdrawal",
            "User": "Ada Lovelace",
            "Username": "al1",
            "Item": "Sample holder",
            "Project": "Project A PI1",
            "Application identifier": "Local",
            "Start time": "04/10/2026 @ 11:00 AM",
            "End time": "04/10/2026 @ 11:00 AM",
            "Rate": "$25/item",
            "Cost": 25.0,
            "Quantity": 1.0,
        },
        {
            "Type": "missed_reservation",
            "User": "Grace Hopper",
            "Username": "gh1",
            "Item": "Missed Reservation - Oxford PECVD",
            "Project": "Project B PI2",
            "Application identifier": "Local",
            "Start time": "04/11/2026 @ 09:00 AM",
            "End time": "04/11/2026 @ 10:00 AM",
            "Rate": "$10/hr",
            "Cost": 10.0,
            "Quantity": 60.0,
        },
        {
            "Type": "tool_usage",
            "User": "Grace Hopper",
            "Username": "gh1",
            "Item": "Oxford PECVD",
            "Project": "Project B PI2",
            "Application identifier": "Local",
            "Start time": "04/11/2026 @ 10:00 AM",
            "End time": "04/11/2026 @ 10:01 AM",
            "Rate": "$10/hr",
            "Cost": 0.17,
            "Quantity": 1.0,
        },
    ]
    return pd.DataFrame(rows)


PROJECT_MAP = {
    "Project A PI1": {
        "id": 1,
        "name": "Project A PI1",
        "contact_name": "Ada PI",
        "contact_email": "ada.pi@example.edu",
        "application_identifier": "Local",
    },
    "Project B PI2": {
        "id": 2,
        "name": "Project B PI2",
        "contact_name": "Grace PI",
        "contact_email": "grace.pi@example.edu",
        "application_identifier": "Local",
    },
}
