from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    slug: str
    title: str
    summary: str
    href: str
    accent: str


TOOLS = (
    ToolDefinition(
        "user-import",
        "User/Account/Project Batch Import",
        "Create NEMO records from an Excel or CSV spreadsheet.",
        "/tools/user-import",
        "#0f766e",
    ),
    ToolDefinition(
        "invoices",
        "NEMO Invoice Generator",
        "Generate matching Excel and PDF invoices from one billing model.",
        "/tools/invoices",
        "#9a3412",
    ),
    ToolDefinition(
        "reports",
        "Reports",
        "Build detailed financial, usage-cap, user/PI, missed-reservation, and lab-user reports.",
        "/tools/reports",
        "#334155",
    ),
    ToolDefinition(
        "excel-to-pdf",
        "Excel Invoice to PDF",
        "Render an edited invoice workbook as PDF.",
        "/tools/excel-to-pdf",
        "#475569",
    ),
    ToolDefinition(
        "replacement",
        "Account/Project Replacement",
        "Clone an account/project or move users to an existing project.",
        "/tools/replacement",
        "#6d28d9",
    ),
    ToolDefinition(
        "jumbotron",
        "Jumbotron",
        "Show live tool usage, reservations, and cancellations.",
        "/jumbotron",
        "#1d4ed8",
    ),
)
