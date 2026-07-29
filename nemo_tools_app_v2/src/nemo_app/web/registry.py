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
        "detailed-financials",
        "Detailed Financials",
        "Build transaction-level financial workbooks with project and charge summaries.",
        "/tools/detailed-financials",
        "#334155",
    ),
    ToolDefinition(
        "usage-caps",
        "Usage Cap Analysis",
        "Analyze session and project caps across one or more usage exports.",
        "/tools/usage-caps",
        "#0369a1",
    ),
    ToolDefinition(
        "user-pi",
        "User PI Report",
        "Build user, project, and principal-investigator relationship reports.",
        "/tools/user-pi",
        "#0f766e",
    ),
    ToolDefinition(
        "excel-to-pdf",
        "Excel Invoice to PDF",
        "Render an edited invoice workbook as PDF.",
        "/tools/excel-to-pdf",
        "#475569",
    ),
    ToolDefinition(
        "missed-reservations",
        "Missed Reservations",
        "Extract missed reservations from a NEMO usage export.",
        "/tools/missed-reservations",
        "#b45309",
    ),
    ToolDefinition(
        "active-users",
        "Active Lab Users",
        "Build a workbook of active users and their lab access.",
        "/tools/active-users",
        "#047857",
    ),
    ToolDefinition(
        "replacement",
        "Replace Account/Project",
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
