from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from nemo_app.billing.constants import DESIRED_LAB_ORDER, DETAIL_SECTION_ORDER
from nemo_app.billing.invoice_model import InvoiceDocument
from nemo_app.billing.text import month_label

CHECK_PAYMENT_APPLICATIONS = frozenset({"industry", "external academic"})


def money(value: float) -> str:
    return f"${float(value):,.2f}"


def _invoice_details_markup(document: InvoiceDocument) -> str:
    details = (
        ("PI", document.pi_name),
        ("Email", document.pi_email or "N/A"),
        ("Billing Month", month_label(document.period)),
        ("Invoice #", document.invoice_number),
    )
    return "<br/>".join(f"<b>{escape(label)}:</b> {escape(str(value))}" for label, value in details)


def _payment_instructions_markup() -> str:
    mailing_address = (
        "Please Mail Checks To:",
        "Columbia Nano Initiative",
        "530 W 120th Street, RM 1001",
        "Mail Code 8903 - CEPSR",
        "Building",
        "New York, NY 10027",
        "Email: cnibilling@columbia.edu",
    )
    payment_note = (
        "Checks Only! Make payable to",
        "Columbia University.",
        "Payment due within 30 days of",
        "receipt.",
    )
    address = "<br/>".join(escape(line) for line in mailing_address)
    note = "<br/>".join(escape(line) for line in payment_note)
    return f"<b>{address}</b><br/><br/><i>{note}</i>"


def _uses_check_payment(document: InvoiceDocument) -> bool:
    return any(
        " ".join(project.application.split()).casefold() in CHECK_PAYMENT_APPLICATIONS
        for project in document.projects
    )


def _footer(canvas, document) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.drawRightString(
        document.pagesize[0] - document.rightMargin,
        0.35 * inch,
        f"Page {canvas.getPageNumber()}",
    )
    canvas.restoreState()


def render_invoice_pdf(
    document: InvoiceDocument,
    output_path: Path,
    *,
    logo_path: Path | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = SimpleDocTemplate(
        str(output_path),
        pagesize=landscape(letter),
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.6 * inch,
        title="NEMO Invoice",
        author="NEMO Tools Hub",
    )
    styles = getSampleStyleSheet()
    small = ParagraphStyle("InvoiceSmall", parent=styles["Normal"], fontSize=8, leading=10)
    small_bold = ParagraphStyle("InvoiceSmallBold", parent=small, fontName="Helvetica-Bold")
    header_details = ParagraphStyle(
        "InvoiceHeaderDetails",
        parent=styles["Normal"],
        fontSize=10,
        leading=13,
    )
    heading = ParagraphStyle(
        "InvoiceHeading", parent=styles["Heading2"], fontSize=11, spaceBefore=9, spaceAfter=4
    )
    title = ParagraphStyle("InvoiceTitle", parent=styles["Title"], fontSize=16, alignment=TA_CENTER)
    payment_instructions = ParagraphStyle(
        "InvoicePaymentInstructions",
        parent=small,
        fontSize=10,
        leading=10,
    )

    def paragraph(value: object, style=small):
        return Paragraph(escape(str(value or "")), style)

    logo: object = paragraph("Columbia University", styles["Heading3"])
    if logo_path and logo_path.exists():
        logo = Image(str(logo_path), width=2.2 * inch, height=0.7 * inch, kind="proportional")
        logo.hAlign = "RIGHT"
    payment_block = [logo]
    if _uses_check_payment(document):
        payment_block.extend(
            [
                Spacer(1, 5),
                Paragraph(_payment_instructions_markup(), payment_instructions),
            ]
        )
    header = Table(
        [
            [
                Paragraph(_invoice_details_markup(document), header_details),
                Paragraph("<b>Columbia Nano Initiative</b><br/>Facility Usage Invoice", title),
                payment_block,
            ]
        ],
        colWidths=[pdf.width * 0.35, pdf.width * 0.35, pdf.width * 0.30],
        style=TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("ALIGN", (2, 0), (2, 0), "RIGHT")]),
    )
    story: list[object] = [header, Spacer(1, 8)]

    summary = [["Lab", "Total Cost"]]
    summary.extend([lab, money(total)] for lab, total in sorted(document.lab_totals.items()))
    summary.extend(
        [["Access fee", money(document.access_fee)], ["TOTAL", money(document.invoice_total)]]
    )
    story.append(
        Table(
            summary,
            colWidths=[2.6 * inch, 1.4 * inch],
            hAlign="LEFT",
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#FCE4D6")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                    ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                    ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ]
            ),
        )
    )

    for lab in DETAIL_SECTION_ORDER:
        lines = document.lines_for_lab(lab)
        if lines.empty:
            continue
        story.append(paragraph(lab, heading))
        headers = ["Date", "User", "Description", "Type", "Project", "Qty", "Rate"]
        if document.show_subsidy:
            headers.append("Subsidy")
        headers.append("Cost")
        rows: list[list[object]] = [[paragraph(name, small_bold) for name in headers]]
        for _, line in lines.iterrows():
            start = line.get("Start_dt")
            values: list[object] = [
                start.strftime("%Y-%m-%d %H:%M") if hasattr(start, "strftime") else "",
                line.get("User", ""),
                line.get("Item_norm", ""),
                line.get("Type", ""),
                line.get("Project", ""),
                line.get("Quantity", ""),
                line.get("Rate", ""),
            ]
            if document.show_subsidy:
                values.append(money(float(line.get("Subsidy", 0) or 0)))
            values.append(money(float(line.get("Cost", 0) or 0)))
            rows.append([paragraph(value) for value in values])
        widths = [0.10, 0.10, 0.20, 0.08, 0.19, 0.06, 0.10]
        if document.show_subsidy:
            widths.extend([0.08, 0.09])
        else:
            widths.append(0.17)
        story.append(
            Table(
                rows,
                colWidths=[pdf.width * value for value in widths],
                repeatRows=1,
                style=TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D9E1F2")),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("ALIGN", (-2, 1), (-1, -1), "RIGHT"),
                    ]
                ),
            )
        )

    story.append(paragraph("Project fees summary", heading))
    project_headers = ["Project", "Type", *DESIRED_LAB_ORDER, "Staff", "Fee", "Total"]
    project_rows: list[list[object]] = [[paragraph(value, small_bold) for value in project_headers]]
    for project in sorted(document.projects, key=lambda value: (-value.total, value.project)):
        values = [
            project.project,
            project.application,
            *(money(project.lab_totals[lab]) for lab in DESIRED_LAB_ORDER),
            money(project.staff_time),
            money(project.access_fee),
            money(project.total),
        ]
        project_rows.append([paragraph(value) for value in values])
    project_widths = [0.31, 0.08, 0.08, 0.07, 0.10, 0.095, 0.075, 0.09, 0.10]
    story.append(
        Table(
            project_rows,
            colWidths=[pdf.width * value for value in project_widths],
            repeatRows=1,
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D9E1F2")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
                ]
            ),
        )
    )
    story.append(Spacer(1, 8))
    story.append(
        Table(
            [
                ["Usage charges total", money(document.usage_total)],
                ["Access fee", money(document.access_fee)],
                ["Invoice total", money(document.invoice_total)],
            ],
            colWidths=[3 * inch, 1.2 * inch],
            hAlign="LEFT",
            style=TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                    ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                    ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ]
            ),
        )
    )
    pdf.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return output_path
