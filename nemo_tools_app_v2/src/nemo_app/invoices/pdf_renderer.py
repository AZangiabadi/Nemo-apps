from __future__ import annotations

from functools import partial
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from nemo_app.billing.invoice_model import InvoiceDocument
from nemo_app.billing.text import month_label

from .customization import DEFAULT_INVOICE_CUSTOMIZATION, InvoiceCustomization

CHECK_PAYMENT_APPLICATIONS = frozenset({"industry", "external academic"})
FONT_FAMILIES = {
    "Helvetica": ("Helvetica", "Helvetica-Bold", "Helvetica-Oblique"),
    "Times": ("Times-Roman", "Times-Bold", "Times-Italic"),
    "Courier": ("Courier", "Courier-Bold", "Courier-Oblique"),
}
COLOR_THEMES = {
    "cni": ("#FCE4D6", "#D9E1F2"),
    "ocean": ("#DBEAFE", "#BFDBFE"),
    "forest": ("#DCFCE7", "#CCFBF1"),
    "grayscale": ("#E5E7EB", "#F3F4F6"),
}
DETAIL_COLUMN_SPECS = {
    "date": ("Date", 0.10),
    "user": ("User", 0.10),
    "description": ("Description", 0.20),
    "type": ("Type", 0.08),
    "project": ("Project", 0.19),
    "quantity": ("Qty", 0.06),
    "rate": ("Rate", 0.10),
    "subsidy": ("Subsidy", 0.08),
    "cost": ("Cost", 0.09),
}
NUMERIC_DETAIL_COLUMNS = frozenset({"quantity", "rate", "subsidy", "cost"})


def money(value: float) -> str:
    return f"${float(value):,.2f}"


def _invoice_detail_lines(
    document: InvoiceDocument,
    customization: InvoiceCustomization | None = None,
) -> tuple[str, ...]:
    settings = customization or DEFAULT_INVOICE_CUSTOMIZATION
    values = {
        "pi": f"PI: {document.pi_name}",
        "email": f"Email: {document.pi_email or 'N/A'}",
        "billing_month": f"Billing Month: {month_label(document.period)}",
        "invoice_number": f"Invoice #: {document.invoice_number}",
        "generated": f"Generated: {document.generated_at.strftime('%Y-%m-%d %H:%M')} ET",
    }
    return tuple(values[field] for field in settings.billing_fields)


def _invoice_details_markup(
    document: InvoiceDocument,
    customization: InvoiceCustomization | None = None,
) -> str:
    return "<br/>".join(
        f"<b>{escape(line)}</b>" for line in _invoice_detail_lines(document, customization)
    )


def _payment_parts(payment_instructions: str) -> tuple[str, str]:
    address, separator, note = payment_instructions.strip().partition("\n\n")
    if not separator:
        note = ""
    address_markup = "<br/>".join(escape(line) for line in address.splitlines())
    note_markup = "<br/>".join(escape(line) for line in note.splitlines())
    return address_markup, note_markup


def _payment_instructions_markup(
    customization: InvoiceCustomization | None = None,
) -> str:
    settings = customization or DEFAULT_INVOICE_CUSTOMIZATION
    address, note = _payment_parts(settings.payment_instructions)
    return f"<br/><b>{address}</b><br/><br/><i>{note}</i>"


def _uses_check_payment(document: InvoiceDocument) -> bool:
    return any(
        " ".join(project.application.split()).casefold() in CHECK_PAYMENT_APPLICATIONS
        for project in document.projects
    )


def _footer(canvas, document, *, font_name: str) -> None:
    canvas.saveState()
    canvas.setFont(font_name, 8)
    canvas.drawRightString(
        document.pagesize[0] - document.rightMargin,
        0.35 * inch,
        f"Page {canvas.getPageNumber()}",
    )
    canvas.restoreState()


def _logo(
    logo_path: Path | None,
    *,
    fallback_text: str,
    fallback_style: ParagraphStyle,
) -> object:
    if not logo_path or not logo_path.exists():
        return Paragraph(escape(fallback_text), fallback_style)
    logo = Image(str(logo_path))
    scale = min(2.2 * inch / logo.imageWidth, 0.9 * inch / logo.imageHeight, 1.0)
    logo.drawWidth = logo.imageWidth * scale
    logo.drawHeight = logo.imageHeight * scale
    logo.hAlign = "RIGHT"
    return logo


def _header(
    document: InvoiceDocument,
    pdf: SimpleDocTemplate,
    *,
    logo_path: Path | None,
    settings: InvoiceCustomization,
    regular_style: ParagraphStyle,
    detail_style: ParagraphStyle,
    title_style: ParagraphStyle,
    payment_bold_style: ParagraphStyle,
    payment_italic_style: ParagraphStyle,
) -> Table:
    body_size = settings.font_size
    logo_cell: list[object] = [
        _logo(
            logo_path,
            fallback_text=settings.main_title,
            fallback_style=regular_style,
        )
    ]
    if settings.payment_instructions and _uses_check_payment(document):
        address, note = _payment_parts(settings.payment_instructions)
        logo_cell.extend([Spacer(1, 8), Paragraph("<br/>" + address, payment_bold_style)])
        if note:
            logo_cell.extend([Spacer(1, 8), Paragraph(note, payment_italic_style)])

    detail_rows = [
        [Paragraph(escape(line), detail_style), "", ""]
        for line in _invoice_detail_lines(document, settings)
    ]
    table_style = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (1, -1), 0),
        ("BOTTOMPADDING", (2, 0), (2, -1), 2),
        ("ALIGN", (1, 0), (1, 0), "CENTER"),
        ("ALIGN", (2, 0), (2, 0), "RIGHT"),
        ("SPAN", (2, 0), (2, len(detail_rows))),
    ]
    if detail_rows:
        table_style.extend(
            [
                *(("SPAN", (0, row), (1, row)) for row in range(1, len(detail_rows) + 1)),
                ("ALIGN", (0, 1), (1, len(detail_rows)), "LEFT"),
            ]
        )
    title_markup = (
        f'<font size="{body_size + 10}"><b>{escape(settings.main_title)}</b></font><br/>'
        f'<font size="{body_size + 7}"><b>{escape(settings.subtitle)}</b></font>'
    )
    return Table(
        [
            [
                Spacer(1, 1),
                Paragraph(title_markup, title_style),
                logo_cell,
            ],
            *detail_rows,
        ],
        colWidths=[
            (pdf.width - 2.6 * inch) / 2,
            (pdf.width - 2.6 * inch) / 2,
            2.6 * inch,
        ],
        style=TableStyle(table_style),
    )


def _detail_value(column: str, line) -> object:
    start = line.get("Start_dt")
    values = {
        "date": start.strftime("%Y-%m-%d %H:%M") if hasattr(start, "strftime") else "",
        "user": line.get("User", ""),
        "description": line.get("Item_norm", ""),
        "type": line.get("Type", ""),
        "project": line.get("Project", ""),
        "quantity": line.get("Quantity", ""),
        "rate": line.get("Rate", ""),
        "subsidy": money(float(line.get("Subsidy", 0) or 0)),
        "cost": money(float(line.get("Cost", 0) or 0)),
    }
    return values[column]


def render_invoice_pdf(
    document: InvoiceDocument,
    output_path: Path,
    *,
    logo_path: Path | None = None,
    customization: InvoiceCustomization | None = None,
) -> Path:
    settings = customization or DEFAULT_INVOICE_CUSTOMIZATION
    regular_font, bold_font, italic_font = FONT_FAMILIES[settings.font_family]
    summary_color, detail_color = COLOR_THEMES[settings.color_theme]
    body_size = settings.font_size

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = SimpleDocTemplate(
        str(output_path),
        pagesize=landscape(letter),
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.6 * inch,
        title=settings.subtitle,
        author="NEMO Tools Hub",
    )
    styles = getSampleStyleSheet()
    normal = ParagraphStyle(
        "InvoiceNormal",
        parent=styles["Normal"],
        fontName=regular_font,
        fontSize=body_size + 6,
        leading=body_size + 6,
    )
    small = ParagraphStyle(
        "InvoiceSmall",
        parent=normal,
        fontName=regular_font,
        fontSize=body_size,
        leading=body_size + 2,
    )
    small_bold = ParagraphStyle("InvoiceSmallBold", parent=small, fontName=bold_font)
    header_details = ParagraphStyle(
        "InvoiceHeaderDetails",
        parent=normal,
        fontName=bold_font,
        alignment=TA_LEFT,
        leading=body_size + 3,
        spaceBefore=0,
        spaceAfter=0,
    )
    heading = ParagraphStyle(
        "InvoiceHeading",
        parent=styles["Heading2"],
        fontName=bold_font,
        fontSize=body_size + 3,
        leading=body_size + 5,
        spaceBefore=9,
        spaceAfter=4,
    )
    title = ParagraphStyle(
        "InvoiceTitle",
        parent=styles["Title"],
        fontName=regular_font,
        fontSize=body_size + 8,
        leading=body_size + 10,
        spaceAfter=6,
        alignment=TA_CENTER,
    )
    payment_bold = ParagraphStyle(
        "InvoicePaymentBold",
        parent=small,
        fontName=bold_font,
        fontSize=body_size + 4,
        leading=body_size + 4,
        alignment=TA_LEFT,
    )
    payment_italic = ParagraphStyle(
        "InvoicePaymentItalic",
        parent=small,
        fontName=italic_font,
        fontSize=body_size + 4,
        leading=body_size + 4,
        alignment=TA_LEFT,
    )

    def paragraph(value: object, style=small):
        return Paragraph(escape(str(value or "")), style)

    story: list[object] = [
        _header(
            document,
            pdf,
            logo_path=logo_path,
            settings=settings,
            regular_style=normal,
            detail_style=header_details,
            title_style=title,
            payment_bold_style=payment_bold,
            payment_italic_style=payment_italic,
        ),
        Spacer(1, 8),
    ]

    lab_labels = dict(settings.lab_sections)
    configured_labs = [lab for lab, _ in settings.lab_sections if lab in document.lab_totals]
    remaining_labs = sorted(set(document.lab_totals) - set(configured_labs))
    summary_labs = [*configured_labs, *remaining_labs]
    summary = [["Lab", "Total Cost"]]
    summary.extend(
        [lab_labels.get(lab, lab), money(document.lab_totals[lab])] for lab in summary_labs
    )
    if settings.include_access_fee:
        summary.append(["Access fee", money(document.access_fee)])
    summary.append(["TOTAL", money(document.invoice_total)])
    story.append(
        Table(
            summary,
            colWidths=[2.6 * inch, 1.4 * inch],
            hAlign="LEFT",
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(summary_color)),
                    ("FONTNAME", (0, 0), (-1, -1), regular_font),
                    ("FONTSIZE", (0, 0), (-1, -1), body_size),
                    ("FONTNAME", (0, 0), (-1, 0), bold_font),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                    ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                    ("FONTNAME", (0, -1), (-1, -1), bold_font),
                ]
            ),
        )
    )

    detail_columns = [
        column for column in settings.detail_columns if column != "subsidy" or document.show_subsidy
    ]
    if detail_columns:
        detail_weights = [DETAIL_COLUMN_SPECS[column][1] for column in detail_columns]
        detail_total_weight = sum(detail_weights)
        numeric_alignments = [
            ("ALIGN", (index, 1), (index, -1), "RIGHT")
            for index, column in enumerate(detail_columns)
            if column in NUMERIC_DETAIL_COLUMNS
        ]
        for lab, display_name in settings.lab_sections:
            lines = document.lines_for_lab(lab)
            if lines.empty:
                continue
            story.append(paragraph(display_name, heading))
            headers = [DETAIL_COLUMN_SPECS[column][0] for column in detail_columns]
            rows: list[list[object]] = [[paragraph(name, small_bold) for name in headers]]
            for _, line in lines.iterrows():
                rows.append([paragraph(_detail_value(column, line)) for column in detail_columns])
            story.append(
                Table(
                    rows,
                    colWidths=[
                        pdf.width * weight / detail_total_weight for weight in detail_weights
                    ],
                    repeatRows=1,
                    style=TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(detail_color)),
                            ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            *numeric_alignments,
                        ]
                    ),
                )
            )

    story.append(
        paragraph(
            "Project fees summary" if settings.include_access_fee else "Project summary",
            heading,
        )
    )
    project_labs = list(settings.lab_sections)
    project_headers = [
        "Project",
        "Type",
        *(display for _, display in project_labs),
        "Staff",
    ]
    if settings.include_access_fee:
        project_headers.append("Fee")
    project_headers.append("Total")
    project_rows: list[list[object]] = [[paragraph(value, small_bold) for value in project_headers]]
    for project in sorted(document.projects, key=lambda value: (-value.total, value.project)):
        values = [
            project.project,
            project.application,
            *(money(project.lab_totals.get(lab, 0.0)) for lab, _ in project_labs),
            money(project.staff_time),
        ]
        if settings.include_access_fee:
            values.append(money(project.access_fee))
        values.append(money(project.total))
        project_rows.append([paragraph(value) for value in values])
    project_weights = [0.31, 0.08, *(0.08 for _ in project_labs), 0.075]
    if settings.include_access_fee:
        project_weights.append(0.09)
    project_weights.append(0.10)
    project_total_weight = sum(project_weights)
    story.append(
        Table(
            project_rows,
            colWidths=[pdf.width * value / project_total_weight for value in project_weights],
            repeatRows=1,
            style=TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(detail_color)),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.black),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
                ]
            ),
        )
    )
    story.append(Spacer(1, 8))

    total_rows = [["Usage charges total", money(document.usage_total)]]
    if settings.include_access_fee:
        total_rows.append(["Access fee", money(document.access_fee)])
    total_rows.append(["Invoice total", money(document.invoice_total)])
    story.append(
        Table(
            total_rows,
            colWidths=[3 * inch, 1.2 * inch],
            hAlign="LEFT",
            style=TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                    ("FONTNAME", (0, 0), (-1, -1), regular_font),
                    ("FONTSIZE", (0, 0), (-1, -1), body_size),
                    ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                    ("FONTNAME", (0, -1), (-1, -1), bold_font),
                ]
            ),
        )
    )
    footer = partial(_footer, font_name=regular_font)
    pdf.build(story, onFirstPage=footer, onLaterPages=footer)
    return output_path
