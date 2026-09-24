from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from nemo_app.billing.constants import ACCESS_FEE_BY_APPLICATION, DESIRED_LAB_ORDER

FONT_OPTIONS = {
    "Helvetica": "Helvetica",
    "Times": "Times",
    "Courier": "Courier",
}
FONT_SIZE_OPTIONS = {
    7: "Compact",
    8: "Standard",
    9: "Comfortable",
    10: "Large",
}
COLOR_THEME_OPTIONS = {
    "cni": "CNI orange and blue",
    "ocean": "Ocean blue",
    "forest": "Forest green",
    "grayscale": "Grayscale",
}
BILLING_FIELD_OPTIONS = {
    "pi": "PI",
    "email": "Email",
    "billing_month": "Billing month",
    "invoice_number": "Invoice number",
    "generated": "Generated date",
}
DETAIL_COLUMN_OPTIONS = {
    "date": "Date",
    "user": "User",
    "description": "Description",
    "type": "Type",
    "project": "Project",
    "quantity": "Quantity",
    "rate": "Rate",
    "subsidy": "Subsidy when applicable",
    "cost": "Cost",
}
INVOICE_NUMBER_OPTIONS = {
    "timestamped": "CNI-YYMM-DDHHMM-###",
    "monthly": "CNI-YYMM-###",
    "calendar": "INV-YYYYMM-####",
    "yearly": "INV-YYYY-#####",
}

DEFAULT_MAIN_TITLE = "Columbia Nano Initiative"
DEFAULT_SUBTITLE = "Facility Usage Invoice"
DEFAULT_LAB_SECTIONS = tuple((lab, lab) for lab in DESIRED_LAB_ORDER)
DEFAULT_BILLING_FIELDS = tuple(BILLING_FIELD_OPTIONS)
DEFAULT_DETAIL_COLUMNS = tuple(DETAIL_COLUMN_OPTIONS)
DEFAULT_PAYMENT_INSTRUCTIONS = """Please Mail Checks To:
Columbia Nano Initiative
530 W 120th Street, RM 1001
Mail Code 8903 - CEPSR Building
New York, NY 10027
Email: cnibilling@columbia.edu

Checks Only! Make payable to Columbia University.
Payment due within 30 days of receipt."""


def parse_lab_sections(value: str) -> tuple[tuple[str, str], ...]:
    sections: list[tuple[str, str]] = []
    seen: set[str] = set()
    for raw_line in value.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        source, separator, display = line.partition("=")
        source = source.strip()
        display = display.strip() if separator else source
        if not source or not display:
            raise ValueError("Each lab line must use Source name = Display name.")
        if len(source) > 100 or len(display) > 100:
            raise ValueError("Lab names must contain 100 characters or fewer.")
        key = source.casefold()
        if key in seen:
            raise ValueError(f"Lab {source!r} is listed more than once.")
        seen.add(key)
        sections.append((source, display))
    if len(sections) > 30:
        raise ValueError("Configure no more than 30 lab sections.")
    return tuple(sections)


def format_lab_sections(sections: Sequence[Sequence[str]]) -> str:
    return "\n".join(
        source if source == display else f"{source} = {display}" for source, display in sections
    )


@dataclass(frozen=True, slots=True)
class InvoiceCustomization:
    main_title: str = DEFAULT_MAIN_TITLE
    subtitle: str = DEFAULT_SUBTITLE
    lab_sections: tuple[tuple[str, str], ...] = DEFAULT_LAB_SECTIONS
    include_access_fee: bool = True
    access_fees: dict[str, float] = field(default_factory=lambda: dict(ACCESS_FEE_BY_APPLICATION))
    billing_fields: tuple[str, ...] = DEFAULT_BILLING_FIELDS
    font_family: str = "Helvetica"
    font_size: int = 8
    color_theme: str = "cni"
    detail_columns: tuple[str, ...] = DEFAULT_DETAIL_COLUMNS
    invoice_number_format: str = "timestamped"
    payment_instructions: str = DEFAULT_PAYMENT_INSTRUCTIONS

    def __post_init__(self) -> None:
        if not self.main_title.strip() or len(self.main_title) > 120:
            raise ValueError("The PDF title must contain 1 to 120 characters.")
        if not self.subtitle.strip() or len(self.subtitle) > 120:
            raise ValueError("The PDF subtitle must contain 1 to 120 characters.")
        if self.font_family not in FONT_OPTIONS:
            raise ValueError("Choose a supported PDF font.")
        if self.font_size not in FONT_SIZE_OPTIONS:
            raise ValueError("Choose a supported PDF font size.")
        if self.color_theme not in COLOR_THEME_OPTIONS:
            raise ValueError("Choose a supported PDF color theme.")
        if self.invoice_number_format not in INVOICE_NUMBER_OPTIONS:
            raise ValueError("Choose a supported invoice number format.")
        if len(self.lab_sections) > 30:
            raise ValueError("Configure no more than 30 lab sections.")
        lab_names: set[str] = set()
        for source, display in self.lab_sections:
            if not source.strip() or not display.strip():
                raise ValueError("Lab source and display names cannot be empty.")
            if len(source) > 100 or len(display) > 100:
                raise ValueError("Lab names must contain 100 characters or fewer.")
            normalized_source = source.casefold()
            if normalized_source in lab_names:
                raise ValueError(f"Lab {source!r} is listed more than once.")
            lab_names.add(normalized_source)
        invalid_fields = set(self.billing_fields) - set(BILLING_FIELD_OPTIONS)
        if invalid_fields:
            raise ValueError("Unknown billing information fields were selected.")
        invalid_columns = set(self.detail_columns) - set(DETAIL_COLUMN_OPTIONS)
        if invalid_columns:
            raise ValueError("Unknown invoice columns were selected.")
        if not self.detail_columns:
            raise ValueError("Select at least one invoice detail column.")
        if len(set(self.detail_columns)) != len(self.detail_columns):
            raise ValueError("Each invoice detail column can be selected only once.")
        if len(self.payment_instructions) > 2000:
            raise ValueError("Payment instructions must contain 2,000 characters or fewer.")
        for application, amount in self.access_fees.items():
            if not application.strip() or float(amount) < 0 or float(amount) > 1_000_000:
                raise ValueError("Access fees must be between $0 and $1,000,000.")

    @classmethod
    def from_mapping(cls, values: Mapping[str, object] | None) -> InvoiceCustomization:
        if not values:
            return cls()
        lab_sections = tuple(
            (str(section[0]), str(section[1]))
            for section in values.get("lab_sections", DEFAULT_LAB_SECTIONS)  # type: ignore[arg-type]
        )
        access_fees = {
            str(application): float(amount)
            for application, amount in dict(
                values.get("access_fees", ACCESS_FEE_BY_APPLICATION)  # type: ignore[arg-type]
            ).items()
        }
        return cls(
            main_title=str(values.get("main_title", DEFAULT_MAIN_TITLE)).strip(),
            subtitle=str(values.get("subtitle", DEFAULT_SUBTITLE)).strip(),
            lab_sections=lab_sections,
            include_access_fee=bool(values.get("include_access_fee", True)),
            access_fees=access_fees,
            billing_fields=tuple(
                str(value)
                for value in values.get("billing_fields", DEFAULT_BILLING_FIELDS)  # type: ignore[union-attr]
            ),
            font_family=str(values.get("font_family", "Helvetica")),
            font_size=int(values.get("font_size", 8)),
            color_theme=str(values.get("color_theme", "cni")),
            detail_columns=tuple(
                str(value)
                for value in values.get("detail_columns", DEFAULT_DETAIL_COLUMNS)  # type: ignore[union-attr]
            ),
            invoice_number_format=str(values.get("invoice_number_format", "timestamped")),
            payment_instructions=str(
                values.get("payment_instructions", DEFAULT_PAYMENT_INSTRUCTIONS)
            ).strip(),
        )


DEFAULT_INVOICE_CUSTOMIZATION = InvoiceCustomization()
