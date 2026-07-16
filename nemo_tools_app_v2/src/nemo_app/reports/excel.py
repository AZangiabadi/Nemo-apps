from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl.styles import Alignment

from nemo_app.invoices.excel_styles import (
    BORDER,
    CURRENCY_FORMAT,
    autosize_columns,
    style_table_header,
)


def write_frame(
    writer: pd.ExcelWriter,
    sheet_name: str,
    frame: pd.DataFrame,
    *,
    currency_columns: set[str] | None = None,
    date_columns: set[str] | None = None,
) -> None:
    frame.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    worksheet = writer.sheets[sheet_name[:31]]
    style_table_header(worksheet, 1, 1, [str(column) for column in frame.columns])
    currency_columns = currency_columns or set()
    date_columns = date_columns or set()
    for column_index, column_name in enumerate(frame.columns, 1):
        for row in range(2, worksheet.max_row + 1):
            cell = worksheet.cell(row, column_index)
            cell.border = BORDER
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if column_name in currency_columns:
                cell.number_format = CURRENCY_FORMAT
            elif column_name in date_columns:
                cell.number_format = "yyyy-mm-dd hh:mm:ss"
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions
    worksheet.sheet_view.showGridLines = False
    autosize_columns(worksheet, minimum=11, maximum=58)


def save_frames(
    output_path: Path,
    sheets: list[tuple[str, pd.DataFrame, set[str], set[str]]],
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, frame, currency, dates in sheets:
            write_frame(
                writer,
                name,
                frame,
                currency_columns=currency,
                date_columns=dates,
            )
    return output_path
