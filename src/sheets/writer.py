"""
Обратная запись данных о выставленном счёте в Google Sheets.
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import date
from typing import Iterable

from gspread.utils import rowcol_to_a1

from src.sheets.reader import _load_schema, _parse_sheet_url, get_sheets_client

logger = logging.getLogger(__name__)

_REQUIRED_HEADERS = [
    "Дата",
    "Контрагент",
    "Примечание",
    "Структура",
    "Операция",
    "Объект",
    "Документ",
]


def _build_sheet_row_hash(
    *,
    date_str: str,
    counterparty: str,
    note: str,
    structure: str,
    operation: str,
    object_count: str,
) -> str:
    return hashlib.sha256(
        f"{date_str}|{counterparty}|{note}|{structure}|{operation}|{object_count}".encode("utf-8")
    ).hexdigest()


def mark_document_in_sheet(
    *,
    sheet_row_hashes: Iterable[str],
    invoice_number: str,
    invoice_date: date,
    sheet_url: str | None = None,
    sheet_name: str | None = None,
) -> int:
    """
    Запись в колонку "Документ" для строк, вошедших в выставленный счёт.

    Записывает значение формата: "Счет <НОМЕР> от <ДАТА>".
    """
    hashes = {h for h in sheet_row_hashes if h}
    if not hashes:
        return 0

    schema = _load_schema()
    url = sheet_url or os.environ.get("GOOGLE_SHEET_URL") or schema.get("google_sheet_url")
    if not url:
        logger.warning("Sheets: GOOGLE_SHEET_URL не задан, запись в колонку 'Документ' пропущена")
        return 0

    client = get_sheets_client()
    sheet_id, gid = _parse_sheet_url(url)
    spreadsheet = client.open_by_key(sheet_id)

    if sheet_name:
        worksheet = spreadsheet.worksheet(sheet_name)
    elif gid is not None:
        worksheet = spreadsheet.get_worksheet_by_id(gid)
    else:
        worksheet = spreadsheet.sheet1

    values = worksheet.get_all_values()
    if not values:
        logger.warning("Sheets: таблица пуста, запись в колонку 'Документ' пропущена")
        return 0

    header_row_idx = None
    for idx, row in enumerate(values):
        cells = [str(c).strip() if c else "" for c in row]
        if "Дата" in cells and "Контрагент" in cells:
            header_row_idx = idx
            break
    if header_row_idx is None:
        logger.warning("Sheets: строка заголовков не найдена, запись в колонку 'Документ' пропущена")
        return 0

    header_row = [str(c).strip() if c else "" for c in values[header_row_idx]]
    col_indices = {h: i for i, h in enumerate(header_row)}
    missing = [h for h in _REQUIRED_HEADERS if h not in col_indices]
    if missing:
        logger.warning("Sheets: не найдены колонки %s, запись в 'Документ' пропущена", missing)
        return 0

    doc_col_1b = col_indices["Документ"] + 1
    doc_text = f"Счет {invoice_number} от {invoice_date.strftime('%d.%m.%Y')}"

    updates = []
    for row_num, row in enumerate(values[header_row_idx + 1 :], start=header_row_idx + 2):
        def _cell(header: str) -> str:
            idx = col_indices[header]
            return str(row[idx]).strip() if len(row) > idx and row[idx] is not None else ""

        date_str = _cell("Дата")
        if not date_str:
            continue

        counterparty = _cell("Контрагент")
        note = _cell("Примечание")
        structure = _cell("Структура")
        operation = _cell("Операция")
        object_count = _cell("Объект") or "1"

        row_hash = _build_sheet_row_hash(
            date_str=date_str,
            counterparty=counterparty,
            note=note,
            structure=structure,
            operation=operation,
            object_count=object_count,
        )
        if row_hash not in hashes:
            continue

        current_doc = _cell("Документ")
        if current_doc == doc_text:
            continue

        updates.append(
            {
                "range": rowcol_to_a1(row_num, doc_col_1b),
                "values": [[doc_text]],
            }
        )

    if not updates:
        return 0

    worksheet.batch_update(updates)
    logger.info("Sheets: обновлена колонка 'Документ' для %s строк", len(updates))
    return len(updates)
