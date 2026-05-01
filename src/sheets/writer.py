"""
Обратная запись данных о выставленном счёте в Google Sheets.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from collections import Counter
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable

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

_CASHLESS_EXPENSE_SHEET_NAME = "Безнал-Расходы"
_CASHLESS_EXPENSE_HEADERS = [
    "Месяц",
    "Дата",
    "Сумма",
    "Контрагент",
    "Назначение платежа",
    "Расчетный счет",
    "Объект",
    "Подразделение",
    "Статья затрат",
    "Подразделение",
    "Статья затрат",
]
_CASHLESS_EXPENSE_KEY_HEADERS = [
    "Месяц",
    "Дата",
    "Сумма",
    "Контрагент",
    "Назначение платежа",
    "Расчетный счет",
]
_MONEY_Q = Decimal("0.01")


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


def _normalize_sheet_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\u00a0", " ").split())


def _parse_sheet_money(value: Any) -> Decimal | None:
    raw = _normalize_sheet_text(value)
    if not raw:
        return None

    cleaned = raw.replace(" ", "").replace("\u00a0", "").replace("₽", "")
    cleaned = re.sub(r"[^\d,.\-]", "", cleaned)
    if not cleaned:
        return None

    has_comma = "," in cleaned
    has_dot = "." in cleaned
    if has_comma and has_dot:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif has_comma:
        cleaned = cleaned.replace(",", ".")

    if cleaned.count(".") > 1:
        parts = cleaned.split(".")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return Decimal(cleaned).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None


def _normalize_sheet_money_key(value: Any) -> str:
    parsed = _parse_sheet_money(value)
    if parsed is None:
        return _normalize_sheet_text(value)
    return str(parsed)


def _find_header_row(values: list[list[Any]], required_headers: Iterable[str]) -> tuple[int, list[str]] | None:
    required = set(required_headers)
    for idx, row in enumerate(values):
        cells = [str(c).strip() if c else "" for c in row]
        if required.issubset(set(cells)):
            return idx, cells
    return None


def _cashless_expense_key_from_values(
    *,
    month: Any,
    date_str: Any,
    amount: Any,
    counterparty: Any,
    pay_purpose: Any,
    account_label: Any,
) -> tuple[str, str, str, str, str, str]:
    return (
        _normalize_sheet_text(month),
        _normalize_sheet_text(date_str),
        _normalize_sheet_money_key(amount),
        _normalize_sheet_text(counterparty),
        _normalize_sheet_text(pay_purpose),
        _normalize_sheet_text(account_label),
    )


def _cashless_expense_key_from_sheet_row(row: list[Any], col_indices: dict[str, int]) -> tuple[str, str, str, str, str, str]:
    def _cell(header: str) -> str:
        idx = col_indices[header]
        return str(row[idx]).strip() if len(row) > idx and row[idx] is not None else ""

    return _cashless_expense_key_from_values(
        month=_cell("Месяц"),
        date_str=_cell("Дата"),
        amount=_cell("Сумма"),
        counterparty=_cell("Контрагент"),
        pay_purpose=_cell("Назначение платежа"),
        account_label=_cell("Расчетный счет"),
    )


def _cashless_expense_values(row: dict[str, Any]) -> list[Any]:
    return [
        row.get("month", ""),
        row.get("date", ""),
        row.get("amount", ""),
        row.get("counterparty", ""),
        row.get("pay_purpose", ""),
        row.get("account_label", ""),
        row.get("object", ""),
        row.get("department", ""),
        row.get("cost_article", ""),
        row.get("department_2", ""),
        row.get("cost_article_2", ""),
    ]


def _cashless_expense_key_from_row(row: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return _cashless_expense_key_from_values(
        month=row.get("month", ""),
        date_str=row.get("date", ""),
        amount=row.get("amount", ""),
        counterparty=row.get("counterparty", ""),
        pay_purpose=row.get("pay_purpose", ""),
        account_label=row.get("account_label", ""),
    )


def append_cashless_expense_rows(
    rows: Iterable[dict[str, Any]],
    *,
    sheet_url: str | None = None,
    sheet_name: str | None = None,
) -> dict[str, Any]:
    """
    Добавление исходящих операций в лист безналичных расходов.

    Дедупликация перед append выполняется по банковским колонкам:
    месяц, дата, сумма, контрагент, назначение платежа, расчетный счет.
    """
    prepared_rows = list(rows)
    if not prepared_rows:
        return {"appended": 0, "skipped_existing": 0, "processed_operation_ids": []}

    schema = _load_schema()
    url = sheet_url or os.environ.get("GOOGLE_SHEET_URL") or schema.get("google_sheet_url")
    if not url:
        logger.warning("Sheets: GOOGLE_SHEET_URL не задан, запись в '%s' пропущена", _CASHLESS_EXPENSE_SHEET_NAME)
        return {"appended": 0, "skipped_existing": 0, "processed_operation_ids": []}

    target_sheet_name = (
        sheet_name
        or os.environ.get("GOOGLE_CASHLESS_EXPENSES_SHEET_NAME")
        or schema.get("google_cashless_expenses_sheet_name")
        or _CASHLESS_EXPENSE_SHEET_NAME
    )
    target_sheet_name = str(target_sheet_name or "").strip() or _CASHLESS_EXPENSE_SHEET_NAME

    client = get_sheets_client()
    sheet_id, _ = _parse_sheet_url(url)
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.worksheet(target_sheet_name)

    values = worksheet.get_all_values()
    if not values:
        logger.warning("Sheets: лист '%s' пуст, запись расходов пропущена", target_sheet_name)
        return {"appended": 0, "skipped_existing": 0, "processed_operation_ids": []}

    header = _find_header_row(values, _CASHLESS_EXPENSE_KEY_HEADERS)
    if header is None:
        logger.warning("Sheets: в листе '%s' не найдены заголовки расходов", target_sheet_name)
        return {"appended": 0, "skipped_existing": 0, "processed_operation_ids": []}

    header_row_idx, header_row = header
    col_indices: dict[str, int] = {}
    for idx, header_name in enumerate(header_row):
        if header_name in _CASHLESS_EXPENSE_KEY_HEADERS and header_name not in col_indices:
            col_indices[header_name] = idx

    missing = [h for h in _CASHLESS_EXPENSE_KEY_HEADERS if h not in col_indices]
    if missing:
        logger.warning("Sheets: в листе '%s' не найдены колонки %s", target_sheet_name, missing)
        return {"appended": 0, "skipped_existing": 0, "processed_operation_ids": []}

    existing_keys: Counter[tuple[str, str, str, str, str, str]] = Counter()
    for row in values[header_row_idx + 1 :]:
        key = _cashless_expense_key_from_sheet_row(row, col_indices)
        if any(key):
            existing_keys[key] += 1

    rows_to_append: list[list[Any]] = []
    appended_ids: list[int] = []
    skipped_ids: list[int] = []

    for row in prepared_rows:
        operation_row_id = int(row.get("operation_row_id") or 0)
        key = _cashless_expense_key_from_row(row)
        if existing_keys[key] > 0:
            existing_keys[key] -= 1
            if operation_row_id:
                skipped_ids.append(operation_row_id)
            continue

        rows_to_append.append(_cashless_expense_values(row))
        if operation_row_id:
            appended_ids.append(operation_row_id)

    if rows_to_append:
        worksheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")

    processed_ids = appended_ids + skipped_ids
    logger.info(
        "Sheets: лист '%s', добавлено расходов=%s, уже было=%s",
        target_sheet_name,
        len(appended_ids),
        len(skipped_ids),
    )
    return {
        "appended": len(appended_ids),
        "skipped_existing": len(skipped_ids),
        "processed_operation_ids": processed_ids,
    }
