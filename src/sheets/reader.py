"""
Чтение данных из Google Sheets для синхронизации в MySQL.
"""
from __future__ import annotations

import hashlib
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import gspread
from google.oauth2.service_account import Credentials

from src.sheets.waybill_notes import extract_waybill_token

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials" / "google_service_account.json"

logger = logging.getLogger(__name__)
_ZERO_WIDTH_CHARS = "\u200b\u200c\u200d\ufeff"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _load_schema() -> dict:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_sheet_url(url: str) -> tuple[str, int | None]:
    """Извлечение ID таблицы и gid листа из URL."""
    sheet_id = ""
    gid = None
    if "/d/" in url:
        start = url.find("/d/") + 3
        rest = url[start:]
        sheet_id = rest.split("/")[0].split("?")[0]
    else:
        sheet_id = url.strip()
    parsed = urlparse(url)
    for part in (parsed.query, parsed.fragment):
        if part and "gid=" in part:
            params = parse_qs(part)
            if "gid" in params:
                gid = int(params["gid"][0])
                break
    return sheet_id, gid


def _parse_date_safe(date_str: str) -> datetime | None:
    """Парсинг DD.MM.YYYY, возвращает None при ошибке."""
    try:
        return datetime.strptime(date_str.strip(), "%d.%m.%Y")
    except (ValueError, AttributeError):
        return None


def _clean_cell(value) -> str:
    """Нормализация текста ячейки для стабильного хеша и парсинга."""
    raw = str(value or "").replace("\u00a0", " ")
    for char in _ZERO_WIDTH_CHARS:
        raw = raw.replace(char, "")
    return raw.strip()


def _first_non_empty(row: dict, headers: list[str]) -> str:
    for header in headers:
        value = _clean_cell(row.get(header, ""))
        if value:
            return value
    return ""


def get_sheets_client() -> gspread.Client:
    """Создание клиента gspread."""
    creds_path = os.environ.get("GOOGLE_CREDENTIALS_PATH", str(CREDENTIALS_PATH))
    credentials = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return gspread.authorize(credentials)


def read_works(
    sheet_url: str | None = None,
    sheet_name: str | None = None,
    last_date: date | None = None,
) -> list[dict]:
    """
    Чтение строк-работ из Google Sheets.
    При last_date обрабатываются только строки с датой >= last_date.

    Возвращает список словарей с ключами:
    date, counterparty_name, note, structure, operation, object_count, revenue,
    sheet_row_hash, waybill_file_token
    """
    schema = _load_schema()
    url = sheet_url or os.environ.get("GOOGLE_SHEET_URL") or schema.get("google_sheet_url")
    if not url:
        raise ValueError("Укажите GOOGLE_SHEET_URL в .env или google_sheet_url в config/schema.json")

    client = get_sheets_client()
    sheet_id, gid = _parse_sheet_url(url)
    spreadsheet = client.open_by_key(sheet_id)

    if sheet_name:
        worksheet = spreadsheet.worksheet(sheet_name)
    elif gid is not None:
        worksheet = spreadsheet.get_worksheet_by_id(gid)
    else:
        worksheet = spreadsheet.sheet1

    # Только нужные колонки — обходит дубликаты/пустые заголовки в строке заголовков.
    revenue_headers = ["Выручка", "Приход"]
    required_headers = [
        "Дата",
        "Контрагент",
        "Примечание",
        "Структура",
        "Операция",
        "Объект",
        *revenue_headers,
    ]
    try:
        records = worksheet.get_all_records(expected_headers=required_headers)
    except Exception:
        # Если expected_headers не совпадают — читаем вручную: ищем строку с заголовками.
        values = worksheet.get_all_values()
        if not values or len(values) < 2:
            return []
        header_row_idx = None
        for idx, row in enumerate(values):
            cells = [str(c).strip() if c else "" for c in row]
            if "Дата" in cells:
                header_row_idx = idx
                break
        if header_row_idx is None:
            return []
        header_row = [str(c).strip() if c else "" for c in values[header_row_idx]]
        col_indices = {h: i for i, h in enumerate(header_row) if h in required_headers}
        records = []
        for row in values[header_row_idx + 1 :]:
            rec = {h: row[col_indices[h]] if h in col_indices and len(row) > col_indices[h] else "" for h in required_headers}
            records.append(rec)

    works = []
    for row in records:
        date_val = row.get("Дата", "")
        if not date_val or not str(date_val).strip():
            continue
        date_str = _clean_cell(date_val)
        parsed = _parse_date_safe(date_str)
        if last_date and parsed is not None and parsed.date() < last_date:
            continue
        counterparty = _clean_cell(row.get("Контрагент", ""))
        raw_note = _clean_cell(row.get("Примечание", ""))
        note, waybill_file_token = extract_waybill_token(raw_note)
        structure = _clean_cell(row.get("Структура", ""))
        operation = _clean_cell(row.get("Операция", ""))
        if operation != "Поступление по основной деятельности":
            continue
        object_count = _clean_cell(row.get("Объект", "") or "1") or "1"
        revenue = _first_non_empty(row, revenue_headers)

        sheet_row_hash = hashlib.sha256(
            f"{date_str}|{counterparty}|{raw_note}|{structure}|{operation}|{object_count}".encode("utf-8")
        ).hexdigest()

        works.append({
            "date": date_str,
            "counterparty_name": counterparty,
            "note": note,
            "structure": structure,
            "operation": operation,
            "object_count": object_count,
            "revenue": revenue,
            "sheet_row_hash": sheet_row_hash,
            "waybill_file_token": waybill_file_token,
        })
    return works


def read_counterparties(
    sheet_url: str | None = None,
    sheet_name: str | None = None,
) -> list[dict]:
    """
    Чтение контрагентов из вкладки Google Sheets.

    Ожидаемые заголовки:
    - ИНН контрагента
    - КПП контрагента
    - Email
    - Email бухгалтера
    - Сокращенное наименование
    - Наименование контрагента
    - Договор (опционально)
    - Напоминания по неоплаченным счетам (опционально)

    Возвращает список словарей с ключами:
    inn, kpp, email, email_accountant, short_name, name, contract,
    payment_reminders_enabled
    """
    schema = _load_schema()
    url = sheet_url or os.environ.get("GOOGLE_SHEET_URL") or schema.get("google_sheet_url")
    if not url:
        raise ValueError("Укажите GOOGLE_SHEET_URL в .env или google_sheet_url в config/schema.json")

    cp_sheet_name = (
        sheet_name
        or os.environ.get("GOOGLE_COUNTERPARTIES_SHEET_NAME")
        or schema.get("google_counterparties_sheet_name")
        or "Контрагенты"
    )
    cp_sheet_name = str(cp_sheet_name or "").strip() or "Контрагенты"

    client = get_sheets_client()
    sheet_id, _ = _parse_sheet_url(url)
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.worksheet(cp_sheet_name)

    payment_reminders_headers = [
        "Напоминания по неоплаченным счетам",
        "Напоминания об оплате",
        "payment_reminders_enabled",
    ]
    required_headers = [
        "ИНН контрагента",
        "КПП контрагента",
        "Email",
        "Email бухгалтера",
        "Сокращенное наименование",
        "Наименование контрагента",
        "Договор",
        *payment_reminders_headers,
    ]
    try:
        records = worksheet.get_all_records(expected_headers=required_headers)
    except Exception:
        values = worksheet.get_all_values()
        if not values or len(values) < 2:
            return []
        header_row_idx = None
        for idx, row in enumerate(values):
            cells = [str(c).strip() if c else "" for c in row]
            if (
                "ИНН контрагента" in cells
                and "Сокращенное наименование" in cells
                and "Наименование контрагента" in cells
            ):
                header_row_idx = idx
                break
        if header_row_idx is None:
            return []
        header_row = [str(c).strip() if c else "" for c in values[header_row_idx]]
        col_indices = {h: i for i, h in enumerate(header_row) if h in required_headers}
        records = []
        for row in values[header_row_idx + 1 :]:
            rec = {h: row[col_indices[h]] if h in col_indices and len(row) > col_indices[h] else "" for h in required_headers}
            records.append(rec)

    counterparties = []
    for row in records:
        inn = str(row.get("ИНН контрагента", "") or "").strip()
        kpp = str(row.get("КПП контрагента", "") or "").strip()
        email = str(row.get("Email", "") or "").strip()
        email_accountant = str(row.get("Email бухгалтера", "") or "").strip()
        short_name = str(row.get("Сокращенное наименование", "") or "").strip()
        name = str(row.get("Наименование контрагента", "") or "").strip()
        contract = str(row.get("Договор", "") or "").strip()
        payment_reminders_enabled = ""
        for header in payment_reminders_headers:
            raw_payment_reminders_enabled = row.get(header, "")
            if raw_payment_reminders_enabled is None:
                raw_payment_reminders_enabled = ""
            payment_reminders_enabled = str(raw_payment_reminders_enabled).strip()
            if payment_reminders_enabled:
                break

        if not any([inn, kpp, email, email_accountant, short_name, name, contract]):
            continue

        counterparties.append(
            {
                "inn": inn,
                "kpp": kpp,
                "email": email,
                "email_accountant": email_accountant,
                "short_name": short_name,
                "name": name,
                "contract": contract,
                "payment_reminders_enabled": payment_reminders_enabled,
            }
        )
    return counterparties
