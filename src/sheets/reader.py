"""
Чтение работ из Google Sheets для синхронизации в MySQL.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import gspread
from google.oauth2.service_account import Credentials

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials" / "google_service_account.json"

logger = logging.getLogger(__name__)

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


def get_sheets_client() -> gspread.Client:
    """Создание клиента gspread."""
    creds_path = os.environ.get("GOOGLE_CREDENTIALS_PATH", str(CREDENTIALS_PATH))
    credentials = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return gspread.authorize(credentials)


def read_works(
    sheet_url: str | None = None,
    sheet_name: str | None = None,
) -> list[dict]:
    """
    Чтение всех строк-работ из Google Sheets.

    Возвращает список словарей с ключами:
    date, counterparty_name, note, structure, operation, object_count, sheet_row_hash
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
    required_headers = ["Дата", "Контрагент", "Примечание", "Структура", "Операция", "Объект"]
    try:
        records = worksheet.get_all_records(expected_headers=required_headers)
    except Exception:
        # Если expected_headers не совпадают с таблицей — читаем все значения и парсим вручную.
        values = worksheet.get_all_values()
        if not values or len(values) < 2:
            return []
        header_row = values[0]
        col_indices = {h: i for i, h in enumerate(header_row) if h in required_headers}
        records = []
        for row in values[1:]:
            rec = {h: row[col_indices[h]] if h in col_indices and len(row) > col_indices[h] else "" for h in required_headers}
            records.append(rec)

    works = []
    for row in records:
        date_val = row.get("Дата", "")
        if not date_val or not str(date_val).strip():
            continue
        date_str = str(date_val).strip()
        counterparty = str(row.get("Контрагент", "") or "").strip()
        note = str(row.get("Примечание", "") or "").strip()
        structure = str(row.get("Структура", "") or "").strip()
        operation = str(row.get("Операция", "") or "").strip()
        object_count = str(row.get("Объект", "") or "1").strip() or "1"

        sheet_row_hash = hashlib.sha256(
            f"{date_str}|{counterparty}|{note}|{structure}|{operation}|{object_count}".encode("utf-8")
        ).hexdigest()

        works.append({
            "date": date_str,
            "counterparty_name": counterparty,
            "note": note,
            "structure": structure,
            "operation": operation,
            "object_count": object_count,
            "sheet_row_hash": sheet_row_hash,
        })
    return works
