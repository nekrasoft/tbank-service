"""
Синхронизация работ из Google Sheets в MySQL.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from src.db.connection import get_session
from src.db.repos import works as works_repo
from src.sheets.reader import read_works

logger = logging.getLogger(__name__)


def _parse_date(date_str: str) -> date | None:
    """Парсинг даты DD.MM.YYYY в date."""
    try:
        return datetime.strptime(date_str.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None


def _parse_revenue(value: str | None) -> Decimal | None:
    """Парсинг суммы выручки в Decimal(14,2)."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    cleaned = raw.replace("\u00a0", "").replace(" ", "")
    cleaned = cleaned.replace("₽", "")
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
        return Decimal(cleaned).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None


def sync_sheets_to_mysql(
    sheet_url: str | None = None,
    sheet_name: str | None = None,
) -> int:
    """
    Синхронизация работ из Google Sheets в MySQL.
    Читает только строки с датой >= последней импортированной. Дедупликация по sheet_row_hash.
    Возвращает количество добавленных строк.
    """
    session = get_session()
    try:
        last_date = works_repo.get_max_date(session)
        rows = read_works(sheet_url=sheet_url, sheet_name=sheet_name, last_date=last_date)
        if not rows:
            logger.info("Синхронизация: в таблице нет строк для импорта")
            return 0

        added = 0
        for row in rows:
            parsed_revenue = _parse_revenue(row.get("revenue"))
            if row.get("revenue") and parsed_revenue is None:
                logger.warning(
                    "Синхронизация: не удалось распарсить выручку '%s' (hash=%s)",
                    row.get("revenue"),
                    row.get("sheet_row_hash"),
                )

            if works_repo.exists_by_hash(session, row["sheet_row_hash"]):
                if parsed_revenue is not None:
                    works_repo.update_revenue_by_hash_if_uninvoiced(
                        session,
                        sheet_row_hash=row["sheet_row_hash"],
                        revenue=parsed_revenue,
                    )
                continue
            parsed_date = _parse_date(row["date"])
            if parsed_date is None:
                logger.warning(
                    "Синхронизация: пропуск строки с невалидной датой '%s' (hash=%s)",
                    row.get("date"),
                    row.get("sheet_row_hash"),
                )
                continue
            works_repo.create(
                session,
                date=parsed_date,
                counterparty_name=row["counterparty_name"],
                note=row["note"],
                structure=row["structure"],
                operation=row["operation"],
                object_count=row["object_count"],
                revenue=parsed_revenue,
                sheet_row_hash=row["sheet_row_hash"],
            )
            added += 1
        session.commit()
        logger.info("Синхронизация: добавлено %s новых работ из %s строк", added, len(rows))
        return added
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
