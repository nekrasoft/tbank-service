"""
Синхронизация работ из Google Sheets в MySQL.
"""
from __future__ import annotations

import logging
from datetime import datetime

from src.db.connection import get_session
from src.db.repos import works as works_repo
from src.db.repos import works as works_repo
from src.sheets.reader import read_works

logger = logging.getLogger(__name__)


def _parse_date(date_str: str) -> datetime:
    """Парсинг даты DD.MM.YYYY в date."""
    try:
        return datetime.strptime(date_str.strip(), "%d.%m.%Y").date()
    except ValueError:
        return datetime.now().date()


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
    last_date = works_repo.get_max_date(session)
    rows = read_works(sheet_url=sheet_url, sheet_name=sheet_name, last_date=last_date)
    if not rows:
        logger.info("Синхронизация: в таблице нет строк для импорта")
        return 0

    added = 0
    try:
        for row in rows:
            if works_repo.exists_by_hash(session, row["sheet_row_hash"]):
                continue
            works_repo.create(
                session,
                date=_parse_date(row["date"]),
                counterparty_name=row["counterparty_name"],
                note=row["note"],
                structure=row["structure"],
                operation=row["operation"],
                object_count=row["object_count"],
                sheet_row_hash=row["sheet_row_hash"],
            )
            added += 1
        session.commit()
        logger.info("Синхронизация: добавлено %s новых работ из %s строк", added, len(rows))
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    return added
