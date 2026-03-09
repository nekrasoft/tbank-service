# Репозиторий работ (синк из Google Sheets)
from __future__ import annotations

from datetime import date

from sqlalchemy import select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from src.db.models import Work


def get_uninvoiced_by_counterparty(
    session: Session, counterparty_name: str, note: str
) -> list[Work]:
    """Получение работ без выставленного счёта для пары (контрагент, примечание)."""
    note_val = note or ""
    result = session.execute(
        select(Work)
        .where(Work.invoice_id.is_(None))
        .where(Work.counterparty_name == counterparty_name)
        .where(func.coalesce(Work.note, "") == note_val)
        .order_by(Work.date, Work.id)
    )
    return list(result.scalars().all())


def get_all_uninvoiced_groups(session: Session) -> list[tuple[str, str]]:
    """Получение уникальных пар (counterparty_name, note) с невыставленными работами."""
    result = session.execute(
        select(Work.counterparty_name, Work.note)
        .where(Work.invoice_id.is_(None))
        .distinct()
    )
    return [(row[0], row[1] or "") for row in result.all()]


def exists_by_hash(session: Session, sheet_row_hash: str) -> bool:
    """Проверка существования работы по хешу (дедупликация)."""
    result = session.execute(
        select(Work.id).where(Work.sheet_row_hash == sheet_row_hash).limit(1)
    )
    return result.scalars().first() is not None


def create(
    session: Session,
    *,
    date: date,
    counterparty_name: str,
    note: str,
    structure: str,
    operation: str,
    object_count: str,
    sheet_row_hash: str,
) -> Work:
    """Создание записи о работе."""
    work = Work(
        date=date,
        counterparty_name=counterparty_name,
        note=note or "",
        structure=structure,
        operation=operation,
        object_count=object_count or "1",
        sheet_row_hash=sheet_row_hash,
    )
    session.add(work)
    session.flush()
    session.refresh(work)
    return work


def update_invoice_id(
    session: Session, work_ids: list[int], invoice_id: int
) -> int:
    """Привязка работ к выставленному счёту."""
    result = session.execute(
        update(Work).where(Work.id.in_(work_ids)).values(invoice_id=invoice_id)
    )
    return result.rowcount or 0
