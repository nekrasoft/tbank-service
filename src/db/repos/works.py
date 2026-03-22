# Репозиторий работ (синк из Google Sheets)
from __future__ import annotations

from datetime import date

from sqlalchemy import select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from src.db.models import Work


def get_uninvoiced_by_counterparty(
    session: Session, counterparty_name: str
) -> list[Work]:
    """Получение работ без выставленного счёта для контрагента."""
    result = session.execute(
        select(Work)
        .where(Work.invoice_id.is_(None))
        .where(Work.counterparty_name == counterparty_name)
        .order_by(Work.date, Work.id)
    )
    return list(result.scalars().all())


def get_uninvoiced_by_counterparty_for_update(
    session: Session, counterparty_name: str
) -> list[Work]:
    """Получение и блокировка работ без счёта для контрагента."""
    result = session.execute(
        select(Work)
        .where(Work.invoice_id.is_(None))
        .where(Work.counterparty_name == counterparty_name)
        .order_by(Work.date, Work.id)
        .with_for_update()
    )
    return list(result.scalars().all())


def get_all_uninvoiced_counterparties(session: Session) -> list[str]:
    """Получение уникальных контрагентов с невыставленными работами."""
    result = session.execute(
        select(Work.counterparty_name)
        .where(Work.invoice_id.is_(None))
        .distinct()
    )
    return [row[0] for row in result.all()]


def get_max_date(session: Session) -> date | None:
    """Максимальная дата среди импортированных работ (для инкрементального чтения)."""
    result = session.execute(select(func.max(Work.date)))
    value = result.scalar()
    return value if value else None


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
    if not work_ids:
        return 0
    result = session.execute(
        update(Work)
        .where(Work.id.in_(work_ids))
        .where(Work.invoice_id.is_(None))
        .values(invoice_id=invoice_id)
    )
    return result.rowcount or 0
