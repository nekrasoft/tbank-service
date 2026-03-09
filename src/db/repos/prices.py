# Репозиторий прайсов
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Price


def get_by_counterparty_and_operation(
    session: Session, counterparty_id: int, operation_type: str
) -> Price | None:
    """Получение цены по контрагенту и типу операции."""
    result = session.execute(
        select(Price)
        .where(Price.counterparty_id == counterparty_id)
        .where(Price.operation_type == operation_type)
    )
    return result.scalars().first()


def get_all_by_counterparty(
    session: Session, counterparty_id: int
) -> list[Price]:
    """Получение всех цен контрагента."""
    result = session.execute(
        select(Price)
        .where(Price.counterparty_id == counterparty_id)
        .order_by(Price.operation_type)
    )
    return list(result.scalars().all())


def create(
    session: Session,
    *,
    counterparty_id: int,
    operation_type: str,
    price: float,
    vat: str = "None",
) -> Price:
    """Создание записи о цене."""
    p = Price(
        counterparty_id=counterparty_id,
        operation_type=operation_type,
        price=price,
        vat=vat or "None",
    )
    session.add(p)
    session.flush()
    session.refresh(p)
    return p
