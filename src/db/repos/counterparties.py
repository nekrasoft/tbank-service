# Репозиторий контрагентов
from __future__ import annotations

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from src.db.models import Counterparty


def get_by_id(session: Session, counterparty_id: int) -> Counterparty | None:
    """Получение контрагента по ID."""
    result = session.execute(select(Counterparty).where(Counterparty.id == counterparty_id))
    return result.scalars().first()


def get_by_name_and_note(
    session: Session, name: str, note: str
) -> Counterparty | None:
    """Получение контрагента по имени и примечанию (для матчинга с works)."""
    note_val = note or ""
    result = session.execute(
        select(Counterparty)
        .where(Counterparty.name == name)
        .where(func.coalesce(Counterparty.note, "") == note_val)
    )
    return result.scalars().first()


def get_all(session: Session) -> list[Counterparty]:
    """Получение всех контрагентов."""
    result = session.execute(select(Counterparty).order_by(Counterparty.name))
    return list(result.scalars().all())


def create(
    session: Session,
    *,
    name: str,
    inn: str,
    kpp: str | None = None,
    email: str | None = None,
    phone: str = "",
    note: str = "",
) -> Counterparty:
    """Создание контрагента."""
    cp = Counterparty(
        name=name,
        inn=inn,
        kpp=kpp or "",
        email=email or "",
        phone=phone or "",
        note=note or "",
    )
    session.add(cp)
    session.flush()
    session.refresh(cp)
    return cp
