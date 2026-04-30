# Репозиторий контрагентов
from __future__ import annotations

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from src.db.models import Counterparty


def get_by_id(session: Session, counterparty_id: int) -> Counterparty | None:
    """Получение контрагента по ID."""
    result = session.execute(select(Counterparty).where(Counterparty.id == counterparty_id))
    return result.scalars().first()


def get_by_name_and_note(
    session: Session, name: str, note: str
) -> Counterparty | None:
    """Получение контрагента по полному имени и примечанию."""
    note_val = note or ""
    result = session.execute(
        select(Counterparty)
        .where(Counterparty.name == name)
        .where(func.coalesce(Counterparty.note, "") == note_val)
    )
    return result.scalars().first()


def get_by_inn(session: Session, inn: str) -> Counterparty | None:
    """Получение контрагента по ИНН."""
    result = session.execute(
        select(Counterparty).where(Counterparty.inn == inn)
    )
    return result.scalars().first()


def get_by_short_name(
    session: Session, short_name: str, note: str
) -> Counterparty | None:
    """Получение контрагента по короткому имени."""
    result = session.execute(
        select(Counterparty)
        .where(Counterparty.short_name == short_name)
    )
    return result.scalars().first()


def get_all(session: Session) -> list[Counterparty]:
    """Получение всех контрагентов."""
    result = session.execute(select(Counterparty).order_by(Counterparty.short_name))
    return list(result.scalars().all())


def create(
    session: Session,
    *,
    name: str,
    short_name: str,
    inn: str,
    kpp: str | None = None,
    email: str | None = None,
    email_accountant: str | None = None,
    payment_reminders_enabled: bool = True,
    phone: str = "",
    note: str = "",
    contract: str | None = None,
    invoice_schedule: str = "2weeks",
    status: str = "active",
    operation_type: str | None = None,
) -> Counterparty:
    """Создание контрагента."""
    cp = Counterparty(
        name=name,
        short_name=short_name,
        inn=inn,
        kpp=kpp or "",
        email=email or "",
        email_accountant=email_accountant or "",
        payment_reminders_enabled=bool(payment_reminders_enabled),
        phone=phone or "",
        note=note or "",
        contract=(contract or "").strip() or None,
        invoice_schedule=(invoice_schedule or "2weeks").strip(),
        status=(status or "active").strip(),
        operation_type=(operation_type or "").strip() or None,
    )
    session.add(cp)
    session.flush()
    session.refresh(cp)
    return cp


def update_bitrix_company_id(
    session: Session,
    *,
    counterparty_id: int,
    bitrix_company_id: int | None,
) -> int:
    """Обновление привязки контрагента к компании в Bitrix24."""
    value = None if bitrix_company_id is None else int(bitrix_company_id)
    result = session.execute(
        update(Counterparty)
        .where(Counterparty.id == int(counterparty_id))
        .values(bitrix_company_id=value)
    )
    return int(result.rowcount or 0)
