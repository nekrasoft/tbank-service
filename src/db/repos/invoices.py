# Репозиторий выставленных счетов
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from src.db.models import Invoice, InvoiceItem


def create(
    session: Session,
    *,
    invoice_number: str,
    tbank_invoice_id: str | None,
    counterparty_id: int,
    due_date: date | None = None,
    status: str = "issued",
    pdf_url: str | None = None,
) -> Invoice:
    """Создание записи о выставленном счёте."""
    inv = Invoice(
        invoice_number=invoice_number,
        tbank_invoice_id=tbank_invoice_id,
        counterparty_id=counterparty_id,
        issued_at=datetime.utcnow(),
        due_date=due_date,
        status=status,
        pdf_url=pdf_url,
    )
    session.add(inv)
    session.flush()
    session.refresh(inv)
    return inv


def add_item(
    session: Session,
    *,
    invoice_id: int,
    name: str,
    price: float,
    amount: float,
    unit: str = "ед.",
    vat: str = "None",
) -> InvoiceItem:
    """Добавление позиции к счёту."""
    item = InvoiceItem(
        invoice_id=invoice_id,
        name=name,
        price=price,
        amount=amount,
        unit=unit,
        vat=vat or "None",
    )
    session.add(item)
    session.flush()
    session.refresh(item)
    return item


def get_by_id(session: Session, invoice_id: int) -> Invoice | None:
    """Получение счёта по ID."""
    result = session.execute(select(Invoice).where(Invoice.id == invoice_id))
    return result.scalars().first()


def get_items(session: Session, invoice_id: int) -> list[InvoiceItem]:
    """Получение позиций счёта."""
    result = session.execute(
        select(InvoiceItem).where(InvoiceItem.invoice_id == invoice_id)
    )
    return list(result.scalars().all())


def mark_as_issued(
    session: Session,
    *,
    invoice_id: int,
    tbank_invoice_id: str | None,
    pdf_url: str | None = None,
) -> int:
    """Пометка счёта как успешно отправленного в T-Bank."""
    values = {
        "status": "issued",
        "tbank_invoice_id": tbank_invoice_id,
    }
    if pdf_url is not None:
        values["pdf_url"] = pdf_url
    result = session.execute(
        update(Invoice)
        .where(Invoice.id == invoice_id)
        .values(**values)
    )
    return result.rowcount or 0


def mark_as_failed(session: Session, *, invoice_id: int) -> int:
    """Пометка счёта как неотправленного в T-Bank."""
    result = session.execute(
        update(Invoice)
        .where(Invoice.id == invoice_id)
        .values(status="failed_send")
    )
    return result.rowcount or 0
