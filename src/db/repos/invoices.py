# Репозиторий выставленных счетов
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.orm import Session, joinedload, selectinload

from src.db.models import Counterparty, Invoice, InvoiceItem


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
        paid_amount=Decimal("0.00"),
        paid_at=None,
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
    unit: str = "шт",
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
    payment_link: str | None = None,
    recipient_emails_snapshot: str | None = None,
    bitrix_task_id: int | None = None,
    bitrix_deal_id: int | None = None,
) -> int:
    """Пометка счёта как успешно отправленного в T-Bank."""
    values: dict[str, object] = {
        "status": "issued",
        "tbank_invoice_id": tbank_invoice_id,
    }
    if pdf_url is not None:
        values["pdf_url"] = pdf_url
    if payment_link is not None:
        values["payment_link"] = payment_link
    if recipient_emails_snapshot is not None:
        values["recipient_emails_snapshot"] = recipient_emails_snapshot
    if bitrix_task_id is not None:
        values["bitrix_task_id"] = bitrix_task_id
    if bitrix_deal_id is not None:
        values["bitrix_deal_id"] = bitrix_deal_id
    result = session.execute(
        update(Invoice)
        .where(Invoice.id == invoice_id)
        .values(**values)
    )
    return result.rowcount or 0


def update_bitrix_links(
    session: Session,
    *,
    invoice_id: int,
    bitrix_task_id: int | None = None,
    bitrix_deal_id: int | None = None,
) -> int:
    """Сохраняет в счёте связки c задачей/сделкой Bitrix24."""
    values: dict[str, int] = {}
    if bitrix_task_id is not None:
        values["bitrix_task_id"] = int(bitrix_task_id)
    if bitrix_deal_id is not None:
        values["bitrix_deal_id"] = int(bitrix_deal_id)
    if not values:
        return 0

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


def get_open_for_payment_matching(session: Session) -> list[Invoice]:
    """
    Счета-кандидаты для матчинга входящих платежей.

    Берём только счета, реально отправленные в T-Bank и еще не закрытые полностью.
    """
    result = session.execute(
        select(Invoice)
        .where(Invoice.status.in_(("issued", "partially_paid")))
        .options(joinedload(Invoice.counterparty), selectinload(Invoice.items))
        .order_by(Invoice.issued_at.asc(), Invoice.id.asc())
    )
    return list(result.scalars().all())


def get_for_payment_recalc(session: Session, invoice_ids: list[int]) -> list[Invoice]:
    """Получение счетов для пересчета статуса оплаты."""
    if not invoice_ids:
        return []
    result = session.execute(
        select(Invoice)
        .where(Invoice.id.in_(invoice_ids))
        .options(joinedload(Invoice.counterparty), selectinload(Invoice.items))
    )
    return list(result.scalars().all())


def get_unpaid_due_for_reminders(
    session: Session,
    *,
    due_on_or_before: date,
    paid_tolerance: Decimal = Decimal("0.01"),
    limit: int | None = None,
) -> list[Invoice]:
    """
    Счета-кандидаты для напоминаний клиентам.

    Берем только полностью неоплаченные счета со сроком оплаты <= due_on_or_before.
    """
    stmt = (
        select(Invoice)
        .join(Invoice.counterparty)
        .where(Invoice.due_date.is_not(None))
        .where(Invoice.due_date <= due_on_or_before)
        .where(Invoice.status == "issued")
        .where(Invoice.paid_amount <= paid_tolerance)
        .where(Counterparty.payment_reminders_enabled.is_(True))
        .options(joinedload(Invoice.counterparty), selectinload(Invoice.items))
        .order_by(Invoice.due_date.asc(), Invoice.id.asc())
    )
    if limit is not None and limit > 0:
        stmt = stmt.limit(limit)
    result = session.execute(stmt)
    return list(result.scalars().all())


def get_paid_due_for_payment_thank_email(
    session: Session,
    *,
    paid_from: datetime,
    paid_to: datetime,
    limit: int | None = None,
) -> list[Invoice]:
    """Оплаченные счета, по которым еще не отправили email-благодарность."""
    stmt = (
        select(Invoice)
        .where(Invoice.status == "paid")
        .where(Invoice.paid_at.is_not(None))
        .where(Invoice.paid_at >= paid_from)
        .where(Invoice.paid_at < paid_to)
        .where(Invoice.payment_thank_email_sent_at.is_(None))
        .options(joinedload(Invoice.counterparty), selectinload(Invoice.items))
        .order_by(Invoice.paid_at.asc(), Invoice.id.asc())
    )
    if limit is not None and limit > 0:
        stmt = stmt.limit(limit)
    result = session.execute(stmt)
    return list(result.scalars().all())


def update_payment_state(
    session: Session,
    *,
    invoice_id: int,
    status: str,
    paid_amount: Decimal,
    paid_at: datetime | None,
) -> int:
    """Обновление статуса и агрегированных полей оплаты счета."""
    result = session.execute(
        update(Invoice)
        .where(Invoice.id == invoice_id)
        .values(
            status=status,
            paid_amount=paid_amount,
            paid_at=paid_at,
        )
    )
    return result.rowcount or 0


def mark_payment_thank_email_sent(
    session: Session,
    *,
    invoice_id: int,
    sent_at: datetime,
) -> int:
    """Отмечает, что email-благодарность за оплату успешно отправлена."""
    result = session.execute(
        update(Invoice)
        .where(Invoice.id == invoice_id)
        .where(Invoice.payment_thank_email_sent_at.is_(None))
        .values(payment_thank_email_sent_at=sent_at)
    )
    return result.rowcount or 0
