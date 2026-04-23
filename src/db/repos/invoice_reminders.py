"""Репозиторий журнала напоминаний об оплате счетов."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import InvoicePaymentReminder


def get_sent_offsets_by_invoice(
    session: Session,
    *,
    invoice_ids: list[int],
    channel: str = "email",
) -> dict[int, set[int]]:
    """Возвращает уже отправленные offsets по каждому счету."""
    if not invoice_ids:
        return {}

    result = session.execute(
        select(
            InvoicePaymentReminder.invoice_id,
            InvoicePaymentReminder.schedule_offset_days,
        )
        .where(InvoicePaymentReminder.invoice_id.in_(invoice_ids))
        .where(InvoicePaymentReminder.channel == channel)
        .where(InvoicePaymentReminder.status == "sent")
    )

    sent_map: dict[int, set[int]] = {}
    for invoice_id, offset in result.all():
        inv_id = int(invoice_id)
        day_offset = int(offset)
        sent_map.setdefault(inv_id, set()).add(day_offset)
    return sent_map


def add_attempt(
    session: Session,
    *,
    invoice_id: int,
    channel: str,
    schedule_offset_days: int,
    overdue_days_at_send: int,
    recipient_snapshot: str | None,
    status: str,
    error_text: str | None = None,
    sent_at: datetime | None = None,
) -> InvoicePaymentReminder:
    """Фиксирует одну попытку отправки напоминания."""
    row = InvoicePaymentReminder(
        invoice_id=int(invoice_id),
        channel=(channel or "email").strip() or "email",
        schedule_offset_days=int(schedule_offset_days),
        overdue_days_at_send=int(overdue_days_at_send),
        recipient_snapshot=(recipient_snapshot or "").strip() or None,
        status=(status or "").strip() or "failed",
        error_text=(error_text or "").strip() or None,
        sent_at=sent_at,
        created_at=datetime.utcnow(),
    )
    session.add(row)
    session.flush()
    session.refresh(row)
    return row
