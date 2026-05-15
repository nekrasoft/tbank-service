"""
Unit-тесты для парсинга назначения платежа в cron_payments.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

from src.cli.cron_payments import _extract_invoice_numbers
from src.cli import cron_payments
from src.notifications import invoice_reminder_email


def test_extract_invoice_numbers_multiple_with_dates() -> None:
    text = (
        "Оплата по счету № 199 от 30.04.2026, № 209 от 02.05.2026 "
        "за услуги спецтехники( ломовоз) вывоз и утилизация мусора "
        "объемом 30м3 Сумма 76000-00 Без налога (НДС)"
    )

    assert _extract_invoice_numbers(text) == {"199", "209"}


def test_payment_thanks_include_newly_paid_invoice_outside_business_day(monkeypatch) -> None:
    session = MagicMock()
    sent_invoice_numbers: list[str] = []
    marked_invoice_ids: list[int] = []
    extra_invoice_ids: list[int] = []

    def make_invoice(invoice_id: int, number: str, paid_at: datetime) -> SimpleNamespace:
        return SimpleNamespace(
            id=invoice_id,
            invoice_number=number,
            recipient_emails_snapshot=f"client{invoice_id}@example.com",
            counterparty=SimpleNamespace(
                name=f"Контрагент {number}",
                email="",
                email_accountant="",
            ),
            counterparty_id=invoice_id,
            issued_at=datetime(2026, 5, 11),
            paid_at=paid_at,
            due_date=None,
            items=[SimpleNamespace(price=Decimal("100.00"), amount=Decimal("1"))],
        )

    due_invoice = make_invoice(131, "225", datetime(2026, 5, 14, 21))
    newly_paid_invoice = make_invoice(127, "221", datetime(2026, 5, 13, 21))
    connection_module = ModuleType("src.db.connection")
    invoices_repo_module = ModuleType("src.db.repos.invoices")

    monkeypatch.setattr(cron_payments, "_business_today", lambda: date(2026, 5, 15))
    monkeypatch.setattr(
        cron_payments,
        "_utc_naive_bounds_for_business_date",
        lambda _day: (datetime(2026, 5, 14, 21), datetime(2026, 5, 15, 21)),
    )
    connection_module.get_session = lambda: session
    invoices_repo_module.get_paid_due_for_payment_thank_email = (
        lambda *_args, **_kwargs: [due_invoice]
    )

    def fake_get_pending_by_ids(*_args, invoice_ids: list[int], **_kwargs):
        extra_invoice_ids.extend(invoice_ids)
        return [newly_paid_invoice]

    invoices_repo_module.get_paid_pending_payment_thank_email_by_ids = fake_get_pending_by_ids
    invoices_repo_module.mark_payment_thank_email_sent = (
        lambda _session, *, invoice_id, sent_at: marked_invoice_ids.append(invoice_id) or 1
    )
    monkeypatch.setitem(sys.modules, "src.db.connection", connection_module)
    monkeypatch.setitem(sys.modules, "src.db.repos.invoices", invoices_repo_module)
    monkeypatch.setattr(
        invoice_reminder_email,
        "send_invoice_payment_thank_you",
        lambda *, invoice_number, **_kwargs: sent_invoice_numbers.append(invoice_number),
    )

    stats = cron_payments._send_due_payment_thank_you_emails(
        limit=5000,
        newly_paid_invoice_ids=[127],
    )

    assert extra_invoice_ids == [127]
    assert sent_invoice_numbers == ["225", "221"]
    assert marked_invoice_ids == [131, 127]
    assert stats == {"candidates": 2, "sent": 2, "failed": 0, "skipped": 0}


def test_extract_invoice_numbers_multiple_with_dates_and_leading_zeros() -> None:
    text = "Оплата по счету № 00199 от 30.04.2026, № 00209 от 02.05.2026"

    assert _extract_invoice_numbers(text) == {"199", "209"}
