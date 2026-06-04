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


def test_manual_payment_thanks_dry_run_does_not_send_or_mark(monkeypatch) -> None:
    session = MagicMock()
    send_mock = MagicMock()
    mark_mock = MagicMock()

    invoice = SimpleNamespace(
        id=131,
        invoice_number="225",
        status="paid",
        payment_thank_email_sent_at=None,
        recipient_emails_snapshot="client@example.com",
        counterparty=SimpleNamespace(
            name="Контрагент 225",
            email="",
            email_accountant="accountant@example.com",
        ),
        counterparty_id=131,
        issued_at=datetime(2026, 5, 11),
        paid_at=datetime(2026, 5, 21, 10),
        due_date=None,
        items=[SimpleNamespace(price=Decimal("100.00"), amount=Decimal("1"))],
    )
    requested_numbers: list[str] = []
    connection_module = ModuleType("src.db.connection")
    invoices_repo_module = ModuleType("src.db.repos.invoices")

    connection_module.get_session = lambda: session

    def fake_get_by_numbers(*_args, invoice_numbers: list[str], **_kwargs):
        requested_numbers.extend(invoice_numbers)
        return [invoice]

    invoices_repo_module.get_by_invoice_numbers_for_payment_thank_email = fake_get_by_numbers
    invoices_repo_module.mark_payment_thank_email_sent = mark_mock
    monkeypatch.setitem(sys.modules, "src.db.connection", connection_module)
    monkeypatch.setitem(sys.modules, "src.db.repos.invoices", invoices_repo_module)
    monkeypatch.setattr(invoice_reminder_email, "send_invoice_payment_thank_you", send_mock)

    stats = cron_payments._send_manual_payment_thank_you_emails(
        invoice_numbers=["00225"],
        dry_run=True,
    )

    assert requested_numbers == ["225"]
    send_mock.assert_not_called()
    mark_mock.assert_not_called()
    session.rollback.assert_called_once()
    assert stats == {
        "requested": 1,
        "found": 1,
        "candidates": 1,
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "missing": 0,
    }


def test_invoice_matching_uses_operation_date_before_doc_date() -> None:
    counterparty = SimpleNamespace(
        inn="9723164328",
        name='ООО "ПЛАЙВУД МАРКЕТ"',
        short_name="Плайвуд Маркет",
    )
    invoice = SimpleNamespace(
        id=134,
        invoice_number="228",
        issued_at=datetime(2026, 5, 15, 13, 1, 2),
        counterparty=counterparty,
    )
    operation = SimpleNamespace(
        operation_amount=Decimal("10600.00"),
        account_amount=None,
        ruble_amount=None,
        operation_date=datetime(2026, 5, 15, 13, 34, 19),
        trxn_post_date=datetime(2026, 5, 15, 13, 34, 29),
        authorization_date=None,
        doc_date=datetime(2026, 5, 14, 21, 0, 0),
        charge_date=None,
        draw_date=None,
        payer_inn="9723164328",
        counterparty_inn="9723164328",
        payer_name='ООО "ПЛАЙВУД МАРКЕТ"',
        counterparty_name='ООО "ПЛАЙВУД МАРКЕТ"',
        pay_purpose=(
            "Оплата по счету № 228 от 15.05.2026 за Услуги спецтехники "
            "(ломовоз) - вывоз и утилизация мусора. Сумма 10600-00 Без налога (НДС)"
        ),
        description=None,
    )

    decision = cron_payments._match_operation_to_invoice(
        operation,
        invoice_state={
            134: {
                "invoice": invoice,
                "total": Decimal("10600.00"),
                "paid": Decimal("0.00"),
            },
        },
        invoices_by_number={"228": [134]},
        invoices_by_inn={"9723164328": [134]},
        open_invoice_ids=[134],
    )

    assert decision is not None
    assert decision["invoice_id"] == 134
    assert decision["method"] == "invoice_number"


def test_extract_invoice_numbers_multiple_with_dates_and_leading_zeros() -> None:
    text = "Оплата по счету № 00199 от 30.04.2026, № 00209 от 02.05.2026"

    assert _extract_invoice_numbers(text) == {"199", "209"}


def test_profit_fuel_expense_allocates_amount_by_fixed_days() -> None:
    operation = SimpleNamespace(
        id=10,
        operation_amount=Decimal("30000.00"),
        account_amount=None,
        ruble_amount=None,
        cashless_expense_business_date=date(2026, 6, 1),
        cashless_expense_operation_code="183",
        cashless_expense_operation_name="ГСМ",
    )

    rows = cron_payments._daily_expense_allocation_rows_for_operation(
        operation,
        fuel_allocation_days=2,
    )

    assert rows == [
        {
            "statement_operation_id": 10,
            "expense_date": date(2026, 6, 1),
            "expense_code": "183",
            "expense_name": "ГСМ",
            "amount": Decimal("15000.00"),
            "allocation_days": 2,
            "allocation_method": "fixed_days",
        },
        {
            "statement_operation_id": 10,
            "expense_date": date(2026, 6, 2),
            "expense_code": "183",
            "expense_name": "ГСМ",
            "amount": Decimal("15000.00"),
            "allocation_days": 2,
            "allocation_method": "fixed_days",
        },
    ]


def test_profit_landfill_expense_allocates_to_operation_day() -> None:
    operation = SimpleNamespace(
        id=11,
        operation_amount=Decimal("10600.00"),
        account_amount=None,
        ruble_amount=None,
        cashless_expense_business_date=date(2026, 6, 1),
        cashless_expense_operation_code="185",
        cashless_expense_operation_name="Утилизация (полигон)",
    )

    rows = cron_payments._daily_expense_allocation_rows_for_operation(
        operation,
        fuel_allocation_days=2,
    )

    assert rows == [
        {
            "statement_operation_id": 11,
            "expense_date": date(2026, 6, 1),
            "expense_code": "185",
            "expense_name": "Утилизация (полигон)",
            "amount": Decimal("10600.00"),
            "allocation_days": 1,
            "allocation_method": "operation_day",
        },
    ]


def test_cashless_expense_analytics_details_returns_codes_from_fallback_rule() -> None:
    analytics = cron_payments._parse_pay_purpose_analytics_details(
        "Оплата топлива AZS",
        "АЗС",
        structure_by_code={"1202": "ЮЛ - Контейнеры"},
        operation_by_code={"183": "ГСМ"},
        fallback_rules={
            "default_structure_code": "1202",
            "rules": [
                {
                    "contains_any": ["azs"],
                    "operation_code": "183",
                }
            ],
        },
    )

    assert analytics == {
        "structure_code": "1202",
        "structure_name": "ЮЛ - Контейнеры",
        "operation_code": "183",
        "operation_name": "ГСМ",
        "classification_source": "fallback",
    }
