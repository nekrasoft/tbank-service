"""
Unit-тесты для парсинга назначения платежа в cron_payments.
"""

from __future__ import annotations

from src.cli.cron_payments import _extract_invoice_numbers


def test_extract_invoice_numbers_multiple_with_dates() -> None:
    text = (
        "Оплата по счету № 199 от 30.04.2026, № 209 от 02.05.2026 "
        "за услуги спецтехники( ломовоз) вывоз и утилизация мусора "
        "объемом 30м3 Сумма 76000-00 Без налога (НДС)"
    )

    assert _extract_invoice_numbers(text) == {"199", "209"}


def test_extract_invoice_numbers_multiple_with_dates_and_leading_zeros() -> None:
    text = "Оплата по счету № 00199 от 30.04.2026, № 00209 от 02.05.2026"

    assert _extract_invoice_numbers(text) == {"199", "209"}
