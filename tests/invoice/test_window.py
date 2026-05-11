"""
Unit tests for invoice date window logic.
"""
from __future__ import annotations

from datetime import date, datetime

import pytest

from src.invoice.window import (
    add_business_days,
    build_invoice_work_date_window,
    build_invoice_work_date_window_manual,
    env_bool,
)


class TestAddBusinessDays:
    """Tests for add_business_days helper."""

    def test_zero_days(self):
        """Ноль рабочих дней — возвращает ту же дату."""
        d = date(2026, 3, 16)  # понедельник
        result = add_business_days(d, 0)
        assert result == d

    def test_one_day_monday_to_tuesday(self):
        """Пн -> Вт (1 рабочий день)."""
        d = date(2026, 3, 16)  # понедельник
        result = add_business_days(d, 1)
        assert result == date(2026, 3, 17)

    def test_weekend_skip(self):
        """Пт -> Пн (через выходные)."""
        d = date(2026, 3, 20)  # пятница
        result = add_business_days(d, 1)
        assert result == date(2026, 3, 23)  # понедельник

    def test_negative_raises(self):
        """Отрицательное количество — ошибка."""
        with pytest.raises(ValueError):
            add_business_days(date(2026, 3, 16), -1)

    def test_multiple_days(self):
        """Несколько рабочих дней."""
        d = date(2026, 3, 16)  # понедельник
        result = add_business_days(d, 5)
        # пн, вт, ср, чт, пт = 5 дней
        assert result == date(2026, 3, 20)


class TestBuildInvoiceWorkDateWindow:
    """Tests for build_invoice_work_date_window (cron mode)."""

    def test_strict_period_false(self):
        """strict_period=False: без нижней границы."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="monthly",
            run_at=run_at,
            strict_period=False,
        )
        assert date_from is None
        assert date_to == date(2026, 3, 15)

    def test_monthly_strict(self):
        """monthly: с 1-го числа месяца."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="monthly",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 1)
        assert date_to == date(2026, 3, 15)

    def test_2weeks_first_half(self):
        """2weeks: 1-15 число."""
        run_at = datetime(2026, 3, 10, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="2weeks",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 1)
        assert date_to == date(2026, 3, 10)

    def test_2weeks_second_half(self):
        """2weeks: 16-конец месяца."""
        run_at = datetime(2026, 3, 20, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="2weeks",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 16)
        assert date_to == date(2026, 3, 20)

    def test_10days_first_decade(self):
        """10days: 1-10 число."""
        run_at = datetime(2026, 3, 5, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="10days",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 1)
        assert date_to == date(2026, 3, 5)

    def test_10days_second_decade(self):
        """10days: 11-20 число."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="10days",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 11)
        assert date_to == date(2026, 3, 15)

    def test_10days_third_decade(self):
        """10days: 21-конец месяца."""
        run_at = datetime(2026, 3, 25, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="10days",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 21)
        assert date_to == date(2026, 3, 25)

    def test_daily_strict(self):
        """daily: только текущий день."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="daily",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 15)
        assert date_to == date(2026, 3, 15)

    def test_unknown_schedule_defaults_to_monthly(self):
        """Неизвестный schedule -> monthly."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule="unknown",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 1)
        assert date_to == date(2026, 3, 15)

    def test_none_schedule_defaults_to_monthly(self):
        """None schedule -> monthly."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule=None,
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 1)


class TestBuildInvoiceWorkDateWindowManual:
    """Tests for build_invoice_work_date_window_manual (manual mode)."""

    def test_strict_period_false(self):
        """strict_period=False: без границ."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="monthly",
            run_at=run_at,
            strict_period=False,
        )
        assert date_from is None
        assert date_to is None

    def test_monthly_manual(self):
        """monthly: весь месяц."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="monthly",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 1)
        assert date_to == date(2026, 3, 31)  # последний день марта

    def test_february_leap_year(self):
        """Февраль в високосном году."""
        run_at = datetime(2026, 2, 10, 10, 0)  # 2026 НЕ високосный
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="monthly",
            run_at=run_at,
            strict_period=True,
        )
        assert date_to == date(2026, 2, 28)

    def test_2weeks_first_half_manual(self):
        """2weeks: 1-15 число (правая граница 15)."""
        run_at = datetime(2026, 3, 10, 10, 0)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="2weeks",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 1)
        assert date_to == date(2026, 3, 15)

    def test_2weeks_second_half_manual(self):
        """2weeks: 16-конец месяца."""
        run_at = datetime(2026, 3, 20, 10, 0)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="2weeks",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 16)
        assert date_to == date(2026, 3, 31)

    def test_10days_first_decade_manual(self):
        """10days: 1-10 (правая граница 10)."""
        run_at = datetime(2026, 3, 5, 10, 0)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="10days",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 1)
        assert date_to == date(2026, 3, 10)

    def test_10days_second_decade_manual(self):
        """10days: 11-20."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="10days",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 11)
        assert date_to == date(2026, 3, 20)

    def test_10days_third_decade_manual(self):
        """10days: 21-конец месяца."""
        run_at = datetime(2026, 3, 25, 10, 0)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="10days",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 21)
        assert date_to == date(2026, 3, 31)

    def test_daily_manual(self):
        """daily: только текущий день."""
        run_at = datetime(2026, 3, 15, 10, 0)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule="daily",
            run_at=run_at,
            strict_period=True,
        )
        assert date_from == date(2026, 3, 15)
        assert date_to == date(2026, 3, 15)


class TestEnvBool:
    """Tests for env_bool helper."""

    def test_default_value(self):
        """Если переменной нет — возвращает default."""
        import os
        # Убираем переменную если есть
        old_val = os.environ.pop("TEST_BOOL_VAR", None)
        try:
            result = env_bool("TEST_BOOL_VAR", True)
            assert result is True
            result = env_bool("TEST_BOOL_VAR", False)
            assert result is False
        finally:
            if old_val is not None:
                os.environ["TEST_BOOL_VAR"] = old_val

    def test_truthy_values(self):
        """Правдивые значения."""
        import os
        for val in ("1", "true", "yes", "y", "on", "TRUE", "YES"):
            os.environ["TEST_BOOL"] = val
            assert env_bool("TEST_BOOL", False) is True

    def test_falsy_values(self):
        """Неправдивые значения."""
        import os
        for val in ("0", "false", "no", "n", "off", "FALSE", "NO"):
            os.environ["TEST_BOOL"] = val
            assert env_bool("TEST_BOOL", True) is False
