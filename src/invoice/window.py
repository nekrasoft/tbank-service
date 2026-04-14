"""
Правила отбора работ по дате для выставления счёта.
"""
from __future__ import annotations

import calendar
import os
from datetime import date, datetime


def env_bool(name: str, default: bool) -> bool:
    """Чтение bool-переменной окружения."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def add_business_days(start_date: date, business_days: int) -> date:
    """Добавляет к дате N рабочих дней (пн-пт), исключая стартовый день."""
    if business_days < 0:
        raise ValueError("business_days не может быть отрицательным")
    if business_days == 0:
        return start_date

    current = start_date
    added = 0
    while added < business_days:
        current = current.fromordinal(current.toordinal() + 1)
        if current.weekday() < 5:  # 0..4 = пн..пт
            added += 1
    return current


def build_invoice_work_date_window(
    *,
    invoice_schedule: str | None,
    run_at: datetime,
    strict_period: bool,
) -> tuple[date | None, date]:
    """
    Возвращает (date_from, date_to) для отбора работ в счёт.

    Базовый режим:
    - date_from = None (без нижней границы)
    - date_to = дата запуска (не включать будущие работы)

    strict_period=True:
    - monthly: с 1-го числа текущего месяца
    - 2weeks: 1-15 или 16-конец месяца
    - 10days: 1-10, 11-20 или 21-конец месяца
    - daily: только текущий день
    """
    date_to = run_at.date()
    if not strict_period:
        return None, date_to

    schedule = (invoice_schedule or "monthly").strip().lower()
    if schedule == "daily":
        return date_to, date_to
    if schedule == "2weeks":
        if run_at.day <= 15:
            return date(date_to.year, date_to.month, 1), date_to
        return date(date_to.year, date_to.month, 16), date_to
    if schedule == "10days":
        if run_at.day <= 10:
            return date(date_to.year, date_to.month, 1), date_to
        if run_at.day <= 20:
            return date(date_to.year, date_to.month, 11), date_to
        return date(date_to.year, date_to.month, 21), date_to

    # monthly и неизвестные значения — с начала месяца.
    return date(date_to.year, date_to.month, 1), date_to


def build_invoice_work_date_window_manual(
    *,
    invoice_schedule: str | None,
    run_at: datetime,
    strict_period: bool,
) -> tuple[date | None, date | None]:
    """
    Окно дат для ручного режима.

    Отличие от cron-режима: правая граница не зависит от текущего времени запуска,
    а фиксируется концом выбранного периода.

    strict_period=False:
    - без границ (берутся все невыставленные работы)

    strict_period=True:
    - monthly: с 1-го по последний день текущего месяца
    - 2weeks: с 1-го по 15-е или с 16-го по конец месяца
    - 10days: с 1-го по 10-е, с 11-го по 20-е или с 21-го по конец месяца
    - daily: только текущий день
    """
    if not strict_period:
        return None, None

    date_to = run_at.date()
    schedule = (invoice_schedule or "monthly").strip().lower()
    _, last_day = calendar.monthrange(date_to.year, date_to.month)

    if schedule == "daily":
        return date_to, date_to
    if schedule == "2weeks":
        if run_at.day <= 15:
            return date(date_to.year, date_to.month, 1), date(date_to.year, date_to.month, 15)
        return date(date_to.year, date_to.month, 16), date(date_to.year, date_to.month, last_day)
    if schedule == "10days":
        if run_at.day <= 10:
            return date(date_to.year, date_to.month, 1), date(date_to.year, date_to.month, 10)
        if run_at.day <= 20:
            return date(date_to.year, date_to.month, 11), date(date_to.year, date_to.month, 20)
        return date(date_to.year, date_to.month, 21), date(date_to.year, date_to.month, last_day)

    # monthly и неизвестные значения — весь текущий месяц.
    return date(date_to.year, date_to.month, 1), date(date_to.year, date_to.month, last_day)
