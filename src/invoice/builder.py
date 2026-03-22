"""
Сборка позиций счёта из работ и прайсов.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import date as date_type
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

from sqlalchemy.orm import Session

from src.db.models import Work
from src.db.repos import prices as prices_repo

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OPERATIONS_PATH = PROJECT_ROOT / "config" / "operations.json"

# Маппинг (структура, операция) -> operation_type
_OPERATION_TYPE_MAP: dict[tuple[str, str], str] = {}
_MONEY_Q = Decimal("0.01")


def _load_operation_type_map() -> dict[tuple[str, str], str]:
    """Загрузка маппинга структура+операция -> operation_type."""
    global _OPERATION_TYPE_MAP
    if _OPERATION_TYPE_MAP:
        return _OPERATION_TYPE_MAP
    with open(OPERATIONS_PATH, "r", encoding="utf-8") as f:
        ops = json.load(f)
    for op_type, data in ops.items():
        struct = (data.get("структура") or "").strip()
        oper = (data.get("операция") or "").strip()
        if struct and oper:
            _OPERATION_TYPE_MAP[(struct, oper)] = op_type
    return _OPERATION_TYPE_MAP


def _get_operation_type(work: Work) -> str | None:
    """Определение типа операции по структуре и операции."""
    m = _load_operation_type_map()
    key = ((work.structure or "").strip(), (work.operation or "").strip())
    return m.get(key)


def _parse_amount(object_count: str | None) -> float:
    """Парсинг количества (контейнеры, ходки)."""
    if not object_count or not str(object_count).strip():
        return 1.0
    try:
        val = float(str(object_count).strip().replace(",", "."))
        return max(0.01, val)
    except ValueError:
        return 1.0


def _format_amount(amount: float) -> str:
    """Форматирование количества без лишних нулей."""
    rounded_int = int(round(amount))
    if abs(amount - rounded_int) < 1e-9:
        return str(rounded_int)
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def _parse_revenue(value: object) -> Decimal | None:
    """Парсинг выручки из Work.revenue. Пустые/невалидные значения -> None."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        dec = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        cleaned = raw.replace("\u00a0", "").replace(" ", "").replace(",", ".")
        try:
            dec = Decimal(cleaned)
        except (InvalidOperation, ValueError):
            return None
    if dec <= 0:
        return None
    return dec.quantize(_MONEY_Q, rounding=ROUND_HALF_UP)


def build_invoice_comment(works: list[Work]) -> str:
    """
    Сборка комментария к счёту для T-Bank.

    Формат:
    Оказаны услуги:
    05.03.2026 Свободы 111А - 3 ед., 10.03.2026 Знак - 4 ед.
    """
    grouped: dict[tuple[date_type, str], float] = defaultdict(float)
    for work in works:
        note = (work.note or "").strip()
        grouped[(work.date, note)] += _parse_amount(work.object_count)

    if not grouped:
        return "Оказаны услуги."

    parts: list[str] = []
    for (work_date, note), total in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
        date_str = work_date.strftime("%d.%m.%Y")
        amount_str = _format_amount(total)
        if note:
            parts.append(f"{date_str} {note} - {amount_str} ед.")
        else:
            parts.append(f"{date_str} - {amount_str} ед.")

    return "Оказаны услуги:\n" + ", ".join(parts)


def build_invoice_items(
    session: Session,
    works: list[Work],
    counterparty_id: int,
) -> list[dict]:
    """
    Формирование позиций счёта для T-Bank из списка работ.

    Группирует работы по operation_type, суммирует количество,
    получает цену из прайсов. Пропускает advance и landfill_unload.

    :return: Список [{name, price, unit, vat, amount}]
    """
    with open(OPERATIONS_PATH, "r", encoding="utf-8") as f:
        ops_config = json.load(f)

    # Группировка по operation_type:
    # - fallback_amount: количество, которое нужно считать по прайсу
    # - revenue_total: сумма выручки из таблицы
    fallback_amount_by_type: dict[str, float] = defaultdict(float)
    revenue_total_by_type: dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
    for w in works:
        op_type = _get_operation_type(w)
        if not op_type or op_type in ("advance", "landfill_unload"):
            continue
        revenue = _parse_revenue(getattr(w, "revenue", None))
        if revenue is not None:
            revenue_total_by_type[op_type] += revenue
            continue
        amount = _parse_amount(w.object_count)
        fallback_amount_by_type[op_type] += amount

    op_types = list(dict.fromkeys([*fallback_amount_by_type.keys(), *revenue_total_by_type.keys()]))
    if not op_types:
        return []

    items = []
    for op_type in op_types:
        fallback_amount = fallback_amount_by_type.get(op_type, 0.0)
        revenue_total = revenue_total_by_type.get(op_type, Decimal("0.00"))

        price_rec = prices_repo.get_by_counterparty_and_operation(session, counterparty_id, op_type)
        if fallback_amount > 0 and not price_rec:
            raise ValueError(
                f"Не найдена цена для контрагента id={counterparty_id}, тип операции={op_type}"
            )

        display_name = (
            ops_config.get(op_type, {}).get("display_name") or op_type
        )

        if revenue_total > 0:
            fallback_total = Decimal("0.00")
            if fallback_amount > 0 and price_rec is not None:
                fallback_amount_dec = Decimal(str(round(fallback_amount, 2)))
                fallback_total = (Decimal(price_rec.price) * fallback_amount_dec).quantize(
                    _MONEY_Q,
                    rounding=ROUND_HALF_UP,
                )
            total_price = (revenue_total + fallback_total).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
            items.append({
                "name": display_name,
                "price": float(total_price),
                "unit": "ед.",
                "vat": (price_rec.vat if price_rec else "None") or "None",
                "amount": 1.0,
            })
            continue

        items.append({
            "name": display_name,
            "price": float(price_rec.price),
            "unit": "ед.",
            "vat": price_rec.vat or "None",
            "amount": round(fallback_amount, 2),
        })
    return items
