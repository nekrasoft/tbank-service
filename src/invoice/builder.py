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
_BUNKER_VOLUME_M3 = 8.0
_TRIP_VOLUME_M3 = 30.0


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


def _parse_amount_decimal(object_count: str | None) -> Decimal:
    """Парсинг количества в Decimal(2) для расчёта ценовых сегментов."""
    if not object_count or not str(object_count).strip():
        return Decimal("1.00")
    raw = str(object_count).strip().replace(",", ".")
    try:
        dec = Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal("1.00")
    if dec <= 0:
        return Decimal("0.01")
    return dec.quantize(_MONEY_Q, rounding=ROUND_HALF_UP)


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


def _comment_unit_and_volume_m3(work: Work) -> tuple[str, float]:
    """Единица и объём в м3 на 1 единицу для комментария счёта."""
    op_type = _get_operation_type(work)
    if op_type == "trip_removal":
        return "рейс", _TRIP_VOLUME_M3
    return "шт", _BUNKER_VOLUME_M3


def build_invoice_comment(works: list[Work], contract: str | None = None) -> str:
    """
    Сборка комментария к счёту для T-Bank.

    Формат:
    Договор №111 от 12.03.2025
    Оказаны услуги за период 05.03.2026 - 10.03.2026:
    05.03.2026 Свободы 111А - 3 шт, 10.03.2026 Знак - 4 шт
    """
    contract_line = (contract or "").strip()
    grouped: dict[tuple[date_type, str, str, str | None], float] = defaultdict(float)
    total_volume = 0.0
    for work in works:
        amount = _parse_amount(work.object_count)
        note = (work.note or "").strip()
        unit, volume_m3 = _comment_unit_and_volume_m3(work)
        op_type = _get_operation_type(work)
        grouped[(work.date, note, unit, op_type)] += amount
        total_volume += amount * volume_m3

    if not grouped:
        body = "Оказаны услуги."
        return f"{contract_line}\n{body}" if contract_line else body

    parts: list[str] = []
    for (work_date, note, unit, _op_type), total in sorted(
        grouped.items(),
        key=lambda x: (x[0][0], x[0][1], x[0][2], x[0][3] or ""),
    ):
        date_str = work_date.strftime("%d.%m.%Y")
        amount_str = _format_amount(total)
        if note:
            parts.append(f"{date_str} {note} - {amount_str} {unit}")
        else:
            parts.append(f"{date_str} - {amount_str} {unit}")

    work_dates = [key[0] for key in grouped]
    period_start = min(work_dates)
    period_end = max(work_dates)
    period_text = f"{period_start.strftime('%d.%m.%Y')} - {period_end.strftime('%d.%m.%Y')}"

    total_volume_str = _format_amount(total_volume)
    body = (
        f"Оказаны услуги за период {period_text}:\n"
        + ", ".join(parts)
        + f"\nОбщий объем: {total_volume_str} м3"
    )
    return f"{contract_line}\n{body}" if contract_line else body


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

    price_cache: dict[str, object | None] = {}

    def _get_price_record(op_type: str):
        if op_type not in price_cache:
            price_cache[op_type] = prices_repo.get_by_counterparty_and_operation(
                session,
                counterparty_id,
                op_type,
            )
        return price_cache[op_type]

    # Сегменты для позиции счёта: разбиваем по operation_type и моментам смены unit price.
    # Если unit price меняется, начинаем новую позицию (даже при одинаковом display_name).
    segments_by_type: dict[str, list[dict]] = defaultdict(list)
    active_segment_by_type: dict[str, dict] = {}
    op_type_order: list[str] = []

    for w in sorted(works, key=lambda x: (x.date, x.id or 0)):
        op_type = _get_operation_type(w)
        if not op_type or op_type in ("advance", "landfill_unload"):
            continue

        if op_type not in op_type_order:
            op_type_order.append(op_type)

        amount_dec = _parse_amount_decimal(w.object_count)
        revenue = _parse_revenue(getattr(w, "revenue", None))
        if revenue is not None:
            unit_price = (revenue / amount_dec).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
        else:
            price_rec = _get_price_record(op_type)
            if not price_rec:
                raise ValueError(
                    f"Не найдена цена для контрагента id={counterparty_id}, тип операции={op_type}"
                )
            unit_price = Decimal(str(price_rec.price)).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)

        active = active_segment_by_type.get(op_type)
        if active is None:
            active_segment_by_type[op_type] = {
                "unit_price": unit_price,
                "amount": amount_dec,
                "start_date": w.date,
                "end_date": w.date,
            }
            continue

        if active["unit_price"] != unit_price:
            segments_by_type[op_type].append(active)
            logger.info(
                "Смена цены для %s: %s -> %s (с %s)",
                op_type,
                active["unit_price"],
                unit_price,
                w.date.strftime("%d.%m.%Y"),
            )
            active_segment_by_type[op_type] = {
                "unit_price": unit_price,
                "amount": amount_dec,
                "start_date": w.date,
                "end_date": w.date,
            }
            continue

        active["amount"] += amount_dec
        active["end_date"] = w.date

    if not active_segment_by_type:
        return []

    items = []
    for op_type in op_type_order:
        active = active_segment_by_type.get(op_type)
        if active is not None:
            segments_by_type[op_type].append(active)

        display_name = ops_config.get(op_type, {}).get("display_name") or op_type
        price_rec = _get_price_record(op_type)
        vat = (price_rec.vat if price_rec else "None") or "None"

        for segment in segments_by_type.get(op_type, []):
            amount = Decimal(segment["amount"]).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
            unit_price = Decimal(segment["unit_price"]).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
            items.append({
                "name": display_name,
                "price": float(unit_price),
                "unit": "шт",
                "vat": vat,
                "amount": float(amount),
                "operation_type": op_type,
            })
    return items
