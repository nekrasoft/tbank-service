"""
Сборка позиций счёта из работ и прайсов.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path

from sqlalchemy.orm import Session

from src.db.models import Work
from src.db.repos import prices as prices_repo

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OPERATIONS_PATH = PROJECT_ROOT / "config" / "operations.json"

# Маппинг (структура, операция) -> operation_type
_OPERATION_TYPE_MAP: dict[tuple[str, str], str] = {}


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

    # Группировка по operation_type с суммированием количества
    by_type: dict[str, float] = defaultdict(float)
    for w in works:
        op_type = _get_operation_type(w)
        if not op_type or op_type in ("advance", "landfill_unload"):
            continue
        amount = _parse_amount(w.object_count)
        by_type[op_type] += amount

    if not by_type:
        return []

    items = []
    for op_type, total_amount in by_type.items():
        price_rec = prices_repo.get_by_counterparty_and_operation(
            session, counterparty_id, op_type
        )
        if not price_rec:
            raise ValueError(
                f"Не найдена цена для контрагента id={counterparty_id}, "
                f"тип операции={op_type}"
            )
        display_name = (
            ops_config.get(op_type, {}).get("display_name") or op_type
        )
        items.append({
            "name": display_name,
            "price": float(price_rec.price),
            "unit": "ед.",
            "vat": price_rec.vat or "None",
            "amount": round(total_amount, 2),
        })
    return items
