"""Read-only доступ к справочнику бункеров."""
from __future__ import annotations

from typing import Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session


def _normalize_bunker_numbers(numbers: Iterable[str]) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for number in numbers:
        raw = str(number or "").strip()
        if not raw.isdigit():
            continue
        value = int(raw)
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def get_addresses_by_counterparty_and_numbers(
    session: Session,
    *,
    counterparty_id: int,
    numbers: Iterable[str],
) -> dict[str, str]:
    """Возвращает адреса бункеров по контрагенту и номерам."""
    bunker_numbers = _normalize_bunker_numbers(numbers)
    if not bunker_numbers:
        return {}

    stmt = text(
        """
        SELECT number, address
        FROM bunkers
        WHERE counterparty_id = :counterparty_id
          AND number IN :numbers
          AND address IS NOT NULL
          AND address != ''
        """
    ).bindparams(bindparam("numbers", expanding=True))

    rows = session.execute(
        stmt,
        {
            "counterparty_id": int(counterparty_id),
            "numbers": bunker_numbers,
        },
    ).all()

    result: dict[str, str] = {}
    for number, address in rows:
        address_text = str(address or "").strip()
        if len(address_text) <= 20:
            continue
        try:
            number_key = str(int(number))
        except (TypeError, ValueError):
            number_key = str(number or "").strip()
        if number_key and number_key not in result:
            result[number_key] = address_text
    return result
