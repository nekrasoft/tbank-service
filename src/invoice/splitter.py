"""
Разбиение работ на группы для формирования отдельных счетов.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from src.db.models import Work

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SPLIT_RULES_PATH = PROJECT_ROOT / "config" / "invoice_split_rules.json"


@dataclass
class InvoiceWorkGroup:
    """Группа работ для отдельного счёта."""
    key: str
    label: str | None
    email: str | list[str] | None
    works: list[Work]


@dataclass
class _GroupRule:
    key: str
    label: str | None
    email: str | list[str] | None
    note_contains_any: list[str]
    is_default: bool


@lru_cache(maxsize=1)
def _load_split_rules() -> dict[str, Any]:
    """Загрузка правил разбиения из config/invoice_split_rules.json."""
    if not SPLIT_RULES_PATH.exists():
        return {}
    try:
        with open(SPLIT_RULES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error("Не удалось прочитать %s: %s", SPLIT_RULES_PATH, e)
        return {}
    if isinstance(data, dict):
        return data
    logger.error("Ожидался JSON-объект в %s, получено: %s", SPLIT_RULES_PATH, type(data).__name__)
    return {}


def _normalize_group_rules(raw_groups: Any) -> list[_GroupRule]:
    """Нормализация списка правил группировки."""
    if not isinstance(raw_groups, list):
        return []

    def _normalize_email(raw_email: Any) -> str | list[str] | None:
        if isinstance(raw_email, str):
            email = raw_email.strip()
            return email or None
        if isinstance(raw_email, list):
            emails: list[str] = []
            for value in raw_email:
                email = str(value).strip()
                if email:
                    emails.append(email)
            return emails or None
        return None

    normalized: list[_GroupRule] = []
    for idx, raw in enumerate(raw_groups, start=1):
        if not isinstance(raw, dict):
            continue
        key = str(raw.get("key") or f"group_{idx}").strip().lower()
        if not key:
            key = f"group_{idx}"
        label_raw = raw.get("label")
        label = str(label_raw).strip() if isinstance(label_raw, str) else None
        email = _normalize_email(raw.get("email"))
        note_tokens_raw = raw.get("note_contains_any")
        note_tokens: list[str] = []
        if isinstance(note_tokens_raw, list):
            for token in note_tokens_raw:
                token_norm = str(token).strip().lower()
                if token_norm:
                    note_tokens.append(token_norm)
        is_default = bool(raw.get("default"))
        normalized.append(
            _GroupRule(
                key=key,
                label=label or None,
                email=email,
                note_contains_any=note_tokens,
                is_default=is_default,
            )
        )
    return normalized


def split_works_for_counterparty(
    *,
    counterparty_short_name: str,
    works: list[Work],
) -> list[InvoiceWorkGroup]:
    """
    Разбивает список работ контрагента по правилам из invoice_split_rules.json.

    Если правило не найдено, возвращает одну группу со всеми работами.
    """
    if not works:
        return []

    rules = _load_split_rules()
    counterparties = rules.get("counterparties")
    if not isinstance(counterparties, dict):
        return [InvoiceWorkGroup(key="default", label=None, email=None, works=works)]

    cp_key = (counterparty_short_name or "").strip()
    raw_cp_rules = counterparties.get(cp_key)
    if not isinstance(raw_cp_rules, dict):
        cp_key_lower = cp_key.lower()
        for key, value in counterparties.items():
            if str(key).strip().lower() == cp_key_lower and isinstance(value, dict):
                raw_cp_rules = value
                break
    if not isinstance(raw_cp_rules, dict):
        return [InvoiceWorkGroup(key="default", label=None, email=None, works=works)]

    group_rules = _normalize_group_rules(raw_cp_rules.get("groups"))
    if not group_rules:
        return [InvoiceWorkGroup(key="default", label=None, email=None, works=works)]

    default_rule = next((rule for rule in group_rules if rule.is_default), None)
    if default_rule is None:
        default_rule = _GroupRule(
            key="default",
            label="Остальное",
            email=None,
            note_contains_any=[],
            is_default=True,
        )
        group_rules.append(default_rule)

    grouped: dict[str, list[Work]] = {rule.key: [] for rule in group_rules}
    for work in works:
        note_norm = (work.note or "").strip().lower()
        matched_key: str | None = None
        for rule in group_rules:
            if rule.is_default:
                continue
            if any(token in note_norm for token in rule.note_contains_any):
                matched_key = rule.key
                break
        if matched_key is None:
            matched_key = default_rule.key
        grouped.setdefault(matched_key, []).append(work)

    result: list[InvoiceWorkGroup] = []
    for rule in group_rules:
        group_works = grouped.get(rule.key) or []
        if not group_works:
            continue
        result.append(
            InvoiceWorkGroup(
                key=rule.key,
                label=rule.label,
                email=rule.email,
                works=group_works,
            )
        )
    if len(result) > 1:
        logger.info(
            "Разбиение счета для %s: %s",
            counterparty_short_name,
            ", ".join(f"{group.key}={len(group.works)}" for group in result),
        )
    return result
