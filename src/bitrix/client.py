"""Клиент Bitrix24 CRM (входящий вебхук)."""
from __future__ import annotations

import logging
import os
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_EMAIL_SPLIT_RE = re.compile(r"[,;\n]+")


def _get_webhook_url() -> str:
    """URL входящего вебхука Bitrix24 из .env."""
    webhook = os.environ.get("BITRIX24_WEBHOOK_URL", "").strip()
    if not webhook:
        raise ValueError("Задайте BITRIX24_WEBHOOK_URL в .env")
    return webhook.rstrip("/")


def _get_webhook_base_url() -> str:
    """
    Приводит webhook URL к базовому виду без имени метода.

    Поддерживаются варианты:
    - https://<portal>/rest/<user>/<code>
    - .../crm.company.add
    - .../crm.company.add.json
    """
    webhook = _get_webhook_url()

    if webhook.endswith(".json"):
        return webhook.rsplit("/", 1)[0]

    last_segment = webhook.rsplit("/", 1)[-1]
    if last_segment.startswith("crm."):
        return webhook.rsplit("/", 1)[0]
    return webhook


def _method_url(method_name: str) -> str:
    """Собирает URL метода Bitrix24 из базового webhook URL."""
    base = _get_webhook_base_url()
    return f"{base}/{method_name}.json"


def _call_method(method_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Вызов REST-метода Bitrix24 по webhook URL."""
    url = _method_url(method_name)
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, json=payload)
        if resp.is_error:
            body_preview = resp.text[:2000]
            logger.error(
                "Bitrix24: ошибка %s, status=%s, body=%s",
                method_name,
                resp.status_code,
                body_preview,
            )
        resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Bitrix24: неожиданный формат ответа {method_name}: {data}")
    if "error" in data:
        error = data.get("error")
        description = data.get("error_description")
        raise RuntimeError(f"Bitrix24 API error: {error} ({description})")
    return data


def _normalize_emails(value: str | list[str] | None) -> list[str]:
    """Нормализация email(ов) из строки/списка в уникальный список."""
    if value is None:
        return []

    parts: list[str] = []
    if isinstance(value, str):
        parts = _EMAIL_SPLIT_RE.split(value)
    else:
        for item in value:
            parts.extend(_EMAIL_SPLIT_RE.split(str(item)))

    emails: list[str] = []
    seen: set[str] = set()
    for raw in parts:
        email = raw.strip()
        if not email:
            continue
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        emails.append(email)
    return emails


def add_company(
    *,
    title: str,
    email: str | list[str] | None = None,
    phone: str | None = None,
    comments: str | None = None,
    custom_fields: dict[str, Any] | None = None,
) -> int:
    """
    Создаёт компанию в Bitrix24 CRM методом crm.company.add.

    Возвращает ID созданной компании.
    """
    fields: dict[str, Any] = {
        "TITLE": str(title).strip()[:255],
    }

    emails = _normalize_emails(email)
    if emails:
        fields["EMAIL"] = [
            {
                "VALUE": value,
                "VALUE_TYPE": "WORK",
            }
            for value in emails
        ]

    phone_norm = (phone or "").strip()
    if phone_norm:
        fields["PHONE"] = [
            {
                "VALUE": phone_norm,
                "VALUE_TYPE": "WORK",
            }
        ]

    comments_norm = (comments or "").strip()
    if comments_norm:
        fields["COMMENTS"] = comments_norm

    for field_name, value in (custom_fields or {}).items():
        field_key = str(field_name).strip()
        if not field_key:
            continue
        value_norm = (value or "").strip() if isinstance(value, str) else value
        if value_norm in (None, ""):
            continue
        fields[field_key] = value_norm

    payload = {
        "fields": fields,
        "params": {
            "REGISTER_SONET_EVENT": "N",
        },
    }
    data = _call_method("crm.company.add", payload)

    result = data.get("result")
    if isinstance(result, int):
        return result
    if isinstance(result, str) and result.isdigit():
        return int(result)
    raise RuntimeError(f"Неожиданный ответ Bitrix24: {data}")


def list_companies(
    *,
    filter_fields: dict[str, Any],
    select_fields: list[str] | None = None,
    order_fields: dict[str, str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Получение списка компаний из Bitrix24 по фильтру crm.company.list."""
    result_items: list[dict[str, Any]] = []
    start = 0
    pages = 0

    while True:
        pages += 1
        if pages > 500:
            raise RuntimeError("Bitrix24: слишком много страниц при crm.company.list")

        payload: dict[str, Any] = {
            "filter": filter_fields or {},
            "select": select_fields or ["ID", "TITLE"],
            "order": order_fields or {"ID": "ASC"},
            "start": start,
        }
        data = _call_method("crm.company.list", payload)
        page_items = data.get("result")
        if not isinstance(page_items, list):
            raise RuntimeError(f"Неожиданный ответ Bitrix24 crm.company.list: {data}")

        for item in page_items:
            if isinstance(item, dict):
                result_items.append(item)
            else:
                raise RuntimeError(f"Bitrix24: неожиданный элемент result: {item}")

        if limit is not None and limit > 0 and len(result_items) >= limit:
            return result_items[:limit]

        next_start = data.get("next")
        if next_start is None:
            return result_items
        try:
            start = int(next_start)
        except (TypeError, ValueError):
            raise RuntimeError(f"Bitrix24: неожиданный next в crm.company.list: {data}") from None
