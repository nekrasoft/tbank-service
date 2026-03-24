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


def _method_url(method_name: str) -> str:
    """Собирает URL метода Bitrix24 из базового webhook URL."""
    webhook = _get_webhook_url()
    with_json_suffix = f"{method_name}.json"

    if webhook.endswith(with_json_suffix):
        return webhook
    if webhook.endswith(method_name):
        return f"{webhook}.json"
    return f"{webhook}/{with_json_suffix}"


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

    url = _method_url("crm.company.add")
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, json=payload)
        if resp.is_error:
            body_preview = resp.text[:2000]
            logger.error(
                "Bitrix24: ошибка crm.company.add, status=%s, body=%s",
                resp.status_code,
                body_preview,
            )
        resp.raise_for_status()

    data = resp.json()
    if "error" in data:
        error = data.get("error")
        description = data.get("error_description")
        raise RuntimeError(f"Bitrix24 API error: {error} ({description})")

    result = data.get("result")
    if isinstance(result, int):
        return result
    if isinstance(result, str) and result.isdigit():
        return int(result)
    raise RuntimeError(f"Неожиданный ответ Bitrix24: {data}")
