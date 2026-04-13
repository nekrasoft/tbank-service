"""
Клиент T-Bank API: выставление счетов.
Документация: https://developer.tbank.ru/docs/api
"""
from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import date, datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Ограничение T-Bank: 4 запроса в секунду
TBANK_RPS_LIMIT = 4

_KPP_RE = re.compile(r"^\d{9}$")
_PHONE_RE = re.compile(r"^\+\d{10,15}$")
_EMAIL_SPLIT_RE = re.compile(r"[,\n;]+")


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


def _get_base_url() -> str:
    """Базовый URL API из .env."""
    return os.environ.get(
        "TBANK_API_URL",
        "https://business.tbank.ru/openapi/api/v1",
    ).rstrip("/")


def _get_token() -> str:
    """Токен авторизации."""
    token = os.environ.get("TBANK_TOKEN", "").strip()
    if not token:
        raise ValueError("Задайте TBANK_TOKEN в .env")
    return token


def _format_utc_datetime(dt: datetime) -> str:
    """Приведение datetime к RFC3339 UTC формату для query-параметров выписки."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def send_invoice(
    *,
    invoice_number: str,
    due_date: date,
    invoice_date: date | None = None,
    payer_name: str,
    payer_inn: str,
    payer_kpp: str,
    items: list[dict[str, Any]],
    email: str | list[str] | None = None,
    contact_phone: str | None = None,
    comment: str | None = None,
    custom_payment_purpose: str | None = None,
    account_number: str | None = None,
) -> dict[str, Any]:
    """
    Выставление счёта через T-Bank API.

    :param invoice_number: Номер счёта (только цифры, до 15 символов)
    :param due_date: Срок оплаты
    :param invoice_date: Дата выставления (если None — текущая)
    :param payer_name: Наименование плательщика
    :param payer_inn: ИНН плательщика
    :param payer_kpp: КПП плательщика
    :param items: Позиции счёта [{name, price, unit, vat, amount}]
    :param email: Email(ы) для отправки счёта (строка или список строк)
    :param contact_phone: Телефон в формате +79XXXXXXXXX
    :param comment: Комментарий к счёту
    :param custom_payment_purpose: Назначение платежа
    :param account_number: Расчётный счёт (опционально)
    :return: Ответ API (содержит invoiceId и др.)
    """
    url = f"{_get_base_url()}/invoice/send"
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
        "X-Request-Id": str(uuid.uuid4()),
    }

    due_str = due_date.strftime("%Y-%m-%d")
    inv_date = invoice_date or date.today()
    inv_date_str = inv_date.strftime("%Y-%m-%d")

    payer: dict[str, Any] = {
        "name": payer_name[:512],
        "inn": payer_inn,
    }
    payer_kpp_norm = (payer_kpp or "").strip()
    if payer_kpp_norm:
        if _KPP_RE.match(payer_kpp_norm):
            payer["kpp"] = payer_kpp_norm
        else:
            logger.warning("T-Bank: пропускаем некорректный payer.kpp='%s'", payer_kpp)

    payload = {
        "invoiceNumber": invoice_number,
        "dueDate": due_str,
        "invoiceDate": inv_date_str,
        "payer": payer,
        "items": [
            {
                "name": str(item["name"])[:1000],
                "price": float(item["price"]),
                "unit": str(item.get("unit", "шт"))[:50],
                "vat": str(item.get("vat", "None")),
                "amount": float(item["amount"]),
            }
            for item in items
        ],
    }

    if account_number:
        payload["accountNumber"] = account_number

    contacts = []
    for email_value in _normalize_emails(email):
        contacts.append({"email": email_value})
    if contact_phone:
        phone_norm = str(contact_phone).strip()
        if _PHONE_RE.match(phone_norm):
            contacts.append({"contactPhone": phone_norm})
        else:
            logger.warning(
                "T-Bank: пропускаем некорректный contactPhone='%s' (ожидается +79XXXXXXXXX)",
                contact_phone,
            )
    if contacts:
        payload["contacts"] = contacts

    if comment:
        payload["comment"] = str(comment)[:1000]
    if custom_payment_purpose:
        payload["customPaymentPurpose"] = str(custom_payment_purpose)[:512]

    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, json=payload, headers=headers)
        if resp.is_error:
            response_preview = resp.text[:2000]
            logger.error(
                "T-Bank: ошибка выставления счёта %s, status=%s, body=%s",
                invoice_number,
                resp.status_code,
                response_preview,
            )
            logger.error(
                "T-Bank: payload(invoice/send) для счёта %s: %s",
                invoice_number,
                payload,
            )
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            "T-Bank: счёт %s выставлен, invoiceId=%s",
            invoice_number,
            data.get("invoiceId", "?"),
        )
        return data


def get_invoice_info(invoice_id: str) -> dict[str, Any]:
    """
    Получение информации о выставленном счёте.

    :param invoice_id: Идентификатор счёта в T-Bank
    :return: Данные счёта (статус и др.)
    """
    url = f"{_get_base_url()}/invoice/{invoice_id}/info"
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "X-Request-Id": str(uuid.uuid4()),
    }

    with httpx.Client(timeout=15.0) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()


def get_statement(
    *,
    account_number: str,
    from_dt: datetime,
    to_dt: datetime,
    cursor: str | None = None,
    limit: int = 200,
    operation_status: str | None = "Transaction",
    operation_type: str | None = None,
    with_balances: bool = False,
) -> dict[str, Any]:
    """
    Получение выписки по счету через GET /api/v1/statement.

    :param account_number: Расчетный счет, по которому читаем выписку
    :param from_dt: Левая граница периода (UTC)
    :param to_dt: Правая граница периода (UTC)
    :param cursor: Курсор пагинации (nextCursor из предыдущего ответа)
    :param limit: Размер страницы (макс. 200)
    :param operation_status: Фильтр статуса операции (обычно Transaction)
    :param operation_type: Фильтр типа операции (если нужен)
    :param with_balances: Возвращать balances (опционально)
    :return: JSON ответа API
    """
    url = f"{_get_base_url()}/statement"
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "X-Request-Id": str(uuid.uuid4()),
    }
    params: dict[str, Any] = {
        "accountNumber": account_number,
        "from": _format_utc_datetime(from_dt),
        "to": _format_utc_datetime(to_dt),
        "limit": max(1, min(int(limit), 200)),
        "withBalances": "true" if with_balances else "false",
    }
    if cursor:
        params["cursor"] = cursor
    if operation_status:
        params["operationStatus"] = operation_status
    if operation_type:
        params["operationType"] = operation_type

    with httpx.Client(timeout=30.0) as client:
        resp = client.get(url, params=params, headers=headers)
        if resp.is_error:
            logger.error(
                "T-Bank: ошибка получения выписки, account=%s status=%s body=%s",
                account_number,
                resp.status_code,
                resp.text[:2000],
            )
        resp.raise_for_status()
        return resp.json()
