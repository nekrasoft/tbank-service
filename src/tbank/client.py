"""
Клиент T-Bank API: выставление счетов.
Документация: https://developer.tbank.ru/docs/api
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import date
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Ограничение T-Bank: 4 запроса в секунду
TBANK_RPS_LIMIT = 4


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


def send_invoice(
    *,
    invoice_number: str,
    due_date: date,
    invoice_date: date | None = None,
    payer_name: str,
    payer_inn: str,
    payer_kpp: str,
    items: list[dict[str, Any]],
    email: str | None = None,
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
    :param email: Email для отправки счёта контрагенту
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

    payload = {
        "invoiceNumber": invoice_number,
        "dueDate": due_str,
        "invoiceDate": inv_date_str,
        "payer": {
            "name": payer_name[:512],
            "inn": payer_inn,
            "kpp": payer_kpp,
        },
        "items": [
            {
                "name": str(item["name"])[:1000],
                "price": float(item["price"]),
                "unit": str(item.get("unit", "ед."))[:50],
                "vat": str(item.get("vat", "None")),
                "amount": float(item["amount"]),
            }
            for item in items
        ],
    }

    if account_number:
        payload["accountNumber"] = account_number

    contacts = []
    if email:
        contacts.append({"email": email})
    if contact_phone:
        contacts.append({"contactPhone": contact_phone})
    if contacts:
        payload["contacts"] = contacts

    if comment:
        payload["comment"] = str(comment)[:1000]
    if custom_payment_purpose:
        payload["customPaymentPurpose"] = str(custom_payment_purpose)[:512]

    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, json=payload, headers=headers)
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
    url = f"{_get_base_url()}/openapi/invoice/{invoice_id}/info"
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "X-Request-Id": str(uuid.uuid4()),
    }

    with httpx.Client(timeout=15.0) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()
