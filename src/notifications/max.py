"""
Отправка уведомлений в MAX бухгалтерам.
"""
from __future__ import annotations

import asyncio
import logging
import os

logger = logging.getLogger(__name__)


def _get_bot_token() -> str | None:
    """Токен MAX-бота из .env (None, если не задан)."""
    token = os.environ.get("MAX_BOT_TOKEN", "").strip()
    if not token:
        logger.info("MAX: MAX_BOT_TOKEN не задан — пропускаем отправку уведомления")
        return None
    return token


def _get_accountants_chat_id() -> int:
    """ID чата бухгалтеров в MAX."""
    raw = os.environ.get("MAX_ACCOUNTANTS_CHAT_ID", "").strip()
    if not raw:
        raise ValueError("Задайте MAX_ACCOUNTANTS_CHAT_ID в .env")
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError("MAX_ACCOUNTANTS_CHAT_ID должен быть числом") from e


def send_invoice_notification(
    *,
    counterparty_name: str,
    invoice_number: str,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
) -> bool:
    """
    Отправка уведомления бухгалтерам о выставленном счёте в MAX.

    :param counterparty_name: Наименование контрагента
    :param invoice_number: Номер счёта
    :param tbank_invoice_id: ID счёта в T-Bank
    :param invoice_link: Ссылка на оплату (если есть)
    :return: True при успехе
    """
    token = _get_bot_token()
    if not token:
        return False

    try:
        from maxapi import Bot
    except ImportError:
        logger.warning("maxapi не установлен — уведомление в MAX не отправлено")
        return False

    chat_id = _get_accountants_chat_id()

    lines = [
        "Выставлен счёт",
        f"Контрагент: {counterparty_name}",
        f"Номер счёта: {invoice_number}",
    ]
    if tbank_invoice_id:
        lines.append(f"T-Bank ID: {tbank_invoice_id}")
    if invoice_link:
        lines.append(f"Ссылка: {invoice_link}")
    lines.extend(
        [
            "",
            "Необходимо в ТБанке создать Акт для данного счета и отправить оба документа в ЭДО",
            "https://business.tbank.ru/sme/invoices/outgoing/submitted",
        ]
    )
    text = "\n".join(lines)

    bot = Bot(token=token)

    async def _send() -> None:
        await bot.send_message(chat_id=chat_id, text=text)

    try:
        asyncio.run(_send())
        logger.info("MAX: уведомление отправлено в чат %s", chat_id)
        return True
    except Exception as e:
        logger.error("MAX: ошибка отправки — %s", e)
        return False
