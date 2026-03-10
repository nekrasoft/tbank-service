"""
Отправка уведомлений в Telegram бухгалтерам.
"""
from __future__ import annotations

import asyncio
import logging
import os

logger = logging.getLogger(__name__)


def _get_bot_token() -> str:
    """Токен бота из .env."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("Задайте TELEGRAM_BOT_TOKEN в .env")
    return token


def _get_accountants_chat_id() -> str:
    """ID чата бухгалтеров."""
    chat_id = os.environ.get("TELEGRAM_ACCOUNTANTS_CHAT_ID", "").strip()
    if not chat_id:
        raise ValueError("Задайте TELEGRAM_ACCOUNTANTS_CHAT_ID в .env")
    return chat_id


def send_invoice_notification(
    *,
    counterparty_name: str,
    invoice_number: str,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
) -> bool:
    """
    Отправка уведомления бухгалтерам о выставленном счёте.

    :param counterparty_name: Наименование контрагента
    :param invoice_number: Номер счёта
    :param tbank_invoice_id: ID счёта в T-Bank
    :param invoice_link: Ссылка на оплату (если есть)
    :return: True при успехе
    """
    try:
        from telegram import Bot
    except ImportError:
        logger.warning("python-telegram-bot не установлен — уведомление не отправлено")
        return False

    token = _get_bot_token()
    chat_id = _get_accountants_chat_id()

    lines = [
        "📋 Выставлен счёт",
        f"Контрагент: {counterparty_name}",
        f"Номер счёта: {invoice_number}",
    ]
    if tbank_invoice_id:
        lines.append(f"T-Bank ID: {tbank_invoice_id}")
    if invoice_link:
        lines.append(f"Ссылка: {invoice_link}")
    text = "\n".join(lines)

    bot = Bot(token=token)

    async def _send() -> None:
        await bot.send_message(chat_id=chat_id, text=text)

    try:
        asyncio.run(_send())
        logger.info("Telegram: уведомление отправлено в чат %s", chat_id)
        return True
    except Exception as e:
        logger.error("Telegram: ошибка отправки — %s", e)
        return False


def send_invoice_notification_bytes(
    *,
    counterparty_name: str,
    invoice_number: str,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
) -> bool:
    """
    Отправка уведомления бухгалтерам о выставленном счёте.

    :param counterparty_name: Наименование контрагента
    :param invoice_number: Номер счёта
    :param tbank_invoice_id: ID счёта в T-Bank
    :param invoice_link: Ссылка на оплату (если есть)
    :return: True при успехе
    """
    return send_invoice_notification(
        counterparty_name=counterparty_name,
        invoice_number=invoice_number,
        tbank_invoice_id=tbank_invoice_id,
        invoice_link=invoice_link,
    )
