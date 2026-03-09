"""
Отправка уведомлений в Telegram бухгалтерам.
"""
from __future__ import annotations

import io
import logging
import os
from pathlib import Path

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
    act_pdf_path: str | Path | None = None,
) -> bool:
    """
    Отправка уведомления бухгалтерам о выставленном счёте.

    :param counterparty_name: Наименование контрагента
    :param invoice_number: Номер счёта
    :param tbank_invoice_id: ID счёта в T-Bank
    :param invoice_link: Ссылка на оплату (если есть)
    :param act_pdf_path: Путь к PDF акта для отправки
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
    try:
        if act_pdf_path and Path(act_pdf_path).exists():
            with open(act_pdf_path, "rb") as f:
                bot.send_document(
                    chat_id=chat_id,
                    document=f,
                    filename=f"Акт_{invoice_number}.pdf",
                    caption=text,
                )
        else:
            bot.send_message(chat_id=chat_id, text=text)
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
    act_pdf_bytes: bytes | None = None,
) -> bool:
    """
    Отправка уведомления с PDF акта из bytes (без сохранения в файл).

    :param act_pdf_bytes: PDF акта в виде bytes
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
    try:
        if act_pdf_bytes:
            bot.send_document(
                chat_id=chat_id,
                document=io.BytesIO(act_pdf_bytes),
                filename=f"Акт_{invoice_number}.pdf",
                caption=text,
            )
        else:
            bot.send_message(chat_id=chat_id, text=text)
        logger.info("Telegram: уведомление отправлено в чат %s", chat_id)
        return True
    except Exception as e:
        logger.error("Telegram: ошибка отправки — %s", e)
        return False
