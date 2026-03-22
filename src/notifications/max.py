"""
Отправка уведомлений в MAX бухгалтерам.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import os
from typing import Any
from maxapi.enums.parse_mode import ParseMode

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
        "💰 **Выставлен счёт**",
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
            "‼️ **Необходимо в ТБанке создать Акт для данного счета и отправить оба документа в ЭДО**",
        ]
    )
    text = "\n".join(lines)

    bot = Bot(token=token)

    async def _send() -> None:
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN)
        finally:
            await _close_bot(bot)

    try:
        asyncio.run(_send())
        logger.info("MAX: уведомление отправлено в чат %s", chat_id)
        return True
    except Exception as e:
        logger.error("MAX: ошибка отправки — %s", e)
        return False


async def _close_bot(bot: Any) -> None:
    """
    Безопасное закрытие ресурсов maxapi.Bot.

    Нужен явный close, иначе при короткоживущем процессе остаются
    unclosed aiohttp sessions/connectors.
    """
    for method_name in ("close", "aclose", "shutdown"):
        method = getattr(bot, method_name, None)
        if callable(method):
            try:
                result = method()
                if inspect.isawaitable(result):
                    await result
                return
            except Exception as e:
                logger.debug("MAX: не удалось закрыть bot.%s(): %s", method_name, e)

    if await _close_possible_session(bot):
        return

    for holder_name in ("api", "_api", "client", "_client"):
        holder = getattr(bot, holder_name, None)
        if holder and await _close_possible_session(holder):
            return


async def _close_possible_session(obj: Any) -> bool:
    """Пытаемся найти и закрыть aiohttp-сессию в объекте."""
    for session_name in ("session", "_session", "client_session", "_client_session"):
        session = getattr(obj, session_name, None)
        if session is None:
            continue
        close = getattr(session, "close", None)
        if not callable(close):
            continue
        try:
            result = close()
            if inspect.isawaitable(result):
                await result
            return True
        except Exception as e:
            logger.debug("MAX: не удалось закрыть session %s: %s", session_name, e)
    return False
