"""
Создание задач в Bitrix24 для бухгалтеров.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import logging
import os
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from src.bitrix.client import add_task

logger = logging.getLogger(__name__)

_TASK_WEBHOOK_ENV = "BITRIX24_TASK_WEBHOOK_URL"
_TASK_TITLE_PREFIX = "[Киров] Обработать новый счет №"
_TASK_RESPONSIBLE_ID = 31648
_TASK_AUDITORS = [8, 54, 18, 33036]
_TASK_TAGS = ["киров", "новый_счет", "отправить в ЭДО"]
_TASK_PRIORITY = 2
_task_webhook_missing_logged = False


def _is_task_webhook_configured() -> bool:
    """Проверяет наличие webhook для задач в Bitrix24."""
    global _task_webhook_missing_logged
    webhook = (os.environ.get(_TASK_WEBHOOK_ENV) or "").strip()
    if webhook:
        return True
    if not _task_webhook_missing_logged:
        logger.info("%s не задан — создание задач в Bitrix24 отключено", _TASK_WEBHOOK_ENV)
        _task_webhook_missing_logged = True
    return False


def create_invoice_task(
    *,
    counterparty_name: str,
    invoice_number: str,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
    pdf_url: str | None = None,
    invoice_items: list[dict[str, Any]] | None = None,
) -> str | None:
    """Создаёт задачу в Bitrix24 по факту выставления счёта."""
    if not _is_task_webhook_configured():
        return None

    text = _build_task_description(
        counterparty_name=counterparty_name,
        invoice_number=invoice_number,
        invoice_amount=_calculate_invoice_amount(invoice_items),
        tbank_invoice_id=tbank_invoice_id,
        invoice_link=invoice_link,
        pdf_url=pdf_url,
    )
    deadline = datetime.now().astimezone() + timedelta(days=1)
    task_title = f"{_TASK_TITLE_PREFIX}{invoice_number}"

    try:
        task_id = add_task(
            title=task_title,
            responsible_id=_TASK_RESPONSIBLE_ID,
            auditors=_TASK_AUDITORS,
            description=text,
            tags=_TASK_TAGS,
            deadline=deadline,
            priority=_TASK_PRIORITY,
            description_in_bbcode=True,
        )
        task_url = _build_task_url(task_id)
        if task_url:
            logger.info(
                "Bitrix24 task: создана задача id=%s по счёту %s, url=%s",
                task_id,
                invoice_number,
                task_url,
            )
        else:
            logger.info("Bitrix24 task: создана задача id=%s по счёту %s", task_id, invoice_number)
        return task_url
    except Exception as e:
        logger.error("Bitrix24 task: ошибка создания задачи по счёту %s — %s", invoice_number, e)
        return None


def _build_task_description(
    *,
    counterparty_name: str,
    invoice_number: str,
    invoice_amount: Decimal | None = None,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
    pdf_url: str | None = None,
) -> str:
    """
    Описание задачи для Bitrix24 в BBCode.

    DESCRIPTION_IN_BBCODE=Y нужен, чтобы [B]...[/B] отображался жирным.
    """
    lines = [
        f"[B]Контрагент[/B]: {counterparty_name}",
    ]
    if invoice_amount is not None:
        lines.append(f"[B]Сумма[/B]: {_format_money(invoice_amount)}")
    if tbank_invoice_id:
        lines.append(f"[B]T-Bank ID[/B]: {tbank_invoice_id}")
    if pdf_url:
        lines.append(f"[B]PDF[/B]: {pdf_url}")
    lines.extend(
        [
            "",
            "[B]Необходимо в ТБанке создать Акт для данного счета и отправить оба документа в ЭДО[/B]",
        ]
    )
    return "\n".join(lines)


def _calculate_invoice_amount(invoice_items: list[dict[str, Any]] | None) -> Decimal | None:
    """Считает итог по позициям счёта: sum(price * amount)."""
    if not invoice_items:
        return None

    total = Decimal("0")
    has_any = False
    for item in invoice_items:
        try:
            price = Decimal(str(item.get("price")))
            amount = Decimal(str(item.get("amount")))
        except Exception:
            continue
        total += price * amount
        has_any = True

    if not has_any:
        return None
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _format_money(value: Decimal) -> str:
    """Форматирует сумму в человекочитаемом виде для текста задачи."""
    return f"{value:.2f} ₽"


def _build_task_url(task_id: int) -> str | None:
    """Строит ссылку на задачу Bitrix24 по task_id и webhook-хосту."""
    webhook = (os.environ.get(_TASK_WEBHOOK_ENV) or "").strip()
    if not webhook:
        return None
    parsed = urlsplit(webhook)
    if not parsed.scheme or not parsed.netloc:
        return None
    portal_base = f"{parsed.scheme}://{parsed.netloc}"
    return (
        f"{portal_base}/company/personal/user/{_TASK_RESPONSIBLE_ID}/"
        f"tasks/task/view/{int(task_id)}/"
    )
