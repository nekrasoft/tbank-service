"""
Создание задач в Bitrix24 для бухгалтеров.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from src.bitrix.client import add_task
from src.notifications.max import build_invoice_notification_text

logger = logging.getLogger(__name__)

_TASK_WEBHOOK_ENV = "BITRIX24_TASK_WEBHOOK_URL"
_TASK_TITLE = "[Киров] Обработать новый счет для контрагента"
_TASK_RESPONSIBLE_ID = 31746
_TASK_TAGS = ["#киров", "#новый_счет"]
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
) -> bool:
    """Создаёт задачу в Bitrix24 по факту выставления счёта."""
    if not _is_task_webhook_configured():
        return False

    text = build_invoice_notification_text(
        counterparty_name=counterparty_name,
        invoice_number=invoice_number,
        tbank_invoice_id=tbank_invoice_id,
        invoice_link=invoice_link,
    )
    deadline = datetime.now().astimezone() + timedelta(days=1)

    try:
        task_id = add_task(
            title=_TASK_TITLE,
            responsible_id=_TASK_RESPONSIBLE_ID,
            description=text,
            tags=_TASK_TAGS,
            deadline=deadline,
        )
        logger.info("Bitrix24 task: создана задача id=%s по счёту %s", task_id, invoice_number)
        return True
    except Exception as e:
        logger.error("Bitrix24 task: ошибка создания задачи по счёту %s — %s", invoice_number, e)
        return False

