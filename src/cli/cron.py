"""
CLI: выставление счетов по крону (последний день месяца).
Запуск: python -m src.cli.cron
Использование: добавить в crontab на последний день месяца.
"""
from __future__ import annotations

import calendar
import logging
import sys
import time
from datetime import date
from pathlib import Path

# Загрузка .env
_env = Path(__file__).resolve().parent.parent.parent / ".env"
if _env.exists():
    from dotenv import load_dotenv
    load_dotenv(_env)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Ограничение T-Bank: 4 запроса в секунду
TBANK_DELAY_SEC = 0.3


def _is_last_day_of_month() -> bool:
    """Проверка: сегодня последний день месяца."""
    today = date.today()
    _, last_day = calendar.monthrange(today.year, today.month)
    return today.day == last_day


def main() -> None:
    """Запуск крона: синк + выставление счетов для всех контрагентов с новыми работами."""
    from src.db.connection import get_session
    from src.db.repos import counterparties as cp_repo
    from src.db.repos import invoices as inv_repo
    from src.db.repos import invoice_number as num_repo
    from src.db.repos import works as works_repo
    from src.invoice.builder import build_invoice_items
    from src.notifications.telegram import send_invoice_notification_bytes
    from src.sheets.sync import sync_sheets_to_mysql
    from src.tbank.client import send_invoice
    from datetime import timedelta

    # Опционально: проверка последнего дня месяца
    # if not _is_last_day_of_month():
    #     logger.info("Сегодня не последний день месяца — выход")
    #     return

    # 1. Синхронизация Sheets → MySQL
    logger.info("Синхронизация Sheets → MySQL...")
    sync_sheets_to_mysql()

    session = get_session()
    issued = 0
    errors = []
    try:
        groups = works_repo.get_all_uninvoiced_groups(session)
        if not groups:
            logger.info("Нет контрагентов с невыставленными работами")
            return

        for counterparty_name, note in groups:
            try:
                cp = cp_repo.get_by_short_name(session, counterparty_name, note)
                if not cp:
                    logger.warning(
                        "Контрагент не найден в справочнике: %s (примечание: %s) — пропуск",
                        counterparty_name,
                        note or "(пусто)",
                    )
                    continue

                works = works_repo.get_uninvoiced_by_counterparty(
                    session, counterparty_name, note
                )
                if not works:
                    continue

                items = build_invoice_items(session, works, cp.id)
                if not items:
                    logger.warning(
                        "Нет цен для %s — пропуск",
                        counterparty_name,
                    )
                    continue

                inv_num = num_repo.get_next_number(session)
                today = date.today()
                due_date = today + timedelta(days=14)

                resp = send_invoice(
                    invoice_number=inv_num,
                    due_date=due_date,
                    invoice_date=today,
                    payer_name=cp.name,
                    payer_inn=cp.inn,
                    payer_kpp=cp.kpp or "",
                    items=items,
                    email=cp.email,
                    contact_phone=cp.phone if cp.phone else None,
                )
                tbank_id = resp.get("invoiceId") or resp.get("id")
                time.sleep(TBANK_DELAY_SEC)

                inv = inv_repo.create(
                    session,
                    invoice_number=inv_num,
                    tbank_invoice_id=str(tbank_id) if tbank_id else None,
                    counterparty_id=cp.id,
                    due_date=due_date,
                )
                for item in items:
                    inv_repo.add_item(
                        session,
                        invoice_id=inv.id,
                        name=item["name"],
                        price=item["price"],
                        amount=item["amount"],
                        unit=item.get("unit", "ед."),
                        vat=item.get("vat", "None"),
                    )
                works_repo.update_invoice_id(session, [w.id for w in works], inv.id)

                send_invoice_notification_bytes(
                    counterparty_name=cp.name,
                    invoice_number=inv_num,
                    tbank_invoice_id=str(tbank_id) if tbank_id else None,
                )
                issued += 1
                logger.info("Счёт %s выставлен для %s", inv_num, cp.name)
            except Exception as e:
                errors.append(f"{counterparty_name}: {e}")
                logger.exception("Ошибка при выставлении счёта для %s", counterparty_name)

        session.commit()
        logger.info("Крон завершён. Выставлено счетов: %s", issued)
        if errors:
            logger.warning("Ошибки: %s", errors)
    except Exception as e:
        session.rollback()
        logger.error("Критическая ошибка крона: %s", e)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
