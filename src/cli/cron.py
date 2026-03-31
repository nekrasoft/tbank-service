"""
CLI: выставление счетов по крону (последний день месяца).
Запуск: python3 -m src.cli.cron
Использование: добавить в crontab на последний день месяца.
"""
from __future__ import annotations

import calendar
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

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
# Опциональный override из ENV: если задан, все счета отправляются на этот email.
DEBUG_FORCE_EMAIL = (os.environ.get("DEBUG_FORCE_EMAIL") or "").strip() or None

# Окна выставления для периодичности.
RUN_AT_EVENING_START_HOUR = 19
BIWEEKLY_MIDMONTH_DAY = 15

def _is_last_day_of_month(target_date: date) -> bool:
    """Проверка: переданная дата — последний день месяца."""
    _, last_day = calendar.monthrange(target_date.year, target_date.month)
    return target_date.day == last_day


def _is_counterparty_due(invoice_schedule: str | None, run_at: datetime) -> bool:
    """
    Проверка, должен ли контрагент выставляться в текущий запуск.

    Поддерживаемые значения:
    - monthly: последний день месяца, после 22:00
    - 2weeks: последний день месяца или 15 число, после 22:00
    - daily: в любой день после 22:00
    """
    schedule = (invoice_schedule or "monthly").strip().lower()

    if schedule == "daily":
        return run_at.hour >= RUN_AT_EVENING_START_HOUR

    if schedule == "monthly":
        return _is_last_day_of_month(run_at.date()) and run_at.hour >= RUN_AT_EVENING_START_HOUR

    if schedule == "2weeks":
        return (
            run_at.hour >= RUN_AT_EVENING_START_HOUR
            and (_is_last_day_of_month(run_at.date()) or run_at.day == BIWEEKLY_MIDMONTH_DAY)
        )

    logger.warning(
        "Неизвестный invoice_schedule='%s', используем monthly-правило",
        invoice_schedule,
    )
    return _is_last_day_of_month(run_at.date()) and run_at.hour >= RUN_AT_EVENING_START_HOUR


def _get_uninvoiced_counterparties() -> list[str]:
    """Получение контрагентов с невыставленными работами."""
    from src.db.connection import get_session
    from src.db.repos import works as works_repo

    session = get_session()
    try:
        return works_repo.get_all_uninvoiced_counterparties(session)
    finally:
        session.close()


def _prepare_pending_invoice(counterparty_name: str, run_at: datetime) -> dict[str, Any] | None:
    """Подготовка и фиксация pending-счёта в БД до вызова внешнего API."""
    from src.db.connection import get_session
    from src.db.repos import counterparties as cp_repo
    from src.db.repos import invoices as inv_repo
    from src.db.repos import invoice_number as num_repo
    from src.db.repos import works as works_repo
    from src.invoice.builder import build_invoice_comment, build_invoice_items
    from src.invoice.window import build_invoice_work_date_window, env_bool

    session = get_session()
    try:
        cp = cp_repo.get_by_short_name(session, counterparty_name, "")
        if not cp:
            logger.warning(
                "Контрагент не найден: %s — пропуск",
                counterparty_name,
            )
            return None
        if not _is_counterparty_due(cp.invoice_schedule, run_at):
            logger.info(
                "Контрагент %s (schedule=%s): вне окна выставления — пропуск",
                counterparty_name,
                cp.invoice_schedule,
            )
            return None

        strict_period = env_bool("INVOICE_STRICT_PERIOD", False)
        warn_out_of_period = env_bool("INVOICE_WARN_OUT_OF_PERIOD", True)
        date_from, date_to = build_invoice_work_date_window(
            invoice_schedule=cp.invoice_schedule,
            run_at=run_at,
            strict_period=strict_period,
        )
        if strict_period and warn_out_of_period and date_from is not None:
            old_count = works_repo.count_uninvoiced_before_date(
                session,
                counterparty_name,
                before_date=date_from,
            )
            if old_count > 0:
                logger.warning(
                    "Контрагент %s: %s невыставленных работ до %s вне текущего strict-периода",
                    counterparty_name,
                    old_count,
                    date_from.strftime("%d.%m.%Y"),
                )

        works = works_repo.get_uninvoiced_by_counterparty_for_update(
            session,
            counterparty_name,
            date_from=date_from,
            date_to=date_to,
        )
        if not works:
            return None

        items = build_invoice_items(session, works, cp.id)
        if not items:
            logger.warning("Нет цен для %s — пропуск", counterparty_name)
            return None
        comment = build_invoice_comment(works)

        today = date.today()
        due_date = today + timedelta(days=14)
        inv_num = num_repo.get_next_number(session)
        inv = inv_repo.create(
            session,
            invoice_number=inv_num,
            tbank_invoice_id=None,
            counterparty_id=cp.id,
            due_date=due_date,
            status="pending_send",
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

        claimed = works_repo.update_invoice_id(session, [w.id for w in works], inv.id)
        if claimed != len(works):
            session.rollback()
            logger.warning(
                "Работы изменились параллельно для %s (ожидалось %s, обновлено %s) — пропуск",
                counterparty_name,
                len(works),
                claimed,
            )
            return None

        session.commit()
        return {
            "invoice_id": inv.id,
            "invoice_number": inv_num,
            "counterparty_name": cp.name,
            "counterparty_short_name": cp.short_name,
            "bitrix_company_id": cp.bitrix_company_id,
            "payer_name": cp.name,
            "payer_inn": cp.inn,
            "payer_kpp": cp.kpp or "",
            "email": cp.email or None,
            "contact_phone": cp.phone or None,
            "due_date": due_date,
            "invoice_date": today,
            "items": items,
            "comment": comment,
            "sheet_row_hashes": [w.sheet_row_hash for w in works if w.sheet_row_hash],
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _mark_invoice_issued(
    *,
    invoice_id: int,
    tbank_invoice_id: str | None,
    pdf_url: str | None = None,
) -> None:
    """Фиксация успешной отправки счёта в T-Bank."""
    from src.db.connection import get_session
    from src.db.repos import invoices as inv_repo

    session = get_session()
    try:
        updated = inv_repo.mark_as_issued(
            session,
            invoice_id=invoice_id,
            tbank_invoice_id=tbank_invoice_id,
            pdf_url=pdf_url,
        )
        if updated != 1:
            raise RuntimeError(f"Invoice id={invoice_id} не найден для mark_as_issued")
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _mark_invoice_failed(*, invoice_id: int) -> None:
    """Фиксация неуспешной отправки счёта в T-Bank."""
    from src.db.connection import get_session
    from src.db.repos import invoices as inv_repo

    session = get_session()
    try:
        updated = inv_repo.mark_as_failed(session, invoice_id=invoice_id)
        if updated != 1:
            raise RuntimeError(f"Invoice id={invoice_id} не найден для mark_as_failed")
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main() -> None:
    """Запуск крона: синк + выставление счетов для всех контрагентов с новыми работами."""
    from src.notifications.bitrix_task import create_invoice_task
    from src.notifications.max import send_invoice_notification as send_max_notification
    from src.notifications.telegram import send_invoice_notification_bytes
    from src.sheets.sync import sync_sheets_to_mysql
    from src.sheets.writer import mark_document_in_sheet
    from src.tbank.client import send_invoice

    # 1. Синхронизация Sheets → MySQL
    run_at = datetime.now()
    logger.info("Время запуска крона: %s", run_at.strftime("%Y-%m-%d %H:%M:%S"))
    if DEBUG_FORCE_EMAIL:
        logger.warning("Используется DEBUG_FORCE_EMAIL override: %s", DEBUG_FORCE_EMAIL)
    logger.info("Синхронизация Sheets → MySQL...")
    sync_sheets_to_mysql()

    issued = 0
    errors = []
    counterparties = _get_uninvoiced_counterparties()
    if not counterparties:
        logger.info("Нет контрагентов с невыставленными работами")
        return

    for counterparty_name in counterparties:
        prepared: dict[str, Any] | None = None
        sent_to_tbank = False
        try:
            prepared = _prepare_pending_invoice(counterparty_name, run_at)
            if not prepared:
                continue

            invoice_number = prepared["invoice_number"]
            resp = send_invoice(
                invoice_number=invoice_number,
                due_date=prepared["due_date"],
                invoice_date=prepared["invoice_date"],
                payer_name=prepared["payer_name"],
                payer_inn=prepared["payer_inn"],
                payer_kpp=prepared["payer_kpp"],
                items=prepared["items"],
                email=DEBUG_FORCE_EMAIL or prepared["email"],
                contact_phone=prepared["contact_phone"],
                comment=prepared["comment"],
            )
            sent_to_tbank = True
            tbank_id = resp.get("invoiceId") or resp.get("id")
            invoice_link = (
                resp.get("paymentLink")
                or resp.get("invoiceLink")
                or resp.get("link")
            )
            pdf_url = resp.get("pdfUrl")
            _mark_invoice_issued(
                invoice_id=prepared["invoice_id"],
                tbank_invoice_id=str(tbank_id) if tbank_id else None,
                pdf_url=str(pdf_url) if pdf_url else None,
            )
            try:
                marked_rows = mark_document_in_sheet(
                    sheet_row_hashes=prepared["sheet_row_hashes"],
                    invoice_number=invoice_number,
                    invoice_date=prepared["invoice_date"],
                )
                logger.info(
                    "Sheets: для счёта %s заполнена колонка 'Документ' в %s строках",
                    invoice_number,
                    marked_rows,
                )
            except Exception:
                logger.exception("Ошибка записи в Sheets по счёту %s", invoice_number)

            try:
                send_invoice_notification_bytes(
                    counterparty_name=prepared["counterparty_name"],
                    invoice_number=invoice_number,
                    tbank_invoice_id=str(tbank_id) if tbank_id else None,
                    invoice_link=str(invoice_link) if invoice_link else None,
                )
            except Exception:
                logger.exception("Ошибка Telegram-уведомления по счёту %s", invoice_number)
            bitrix_task_url: str | None = None
            try:
                bitrix_task_url = create_invoice_task(
                    counterparty_name=prepared["counterparty_name"],
                    counterparty_short_name=prepared["counterparty_short_name"],
                    invoice_number=invoice_number,
                    bitrix_company_id=prepared["bitrix_company_id"],
                    tbank_invoice_id=str(tbank_id) if tbank_id else None,
                    invoice_link=str(invoice_link) if invoice_link else None,
                    pdf_url=str(pdf_url) if pdf_url else None,
                    invoice_items=prepared["items"],
                )
            except Exception:
                logger.exception("Ошибка создания задачи Bitrix24 по счёту %s", invoice_number)
            try:
                send_max_notification(
                    counterparty_name=prepared["counterparty_name"],
                    counterparty_short_name=prepared["counterparty_short_name"],
                    invoice_number=invoice_number,
                    invoice_items=prepared["items"],
                    bitrix_task_url=bitrix_task_url,
                )
            except Exception:
                logger.exception("Ошибка MAX-уведомления по счёту %s", invoice_number)
            issued += 1
            logger.info("Счёт %s выставлен для %s", invoice_number, prepared["counterparty_name"])
            time.sleep(TBANK_DELAY_SEC)
        except Exception as e:
            if prepared is not None and not sent_to_tbank:
                try:
                    _mark_invoice_failed(invoice_id=prepared["invoice_id"])
                except Exception:
                    logger.exception(
                        "Не удалось пометить счёт %s как failed_send",
                        prepared["invoice_number"],
                    )
            if prepared is not None and sent_to_tbank:
                logger.error(
                    "Счёт %s отправлен в T-Bank, но локальная фиксация завершилась ошибкой",
                    prepared["invoice_number"],
                )
            errors.append(f"{counterparty_name}: {e}")
            logger.exception("Ошибка при выставлении счёта для %s", counterparty_name)

    logger.info("Крон завершён. Выставлено счетов: %s", issued)
    if errors:
        logger.warning("Ошибки: %s", errors)
        sys.exit(1)


if __name__ == "__main__":
    main()
