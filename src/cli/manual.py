"""
CLI: ручное выставление счёта для одного контрагента.
Запуск: python3 -m src.cli.manual --counterparty "Алтай-Строй"
Или: python3 -m src.cli.manual --counterparty "Алтай-Строй" --ignore-schedule-window
Или: python3 -m src.cli.manual --counterparty "Алтай-Строй" --from-date 01.03.2026
Или: python3 -m src.cli.manual --counterparty "Алтай-Строй" --dry-run
--counterparty ожидает короткое имя контрагента (short_name).
"""
from __future__ import annotations

import argparse
import calendar
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
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

# Опциональный override из ENV: если задан, все счета отправляются на этот email.
DEBUG_FORCE_EMAIL = (os.environ.get("DEBUG_FORCE_EMAIL") or "").strip() or None

# Окна выставления для периодичности.
MONTHLY_EVENING_START_HOUR = 19
BIWEEKLY_MIDMONTH_DAY = 15
BIWEEKLY_MORNING_START_HOUR = 5
BIWEEKLY_MORNING_END_HOUR = 9


def _parse_date_arg(value: str) -> date:
    """Парсинг даты аргумента CLI в формате DD.MM.YYYY."""
    try:
        return datetime.strptime(value.strip(), "%d.%m.%Y").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Неверная дата '{value}', ожидается формат DD.MM.YYYY"
        ) from e


def _format_money(value: Decimal) -> str:
    """Форматирование суммы в виде 2 знаков после запятой."""
    return f"{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}"


def _calculate_items_total(items: list[dict[str, Any]]) -> Decimal:
    """Итог по позициям счёта: sum(price * amount)."""
    total = Decimal("0.00")
    for item in items:
        try:
            price = Decimal(str(item.get("price")))
            amount = Decimal(str(item.get("amount")))
        except Exception:
            continue
        total += price * amount
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _format_date_range(date_from: date | None, date_to: date | None) -> str:
    """Человеко-читаемое представление диапазона дат."""
    if date_from and date_to:
        return f"{date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}"
    if date_from:
        return f"с {date_from.strftime('%d.%m.%Y')}"
    if date_to:
        return f"до {date_to.strftime('%d.%m.%Y')}"
    return "без ограничений"


def _log_dry_run_preview(prepared: dict[str, Any]) -> None:
    """Логирование превью счёта в dry-run режиме."""
    items = prepared.get("items") or []
    logger.info(
        "DRY-RUN: контрагент=%s, период=%s, работ=%s, позиций=%s",
        prepared.get("counterparty_name"),
        _format_date_range(prepared.get("date_from"), prepared.get("date_to")),
        prepared.get("works_count", 0),
        len(items),
    )
    for idx, item in enumerate(items, start=1):
        try:
            price = Decimal(str(item.get("price"))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            amount = Decimal(str(item.get("amount")))
        except Exception:
            logger.info("DRY-RUN: [%s] %s (некорректные price/amount)", idx, item.get("name", ""))
            continue
        line_total = (price * amount).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        logger.info(
            "DRY-RUN: [%s] %s | amount=%s %s | price=%s | total=%s",
            idx,
            item.get("name", ""),
            item.get("amount"),
            item.get("unit", "шт"),
            _format_money(price),
            _format_money(line_total),
        )
    logger.info("DRY-RUN: итоговая сумма счёта=%s", _format_money(_calculate_items_total(items)))
    if prepared.get("comment"):
        logger.info("DRY-RUN: комментарий к счёту:\n%s", prepared["comment"])
    logger.info("DRY-RUN: изменения в БД и внешние вызовы не выполнялись")


def _is_last_day_of_month(target_date: date) -> bool:
    """Проверка: переданная дата — последний день месяца."""
    _, last_day = calendar.monthrange(target_date.year, target_date.month)
    return target_date.day == last_day


def _is_counterparty_due(invoice_schedule: str | None, run_at: datetime) -> bool:
    """
    Проверка, должен ли контрагент выставляться в текущий запуск.

    Поддерживаемые значения:
    - monthly: последний день месяца, после 21:00
    - 2weeks: последний день месяца или 15 число утром (08:00-11:59)
    - daily: в любой запуск
    """
    schedule = (invoice_schedule or "monthly").strip().lower()

    if schedule == "daily":
        return True

    if schedule == "monthly":
        return _is_last_day_of_month(run_at.date()) and run_at.hour >= MONTHLY_EVENING_START_HOUR

    if schedule == "2weeks":
        if _is_last_day_of_month(run_at.date()):
            return True
        return (
            run_at.day == BIWEEKLY_MIDMONTH_DAY
            and BIWEEKLY_MORNING_START_HOUR <= run_at.hour < BIWEEKLY_MORNING_END_HOUR
        )

    logger.warning(
        "Неизвестный invoice_schedule='%s', используем monthly-правило",
        invoice_schedule,
    )
    return _is_last_day_of_month(run_at.date()) and run_at.hour >= MONTHLY_EVENING_START_HOUR


def _prepare_pending_invoice(
    counterparty: str,
    *,
    ignore_schedule_window: bool = False,
    from_date: date | None = None,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """Подготовка и фиксация pending-счёта в БД до вызова внешнего API."""
    from src.db.connection import get_session
    from src.db.repos import counterparties as cp_repo
    from src.db.repos import invoices as inv_repo
    from src.db.repos import invoice_number as num_repo
    from src.db.repos import works as works_repo
    from src.invoice.builder import build_invoice_comment, build_invoice_items
    from src.invoice.window import build_invoice_work_date_window_manual, env_bool

    session = get_session()
    try:
        cp = cp_repo.get_by_short_name(session, counterparty, "")
        if not cp:
            logger.error(
                "Контрагент не найден: %s. Проверьте short_name.",
                counterparty,
            )
            return None

        run_at = datetime.now()
        if not ignore_schedule_window and not _is_counterparty_due(cp.invoice_schedule, run_at):
            logger.info(
                "Контрагент %s (schedule=%s): вне окна выставления — пропуск",
                counterparty,
                cp.invoice_schedule,
            )
            return None
        if ignore_schedule_window:
            logger.warning(
                "Контрагент %s: проверка окна выставления отключена флагом --ignore-schedule-window",
                counterparty,
            )

        strict_period = env_bool("INVOICE_STRICT_PERIOD", False)
        warn_out_of_period = env_bool("INVOICE_WARN_OUT_OF_PERIOD", True)
        date_from, date_to = build_invoice_work_date_window_manual(
            invoice_schedule=cp.invoice_schedule,
            run_at=run_at,
            strict_period=strict_period,
        )
        if from_date is not None:
            date_from = max(date_from, from_date) if date_from is not None else from_date
        if strict_period and warn_out_of_period and date_from is not None:
            old_count = works_repo.count_uninvoiced_before_date(
                session,
                counterparty,
                before_date=date_from,
            )
            if old_count > 0:
                logger.warning(
                    "Контрагент %s: %s невыставленных работ до %s вне текущего strict-периода",
                    counterparty,
                    old_count,
                    date_from.strftime("%d.%m.%Y"),
                )

        if dry_run:
            works = works_repo.get_uninvoiced_by_counterparty(
                session,
                counterparty,
                date_from=date_from,
                date_to=date_to,
            )
        else:
            works = works_repo.get_uninvoiced_by_counterparty_for_update(
                session,
                counterparty,
                date_from=date_from,
                date_to=date_to,
            )
        if not works:
            logger.error("Нет невыставленных работ для контрагента %s", counterparty)
            return None

        items = build_invoice_items(session, works, cp.id)
        if not items:
            logger.error("Не удалось сформировать позиции счёта (нет цен?)")
            return None
        comment = build_invoice_comment(works)

        if dry_run:
            return {
                "counterparty_name": cp.name,
                "counterparty_short_name": cp.short_name,
                "items": items,
                "comment": comment,
                "works_count": len(works),
                "date_from": date_from,
                "date_to": date_to,
            }

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
                unit=item.get("unit", "шт"),
                vat=item.get("vat", "None"),
            )
        claimed = works_repo.update_invoice_id(session, [w.id for w in works], inv.id)
        if claimed != len(works):
            session.rollback()
            logger.warning(
                "Работы изменились параллельно (ожидалось %s, обновлено %s), повторите запуск.",
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
    """Ручное выставление счёта."""
    parser = argparse.ArgumentParser(description="Выставить счёт контрагенту")
    parser.add_argument("--counterparty", "-c", required=True, help="Короткое имя контрагента (short_name)")
    parser.add_argument(
        "--ignore-schedule-window",
        action="store_true",
        help="Игнорировать окно выставления по invoice_schedule",
    )
    parser.add_argument(
        "--from-date",
        type=_parse_date_arg,
        default=None,
        help="Учитывать работы начиная с даты DD.MM.YYYY (нижняя граница отбора)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Собрать и показать превью счёта без записи в БД и отправки в T-Bank",
    )
    args = parser.parse_args()

    prepared = _prepare_pending_invoice(
        args.counterparty,
        ignore_schedule_window=args.ignore_schedule_window,
        from_date=args.from_date,
        dry_run=args.dry_run,
    )
    if not prepared:
        sys.exit(1)
    if args.dry_run:
        _log_dry_run_preview(prepared)
        return

    from src.notifications.bitrix_task import create_invoice_task
    from src.notifications.max import send_invoice_notification as send_max_notification
    from src.notifications.telegram import send_invoice_notification_bytes
    from src.sheets.writer import mark_document_in_sheet
    from src.tbank.client import send_invoice

    invoice_id = prepared["invoice_id"]
    invoice_number = prepared["invoice_number"]
    counterparty_name = prepared["counterparty_name"]
    sent_to_tbank = False
    target_email = DEBUG_FORCE_EMAIL or prepared["email"]
    if DEBUG_FORCE_EMAIL:
        logger.warning("Используется DEBUG_FORCE_EMAIL override: %s", DEBUG_FORCE_EMAIL)

    try:
        resp = send_invoice(
            invoice_number=invoice_number,
            due_date=prepared["due_date"],
            invoice_date=prepared["invoice_date"],
            payer_name=prepared["payer_name"],
            payer_inn=prepared["payer_inn"],
            payer_kpp=prepared["payer_kpp"],
            items=prepared["items"],
            email=target_email,
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
            invoice_id=invoice_id,
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
                counterparty_name=counterparty_name,
                invoice_number=invoice_number,
                tbank_invoice_id=str(tbank_id) if tbank_id else None,
                invoice_link=str(invoice_link) if invoice_link else None,
            )
        except Exception:
            logger.exception("Ошибка Telegram-уведомления по счёту %s", invoice_number)
        bitrix_task_url: str | None = None
        try:
            bitrix_task_url = create_invoice_task(
                counterparty_name=counterparty_name,
                counterparty_short_name=prepared["counterparty_short_name"],
                invoice_number=invoice_number,
                invoice_date=prepared["invoice_date"],
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
                counterparty_name=counterparty_name,
                invoice_number=invoice_number,
                bitrix_task_url=bitrix_task_url,
            )
        except Exception:
            logger.exception("Ошибка MAX-уведомления по счёту %s", invoice_number)
        time.sleep(0.3)  # Ограничение 4 req/sec
        logger.info("Счёт %s успешно выставлен для %s", invoice_number, counterparty_name)
    except Exception:
        if not sent_to_tbank:
            try:
                _mark_invoice_failed(invoice_id=invoice_id)
            except Exception:
                logger.exception("Не удалось пометить счёт %s как failed_send", invoice_number)
        else:
            logger.error(
                "Счёт %s отправлен в T-Bank, но локальная фиксация завершилась ошибкой",
                invoice_number,
            )
        logger.exception("Ошибка отправки/фиксации счёта %s", invoice_number)
        sys.exit(1)


if __name__ == "__main__":
    main()
