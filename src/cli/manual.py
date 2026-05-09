"""
CLI: ручное выставление счёта для одного контрагента.
Запуск: python3 -m src.cli.manual --counterparty "Алтай-Строй"
Или: python3 -m src.cli.manual --counterparty "Алтай-Строй" --ignore-schedule-window
Или: python3 -m src.cli.manual --counterparty "Алтай-Строй" --from-date 01.03.2026
Или: python3 -m src.cli.manual --counterparty "Алтай-Строй" --from-date 01.03.2026 --to-date 31.03.2026
Или: python3 -m src.cli.manual --counterparty "Алтай-Строй" --dry-run
Или: python3 -m src.cli.manual --counterparty "Алтай-Строй" --dry-run --dry-run-bitrix
--counterparty ожидает короткое имя контрагента (short_name).
"""
from __future__ import annotations

import argparse
import calendar
import logging
import os
import sys
import time
from datetime import date, datetime
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
TEN_DAYS_FIRST_CUTOFF_DAY = 10
TEN_DAYS_SECOND_CUTOFF_DAY = 20


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


def _build_bitrix_task_file_attachments(work_files: list[Any]) -> list[dict[str, Any]]:
    """Готовит файлы работ к загрузке в задачу Bitrix24."""
    attachments: list[dict[str, Any]] = []
    for work_file in work_files:
        file_data = getattr(work_file, "file_data", None)
        if isinstance(file_data, memoryview):
            file_data = file_data.tobytes()
        elif isinstance(file_data, bytearray):
            file_data = bytes(file_data)

        attachments.append(
            {
                "work_file_id": getattr(work_file, "id", None),
                "work_id": getattr(work_file, "work_id", None),
                "file_token": getattr(work_file, "file_token", None),
                "file_name": getattr(work_file, "file_name", None),
                "content_type": getattr(work_file, "content_type", None),
                "file_data": file_data if isinstance(file_data, bytes) else b"",
            }
        )
    return attachments


def _format_date_range(date_from: date | None, date_to: date | None) -> str:
    """Человеко-читаемое представление диапазона дат."""
    if date_from and date_to:
        return f"{date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}"
    if date_from:
        return f"с {date_from.strftime('%d.%m.%Y')}"
    if date_to:
        return f"до {date_to.strftime('%d.%m.%Y')}"
    return "без ограничений"


def _log_dry_run_preview(prepared: dict[str, Any], *, dry_run_bitrix: bool = False) -> None:
    """Логирование превью счёта в dry-run режиме."""
    items = prepared.get("items") or []
    target_email = prepared.get("email")
    logger.info(
        "DRY-RUN: контрагент=%s, период=%s, работ=%s, позиций=%s, email=%s",
        prepared.get("counterparty_name"),
        _format_date_range(prepared.get("date_from"), prepared.get("date_to")),
        prepared.get("works_count", 0),
        len(items),
        target_email if target_email else "(не задан)",
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
    bitrix_files_count = len(prepared.get("bitrix_task_files") or [])
    if bitrix_files_count:
        logger.info("DRY-RUN: файлов работ для задачи Bitrix24=%s", bitrix_files_count)
    if prepared.get("comment"):
        logger.info("DRY-RUN: комментарий к счёту:\n%s", prepared["comment"])
    if dry_run_bitrix:
        logger.info(
            "DRY-RUN: запись в БД, отправка в T-Bank, запись в Sheets и чат-уведомления отключены; "
            "будет выполнен только вызов Bitrix24",
        )
    else:
        logger.info("DRY-RUN: изменения в БД и внешние вызовы не выполнялись")


def _build_dry_run_invoice_number() -> str:
    """Формирует тестовый номер счёта для dry-run вызова Bitrix24."""
    return f"DRYRUN-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"


def _is_last_day_of_month(target_date: date) -> bool:
    """Проверка: переданная дата — последний день месяца."""
    _, last_day = calendar.monthrange(target_date.year, target_date.month)
    return target_date.day == last_day


def _is_counterparty_due(invoice_schedule: str | None, run_at: datetime) -> bool:
    """
    Проверка, должен ли контрагент выставляться в текущий запуск.

    Поддерживаемые значения:
    - monthly: последний день месяца, после 19:00
    - 2weeks: последний день месяца или 15 число утром (05:00-08:59)
    - 10days: 10-е, 20-е или последний день месяца, после 19:00
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

    if schedule == "10days":
        return (
            run_at.hour >= MONTHLY_EVENING_START_HOUR
            and (
                _is_last_day_of_month(run_at.date())
                or run_at.day in (TEN_DAYS_FIRST_CUTOFF_DAY, TEN_DAYS_SECOND_CUTOFF_DAY)
            )
        )

    logger.warning(
        "Неизвестный invoice_schedule='%s', используем monthly-правило",
        invoice_schedule,
    )
    return _is_last_day_of_month(run_at.date()) and run_at.hour >= MONTHLY_EVENING_START_HOUR


def _prepare_pending_invoices(
    counterparty: str,
    *,
    ignore_schedule_window: bool = False,
    from_date: date | None = None,
    to_date: date | None = None,
    dry_run: bool = False,
    dry_run_include_issued: bool = False,
) -> list[dict[str, Any]]:
    """Подготовка и фиксация одного или нескольких pending-счётов в БД."""
    from src.db.connection import get_session
    from src.db.repos import bunkers as bunkers_repo
    from src.db.repos import counterparties as cp_repo
    from src.db.repos import invoices as inv_repo
    from src.db.repos import invoice_number as num_repo
    from src.db.repos import works as works_repo
    from src.db.repos import works_files as works_files_repo
    from src.invoice.builder import (
        build_invoice_comment,
        build_custom_payment_purpose,
        build_invoice_items,
        build_invoice_period_text,
        collect_bunker_numbers,
    )
    from src.invoice.splitter import split_works_for_counterparty
    from src.invoice.window import add_business_days, build_invoice_work_date_window_manual, env_bool

    session = get_session()
    try:
        cp = cp_repo.get_by_short_name(session, counterparty, "")
        if not cp:
            logger.error(
                "Контрагент не найден: %s. Проверьте short_name.",
                counterparty,
            )
            return []

        run_at = datetime.now()
        if not ignore_schedule_window and not _is_counterparty_due(cp.invoice_schedule, run_at):
            logger.info(
                "Контрагент %s (schedule=%s): вне окна выставления — пропуск",
                counterparty,
                cp.invoice_schedule,
            )
            return []
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
        report_period_from, report_period_to = build_invoice_work_date_window_manual(
            invoice_schedule=cp.invoice_schedule,
            run_at=run_at,
            strict_period=True,
        )
        if from_date is not None:
            date_from = max(date_from, from_date) if date_from is not None else from_date
        if to_date is not None:
            date_to = min(date_to, to_date) if date_to is not None else to_date
        if from_date is not None or to_date is not None:
            report_period_from = date_from
            report_period_to = date_to
        period_text = build_invoice_period_text(
            report_period_from=report_period_from,
            report_period_to=report_period_to,
        )
        if date_from is not None and date_to is not None and date_from > date_to:
            logger.error(
                "Пустой диапазон дат для %s: from_date=%s, to_date=%s",
                counterparty,
                date_from.strftime("%d.%m.%Y"),
                date_to.strftime("%d.%m.%Y"),
            )
            return []
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

        if dry_run and dry_run_include_issued:
            works = works_repo.get_by_counterparty(
                session,
                counterparty,
                date_from=date_from,
                date_to=date_to,
            )
        elif dry_run:
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
            if dry_run and dry_run_include_issued:
                logger.error(
                    "Нет работ для контрагента %s в заданном диапазоне (включая уже выставленные)",
                    counterparty,
                )
            else:
                logger.error("Нет невыставленных работ для контрагента %s", counterparty)
            return []

        work_groups = split_works_for_counterparty(
            counterparty_short_name=cp.short_name,
            works=works,
        )
        if not work_groups:
            return []

        if dry_run:
            prepared_preview: list[dict[str, Any]] = []
            for group in work_groups:
                items = build_invoice_items(session, group.works, cp.id)
                if not items:
                    logger.error(
                        "Не удалось сформировать позиции счёта (нет цен?) для группы '%s'",
                        group.label or group.key,
                    )
                    return []
                bunker_addresses = bunkers_repo.get_addresses_by_counterparty_and_numbers(
                    session,
                    counterparty_id=cp.id,
                    numbers=collect_bunker_numbers(group.works),
                )
                task_file_attachments = _build_bitrix_task_file_attachments(
                    works_files_repo.get_by_work_ids(session, [w.id for w in group.works])
                )
                prepared_preview.append(
                    {
                        "counterparty_name": cp.name,
                        "counterparty_short_name": cp.short_name,
                        "counterparty_contract": cp.contract or None,
                        "bitrix_company_id": cp.bitrix_company_id,
                        "email": group.email if group.email is not None else (cp.email or None),
                        "items": items,
                        "comment": build_invoice_comment(
                            group.works,
                            contract=cp.contract,
                            report_period_from=report_period_from,
                            report_period_to=report_period_to,
                            bunker_addresses_by_number=bunker_addresses,
                        ),
                        "custom_payment_purpose": build_custom_payment_purpose(
                            invoice_number=None,
                            contract=cp.contract,
                        ),
                        "period_text": period_text,
                        "bitrix_task_files": task_file_attachments,
                        "works_count": len(group.works),
                        "date_from": date_from,
                        "date_to": date_to,
                        "invoice_date": date.today(),
                        "split_group_key": group.key,
                        "split_group_label": group.label,
                    }
                )
            return prepared_preview

        today = date.today()
        due_date = add_business_days(today, 5)
        prepared_invoices: list[dict[str, Any]] = []
        for group in work_groups:
            task_file_attachments = _build_bitrix_task_file_attachments(
                works_files_repo.get_by_work_ids(session, [w.id for w in group.works])
            )
            items = build_invoice_items(session, group.works, cp.id)
            if not items:
                logger.error(
                    "Не удалось сформировать позиции счёта (нет цен?) для группы '%s'",
                    group.label or group.key,
                )
                return []

            inv_num = num_repo.get_next_number(session)
            bunker_addresses = bunkers_repo.get_addresses_by_counterparty_and_numbers(
                session,
                counterparty_id=cp.id,
                numbers=collect_bunker_numbers(group.works),
            )
            comment = build_invoice_comment(
                group.works,
                contract=cp.contract,
                invoice_number=inv_num,
                report_period_from=report_period_from,
                report_period_to=report_period_to,
                bunker_addresses_by_number=bunker_addresses,
            )
            custom_payment_purpose = build_custom_payment_purpose(
                invoice_number=inv_num,
                contract=cp.contract,
            )
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
            claimed = works_repo.update_invoice_id(session, [w.id for w in group.works], inv.id)
            if claimed != len(group.works):
                session.rollback()
                logger.warning(
                    "Работы изменились параллельно (группа '%s', ожидалось %s, обновлено %s), повторите запуск.",
                    group.label or group.key,
                    len(group.works),
                    claimed,
                )
                return []

            prepared_invoices.append(
                {
                    "invoice_id": inv.id,
                    "invoice_number": inv_num,
                    "counterparty_name": cp.name,
                    "counterparty_short_name": cp.short_name,
                    "counterparty_contract": cp.contract or None,
                    "bitrix_company_id": cp.bitrix_company_id,
                    "payer_name": cp.name,
                    "payer_inn": cp.inn,
                    "payer_kpp": cp.kpp or "",
                    "email": group.email if group.email is not None else (cp.email or None),
                    "contact_phone": cp.phone or None,
                    "due_date": due_date,
                    "invoice_date": today,
                    "items": items,
                    "comment": comment,
                    "custom_payment_purpose": custom_payment_purpose,
                    "period_text": period_text,
                    "bitrix_task_files": task_file_attachments,
                    "sheet_row_hashes": [w.sheet_row_hash for w in group.works if w.sheet_row_hash],
                    "split_group_key": group.key,
                    "split_group_label": group.label,
                    "works_count": len(group.works),
                }
            )

        session.commit()
        return prepared_invoices
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
    payment_link: str | None = None,
    recipient_emails_snapshot: str | None = None,
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
            payment_link=payment_link,
            recipient_emails_snapshot=recipient_emails_snapshot,
        )
        if updated != 1:
            raise RuntimeError(f"Invoice id={invoice_id} не найден для mark_as_issued")
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _save_invoice_bitrix_links(
    *,
    invoice_id: int,
    bitrix_task_id: int | None = None,
    bitrix_deal_id: int | None = None,
) -> None:
    """Сохраняет связки счёта с задачей/сделкой Bitrix24."""
    from src.db.connection import get_session
    from src.db.repos import invoices as inv_repo

    if bitrix_task_id is None and bitrix_deal_id is None:
        return

    session = get_session()
    try:
        updated = inv_repo.update_bitrix_links(
            session,
            invoice_id=invoice_id,
            bitrix_task_id=bitrix_task_id,
            bitrix_deal_id=bitrix_deal_id,
        )
        if updated != 1:
            raise RuntimeError(f"Invoice id={invoice_id} не найден для update_bitrix_links")
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
        "--to-date",
        type=_parse_date_arg,
        default=None,
        help="Учитывать работы до даты DD.MM.YYYY включительно (верхняя граница отбора)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Собрать и показать превью счёта без записи в БД и отправки в T-Bank",
    )
    parser.add_argument(
        "--dry-run-bitrix",
        action="store_true",
        help=(
            "Вместе с --dry-run выполнить реальное создание сделки/задачи в Bitrix24 "
            "(остальные внешние вызовы и запись в БД останутся отключены)"
        ),
    )
    parser.add_argument(
        "--dry-run-include-issued",
        action="store_true",
        help=(
            "Вместе с --dry-run учитывать работы из диапазона независимо от того, "
            "привязаны ли они уже к выставленным счетам"
        ),
    )
    args = parser.parse_args()
    if args.dry_run_bitrix and not args.dry_run:
        parser.error("--dry-run-bitrix можно использовать только вместе с --dry-run")
    if args.dry_run_include_issued and not args.dry_run:
        parser.error("--dry-run-include-issued можно использовать только вместе с --dry-run")
    if args.from_date and args.to_date and args.from_date > args.to_date:
        parser.error("--from-date не может быть больше --to-date")

    prepared_invoices = _prepare_pending_invoices(
        args.counterparty,
        ignore_schedule_window=args.ignore_schedule_window,
        from_date=args.from_date,
        to_date=args.to_date,
        dry_run=args.dry_run,
        dry_run_include_issued=args.dry_run_include_issued,
    )
    if not prepared_invoices:
        sys.exit(1)
    if args.dry_run:
        for idx, prepared in enumerate(prepared_invoices, start=1):
            split_label = prepared.get("split_group_label") or prepared.get("split_group_key")
            if split_label:
                logger.info(
                    "DRY-RUN: подготовлен счёт %s/%s (группа: %s)",
                    idx,
                    len(prepared_invoices),
                    split_label,
                )
            _log_dry_run_preview(prepared, dry_run_bitrix=args.dry_run_bitrix)
        if not args.dry_run_bitrix:
            return

        from src.notifications.bitrix_task import create_invoice_task_with_meta

        dry_run_base = _build_dry_run_invoice_number()
        for idx, prepared in enumerate(prepared_invoices, start=1):
            dry_run_invoice_number = (
                f"{dry_run_base}-{idx}" if len(prepared_invoices) > 1 else dry_run_base
            )
            logger.warning(
                "DRY-RUN Bitrix-only: создание сделки/задачи в Bitrix24 с тестовым номером счёта %s",
                dry_run_invoice_number,
            )
            try:
                bitrix_result = create_invoice_task_with_meta(
                    counterparty_name=prepared["counterparty_name"],
                    counterparty_short_name=prepared["counterparty_short_name"],
                    counterparty_contract=prepared.get("counterparty_contract"),
                    invoice_number=dry_run_invoice_number,
                    invoice_date=prepared["invoice_date"],
                    bitrix_company_id=prepared["bitrix_company_id"],
                    invoice_items=prepared["items"],
                    task_file_attachments=prepared.get("bitrix_task_files"),
                    period_text=prepared.get("period_text"),
                    log_deal_request_payload=True,
                )
                bitrix_task_url = bitrix_result.task_url if bitrix_result else None
                if bitrix_task_url:
                    logger.info("DRY-RUN Bitrix-only: задача создана, url=%s", bitrix_task_url)
                else:
                    logger.info(
                        "DRY-RUN Bitrix-only: вызов Bitrix24 выполнен (возможно, без ссылки на задачу); "
                        "проверьте логи выше",
                    )
            except Exception:
                logger.exception("DRY-RUN Bitrix-only: ошибка создания сделки/задачи в Bitrix24")
                sys.exit(1)
        return

    from src.notifications.bitrix_task import create_invoice_task_with_meta
    from src.notifications.invoice_reminder_email import normalize_emails
    from src.notifications.max import send_invoice_notification as send_max_notification
    from src.notifications.telegram import send_invoice_notification_bytes
    from src.sheets.writer import mark_document_in_sheet
    from src.tbank.client import send_invoice

    had_errors = False
    if DEBUG_FORCE_EMAIL:
        logger.warning("Используется DEBUG_FORCE_EMAIL override: %s", DEBUG_FORCE_EMAIL)

    for prepared in prepared_invoices:
        invoice_id = prepared["invoice_id"]
        invoice_number = prepared["invoice_number"]
        counterparty_name = prepared["counterparty_name"]
        sent_to_tbank = False
        target_email = DEBUG_FORCE_EMAIL or prepared["email"]
        recipient_emails_snapshot = ", ".join(normalize_emails(target_email)) or None
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
                custom_payment_purpose=prepared.get("custom_payment_purpose"),
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
                payment_link=str(invoice_link) if invoice_link else None,
                recipient_emails_snapshot=recipient_emails_snapshot,
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
                bitrix_result = create_invoice_task_with_meta(
                    counterparty_name=counterparty_name,
                    counterparty_short_name=prepared["counterparty_short_name"],
                    counterparty_contract=prepared.get("counterparty_contract"),
                    invoice_number=invoice_number,
                    invoice_date=prepared["invoice_date"],
                    bitrix_company_id=prepared["bitrix_company_id"],
                    tbank_invoice_id=str(tbank_id) if tbank_id else None,
                    invoice_link=str(invoice_link) if invoice_link else None,
                    pdf_url=str(pdf_url) if pdf_url else None,
                    invoice_items=prepared["items"],
                    task_file_attachments=prepared.get("bitrix_task_files"),
                    period_text=prepared.get("period_text"),
                )
                if bitrix_result:
                    bitrix_task_url = bitrix_result.task_url
                    try:
                        _save_invoice_bitrix_links(
                            invoice_id=invoice_id,
                            bitrix_task_id=bitrix_result.task_id,
                            bitrix_deal_id=bitrix_result.deal_id,
                        )
                    except Exception:
                        logger.exception(
                            "Ошибка сохранения связки Bitrix24 по счёту %s",
                            invoice_number,
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
            split_label = prepared.get("split_group_label") or prepared.get("split_group_key")
            if split_label:
                logger.info(
                    "Счёт %s успешно выставлен для %s (группа: %s)",
                    invoice_number,
                    counterparty_name,
                    split_label,
                )
            else:
                logger.info("Счёт %s успешно выставлен для %s", invoice_number, counterparty_name)
        except Exception:
            had_errors = True
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

    if had_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
