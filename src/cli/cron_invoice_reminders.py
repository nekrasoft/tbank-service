"""
CLI: крон напоминаний клиентам о неоплаченных счетах.
Запуск: python3 -m src.cli.cron_invoice_reminders
Или:   python3 -m src.cli.cron_invoice_reminders --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
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

_DEFAULT_OFFSETS = (3, 7, 10, 14)
_DEFAULT_LIMIT = 5000
_MONEY_Q = Decimal("0.01")


def _env_int(name: str, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("ENV %s='%s' не число, используем %s", name, raw, default)
        return default
    if min_value is not None and value < min_value:
        value = min_value
    if max_value is not None and value > max_value:
        value = max_value
    return value


def _parse_offsets(raw: str | None) -> list[int]:
    if not raw:
        return list(_DEFAULT_OFFSETS)

    offsets: list[int] = []
    seen: set[int] = set()
    for chunk in raw.replace(";", ",").split(","):
        part = chunk.strip()
        if not part:
            continue
        try:
            value = int(part)
        except ValueError:
            logger.warning("Пропускаем некорректный offset '%s'", part)
            continue
        if value < 0:
            logger.warning("Пропускаем отрицательный offset '%s'", part)
            continue
        if value in seen:
            continue
        seen.add(value)
        offsets.append(value)

    if not offsets:
        return list(_DEFAULT_OFFSETS)
    offsets.sort()
    return offsets


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Отправка email-напоминаний по просроченным неоплаченным счетам",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать, какие напоминания были бы отправлены, без записи в БД и отправки email",
    )
    parser.add_argument(
        "--offsets",
        default=None,
        help="Переопределить шаги напоминаний (через запятую), например: 3,7,10,14",
    )
    return parser.parse_args()


def _invoice_total(invoice: Any) -> Decimal:
    total = Decimal("0.00")
    for item in invoice.items:
        price = Decimal(str(item.price or 0))
        amount = Decimal(str(item.amount or 0))
        total += price * amount
    return total.quantize(_MONEY_Q, rounding=ROUND_HALF_UP)


def _to_emails_snapshot(emails: list[str]) -> str | None:
    normalized = [email.strip() for email in emails if email and email.strip()]
    if not normalized:
        return None
    return ", ".join(normalized)


def _run_reminders(
    *,
    offsets: list[int],
    limit: int,
    dry_run: bool,
) -> dict[str, int]:
    from src.db.connection import get_session
    from src.db.repos import invoice_reminders as reminders_repo
    from src.db.repos import invoices as invoices_repo
    from src.notifications.invoice_reminder_email import (
        normalize_emails,
        send_invoice_payment_reminder,
    )

    today = date.today()
    now_utc = datetime.utcnow().replace(microsecond=0)
    stats = {
        "invoices": 0,
        "due_offsets": 0,
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "would_send": 0,
        "would_skip": 0,
    }

    session = get_session()
    try:
        invoices = invoices_repo.get_unpaid_due_for_reminders(
            session,
            due_on_or_before=today,
            paid_tolerance=_MONEY_Q,
            limit=limit,
        )
        stats["invoices"] = len(invoices)

        if not invoices:
            logger.info("Нет счетов-кандидатов для напоминаний")
            return stats

        sent_offsets_by_invoice = reminders_repo.get_sent_offsets_by_invoice(
            session,
            invoice_ids=[int(invoice.id) for invoice in invoices],
            channel="email",
        )

        for invoice in invoices:
            due_date = invoice.due_date
            if due_date is None:
                continue

            overdue_days = (today - due_date).days
            if overdue_days < 0:
                continue

            already_sent = sent_offsets_by_invoice.get(int(invoice.id), set())
            due_offsets = [
                offset
                for offset in offsets
                if overdue_days >= offset and offset not in already_sent
            ]
            if not due_offsets:
                continue

            # Если к текущей дате накопилось несколько неотправленных шагов,
            # отправляем только один — самый поздний (максимальный offset).
            due_offset = max(due_offsets)

            recipient_source = (
                (invoice.recipient_emails_snapshot or "").strip()
                or (invoice.counterparty.email if invoice.counterparty else None)
            )
            recipients = normalize_emails(recipient_source)
            recipient_snapshot = _to_emails_snapshot(recipients)

            counterparty_name = (
                (invoice.counterparty.name if invoice.counterparty else "")
                or f"контрагент #{invoice.counterparty_id}"
            )
            invoice_number = (invoice.invoice_number or "").strip() or str(invoice.id)
            total_amount = _invoice_total(invoice)
            payment_link = (invoice.payment_link or "").strip() or None

            stats["due_offsets"] += 1

            if not recipients:
                message = "Не задан email получателя для счета"
                if dry_run:
                    stats["would_skip"] += 1
                    logger.info(
                        "DRY-RUN: пропустили бы reminder invoice=%s offset=%s (%s)",
                        invoice_number,
                        due_offset,
                        message,
                    )
                    continue

                reminders_repo.add_attempt(
                    session,
                    invoice_id=int(invoice.id),
                    channel="email",
                    schedule_offset_days=due_offset,
                    overdue_days_at_send=overdue_days,
                    recipient_snapshot=recipient_snapshot,
                    status="skipped",
                    error_text=message,
                    sent_at=None,
                )
                stats["skipped"] += 1
                logger.warning(
                    "Reminder skipped invoice=%s offset=%s: %s",
                    invoice_number,
                    due_offset,
                    message,
                )
                continue

            if dry_run:
                stats["would_send"] += 1
                logger.info(
                    "DRY-RUN: отправили бы reminder invoice=%s offset=%s recipients=%s overdue=%s",
                    invoice_number,
                    due_offset,
                    recipient_snapshot,
                    overdue_days,
                )
                continue

            try:
                send_invoice_payment_reminder(
                    recipients=recipients,
                    invoice_number=invoice_number,
                    counterparty_name=counterparty_name,
                    due_date=due_date,
                    overdue_days=overdue_days,
                    total_amount=total_amount,
                    payment_link=payment_link,
                )
                reminders_repo.add_attempt(
                    session,
                    invoice_id=int(invoice.id),
                    channel="email",
                    schedule_offset_days=due_offset,
                    overdue_days_at_send=overdue_days,
                    recipient_snapshot=recipient_snapshot,
                    status="sent",
                    error_text=None,
                    sent_at=now_utc,
                )
                sent_offsets_by_invoice.setdefault(int(invoice.id), set()).add(due_offset)
                stats["sent"] += 1
            except Exception as e:
                error_text = str(e).strip()[:2000] or "Ошибка отправки email"
                reminders_repo.add_attempt(
                    session,
                    invoice_id=int(invoice.id),
                    channel="email",
                    schedule_offset_days=due_offset,
                    overdue_days_at_send=overdue_days,
                    recipient_snapshot=recipient_snapshot,
                    status="failed",
                    error_text=error_text,
                    sent_at=None,
                )
                stats["failed"] += 1
                logger.exception(
                    "Ошибка отправки reminder invoice=%s offset=%s",
                    invoice_number,
                    due_offset,
                )

        if dry_run:
            session.rollback()
            return stats

        session.commit()
        return stats
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main() -> None:
    args = _parse_args()
    env_offsets = _parse_offsets((os.environ.get("INVOICE_REMINDER_OFFSETS_DAYS") or "").strip())
    offsets = _parse_offsets(args.offsets) if args.offsets is not None else env_offsets
    limit = _env_int(
        "INVOICE_REMINDER_LIMIT",
        _DEFAULT_LIMIT,
        min_value=1,
        max_value=100_000,
    )

    logger.info(
        "Запуск cron_invoice_reminders dry_run=%s offsets=%s limit=%s",
        args.dry_run,
        offsets,
        limit,
    )

    stats = _run_reminders(
        offsets=offsets,
        limit=limit,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info(
            "cron_invoice_reminders DRY-RUN завершен: invoices=%s due_offsets=%s would_send=%s would_skip=%s",
            stats.get("invoices", 0),
            stats.get("due_offsets", 0),
            stats.get("would_send", 0),
            stats.get("would_skip", 0),
        )
        return

    logger.info(
        "cron_invoice_reminders завершен: invoices=%s due_offsets=%s sent=%s failed=%s skipped=%s",
        stats.get("invoices", 0),
        stats.get("due_offsets", 0),
        stats.get("sent", 0),
        stats.get("failed", 0),
        stats.get("skipped", 0),
    )


if __name__ == "__main__":
    main()
