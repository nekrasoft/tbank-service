"""
CLI: ручное выставление счёта для одного контрагента.
Запуск: python3 -m src.cli.manual --counterparty "Алтай-Строй" --note "Ердякова 9"
--counterparty ожидает короткое имя контрагента (short_name).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, timedelta
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


def _prepare_pending_invoice(counterparty: str, note: str) -> dict[str, Any] | None:
    """Подготовка и фиксация pending-счёта в БД до вызова внешнего API."""
    from src.db.connection import get_session
    from src.db.repos import counterparties as cp_repo
    from src.db.repos import invoices as inv_repo
    from src.db.repos import invoice_number as num_repo
    from src.db.repos import works as works_repo
    from src.invoice.builder import build_invoice_items

    session = get_session()
    try:
        cp = cp_repo.get_by_short_name(session, counterparty, note)
        if not cp:
            logger.error(
                "Контрагент не найден: %s (примечание: %s). Проверьте short_name.",
                counterparty,
                note or "(пусто)",
            )
            return None

        works = works_repo.get_uninvoiced_by_counterparty_for_update(
            session, counterparty, note
        )
        if not works:
            logger.error("Нет невыставленных работ для контрагента %s", counterparty)
            return None

        items = build_invoice_items(session, works, cp.id)
        if not items:
            logger.error("Не удалось сформировать позиции счёта (нет цен?)")
            return None

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
            "payer_name": cp.name,
            "payer_inn": cp.inn,
            "payer_kpp": cp.kpp or "",
            "email": cp.email or None,
            "contact_phone": cp.phone or None,
            "due_date": due_date,
            "invoice_date": today,
            "items": items,
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
    parser.add_argument("--note", "-n", default="", help="Примечание (для матчинга)")
    args = parser.parse_args()

    from src.notifications.telegram import send_invoice_notification_bytes
    from src.tbank.client import send_invoice
    prepared = _prepare_pending_invoice(args.counterparty, args.note)
    if not prepared:
        sys.exit(1)

    invoice_id = prepared["invoice_id"]
    invoice_number = prepared["invoice_number"]
    counterparty_name = prepared["counterparty_name"]
    sent_to_tbank = False

    try:
        resp = send_invoice(
            invoice_number=invoice_number,
            due_date=prepared["due_date"],
            invoice_date=prepared["invoice_date"],
            payer_name=prepared["payer_name"],
            payer_inn=prepared["payer_inn"],
            payer_kpp=prepared["payer_kpp"],
            items=prepared["items"],
            email=prepared["email"],
            contact_phone=prepared["contact_phone"],
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
            send_invoice_notification_bytes(
                counterparty_name=counterparty_name,
                invoice_number=invoice_number,
                tbank_invoice_id=str(tbank_id) if tbank_id else None,
                invoice_link=str(invoice_link) if invoice_link else None,
            )
        except Exception:
            logger.exception("Ошибка Telegram-уведомления по счёту %s", invoice_number)
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
