"""
CLI: ручное выставление счёта для одного контрагента.
Запуск: python -m src.cli.manual --counterparty "Алтай-Строй" --note "Ердякова 9"
--counterparty ожидает короткое имя контрагента (short_name).
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
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


def main() -> None:
    """Ручное выставление счёта."""
    parser = argparse.ArgumentParser(description="Выставить счёт контрагенту")
    parser.add_argument("--counterparty", "-c", required=True, help="Короткое имя контрагента (short_name)")
    parser.add_argument("--note", "-n", default="", help="Примечание (для матчинга)")
    args = parser.parse_args()

    from src.db.connection import get_session
    from src.db.repos import counterparties as cp_repo
    from src.db.repos import invoices as inv_repo
    from src.db.repos import invoice_number as num_repo
    from src.db.repos import works as works_repo
    from src.invoice.builder import build_invoice_items
    from src.invoice.act_generator import generate_act_pdf
    from src.notifications.telegram import send_invoice_notification_bytes
    from src.tbank.client import send_invoice
    import time

    session = get_session()
    try:
        # Поиск контрагента по короткому имени
        cp = cp_repo.get_by_short_name(session, args.counterparty, args.note)
        if not cp:
            logger.error(
                "Контрагент не найден: %s (примечание: %s). "
                "Проверьте short_name в таблице counterparties.",
                args.counterparty,
                args.note or "(пусто)",
            )
            sys.exit(1)

        # Работы без счёта
        works = works_repo.get_uninvoiced_by_counterparty(
            session, args.counterparty, args.note
        )
        if not works:
            logger.error("Нет невыставленных работ для контрагента %s", args.counterparty)
            sys.exit(1)

        # Сборка позиций
        items = build_invoice_items(session, works, cp.id)
        if not items:
            logger.error("Не удалось сформировать позиции счёта (нет цен?)")
            sys.exit(1)

        # Выделение номера
        inv_num = num_repo.get_next_number(session)
        today = date.today()
        due_date = today + timedelta(days=14)

        # T-Bank API
        # resp = send_invoice(
        #     invoice_number=inv_num,
        #     due_date=due_date,
        #     invoice_date=today,
        #     payer_name=cp.name,
        #     payer_inn=cp.inn,
        #     payer_kpp=cp.kpp or "",
        #     items=items,
        #     email=cp.email,
        #     contact_phone=cp.phone if cp.phone else None,
        # )
        tbank_id = "1234567890" #resp.get("invoiceId") or resp.get("id")
        time.sleep(0.3)  # Ограничение 4 req/sec

        # Сохранение в БД
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
        works_repo.update_invoice_id(
            session, [w.id for w in works], inv.id
        )
        session.commit()

        # Акт и Telegram
        act_pdf = generate_act_pdf(
            counterparty_name=cp.name,
            invoice_number=inv_num,
            invoice_date=today,
            items=items,
        )
        send_invoice_notification_bytes(
            counterparty_name=cp.name,
            invoice_number=inv_num,
            tbank_invoice_id=str(tbank_id) if tbank_id else None,
            act_pdf_bytes=act_pdf,
        )

        logger.info("Счёт %s успешно выставлен для %s", inv_num, cp.name)
    except Exception as e:
        session.rollback()
        logger.error("Ошибка: %s", e)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
