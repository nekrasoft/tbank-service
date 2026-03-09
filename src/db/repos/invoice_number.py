# Выделение номера счёта в транзакции (защита от дублирования)
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import InvoiceNumberSeq


def get_next_number(session: Session) -> str:
    """
    Выделение следующего порядкового номера счёта.
    T-Bank принимает invoiceNumber как строку до 15 цифр (^\\d{1,15}$).
    Формат: порядковый номер, например 1, 2, 3...
    """
    year_month = datetime.utcnow().strftime("%Y-%m")
    # SELECT ... FOR UPDATE для блокировки строки
    result = session.execute(
        select(InvoiceNumberSeq)
        .where(InvoiceNumberSeq.year_month == year_month)
        .with_for_update()
    )
    row = result.scalars().first()

    if row:
        row.last_number += 1
        next_num = row.last_number
    else:
        # Первая запись за этот месяц
        seq = InvoiceNumberSeq(year_month=year_month, last_number=1)
        session.add(seq)
        session.flush()
        next_num = 1

    # T-Bank: только цифры, до 15 символов
    return str(next_num)
