# Выделение номера счёта в транзакции (защита от дублирования)
from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.models import InvoiceNumberSeq


def get_next_number(session: Session) -> str:
    """
    Выделение следующего порядкового номера счёта.
    T-Bank принимает invoiceNumber как строку до 15 цифр (^\\d{1,15}$).
    Формат: порядковый номер, например 1, 2, 3...

    Нумерация сбрасывается ежегодно:
    - ключ последовательности: YYYY;
    - legacy-данные с ключами YYYY-MM учитываются при первом вызове года,
      чтобы корректно продолжить нумерацию без отката.
    """
    year = datetime.utcnow().strftime("%Y")

    # SELECT ... FOR UPDATE для блокировки строки
    result = session.execute(
        select(InvoiceNumberSeq)
        .where(InvoiceNumberSeq.year_month == year)
        .with_for_update()
    )
    row = result.scalars().first()

    if row:
        row.last_number += 1
        next_num = row.last_number
    else:
        # Первая запись за год: учитываем legacy-ключи YYYY-MM как baseline.
        baseline = session.execute(
            select(func.max(InvoiceNumberSeq.last_number)).where(
                or_(
                    InvoiceNumberSeq.year_month == year,
                    InvoiceNumberSeq.year_month.like(f"{year}-%"),
                )
            )
        ).scalar()
        next_num = int(baseline or 0) + 1

        seq = InvoiceNumberSeq(year_month=year, last_number=next_num)
        try:
            # В конкурентном доступе параллельная транзакция может уже создать годовой ключ.
            # В этом случае перечитываем строку с lock и инкрементируем её.
            with session.begin_nested():
                session.add(seq)
                session.flush()
        except IntegrityError:
            row = session.execute(
                select(InvoiceNumberSeq)
                .where(InvoiceNumberSeq.year_month == year)
                .with_for_update()
            ).scalars().first()
            if row is None:
                raise
            row.last_number += 1
            next_num = row.last_number

    # T-Bank: только цифры, до 15 символов
    return str(next_num)
