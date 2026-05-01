"""Репозиторий операций выписки T-Bank и состояния синка."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import json
from typing import Any

from sqlalchemy import or_, select, update
from sqlalchemy.orm import Session

from src.db.models import TBankStatementOperation, TBankStatementSyncState


def _serialize_raw_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


def get_or_create_sync_state(session: Session, *, account_number: str) -> TBankStatementSyncState:
    """Получение/создание состояния синка по расчетному счету."""
    result = session.execute(
        select(TBankStatementSyncState)
        .where(TBankStatementSyncState.account_number == account_number)
    )
    state = result.scalars().first()
    if state:
        return state

    state = TBankStatementSyncState(account_number=account_number)
    session.add(state)
    session.flush()
    session.refresh(state)
    return state


def update_sync_state(
    session: Session,
    *,
    account_number: str,
    last_from: datetime,
    last_to: datetime,
    last_success_at: datetime,
) -> None:
    """Обновление временных границ успешного синка."""
    result = session.execute(
        update(TBankStatementSyncState)
        .where(TBankStatementSyncState.account_number == account_number)
        .values(
            last_from=last_from,
            last_to=last_to,
            last_success_at=last_success_at,
            updated_at=datetime.utcnow(),
        )
    )
    if not (result.rowcount or 0):
        state = TBankStatementSyncState(
            account_number=account_number,
            last_from=last_from,
            last_to=last_to,
            last_success_at=last_success_at,
        )
        session.add(state)


def upsert_operation(
    session: Session,
    *,
    operation_data: dict[str, Any],
    raw_payload: dict[str, Any],
) -> tuple[TBankStatementOperation, bool]:
    """Upsert операции выписки по dedupe_key."""
    dedupe_key = str(operation_data["dedupe_key"])
    result = session.execute(
        select(TBankStatementOperation)
        .where(TBankStatementOperation.dedupe_key == dedupe_key)
    )
    existing = result.scalars().first()
    now = datetime.utcnow()
    raw_json = _serialize_raw_payload(raw_payload)

    if existing:
        for key, value in operation_data.items():
            if key == "dedupe_key":
                continue
            setattr(existing, key, value)
        existing.raw_payload = raw_json
        existing.updated_at = now
        session.flush()
        return existing, False

    row = TBankStatementOperation(
        **operation_data,
        raw_payload=raw_json,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    session.flush()
    session.refresh(row)
    return row, True


def get_unmatched_incoming(session: Session, *, limit: int | None = None) -> list[TBankStatementOperation]:
    """Невсмэтченные входящие операции, пригодные для автопривязки к счетам."""
    stmt = (
        select(TBankStatementOperation)
        .where(TBankStatementOperation.is_incoming.is_(True))
        .where(TBankStatementOperation.matched_invoice_id.is_(None))
        .where(
            or_(
                TBankStatementOperation.operation_status.is_(None),
                TBankStatementOperation.operation_status == "Transaction",
            )
        )
        .order_by(TBankStatementOperation.operation_date.asc(), TBankStatementOperation.id.asc())
    )
    if limit is not None and limit > 0:
        stmt = stmt.limit(limit)

    result = session.execute(stmt)
    return list(result.scalars().all())


def assign_operation_to_invoice(
    session: Session,
    *,
    operation_row_id: int,
    invoice_id: int,
    match_confidence: Decimal,
    match_method: str,
    matched_at: datetime,
) -> int:
    """Привязка операции выписки к счету."""
    result = session.execute(
        update(TBankStatementOperation)
        .where(TBankStatementOperation.id == operation_row_id)
        .values(
            matched_invoice_id=invoice_id,
            match_confidence=match_confidence,
            match_method=match_method,
            matched_at=matched_at,
        )
    )
    return int(result.rowcount or 0)


def get_matched_incoming_for_invoices(
    session: Session,
    *,
    invoice_ids: list[int],
) -> list[TBankStatementOperation]:
    """Все входящие Transaction-операции, уже привязанные к указанным счетам."""
    if not invoice_ids:
        return []

    result = session.execute(
        select(TBankStatementOperation)
        .where(TBankStatementOperation.matched_invoice_id.in_(invoice_ids))
        .where(TBankStatementOperation.is_incoming.is_(True))
        .where(
            or_(
                TBankStatementOperation.operation_status.is_(None),
                TBankStatementOperation.operation_status == "Transaction",
            )
        )
        .order_by(TBankStatementOperation.operation_date.asc(), TBankStatementOperation.id.asc())
    )
    return list(result.scalars().all())


def get_unsynced_cashless_expenses(
    session: Session,
    *,
    limit: int | None = None,
    operation_date_from: datetime | None = None,
    include_synced: bool = False,
) -> list[TBankStatementOperation]:
    """Исходящие Transaction-операции для выгрузки в лист безналичных расходов."""
    stmt = (
        select(TBankStatementOperation)
        .where(TBankStatementOperation.is_incoming.is_(False))
        .where(TBankStatementOperation.operation_date.is_not(None))
        .where(TBankStatementOperation.operation_amount > Decimal("0.00"))
        .where(
            or_(
                TBankStatementOperation.operation_status.is_(None),
                TBankStatementOperation.operation_status == "Transaction",
            )
        )
        .order_by(TBankStatementOperation.operation_date.asc(), TBankStatementOperation.id.asc())
    )
    if not include_synced:
        stmt = stmt.where(TBankStatementOperation.cashless_expense_sheet_synced_at.is_(None))
    if operation_date_from is not None:
        stmt = stmt.where(TBankStatementOperation.operation_date >= operation_date_from)
    if limit is not None and limit > 0:
        stmt = stmt.limit(limit)

    result = session.execute(stmt)
    return list(result.scalars().all())


def mark_cashless_expenses_sheet_synced(
    session: Session,
    *,
    operation_ids: list[int],
    synced_at: datetime,
    update_existing: bool = False,
) -> int:
    """Отметить исходящие операции как обработанные для листа безналичных расходов."""
    ids = [int(operation_id) for operation_id in operation_ids if operation_id]
    if not ids:
        return 0

    stmt = update(TBankStatementOperation).where(TBankStatementOperation.id.in_(ids))
    if not update_existing:
        stmt = stmt.where(TBankStatementOperation.cashless_expense_sheet_synced_at.is_(None))

    result = session.execute(
        stmt.values(
            cashless_expense_sheet_synced_at=synced_at,
            updated_at=datetime.utcnow(),
        )
    )
    return int(result.rowcount or 0)


def get_unsynced_cashless_incomes(
    session: Session,
    *,
    limit: int | None = None,
    operation_date_from: datetime | None = None,
    include_synced: bool = False,
) -> list[TBankStatementOperation]:
    """Входящие Transaction-операции для выгрузки в лист безналичных доходов."""
    stmt = (
        select(TBankStatementOperation)
        .where(TBankStatementOperation.is_incoming.is_(True))
        .where(TBankStatementOperation.operation_date.is_not(None))
        .where(TBankStatementOperation.operation_amount > Decimal("0.00"))
        .where(
            or_(
                TBankStatementOperation.operation_status.is_(None),
                TBankStatementOperation.operation_status == "Transaction",
            )
        )
        .order_by(TBankStatementOperation.operation_date.asc(), TBankStatementOperation.id.asc())
    )
    if not include_synced:
        stmt = stmt.where(TBankStatementOperation.cashless_income_sheet_synced_at.is_(None))
    if operation_date_from is not None:
        stmt = stmt.where(TBankStatementOperation.operation_date >= operation_date_from)
    if limit is not None and limit > 0:
        stmt = stmt.limit(limit)

    result = session.execute(stmt)
    return list(result.scalars().all())


def mark_cashless_incomes_sheet_synced(
    session: Session,
    *,
    operation_ids: list[int],
    synced_at: datetime,
    update_existing: bool = False,
) -> int:
    """Отметить входящие операции как обработанные для листа безналичных доходов."""
    ids = [int(operation_id) for operation_id in operation_ids if operation_id]
    if not ids:
        return 0

    stmt = update(TBankStatementOperation).where(TBankStatementOperation.id.in_(ids))
    if not update_existing:
        stmt = stmt.where(TBankStatementOperation.cashless_income_sheet_synced_at.is_(None))

    result = session.execute(
        stmt.values(
            cashless_income_sheet_synced_at=synced_at,
            updated_at=datetime.utcnow(),
        )
    )
    return int(result.rowcount or 0)
