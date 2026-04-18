"""
CLI: отдельный крон синка выписки T-Bank и автозачета оплат по счетам.
Запуск: python3 -m src.cli.cron_payments
Или:   python3 -m src.cli.cron_payments --dry-run
Или:   python3 -m src.cli.cron_payments --dry-run --dry-run-bitrix
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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
_MONEY_Q = Decimal("0.01")
_AMOUNT_TOLERANCE = Decimal("0.01")

_DEFAULT_INITIAL_LOOKBACK_DAYS = 90
_DEFAULT_OVERLAP_MINUTES = 180
_DEFAULT_PAGE_LIMIT = 200
_DEFAULT_UNMATCHED_LIMIT = 5000

_INVOICE_HINT_RE = re.compile(
    r"(?:сч[её]т(?:а|у|ом|ов)?|сч\.?|с/ф|сф|invoice|inv)"
    r"\s*(?:[:\-]\s*)?(?:(?:№|#|no|n[оo])\s*)?(\d{1,15})\b",
    re.IGNORECASE,
)
_NON_ALNUM_RE = re.compile(r"[^0-9a-zа-яё]+", re.IGNORECASE)


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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Синк выписки T-Bank и автозачет оплат по invoices",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Выполнить матчинг в режиме предпросмотра: без синка выписки, "
            "без сохранения изменений в БД и без внешних вызовов"
        ),
    )
    parser.add_argument(
        "--dry-run-bitrix",
        action="store_true",
        help=(
            "Вместе с --dry-run выполнить только Bitrix24-обновления "
            "для счетов, которые в этом прогоне стали бы paid"
        ),
    )
    args = parser.parse_args()
    if args.dry_run_bitrix and not args.dry_run:
        parser.error("--dry-run-bitrix можно использовать только вместе с --dry-run")
    return args



def _get_account_numbers() -> list[str]:
    raw = (os.environ.get("TBANK_STATEMENT_ACCOUNT_NUMBERS") or "").strip()
    if not raw:
        raise ValueError("Задайте TBANK_STATEMENT_ACCOUNT_NUMBERS в .env")

    parts = re.split(r"[,;\s]+", raw)
    accounts: list[str] = []
    seen: set[str] = set()
    for part in parts:
        acc = part.strip()
        if not acc:
            continue
        if acc in seen:
            continue
        seen.add(acc)
        accounts.append(acc)

    if not accounts:
        raise ValueError("TBANK_STATEMENT_ACCOUNT_NUMBERS пустой")
    return accounts



def _to_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)



def _to_utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)



def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)



def _parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value.quantize(_MONEY_Q, rounding=ROUND_HALF_UP)

    raw = str(value).strip()
    if not raw:
        return None
    cleaned = raw.replace("\u00a0", "").replace(" ", "").replace(",", ".")
    try:
        return Decimal(cleaned).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None



def _operation_amount_from_raw(op: dict[str, Any]) -> Decimal:
    amount = (
        _parse_decimal(op.get("operationAmount"))
        or _parse_decimal(op.get("accountAmount"))
        or _parse_decimal(op.get("rubleAmount"))
        or _parse_decimal(op.get("credit"))
        or _parse_decimal(op.get("debit"))
    )
    if amount is None or amount <= 0:
        return Decimal("0.00")
    return amount



def _operation_amount_from_row(op_row: Any) -> Decimal:
    amount = op_row.operation_amount or op_row.account_amount or op_row.ruble_amount
    if amount is None:
        return Decimal("0.00")
    return Decimal(str(amount)).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)



def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return _NON_ALNUM_RE.sub("", value.lower())



def _extract_invoice_numbers(*texts: str | None) -> set[str]:
    joined = "\n".join(part for part in texts if part)
    if not joined:
        return set()
    return {m.group(1).lstrip("0") or "0" for m in _INVOICE_HINT_RE.finditer(joined)}



def _make_dedupe_key(
    *,
    account_number: str,
    operation_id: str | None,
    operation_date: datetime | None,
    operation_amount: Decimal | None,
    document_number: str | None,
    payer_inn: str | None,
    pay_purpose: str | None,
) -> tuple[str, str]:
    op_id = (operation_id or "").strip()
    if op_id:
        return op_id, f"{account_number}:{op_id}"

    payload = "|".join(
        [
            account_number,
            operation_date.isoformat() if operation_date else "",
            str(operation_amount or ""),
            (document_number or "").strip(),
            (payer_inn or "").strip(),
            (pay_purpose or "").strip()[:256],
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    fallback_id = f"fallback-{digest[:24]}"
    return fallback_id, f"{account_number}:{fallback_id}"



def _is_incoming_operation(op: dict[str, Any], *, amount: Decimal) -> bool:
    operation_type = str(op.get("typeOfOperation") or "").strip().lower()
    if operation_type == "credit":
        return amount > 0
    if operation_type == "debit":
        return False

    credit = _parse_decimal(op.get("credit")) or Decimal("0.00")
    debit = _parse_decimal(op.get("debit")) or Decimal("0.00")
    if credit > 0 and debit <= 0:
        return True
    return False



def _normalize_operation(raw_op: dict[str, Any], *, default_account_number: str) -> dict[str, Any]:
    payer = raw_op.get("payer") if isinstance(raw_op.get("payer"), dict) else {}
    receiver = raw_op.get("receiver") if isinstance(raw_op.get("receiver"), dict) else {}
    counterparty = raw_op.get("counterParty") if isinstance(raw_op.get("counterParty"), dict) else {}

    account_number = str(raw_op.get("accountNumber") or default_account_number).strip()
    operation_date = _parse_iso_datetime(raw_op.get("operationDate"))
    operation_amount = _operation_amount_from_raw(raw_op)

    operation_id, dedupe_key = _make_dedupe_key(
        account_number=account_number,
        operation_id=str(raw_op.get("operationId") or "").strip() or None,
        operation_date=operation_date,
        operation_amount=operation_amount,
        document_number=str(raw_op.get("documentNumber") or "").strip() or None,
        payer_inn=str(payer.get("inn") or "").strip() or None,
        pay_purpose=str(raw_op.get("payPurpose") or "").strip() or None,
    )

    return {
        "account_number": account_number,
        "dedupe_key": dedupe_key,
        "operation_id": operation_id,
        "operation_status": str(raw_op.get("operationStatus") or "").strip() or None,
        "type_of_operation": str(raw_op.get("typeOfOperation") or "").strip() or None,
        "category": str(raw_op.get("category") or "").strip() or None,
        "operation_date": operation_date,
        "trxn_post_date": _parse_iso_datetime(raw_op.get("trxnPostDate")),
        "authorization_date": _parse_iso_datetime(raw_op.get("authorizationDate")),
        "draw_date": _parse_iso_datetime(raw_op.get("drawDate")),
        "charge_date": _parse_iso_datetime(raw_op.get("chargeDate")),
        "doc_date": _parse_iso_datetime(raw_op.get("docDate")),
        "document_number": str(raw_op.get("documentNumber") or "").strip() or None,
        "operation_amount": operation_amount,
        "account_amount": _parse_decimal(raw_op.get("accountAmount")),
        "ruble_amount": _parse_decimal(raw_op.get("rubleAmount")),
        "description": str(raw_op.get("description") or "").strip() or None,
        "pay_purpose": str(raw_op.get("payPurpose") or "").strip() or None,
        "payer_name": str(payer.get("name") or "").strip() or None,
        "payer_inn": str(payer.get("inn") or "").strip() or None,
        "payer_account": str(payer.get("acct") or "").strip() or None,
        "receiver_name": str(receiver.get("name") or "").strip() or None,
        "receiver_inn": str(receiver.get("inn") or "").strip() or None,
        "receiver_account": str(receiver.get("acct") or "").strip() or None,
        "counterparty_name": str(counterparty.get("name") or "").strip() or None,
        "counterparty_inn": str(counterparty.get("inn") or "").strip() or None,
        "counterparty_account": str(counterparty.get("account") or "").strip() or None,
        "is_incoming": _is_incoming_operation(raw_op, amount=operation_amount),
    }



def _invoice_total(invoice: Any) -> Decimal:
    total = Decimal("0.00")
    for item in invoice.items:
        price = Decimal(str(item.price or 0))
        amount = Decimal(str(item.amount or 0))
        total += price * amount
    return total.quantize(_MONEY_Q, rounding=ROUND_HALF_UP)



def _build_invoice_state(open_invoices: list[Any], matched_incoming: list[Any]) -> dict[int, dict[str, Any]]:
    invoice_by_id: dict[int, Any] = {int(invoice.id): invoice for invoice in open_invoices}
    paid_by_invoice: dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
    for op in matched_incoming:
        invoice_id = int(op.matched_invoice_id)
        invoice = invoice_by_id.get(invoice_id)
        payment_dt = _payment_datetime(op)
        if invoice and not _is_payment_after_invoice_issue(
            payment_dt=payment_dt,
            invoice_issued_at=invoice.issued_at,
        ):
            continue
        paid_by_invoice[invoice_id] += _operation_amount_from_row(op)

    state: dict[int, dict[str, Any]] = {}
    for invoice in open_invoices:
        paid = paid_by_invoice.get(invoice.id, Decimal("0.00")).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
        state[invoice.id] = {
            "invoice": invoice,
            "total": _invoice_total(invoice),
            "paid": paid,
        }
    return state



def _remaining_amount(entry: dict[str, Any]) -> Decimal:
    remaining = (entry["total"] - entry["paid"]).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
    if remaining < Decimal("0.00"):
        return Decimal("0.00")
    return remaining



def _amount_equal(left: Decimal, right: Decimal, *, tolerance: Decimal = _AMOUNT_TOLERANCE) -> bool:
    return abs(left - right) <= tolerance



def _payer_name_matches_counterparty(payer_name: str | None, *, full_name: str | None, short_name: str | None) -> bool:
    payer_norm = _normalize_text(payer_name)
    if not payer_norm:
        return False

    for candidate in (full_name, short_name):
        norm = _normalize_text(candidate)
        if len(norm) < 4:
            continue
        if norm in payer_norm or payer_norm in norm:
            return True
    return False



def _choose_unique_best(scored_candidates: list[tuple[Decimal, int, str]]) -> tuple[int, Decimal, str] | None:
    if not scored_candidates:
        return None
    scored_candidates.sort(key=lambda x: (x[0], -x[1]), reverse=True)
    best = scored_candidates[0]
    if len(scored_candidates) == 1:
        return best[1], best[0], best[2]

    second = scored_candidates[1]
    if best[0] - second[0] < Decimal("0.05"):
        return None
    return best[1], best[0], best[2]



def _match_operation_to_invoice(
    operation: Any,
    *,
    invoice_state: dict[int, dict[str, Any]],
    invoices_by_number: dict[str, list[int]],
    invoices_by_inn: dict[str, list[int]],
    open_invoice_ids: list[int],
) -> dict[str, Any] | None:
    amount = _operation_amount_from_row(operation)
    if amount <= 0:
        return None

    payment_dt = _payment_datetime(operation)
    payer_inn = (operation.payer_inn or operation.counterparty_inn or "").strip()
    payer_name = operation.payer_name or operation.counterparty_name

    # 1) Наиболее надежно: номер счета явно встречается в назначении/описании.
    mentioned_numbers = _extract_invoice_numbers(operation.pay_purpose, operation.description)
    if mentioned_numbers:
        scored: list[tuple[Decimal, int, str]] = []
        for number in mentioned_numbers:
            for invoice_id in invoices_by_number.get(number, []):
                entry = invoice_state.get(invoice_id)
                if not entry:
                    continue
                remaining = _remaining_amount(entry)
                if remaining <= 0:
                    continue
                invoice = entry["invoice"]
                if not _is_payment_after_invoice_issue(
                    payment_dt=payment_dt,
                    invoice_issued_at=invoice.issued_at,
                ):
                    continue
                score = Decimal("1.00")
                method = "invoice_number"
                if payer_inn and invoice.counterparty and (invoice.counterparty.inn or "").strip() == payer_inn:
                    score += Decimal("0.20")
                if _amount_equal(amount, remaining) or _amount_equal(amount, entry["total"]):
                    score += Decimal("0.20")
                if _payer_name_matches_counterparty(
                    payer_name,
                    full_name=invoice.counterparty.name if invoice.counterparty else None,
                    short_name=invoice.counterparty.short_name if invoice.counterparty else None,
                ):
                    score += Decimal("0.10")
                scored.append((score, invoice_id, method))

        selected = _choose_unique_best(scored)
        if selected:
            invoice_id, confidence, method = selected
            return {
                "invoice_id": invoice_id,
                "confidence": confidence,
                "method": method,
                "amount": amount,
            }

    # 2) Надежный fallback: уникальный ИНН + сумма в пределах остатка.
    if payer_inn:
        candidates = []
        for invoice_id in invoices_by_inn.get(payer_inn, []):
            entry = invoice_state.get(invoice_id)
            if not entry:
                continue
            invoice = entry["invoice"]
            if not _is_payment_after_invoice_issue(
                payment_dt=payment_dt,
                invoice_issued_at=invoice.issued_at,
            ):
                continue
            remaining = _remaining_amount(entry)
            if remaining <= 0:
                continue
            if amount - remaining > _AMOUNT_TOLERANCE:
                continue
            candidates.append(invoice_id)

        if len(candidates) == 1:
            return {
                "invoice_id": candidates[0],
                "confidence": Decimal("0.88"),
                "method": "payer_inn_amount",
                "amount": amount,
            }

    # 3) Осторожный fallback: уникальное совпадение по имени + сумме.
    scored_name: list[tuple[Decimal, int, str]] = []
    for invoice_id in open_invoice_ids:
        entry = invoice_state.get(invoice_id)
        if not entry:
            continue
        remaining = _remaining_amount(entry)
        if remaining <= 0:
            continue
        if amount - remaining > _AMOUNT_TOLERANCE:
            continue

        invoice = entry["invoice"]
        if not _is_payment_after_invoice_issue(
            payment_dt=payment_dt,
            invoice_issued_at=invoice.issued_at,
        ):
            continue
        if not _payer_name_matches_counterparty(
            payer_name,
            full_name=invoice.counterparty.name if invoice.counterparty else None,
            short_name=invoice.counterparty.short_name if invoice.counterparty else None,
        ):
            continue
        score = Decimal("0.74")
        if _amount_equal(amount, remaining):
            score += Decimal("0.06")
        scored_name.append((score, invoice_id, "payer_name_amount"))

    selected = _choose_unique_best(scored_name)
    if selected:
        invoice_id, confidence, method = selected
        return {
            "invoice_id": invoice_id,
            "confidence": confidence,
            "method": method,
            "amount": amount,
        }

    return None



def _payment_datetime(op_row: Any) -> datetime | None:
    return op_row.charge_date or op_row.draw_date or op_row.operation_date


def _is_payment_after_invoice_issue(
    *,
    payment_dt: datetime | None,
    invoice_issued_at: datetime | None,
) -> bool:
    """
    Платеж учитываем по счету только если он не раньше issued_at.

    Если одна из дат отсутствует, считаем операцию допустимой.
    """
    if payment_dt is None or invoice_issued_at is None:
        return True

    payment_utc = _to_utc_aware(payment_dt)
    issued_utc = _to_utc_aware(invoice_issued_at)
    if payment_utc is None or issued_utc is None:
        return True
    return payment_utc >= issued_utc



def _recalculate_payment_state(
    session: Any,
    *,
    invoice_ids: set[int],
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    from src.db.repos import invoices as inv_repo
    from src.db.repos import statement_operations as st_ops_repo

    if not invoice_ids:
        return {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []

    invoice_list = inv_repo.get_for_payment_recalc(session, sorted(invoice_ids))
    matched = st_ops_repo.get_matched_incoming_for_invoices(session, invoice_ids=sorted(invoice_ids))

    invoice_by_id: dict[int, Any] = {int(invoice.id): invoice for invoice in invoice_list}
    paid_sum: dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
    paid_at: dict[int, datetime | None] = {}

    for op in matched:
        inv_id = int(op.matched_invoice_id)
        payment_dt = _payment_datetime(op)
        invoice = invoice_by_id.get(inv_id)
        if invoice and not _is_payment_after_invoice_issue(
            payment_dt=payment_dt,
            invoice_issued_at=invoice.issued_at,
        ):
            continue

        paid_sum[inv_id] += _operation_amount_from_row(op)
        if payment_dt is None:
            continue
        current = paid_at.get(inv_id)
        if current is None or payment_dt > current:
            paid_at[inv_id] = payment_dt

    stats = {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}
    newly_paid: list[dict[str, Any]] = []
    for invoice in invoice_list:
        total = _invoice_total(invoice)
        paid = paid_sum.get(invoice.id, Decimal("0.00")).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
        prev_status = str(invoice.status or "").strip()

        if paid <= _AMOUNT_TOLERANCE:
            new_status = "issued"
            new_paid_at = None
            stats["issued"] += 1
        elif paid + _AMOUNT_TOLERANCE < total:
            new_status = "partially_paid"
            new_paid_at = None
            stats["partially_paid"] += 1
        else:
            new_status = "paid"
            new_paid_at = paid_at.get(invoice.id) or invoice.paid_at or datetime.utcnow()
            stats["paid"] += 1

        needs_update = (
            invoice.status != new_status
            or Decimal(str(invoice.paid_amount or 0)).quantize(_MONEY_Q, rounding=ROUND_HALF_UP) != paid
            or invoice.paid_at != new_paid_at
        )
        if not needs_update:
            continue

        updated = inv_repo.update_payment_state(
            session,
            invoice_id=invoice.id,
            status=new_status,
            paid_amount=paid,
            paid_at=new_paid_at,
        )
        if updated:
            stats["updated"] += 1
            if new_status == "paid" and prev_status != "paid":
                newly_paid.append(
                    {
                        "invoice_id": invoice.id,
                        "invoice_number": invoice.invoice_number,
                        "bitrix_task_id": invoice.bitrix_task_id,
                        "bitrix_deal_id": invoice.bitrix_deal_id,
                    }
                )

    return stats, newly_paid


def _sync_paid_invoices_to_bitrix(newly_paid: list[dict[str, Any]]) -> None:
    """Для новых paid-счетов пишет комментарий в задачу и закрывает сделку в Bitrix24."""
    if not newly_paid:
        return

    try:
        from src.notifications.bitrix_task import mark_invoice_paid_in_bitrix
    except Exception:
        logger.exception("Не удалось загрузить Bitrix24 payment-notifier")
        return

    for item in newly_paid:
        invoice_number = str(item.get("invoice_number") or "").strip() or str(item.get("invoice_id"))
        try:
            mark_invoice_paid_in_bitrix(
                invoice_number=invoice_number,
                bitrix_task_id=item.get("bitrix_task_id"),
                bitrix_deal_id=item.get("bitrix_deal_id"),
            )
        except Exception:
            logger.exception(
                "Ошибка синхронизации оплаты счёта %s в Bitrix24",
                invoice_number,
            )


def _log_dry_run_paid_preview(newly_paid: list[dict[str, Any]]) -> None:
    if not newly_paid:
        logger.info("DRY-RUN: нет счетов, которые перешли бы в paid")
        return

    logger.info("DRY-RUN: в paid перешли бы %s счет(ов)", len(newly_paid))
    for item in newly_paid:
        logger.info(
            "DRY-RUN: paid invoice=%s (id=%s, task_id=%s, deal_id=%s)",
            item.get("invoice_number"),
            item.get("invoice_id"),
            item.get("bitrix_task_id"),
            item.get("bitrix_deal_id"),
        )


def _sync_statement_for_account(
    *,
    account_number: str,
    initial_lookback_days: int,
    overlap_minutes: int,
    page_limit: int,
) -> tuple[int, int]:
    from src.db.connection import get_session
    from src.db.repos import statement_operations as st_ops_repo
    from src.tbank.client import get_statement

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    session = get_session()
    try:
        state = st_ops_repo.get_or_create_sync_state(session, account_number=account_number)
        session.flush()

        if state.last_success_at:
            from_utc = _to_utc_aware(state.last_success_at) - timedelta(minutes=overlap_minutes)
        else:
            from_utc = now_utc - timedelta(days=initial_lookback_days)
        to_utc = now_utc
        if from_utc >= to_utc:
            from_utc = to_utc - timedelta(minutes=max(5, overlap_minutes))

        logger.info(
            "Синк выписки account=%s from=%s to=%s",
            account_number,
            from_utc.isoformat(),
            to_utc.isoformat(),
        )

        cursor: str | None = None
        pages = 0
        fetched = 0
        created = 0
        while True:
            data = get_statement(
                account_number=account_number,
                from_dt=from_utc,
                to_dt=to_utc,
                cursor=cursor,
                limit=page_limit,
                operation_status="Transaction",
                with_balances=False,
            )
            operations = data.get("operations") or []
            pages += 1
            logger.info(
                "Выписка account=%s page=%s operations=%s cursor=%s",
                account_number,
                pages,
                len(operations),
                cursor if cursor else "<start>",
            )

            for raw in operations:
                if not isinstance(raw, dict):
                    continue
                op_data = _normalize_operation(raw, default_account_number=account_number)
                _, is_created = st_ops_repo.upsert_operation(
                    session,
                    operation_data=op_data,
                    raw_payload=raw,
                )
                fetched += 1
                if is_created:
                    created += 1

            session.commit()

            next_cursor = str(data.get("nextCursor") or "").strip() or None
            if not next_cursor:
                break
            if next_cursor == cursor:
                logger.warning(
                    "Получен одинаковый nextCursor account=%s (%s), прекращаем пагинацию",
                    account_number,
                    next_cursor,
                )
                break
            cursor = next_cursor
            time.sleep(TBANK_DELAY_SEC)

        st_ops_repo.update_sync_state(
            session,
            account_number=account_number,
            last_from=_to_utc_naive(from_utc),
            last_to=_to_utc_naive(to_utc),
            last_success_at=_to_utc_naive(to_utc),
        )
        session.commit()
        return fetched, created
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()



def _run_matching(
    unmatched_limit: int,
    *,
    dry_run: bool = False,
) -> tuple[int, dict[str, int], list[dict[str, Any]]]:
    from src.db.connection import get_session
    from src.db.repos import invoices as inv_repo
    from src.db.repos import statement_operations as st_ops_repo

    session = get_session()
    try:
        unmatched = st_ops_repo.get_unmatched_incoming(session, limit=unmatched_limit)
        if not unmatched:
            logger.info("Нет новых неприлинкованных входящих операций")
            return 0, {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []

        open_invoices = inv_repo.get_open_for_payment_matching(session)
        if not open_invoices:
            logger.info("Нет открытых счетов для автозачета оплат")
            return 0, {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []

        matched_incoming = st_ops_repo.get_matched_incoming_for_invoices(
            session,
            invoice_ids=[invoice.id for invoice in open_invoices],
        )
        invoice_state = _build_invoice_state(open_invoices, matched_incoming)

        invoices_by_number: dict[str, list[int]] = defaultdict(list)
        invoices_by_inn: dict[str, list[int]] = defaultdict(list)
        open_invoice_ids: list[int] = []

        for invoice in open_invoices:
            entry = invoice_state.get(invoice.id)
            if not entry or _remaining_amount(entry) <= 0:
                continue

            open_invoice_ids.append(invoice.id)
            normalized_number = (invoice.invoice_number or "").strip().lstrip("0") or "0"
            invoices_by_number[normalized_number].append(invoice.id)

            if invoice.counterparty and invoice.counterparty.inn:
                invoices_by_inn[invoice.counterparty.inn.strip()].append(invoice.id)

        if not open_invoice_ids:
            logger.info("Все открытые счета уже полностью оплачены, матчить нечего")
            return 0, {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []

        matched_count = 0
        touched_invoice_ids: set[int] = set()
        now_utc_naive = datetime.utcnow().replace(microsecond=0)

        for operation in unmatched:
            decision = _match_operation_to_invoice(
                operation,
                invoice_state=invoice_state,
                invoices_by_number=invoices_by_number,
                invoices_by_inn=invoices_by_inn,
                open_invoice_ids=open_invoice_ids,
            )
            if not decision:
                continue

            invoice_id = int(decision["invoice_id"])
            entry = invoice_state.get(invoice_id)
            if not entry:
                continue

            updated = st_ops_repo.assign_operation_to_invoice(
                session,
                operation_row_id=operation.id,
                invoice_id=invoice_id,
                match_confidence=Decimal(str(decision["confidence"])).quantize(Decimal("0.0001")),
                match_method=str(decision["method"]),
                matched_at=now_utc_naive,
            )
            if not updated:
                continue

            amount = Decimal(str(decision["amount"])).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
            entry["paid"] = (entry["paid"] + amount).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
            touched_invoice_ids.add(invoice_id)
            matched_count += 1

        if matched_count:
            recalc_stats, newly_paid = _recalculate_payment_state(session, invoice_ids=touched_invoice_ids)
            if dry_run:
                session.rollback()
                return matched_count, recalc_stats, newly_paid

            session.commit()
            _sync_paid_invoices_to_bitrix(newly_paid)
            return matched_count, recalc_stats, newly_paid

        session.rollback()
        return 0, {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()



def main() -> None:
    """Точка входа платежного cron: синк выписки + матчинг оплат к invoices."""
    args = _parse_args()
    account_numbers = _get_account_numbers()
    initial_lookback_days = _env_int(
        "TBANK_STATEMENT_INITIAL_LOOKBACK_DAYS",
        _DEFAULT_INITIAL_LOOKBACK_DAYS,
        min_value=1,
        max_value=365,
    )
    overlap_minutes = _env_int(
        "TBANK_STATEMENT_OVERLAP_MINUTES",
        _DEFAULT_OVERLAP_MINUTES,
        min_value=5,
        max_value=24 * 60,
    )
    page_limit = _env_int(
        "TBANK_STATEMENT_PAGE_LIMIT",
        _DEFAULT_PAGE_LIMIT,
        min_value=1,
        max_value=200,
    )
    unmatched_limit = _env_int(
        "TBANK_STATEMENT_UNMATCHED_LIMIT",
        _DEFAULT_UNMATCHED_LIMIT,
        min_value=100,
        max_value=100_000,
    )

    logger.info(
        "Запуск cron_payments accounts=%s initial_lookback_days=%s overlap_minutes=%s page_limit=%s dry_run=%s dry_run_bitrix=%s",
        len(account_numbers),
        initial_lookback_days,
        overlap_minutes,
        page_limit,
        args.dry_run,
        args.dry_run_bitrix,
    )

    if args.dry_run:
        logger.warning(
            "DRY-RUN: синк выписки отключен; используем только текущие данные в БД"
        )
        matched_count, recalc_stats, newly_paid = _run_matching(
            unmatched_limit,
            dry_run=True,
        )
        _log_dry_run_paid_preview(newly_paid)

        if args.dry_run_bitrix:
            logger.warning(
                "DRY-RUN-BITRIX: выполняем Bitrix24-обновления для счетов из dry-run"
            )
            _sync_paid_invoices_to_bitrix(newly_paid)
        else:
            logger.info(
                "DRY-RUN: вызовы Bitrix24 отключены (используйте --dry-run --dry-run-bitrix)"
            )

        logger.info(
            (
                "cron_payments DRY-RUN завершен: fetched=%s created=%s matched=%s "
                "invoice_state_updates=%s (paid=%s partially_paid=%s issued=%s)"
            ),
            0,
            0,
            matched_count,
            recalc_stats.get("updated", 0),
            recalc_stats.get("paid", 0),
            recalc_stats.get("partially_paid", 0),
            recalc_stats.get("issued", 0),
        )
        return

    total_fetched = 0
    total_created = 0
    sync_errors: list[str] = []

    for account_number in account_numbers:
        try:
            fetched, created = _sync_statement_for_account(
                account_number=account_number,
                initial_lookback_days=initial_lookback_days,
                overlap_minutes=overlap_minutes,
                page_limit=page_limit,
            )
            total_fetched += fetched
            total_created += created
            logger.info(
                "Синк выписки account=%s завершен: fetched=%s created=%s",
                account_number,
                fetched,
                created,
            )
        except Exception as e:
            sync_errors.append(f"{account_number}: {e}")
            logger.exception("Ошибка синка выписки account=%s", account_number)

    if sync_errors:
        logger.error("Синк выписки завершился с ошибками: %s", sync_errors)
        sys.exit(1)

    matched_count, recalc_stats, _ = _run_matching(unmatched_limit)

    logger.info(
        (
            "cron_payments завершен: fetched=%s created=%s matched=%s "
            "invoice_state_updates=%s (paid=%s partially_paid=%s issued=%s)"
        ),
        total_fetched,
        total_created,
        matched_count,
        recalc_stats.get("updated", 0),
        recalc_stats.get("paid", 0),
        recalc_stats.get("partially_paid", 0),
        recalc_stats.get("issued", 0),
    )


if __name__ == "__main__":
    main()
