"""
CLI: отдельный крон синка выписки T-Bank и автозачета оплат по счетам.
Запуск: python3 -m src.cli.cron_payments
Или:   python3 -m src.cli.cron_payments --dry-run
Или:   python3 -m src.cli.cron_payments --dry-run --dry-run-bitrix
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Загрузка .env
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
_env = PROJECT_ROOT / ".env"
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
_DEFAULT_PAYMENT_THANK_EMAIL_LIMIT = 5000
_DEFAULT_CASHLESS_EXPENSE_SYNC_LIMIT = 5000
_DEFAULT_CASHLESS_INCOME_SYNC_LIMIT = 5000
_DEFAULT_CASHLESS_ACCOUNT_LABEL = "Благосервис ТБанк"
DEBUG_FORCE_EMAIL = (os.environ.get("DEBUG_FORCE_EMAIL") or "").strip() or None

_INVOICE_HINT_RE = re.compile(
    r"(?:сч[её]т(?:а|у|ом|ов|ам|ами|ах)?|сч\.?|с/ф|сф|invoice|inv)"
    r"\s*(?:[:\-]\s*)?(?:(?:№|#|no|n[оo])\s*)?(\d{1,15})\b",
    re.IGNORECASE,
)
_INVOICE_LIST_HINT_RE = re.compile(
    r"(?:сч[её]т(?:а|у|ом|ов|ам|ами|ах)?|сч\.?|с/ф|сф|invoice|inv)"
    r"\s*(?:[:\-]\s*)?(?:(?:№|#|no|n[оo])\s*)?"
    r"("
    r"\d{1,15}(?:\s*от\s*\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4})?"
    r"(?:\s*(?:[,;/]|\bи\b|\band\b)\s*(?:(?:№|#|no|n[оo])\s*)?\d{1,15}"
    r"(?:\s*от\s*\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4})?)*"
    r")\b",
    re.IGNORECASE,
)
_INVOICE_LIST_NUMBER_RE = re.compile(
    r"(?:^|[,;/]|\bи\b|\band\b)\s*(?:(?:№|#|no|n[оo])\s*)?"
    r"(\d{1,15})(?:\s*от\s*\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4})?",
    re.IGNORECASE,
)
_NON_ALNUM_RE = re.compile(r"[^0-9a-zа-яё]+", re.IGNORECASE)
_PAY_PURPOSE_ANALYTICS_CODE_RE = re.compile(r"^\s*(\d{4})-(\d{2,4})-\d+\b")
_RULE_NON_ALNUM_RE = re.compile(r"[^0-9a-zа-я]+", re.IGNORECASE)


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


def _parse_date_arg(value: str) -> date:
    raw = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError("ожидается дата YYYY-MM-DD или DD.MM.YYYY")


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
    parser.add_argument(
        "--force-cashless-expenses",
        action="store_true",
        help=(
            "Повторно выгрузить расходы в Sheets, игнорируя отметку "
            "cashless_expense_sheet_synced_at в БД. Дедупликация строк в листе сохраняется."
        ),
    )
    parser.add_argument(
        "--statement-date",
        "--statement-from-date",
        "--statement-for-date",
        dest="statement_date",
        type=_parse_date_arg,
        metavar="DATE",
        help=(
            "Запросить выписку T-Bank начиная с указанного бизнес-дня "
            "(YYYY-MM-DD или DD.MM.YYYY), игнорируя last_success_at. "
            "Состояние обычного инкрементального синка не обновляется."
        ),
    )
    parser.add_argument(
        "--cashless-expenses-from-date",
        "--expenses-from-date",
        "--from-date",
        dest="cashless_expenses_from_date",
        type=_parse_date_arg,
        metavar="DATE",
        help=(
            "Нижняя дата операций для выгрузки расходов в Sheets "
            "(YYYY-MM-DD или DD.MM.YYYY). Переопределяет GOOGLE_CASHLESS_EXPENSE_SYNC_FROM_DATE."
        ),
    )
    parser.add_argument(
        "--force-cashless-incomes",
        action="store_true",
        help=(
            "Повторно выгрузить доходы в Sheets, игнорируя отметку "
            "cashless_income_sheet_synced_at в БД. Дедупликация строк в листе сохраняется."
        ),
    )
    parser.add_argument(
        "--cashless-incomes-from-date",
        "--incomes-from-date",
        dest="cashless_incomes_from_date",
        type=_parse_date_arg,
        metavar="DATE",
        help=(
            "Нижняя дата операций для выгрузки доходов в Sheets "
            "(YYYY-MM-DD или DD.MM.YYYY). Переопределяет GOOGLE_CASHLESS_INCOME_SYNC_FROM_DATE."
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


def _get_account_labels() -> dict[str, str]:
    raw = (os.environ.get("TBANK_STATEMENT_ACCOUNT_LABELS") or "").strip()
    if not raw:
        return {}

    labels: dict[str, str] = {}
    for part in re.split(r"[,;\n]+", raw):
        item = part.strip()
        if not item:
            continue
        if "=" not in item:
            logger.warning(
                "ENV TBANK_STATEMENT_ACCOUNT_LABELS: пропускаем элемент без '=': %s",
                item,
            )
            continue
        account, label = item.split("=", 1)
        account = account.strip()
        label = label.strip()
        if account and label:
            labels[account] = label

    return labels


def _account_label(account_number: str, labels: dict[str, str]) -> str:
    default_label = (
        os.environ.get("TBANK_STATEMENT_DEFAULT_ACCOUNT_LABEL", "").strip()
        or _DEFAULT_CASHLESS_ACCOUNT_LABEL
    )
    return labels.get(account_number, default_label)


def _cashless_sheet_sync_from(from_date: date | None = None, *, env_name: str) -> datetime | None:
    parsed_day = from_date
    if parsed_day is None:
        raw = (os.environ.get(env_name) or "").strip()
        if not raw:
            return None

        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                parsed_day = datetime.strptime(raw, fmt).date()
                break
            except ValueError:
                continue

        if parsed_day is None:
            logger.warning(
                "ENV %s='%s' не дата YYYY-MM-DD/DD.MM.YYYY, фильтр отключен",
                env_name,
                raw,
            )
            return None

    from_utc, _ = _utc_naive_bounds_for_business_date(parsed_day)
    return from_utc


def _cashless_expense_sync_from(from_date: date | None = None) -> datetime | None:
    return _cashless_sheet_sync_from(from_date, env_name="GOOGLE_CASHLESS_EXPENSE_SYNC_FROM_DATE")


def _cashless_income_sync_from(from_date: date | None = None) -> datetime | None:
    return _cashless_sheet_sync_from(from_date, env_name="GOOGLE_CASHLESS_INCOME_SYNC_FROM_DATE")



def _to_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)


def _get_business_timezone():
    raw = (os.environ.get("APP_TIMEZONE") or os.environ.get("TZ") or "Europe/Moscow").strip()
    try:
        return ZoneInfo(raw)
    except ZoneInfoNotFoundError:
        logger.warning("Неизвестная таймзона APP_TIMEZONE/TZ='%s', используем UTC", raw)
        return timezone.utc


def _business_today() -> date:
    return datetime.now(_get_business_timezone()).date()


def _utc_naive_bounds_for_business_date(day: date) -> tuple[datetime, datetime]:
    tz = _get_business_timezone()
    start_local = datetime.combine(day, datetime.min.time()).replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return _to_utc_naive(start_local), _to_utc_naive(end_local)



def _to_utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _business_date_from_utc_naive(dt: datetime | None) -> date | None:
    utc_dt = _to_utc_aware(dt)
    if utc_dt is None:
        return None
    return utc_dt.astimezone(_get_business_timezone()).date()



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


def _format_money_ru(amount: Decimal) -> str:
    amount = Decimal(str(amount)).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
    return f"{amount:,.2f}".replace(",", "\u00a0").replace(".", ",")


def _operation_counterparty_for_expense(operation: Any) -> str:
    return (
        str(operation.receiver_name or "").strip()
        or str(operation.counterparty_name or "").strip()
        or str(operation.payer_name or "").strip()
    )


def _operation_counterparty_for_income(operation: Any) -> str:
    return (
        str(operation.payer_name or "").strip()
        or str(operation.counterparty_name or "").strip()
        or str(operation.receiver_name or "").strip()
    )


def _operation_counterparty_inn_for_income(operation: Any) -> str:
    return str(operation.payer_inn or "").strip() or str(operation.counterparty_inn or "").strip()


def _operation_purpose_for_sheet(operation: Any) -> str:
    return str(operation.pay_purpose or "").strip() or str(operation.description or "").strip()


def _load_code_dictionary(filename: str) -> dict[str, str]:
    path = CONFIG_DIR / filename
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        logger.warning("Справочник %s не найден, аналитика расходов будет пустой", path)
        return {}

    if not isinstance(raw, dict):
        logger.warning("Справочник %s должен быть JSON object, аналитика расходов будет пустой", path)
        return {}

    return {str(code).strip(): str(name).strip() for code, name in raw.items() if str(code).strip()}


def _load_cashless_expense_fallback_rules() -> dict[str, Any]:
    path = CONFIG_DIR / "cashless_expense_fallback_rules.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        logger.warning("Справочник %s не найден, fallback-аналитика расходов отключена", path)
        return {"default_structure_code": "", "rules": []}

    if not isinstance(raw, dict):
        logger.warning("Справочник %s должен быть JSON object, fallback-аналитика расходов отключена", path)
        return {"default_structure_code": "", "rules": []}

    rules = raw.get("rules")
    if not isinstance(rules, list):
        logger.warning("В справочнике %s поле rules должно быть массивом", path)
        rules = []

    return {
        "default_structure_code": (
            os.environ.get("GOOGLE_CASHLESS_DEFAULT_STRUCTURE_CODE", "").strip()
            or str(raw.get("default_structure_code") or "").strip()
        ),
        "rules": [rule for rule in rules if isinstance(rule, dict)],
    }


def _default_cashless_structure_name(structure_by_code: dict[str, str]) -> str:
    fallback_rules = _load_cashless_expense_fallback_rules()
    structure_code = str(fallback_rules.get("default_structure_code") or "").strip()
    if not structure_code:
        return ""

    structure_name = structure_by_code.get(structure_code, "")
    if not structure_name:
        logger.warning("Код дефолтной структуры %s не найден в config/structure.json", structure_code)
    return structure_name


def _normalize_cashless_rule_text(value: str | None) -> str:
    normalized = str(value or "").lower().replace("ё", "е")
    normalized = _RULE_NON_ALNUM_RE.sub(" ", normalized)
    return f" {' '.join(normalized.split())} "


def _cashless_rule_contains_any(normalized_text: str, patterns: Any) -> bool:
    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, list):
        return False

    for pattern in patterns:
        normalized_pattern = _normalize_cashless_rule_text(str(pattern))
        if normalized_pattern.strip() and normalized_pattern in normalized_text:
            return True
    return False


def _cashless_expense_fallback_rule_result(
    rule: dict[str, Any],
    *,
    default_structure_code: str,
    structure_by_code: dict[str, str],
    operation_by_code: dict[str, str],
) -> tuple[str, str]:
    structure_code = str(rule.get("structure_code") or default_structure_code).strip()
    operation_code = str(rule.get("operation_code") or "").strip()
    return (
        structure_by_code.get(structure_code, ""),
        operation_by_code.get(operation_code, ""),
    )


def _match_cashless_expense_fallback_rule(
    pay_purpose: str,
    counterparty: str,
    *,
    fallback_rules: dict[str, Any],
    structure_by_code: dict[str, str],
    operation_by_code: dict[str, str],
) -> tuple[str, str]:
    normalized_pay_purpose = _normalize_cashless_rule_text(pay_purpose)
    normalized_counterparty = _normalize_cashless_rule_text(counterparty)
    default_structure_code = str(fallback_rules.get("default_structure_code") or "").strip()
    rules = fallback_rules.get("rules") or []

    for rule in rules:
        if not _cashless_rule_contains_any(normalized_counterparty, rule.get("counterparty_contains_any")):
            continue
        return _cashless_expense_fallback_rule_result(
            rule,
            default_structure_code=default_structure_code,
            structure_by_code=structure_by_code,
            operation_by_code=operation_by_code,
        )

    for rule in rules:
        if not _cashless_rule_contains_any(normalized_pay_purpose, rule.get("contains_any")):
            continue
        return _cashless_expense_fallback_rule_result(
            rule,
            default_structure_code=default_structure_code,
            structure_by_code=structure_by_code,
            operation_by_code=operation_by_code,
        )

    return "", ""


def _parse_pay_purpose_analytics(
    pay_purpose: str,
    counterparty: str,
    *,
    structure_by_code: dict[str, str],
    operation_by_code: dict[str, str],
    fallback_rules: dict[str, Any],
) -> tuple[str, str]:
    match = _PAY_PURPOSE_ANALYTICS_CODE_RE.match(pay_purpose or "")
    if not match:
        return _match_cashless_expense_fallback_rule(
            pay_purpose,
            counterparty,
            fallback_rules=fallback_rules,
            structure_by_code=structure_by_code,
            operation_by_code=operation_by_code,
        )

    structure_code = match.group(1)
    operation_code = match.group(2).lstrip("0") or "0"
    structure_name = structure_by_code.get(structure_code, "")
    operation_name = operation_by_code.get(operation_code, "")
    if structure_name and operation_name:
        return structure_name, operation_name

    fallback_structure_name, fallback_operation_name = _match_cashless_expense_fallback_rule(
        pay_purpose,
        counterparty,
        fallback_rules=fallback_rules,
        structure_by_code=structure_by_code,
        operation_by_code=operation_by_code,
    )
    return (
        structure_name or fallback_structure_name,
        operation_name or fallback_operation_name,
    )


def _cashless_operation_date(operation: Any) -> date | None:
    dt = _operation_effective_datetime(operation)
    return _business_date_from_utc_naive(dt)



def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return _NON_ALNUM_RE.sub("", value.lower())



def _extract_invoice_numbers(*texts: str | None) -> set[str]:
    joined = "\n".join(part for part in texts if part)
    if not joined:
        return set()

    numbers = {m.group(1).lstrip("0") or "0" for m in _INVOICE_HINT_RE.finditer(joined)}
    for match in _INVOICE_LIST_HINT_RE.finditer(joined):
        numbers.update(number.lstrip("0") or "0" for number in _INVOICE_LIST_NUMBER_RE.findall(match.group(1)))
    return numbers


def _normalize_invoice_number(value: Any) -> str:
    return str(value or "").strip().lstrip("0") or "0"


def _operation_invoice_numbers(operation: Any) -> set[str]:
    return _extract_invoice_numbers(
        getattr(operation, "pay_purpose", None),
        getattr(operation, "description", None),
    )


def _operation_mentions_different_invoice_number(operation: Any, invoice: Any) -> bool:
    mentioned_numbers = _operation_invoice_numbers(operation)
    if not mentioned_numbers:
        return False
    return _normalize_invoice_number(invoice.invoice_number) not in mentioned_numbers



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


_STATEMENT_WINDOW_DATE_FIELDS = (
    "doc_date",
    "trxn_post_date",
    "authorization_date",
    "operation_date",
    "charge_date",
    "draw_date",
)


_STATEMENT_QUERY_DATE_FIELDS = (
    "operation_date",
    "charge_date",
    "draw_date",
    "trxn_post_date",
    "authorization_date",
    "doc_date",
)

_PAYMENT_MATCH_DATE_FIELDS = (
    "operation_date",
    "trxn_post_date",
    "authorization_date",
    "doc_date",
    "charge_date",
    "draw_date",
)


def _operation_query_datetime_with_field(operation_data: dict[str, Any]) -> tuple[str | None, datetime | None]:
    for field_name in _STATEMENT_QUERY_DATE_FIELDS:
        value = operation_data.get(field_name)
        if value is not None:
            return field_name, value
    return None, None


def _operation_query_datetime(operation_data: dict[str, Any]) -> datetime | None:
    _, value = _operation_query_datetime_with_field(operation_data)
    return value


def _operation_effective_datetime(operation: Any) -> datetime | None:
    for field_name in _STATEMENT_WINDOW_DATE_FIELDS:
        value = getattr(operation, field_name, None)
        if value is not None:
            return value
    return None


def _dt_log(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _is_operation_in_window(
    operation_data: dict[str, Any],
    *,
    from_utc: datetime,
    to_utc: datetime,
) -> bool:
    operation_dt = _to_utc_aware(_operation_query_datetime(operation_data))
    if operation_dt is None:
        return True
    return from_utc <= operation_dt < to_utc



def _invoice_total(invoice: Any) -> Decimal:
    total = Decimal("0.00")
    for item in invoice.items:
        price = Decimal(str(item.price or 0))
        amount = Decimal(str(item.amount or 0))
        total += price * amount
    return total.quantize(_MONEY_Q, rounding=ROUND_HALF_UP)


def _invoice_paid_amount(invoice: Any) -> Decimal:
    return Decimal(str(invoice.paid_amount or 0)).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)


def _invoice_needs_payment_backfill(invoice: Any) -> bool:
    if str(invoice.status or "").strip() != "paid":
        return False
    if invoice.paid_at is None:
        return True
    return abs(_invoice_paid_amount(invoice) - _invoice_total(invoice)) > _AMOUNT_TOLERANCE


def _merge_invoices_by_id(*invoice_lists: list[Any]) -> list[Any]:
    by_id: dict[int, Any] = {}
    for invoice_list in invoice_lists:
        for invoice in invoice_list:
            by_id.setdefault(int(invoice.id), invoice)
    return list(by_id.values())


def _invoice_state_is_fully_paid(entry: dict[str, Any] | None) -> bool:
    if not entry:
        return False
    paid = Decimal(str(entry["paid"])).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
    total = Decimal(str(entry["total"])).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
    return paid > _AMOUNT_TOLERANCE and paid + _AMOUNT_TOLERANCE >= total



def _build_invoice_state(open_invoices: list[Any], matched_incoming: list[Any]) -> dict[int, dict[str, Any]]:
    invoice_by_id: dict[int, Any] = {int(invoice.id): invoice for invoice in open_invoices}
    paid_by_invoice: dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
    for op in matched_incoming:
        invoice_id = int(op.matched_invoice_id)
        invoice = invoice_by_id.get(invoice_id)
        payment_dt = _payment_match_datetime(op)
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


def _choose_earliest_invoice_id(
    candidate_ids: list[int],
    invoice_state: dict[int, dict[str, Any]],
) -> int | None:
    candidates: list[tuple[datetime, int]] = []
    for invoice_id in candidate_ids:
        entry = invoice_state.get(invoice_id)
        if not entry:
            continue
        invoice = entry["invoice"]
        issued_at = getattr(invoice, "issued_at", None)
        issued_utc = _to_utc_aware(issued_at)
        if issued_utc is None:
            continue
        candidates.append((issued_utc, invoice_id))

    if not candidates:
        return min(candidate_ids) if candidate_ids else None

    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][1]



def _match_operation_to_invoice(
    operation: Any,
    *,
    invoice_state: dict[int, dict[str, Any]],
    invoices_by_number: dict[str, list[int]],
    invoices_by_inn: dict[str, list[int]],
    open_invoice_ids: list[int],
    strict_amount_fallback_invoice_ids: set[int] | None = None,
) -> dict[str, Any] | None:
    amount = _operation_amount_from_row(operation)
    if amount <= 0:
        return None

    payment_dt = _payment_match_datetime(operation)
    payer_inn = (operation.payer_inn or operation.counterparty_inn or "").strip()
    if not payer_inn:
        return None
    payer_name = operation.payer_name or operation.counterparty_name
    strict_amount_fallback_invoice_ids = strict_amount_fallback_invoice_ids or set()

    def _uses_strict_amount_fallback(invoice_id: int) -> bool:
        return invoice_id in strict_amount_fallback_invoice_ids

    def _has_required_inn(invoice: Any) -> bool:
        if not invoice or not invoice.counterparty:
            return False
        invoice_inn = (invoice.counterparty.inn or "").strip()
        return bool(invoice_inn) and invoice_inn == payer_inn

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
                if not _has_required_inn(invoice):
                    continue
                if not _is_payment_after_invoice_issue(
                    payment_dt=payment_dt,
                    invoice_issued_at=invoice.issued_at,
                ):
                    continue
                score = Decimal("1.00")
                method = "invoice_number"
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
        return None

    # 2) Надежный fallback: уникальный ИНН + сумма в пределах остатка.
    # Для paid-backfill счетов fallback строже: только точная сумма и только
    # если нет обычных issued/partially_paid кандидатов.
    candidates = []
    strict_amount_candidates = []
    for invoice_id in invoices_by_inn.get(payer_inn, []):
        entry = invoice_state.get(invoice_id)
        if not entry:
            continue
        invoice = entry["invoice"]
        if not _has_required_inn(invoice):
            continue
        if not _is_payment_after_invoice_issue(
            payment_dt=payment_dt,
            invoice_issued_at=invoice.issued_at,
        ):
            continue
        remaining = _remaining_amount(entry)
        if remaining <= 0:
            continue
        if _uses_strict_amount_fallback(invoice_id):
            if _amount_equal(amount, remaining):
                strict_amount_candidates.append(invoice_id)
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
    if len(candidates) > 1:
        invoice_id = _choose_earliest_invoice_id(candidates, invoice_state)
        if invoice_id is not None:
            return {
                "invoice_id": invoice_id,
                "confidence": Decimal("0.86"),
                "method": "payer_inn_amount_earliest",
                "amount": amount,
            }
    if len(strict_amount_candidates) == 1:
        return {
            "invoice_id": strict_amount_candidates[0],
            "confidence": Decimal("0.84"),
            "method": "payer_inn_exact_amount_paid_backfill",
            "amount": amount,
        }

    # 3) Осторожный fallback: уникальное совпадение по имени + сумме.
    scored_name: list[tuple[Decimal, int, str]] = []
    for invoice_id in open_invoice_ids:
        if _uses_strict_amount_fallback(invoice_id):
            continue
        entry = invoice_state.get(invoice_id)
        if not entry:
            continue
        remaining = _remaining_amount(entry)
        if remaining <= 0:
            continue
        if amount - remaining > _AMOUNT_TOLERANCE:
            continue

        invoice = entry["invoice"]
        if not _has_required_inn(invoice):
            continue
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


def _build_multi_invoice_payment_allocations(
    operations: list[Any],
    *,
    invoice_state: dict[int, dict[str, Any]],
    invoices_by_number: dict[str, list[int]],
    target_invoice_ids: set[int],
    preserve_paid_status_ids: set[int] | None = None,
) -> dict[int, list[dict[str, Any]]]:
    allocations: dict[int, list[dict[str, Any]]] = defaultdict(list)
    if not operations or not target_invoice_ids:
        return allocations
    preserve_paid_status_ids = preserve_paid_status_ids or set()

    def _allocation_amount(invoice_id: int, entry: dict[str, Any]) -> Decimal:
        if invoice_id in preserve_paid_status_ids:
            return Decimal(str(entry["total"])).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
        return _remaining_amount(entry)

    for operation in operations:
        mentioned_numbers = _operation_invoice_numbers(operation)
        if len(mentioned_numbers) < 2:
            continue

        operation_amount = _operation_amount_from_row(operation)
        if operation_amount <= 0:
            continue

        payer_inn = str(operation.payer_inn or operation.counterparty_inn or "").strip()
        if not payer_inn:
            continue

        payment_dt = _payment_datetime(operation)
        match_dt = _payment_match_datetime(operation)
        candidate_entries: dict[int, dict[str, Any]] = {}
        for number in mentioned_numbers:
            for invoice_id in invoices_by_number.get(number, []):
                entry = invoice_state.get(invoice_id)
                if not entry:
                    continue
                invoice = entry["invoice"]
                if _allocation_amount(invoice_id, entry) <= _AMOUNT_TOLERANCE:
                    continue
                invoice_inn = str(invoice.counterparty.inn if invoice.counterparty else "").strip()
                if not invoice_inn or invoice_inn != payer_inn:
                    continue
                if not _is_payment_after_invoice_issue(
                    payment_dt=match_dt,
                    invoice_issued_at=invoice.issued_at,
                ):
                    continue
                candidate_entries[invoice_id] = entry

        if len(candidate_entries) < 2:
            continue
        allocation_invoice_ids = set(candidate_entries) & target_invoice_ids
        if not allocation_invoice_ids:
            continue

        total = sum(
            (
                _allocation_amount(invoice_id, entry)
                for invoice_id, entry in candidate_entries.items()
            ),
            Decimal("0.00"),
        ).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
        if not _amount_equal(operation_amount, total):
            logger.info(
                (
                    "Мультисчетный платеж не распределен: statement_row_id=%s operation_id=%s "
                    "amount=%s candidate_invoice_ids=%s allocation_target_invoice_ids=%s "
                    "candidate_total=%s invoice_numbers_in_text=%s"
                ),
                operation.id,
                operation.operation_id,
                _format_money_ru(operation_amount),
                sorted(candidate_entries),
                sorted(allocation_invoice_ids),
                _format_money_ru(total),
                ",".join(sorted(mentioned_numbers)),
            )
            continue

        note = "multi_invoice_numbers=" + ",".join(sorted(mentioned_numbers))
        for invoice_id, entry in candidate_entries.items():
            if invoice_id not in allocation_invoice_ids:
                continue
            allocations[invoice_id].append(
                {
                    "operation": operation,
                    "amount": _allocation_amount(invoice_id, entry),
                    "payment_dt": payment_dt,
                    "note": note,
                }
            )

    return allocations



def _payment_datetime(op_row: Any) -> datetime | None:
    return _operation_effective_datetime(op_row)


def _payment_match_datetime(op_row: Any) -> datetime | None:
    for field_name in _PAYMENT_MATCH_DATE_FIELDS:
        value = getattr(op_row, field_name, None)
        if value is not None:
            return value
    return None


def _operation_on_or_after(operation: Any, from_utc: datetime | None) -> bool:
    if from_utc is None:
        return True
    operation_dt = _to_utc_aware(_payment_datetime(operation))
    if operation_dt is None:
        return True
    return operation_dt >= from_utc


def _format_dt_log(value: datetime | None) -> str | None:
    return value.isoformat(sep=" ") if value else None


def _log_text(value: Any, *, max_len: int = 1200) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_len:
        return text
    return f"{text[:max_len]}..."


def _log_statement_operation_context(
    *,
    prefix: str,
    operation: Any,
    allocated_amount: Decimal | None = None,
    allocation_note: str | None = None,
) -> None:
    purpose = str(operation.pay_purpose or "").strip()
    description = str(operation.description or "").strip()
    invoice_numbers = ",".join(sorted(_extract_invoice_numbers(purpose, description))) or None
    logger.info(
        (
            "%s statement_row_id=%s account=%s operation_id=%s document_number=%s "
            "amount=%s allocated_amount=%s payment_dt=%s operation_date=%s doc_date=%s trxn_post_date=%s "
            "payer_inn=%s payer_name=%s match_method=%s match_confidence=%s "
            "invoice_numbers_in_text=%s allocation_note=%s pay_purpose=%s description=%s"
        ),
        prefix,
        operation.id,
        operation.account_number,
        operation.operation_id,
        operation.document_number,
        _format_money_ru(_operation_amount_from_row(operation)),
        _format_money_ru(allocated_amount) if allocated_amount is not None else None,
        _format_dt_log(_payment_datetime(operation)),
        _format_dt_log(operation.operation_date),
        _format_dt_log(operation.doc_date),
        _format_dt_log(operation.trxn_post_date),
        operation.payer_inn or operation.counterparty_inn,
        _log_text(operation.payer_name or operation.counterparty_name),
        operation.match_method,
        operation.match_confidence,
        invoice_numbers,
        allocation_note,
        _log_text(purpose),
        _log_text(description),
    )


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
    preserve_paid_status_ids: set[int] | None = None,
    extra_payment_allocations: dict[int, list[dict[str, Any]]] | None = None,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    from src.db.repos import invoices as inv_repo
    from src.db.repos import statement_operations as st_ops_repo

    if not invoice_ids:
        return {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []
    preserve_paid_status_ids = preserve_paid_status_ids or set()
    extra_payment_allocations = extra_payment_allocations or {}

    invoice_list = inv_repo.get_for_payment_recalc(session, sorted(invoice_ids))
    matched = st_ops_repo.get_matched_incoming_for_invoices(session, invoice_ids=sorted(invoice_ids))

    invoice_by_id: dict[int, Any] = {int(invoice.id): invoice for invoice in invoice_list}
    paid_sum: dict[int, Decimal] = defaultdict(lambda: Decimal("0.00"))
    paid_at: dict[int, datetime | None] = {}
    payment_ops_by_invoice: dict[int, list[tuple[Any, Decimal | None, str | None]]] = defaultdict(list)
    ignored_ops_by_invoice: dict[int, list[tuple[Any, str]]] = defaultdict(list)

    for op in matched:
        inv_id = int(op.matched_invoice_id)
        payment_dt = _payment_datetime(op)
        match_dt = _payment_match_datetime(op)
        invoice = invoice_by_id.get(inv_id)
        if invoice and _operation_mentions_different_invoice_number(op, invoice):
            ignored_ops_by_invoice[inv_id].append((op, "в назначении указан другой номер счета"))
            continue
        if invoice and not _is_payment_after_invoice_issue(
            payment_dt=match_dt,
            invoice_issued_at=invoice.issued_at,
        ):
            ignored_ops_by_invoice[inv_id].append((op, "платеж раньше issued_at"))
            continue

        payment_ops_by_invoice[inv_id].append((op, None, None))
        paid_sum[inv_id] += _operation_amount_from_row(op)
        if payment_dt is None:
            continue
        current = paid_at.get(inv_id)
        if current is None or payment_dt > current:
            paid_at[inv_id] = payment_dt

    for inv_id, allocations in extra_payment_allocations.items():
        invoice = invoice_by_id.get(int(inv_id))
        if not invoice:
            continue
        for allocation in allocations:
            op = allocation.get("operation")
            amount = Decimal(str(allocation.get("amount") or 0)).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
            if op is None or amount <= 0:
                continue
            payment_dt = allocation.get("payment_dt")
            if not isinstance(payment_dt, datetime):
                payment_dt = _payment_datetime(op)
            match_dt = _payment_match_datetime(op)
            if not _is_payment_after_invoice_issue(
                payment_dt=match_dt,
                invoice_issued_at=invoice.issued_at,
            ):
                ignored_ops_by_invoice[int(inv_id)].append((op, "платеж раньше issued_at"))
                continue
            note = str(allocation.get("note") or "").strip() or None
            payment_ops_by_invoice[int(inv_id)].append((op, amount, note))
            paid_sum[int(inv_id)] += amount
            if payment_dt is None:
                continue
            current = paid_at.get(int(inv_id))
            if current is None or payment_dt > current:
                paid_at[int(inv_id)] = payment_dt

    stats = {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}
    newly_paid: list[dict[str, Any]] = []
    for invoice in invoice_list:
        total = _invoice_total(invoice)
        paid = paid_sum.get(invoice.id, Decimal("0.00")).quantize(_MONEY_Q, rounding=ROUND_HALF_UP)
        prev_status = str(invoice.status or "").strip()

        preserve_existing_paid = invoice.id in preserve_paid_status_ids and prev_status == "paid"
        if preserve_existing_paid and paid <= _AMOUNT_TOLERANCE:
            logger.info(
                (
                    "Backfill оплаты счета id=%s number=%s пропущен: "
                    "нет учитываемых операций после даты выставления; status=%s paid_amount=%s paid_at=%s"
                ),
                invoice.id,
                invoice.invoice_number,
                invoice.status,
                _format_money_ru(_invoice_paid_amount(invoice)),
                _format_dt_log(invoice.paid_at),
            )
            for op, reason in ignored_ops_by_invoice.get(invoice.id, []):
                _log_statement_operation_context(
                    prefix=f"Операция выписки не учтена для счета id={invoice.id} number={invoice.invoice_number}: {reason}",
                    operation=op,
                )
            continue
        if preserve_existing_paid:
            new_status = "paid"
            new_paid_at = paid_at.get(invoice.id) or invoice.paid_at or datetime.utcnow()
            stats["paid"] += 1
        elif paid <= _AMOUNT_TOLERANCE:
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

        logger.info(
            (
                "Обновляем оплату счета id=%s number=%s total=%s "
                "status=%s->%s paid_amount=%s->%s paid_at=%s->%s preserve_paid=%s"
            ),
            invoice.id,
            invoice.invoice_number,
            _format_money_ru(total),
            invoice.status,
            new_status,
            _format_money_ru(_invoice_paid_amount(invoice)),
            _format_money_ru(paid),
            _format_dt_log(invoice.paid_at),
            _format_dt_log(new_paid_at),
            preserve_existing_paid,
        )
        for op, allocated_amount, allocation_note in payment_ops_by_invoice.get(invoice.id, []):
            _log_statement_operation_context(
                prefix=f"Операция выписки для обновления счета id={invoice.id} number={invoice.invoice_number}",
                operation=op,
                allocated_amount=allocated_amount,
                allocation_note=allocation_note,
            )
        for op, reason in ignored_ops_by_invoice.get(invoice.id, []):
            _log_statement_operation_context(
                prefix=f"Операция выписки не учтена для счета id={invoice.id} number={invoice.invoice_number}: {reason}",
                operation=op,
            )
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


def _build_payment_thank_recipients(invoice: Any) -> list[str]:
    from src.notifications.invoice_reminder_email import normalize_emails

    if DEBUG_FORCE_EMAIL:
        return normalize_emails(DEBUG_FORCE_EMAIL)

    recipient_sources: list[str] = []
    primary = (invoice.recipient_emails_snapshot or "").strip()
    if not primary and invoice.counterparty:
        primary = (invoice.counterparty.email or "").strip()
    if primary:
        recipient_sources.append(primary)

    accountant_email = ""
    if invoice.counterparty:
        accountant_email = (invoice.counterparty.email_accountant or "").strip()
    if accountant_email:
        recipient_sources.append(accountant_email)

    return normalize_emails(recipient_sources)


def _send_due_payment_thank_you_emails(
    *,
    limit: int,
    newly_paid_invoice_ids: list[int] | None = None,
) -> dict[str, int]:
    """Отправляет email-благодарности по due-счетам и новым оплатам текущего запуска."""
    from src.db.connection import get_session
    from src.db.repos import invoices as inv_repo
    from src.notifications.invoice_reminder_email import send_invoice_payment_thank_you

    business_day = _business_today()
    paid_from, paid_to = _utc_naive_bounds_for_business_date(business_day)
    stats = {
        "candidates": 0,
        "sent": 0,
        "failed": 0,
        "skipped": 0,
    }

    session = get_session()
    try:
        invoices = inv_repo.get_paid_due_for_payment_thank_email(
            session,
            paid_from=paid_from,
            paid_to=paid_to,
            limit=limit,
        )
        seen_invoice_ids = {int(invoice.id) for invoice in invoices}
        extra_invoice_ids = [
            int(invoice_id)
            for invoice_id in newly_paid_invoice_ids or []
            if int(invoice_id) not in seen_invoice_ids
        ]
        if extra_invoice_ids:
            invoices.extend(
                inv_repo.get_paid_pending_payment_thank_email_by_ids(
                    session,
                    invoice_ids=extra_invoice_ids,
                )
            )
        stats["candidates"] = len(invoices)
        if not invoices:
            return stats

        if DEBUG_FORCE_EMAIL:
            logger.warning("Используется DEBUG_FORCE_EMAIL override для thank-you email: %s", DEBUG_FORCE_EMAIL)

        for invoice in invoices:
            invoice_number = (invoice.invoice_number or "").strip() or str(invoice.id)
            recipients = _build_payment_thank_recipients(invoice)
            recipient_snapshot = ", ".join(recipients) or None
            if not recipients:
                stats["skipped"] += 1
                logger.warning(
                    "Payment thank-you email skipped invoice=%s: не задан email получателя",
                    invoice_number,
                )
                continue

            counterparty_name = (
                (invoice.counterparty.name if invoice.counterparty else "")
                or f"контрагент #{invoice.counterparty_id}"
            )
            invoice_date = _business_date_from_utc_naive(invoice.issued_at) or invoice.issued_at.date()
            payment_date = _business_date_from_utc_naive(invoice.paid_at) or business_day
            was_overdue = invoice.due_date is not None and payment_date > invoice.due_date
            total_amount = _invoice_total(invoice)

            try:
                send_invoice_payment_thank_you(
                    recipients=recipients,
                    invoice_number=invoice_number,
                    counterparty_name=counterparty_name,
                    invoice_date=invoice_date,
                    payment_date=payment_date,
                    due_date=invoice.due_date,
                    total_amount=total_amount,
                    was_overdue=was_overdue,
                )
                sent_at = datetime.utcnow().replace(microsecond=0)
                marked = inv_repo.mark_payment_thank_email_sent(
                    session,
                    invoice_id=int(invoice.id),
                    sent_at=sent_at,
                )
                if not marked:
                    session.rollback()
                    stats["skipped"] += 1
                    logger.info(
                        "Payment thank-you email already marked invoice=%s recipients=%s",
                        invoice_number,
                        recipient_snapshot,
                    )
                    continue

                session.commit()
                stats["sent"] += 1
                logger.info(
                    "Payment thank-you email sent invoice=%s recipients=%s overdue=%s",
                    invoice_number,
                    recipient_snapshot,
                    was_overdue,
                )
            except Exception:
                session.rollback()
                stats["failed"] += 1
                logger.exception(
                    "Ошибка отправки thank-you email invoice=%s recipients=%s",
                    invoice_number,
                    recipient_snapshot,
                )

        return stats
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


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
    statement_date: date | None = None,
    dry_run: bool = False,
    session: Any | None = None,
) -> dict[str, int]:
    from src.db.repos import statement_operations as st_ops_repo
    from src.tbank.client import get_statement

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    own_session = session is None
    if session is None:
        from src.db.connection import get_session

        session = get_session()
    try:
        if statement_date is not None:
            business_today = _business_today()
            if statement_date > business_today:
                raise ValueError(
                    f"--statement-date={statement_date.isoformat()} находится в будущем "
                    f"для бизнес-таймзоны {_get_business_timezone()}"
                )

            from_naive, _ = _utc_naive_bounds_for_business_date(statement_date)
            from_utc = _to_utc_aware(from_naive)
            to_utc = now_utc
            sync_mode = f"manual-from-date:{statement_date.isoformat()}"
        else:
            state = st_ops_repo.get_or_create_sync_state(session, account_number=account_number)
            session.flush()

            if state.last_success_at:
                from_utc = _to_utc_aware(state.last_success_at) - timedelta(minutes=overlap_minutes)
            else:
                from_utc = now_utc - timedelta(days=initial_lookback_days)
            to_utc = now_utc
            sync_mode = "incremental"

        if from_utc >= to_utc:
            from_utc = to_utc - timedelta(minutes=max(5, overlap_minutes))

        logger.info(
            "Синк выписки account=%s mode=%s query_from=%s query_to=%s",
            account_number,
            sync_mode,
            from_utc.isoformat(),
            to_utc.isoformat(),
        )

        cursor: str | None = None
        pages = 0
        stats = {
            "fetched": 0,
            "created": 0,
            "existing": 0,
            "skipped_out_of_window": 0,
        }
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
                stats["fetched"] += 1
                if not _is_operation_in_window(op_data, from_utc=from_utc, to_utc=to_utc):
                    stats["skipped_out_of_window"] += 1
                    if statement_date is not None and stats["skipped_out_of_window"] <= 20:
                        date_field, date_value = _operation_query_datetime_with_field(op_data)
                        logger.info(
                            (
                                "Ручной синк: операция вне запрошенного окна account=%s "
                                "operation_id=%s date_field=%s date_value=%s "
                                "operation_date=%s charge_date=%s draw_date=%s "
                                "trxn_post_date=%s authorization_date=%s doc_date=%s "
                                "amount=%s status=%s type=%s"
                            ),
                            account_number,
                            op_data.get("operation_id"),
                            date_field,
                            _dt_log(_to_utc_aware(date_value)),
                            _dt_log(op_data.get("operation_date")),
                            _dt_log(op_data.get("charge_date")),
                            _dt_log(op_data.get("draw_date")),
                            _dt_log(op_data.get("trxn_post_date")),
                            _dt_log(op_data.get("authorization_date")),
                            _dt_log(op_data.get("doc_date")),
                            op_data.get("operation_amount"),
                            op_data.get("operation_status"),
                            op_data.get("type_of_operation"),
                        )
                    continue
                _, is_created = st_ops_repo.upsert_operation(
                    session,
                    operation_data=op_data,
                    raw_payload=raw,
                )
                if is_created:
                    stats["created"] += 1
                else:
                    stats["existing"] += 1

            if dry_run:
                session.flush()
            else:
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

        if statement_date is None and not dry_run:
            st_ops_repo.update_sync_state(
                session,
                account_number=account_number,
                last_from=_to_utc_naive(from_utc),
                last_to=_to_utc_naive(to_utc),
                last_success_at=_to_utc_naive(to_utc),
            )
            session.commit()
        elif statement_date is None:
            logger.info(
                "DRY-RUN: синк выписки account=%s завершен без обновления last_success_at",
                account_number,
            )
        else:
            logger.info(
                "Ручной синк выписки account=%s from_date=%s завершен без обновления last_success_at",
                account_number,
                statement_date.isoformat(),
            )
        if stats["skipped_out_of_window"]:
            logger.info(
                "Синк выписки account=%s: пропущено вне запрошенного окна %s операций",
                account_number,
                stats["skipped_out_of_window"],
            )
        return stats
    except Exception:
        session.rollback()
        raise
    finally:
        if own_session:
            session.close()


def _build_cashless_expense_sheet_rows(
    operations: list[Any],
    *,
    account_labels: dict[str, str],
    structure_by_code: dict[str, str],
    operation_by_code: dict[str, str],
    fallback_rules: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for operation in operations:
        business_day = _cashless_operation_date(operation)
        amount = _operation_amount_from_row(operation)
        if business_day is None or amount <= 0:
            continue

        account_number = str(operation.account_number or "").strip()
        pay_purpose = _operation_purpose_for_sheet(operation)
        counterparty = _operation_counterparty_for_expense(operation)
        structure_name, operation_name = _parse_pay_purpose_analytics(
            pay_purpose,
            counterparty,
            structure_by_code=structure_by_code,
            operation_by_code=operation_by_code,
            fallback_rules=fallback_rules,
        )
        rows.append(
            {
                "operation_row_id": int(operation.id),
                "month": business_day.month,
                "date": business_day.strftime("%d.%m.%Y"),
                "amount": _format_money_ru(amount),
                "counterparty": counterparty,
                "pay_purpose": pay_purpose,
                "account_label": _account_label(account_number, account_labels),
                "structure": structure_name,
                "operation": operation_name,
            }
        )
    return rows


def _sync_cashless_expenses_to_sheets(
    *,
    limit: int,
    force: bool = False,
    from_date: date | None = None,
) -> dict[str, int]:
    from src.db.connection import get_session
    from src.db.repos import statement_operations as st_ops_repo
    from src.sheets.writer import append_cashless_expense_rows

    session = get_session()
    try:
        operations = st_ops_repo.get_unsynced_cashless_expenses(
            session,
            limit=limit,
            operation_date_from=_cashless_expense_sync_from(from_date),
            include_synced=force,
        )
        stats = {
            "candidates": len(operations),
            "appended": 0,
            "skipped_existing": 0,
            "marked": 0,
        }
        if not operations:
            logger.info(
                "Sheets: нет исходящих операций для листа безналичных расходов (force=%s)",
                force,
            )
            return stats

        rows = _build_cashless_expense_sheet_rows(
            operations,
            account_labels=_get_account_labels(),
            structure_by_code=_load_code_dictionary("structure.json"),
            operation_by_code=_load_code_dictionary("operation.json"),
            fallback_rules=_load_cashless_expense_fallback_rules(),
        )
        if not rows:
            logger.info("Sheets: нет пригодных строк для листа безналичных расходов")
            return stats

        result = append_cashless_expense_rows(rows)
        processed_ids = [int(row_id) for row_id in result.get("processed_operation_ids", []) if row_id]
        if processed_ids:
            stats["marked"] = st_ops_repo.mark_cashless_expenses_sheet_synced(
                session,
                operation_ids=processed_ids,
                synced_at=datetime.utcnow().replace(microsecond=0),
                update_existing=force,
            )
            session.commit()
        else:
            session.rollback()

        stats["appended"] = int(result.get("appended") or 0)
        stats["skipped_existing"] = int(result.get("skipped_existing") or 0)
        return stats
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _build_cashless_income_sheet_rows(
    operations: list[Any],
    *,
    account_labels: dict[str, str],
    default_structure_name: str,
    counterparty_short_names_by_inn: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for operation in operations:
        business_day = _cashless_operation_date(operation)
        amount = _operation_amount_from_row(operation)
        if business_day is None or amount <= 0:
            continue

        account_number = str(operation.account_number or "").strip()
        counterparty_inn = _operation_counterparty_inn_for_income(operation)
        rows.append(
            {
                "operation_row_id": int(operation.id),
                "month": business_day.month,
                "date": business_day.strftime("%d.%m.%Y"),
                "amount": _format_money_ru(amount),
                "counterparty": _operation_counterparty_for_income(operation),
                "pay_purpose": _operation_purpose_for_sheet(operation),
                "account_label": _account_label(account_number, account_labels),
                "structure": default_structure_name,
                "counterparty_short_name": counterparty_short_names_by_inn.get(counterparty_inn, ""),
            }
        )
    return rows


def _sync_cashless_incomes_to_sheets(
    *,
    limit: int,
    force: bool = False,
    from_date: date | None = None,
) -> dict[str, int]:
    from src.db.connection import get_session
    from src.db.repos import counterparties as cp_repo
    from src.db.repos import statement_operations as st_ops_repo
    from src.sheets.writer import append_cashless_income_rows

    session = get_session()
    try:
        operations = st_ops_repo.get_unsynced_cashless_incomes(
            session,
            limit=limit,
            operation_date_from=_cashless_income_sync_from(from_date),
            include_synced=force,
        )
        stats = {
            "candidates": len(operations),
            "appended": 0,
            "skipped_existing": 0,
            "marked": 0,
        }
        if not operations:
            logger.info(
                "Sheets: нет входящих операций для листа безналичных доходов (force=%s)",
                force,
            )
            return stats

        rows = _build_cashless_income_sheet_rows(
            operations,
            account_labels=_get_account_labels(),
            default_structure_name=_default_cashless_structure_name(_load_code_dictionary("structure.json")),
            counterparty_short_names_by_inn=cp_repo.get_short_names_by_inn(
                session,
                [_operation_counterparty_inn_for_income(operation) for operation in operations],
            ),
        )
        if not rows:
            logger.info("Sheets: нет пригодных строк для листа безналичных доходов")
            return stats

        result = append_cashless_income_rows(rows)
        processed_ids = [int(row_id) for row_id in result.get("processed_operation_ids", []) if row_id]
        if processed_ids:
            stats["marked"] = st_ops_repo.mark_cashless_incomes_sheet_synced(
                session,
                operation_ids=processed_ids,
                synced_at=datetime.utcnow().replace(microsecond=0),
                update_existing=force,
            )
            session.commit()
        else:
            session.rollback()

        stats["appended"] = int(result.get("appended") or 0)
        stats["skipped_existing"] = int(result.get("skipped_existing") or 0)
        return stats
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()



def _run_matching(
    unmatched_limit: int,
    *,
    dry_run: bool = False,
    include_paid_backfill: bool = False,
    session: Any | None = None,
    statement_from_utc: datetime | None = None,
) -> tuple[int, dict[str, int], list[dict[str, Any]]]:
    from src.db.repos import invoices as inv_repo
    from src.db.repos import statement_operations as st_ops_repo

    own_session = session is None
    if session is None:
        from src.db.connection import get_session

        session = get_session()
    try:
        unmatched = st_ops_repo.get_unmatched_incoming(
            session,
            limit=unmatched_limit,
            operation_date_from=_to_utc_naive(statement_from_utc) if statement_from_utc else None,
        )
        if statement_from_utc is not None:
            before_filter = len(unmatched)
            unmatched = [
                operation
                for operation in unmatched
                if _operation_on_or_after(operation, statement_from_utc)
            ]
            if before_filter != len(unmatched):
                logger.info(
                    "Фильтр statement-date: из unmatched операций после точной проверки окна оставлено %s из %s",
                    len(unmatched),
                    before_filter,
                )
        if not unmatched and not include_paid_backfill:
            logger.info("Нет новых неприлинкованных входящих операций")
            return 0, {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []

        open_invoices = inv_repo.get_open_for_payment_matching(session)
        paid_context_invoices: list[Any] = []
        paid_backfill_invoices: list[Any] = []
        if include_paid_backfill:
            paid_context_invoices = inv_repo.get_paid_for_payment_backfill(session)
            paid_backfill_invoices = [
                invoice
                for invoice in paid_context_invoices
                if _invoice_needs_payment_backfill(invoice)
            ]
            if paid_backfill_invoices:
                logger.info(
                    "Ручной backfill оплат: найдено paid-счетов с неполными paid_at/paid_amount: %s",
                    len(paid_backfill_invoices),
                )

        candidate_invoices = _merge_invoices_by_id(open_invoices, paid_backfill_invoices)
        allocation_context_invoices = _merge_invoices_by_id(candidate_invoices, paid_context_invoices)
        if not candidate_invoices:
            logger.info("Нет счетов для автозачета оплат")
            return 0, {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []

        matched_incoming = st_ops_repo.get_matched_incoming_for_invoices(
            session,
            invoice_ids=[invoice.id for invoice in candidate_invoices],
        )
        invoice_state = _build_invoice_state(candidate_invoices, matched_incoming)

        paid_backfill_invoice_ids = {int(invoice.id) for invoice in paid_backfill_invoices}
        matched_invoice_ids = {
            int(operation.matched_invoice_id)
            for operation in matched_incoming
            if operation.matched_invoice_id
        }
        matched_window_invoice_ids = {
            int(operation.matched_invoice_id)
            for operation in matched_incoming
            if operation.matched_invoice_id and _operation_on_or_after(operation, statement_from_utc)
        }
        backfill_recalc_ids = {
            invoice_id
            for invoice_id in paid_backfill_invoice_ids & matched_window_invoice_ids
            if _invoice_state_is_fully_paid(invoice_state.get(invoice_id))
        }
        if backfill_recalc_ids:
            logger.info(
                "Ручной backfill оплат: %s paid-счетов уже имеют привязанные операции на полную сумму",
                len(backfill_recalc_ids),
            )

        invoices_by_number: dict[str, list[int]] = defaultdict(list)
        invoices_by_inn: dict[str, list[int]] = defaultdict(list)
        open_invoice_ids: list[int] = []

        for invoice in candidate_invoices:
            entry = invoice_state.get(invoice.id)
            if not entry or _remaining_amount(entry) <= 0:
                continue

            open_invoice_ids.append(invoice.id)
            normalized_number = (invoice.invoice_number or "").strip().lstrip("0") or "0"
            invoices_by_number[normalized_number].append(invoice.id)

            if invoice.counterparty and invoice.counterparty.inn:
                invoices_by_inn[invoice.counterparty.inn.strip()].append(invoice.id)

        allocation_invoice_state = dict(invoice_state)
        for invoice in allocation_context_invoices:
            invoice_id = int(invoice.id)
            if invoice_id in allocation_invoice_state:
                continue
            allocation_invoice_state[invoice_id] = {
                "invoice": invoice,
                "total": _invoice_total(invoice),
                "paid": Decimal("0.00"),
            }
        allocation_invoices_by_number: dict[str, list[int]] = defaultdict(list)
        for invoice in allocation_context_invoices:
            normalized_number = _normalize_invoice_number(invoice.invoice_number)
            allocation_invoices_by_number[normalized_number].append(int(invoice.id))

        if not open_invoice_ids:
            logger.info("Все счета-кандидаты уже полностью оплачены, новых операций матчить некуда")

        multi_invoice_target_invoice_ids = set(open_invoice_ids) | paid_backfill_invoice_ids
        multi_invoice_payment_allocations = _build_multi_invoice_payment_allocations(
            unmatched,
            invoice_state=allocation_invoice_state,
            invoices_by_number=allocation_invoices_by_number,
            target_invoice_ids=multi_invoice_target_invoice_ids,
            preserve_paid_status_ids=paid_backfill_invoice_ids,
        )
        multi_invoice_payment_invoice_ids = set(multi_invoice_payment_allocations)
        multi_invoice_paid_backfill_invoice_ids = multi_invoice_payment_invoice_ids & paid_backfill_invoice_ids
        multi_invoice_normal_invoice_ids = multi_invoice_payment_invoice_ids - paid_backfill_invoice_ids
        if multi_invoice_payment_invoice_ids:
            logger.info(
                (
                    "Мультисчетные платежи: %s счетов будут обновлены по распределенным операциям "
                    "(обычные=%s, paid-backfill=%s)"
                ),
                len(multi_invoice_payment_invoice_ids),
                len(multi_invoice_normal_invoice_ids),
                len(multi_invoice_paid_backfill_invoice_ids),
            )

        matched_count = 0
        touched_invoice_ids: set[int] = set()
        now_utc_naive = datetime.utcnow().replace(microsecond=0)

        operations_to_match = unmatched if open_invoice_ids else []
        for operation in operations_to_match:
            decision = _match_operation_to_invoice(
                operation,
                invoice_state=invoice_state,
                invoices_by_number=invoices_by_number,
                invoices_by_inn=invoices_by_inn,
                open_invoice_ids=open_invoice_ids,
                strict_amount_fallback_invoice_ids=paid_backfill_invoice_ids,
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

        normal_touched_ids = touched_invoice_ids - paid_backfill_invoice_ids
        touched_paid_backfill_ready_ids = {
            invoice_id
            for invoice_id in touched_invoice_ids & paid_backfill_invoice_ids
            if _invoice_state_is_fully_paid(invoice_state.get(invoice_id))
        }
        if touched_paid_backfill_ready_ids:
            logger.info(
                "Ручной backfill оплат: %s paid-счетов доукомплектованы новыми операциями на полную сумму",
                len(touched_paid_backfill_ready_ids),
            )
        paid_backfill_ids_with_direct_operations = (
            touched_invoice_ids | (paid_backfill_invoice_ids & matched_window_invoice_ids)
        ) & paid_backfill_invoice_ids
        partial_paid_backfill_ids = paid_backfill_ids_with_direct_operations - (
            touched_paid_backfill_ready_ids | backfill_recalc_ids
        )
        if partial_paid_backfill_ids:
            logger.info(
                (
                    "Ручной backfill оплат: %s paid-счетов имеют привязанные операции, "
                    "но сумма пока меньше итога счета; обновляем paid_amount/paid_at, статус paid сохраняем"
                ),
                len(partial_paid_backfill_ids),
            )

        paid_backfill_recalc_ids = (
            touched_paid_backfill_ready_ids
            | backfill_recalc_ids
            | partial_paid_backfill_ids
            | multi_invoice_paid_backfill_invoice_ids
        )
        paid_backfill_ids_with_operations = (
            paid_backfill_ids_with_direct_operations | multi_invoice_paid_backfill_invoice_ids
        )
        paid_backfill_without_operations = paid_backfill_invoice_ids - paid_backfill_ids_with_operations
        if paid_backfill_without_operations:
            logger.info(
                "Ручной backfill оплат: %s paid-счетов пока без привязанных операций; paid_at/paid_amount не обновлены",
                len(paid_backfill_without_operations),
            )

        recalc_invoice_ids = normal_touched_ids | multi_invoice_normal_invoice_ids | paid_backfill_recalc_ids
        if recalc_invoice_ids:
            recalc_stats, newly_paid = _recalculate_payment_state(
                session,
                invoice_ids=recalc_invoice_ids,
                preserve_paid_status_ids=paid_backfill_recalc_ids,
                extra_payment_allocations=multi_invoice_payment_allocations,
            )
            if dry_run:
                session.rollback()
                return matched_count, recalc_stats, newly_paid

            session.commit()
            _sync_paid_invoices_to_bitrix(newly_paid)
            return matched_count, recalc_stats, newly_paid

        if matched_count:
            if dry_run:
                session.rollback()
            else:
                session.commit()
            return matched_count, {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []

        session.rollback()
        return 0, {"paid": 0, "partially_paid": 0, "issued": 0, "updated": 0}, []
    except Exception:
        session.rollback()
        raise
    finally:
        if own_session:
            session.close()


def _statement_from_utc(statement_date: date | None) -> datetime | None:
    if statement_date is None:
        return None
    from_naive, _ = _utc_naive_bounds_for_business_date(statement_date)
    return _to_utc_aware(from_naive)



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
    payment_thank_email_limit = _env_int(
        "INVOICE_PAYMENT_THANK_EMAIL_LIMIT",
        _DEFAULT_PAYMENT_THANK_EMAIL_LIMIT,
        min_value=1,
        max_value=100_000,
    )
    cashless_expense_sync_limit = _env_int(
        "GOOGLE_CASHLESS_EXPENSE_SYNC_LIMIT",
        _DEFAULT_CASHLESS_EXPENSE_SYNC_LIMIT,
        min_value=1,
        max_value=100_000,
    )
    cashless_income_sync_limit = _env_int(
        "GOOGLE_CASHLESS_INCOME_SYNC_LIMIT",
        _DEFAULT_CASHLESS_INCOME_SYNC_LIMIT,
        min_value=1,
        max_value=100_000,
    )

    logger.info(
        (
            "Запуск cron_payments accounts=%s initial_lookback_days=%s overlap_minutes=%s "
            "page_limit=%s statement_date=%s payment_thank_email_limit=%s cashless_expense_sync_limit=%s "
            "cashless_income_sync_limit=%s force_cashless_expenses=%s cashless_expenses_from_date=%s "
            "force_cashless_incomes=%s cashless_incomes_from_date=%s "
            "dry_run=%s dry_run_bitrix=%s"
        ),
        len(account_numbers),
        initial_lookback_days,
        overlap_minutes,
        page_limit,
        args.statement_date.isoformat() if args.statement_date else None,
        payment_thank_email_limit,
        cashless_expense_sync_limit,
        cashless_income_sync_limit,
        args.force_cashless_expenses,
        args.cashless_expenses_from_date.isoformat() if args.cashless_expenses_from_date else None,
        args.force_cashless_incomes,
        args.cashless_incomes_from_date.isoformat() if args.cashless_incomes_from_date else None,
        args.dry_run,
        args.dry_run_bitrix,
    )

    if args.dry_run:
        total_fetched = 0
        total_created = 0
        total_existing = 0
        total_skipped_out_of_window = 0

        if args.statement_date:
            from src.db.connection import get_session

            logger.warning(
                (
                    "DRY-RUN: запрашиваем выписку T-Bank и выполняем предпросмотр матчинга "
                    "в одной транзакции; изменения в БД будут отменены"
                )
            )
            preview_session = get_session()
            sync_errors: list[str] = []
            try:
                for account_number in account_numbers:
                    try:
                        sync_stats = _sync_statement_for_account(
                            account_number=account_number,
                            initial_lookback_days=initial_lookback_days,
                            overlap_minutes=overlap_minutes,
                            page_limit=page_limit,
                            statement_date=args.statement_date,
                            dry_run=True,
                            session=preview_session,
                        )
                        fetched = sync_stats.get("fetched", 0)
                        created = sync_stats.get("created", 0)
                        existing = sync_stats.get("existing", 0)
                        skipped_out_of_window = sync_stats.get("skipped_out_of_window", 0)
                        total_fetched += fetched
                        total_created += created
                        total_existing += existing
                        total_skipped_out_of_window += skipped_out_of_window
                        logger.info(
                            (
                                "DRY-RUN: синк выписки account=%s завершен: "
                                "fetched=%s created=%s existing=%s skipped_out_of_window=%s"
                            ),
                            account_number,
                            fetched,
                            created,
                            existing,
                            skipped_out_of_window,
                        )
                    except Exception as e:
                        sync_errors.append(f"{account_number}: {e}")
                        logger.exception("DRY-RUN: ошибка синка выписки account=%s", account_number)

                if sync_errors:
                    logger.error("DRY-RUN: синк выписки завершился с ошибками: %s", sync_errors)
                    preview_session.rollback()
                    sys.exit(1)

                matched_count, recalc_stats, newly_paid = _run_matching(
                    unmatched_limit,
                    dry_run=True,
                    include_paid_backfill=True,
                    session=preview_session,
                    statement_from_utc=_statement_from_utc(args.statement_date),
                )
            finally:
                preview_session.close()
        else:
            logger.warning(
                "DRY-RUN: синк выписки, расходов и доходов в Sheets отключен; используем только текущие данные в БД"
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
        logger.info("DRY-RUN: sync расходов/доходов в Sheets и thank-you email отключены")

        logger.info(
            (
                "cron_payments DRY-RUN завершен: fetched=%s created=%s existing=%s "
                "skipped_out_of_window=%s matched=%s "
                "invoice_state_updates=%s (paid=%s partially_paid=%s issued=%s)"
            ),
            total_fetched,
            total_created,
            total_existing,
            total_skipped_out_of_window,
            matched_count,
            recalc_stats.get("updated", 0),
            recalc_stats.get("paid", 0),
            recalc_stats.get("partially_paid", 0),
            recalc_stats.get("issued", 0),
        )
        return

    total_fetched = 0
    total_created = 0
    total_existing = 0
    total_skipped_out_of_window = 0
    sync_errors: list[str] = []

    for account_number in account_numbers:
        try:
            sync_stats = _sync_statement_for_account(
                account_number=account_number,
                initial_lookback_days=initial_lookback_days,
                overlap_minutes=overlap_minutes,
                page_limit=page_limit,
                statement_date=args.statement_date,
            )
            fetched = sync_stats.get("fetched", 0)
            created = sync_stats.get("created", 0)
            existing = sync_stats.get("existing", 0)
            skipped_out_of_window = sync_stats.get("skipped_out_of_window", 0)
            total_fetched += fetched
            total_created += created
            total_existing += existing
            total_skipped_out_of_window += skipped_out_of_window
            logger.info(
                "Синк выписки account=%s завершен: fetched=%s created=%s existing=%s skipped_out_of_window=%s",
                account_number,
                fetched,
                created,
                existing,
                skipped_out_of_window,
            )
        except Exception as e:
            sync_errors.append(f"{account_number}: {e}")
            logger.exception("Ошибка синка выписки account=%s", account_number)

    if sync_errors:
        logger.error("Синк выписки завершился с ошибками: %s", sync_errors)
        sys.exit(1)

    cashless_expense_stats = {
        "candidates": 0,
        "appended": 0,
        "skipped_existing": 0,
        "marked": 0,
        "failed": 0,
    }
    try:
        cashless_expense_stats.update(
            _sync_cashless_expenses_to_sheets(
                limit=cashless_expense_sync_limit,
                force=args.force_cashless_expenses,
                from_date=args.cashless_expenses_from_date,
            )
        )
    except Exception:
        cashless_expense_stats["failed"] = 1
        logger.exception("Ошибка синхронизации расходов в Sheets")

    cashless_income_stats = {
        "candidates": 0,
        "appended": 0,
        "skipped_existing": 0,
        "marked": 0,
        "failed": 0,
    }
    try:
        cashless_income_stats.update(
            _sync_cashless_incomes_to_sheets(
                limit=cashless_income_sync_limit,
                force=args.force_cashless_incomes,
                from_date=args.cashless_incomes_from_date,
            )
        )
    except Exception:
        cashless_income_stats["failed"] = 1
        logger.exception("Ошибка синхронизации доходов в Sheets")

    matched_count, recalc_stats, newly_paid = _run_matching(
        unmatched_limit,
        include_paid_backfill=args.statement_date is not None,
        statement_from_utc=_statement_from_utc(args.statement_date),
    )
    payment_thank_stats = _send_due_payment_thank_you_emails(
        limit=payment_thank_email_limit,
        newly_paid_invoice_ids=[int(item["invoice_id"]) for item in newly_paid],
    )

    logger.info(
        (
            "cron_payments завершен: fetched=%s created=%s existing=%s skipped_out_of_window=%s matched=%s "
            "invoice_state_updates=%s (paid=%s partially_paid=%s issued=%s) "
            "cashless_expenses_candidates=%s appended=%s skipped_existing=%s marked=%s failed=%s "
            "cashless_incomes_candidates=%s appended=%s skipped_existing=%s marked=%s failed=%s "
            "payment_thanks_candidates=%s sent=%s failed=%s skipped=%s"
        ),
        total_fetched,
        total_created,
        total_existing,
        total_skipped_out_of_window,
        matched_count,
        recalc_stats.get("updated", 0),
        recalc_stats.get("paid", 0),
        recalc_stats.get("partially_paid", 0),
        recalc_stats.get("issued", 0),
        cashless_expense_stats.get("candidates", 0),
        cashless_expense_stats.get("appended", 0),
        cashless_expense_stats.get("skipped_existing", 0),
        cashless_expense_stats.get("marked", 0),
        cashless_expense_stats.get("failed", 0),
        cashless_income_stats.get("candidates", 0),
        cashless_income_stats.get("appended", 0),
        cashless_income_stats.get("skipped_existing", 0),
        cashless_income_stats.get("marked", 0),
        cashless_income_stats.get("failed", 0),
        payment_thank_stats.get("candidates", 0),
        payment_thank_stats.get("sent", 0),
        payment_thank_stats.get("failed", 0),
        payment_thank_stats.get("skipped", 0),
    )


if __name__ == "__main__":
    main()
