"""
Синхронизация данных из Google Sheets в MySQL.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from sqlalchemy.orm import Session

from src.db.connection import get_session
from src.db.repos import counterparties as cp_repo
from src.db.repos import works as works_repo
from src.db.repos import works_files as works_files_repo
from src.sheets.reader import read_counterparties, read_works

logger = logging.getLogger(__name__)
_VALID_INN_LENGTHS = {10, 12}
_BOOL_TRUE_VALUES = {
    "1",
    "true",
    "yes",
    "y",
    "да",
    "д",
    "on",
    "+",
    "вкл",
    "включено",
    "enabled",
}
_BOOL_FALSE_VALUES = {
    "0",
    "false",
    "no",
    "n",
    "нет",
    "н",
    "off",
    "-",
    "выкл",
    "выключено",
    "disabled",
}
_VOLUME_Q = Decimal("0.01")
_CONTAINER_VOLUME_M3 = Decimal("8.00")
_TRIP_REMOVAL_STRUCTURES = {
    "юл - вывоз мусора",
    "фл - вывоз мусора",
}
_CONTAINER_STRUCTURES = {
    "юл - контейнеры",
}
_VOLUME_NOTE_RE = re.compile(
    r"(?P<prefix>^|[\s,;])"
    r"об[ъь]?[её]м\s*[:=\-]?\s*"
    r"(?P<value>\d+(?:[,.]\d+)?)\s*"
    r"(?:м\s*(?:3|³)|м\^?3|куб\.?\s*м\.?)?",
    re.IGNORECASE,
)


def _parse_date(date_str: str) -> date | None:
    """Парсинг даты DD.MM.YYYY в date."""
    try:
        return datetime.strptime(date_str.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None


def _parse_revenue(value: str | None) -> Decimal | None:
    """Парсинг суммы выручки в Decimal(14,2)."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    cleaned = raw.replace("\u00a0", "").replace(" ", "")
    cleaned = cleaned.replace("₽", "")
    cleaned = re.sub(r"[^\d,.\-]", "", cleaned)
    if not cleaned:
        return None

    has_comma = "," in cleaned
    has_dot = "." in cleaned
    if has_comma and has_dot:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif has_comma:
        cleaned = cleaned.replace(",", ".")

    if cleaned.count(".") > 1:
        parts = cleaned.split(".")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return Decimal(cleaned).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None


def _normalize_structure(value: str | None) -> str:
    return str(value or "").strip().lower().replace("ё", "е")


def _parse_positive_decimal(value: str | None, *, default: Decimal) -> Decimal:
    raw = str(value or "").strip().replace(",", ".")
    if not raw:
        return default
    try:
        parsed = Decimal(raw)
    except (InvalidOperation, ValueError):
        return default
    if parsed <= 0:
        return Decimal("0.01")
    return parsed


def _parse_volume_note(note: str | None) -> tuple[str, Decimal | None]:
    """Извлекает 'Объем: 10м3' из примечания и возвращает очищенное примечание."""
    raw_note = str(note or "").strip()
    match = _VOLUME_NOTE_RE.search(raw_note)
    if match is None:
        return raw_note, None

    volume = _parse_positive_decimal(match.group("value"), default=Decimal("0.00"))
    parsed_volume = volume.quantize(_VOLUME_Q, rounding=ROUND_HALF_UP) if volume > 0 else None

    clean_note = _VOLUME_NOTE_RE.sub(" ", raw_note, count=1)
    clean_note = re.sub(r"\s{2,}", " ", clean_note).strip(" ,;:-")
    return clean_note, parsed_volume


def _prepare_work_note_and_volume(row: dict) -> tuple[str, Decimal | None]:
    """Готовит примечание и объем для записи в works."""
    structure = _normalize_structure(row.get("structure"))
    note = str(row.get("note") or "").strip()

    if structure in _TRIP_REMOVAL_STRUCTURES:
        return _parse_volume_note(note)

    if structure in _CONTAINER_STRUCTURES:
        amount = _parse_positive_decimal(
            row.get("object_count"),
            default=Decimal("1.00"),
        )
        volume = (amount * _CONTAINER_VOLUME_M3).quantize(_VOLUME_Q, rounding=ROUND_HALF_UP)
        return note, volume

    return note, None


def _digits_only(value: str | None) -> str:
    return re.sub(r"\D", "", str(value or ""))


def _normalize_inn(value: str | None) -> str:
    inn = _digits_only(value)
    if not inn:
        return ""
    if len(inn) not in _VALID_INN_LENGTHS:
        return ""
    return inn


def _normalize_kpp(value: str | None) -> str:
    kpp = _digits_only(value)
    if not kpp:
        return ""
    if set(kpp) == {"0"}:
        return ""
    if len(kpp) != 9:
        return ""
    return kpp


def _normalize_email_list(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parts = [part.strip() for part in re.split(r"[;,]", raw) if part and part.strip()]
    return ", ".join(parts)


def _parse_optional_bool(value) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    raw = str(value or "").strip().lower().replace("ё", "е")
    if not raw:
        return None
    if raw in _BOOL_TRUE_VALUES:
        return True
    if raw in _BOOL_FALSE_VALUES:
        return False
    try:
        return int(raw) != 0
    except ValueError:
        return None


def _sync_counterparties_rows(session: Session, rows: list[dict]) -> tuple[int, int, int]:
    """
    Upsert контрагентов в counterparties по данным из Sheets.

    Возвращает (created, updated, skipped).
    """
    created = 0
    updated = 0
    skipped = 0

    for row in rows:
        raw_name = str(row.get("name", "") or "").strip()
        raw_short_name = str(row.get("short_name", "") or "").strip()
        raw_inn = str(row.get("inn", "") or "").strip()
        raw_kpp = str(row.get("kpp", "") or "").strip()
        raw_email = str(row.get("email", "") or "").strip()
        raw_email_accountant = str(row.get("email_accountant", "") or "").strip()
        raw_contract = str(row.get("contract", "") or "").strip()
        raw_payment_reminders_enabled = row.get("payment_reminders_enabled", "")

        inn = _normalize_inn(raw_inn)
        if not inn:
            skipped += 1
            logger.warning(
                "Синхронизация контрагентов: пропуск строки с невалидным ИНН '%s' (short_name='%s', name='%s')",
                raw_inn,
                raw_short_name,
                raw_name,
            )
            continue

        if not raw_short_name or not raw_name:
            skipped += 1
            logger.warning(
                "Синхронизация контрагентов: пропуск строки без short_name/name (inn='%s')",
                inn,
            )
            continue

        kpp = _normalize_kpp(raw_kpp)
        raw_kpp_digits = _digits_only(raw_kpp)
        if raw_kpp and raw_kpp_digits and set(raw_kpp_digits) != {"0"} and not kpp:
            logger.warning(
                "Синхронизация контрагентов: КПП '%s' для ИНН %s невалиден, сохраняем пустым",
                raw_kpp,
                inn,
            )
        email = _normalize_email_list(raw_email)
        email_accountant = _normalize_email_list(raw_email_accountant)
        contract = raw_contract
        payment_reminders_enabled = _parse_optional_bool(raw_payment_reminders_enabled)
        if str(raw_payment_reminders_enabled or "").strip() and payment_reminders_enabled is None:
            logger.warning(
                "Синхронизация контрагентов: не удалось распарсить payment_reminders_enabled='%s' для ИНН %s, значение не меняем",
                raw_payment_reminders_enabled,
                inn,
            )

        by_inn = cp_repo.get_by_inn(session, inn)
        by_short_name = cp_repo.get_by_short_name(session, raw_short_name, "")

        if by_inn and by_short_name and by_inn.id != by_short_name.id:
            skipped += 1
            logger.warning(
                "Синхронизация контрагентов: конфликт данных для inn=%s и short_name='%s' (разные записи id=%s и id=%s), строка пропущена",
                inn,
                raw_short_name,
                by_inn.id,
                by_short_name.id,
            )
            continue

        cp = by_inn or by_short_name
        if cp is None:
            cp_repo.create(
                session,
                name=raw_name,
                short_name=raw_short_name,
                inn=inn,
                kpp=kpp,
                email=email,
                email_accountant=email_accountant,
                payment_reminders_enabled=(
                    payment_reminders_enabled
                    if payment_reminders_enabled is not None
                    else True
                ),
                contract=contract,
            )
            created += 1
            continue

        changed = False
        if cp.name != raw_name:
            cp.name = raw_name
            changed = True
        if cp.short_name != raw_short_name:
            cp.short_name = raw_short_name
            changed = True
        if cp.inn != inn:
            cp.inn = inn
            changed = True
        if (cp.kpp or "") != kpp:
            cp.kpp = kpp
            changed = True
        if (cp.email or "") != email:
            cp.email = email
            changed = True
        if (cp.email_accountant or "") != email_accountant:
            cp.email_accountant = email_accountant
            changed = True
        if (cp.contract or "") != contract:
            cp.contract = contract or None
            changed = True
        if (
            payment_reminders_enabled is not None
            and bool(cp.payment_reminders_enabled) != payment_reminders_enabled
        ):
            cp.payment_reminders_enabled = payment_reminders_enabled
            changed = True

        if changed:
            session.flush()
            updated += 1

    return created, updated, skipped


def _link_waybill_file(session: Session, row: dict, work_id: int) -> str:
    token = str(row.get("waybill_file_token") or "").strip()
    if not token:
        return "none"

    work_file = works_files_repo.link_to_work_by_token(
        session,
        file_token=token,
        work_id=work_id,
    )
    if work_file is None:
        logger.warning(
            "Синхронизация: в строке hash=%s указан путевой лист %s, но файл не найден в works_files",
            row.get("sheet_row_hash"),
            token,
        )
        return "missing"
    if work_file.work_id != work_id:
        logger.warning(
            "Синхронизация: путевой лист %s уже привязан к work_id=%s, текущая работа work_id=%s",
            token,
            work_file.work_id,
            work_id,
        )
        return "conflict"
    return "linked"


def sync_sheets_to_mysql(
    sheet_url: str | None = None,
    sheet_name: str | None = None,
    from_date: date | None = None,
    counterparties_sheet_name: str | None = None,
) -> int:
    """
    Синхронизация данных из Google Sheets в MySQL.

    1) Контрагенты (лист `Контрагенты`) → таблица counterparties (upsert).
    2) Работы (основной лист) → таблица works.

    Для работ читаются строки с датой >= from_date, если он задан, иначе >= последней импортированной.
    Для работ применяется дедупликация по sheet_row_hash.
    Возвращает количество добавленных строк.
    """
    session = get_session()
    try:
        cp_rows = read_counterparties(
            sheet_url=sheet_url,
            sheet_name=counterparties_sheet_name,
        )
        cp_created, cp_updated, cp_skipped = _sync_counterparties_rows(session, cp_rows)

        last_date = from_date or works_repo.get_max_date(session)
        rows = read_works(sheet_url=sheet_url, sheet_name=sheet_name, last_date=last_date)

        added = 0
        revenue_updated = 0
        volume_updated = 0
        waybill_linked = 0
        waybill_missing = 0
        waybill_conflicts = 0
        for row in rows:
            parsed_revenue = _parse_revenue(row.get("revenue"))
            note, parsed_volume = _prepare_work_note_and_volume(row)
            parsed_date = _parse_date(row["date"])
            if row.get("revenue") and parsed_revenue is None:
                logger.warning(
                    "Синхронизация: не удалось распарсить выручку '%s' (hash=%s)",
                    row.get("revenue"),
                    row.get("sheet_row_hash"),
                )

            if parsed_date is None:
                logger.warning(
                    "Синхронизация: пропуск строки с невалидной датой '%s' (hash=%s)",
                    row.get("date"),
                    row.get("sheet_row_hash"),
                )
                continue

            existing_work = works_repo.get_by_hash(session, row["sheet_row_hash"])
            if existing_work is not None:
                existing_work.note = note.strip() or None
                if parsed_revenue is not None:
                    existing_work.revenue = parsed_revenue
                    revenue_updated += 1
                if parsed_volume is not None:
                    existing_work.volume = parsed_volume
                    volume_updated += 1
                session.flush()
                link_status = _link_waybill_file(session, row, existing_work.id)
                if link_status == "linked":
                    waybill_linked += 1
                elif link_status == "missing":
                    waybill_missing += 1
                elif link_status == "conflict":
                    waybill_conflicts += 1
                continue

            work = works_repo.create(
                session,
                date=parsed_date,
                counterparty_name=row["counterparty_name"],
                note=note,
                structure=row["structure"],
                operation=row["operation"],
                object_count=row["object_count"],
                volume=parsed_volume,
                revenue=parsed_revenue,
                sheet_row_hash=row["sheet_row_hash"],
            )
            link_status = _link_waybill_file(session, row, work.id)
            if link_status == "linked":
                waybill_linked += 1
            elif link_status == "missing":
                waybill_missing += 1
            elif link_status == "conflict":
                waybill_conflicts += 1
            added += 1

        session.commit()
        logger.info(
            "Синхронизация: контрагенты — создано %s, обновлено %s, пропущено %s; работы — добавлено %s, обновлено выручки %s, обновлено объема %s; путевые листы — привязано %s, не найдено %s, конфликты %s (обработано строк: %s)",
            cp_created,
            cp_updated,
            cp_skipped,
            added,
            revenue_updated,
            volume_updated,
            waybill_linked,
            waybill_missing,
            waybill_conflicts,
            len(rows),
        )
        return added
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
