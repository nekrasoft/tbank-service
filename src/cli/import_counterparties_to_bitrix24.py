"""
CLI: импорт контрагентов из MySQL в Bitrix24 CRM.
Запуск: python3 -m src.cli.import_counterparties_to_bitrix24
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
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

_DEFAULT_DELAY_SEC = 0.35
_CUSTOM_FIELD_ENV_TO_ATTR = {
    "BITRIX24_COMPANY_SHORT_NAME_FIELD": "short_name",
    "BITRIX24_COMPANY_INN_FIELD": "inn",
    "BITRIX24_COMPANY_KPP_FIELD": "kpp",
    "BITRIX24_COMPANY_NOTE_FIELD": "note",
    "BITRIX24_COMPANY_INVOICE_SCHEDULE_FIELD": "invoice_schedule",
}


def _parse_int(value) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _get_delay_sec() -> float:
    raw = (os.environ.get("BITRIX24_IMPORT_DELAY_SEC") or "").strip()
    if not raw:
        return _DEFAULT_DELAY_SEC
    try:
        delay = float(raw)
    except ValueError:
        logger.warning(
            "Некорректный BITRIX24_IMPORT_DELAY_SEC='%s', используем %.2f",
            raw,
            _DEFAULT_DELAY_SEC,
        )
        return _DEFAULT_DELAY_SEC
    return max(delay, 0.0)


def _build_comments(counterparty) -> str:
    lines = [
        "Импорт из tbank-service",
        f"short_name: {counterparty.short_name}",
        f"inn: {counterparty.inn}",
        f"invoice_schedule: {counterparty.invoice_schedule or 'monthly'}",
    ]
    if counterparty.kpp:
        lines.append(f"kpp: {counterparty.kpp}")
    if counterparty.note:
        lines.append(f"note: {counterparty.note}")
    return "\n".join(lines)


def _build_custom_fields(counterparty) -> dict[str, str]:
    fields: dict[str, str] = {}
    for env_name, attr_name in _CUSTOM_FIELD_ENV_TO_ATTR.items():
        bitrix_field = (os.environ.get(env_name) or "").strip()
        if not bitrix_field:
            continue
        value = getattr(counterparty, attr_name)
        if value in (None, ""):
            continue
        fields[bitrix_field] = str(value)
    return fields


def _build_lookup_filters(counterparty) -> list[tuple[str, dict[str, str]]]:
    """Формирует фильтры поиска компании в Bitrix24 по приоритету точности."""
    inn_field = (os.environ.get("BITRIX24_COMPANY_INN_FIELD") or "").strip()
    kpp_field = (os.environ.get("BITRIX24_COMPANY_KPP_FIELD") or "").strip()
    short_name_field = (os.environ.get("BITRIX24_COMPANY_SHORT_NAME_FIELD") or "").strip()

    filters: list[tuple[str, dict[str, str]]] = []
    if inn_field and counterparty.inn and kpp_field and counterparty.kpp:
        filters.append(
            (
                "custom INN+KPP",
                {
                    inn_field: str(counterparty.inn),
                    kpp_field: str(counterparty.kpp),
                },
            )
        )
    if inn_field and counterparty.inn:
        filters.append(("custom INN", {inn_field: str(counterparty.inn)}))
    if short_name_field and counterparty.short_name:
        filters.append(("custom short_name", {short_name_field: str(counterparty.short_name)}))

    # Fallback: точное совпадение имени компании.
    if counterparty.name:
        filters.append(("TITLE", {"TITLE": str(counterparty.name)}))

    unique_filters: list[tuple[str, dict[str, str]]] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for reason, filter_fields in filters:
        key = tuple(sorted((str(k), str(v)) for k, v in filter_fields.items()))
        if key in seen:
            continue
        seen.add(key)
        unique_filters.append((reason, filter_fields))
    return unique_filters


def _find_existing_company_id(counterparty) -> tuple[int | None, str | None]:
    from src.bitrix.client import list_companies

    for reason, filter_fields in _build_lookup_filters(counterparty):
        companies = list_companies(
            filter_fields=filter_fields,
            select_fields=["ID", "TITLE"],
            order_fields={"ID": "ASC"},
            limit=2,
        )
        company_ids = [_parse_int(company.get("ID")) for company in companies]
        company_ids = [company_id for company_id in company_ids if company_id is not None]
        if not company_ids:
            continue

        if len(company_ids) > 1:
            logger.warning(
                "Для '%s' найдено несколько компаний по фильтру %s=%s, используем ID=%s",
                counterparty.short_name,
                reason,
                filter_fields,
                company_ids[0],
            )
        return company_ids[0], reason

    return None, None


def _load_counterparties(limit: int | None, short_names: list[str] | None):
    from src.db.connection import get_session
    from src.db.repos import counterparties as cp_repo

    session = get_session()
    try:
        counterparties = cp_repo.get_all(session)
    finally:
        session.close()

    if short_names:
        wanted = {name.strip() for name in short_names if name.strip()}
        counterparties = [cp for cp in counterparties if cp.short_name in wanted]

    if limit is not None and limit > 0:
        counterparties = counterparties[:limit]

    return counterparties


def main() -> None:
    parser = argparse.ArgumentParser(description="Импорт контрагентов в Bitrix24 CRM")
    parser.add_argument("--dry-run", action="store_true", help="Не отправлять запросы, только показать что будет импортировано")
    parser.add_argument("--limit", type=int, default=None, help="Ограничить количество контрагентов")
    parser.add_argument(
        "--short-name",
        action="append",
        default=None,
        help="Импортировать только указанный short_name (можно передать несколько раз)",
    )
    parser.add_argument("--stop-on-error", action="store_true", help="Остановить импорт при первой ошибке")
    args = parser.parse_args()

    from src.bitrix.client import add_company

    if not args.dry_run and not (os.environ.get("BITRIX24_WEBHOOK_URL") or "").strip():
        logger.error("Не задан BITRIX24_WEBHOOK_URL в .env")
        sys.exit(1)

    delay_sec = _get_delay_sec()
    counterparties = _load_counterparties(limit=args.limit, short_names=args.short_name)
    if not counterparties:
        logger.info("Контрагенты для импорта не найдены")
        return

    logger.info("Найдено контрагентов для импорта: %s", len(counterparties))
    if args.dry_run:
        logger.info("Режим DRY-RUN: запросы в Bitrix24 отправляться не будут")

    created = 0
    skipped_existing = 0
    planned = 0
    failed = 0
    total = len(counterparties)

    for index, cp in enumerate(counterparties, start=1):
        custom_fields = _build_custom_fields(cp)

        if args.dry_run:
            logger.info(
                "[%s/%s] DRY-RUN: short_name='%s', title='%s', custom_fields=%s",
                index,
                total,
                cp.short_name,
                cp.name,
                custom_fields,
            )
            planned += 1
            continue

        try:
            existing_id, matched_by = _find_existing_company_id(cp)
            if existing_id is not None:
                skipped_existing += 1
                logger.info(
                    "[%s/%s] Уже существует '%s' (short_name='%s'), company_id=%s, match=%s",
                    index,
                    total,
                    cp.name,
                    cp.short_name,
                    existing_id,
                    matched_by,
                )
            else:
                company_id = add_company(
                    title=cp.name,
                    email=cp.email,
                    phone=cp.phone,
                    comments=_build_comments(cp),
                    custom_fields=custom_fields,
                )
                created += 1
                logger.info(
                    "[%s/%s] Импортирован '%s' (short_name='%s'), company_id=%s",
                    index,
                    total,
                    cp.name,
                    cp.short_name,
                    company_id,
                )
        except Exception as e:
            failed += 1
            logger.error(
                "[%s/%s] Ошибка импорта '%s' (short_name='%s'): %s",
                index,
                total,
                cp.name,
                cp.short_name,
                e,
            )
            if args.stop_on_error:
                break

        if delay_sec > 0 and index < total:
            time.sleep(delay_sec)

    if args.dry_run:
        logger.info("DRY-RUN завершён. Проверено: %s, всего: %s", planned, total)
    else:
        logger.info(
            "Импорт завершён. Создано: %s, пропущено (уже есть): %s, ошибок: %s, всего: %s",
            created,
            skipped_existing,
            failed,
            total,
        )

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
