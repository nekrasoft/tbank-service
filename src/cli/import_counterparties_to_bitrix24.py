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

    imported = 0
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
            imported += 1
            continue

        try:
            company_id = add_company(
                title=cp.name,
                email=cp.email,
                phone=cp.phone,
                comments=_build_comments(cp),
                custom_fields=custom_fields,
            )
            imported += 1
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

    logger.info(
        "Импорт завершён. Успешно: %s, ошибок: %s, всего: %s",
        imported,
        failed,
        total,
    )

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
