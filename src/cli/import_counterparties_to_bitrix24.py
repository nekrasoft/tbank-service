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
_BITRIX_ENTITY_TYPE_COMPANY = 4
_DEFAULT_REQUISITE_COUNTRY_ID = 1
_BITRIX_COMPANY_CONTRACT_FIELD = "UF_CRM_1667795999022"
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


def _parse_positive_int(value) -> int | None:
    parsed = _parse_int(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


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
        f"invoice_schedule: {counterparty.invoice_schedule or '2weeks'}",
    ]
    if counterparty.kpp:
        lines.append(f"kpp: {counterparty.kpp}")
    if counterparty.note:
        lines.append(f"note: {counterparty.note}")
    if getattr(counterparty, "contract", None):
        lines.append(f"contract: {counterparty.contract}")
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
    contract_value = str(getattr(counterparty, "contract", "") or "").strip()
    if contract_value:
        fields[_BITRIX_COMPANY_CONTRACT_FIELD] = contract_value
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


def _resolve_requisite_preset_id() -> int:
    """
    Определяет preset_id для crm.requisite.add.

    Приоритет:
    1) BITRIX24_REQUISITE_PRESET_ID
    2) Поиск по BITRIX24_REQUISITE_COUNTRY_ID (default 1)
    """
    from src.bitrix.client import list_requisite_preset_fields, list_requisite_presets

    explicit = _parse_positive_int(os.environ.get("BITRIX24_REQUISITE_PRESET_ID"))
    if explicit is not None:
        return explicit

    country_id = _parse_positive_int(os.environ.get("BITRIX24_REQUISITE_COUNTRY_ID")) or _DEFAULT_REQUISITE_COUNTRY_ID
    presets = list_requisite_presets(
        filter_fields={"COUNTRY_ID": country_id, "ACTIVE": "Y"},
        select_fields=["ID", "NAME", "COUNTRY_ID", "ACTIVE", "SORT"],
        order_fields={"SORT": "ASC"},
        limit=100,
    )
    if not presets:
        presets = list_requisite_presets(
            filter_fields={"ACTIVE": "Y"},
            select_fields=["ID", "NAME", "COUNTRY_ID", "ACTIVE", "SORT"],
            order_fields={"SORT": "ASC"},
            limit=100,
        )
    if not presets:
        raise RuntimeError("Bitrix24: не найдено активных шаблонов реквизитов (crm.requisite.preset.list)")

    # Предпочитаем шаблон, где присутствуют оба поля: RQ_INN и RQ_KPP.
    for preset in presets:
        preset_id = _parse_positive_int(preset.get("ID"))
        if preset_id is None:
            continue
        preset_fields = list_requisite_preset_fields(preset_id=preset_id)
        field_names = {str(item.get("FIELD_NAME")) for item in preset_fields}
        if "RQ_INN" in field_names and "RQ_KPP" in field_names:
            return preset_id

    fallback_id = _parse_positive_int(presets[0].get("ID"))
    if fallback_id is None:
        raise RuntimeError(f"Bitrix24: не удалось извлечь ID шаблона реквизитов: {presets[0]}")
    logger.warning(
        "Bitrix24: не найден preset с RQ_INN/RQ_KPP, используем первый доступный preset_id=%s",
        fallback_id,
    )
    return fallback_id


def _ensure_company_requisite(*, counterparty, company_id: int, preset_id: int) -> tuple[str, int | None]:
    """
    Гарантирует наличие реквизита компании с INN/KPP.

    Возвращает:
    - ("created", requisite_id)
    - ("updated", requisite_id)
    - ("exists", requisite_id)
    - ("skipped", None)
    """
    from src.bitrix.client import add_requisite, list_requisites, update_requisite

    inn = str(counterparty.inn or "").strip()
    kpp = str(counterparty.kpp or "").strip()
    if not inn and not kpp:
        return "skipped", None

    requisites = list_requisites(
        filter_fields={"ENTITY_TYPE_ID": _BITRIX_ENTITY_TYPE_COMPANY, "ENTITY_ID": int(company_id)},
        select_fields=["ID", "NAME", "RQ_INN", "RQ_KPP"],
        order_fields={"ID": "ASC"},
    )

    # 1) Уже есть точное совпадение — ничего не делаем.
    for req in requisites:
        req_id = _parse_positive_int(req.get("ID"))
        req_inn = str(req.get("RQ_INN") or "").strip()
        req_kpp = str(req.get("RQ_KPP") or "").strip()
        if req_id is None:
            continue
        if req_inn == inn and (not kpp or req_kpp == kpp):
            return "exists", req_id

    # 2) Если есть реквизит с тем же INN или без INN — обновляем его.
    candidate_id: int | None = None
    candidate_inn = ""
    candidate_kpp = ""
    for req in requisites:
        req_id = _parse_positive_int(req.get("ID"))
        req_inn = str(req.get("RQ_INN") or "").strip()
        req_kpp = str(req.get("RQ_KPP") or "").strip()
        if req_id is None:
            continue
        if not req_inn or req_inn == inn:
            candidate_id = req_id
            candidate_inn = req_inn
            candidate_kpp = req_kpp
            break

    if candidate_id is not None:
        update_fields: dict[str, str] = {}
        if inn and candidate_inn != inn:
            update_fields["RQ_INN"] = inn
        if kpp and candidate_kpp != kpp:
            update_fields["RQ_KPP"] = kpp
        if not update_fields:
            return "exists", candidate_id
        updated = update_requisite(requisite_id=candidate_id, fields=update_fields)
        if not updated:
            raise RuntimeError(f"Bitrix24: crm.requisite.update вернул false для id={candidate_id}")
        return "updated", candidate_id

    # 3) Реквизитов нет/не подходят — создаём новый.
    requisite_name = f"Реквизиты {counterparty.short_name}".strip()[:255]
    created_id = add_requisite(
        entity_type_id=_BITRIX_ENTITY_TYPE_COMPANY,
        entity_id=int(company_id),
        preset_id=int(preset_id),
        name=requisite_name,
        rq_inn=inn,
        rq_kpp=kpp or None,
    )
    return "created", created_id


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


def _save_company_binding(counterparty_id: int, company_id: int) -> None:
    """Сохраняет ID компании Bitrix24 в counterparties.bitrix_company_id."""
    from src.db.connection import get_session
    from src.db.repos import counterparties as cp_repo

    session = get_session()
    try:
        updated = cp_repo.update_bitrix_company_id(
            session,
            counterparty_id=int(counterparty_id),
            bitrix_company_id=int(company_id),
        )
        if updated != 1:
            raise RuntimeError(
                f"Контрагент id={counterparty_id} не найден для сохранения bitrix_company_id",
            )
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


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

    from src.bitrix.client import add_company, update_company

    if not args.dry_run and not (os.environ.get("BITRIX24_WEBHOOK_URL") or "").strip():
        logger.error("Не задан BITRIX24_WEBHOOK_URL в .env")
        sys.exit(1)

    requisite_preset_id: int | None = None
    if not args.dry_run:
        try:
            requisite_preset_id = _resolve_requisite_preset_id()
        except Exception as e:
            logger.error("Не удалось определить preset реквизитов Bitrix24: %s", e)
            sys.exit(1)
        logger.info("Используется preset реквизитов Bitrix24: %s", requisite_preset_id)

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
    requisites_created = 0
    requisites_updated = 0
    requisites_skipped = 0
    bindings_saved = 0
    comments_synced = 0
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
            comment_text = _build_comments(cp)
            if existing_id is not None:
                skipped_existing += 1
                company_id = existing_id
                try:
                    update_fields = {"COMMENTS": comment_text}
                    contract_value = str(getattr(cp, "contract", "") or "").strip()
                    if contract_value:
                        update_fields[_BITRIX_COMPANY_CONTRACT_FIELD] = contract_value
                    comment_updated = update_company(
                        company_id=company_id,
                        fields=update_fields,
                    )
                    if comment_updated:
                        comments_synced += 1
                    else:
                        logger.warning(
                            "[%s/%s] Не удалось обновить COMMENTS для company_id=%s (short_name='%s')",
                            index,
                            total,
                            company_id,
                            cp.short_name,
                        )
                except Exception as update_err:
                    logger.warning(
                        "[%s/%s] Ошибка обновления COMMENTS для company_id=%s (short_name='%s'): %s",
                        index,
                        total,
                        company_id,
                        cp.short_name,
                        update_err,
                    )
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
                    comments=comment_text,
                    custom_fields=custom_fields,
                )
                comments_synced += 1
                created += 1
                logger.info(
                    "[%s/%s] Импортирован '%s' (short_name='%s'), company_id=%s",
                    index,
                    total,
                    cp.name,
                    cp.short_name,
                    company_id,
                )

            if requisite_preset_id is None:
                raise RuntimeError("preset_id реквизитов не определён")
            requisite_action, requisite_id = _ensure_company_requisite(
                counterparty=cp,
                company_id=company_id,
                preset_id=requisite_preset_id,
            )
            if requisite_action == "created":
                requisites_created += 1
            elif requisite_action == "updated":
                requisites_updated += 1
            else:
                requisites_skipped += 1

            _save_company_binding(counterparty_id=cp.id, company_id=company_id)
            bindings_saved += 1
            logger.info(
                "[%s/%s] Реквизиты '%s': action=%s, requisite_id=%s, inn=%s, kpp=%s",
                index,
                total,
                cp.short_name,
                requisite_action,
                requisite_id,
                cp.inn,
                cp.kpp or "",
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
            "Импорт завершён. Компаний создано: %s, компаний пропущено (уже есть): %s, "
            "COMMENTS синхронизировано: %s, реквизитов создано: %s, реквизитов обновлено: %s, реквизитов без изменений: %s, "
            "связок company_id сохранено: %s, ошибок: %s, всего: %s",
            created,
            skipped_existing,
            comments_synced,
            requisites_created,
            requisites_updated,
            requisites_skipped,
            bindings_saved,
            failed,
            total,
        )

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
