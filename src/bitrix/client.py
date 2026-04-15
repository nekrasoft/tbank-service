"""Клиент Bitrix24 CRM (входящий вебхук)."""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_EMAIL_SPLIT_RE = re.compile(r"[,;\n]+")
_METHOD_SEGMENT_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z0-9_]+){2,}$")


def _get_webhook_url(*, env_var: str = "BITRIX24_WEBHOOK_URL") -> str:
    """URL входящего вебхука Bitrix24 из .env."""
    webhook = os.environ.get(env_var, "").strip()
    if not webhook:
        raise ValueError(f"Задайте {env_var} в .env")
    return webhook.rstrip("/")


def _get_webhook_base_url(*, webhook_env_var: str = "BITRIX24_WEBHOOK_URL") -> str:
    """
    Приводит webhook URL к базовому виду без имени метода.

    Поддерживаются варианты:
    - https://<portal>/rest/<user>/<code>
    - .../crm.company.add
    - .../crm.company.add.json
    - .../tasks.task.add
    - .../tasks.task.add.json
    """
    webhook = _get_webhook_url(env_var=webhook_env_var)

    if webhook.endswith(".json"):
        return webhook.rsplit("/", 1)[0]

    last_segment = webhook.rsplit("/", 1)[-1]
    if _METHOD_SEGMENT_RE.fullmatch(last_segment):
        return webhook.rsplit("/", 1)[0]
    return webhook


def _method_url(method_name: str, *, webhook_env_var: str = "BITRIX24_WEBHOOK_URL") -> str:
    """Собирает URL метода Bitrix24 из базового webhook URL."""
    base = _get_webhook_base_url(webhook_env_var=webhook_env_var)
    return f"{base}/{method_name}.json"


def _call_method(
    method_name: str,
    payload: dict[str, Any],
    *,
    webhook_env_var: str = "BITRIX24_WEBHOOK_URL",
) -> dict[str, Any]:
    """Вызов REST-метода Bitrix24 по webhook URL."""
    url = _method_url(method_name, webhook_env_var=webhook_env_var)
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, json=payload)
        if resp.is_error:
            body_preview = resp.text[:2000]
            logger.error(
                "Bitrix24: ошибка %s, status=%s, body=%s",
                method_name,
                resp.status_code,
                body_preview,
            )
        resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Bitrix24: неожиданный формат ответа {method_name}: {data}")
    if "error" in data:
        error = data.get("error")
        description = data.get("error_description")
        raise RuntimeError(f"Bitrix24 API error: {error} ({description})")
    return data


def _log_method_payload(method_name: str, payload: dict[str, Any]) -> None:
    """Логирует payload REST-запроса Bitrix24 в безопасном виде."""
    try:
        payload_text = json.dumps(payload, ensure_ascii=False, default=str)
    except Exception:
        payload_text = str(payload)
    logger.info("Bitrix24 request payload %s: %s", method_name, payload_text)


def _call_list_method(
    *,
    method_name: str,
    filter_fields: dict[str, Any] | None = None,
    select_fields: list[str] | None = None,
    order_fields: dict[str, str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Универсальный листинг с пагинацией (result + next)."""
    result_items: list[dict[str, Any]] = []
    start = 0
    pages = 0

    while True:
        pages += 1
        if pages > 500:
            raise RuntimeError(f"Bitrix24: слишком много страниц при {method_name}")

        payload: dict[str, Any] = {
            "filter": filter_fields or {},
            "select": select_fields or ["ID"],
            "order": order_fields or {"ID": "ASC"},
            "start": start,
        }
        data = _call_method(method_name, payload)
        page_items = data.get("result")
        if not isinstance(page_items, list):
            raise RuntimeError(f"Неожиданный ответ Bitrix24 {method_name}: {data}")

        for item in page_items:
            if isinstance(item, dict):
                result_items.append(item)
            else:
                raise RuntimeError(f"Bitrix24: неожиданный элемент result: {item}")

        if limit is not None and limit > 0 and len(result_items) >= limit:
            return result_items[:limit]

        next_start = data.get("next")
        if next_start is None:
            return result_items
        try:
            start = int(next_start)
        except (TypeError, ValueError):
            raise RuntimeError(f"Bitrix24: неожиданный next в {method_name}: {data}") from None


def _normalize_emails(value: str | list[str] | None) -> list[str]:
    """Нормализация email(ов) из строки/списка в уникальный список."""
    if value is None:
        return []

    parts: list[str] = []
    if isinstance(value, str):
        parts = _EMAIL_SPLIT_RE.split(value)
    else:
        for item in value:
            parts.extend(_EMAIL_SPLIT_RE.split(str(item)))

    emails: list[str] = []
    seen: set[str] = set()
    for raw in parts:
        email = raw.strip()
        if not email:
            continue
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        emails.append(email)
    return emails


def add_company(
    *,
    title: str,
    email: str | list[str] | None = None,
    phone: str | None = None,
    comments: str | None = None,
    custom_fields: dict[str, Any] | None = None,
) -> int:
    """
    Создаёт компанию в Bitrix24 CRM методом crm.company.add.

    Возвращает ID созданной компании.
    """
    fields: dict[str, Any] = {
        "TITLE": str(title).strip()[:255],
    }

    emails = _normalize_emails(email)
    if emails:
        fields["EMAIL"] = [
            {
                "VALUE": value,
                "VALUE_TYPE": "WORK",
            }
            for value in emails
        ]

    phone_norm = (phone or "").strip()
    if phone_norm:
        fields["PHONE"] = [
            {
                "VALUE": phone_norm,
                "VALUE_TYPE": "WORK",
            }
        ]

    comments_norm = (comments or "").strip()
    if comments_norm:
        fields["COMMENTS"] = comments_norm

    for field_name, value in (custom_fields or {}).items():
        field_key = str(field_name).strip()
        if not field_key:
            continue
        value_norm = (value or "").strip() if isinstance(value, str) else value
        if value_norm in (None, ""):
            continue
        fields[field_key] = value_norm

    payload = {
        "fields": fields,
        "params": {
            "REGISTER_SONET_EVENT": "N",
        },
    }
    data = _call_method("crm.company.add", payload)

    result = data.get("result")
    if isinstance(result, int):
        return result
    if isinstance(result, str) and result.isdigit():
        return int(result)
    raise RuntimeError(f"Неожиданный ответ Bitrix24: {data}")


def list_companies(
    *,
    filter_fields: dict[str, Any],
    select_fields: list[str] | None = None,
    order_fields: dict[str, str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Получение списка компаний из Bitrix24 по фильтру crm.company.list."""
    return _call_list_method(
        method_name="crm.company.list",
        filter_fields=filter_fields,
        select_fields=select_fields or ["ID", "TITLE"],
        order_fields=order_fields or {"ID": "ASC"},
        limit=limit,
    )


def update_company(
    *,
    company_id: int,
    fields: dict[str, Any],
    webhook_env_var: str = "BITRIX24_WEBHOOK_URL",
) -> bool:
    """Обновляет компанию в Bitrix24 CRM методом crm.company.update."""
    normalized_fields: dict[str, Any] = {}
    for field_name, value in (fields or {}).items():
        key = str(field_name).strip()
        if not key:
            continue
        value_norm = (value or "").strip() if isinstance(value, str) else value
        if value_norm in (None, ""):
            continue
        normalized_fields[key] = value_norm

    if not normalized_fields:
        return True

    payload = {
        "id": int(company_id),
        "fields": normalized_fields,
        "params": {
            "REGISTER_SONET_EVENT": "N",
        },
    }
    data = _call_method("crm.company.update", payload, webhook_env_var=webhook_env_var)
    result = data.get("result")
    if isinstance(result, bool):
        return result
    if isinstance(result, int):
        return result != 0
    if isinstance(result, str):
        return result.strip().lower() in ("1", "y", "yes", "true")
    raise RuntimeError(f"Неожиданный ответ Bitrix24 crm.company.update: {data}")


def list_requisites(
    *,
    filter_fields: dict[str, Any],
    select_fields: list[str] | None = None,
    order_fields: dict[str, str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Получение списка реквизитов по фильтру crm.requisite.list."""
    return _call_list_method(
        method_name="crm.requisite.list",
        filter_fields=filter_fields,
        select_fields=select_fields or ["ID", "ENTITY_TYPE_ID", "ENTITY_ID", "PRESET_ID", "NAME", "RQ_INN", "RQ_KPP"],
        order_fields=order_fields or {"ID": "ASC"},
        limit=limit,
    )


def list_requisite_presets(
    *,
    filter_fields: dict[str, Any],
    select_fields: list[str] | None = None,
    order_fields: dict[str, str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Получение списка шаблонов реквизитов по фильтру crm.requisite.preset.list."""
    return _call_list_method(
        method_name="crm.requisite.preset.list",
        filter_fields=filter_fields,
        select_fields=select_fields or ["ID", "NAME", "COUNTRY_ID", "ACTIVE", "SORT"],
        order_fields=order_fields or {"ID": "ASC"},
        limit=limit,
    )


def list_requisite_preset_fields(*, preset_id: int) -> list[dict[str, Any]]:
    """Список полей конкретного шаблона реквизитов (crm.requisite.preset.field.list)."""
    data = _call_method(
        "crm.requisite.preset.field.list",
        {
            "preset": {"ID": int(preset_id)},
        },
    )
    result = data.get("result")
    if not isinstance(result, list):
        raise RuntimeError(f"Неожиданный ответ Bitrix24 crm.requisite.preset.field.list: {data}")
    normalized: list[dict[str, Any]] = []
    for item in result:
        if isinstance(item, dict):
            normalized.append(item)
        else:
            raise RuntimeError(f"Bitrix24: неожиданный элемент result: {item}")
    return normalized


def add_requisite(
    *,
    entity_type_id: int,
    entity_id: int,
    preset_id: int,
    name: str,
    rq_inn: str | None = None,
    rq_kpp: str | None = None,
    fields: dict[str, Any] | None = None,
) -> int:
    """Создание реквизита для CRM-сущности (crm.requisite.add)."""
    requisite_fields: dict[str, Any] = {
        "ENTITY_TYPE_ID": int(entity_type_id),
        "ENTITY_ID": int(entity_id),
        "PRESET_ID": int(preset_id),
        "NAME": str(name).strip()[:255],
    }

    inn_norm = (rq_inn or "").strip()
    if inn_norm:
        requisite_fields["RQ_INN"] = inn_norm
    kpp_norm = (rq_kpp or "").strip()
    if kpp_norm:
        requisite_fields["RQ_KPP"] = kpp_norm

    for field_name, value in (fields or {}).items():
        key = str(field_name).strip()
        if not key:
            continue
        value_norm = (value or "").strip() if isinstance(value, str) else value
        if value_norm in (None, ""):
            continue
        requisite_fields[key] = value_norm

    data = _call_method(
        "crm.requisite.add",
        {
            "fields": requisite_fields,
        },
    )
    result = data.get("result")
    if isinstance(result, int):
        return result
    if isinstance(result, str) and result.isdigit():
        return int(result)
    raise RuntimeError(f"Неожиданный ответ Bitrix24 crm.requisite.add: {data}")


def update_requisite(*, requisite_id: int, fields: dict[str, Any]) -> bool:
    """Обновление реквизита (crm.requisite.update)."""
    if not fields:
        return True
    data = _call_method(
        "crm.requisite.update",
        {
            "id": int(requisite_id),
            "fields": fields,
        },
    )
    result = data.get("result")
    if isinstance(result, bool):
        return result
    if isinstance(result, int):
        return result != 0
    if isinstance(result, str):
        return result.strip().lower() in ("1", "y", "yes", "true")
    raise RuntimeError(f"Неожиданный ответ Bitrix24 crm.requisite.update: {data}")


def add_deal(
    *,
    title: str,
    company_id: int | None = None,
    opportunity: Any | None = None,
    stage_id: str | None = None,
    type_id: str | None = None,
    source_id: str | None = None,
    address: str | None = None,
    custom_fields: dict[str, Any] | None = None,
    webhook_env_var: str = "BITRIX24_DEAL_WEBHOOK_URL",
    log_request_payload: bool = False,
) -> int:
    """Создаёт сделку в Bitrix24 CRM методом crm.deal.add."""
    fields: dict[str, Any] = {
        "TITLE": str(title).strip()[:255],
        "ASSIGNED_BY_ID": 31746,
        "CATEGORY_ID": 102,
    }

    if company_id is not None:
        company_id_int = int(company_id)
        if company_id_int > 0:
            fields["COMPANY_ID"] = company_id_int

    if opportunity is not None:
        if isinstance(opportunity, str):
            opportunity_norm = opportunity.strip()
            if opportunity_norm:
                fields["OPPORTUNITY"] = opportunity_norm
        else:
            fields["OPPORTUNITY"] = opportunity

    stage_id_norm = (stage_id or "").strip()
    if stage_id_norm:
        fields["STAGE_ID"] = stage_id_norm

    type_id_norm = (type_id or "").strip()
    if type_id_norm:
        fields["TYPE_ID"] = type_id_norm

    source_id_norm = (source_id or "").strip()
    if source_id_norm:
        fields["SOURCE_ID"] = source_id_norm

    address_norm = (address or "").strip()
    if address_norm:
        fields["ADDRESS"] = address_norm

    for field_name, value in (custom_fields or {}).items():
        key = str(field_name).strip()
        if not key:
            continue
        value_norm = (value or "").strip() if isinstance(value, str) else value
        if value_norm in (None, ""):
            continue
        fields[key] = value_norm

    payload = {
        "fields": fields,
        "params": {
            "REGISTER_SONET_EVENT": "N",
        },
    }
    if log_request_payload:
        _log_method_payload("crm.deal.add", payload)

    data = _call_method("crm.deal.add", payload, webhook_env_var=webhook_env_var)
    result = data.get("result")
    if isinstance(result, int):
        logger.info("Bitrix24: crm.deal.add успешно, deal_id=%s", result)
        return result
    if isinstance(result, str) and result.isdigit():
        deal_id = int(result)
        logger.info("Bitrix24: crm.deal.add успешно, deal_id=%s", deal_id)
        return deal_id
    raise RuntimeError(f"Неожиданный ответ Bitrix24 crm.deal.add: {data}")


def update_deal(
    *,
    deal_id: int,
    fields: dict[str, Any],
    webhook_env_var: str = "BITRIX24_DEAL_WEBHOOK_URL",
    log_request_payload: bool = False,
) -> bool:
    """Обновляет сделку в Bitrix24 CRM методом crm.deal.update."""
    normalized_fields: dict[str, Any] = {}
    for field_name, value in (fields or {}).items():
        key = str(field_name).strip()
        if not key:
            continue
        value_norm = (value or "").strip() if isinstance(value, str) else value
        if value_norm in (None, ""):
            continue
        normalized_fields[key] = value_norm

    if not normalized_fields:
        return True

    payload = {
        "id": int(deal_id),
        "fields": normalized_fields,
        "params": {
            "REGISTER_SONET_EVENT": "N",
        },
    }
    if log_request_payload:
        _log_method_payload("crm.deal.update", payload)
    data = _call_method("crm.deal.update", payload, webhook_env_var=webhook_env_var)
    result = data.get("result")
    if isinstance(result, bool):
        return result
    if isinstance(result, int):
        return result != 0
    if isinstance(result, str):
        return result.strip().lower() in ("1", "y", "yes", "true")
    raise RuntimeError(f"Неожиданный ответ Bitrix24 crm.deal.update: {data}")


def set_deal_product_rows(
    *,
    deal_id: int,
    rows: list[dict[str, Any]],
    webhook_env_var: str = "BITRIX24_DEAL_WEBHOOK_URL",
    log_request_payload: bool = False,
) -> bool:
    """Устанавливает товарные позиции сделки методом crm.deal.productrows.set."""
    normalized_rows: list[dict[str, Any]] = []
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            continue
        row: dict[str, Any] = {}
        for field_name, value in raw_row.items():
            key = str(field_name).strip()
            if not key:
                continue
            if value is None:
                continue
            if isinstance(value, str):
                value_norm = value.strip()
                if not value_norm:
                    continue
                row[key] = value_norm
            else:
                row[key] = value
        if row:
            normalized_rows.append(row)

    if not normalized_rows:
        return True

    payload = {
        "id": int(deal_id),
        "rows": normalized_rows,
    }
    if log_request_payload:
        _log_method_payload("crm.deal.productrows.set", payload)

    data = _call_method("crm.deal.productrows.set", payload, webhook_env_var=webhook_env_var)
    result = data.get("result")
    if isinstance(result, bool):
        return result
    if isinstance(result, int):
        return result != 0
    if isinstance(result, str):
        return result.strip().lower() in ("1", "y", "yes", "true")
    raise RuntimeError(f"Неожиданный ответ Bitrix24 crm.deal.productrows.set: {data}")


def add_task(
    *,
    title: str,
    responsible_id: int,
    auditors: list[int] | None = None,
    crm_bindings: list[str] | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    deadline: datetime | None = None,
    priority: int | None = None,
    flow_id: int | None = None,
    description_in_bbcode: bool = False,
    require_result: bool = False,
    webdav_file_ids: list[int | str] | None = None,
) -> int:
    """
    Создаёт задачу в Bitrix24 методом tasks.task.add.

    Для задач используется отдельный webhook URL:
    BITRIX24_TASK_WEBHOOK_URL
    """
    fields: dict[str, Any] = {
        "TITLE": str(title).strip()[:255],
        "RESPONSIBLE_ID": int(responsible_id),
    }

    auditors_norm: list[int] = []
    seen_auditors: set[int] = set()
    for raw_id in auditors or []:
        user_id = int(raw_id)
        if user_id <= 0 or user_id in seen_auditors:
            continue
        seen_auditors.add(user_id)
        auditors_norm.append(user_id)
    if auditors_norm:
        fields["AUDITORS"] = auditors_norm

    crm_bindings_norm: list[str] = []
    seen_bindings: set[str] = set()
    for raw_binding in crm_bindings or []:
        binding = str(raw_binding).strip()
        if not binding:
            continue
        if binding in seen_bindings:
            continue
        seen_bindings.add(binding)
        crm_bindings_norm.append(binding)
    if crm_bindings_norm:
        fields["UF_CRM_TASK"] = crm_bindings_norm

    description_norm = (description or "").strip()
    if description_norm:
        fields["DESCRIPTION"] = description_norm
        fields["DESCRIPTION_IN_BBCODE"] = "Y" if description_in_bbcode else "N"

    tags_norm: list[str] = []
    seen_tags: set[str] = set()
    for raw_tag in tags or []:
        tag = str(raw_tag).strip()
        if not tag:
            continue
        key = tag.lower()
        if key in seen_tags:
            continue
        seen_tags.add(key)
        tags_norm.append(tag)
    if tags_norm:
        fields["TAGS"] = tags_norm

    if deadline is not None:
        deadline_local = deadline.astimezone() if deadline.tzinfo else deadline.replace(
            tzinfo=datetime.now().astimezone().tzinfo,
        )
        fields["DEADLINE"] = deadline_local.replace(microsecond=0).isoformat()

    if priority is not None:
        priority_int = int(priority)
        if priority_int not in (0, 1, 2):
            raise ValueError("PRIORITY должен быть одним из значений: 0, 1, 2")
        fields["PRIORITY"] = priority_int

    if flow_id is not None:
        flow_id_int = int(flow_id)
        if flow_id_int <= 0:
            raise ValueError("FLOW_ID должен быть положительным целым числом")
        fields["FLOW_ID"] = flow_id_int

    if require_result:
        # CODE=3 -> не завершать задачу без результата.
        fields["SE_PARAMETER"] = [
            {
                "VALUE": "Y",
                "CODE": 3,
            },
        ]

    webdav_files_norm: list[str] = []
    seen_webdav_files: set[str] = set()
    for raw_file_id in webdav_file_ids or []:
        raw_value = str(raw_file_id).strip()
        if not raw_value:
            continue
        num_part = raw_value[1:] if raw_value.lower().startswith("n") else raw_value
        if not num_part.isdigit():
            continue
        file_id_int = int(num_part)
        if file_id_int <= 0:
            continue
        token = f"n{file_id_int}"
        if token in seen_webdav_files:
            continue
        seen_webdav_files.add(token)
        webdav_files_norm.append(token)
    if webdav_files_norm:
        fields["UF_TASK_WEBDAV_FILES"] = webdav_files_norm

    data = _call_method(
        "tasks.task.add",
        {"fields": fields},
        webhook_env_var="BITRIX24_TASK_WEBHOOK_URL",
    )
    result = data.get("result")
    if isinstance(result, int):
        return result
    if isinstance(result, str) and result.isdigit():
        return int(result)
    if isinstance(result, dict):
        task = result.get("task")
        if isinstance(task, dict):
            task_id = task.get("id") or task.get("ID")
            if isinstance(task_id, int):
                return task_id
            if isinstance(task_id, str) and task_id.isdigit():
                return int(task_id)
        task_id = result.get("id") or result.get("ID")
        if isinstance(task_id, int):
            return task_id
        if isinstance(task_id, str) and task_id.isdigit():
            return int(task_id)
    raise RuntimeError(f"Неожиданный ответ Bitrix24 tasks.task.add: {data}")


def add_task_comment(
    *,
    task_id: int,
    message: str,
    webhook_env_var: str = "BITRIX24_TASK_WEBHOOK_URL",
    log_request_payload: bool = False,
) -> int:
    """Добавляет комментарий к задаче Bitrix24 методом task.commentitem.add."""
    comment_text = str(message or "").strip()
    if not comment_text:
        raise ValueError("message не должен быть пустым")

    payload = {
        "TASKID": int(task_id),
        "FIELDS": {
            "POST_MESSAGE": comment_text,
        },
    }
    if log_request_payload:
        _log_method_payload("task.commentitem.add", payload)

    data = _call_method("task.commentitem.add", payload, webhook_env_var=webhook_env_var)
    result = data.get("result")
    if isinstance(result, int):
        return result
    if isinstance(result, str) and result.isdigit():
        return int(result)
    if isinstance(result, dict):
        comment_id = result.get("ID") or result.get("id")
        if isinstance(comment_id, int):
            return comment_id
        if isinstance(comment_id, str) and comment_id.isdigit():
            return int(comment_id)
    raise RuntimeError(f"Неожиданный ответ Bitrix24 task.commentitem.add: {data}")
