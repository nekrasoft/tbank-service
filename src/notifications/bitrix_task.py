"""
Создание задач в Bitrix24 для бухгалтеров.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import logging
import os
from datetime import date, datetime
from typing import Any
from urllib.parse import urlsplit

from src.bitrix.client import (
    add_deal,
    add_task,
    add_task_comment,
    set_deal_product_rows,
    update_deal,
)
from src.invoice.window import add_business_days

logger = logging.getLogger(__name__)

_TASK_WEBHOOK_ENV = "BITRIX24_TASK_WEBHOOK_URL"
_DEAL_WEBHOOK_ENV = "BITRIX24_DEAL_WEBHOOK_URL"
_TASK_TITLE_PREFIX = "[Киров] Обработать счет №"
_TASK_RESPONSIBLE_ID = 31648
_TASK_AUDITORS = [8, 54, 18, 33036]
_TASK_TAGS = ["киров", "новый_счет", "отправить в ЭДО"]
_TASK_PRIORITY = 2
_TASK_FLOW_ID = 16
_TASK_REQUIRE_RESULT = True
_TASK_WEBDAV_FILE_IDS = [1095778]
_DEAL_STAGE_ID = "C102:FINAL_INVOICE"
_DEAL_WON_STAGE_ID = "C102:WON"
_DEAL_TYPE_ID = "SALE"
_DEAL_SOURCE_ID = "PARTNER"
_DEAL_SERVICE_FIELD = "UF_CRM_1640764372166"
_DEAL_SERVICE_DEFAULT_VALUE = 2558
_DEAL_SERVICE_VALUE_BY_OPERATION_TYPE = {
    "container_pickup": 2558,
    "trip_removal": 2550,
}
_DEAL_TITLE_DEFAULT_TEXT = "Вывоз бункеров"
_DEAL_TITLE_TEXT_BY_OPERATION_TYPE = {
    "trip_removal": "Вывоз мусора",
}
_DEAL_SUBJECT_FIELD = "UF_CRM_1640765412209"
_DEAL_SUBJECT_VALUE = 174
_DEAL_PAYMENT_FIELD = "UF_CRM_AMO_586713"
_DEAL_PAYMENT_VALUE = 544
_DEAL_CITY_FIELD = "UF_CRM_AMO_631688"
_DEAL_CITY_VALUE = "Киров"
_DEAL_DIRECTION_FIELD = "UF_CRM_1680515310897"
_DEAL_DIRECTION_VALUE = 4818
_DEAL_ADDRESS = "Киров"
_DEAL_DEFAULT_PRODUCT_NAME = "Услуга по вывозу мусора из контейнера 8 м3"
_TASK_PAYMENT_COMMENT_TEXT = "Оплата поступила"
_task_webhook_missing_logged = False
_deal_webhook_missing_logged = False


@dataclass(frozen=True)
class BitrixInvoiceTaskResult:
    """Результат создания связки сделка+задача в Bitrix24 по счёту."""

    task_id: int | None
    task_url: str | None
    deal_id: int | None


def _is_task_webhook_configured() -> bool:
    """Проверяет наличие webhook для задач в Bitrix24."""
    global _task_webhook_missing_logged
    webhook = (os.environ.get(_TASK_WEBHOOK_ENV) or "").strip()
    if webhook:
        return True
    if not _task_webhook_missing_logged:
        logger.info("%s не задан — создание задач в Bitrix24 отключено", _TASK_WEBHOOK_ENV)
        _task_webhook_missing_logged = True
    return False


def _is_deal_webhook_configured() -> bool:
    """Проверяет наличие webhook для сделок в Bitrix24."""
    global _deal_webhook_missing_logged
    webhook = (os.environ.get(_DEAL_WEBHOOK_ENV) or "").strip()
    if webhook:
        return True
    if not _deal_webhook_missing_logged:
        logger.info("%s не задан — создание сделок в Bitrix24 отключено", _DEAL_WEBHOOK_ENV)
        _deal_webhook_missing_logged = True
    return False


def create_invoice_task(
    *,
    counterparty_name: str,
    counterparty_short_name: str | None = None,
    counterparty_contract: str | None = None,
    invoice_number: str,
    invoice_date: date | datetime | None = None,
    bitrix_company_id: int | None = None,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
    pdf_url: str | None = None,
    invoice_items: list[dict[str, Any]] | None = None,
    period_text: str | None = None,
    log_deal_request_payload: bool = False,
) -> str | None:
    """Создаёт задачу в Bitrix24 по факту выставления счёта."""
    result = create_invoice_task_with_meta(
        counterparty_name=counterparty_name,
        counterparty_short_name=counterparty_short_name,
        counterparty_contract=counterparty_contract,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        bitrix_company_id=bitrix_company_id,
        tbank_invoice_id=tbank_invoice_id,
        invoice_link=invoice_link,
        pdf_url=pdf_url,
        invoice_items=invoice_items,
        period_text=period_text,
        log_deal_request_payload=log_deal_request_payload,
    )
    if not result:
        return None
    return result.task_url


def create_invoice_task_with_meta(
    *,
    counterparty_name: str,
    counterparty_short_name: str | None = None,
    counterparty_contract: str | None = None,
    invoice_number: str,
    invoice_date: date | datetime | None = None,
    bitrix_company_id: int | None = None,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
    pdf_url: str | None = None,
    invoice_items: list[dict[str, Any]] | None = None,
    period_text: str | None = None,
    log_deal_request_payload: bool = False,
) -> BitrixInvoiceTaskResult | None:
    """Создаёт задачу/сделку в Bitrix24 и возвращает их ID/URL."""
    if not _is_task_webhook_configured():
        return None

    invoice_amount = _calculate_invoice_amount(invoice_items)
    text = _build_task_description(
        counterparty_name=counterparty_name,
        counterparty_contract=counterparty_contract,
        invoice_number=invoice_number,
        invoice_amount=invoice_amount,
        tbank_invoice_id=tbank_invoice_id,
        invoice_link=invoice_link,
        pdf_url=pdf_url,
        period_text=period_text,
    )
    deadline = _build_task_deadline()
    task_title = _build_task_title(
        invoice_number=invoice_number,
        counterparty_short_name=counterparty_short_name,
    )
    deal_id: int | None = None
    if _is_deal_webhook_configured():
        try:
            deal_id = _create_invoice_deal(
                invoice_number=invoice_number,
                invoice_date=invoice_date,
                bitrix_company_id=bitrix_company_id,
                invoice_amount=invoice_amount,
                invoice_items=invoice_items,
                log_request_payload=log_deal_request_payload,
            )
        except Exception as e:
            logger.error("Bitrix24 deal: ошибка создания сделки по счёту %s — %s", invoice_number, e)

    crm_bindings = _build_task_crm_bindings(bitrix_company_id, deal_id)
    task_id: int | None = None
    task_url: str | None = None

    try:
        task_id = add_task(
            title=task_title,
            responsible_id=_TASK_RESPONSIBLE_ID,
            auditors=_TASK_AUDITORS,
            crm_bindings=crm_bindings,
            description=text,
            tags=_TASK_TAGS,
            deadline=deadline,
            priority=_TASK_PRIORITY,
            flow_id=_TASK_FLOW_ID,
            description_in_bbcode=True,
            require_result=_TASK_REQUIRE_RESULT,
            webdav_file_ids=_TASK_WEBDAV_FILE_IDS,
        )
        task_url = _build_task_url(task_id)
        if task_url:
            logger.info(
                "Bitrix24 task: создана задача id=%s по счёту %s, url=%s",
                task_id,
                invoice_number,
                task_url,
            )
        else:
            logger.info("Bitrix24 task: создана задача id=%s по счёту %s", task_id, invoice_number)
    except Exception as e:
        logger.error("Bitrix24 task: ошибка создания задачи по счёту %s — %s", invoice_number, e)

    if task_id is None and deal_id is None:
        return None

    return BitrixInvoiceTaskResult(
        task_id=task_id,
        task_url=task_url,
        deal_id=deal_id,
    )


def _build_task_deadline(*, now: datetime | None = None) -> datetime:
    """Возвращает дедлайн задачи на следующий рабочий день в то же время."""
    current = now.astimezone() if now is not None else datetime.now().astimezone()
    deadline_date = add_business_days(current.date(), 1)
    return current.replace(
        year=deadline_date.year,
        month=deadline_date.month,
        day=deadline_date.day,
    )


def mark_invoice_paid_in_bitrix(
    *,
    invoice_number: str,
    bitrix_task_id: int | None,
    bitrix_deal_id: int | None,
) -> None:
    """Пишет комментарий в задачу и переводит связанную сделку в WON."""
    task_id = _normalize_positive_int(bitrix_task_id)
    if task_id is not None:
        if _is_task_webhook_configured():
            try:
                comment_id = add_task_comment(
                    task_id=task_id,
                    message=_TASK_PAYMENT_COMMENT_TEXT,
                    webhook_env_var=_TASK_WEBHOOK_ENV,
                )
                logger.info(
                    "Bitrix24 task: добавлен комментарий id=%s в задачу %s по счёту %s",
                    comment_id,
                    task_id,
                    invoice_number,
                )
            except Exception as e:
                logger.error(
                    "Bitrix24 task: ошибка добавления комментария в задачу %s по счёту %s — %s",
                    task_id,
                    invoice_number,
                    e,
                )
        else:
            logger.info(
                "Bitrix24 task: webhook не настроен, комментарий по оплате не отправлен "
                "(счёт %s, task_id=%s)",
                invoice_number,
                task_id,
            )
    else:
        logger.info(
            "Bitrix24 task: task_id не задан, пропускаем комментарий по оплате (счёт %s)",
            invoice_number,
        )

    deal_id = _normalize_positive_int(bitrix_deal_id)
    if deal_id is None:
        logger.info(
            "Bitrix24 deal: deal_id не задан, пропускаем перевод в WON (счёт %s)",
            invoice_number,
        )
        return

    if not _is_deal_webhook_configured():
        logger.info(
            "Bitrix24 deal: webhook не настроен, перевод в WON не выполнен "
            "(счёт %s, deal_id=%s)",
            invoice_number,
            deal_id,
        )
        return

    try:
        is_updated = update_deal(
            deal_id=deal_id,
            fields={"STAGE_ID": _DEAL_WON_STAGE_ID},
            webhook_env_var=_DEAL_WEBHOOK_ENV,
        )
        if is_updated:
            logger.info(
                "Bitrix24 deal: сделка %s переведена в %s по счёту %s",
                deal_id,
                _DEAL_WON_STAGE_ID,
                invoice_number,
            )
        else:
            logger.warning(
                "Bitrix24 deal: crm.deal.update вернул false для сделки %s по счёту %s",
                deal_id,
                invoice_number,
            )
    except Exception as e:
        logger.error(
            "Bitrix24 deal: ошибка перевода сделки %s в %s по счёту %s — %s",
            deal_id,
            _DEAL_WON_STAGE_ID,
            invoice_number,
            e,
        )


def _build_task_description(
    *,
    counterparty_name: str,
    counterparty_contract: str | None = None,
    invoice_number: str,
    invoice_amount: Decimal | None = None,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
    pdf_url: str | None = None,
    period_text: str | None = None,
) -> str:
    """
    Описание задачи для Bitrix24 в BBCode.

    DESCRIPTION_IN_BBCODE=Y нужен, чтобы [B]...[/B] отображался жирным.
    """
    contract_line = (counterparty_contract or "").strip() or "-"
    lines = [
        f"[B]Контрагент[/B]: {counterparty_name}",
        f"[B]Договор[/B]: {contract_line}",
    ]
    if period_text:
        lines.append(f"[B]Период[/B]: {period_text}")
    if invoice_amount is not None:
        lines.append(f"[B]Сумма[/B]: {_format_money(invoice_amount)}")
    if pdf_url:
        lines.append(f"[B]PDF[/B]: [URL={pdf_url}]Документ[/URL]")
    lines.extend(
        [
            "",
            "[B]Необходимо в ТБанке создать Акт и УПД для данного Счета и отправить все три документа в ЭДО[/B]",
            "",
            "[B]ВНИМАНИЕ:[/B] Для УПД обязательно указать \"Основание передачи\" - детали в приложенном файле.",
        ]
    )
    return "\n".join(lines)


def _calculate_invoice_amount(invoice_items: list[dict[str, Any]] | None) -> Decimal | None:
    """Считает итог по позициям счёта: sum(price * amount)."""
    if not invoice_items:
        return None

    total = Decimal("0")
    has_any = False
    for item in invoice_items:
        try:
            price = Decimal(str(item.get("price")))
            amount = Decimal(str(item.get("amount")))
        except Exception:
            continue
        total += price * amount
        has_any = True

    if not has_any:
        return None
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _format_money(value: Decimal) -> str:
    """Форматирует сумму в человекочитаемом виде для текста задачи."""
    return f"{value:.2f} ₽"


def _build_task_url(task_id: int) -> str | None:
    """Строит ссылку на задачу Bitrix24 по task_id и webhook-хосту."""
    webhook = (os.environ.get(_TASK_WEBHOOK_ENV) or "").strip()
    if not webhook:
        return None
    parsed = urlsplit(webhook)
    if not parsed.scheme or not parsed.netloc:
        return None
    portal_base = f"{parsed.scheme}://{parsed.netloc}"
    return (
        f"{portal_base}/company/personal/user/{_TASK_RESPONSIBLE_ID}/"
        f"tasks/task/view/{int(task_id)}/"
    )


def _create_invoice_deal(
    *,
    invoice_number: str,
    invoice_date: date | datetime | None,
    bitrix_company_id: int | None,
    invoice_amount: Decimal | None,
    invoice_items: list[dict[str, Any]] | None,
    log_request_payload: bool = False,
) -> int | None:
    """Создаёт сделку и товарные позиции по счёту."""
    company_id = _normalize_positive_int(bitrix_company_id)
    if company_id is None:
        logger.warning(
            "Bitrix24 deal: пропуск создания сделки по счёту %s — не задан bitrix_company_id",
            invoice_number,
        )
        return None

    deal_amount = invoice_amount or Decimal("0.00")
    deal_service_value = _resolve_deal_service_value(invoice_items)
    deal_id = add_deal(
        title=_build_deal_title(invoice_date, invoice_items),
        company_id=company_id,
        opportunity=_format_amount_decimal(deal_amount),
        stage_id=_DEAL_STAGE_ID,
        type_id=_DEAL_TYPE_ID,
        source_id=_DEAL_SOURCE_ID,
        address=_DEAL_ADDRESS,
        custom_fields={
            _DEAL_SERVICE_FIELD: deal_service_value,
            _DEAL_SUBJECT_FIELD: _DEAL_SUBJECT_VALUE,
            _DEAL_PAYMENT_FIELD: _DEAL_PAYMENT_VALUE,
            _DEAL_CITY_FIELD: _DEAL_CITY_VALUE,
            _DEAL_DIRECTION_FIELD: _DEAL_DIRECTION_VALUE,
        },
        webhook_env_var=_DEAL_WEBHOOK_ENV,
        log_request_payload=log_request_payload,
    )
    product_rows = _build_deal_product_rows(invoice_items, fallback_amount=deal_amount)
    if product_rows:
        set_ok = set_deal_product_rows(
            deal_id=deal_id,
            rows=product_rows,
            webhook_env_var=_DEAL_WEBHOOK_ENV,
            log_request_payload=log_request_payload,
        )
        if not set_ok:
            logger.warning(
                "Bitrix24 deal: crm.deal.productrows.set вернул false для сделки %s (счёт %s)",
                deal_id,
                invoice_number,
            )

    logger.info(
        "Bitrix24 deal: создана сделка id=%s по счёту %s (company_id=%s, amount=%s, service_value=%s)",
        deal_id,
        invoice_number,
        company_id,
        _format_amount_decimal(deal_amount),
        deal_service_value,
    )
    return deal_id


def _build_deal_title(
    invoice_date: date | datetime | None,
    invoice_items: list[dict[str, Any]] | None,
) -> str:
    """Заголовок сделки в формате [dd.mm.yyyy] <тип услуги>."""
    if isinstance(invoice_date, datetime):
        dt = invoice_date.astimezone().date() if invoice_date.tzinfo else invoice_date.date()
    elif isinstance(invoice_date, date):
        dt = invoice_date
    else:
        dt = date.today()
    deal_title_text = _resolve_deal_title_text(invoice_items)
    return f"[{dt.strftime('%d.%m.%Y')}] {deal_title_text}"


def _resolve_deal_title_text(invoice_items: list[dict[str, Any]] | None) -> str:
    """Определяет текст заголовка сделки по operation_type позиций счёта."""
    for item in invoice_items or []:
        operation_type = _normalize_operation_type(item.get("operation_type"))
        title_text = _DEAL_TITLE_TEXT_BY_OPERATION_TYPE.get(operation_type)
        if title_text:
            return title_text
    return _DEAL_TITLE_DEFAULT_TEXT


def _build_deal_product_rows(
    invoice_items: list[dict[str, Any]] | None,
    *,
    fallback_amount: Decimal,
) -> list[dict[str, Any]]:
    """
    Формирует товарные строки сделки из счёта.

    Название/количество/цена/сумма берутся из позиций счёта.
    """
    rows: list[dict[str, Any]] = []
    for item in invoice_items or []:
        product_name = str(item.get("name") or "").strip() or _DEAL_DEFAULT_PRODUCT_NAME
        price = _try_decimal(item.get("price"))
        quantity = _try_decimal(item.get("amount"))
        if price is None or quantity is None or quantity <= 0:
            continue
        line_sum = (price * quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        rows.append(
            {
                "PRODUCT_NAME": product_name,
                "PRICE": _format_amount_decimal(price),
                "QUANTITY": _format_quantity_decimal(quantity),
                "SUM": _format_amount_decimal(line_sum),
            }
        )

    if rows:
        return rows
    if fallback_amount <= 0:
        return []
    return [
        {
            "PRODUCT_NAME": _DEAL_DEFAULT_PRODUCT_NAME,
            "PRICE": _format_amount_decimal(fallback_amount),
            "QUANTITY": "1",
            "SUM": _format_amount_decimal(fallback_amount),
        }
    ]


def _normalize_operation_type(operation_type: str | None) -> str:
    """Нормализует operation_type для сопоставления с маппингом."""
    return str(operation_type or "").strip().lower()


def _resolve_deal_service_value(invoice_items: list[dict[str, Any]] | None) -> int:
    """
    Определяет значение UF_CRM_1640764372166 по составу позиций счёта.

    Если в счёте несколько разных типов услуг с разными маппингами,
    используется значение первой распознанной позиции.
    """
    matched_values: list[tuple[str, int]] = []
    unknown_operation_types: list[str] = []
    missing_operation_type_count = 0

    for item in invoice_items or []:
        operation_type = _normalize_operation_type(item.get("operation_type"))
        if not operation_type:
            missing_operation_type_count += 1
            continue

        mapped_by_op_type = _DEAL_SERVICE_VALUE_BY_OPERATION_TYPE.get(operation_type)
        if mapped_by_op_type is None:
            unknown_operation_types.append(operation_type)
            continue
        matched_values.append((f"operation_type:{operation_type}", mapped_by_op_type))

    if not matched_values:
        if missing_operation_type_count > 0:
            logger.warning(
                "Bitrix24 deal: %s позиций без operation_type, используем default=%s",
                missing_operation_type_count,
                _DEAL_SERVICE_DEFAULT_VALUE,
            )
        if unknown_operation_types:
            logger.warning(
                "Bitrix24 deal: не найден маппинг услуги для operation_type=%s, используем default=%s",
                ", ".join(sorted(set(unknown_operation_types))),
                _DEAL_SERVICE_DEFAULT_VALUE,
            )
        return _DEAL_SERVICE_DEFAULT_VALUE

    first_value = matched_values[0][1]
    unique_values = {value for _, value in matched_values}
    if len(unique_values) > 1:
        logger.warning(
            "Bitrix24 deal: в счёте несколько типов услуг (%s), используем значение первой позиции=%s",
            ", ".join(f"{name} -> {value}" for name, value in matched_values),
            first_value,
        )
    return first_value


def _try_decimal(value: Any) -> Decimal | None:
    """Пытается привести значение к Decimal(2)."""
    try:
        dec = Decimal(str(value))
    except Exception:
        return None
    return dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _format_amount_decimal(value: Decimal) -> str:
    """Форматирует денежное значение в 2 знака после запятой."""
    return f"{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}"


def _format_quantity_decimal(value: Decimal) -> str:
    """Форматирует количество без лишних нулей."""
    normalized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if normalized == normalized.to_integral():
        return str(int(normalized))
    return format(normalized.normalize(), "f")


def _build_task_crm_bindings(
    bitrix_company_id: int | None,
    bitrix_deal_id: int | None,
) -> list[str] | None:
    """Формирует привязки задачи к компании и сделке через UF_CRM_TASK."""
    bindings: list[str] = []
    company_binding = _build_company_binding(bitrix_company_id)
    if company_binding:
        bindings.append(company_binding)
    deal_binding = _build_deal_binding(bitrix_deal_id)
    if deal_binding:
        bindings.append(deal_binding)
    return bindings or None


def _normalize_positive_int(value: Any) -> int | None:
    """Нормализует положительный ID или возвращает None."""
    if value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return number


def _build_company_binding(bitrix_company_id: int | None) -> str | None:
    """Готовит CRM-привязку компании для UF_CRM_TASK в формате CO_<ID>."""
    company_id = _normalize_positive_int(bitrix_company_id)
    if company_id is None:
        return None
    return f"CO_{company_id}"


def _build_deal_binding(bitrix_deal_id: int | None) -> str | None:
    """Готовит CRM-привязку сделки для UF_CRM_TASK в формате D_<ID>."""
    deal_id = _normalize_positive_int(bitrix_deal_id)
    if deal_id is None:
        return None
    return f"D_{deal_id}"


def _build_task_title(*, invoice_number: str, counterparty_short_name: str | None = None) -> str:
    """Формирует заголовок задачи со short_name контрагента, если он задан."""
    short_name = (counterparty_short_name or "").strip()
    if short_name:
        return f"{_TASK_TITLE_PREFIX}{invoice_number} ({short_name})"
    return f"{_TASK_TITLE_PREFIX}{invoice_number}"
