"""
Создание задач в Bitrix24 для бухгалтеров.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from src.bitrix.client import add_deal, add_task, set_deal_product_rows

logger = logging.getLogger(__name__)

_TASK_WEBHOOK_ENV = "BITRIX24_TASK_WEBHOOK_URL"
_DEAL_WEBHOOK_ENV = "BITRIX24_DEAL_WEBHOOK_URL"
_TASK_TITLE_PREFIX = "[Киров] Обработать счет №"
_TASK_RESPONSIBLE_ID = 31648
_TASK_AUDITORS = [8, 54, 18, 33036]
_TASK_TAGS = ["киров", "новый_счет", "отправить в ЭДО"]
_TASK_PRIORITY = 2
_TASK_REQUIRE_RESULT = True
_DEAL_STAGE_ID = "C102:FINAL_INVOICE"
_DEAL_TYPE_ID = "SALE"
_DEAL_SOURCE_ID = "PARTNER"
_DEAL_SERVICE_FIELD = "UF_CRM_1640764372166"
_DEAL_SERVICE_VALUE = 2558
_DEAL_SUBJECT_FIELD = "UF_CRM_1640765412209"
_DEAL_SUBJECT_VALUE = 174
_DEAL_PAYMENT_FIELD = "UF_CRM_AMO_586713"
_DEAL_PAYMENT_VALUE = 544
_DEAL_CITY_FIELD = "UF_CRM_AMO_631688"
_DEAL_CITY_VALUE = "Киров"
_DEAL_DIRECTION_FIELD = "UF_CRM_1680515310897"
_DEAL_DIRECTION_VALUE = 4818
_DEAL_ADDRESS = "Киров"
_DEAL_PRODUCT_NAME = "Услуга по вывозу мусора из контейнера 8м3"
_task_webhook_missing_logged = False
_deal_webhook_missing_logged = False


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
    invoice_number: str,
    invoice_date: date | datetime | None = None,
    bitrix_company_id: int | None = None,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
    pdf_url: str | None = None,
    invoice_items: list[dict[str, Any]] | None = None,
) -> str | None:
    """Создаёт задачу в Bitrix24 по факту выставления счёта."""
    if not _is_task_webhook_configured():
        return None

    invoice_amount = _calculate_invoice_amount(invoice_items)
    text = _build_task_description(
        counterparty_name=counterparty_name,
        invoice_number=invoice_number,
        invoice_amount=invoice_amount,
        tbank_invoice_id=tbank_invoice_id,
        invoice_link=invoice_link,
        pdf_url=pdf_url,
    )
    deadline = datetime.now().astimezone() + timedelta(days=1)
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
            )
        except Exception as e:
            logger.error("Bitrix24 deal: ошибка создания сделки по счёту %s — %s", invoice_number, e)

    crm_bindings = _build_task_crm_bindings(bitrix_company_id, deal_id)

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
            description_in_bbcode=True,
            require_result=_TASK_REQUIRE_RESULT,
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
        return task_url
    except Exception as e:
        logger.error("Bitrix24 task: ошибка создания задачи по счёту %s — %s", invoice_number, e)
        return None


def _build_task_description(
    *,
    counterparty_name: str,
    invoice_number: str,
    invoice_amount: Decimal | None = None,
    tbank_invoice_id: str | None = None,
    invoice_link: str | None = None,
    pdf_url: str | None = None,
) -> str:
    """
    Описание задачи для Bitrix24 в BBCode.

    DESCRIPTION_IN_BBCODE=Y нужен, чтобы [B]...[/B] отображался жирным.
    """
    lines = [
        f"[B]Контрагент[/B]: {counterparty_name}",
    ]
    if invoice_amount is not None:
        lines.append(f"[B]Сумма[/B]: {_format_money(invoice_amount)}")
    if tbank_invoice_id:
        lines.append(f"[B]T-Bank ID[/B]: {tbank_invoice_id}")
    if pdf_url:
        lines.append(f"[B]PDF[/B]: {pdf_url}")
    lines.extend(
        [
            "",
            "[B]Необходимо в ТБанке создать Акт для данного счета и отправить оба документа в ЭДО[/B]",
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
    deal_id = add_deal(
        title=_build_deal_title(invoice_date),
        company_id=company_id,
        opportunity=_format_amount_decimal(deal_amount),
        stage_id=_DEAL_STAGE_ID,
        type_id=_DEAL_TYPE_ID,
        source_id=_DEAL_SOURCE_ID,
        address=_DEAL_ADDRESS,
        custom_fields={
            _DEAL_SERVICE_FIELD: _DEAL_SERVICE_VALUE,
            _DEAL_SUBJECT_FIELD: _DEAL_SUBJECT_VALUE,
            _DEAL_PAYMENT_FIELD: _DEAL_PAYMENT_VALUE,
            _DEAL_CITY_FIELD: _DEAL_CITY_VALUE,
            _DEAL_DIRECTION_FIELD: _DEAL_DIRECTION_VALUE,
        },
        webhook_env_var=_DEAL_WEBHOOK_ENV,
    )
    product_rows = _build_deal_product_rows(invoice_items, fallback_amount=deal_amount)
    if product_rows:
        set_ok = set_deal_product_rows(
            deal_id=deal_id,
            rows=product_rows,
            webhook_env_var=_DEAL_WEBHOOK_ENV,
        )
        if not set_ok:
            logger.warning(
                "Bitrix24 deal: crm.deal.productrows.set вернул false для сделки %s (счёт %s)",
                deal_id,
                invoice_number,
            )

    logger.info(
        "Bitrix24 deal: создана сделка id=%s по счёту %s (company_id=%s, amount=%s)",
        deal_id,
        invoice_number,
        company_id,
        _format_amount_decimal(deal_amount),
    )
    return deal_id


def _build_deal_title(invoice_date: date | datetime | None) -> str:
    """Заголовок сделки в формате [dd.mm.yyyy] Вывоз бункеров."""
    if isinstance(invoice_date, datetime):
        dt = invoice_date.astimezone().date() if invoice_date.tzinfo else invoice_date.date()
    elif isinstance(invoice_date, date):
        dt = invoice_date
    else:
        dt = date.today()
    return f"[{dt.strftime('%d.%m.%Y')}] Вывоз бункеров"


def _build_deal_product_rows(
    invoice_items: list[dict[str, Any]] | None,
    *,
    fallback_amount: Decimal,
) -> list[dict[str, Any]]:
    """
    Формирует товарные строки сделки из счёта.

    Название товара фиксированное по требованиям CRM, а количество/цена/сумма
    берутся из позиций счёта.
    """
    rows: list[dict[str, Any]] = []
    for item in invoice_items or []:
        price = _try_decimal(item.get("price"))
        quantity = _try_decimal(item.get("amount"))
        if price is None or quantity is None or quantity <= 0:
            continue
        line_sum = (price * quantity).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        rows.append(
            {
                "PRODUCT_NAME": _DEAL_PRODUCT_NAME,
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
            "PRODUCT_NAME": _DEAL_PRODUCT_NAME,
            "PRICE": _format_amount_decimal(fallback_amount),
            "QUANTITY": "1",
            "SUM": _format_amount_decimal(fallback_amount),
        }
    ]


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
