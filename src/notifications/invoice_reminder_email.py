"""Email-уведомления клиентам о просроченной оплате счета."""
from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from email.message import EmailMessage
import logging
import os
import re
import smtplib

logger = logging.getLogger(__name__)

_EMAIL_SPLIT_RE = re.compile(r"[,;\n]+")


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on", "y")


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


def normalize_emails(value: str | list[str] | None) -> list[str]:
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


def _format_money(value: Decimal) -> str:
    normalized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    text = f"{normalized:.2f}".replace(".", ",")
    return f"{text} руб."


def build_invoice_payment_reminder_subject(*, invoice_number: str) -> str:
    """Тема письма-напоминания."""
    return f"Напоминание об оплате счета №{invoice_number}"


def build_invoice_payment_reminder_text(
    *,
    counterparty_name: str,
    invoice_number: str,
    due_date: date,
    overdue_days: int,
    total_amount: Decimal,
    payment_link: str | None,
    pdf_url: str | None,
) -> str:
    """Текст email-напоминания клиенту."""
    lines = [
        f"Здравствуйте, {counterparty_name}.",
        "",
        f"Напоминаем об оплате счета №{invoice_number}.",
        f"Срок оплаты: {due_date.strftime('%d.%m.%Y')}",
        f"Просрочка: {overdue_days} дн.",
        f"Сумма к оплате: {_format_money(total_amount)}",
    ]

    if payment_link:
        lines.extend([
            "",
            f"Ссылка для оплаты: {payment_link}",
        ])
    if pdf_url:
        if not payment_link:
            lines.append("")
        lines.append(f"Счет (PDF): {pdf_url}")

    lines.extend([
        "",
        "Если оплата уже выполнена, пожалуйста, проигнорируйте это письмо.",
    ])
    return "\n".join(lines)


def send_invoice_payment_reminder(
    *,
    recipients: list[str],
    invoice_number: str,
    counterparty_name: str,
    due_date: date,
    overdue_days: int,
    total_amount: Decimal,
    payment_link: str | None,
    pdf_url: str | None,
) -> None:
    """Отправка email-напоминания клиенту о неоплаченном счёте."""
    normalized_recipients = normalize_emails(recipients)
    if not normalized_recipients:
        raise ValueError("Нет валидных email получателей для отправки напоминания")

    host = (os.environ.get("INVOICE_REMINDER_EMAIL_SMTP_HOST") or "").strip()
    if not host:
        raise ValueError("Задайте INVOICE_REMINDER_EMAIL_SMTP_HOST в .env")

    port = _env_int(
        "INVOICE_REMINDER_EMAIL_SMTP_PORT",
        587,
        min_value=1,
        max_value=65535,
    )
    use_tls = _env_bool("INVOICE_REMINDER_EMAIL_SMTP_USE_TLS", True)
    use_ssl = _env_bool("INVOICE_REMINDER_EMAIL_SMTP_USE_SSL", False)
    if use_tls and use_ssl:
        raise ValueError("INVOICE_REMINDER_EMAIL_SMTP_USE_TLS и USE_SSL не могут быть одновременно включены")

    timeout_sec = _env_int(
        "INVOICE_REMINDER_EMAIL_SMTP_TIMEOUT_SEC",
        20,
        min_value=5,
        max_value=120,
    )
    smtp_debug = _env_bool("INVOICE_REMINDER_EMAIL_SMTP_DEBUG", False)

    from_email = (os.environ.get("INVOICE_REMINDER_EMAIL_FROM") or "").strip()
    if not from_email:
        raise ValueError("Задайте INVOICE_REMINDER_EMAIL_FROM в .env")

    smtp_user = (os.environ.get("INVOICE_REMINDER_EMAIL_SMTP_USER") or "").strip() or None
    smtp_password = (os.environ.get("INVOICE_REMINDER_EMAIL_SMTP_PASSWORD") or "").strip() or None
    reply_to = (os.environ.get("INVOICE_REMINDER_EMAIL_REPLY_TO") or "").strip() or None

    subject = build_invoice_payment_reminder_subject(invoice_number=invoice_number)
    text = build_invoice_payment_reminder_text(
        counterparty_name=counterparty_name,
        invoice_number=invoice_number,
        due_date=due_date,
        overdue_days=overdue_days,
        total_amount=total_amount,
        payment_link=payment_link,
        pdf_url=pdf_url,
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(normalized_recipients)
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(text)

    logger.info(
        "Email reminder SMTP config: host=%s port=%s tls=%s ssl=%s user=%s timeout=%ss debug=%s",
        host,
        port,
        use_tls,
        use_ssl,
        "set" if smtp_user else "not_set",
        timeout_sec,
        smtp_debug,
    )

    smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    stage = "init"
    try:
        with smtp_cls(timeout=float(timeout_sec)) as smtp:
            if smtp_debug:
                smtp.set_debuglevel(1)

            stage = "connect"
            smtp.connect(host=host, port=port)

            stage = "ehlo"
            smtp.ehlo()

            if use_tls and not use_ssl:
                stage = "starttls"
                smtp.starttls()
                stage = "ehlo_after_starttls"
                smtp.ehlo()

            if smtp_user:
                stage = "login"
                smtp.login(smtp_user, smtp_password or "")

            stage = "send_message"
            smtp.send_message(msg)
    except TimeoutError as e:
        raise TimeoutError(
            "SMTP timeout: "
            f"stage={stage}, host={host}, port={port}, tls={use_tls}, ssl={use_ssl}, timeout={timeout_sec}s"
        ) from e
    except smtplib.SMTPException as e:
        raise RuntimeError(
            "SMTP error: "
            f"stage={stage}, host={host}, port={port}, tls={use_tls}, ssl={use_ssl}, detail={e}"
        ) from e

    logger.info(
        "Email reminder: отправлено напоминание по счету %s на %s",
        invoice_number,
        ", ".join(normalized_recipients),
    )
