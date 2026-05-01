"""
Модели SQLAlchemy для MySQL.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Counterparty(Base):
    """Контрагент: ИНН, КПП, email для T-Bank API."""
    __tablename__ = "counterparties"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, index=True, comment="Полное имя — для API Т‑Банка, счетов, актов")
    short_name = Column(String(255), nullable=False, comment="Короткое имя — для CLI, sheets, works")
    inn = Column(String(12), nullable=False)
    kpp = Column(String(9), nullable=True)
    email = Column(String(255), nullable=True)
    email_accountant = Column(String(255), nullable=True, comment="Email бухгалтера для напоминаний об оплате")
    payment_reminders_enabled = Column(
        Boolean,
        nullable=False,
        default=True,
        server_default="1",
        comment="Включены email-напоминания о неоплаченных счетах",
    )
    phone = Column(String(20), nullable=True)
    bitrix_company_id = Column(Integer, nullable=True, comment="ID компании в Bitrix24 CRM")
    note = Column(String(255), nullable=True, comment="Примечание для матчинга с works")
    contract = Column(String(255), nullable=True, comment="Строка договора для комментария счёта")
    invoice_schedule = Column(
        String(20),
        nullable=False,
        default="2weeks",
        comment="Периодичность выставления: monthly, 2weeks, 10days, daily",
    )
    status = Column(
        String(20),
        nullable=False,
        default="active",
        comment="Статус контрагента: active, inactive",
    )
    operation_type = Column(
        String(50),
        nullable=True,
        default=None,
        comment="Тип операции по умолчанию: trip_removal, container_pickup",
    )
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_counterparties_name_note", "name", "note"),
        Index("ix_counterparties_short_name", "short_name", unique=True),
        Index("ix_counterparties_inn", "inn", unique=True),
        Index("ix_counterparties_short_name_note", "short_name", "note"),
    )


class Price(Base):
    """Прайс: цена за тип работы для контрагента."""
    __tablename__ = "prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    counterparty_id = Column(Integer, ForeignKey("counterparties.id", ondelete="CASCADE"), nullable=False)
    operation_type = Column(String(50), nullable=False, comment="container_pickup, trip_removal и т.д.")
    price = Column(Numeric(12, 2), nullable=False)
    vat = Column(String(10), nullable=True, default="None", comment="None, 0, 5, 7, 10, 18, 20, 22")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    counterparty = relationship("Counterparty", backref="prices")
    __table_args__ = (
        UniqueConstraint("counterparty_id", "operation_type", name="uq_prices_counterparty_operation"),
    )


class Work(Base):
    """Выполненная работа (синк из Google Sheets)."""
    __tablename__ = "works"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    counterparty_name = Column(String(255), nullable=False, index=True)
    note = Column(String(255), nullable=True)
    structure = Column(String(255), nullable=True)
    operation = Column(String(255), nullable=True)
    object_count = Column(String(50), nullable=True, comment="Количество (контейнеры, ходки и т.д.)")
    revenue = Column(Numeric(14, 2), nullable=True, comment="Выручка из Google Sheets")
    sheet_row_hash = Column(String(64), nullable=False, unique=True, comment="Дедупликация")
    invoice_id = Column(Integer, ForeignKey("invoices.id", ondelete="SET NULL"), nullable=True)  # noqa: E501 — Invoice определена ниже
    created_at = Column(DateTime, default=datetime.utcnow)

    invoice = relationship("Invoice", backref="works")
    __table_args__ = (
        Index("ix_works_invoice_null", "invoice_id"),
        Index("ix_works_counterparty_note", "counterparty_name", "note"),
    )


class Invoice(Base):
    """Выставленный счёт."""
    __tablename__ = "invoices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_number = Column(String(20), nullable=False)
    tbank_invoice_id = Column(String(100), nullable=True)
    counterparty_id = Column(Integer, ForeignKey("counterparties.id", ondelete="RESTRICT"), nullable=False)
    issued_at = Column(DateTime, nullable=False)
    due_date = Column(Date, nullable=True)
    status = Column(String(50), nullable=True, default="issued")
    paid_amount = Column(Numeric(14, 2), nullable=False, default=Decimal("0.00"))
    paid_at = Column(DateTime, nullable=True)
    payment_thank_email_sent_at = Column(
        DateTime,
        nullable=True,
        comment="Когда отправлено email-благодарность за оплату",
    )
    pdf_url = Column(String(500), nullable=True)
    payment_link = Column(String(500), nullable=True, comment="Ссылка на оплату счёта")
    recipient_emails_snapshot = Column(
        String(1000),
        nullable=True,
        comment="Email(ы), на которые счет реально отправлялся при выставлении",
    )
    bitrix_task_id = Column(Integer, nullable=True, comment="ID задачи в Bitrix24")
    bitrix_deal_id = Column(Integer, nullable=True, comment="ID сделки в Bitrix24")
    created_at = Column(DateTime, default=datetime.utcnow)

    counterparty = relationship("Counterparty", backref="invoices")
    items = relationship("InvoiceItem", backref="invoice", cascade="all, delete-orphan")
    __table_args__ = (
        Index("ix_invoices_payment_thank_email", "status", "paid_at", "payment_thank_email_sent_at"),
    )


class InvoiceItem(Base):
    """Позиция счёта."""
    __tablename__ = "invoice_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id = Column(Integer, ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(1000), nullable=False)
    price = Column(Numeric(12, 2), nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    unit = Column(String(50), nullable=False, default="шт")
    vat = Column(String(10), nullable=True, default="None")


class TBankStatementOperation(Base):
    """Операция из выписки T-Bank (raw + нормализованные поля + результат матчинга)."""
    __tablename__ = "tbank_statement_operations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_number = Column(String(32), nullable=False, index=True)
    dedupe_key = Column(String(191), nullable=False, unique=True)
    operation_id = Column(String(128), nullable=False, index=True)
    operation_status = Column(String(32), nullable=True)
    type_of_operation = Column(String(32), nullable=True)
    category = Column(String(64), nullable=True)
    operation_date = Column(DateTime, nullable=True, index=True)
    trxn_post_date = Column(DateTime, nullable=True)
    authorization_date = Column(DateTime, nullable=True)
    draw_date = Column(DateTime, nullable=True)
    charge_date = Column(DateTime, nullable=True)
    doc_date = Column(DateTime, nullable=True)
    document_number = Column(String(64), nullable=True)
    operation_amount = Column(Numeric(14, 2), nullable=True)
    account_amount = Column(Numeric(14, 2), nullable=True)
    ruble_amount = Column(Numeric(14, 2), nullable=True)
    description = Column(Text, nullable=True)
    pay_purpose = Column(Text, nullable=True)
    payer_name = Column(String(255), nullable=True)
    payer_inn = Column(String(12), nullable=True, index=True)
    payer_account = Column(String(32), nullable=True)
    receiver_name = Column(String(255), nullable=True)
    receiver_inn = Column(String(12), nullable=True, index=True)
    receiver_account = Column(String(32), nullable=True)
    counterparty_name = Column(String(255), nullable=True)
    counterparty_inn = Column(String(12), nullable=True)
    counterparty_account = Column(String(32), nullable=True)
    is_incoming = Column(Boolean, nullable=False, default=False)
    cashless_expense_sheet_synced_at = Column(DateTime, nullable=True)
    cashless_income_sheet_synced_at = Column(DateTime, nullable=True)
    matched_invoice_id = Column(Integer, ForeignKey("invoices.id", ondelete="SET NULL"), nullable=True)
    match_confidence = Column(Numeric(5, 4), nullable=True)
    match_method = Column(String(64), nullable=True)
    matched_at = Column(DateTime, nullable=True)
    raw_payload = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    matched_invoice = relationship("Invoice", backref="statement_operations")
    __table_args__ = (
        Index("ix_tbank_statement_ops_unmatched", "matched_invoice_id", "is_incoming"),
        Index("ix_tbank_statement_ops_account_operation_date", "account_number", "operation_date"),
        Index(
            "ix_tbank_statement_ops_expense_sheet_sync",
            "is_incoming",
            "cashless_expense_sheet_synced_at",
            "operation_date",
        ),
        Index(
            "ix_tbank_statement_ops_income_sheet_sync",
            "is_incoming",
            "cashless_income_sheet_synced_at",
            "operation_date",
        ),
    )


class TBankStatementSyncState(Base):
    """Состояние синка выписки по каждому счёту."""
    __tablename__ = "tbank_statement_sync_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_number = Column(String(32), nullable=False, unique=True, index=True)
    last_from = Column(DateTime, nullable=True)
    last_to = Column(DateTime, nullable=True)
    last_success_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class InvoiceNumberSeq(Base):
    """Последовательность номеров счетов (транзакционная нумерация)."""
    __tablename__ = "invoice_number_seq"

    id = Column(Integer, primary_key=True, autoincrement=True)
    year_month = Column(String(7), nullable=False, unique=True, comment="YYYY (legacy: YYYY-MM)")
    last_number = Column(Integer, nullable=False, default=0)


class InvoicePaymentReminder(Base):
    """Журнал отправок напоминаний об оплате по счетам."""
    __tablename__ = "invoice_payment_reminders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id = Column(Integer, ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False, index=True)
    channel = Column(String(20), nullable=False, default="email")
    schedule_offset_days = Column(Integer, nullable=False, comment="Шаг напоминания в днях после due_date")
    overdue_days_at_send = Column(Integer, nullable=True, comment="Фактическая просрочка в днях на момент отправки")
    recipient_snapshot = Column(String(1000), nullable=True, comment="Получатели, куда отправлено напоминание")
    status = Column(String(20), nullable=False, comment="sent, failed, skipped")
    error_text = Column(Text, nullable=True)
    sent_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    invoice = relationship("Invoice", backref="payment_reminders")
    __table_args__ = (
        Index("ix_invoice_payment_reminders_status_created_at", "status", "created_at"),
        Index(
            "ix_invoice_payment_reminders_invoice_channel_offset",
            "invoice_id",
            "channel",
            "schedule_offset_days",
        ),
    )
