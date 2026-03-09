"""
Модели SQLAlchemy для MySQL.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
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
    name = Column(String(255), nullable=False, index=True)
    inn = Column(String(12), nullable=False)
    kpp = Column(String(9), nullable=True)
    email = Column(String(255), nullable=True)
    phone = Column(String(20), nullable=True)
    note = Column(String(255), nullable=True, comment="Примечание для матчинга с works")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_counterparties_name_note", "name", "note"),
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
    invoice_number = Column(String(20), nullable=False, unique=True)
    tbank_invoice_id = Column(String(100), nullable=True)
    counterparty_id = Column(Integer, ForeignKey("counterparties.id", ondelete="RESTRICT"), nullable=False)
    issued_at = Column(DateTime, nullable=False)
    due_date = Column(Date, nullable=True)
    status = Column(String(50), nullable=True, default="issued")
    pdf_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    counterparty = relationship("Counterparty", backref="invoices")
    items = relationship("InvoiceItem", backref="invoice", cascade="all, delete-orphan")


class InvoiceItem(Base):
    """Позиция счёта."""
    __tablename__ = "invoice_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_id = Column(Integer, ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(1000), nullable=False)
    price = Column(Numeric(12, 2), nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    unit = Column(String(50), nullable=False, default="ед.")
    vat = Column(String(10), nullable=True, default="None")


class InvoiceNumberSeq(Base):
    """Последовательность номеров счетов (транзакционная нумерация)."""
    __tablename__ = "invoice_number_seq"

    id = Column(Integer, primary_key=True, autoincrement=True)
    year_month = Column(String(7), nullable=False, unique=True, comment="YYYY-MM")
    last_number = Column(Integer, nullable=False, default=0)
