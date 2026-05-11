"""
Pytest fixtures for tbank-service tests.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass
class MockWork:
    """Mock for Work model — simple dataclass for testing."""
    id: int
    date: date
    counterparty_name: str
    note: str | None = None
    structure: str | None = None
    operation: str | None = None
    object_count: str | None = None
    volume: Decimal | None = None
    revenue: Decimal | None = None
    sheet_row_hash: str = ""
    invoice_id: int | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)


class MockInvoice:
    """Mock for Invoice model."""
    def __init__(
        self,
        id: int,
        counterparty_id: int,
        total: Decimal,
        status: str = "issued",
        paid_amount: Decimal = Decimal("0"),
    ):
        self.id = id
        self.counterparty_id = counterparty_id
        self.total = total
        self.status = status
        self.paid_amount = paid_amount


class MockCounterparty:
    """Mock for Counterparty model."""
    def __init__(
        self,
        id: int,
        inn: str,
        short_name: str,
        email: str | None = None,
        invoice_schedule: str = "2weeks",
    ):
        self.id = id
        self.inn = inn
        self.short_name = short_name
        self.email = email
        self.invoice_schedule = invoice_schedule
