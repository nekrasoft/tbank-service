"""Добавление индекса по номеру счета

Revision ID: 020
Revises: 019
Create Date: 2026-05-22

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def _inspector() -> sa.Inspector:
    bind = op.get_bind()
    return sa.inspect(bind)


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names()


def _has_index(table_name: str, index_name: str) -> bool:
    if not _has_table(table_name):
        return False
    indexes = _inspector().get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def upgrade() -> None:
    if not _has_index("invoices", "ix_invoices_invoice_number"):
        op.create_index(
            "ix_invoices_invoice_number",
            "invoices",
            ["invoice_number"],
            unique=False,
        )


def downgrade() -> None:
    if _has_index("invoices", "ix_invoices_invoice_number"):
        op.drop_index("ix_invoices_invoice_number", table_name="invoices")
