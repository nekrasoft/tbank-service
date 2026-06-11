"""Добавление индекса по FK позиций счёта

Revision ID: 022
Revises: 021
Create Date: 2026-06-11

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def _inspector() -> sa.Inspector:
    bind = op.get_bind()
    return sa.inspect(bind)


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names()


def _has_column(table_name: str, column_name: str) -> bool:
    if not _has_table(table_name):
        return False
    columns = _inspector().get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def _has_index(table_name: str, index_name: str) -> bool:
    if not _has_table(table_name):
        return False
    indexes = _inspector().get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def _has_index_on_columns(table_name: str, column_names: list[str]) -> bool:
    if not _has_table(table_name):
        return False
    indexes = _inspector().get_indexes(table_name)
    return any(idx.get("column_names") == column_names for idx in indexes)


def upgrade() -> None:
    if (
        _has_column("invoice_items", "invoice_id")
        and not _has_index("invoice_items", "ix_invoice_items_invoice_id")
        and not _has_index_on_columns("invoice_items", ["invoice_id"])
    ):
        op.create_index(
            "ix_invoice_items_invoice_id",
            "invoice_items",
            ["invoice_id"],
            unique=False,
        )


def downgrade() -> None:
    if _has_index("invoice_items", "ix_invoice_items_invoice_id"):
        op.drop_index("ix_invoice_items_invoice_id", table_name="invoice_items")
