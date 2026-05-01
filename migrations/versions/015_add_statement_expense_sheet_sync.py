"""Добавление отметки синка расходов выписки в Google Sheets

Revision ID: 015
Revises: 014
Create Date: 2026-05-01

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "015"
down_revision = "014"
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


def upgrade() -> None:
    if not _has_column("tbank_statement_operations", "cashless_expense_sheet_synced_at"):
        op.add_column(
            "tbank_statement_operations",
            sa.Column("cashless_expense_sheet_synced_at", sa.DateTime(), nullable=True),
        )

    if not _has_index("tbank_statement_operations", "ix_tbank_statement_ops_expense_sheet_sync"):
        op.create_index(
            "ix_tbank_statement_ops_expense_sheet_sync",
            "tbank_statement_operations",
            ["is_incoming", "cashless_expense_sheet_synced_at", "operation_date"],
            unique=False,
        )


def downgrade() -> None:
    if _has_index("tbank_statement_operations", "ix_tbank_statement_ops_expense_sheet_sync"):
        op.drop_index("ix_tbank_statement_ops_expense_sheet_sync", table_name="tbank_statement_operations")

    if _has_column("tbank_statement_operations", "cashless_expense_sheet_synced_at"):
        op.drop_column("tbank_statement_operations", "cashless_expense_sheet_synced_at")
