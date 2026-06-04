"""Добавление классификации расходов для дневной прибыли

Revision ID: 021
Revises: 020
Create Date: 2026-06-04

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "021"
down_revision = "020"
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
    statement_columns = [
        ("cashless_expense_business_date", sa.Column("cashless_expense_business_date", sa.Date(), nullable=True)),
        ("cashless_expense_structure_code", sa.Column("cashless_expense_structure_code", sa.String(length=16), nullable=True)),
        ("cashless_expense_structure_name", sa.Column("cashless_expense_structure_name", sa.String(length=255), nullable=True)),
        ("cashless_expense_operation_code", sa.Column("cashless_expense_operation_code", sa.String(length=16), nullable=True)),
        ("cashless_expense_operation_name", sa.Column("cashless_expense_operation_name", sa.String(length=255), nullable=True)),
        (
            "cashless_expense_classification_source",
            sa.Column("cashless_expense_classification_source", sa.String(length=32), nullable=True),
        ),
    ]
    for column_name, column in statement_columns:
        if not _has_column("tbank_statement_operations", column_name):
            op.add_column("tbank_statement_operations", column)

    if not _has_index("tbank_statement_operations", "ix_tbank_statement_ops_expense_profit"):
        op.create_index(
            "ix_tbank_statement_ops_expense_profit",
            "tbank_statement_operations",
            ["is_incoming", "cashless_expense_operation_code", "cashless_expense_business_date"],
            unique=False,
        )

    if not _has_table("daily_expense_allocations"):
        op.create_table(
            "daily_expense_allocations",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("statement_operation_id", sa.Integer(), nullable=False),
            sa.Column("expense_date", sa.Date(), nullable=False),
            sa.Column("expense_code", sa.String(length=16), nullable=False),
            sa.Column("expense_name", sa.String(length=255), nullable=False),
            sa.Column("amount", sa.Numeric(14, 2), nullable=False),
            sa.Column("allocation_days", sa.Integer(), nullable=False),
            sa.Column("allocation_method", sa.String(length=32), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(
                ["statement_operation_id"],
                ["tbank_statement_operations.id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "statement_operation_id",
                "expense_date",
                "expense_code",
                name="uq_daily_expense_allocations_operation_day_code",
            ),
        )

    if not _has_index("daily_expense_allocations", "ix_daily_expense_allocations_date_code"):
        op.create_index(
            "ix_daily_expense_allocations_date_code",
            "daily_expense_allocations",
            ["expense_date", "expense_code"],
            unique=False,
        )
    if not _has_index("daily_expense_allocations", "ix_daily_expense_allocations_code_date"):
        op.create_index(
            "ix_daily_expense_allocations_code_date",
            "daily_expense_allocations",
            ["expense_code", "expense_date"],
            unique=False,
        )
    if not _has_index("works", "ix_works_date"):
        op.create_index("ix_works_date", "works", ["date"], unique=False)


def downgrade() -> None:
    if _has_index("works", "ix_works_date"):
        op.drop_index("ix_works_date", table_name="works")

    if _has_index("daily_expense_allocations", "ix_daily_expense_allocations_code_date"):
        op.drop_index("ix_daily_expense_allocations_code_date", table_name="daily_expense_allocations")
    if _has_index("daily_expense_allocations", "ix_daily_expense_allocations_date_code"):
        op.drop_index("ix_daily_expense_allocations_date_code", table_name="daily_expense_allocations")
    if _has_table("daily_expense_allocations"):
        op.drop_table("daily_expense_allocations")

    if _has_index("tbank_statement_operations", "ix_tbank_statement_ops_expense_profit"):
        op.drop_index("ix_tbank_statement_ops_expense_profit", table_name="tbank_statement_operations")

    for column_name in [
        "cashless_expense_classification_source",
        "cashless_expense_operation_name",
        "cashless_expense_operation_code",
        "cashless_expense_structure_name",
        "cashless_expense_structure_code",
        "cashless_expense_business_date",
    ]:
        if _has_column("tbank_statement_operations", column_name):
            op.drop_column("tbank_statement_operations", column_name)
