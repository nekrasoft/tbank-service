"""Добавление учёта выписки и статусов оплаты счетов

Revision ID: 010
Revises: 009
Create Date: 2026-04-13

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "010"
down_revision = "009"
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
    if not _has_column("invoices", "paid_amount"):
        op.add_column(
            "invoices",
            sa.Column("paid_amount", sa.Numeric(14, 2), nullable=False, server_default="0.00"),
        )
    if not _has_column("invoices", "paid_at"):
        op.add_column("invoices", sa.Column("paid_at", sa.DateTime(), nullable=True))

    if not _has_table("tbank_statement_operations"):
        op.create_table(
            "tbank_statement_operations",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("account_number", sa.String(length=32), nullable=False),
            sa.Column("dedupe_key", sa.String(length=191), nullable=False),
            sa.Column("operation_id", sa.String(length=128), nullable=False),
            sa.Column("operation_status", sa.String(length=32), nullable=True),
            sa.Column("type_of_operation", sa.String(length=32), nullable=True),
            sa.Column("category", sa.String(length=64), nullable=True),
            sa.Column("operation_date", sa.DateTime(), nullable=True),
            sa.Column("trxn_post_date", sa.DateTime(), nullable=True),
            sa.Column("authorization_date", sa.DateTime(), nullable=True),
            sa.Column("draw_date", sa.DateTime(), nullable=True),
            sa.Column("charge_date", sa.DateTime(), nullable=True),
            sa.Column("doc_date", sa.DateTime(), nullable=True),
            sa.Column("document_number", sa.String(length=64), nullable=True),
            sa.Column("operation_amount", sa.Numeric(14, 2), nullable=True),
            sa.Column("account_amount", sa.Numeric(14, 2), nullable=True),
            sa.Column("ruble_amount", sa.Numeric(14, 2), nullable=True),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("pay_purpose", sa.Text(), nullable=True),
            sa.Column("payer_name", sa.String(length=255), nullable=True),
            sa.Column("payer_inn", sa.String(length=12), nullable=True),
            sa.Column("payer_account", sa.String(length=32), nullable=True),
            sa.Column("receiver_name", sa.String(length=255), nullable=True),
            sa.Column("receiver_inn", sa.String(length=12), nullable=True),
            sa.Column("receiver_account", sa.String(length=32), nullable=True),
            sa.Column("counterparty_name", sa.String(length=255), nullable=True),
            sa.Column("counterparty_inn", sa.String(length=12), nullable=True),
            sa.Column("counterparty_account", sa.String(length=32), nullable=True),
            sa.Column("is_incoming", sa.Boolean(), nullable=False, server_default=sa.text("0")),
            sa.Column("matched_invoice_id", sa.Integer(), nullable=True),
            sa.Column("match_confidence", sa.Numeric(5, 4), nullable=True),
            sa.Column("match_method", sa.String(length=64), nullable=True),
            sa.Column("matched_at", sa.DateTime(), nullable=True),
            sa.Column("raw_payload", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(["matched_invoice_id"], ["invoices.id"], ondelete="SET NULL"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("dedupe_key", name="uq_tbank_statement_operations_dedupe_key"),
        )

    if not _has_index("tbank_statement_operations", "ix_tbank_statement_operations_account_number"):
        op.create_index(
            "ix_tbank_statement_operations_account_number",
            "tbank_statement_operations",
            ["account_number"],
            unique=False,
        )
    if not _has_index("tbank_statement_operations", "ix_tbank_statement_operations_operation_id"):
        op.create_index(
            "ix_tbank_statement_operations_operation_id",
            "tbank_statement_operations",
            ["operation_id"],
            unique=False,
        )
    if not _has_index("tbank_statement_operations", "ix_tbank_statement_operations_operation_date"):
        op.create_index(
            "ix_tbank_statement_operations_operation_date",
            "tbank_statement_operations",
            ["operation_date"],
            unique=False,
        )
    if not _has_index("tbank_statement_operations", "ix_tbank_statement_operations_payer_inn"):
        op.create_index(
            "ix_tbank_statement_operations_payer_inn",
            "tbank_statement_operations",
            ["payer_inn"],
            unique=False,
        )
    if not _has_index("tbank_statement_operations", "ix_tbank_statement_operations_receiver_inn"):
        op.create_index(
            "ix_tbank_statement_operations_receiver_inn",
            "tbank_statement_operations",
            ["receiver_inn"],
            unique=False,
        )
    if not _has_index("tbank_statement_operations", "ix_tbank_statement_ops_unmatched"):
        op.create_index(
            "ix_tbank_statement_ops_unmatched",
            "tbank_statement_operations",
            ["matched_invoice_id", "is_incoming"],
            unique=False,
        )
    if not _has_index("tbank_statement_operations", "ix_tbank_statement_ops_account_operation_date"):
        op.create_index(
            "ix_tbank_statement_ops_account_operation_date",
            "tbank_statement_operations",
            ["account_number", "operation_date"],
            unique=False,
        )

    if not _has_table("tbank_statement_sync_state"):
        op.create_table(
            "tbank_statement_sync_state",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("account_number", sa.String(length=32), nullable=False),
            sa.Column("last_from", sa.DateTime(), nullable=True),
            sa.Column("last_to", sa.DateTime(), nullable=True),
            sa.Column("last_success_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("account_number", name="uq_tbank_statement_sync_state_account_number"),
        )

def downgrade() -> None:
    if _has_table("tbank_statement_sync_state"):
        op.drop_table("tbank_statement_sync_state")

    if _has_table("tbank_statement_operations"):
        for idx in (
            "ix_tbank_statement_ops_account_operation_date",
            "ix_tbank_statement_ops_unmatched",
            "ix_tbank_statement_operations_receiver_inn",
            "ix_tbank_statement_operations_payer_inn",
            "ix_tbank_statement_operations_operation_date",
            "ix_tbank_statement_operations_operation_id",
            "ix_tbank_statement_operations_account_number",
        ):
            if _has_index("tbank_statement_operations", idx):
                op.drop_index(idx, table_name="tbank_statement_operations")
        op.drop_table("tbank_statement_operations")

    if _has_column("invoices", "paid_at"):
        op.drop_column("invoices", "paid_at")
    if _has_column("invoices", "paid_amount"):
        op.drop_column("invoices", "paid_amount")
