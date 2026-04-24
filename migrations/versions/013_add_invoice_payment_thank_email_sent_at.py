"""Добавление отметки email-благодарности за оплату

Revision ID: 013
Revises: 012
Create Date: 2026-04-24

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "013"
down_revision = "012"
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
    if not _has_column("invoices", "payment_thank_email_sent_at"):
        op.add_column(
            "invoices",
            sa.Column(
                "payment_thank_email_sent_at",
                sa.DateTime(),
                nullable=True,
                comment="Когда отправлено email-благодарность за оплату",
            ),
        )

    if not _has_index("invoices", "ix_invoices_payment_thank_email"):
        op.create_index(
            "ix_invoices_payment_thank_email",
            "invoices",
            ["status", "paid_at", "payment_thank_email_sent_at"],
            unique=False,
        )


def downgrade() -> None:
    if _has_index("invoices", "ix_invoices_payment_thank_email"):
        op.drop_index("ix_invoices_payment_thank_email", table_name="invoices")

    if _has_column("invoices", "payment_thank_email_sent_at"):
        op.drop_column("invoices", "payment_thank_email_sent_at")
