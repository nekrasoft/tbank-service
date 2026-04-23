"""Добавление напоминаний об оплате счетов

Revision ID: 012
Revises: 011
Create Date: 2026-04-23

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "012"
down_revision = "011"
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
    if not _has_column("invoices", "payment_link"):
        op.add_column(
            "invoices",
            sa.Column("payment_link", sa.String(length=500), nullable=True, comment="Ссылка на оплату счёта"),
        )

    if not _has_column("invoices", "recipient_emails_snapshot"):
        op.add_column(
            "invoices",
            sa.Column(
                "recipient_emails_snapshot",
                sa.String(length=1000),
                nullable=True,
                comment="Email(ы), на которые счет реально отправлялся при выставлении",
            ),
        )

    if not _has_table("invoice_payment_reminders"):
        op.create_table(
            "invoice_payment_reminders",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("invoice_id", sa.Integer(), nullable=False),
            sa.Column("channel", sa.String(length=20), nullable=False),
            sa.Column("schedule_offset_days", sa.Integer(), nullable=False),
            sa.Column("overdue_days_at_send", sa.Integer(), nullable=True),
            sa.Column("recipient_snapshot", sa.String(length=1000), nullable=True),
            sa.Column("status", sa.String(length=20), nullable=False),
            sa.Column("error_text", sa.Text(), nullable=True),
            sa.Column("sent_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(["invoice_id"], ["invoices.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
        )

    if not _has_index("invoice_payment_reminders", "ix_invoice_payment_reminders_invoice_id"):
        op.create_index(
            "ix_invoice_payment_reminders_invoice_id",
            "invoice_payment_reminders",
            ["invoice_id"],
            unique=False,
        )

    if not _has_index("invoice_payment_reminders", "ix_invoice_payment_reminders_status_created_at"):
        op.create_index(
            "ix_invoice_payment_reminders_status_created_at",
            "invoice_payment_reminders",
            ["status", "created_at"],
            unique=False,
        )

    if not _has_index("invoice_payment_reminders", "ix_invoice_payment_reminders_invoice_channel_offset"):
        op.create_index(
            "ix_invoice_payment_reminders_invoice_channel_offset",
            "invoice_payment_reminders",
            ["invoice_id", "channel", "schedule_offset_days"],
            unique=False,
        )


def downgrade() -> None:
    if _has_table("invoice_payment_reminders"):
        for idx in (
            "ix_invoice_payment_reminders_invoice_channel_offset",
            "ix_invoice_payment_reminders_status_created_at",
            "ix_invoice_payment_reminders_invoice_id",
        ):
            if _has_index("invoice_payment_reminders", idx):
                op.drop_index(idx, table_name="invoice_payment_reminders")
        op.drop_table("invoice_payment_reminders")

    if _has_column("invoices", "recipient_emails_snapshot"):
        op.drop_column("invoices", "recipient_emails_snapshot")

    if _has_column("invoices", "payment_link"):
        op.drop_column("invoices", "payment_link")
