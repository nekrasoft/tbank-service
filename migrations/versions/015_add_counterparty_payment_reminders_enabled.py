"""Добавление флага email-напоминаний об оплате

Revision ID: 015
Revises: 014
Create Date: 2026-04-30

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


def upgrade() -> None:
    if not _has_column("counterparties", "payment_reminders_enabled"):
        op.add_column(
            "counterparties",
            sa.Column(
                "payment_reminders_enabled",
                sa.Boolean(),
                nullable=False,
                server_default=sa.true(),
                comment="Включены email-напоминания о неоплаченных счетах",
            ),
        )


def downgrade() -> None:
    if _has_column("counterparties", "payment_reminders_enabled"):
        op.drop_column("counterparties", "payment_reminders_enabled")
