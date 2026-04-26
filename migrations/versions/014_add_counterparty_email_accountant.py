"""Добавление email бухгалтера контрагента

Revision ID: 014
Revises: 013
Create Date: 2026-04-26

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "014"
down_revision = "013"
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
    if not _has_column("counterparties", "email_accountant"):
        op.add_column(
            "counterparties",
            sa.Column(
                "email_accountant",
                sa.String(length=255),
                nullable=True,
                comment="Email бухгалтера для напоминаний об оплате",
            ),
        )


def downgrade() -> None:
    if _has_column("counterparties", "email_accountant"):
        op.drop_column("counterparties", "email_accountant")
