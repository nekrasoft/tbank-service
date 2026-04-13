"""Добавление связки invoices с сущностями Bitrix24

Revision ID: 011
Revises: 010
Create Date: 2026-04-13

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = inspector.get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def upgrade() -> None:
    if not _has_column("invoices", "bitrix_task_id"):
        op.add_column(
            "invoices",
            sa.Column(
                "bitrix_task_id",
                sa.Integer(),
                nullable=True,
                comment="ID задачи в Bitrix24",
            ),
        )
    if not _has_column("invoices", "bitrix_deal_id"):
        op.add_column(
            "invoices",
            sa.Column(
                "bitrix_deal_id",
                sa.Integer(),
                nullable=True,
                comment="ID сделки в Bitrix24",
            ),
        )


def downgrade() -> None:
    if _has_column("invoices", "bitrix_deal_id"):
        op.drop_column("invoices", "bitrix_deal_id")
    if _has_column("invoices", "bitrix_task_id"):
        op.drop_column("invoices", "bitrix_task_id")
