"""Добавление bitrix_company_id в counterparties

Revision ID: 006
Revises: 005
Create Date: 2026-03-30

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = inspector.get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def upgrade() -> None:
    if not _has_column("counterparties", "bitrix_company_id"):
        op.add_column(
            "counterparties",
            sa.Column(
                "bitrix_company_id",
                sa.Integer(),
                nullable=True,
                comment="ID компании в Bitrix24 CRM",
            ),
        )


def downgrade() -> None:
    if _has_column("counterparties", "bitrix_company_id"):
        op.drop_column("counterparties", "bitrix_company_id")

