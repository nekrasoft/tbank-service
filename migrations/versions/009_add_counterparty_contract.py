"""Добавление contract в counterparties

Revision ID: 009
Revises: 008
Create Date: 2026-04-13

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = inspector.get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def upgrade() -> None:
    if not _has_column("counterparties", "contract"):
        op.add_column(
            "counterparties",
            sa.Column(
                "contract",
                sa.String(255),
                nullable=True,
                comment="Строка договора для комментария счёта",
            ),
        )


def downgrade() -> None:
    if _has_column("counterparties", "contract"):
        op.drop_column("counterparties", "contract")
