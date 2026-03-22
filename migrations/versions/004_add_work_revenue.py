"""Добавление revenue в works

Revision ID: 004
Revises: 003
Create Date: 2026-03-22

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = inspector.get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def upgrade() -> None:
    if not _has_column("works", "revenue"):
        op.add_column(
            "works",
            sa.Column(
                "revenue",
                sa.Numeric(14, 2),
                nullable=True,
                comment="Выручка из Google Sheets",
            ),
        )


def downgrade() -> None:
    if _has_column("works", "revenue"):
        op.drop_column("works", "revenue")
