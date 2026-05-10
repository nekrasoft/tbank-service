"""Добавление volume в works

Revision ID: 019
Revises: 018
Create Date: 2026-05-10

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = inspector.get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def upgrade() -> None:
    if not _has_column("works", "volume"):
        op.add_column(
            "works",
            sa.Column(
                "volume",
                sa.Numeric(12, 2),
                nullable=True,
                comment="Объем работ, м3",
            ),
        )


def downgrade() -> None:
    if _has_column("works", "volume"):
        op.drop_column("works", "volume")
