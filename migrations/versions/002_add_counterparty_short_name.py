"""Добавление short_name в counterparties

Revision ID: 002
Revises: 001
Create Date: 2025-03-09

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "counterparties",
        sa.Column("short_name", sa.String(255), nullable=True, comment="Короткое имя — для CLI, sheets, works"),
    )
    # Заполняем short_name из name для существующих записей
    op.execute("UPDATE counterparties SET short_name = name WHERE short_name IS NULL")
    op.alter_column(
        "counterparties",
        "short_name",
        existing_type=sa.String(255),
        nullable=False,
    )
    op.create_index(
        "ix_counterparties_short_name",
        "counterparties",
        ["short_name"],
        unique=False,
    )
    op.create_index(
        "ix_counterparties_short_name_note",
        "counterparties",
        ["short_name", "note"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_counterparties_short_name_note", table_name="counterparties")
    op.drop_index("ix_counterparties_short_name", table_name="counterparties")
    op.drop_column("counterparties", "short_name")
