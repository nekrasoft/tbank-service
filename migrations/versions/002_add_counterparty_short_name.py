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


def _column_exists(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _index_exists(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    if not _column_exists("counterparties", "short_name"):
        op.add_column(
            "counterparties",
            sa.Column("short_name", sa.String(255), nullable=True, comment="Короткое имя — для CLI, sheets, works"),
        )
    # Заполняем short_name из name для существующих записей
    op.execute("UPDATE counterparties SET short_name = name WHERE short_name IS NULL OR short_name = ''")
    if _column_exists("counterparties", "short_name"):
        op.alter_column(
            "counterparties",
            "short_name",
            existing_type=sa.String(255),
            nullable=False,
        )
    if not _index_exists("counterparties", "ix_counterparties_short_name"):
        op.create_index(
            "ix_counterparties_short_name",
            "counterparties",
            ["short_name"],
            unique=False,
        )
    if not _index_exists("counterparties", "ix_counterparties_short_name_note"):
        op.create_index(
            "ix_counterparties_short_name_note",
            "counterparties",
            ["short_name", "note"],
            unique=False,
        )


def downgrade() -> None:
    if _index_exists("counterparties", "ix_counterparties_short_name_note"):
        op.drop_index("ix_counterparties_short_name_note", table_name="counterparties")
    if _index_exists("counterparties", "ix_counterparties_short_name"):
        op.drop_index("ix_counterparties_short_name", table_name="counterparties")
    if _column_exists("counterparties", "short_name"):
        op.drop_column("counterparties", "short_name")
