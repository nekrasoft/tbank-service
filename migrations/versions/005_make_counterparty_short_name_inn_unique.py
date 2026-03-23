"""Уникальные индексы для short_name и inn в counterparties

Revision ID: 005
Revises: 004
Create Date: 2026-03-23

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def _get_index(table_name: str, index_name: str) -> dict | None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for idx in inspector.get_indexes(table_name):
        if idx.get("name") == index_name:
            return idx
    return None


def _assert_no_duplicates(column_name: str) -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            f"""
            SELECT {column_name}, COUNT(*) AS cnt
            FROM counterparties
            GROUP BY {column_name}
            HAVING COUNT(*) > 1
            LIMIT 5
            """
        )
    ).fetchall()
    if rows:
        duplicates = ", ".join(f"{row[0]} (x{row[1]})" for row in rows)
        raise RuntimeError(
            f"Найдены дубликаты в counterparties.{column_name}, "
            f"нельзя создать уникальный индекс: {duplicates}"
        )


def upgrade() -> None:
    _assert_no_duplicates("short_name")
    _assert_no_duplicates("inn")

    short_name_idx = _get_index("counterparties", "ix_counterparties_short_name")
    if short_name_idx is not None and not short_name_idx.get("unique", False):
        op.drop_index("ix_counterparties_short_name", table_name="counterparties")
        short_name_idx = None
    if short_name_idx is None:
        op.create_index("ix_counterparties_short_name", "counterparties", ["short_name"], unique=True)

    inn_idx = _get_index("counterparties", "ix_counterparties_inn")
    if inn_idx is not None and not inn_idx.get("unique", False):
        op.drop_index("ix_counterparties_inn", table_name="counterparties")
        inn_idx = None
    if inn_idx is None:
        op.create_index("ix_counterparties_inn", "counterparties", ["inn"], unique=True)


def downgrade() -> None:
    short_name_idx = _get_index("counterparties", "ix_counterparties_short_name")
    if short_name_idx is not None:
        op.drop_index("ix_counterparties_short_name", table_name="counterparties")
    op.create_index("ix_counterparties_short_name", "counterparties", ["short_name"], unique=False)

    inn_idx = _get_index("counterparties", "ix_counterparties_inn")
    if inn_idx is not None:
        op.drop_index("ix_counterparties_inn", table_name="counterparties")
