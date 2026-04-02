"""Снятие глобальной уникальности номера счёта

Revision ID: 007
Revises: 006
Create Date: 2026-04-02

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def _get_unique_invoice_number_constraint_name() -> str | None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for uc in inspector.get_unique_constraints("invoices"):
        cols = uc.get("column_names") or []
        if cols == ["invoice_number"]:
            return uc.get("name")
    return None


def _get_unique_invoice_number_index_name() -> str | None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for idx in inspector.get_indexes("invoices"):
        cols = idx.get("column_names") or []
        if idx.get("unique", False) and cols == ["invoice_number"]:
            return idx.get("name")
    return None


def _has_unique_invoice_number() -> bool:
    return (
        _get_unique_invoice_number_constraint_name() is not None
        or _get_unique_invoice_number_index_name() is not None
    )


def _assert_no_duplicate_invoice_numbers() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT invoice_number, COUNT(*) AS cnt
            FROM invoices
            GROUP BY invoice_number
            HAVING COUNT(*) > 1
            LIMIT 5
            """
        )
    ).fetchall()
    if rows:
        duplicates = ", ".join(f"{row[0]} (x{row[1]})" for row in rows)
        raise RuntimeError(
            "Найдены дубликаты invoices.invoice_number, "
            "нельзя восстановить уникальность: "
            f"{duplicates}"
        )


def upgrade() -> None:
    uc_name = _get_unique_invoice_number_constraint_name()
    if uc_name:
        op.drop_constraint(uc_name, "invoices", type_="unique")
        return

    idx_name = _get_unique_invoice_number_index_name()
    if idx_name:
        op.drop_index(idx_name, table_name="invoices")


def downgrade() -> None:
    if _has_unique_invoice_number():
        return

    _assert_no_duplicate_invoice_numbers()
    op.create_unique_constraint(
        "uq_invoices_invoice_number",
        "invoices",
        ["invoice_number"],
    )
