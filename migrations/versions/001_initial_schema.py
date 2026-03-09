"""Начальная схема БД

Revision ID: 001
Revises:
Create Date: 2025-03-04

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # counterparties
    op.create_table(
        "counterparties",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("inn", sa.String(12), nullable=False),
        sa.Column("kpp", sa.String(9), nullable=True),
        sa.Column("email", sa.String(255), nullable=True),
        sa.Column("phone", sa.String(20), nullable=True),
        sa.Column("note", sa.String(255), nullable=True, comment="Примечание для матчинга с works"),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_counterparties_name", "counterparties", ["name"], unique=False)
    op.create_index("ix_counterparties_name_note", "counterparties", ["name", "note"], unique=False)

    # invoice_number_seq
    op.create_table(
        "invoice_number_seq",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("year_month", sa.String(7), nullable=False, comment="YYYY-MM"),
        sa.Column("last_number", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("year_month", name="uq_invoice_number_seq_year_month"),
    )

    # invoices (нужен counterparties)
    op.create_table(
        "invoices",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("invoice_number", sa.String(20), nullable=False),
        sa.Column("tbank_invoice_id", sa.String(100), nullable=True),
        sa.Column("counterparty_id", sa.Integer(), nullable=False),
        sa.Column("issued_at", sa.DateTime(), nullable=False),
        sa.Column("due_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(50), nullable=True),
        sa.Column("pdf_url", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["counterparty_id"], ["counterparties.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("invoice_number", name="uq_invoices_invoice_number"),
    )
    op.create_index("ix_invoices_counterparty_id", "invoices", ["counterparty_id"], unique=False)

    # invoice_items
    op.create_table(
        "invoice_items",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("invoice_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(1000), nullable=False),
        sa.Column("price", sa.Numeric(12, 2), nullable=False),
        sa.Column("amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("unit", sa.String(50), nullable=False),
        sa.Column("vat", sa.String(10), nullable=True),
        sa.ForeignKeyConstraint(["invoice_id"], ["invoices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    # works (FK to invoices)
    op.create_table(
        "works",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("counterparty_name", sa.String(255), nullable=False),
        sa.Column("note", sa.String(255), nullable=True),
        sa.Column("structure", sa.String(255), nullable=True),
        sa.Column("operation", sa.String(255), nullable=True),
        sa.Column("object_count", sa.String(50), nullable=True, comment="Количество (контейнеры, ходки и т.д.)"),
        sa.Column("sheet_row_hash", sa.String(64), nullable=False, comment="Дедупликация"),
        sa.Column("invoice_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["invoice_id"], ["invoices.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sheet_row_hash", name="uq_works_sheet_row_hash"),
    )
    op.create_index("ix_works_counterparty_name", "works", ["counterparty_name"], unique=False)
    op.create_index("ix_works_invoice_id", "works", ["invoice_id"], unique=False)
    op.create_index("ix_works_counterparty_note", "works", ["counterparty_name", "note"], unique=False)

    # prices
    op.create_table(
        "prices",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("counterparty_id", sa.Integer(), nullable=False),
        sa.Column("operation_type", sa.String(50), nullable=False, comment="container_pickup, trip_removal и т.д."),
        sa.Column("price", sa.Numeric(12, 2), nullable=False),
        sa.Column("vat", sa.String(10), nullable=True, comment="None, 0, 5, 7, 10, 18, 20, 22"),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["counterparty_id"], ["counterparties.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("counterparty_id", "operation_type", name="uq_prices_counterparty_operation"),
    )


def downgrade() -> None:
    op.drop_table("prices")
    op.drop_index("ix_works_counterparty_note", table_name="works")
    op.drop_index("ix_works_invoice_id", table_name="works")
    op.drop_index("ix_works_counterparty_name", table_name="works")
    op.drop_table("works")
    op.drop_table("invoice_items")
    op.drop_index("ix_invoices_counterparty_id", table_name="invoices")
    op.drop_table("invoices")
    op.drop_table("invoice_number_seq")
    op.drop_index("ix_counterparties_name_note", table_name="counterparties")
    op.drop_index("ix_counterparties_name", table_name="counterparties")
    op.drop_table("counterparties")
