"""Добавление invoice_schedule в counterparties

Revision ID: 003
Revises: 002
Create Date: 2026-03-22

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "counterparties",
        sa.Column(
            "invoice_schedule",
            sa.String(20),
            nullable=True,
            comment="Периодичность выставления: monthly, 2weeks, daily",
        ),
    )
    op.execute(
        "UPDATE counterparties "
        "SET invoice_schedule = 'monthly' "
        "WHERE invoice_schedule IS NULL OR invoice_schedule = ''"
    )
    op.alter_column(
        "counterparties",
        "invoice_schedule",
        existing_type=sa.String(20),
        nullable=False,
    )


def downgrade() -> None:
    op.drop_column("counterparties", "invoice_schedule")
