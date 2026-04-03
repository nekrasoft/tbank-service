"""Добавление status и operation_type в counterparties

Revision ID: 008
Revises: 007
Create Date: 2026-04-03

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = inspector.get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def upgrade() -> None:
    if not _has_column("counterparties", "status"):
        op.add_column(
            "counterparties",
            sa.Column(
                "status",
                sa.String(20),
                nullable=False,
                server_default="active",
                comment="Статус контрагента: active, inactive",
            ),
        )

    if not _has_column("counterparties", "operation_type"):
        op.add_column(
            "counterparties",
            sa.Column(
                "operation_type",
                sa.String(50),
                nullable=True,
                comment="Тип операции по умолчанию: trip_removal, container_pickup",
            ),
        )


def downgrade() -> None:
    if _has_column("counterparties", "operation_type"):
        op.drop_column("counterparties", "operation_type")

    if _has_column("counterparties", "status"):
        op.drop_column("counterparties", "status")
