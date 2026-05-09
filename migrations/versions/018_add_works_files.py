"""Добавление файлов путевых листов к работам

Revision ID: 018
Revises: 017
Create Date: 2026-05-09

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def _inspector() -> sa.Inspector:
    bind = op.get_bind()
    return sa.inspect(bind)


def _has_table(table_name: str) -> bool:
    return table_name in _inspector().get_table_names()


def _has_index(table_name: str, index_name: str) -> bool:
    if not _has_table(table_name):
        return False
    indexes = _inspector().get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def upgrade() -> None:
    if not _has_table("works_files"):
        op.create_table(
            "works_files",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("file_token", sa.String(64), nullable=False),
            sa.Column("work_id", sa.Integer(), nullable=True),
            sa.Column("source", sa.String(20), nullable=False, comment="telegram или max"),
            sa.Column("source_chat_id", sa.String(64), nullable=True),
            sa.Column("source_user_id", sa.String(64), nullable=True),
            sa.Column("source_message_id", sa.String(128), nullable=True),
            sa.Column("source_file_id", sa.String(512), nullable=True),
            sa.Column("file_name", sa.String(255), nullable=True),
            sa.Column("content_type", sa.String(100), nullable=True),
            sa.Column("file_size", sa.BigInteger(), nullable=False),
            sa.Column("file_sha256", sa.String(64), nullable=False),
            sa.Column("file_data", mysql.MEDIUMBLOB(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("linked_at", sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(["work_id"], ["works.id"], ondelete="SET NULL"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("file_token", name="uq_works_files_file_token"),
        )

    if not _has_index("works_files", "ix_works_files_work_id"):
        op.create_index("ix_works_files_work_id", "works_files", ["work_id"], unique=False)
    if not _has_index("works_files", "ix_works_files_source"):
        op.create_index("ix_works_files_source", "works_files", ["source"], unique=False)


def downgrade() -> None:
    if _has_index("works_files", "ix_works_files_source"):
        op.drop_index("ix_works_files_source", table_name="works_files")
    if _has_index("works_files", "ix_works_files_work_id"):
        op.drop_index("ix_works_files_work_id", table_name="works_files")
    if _has_table("works_files"):
        op.drop_table("works_files")
