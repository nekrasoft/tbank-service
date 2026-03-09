"""
Alembic env.py — подключение к БД из .env.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Добавляем корень проекта в path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

# Загрузка .env
_env_file = _project_root / ".env"
if _env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_file)

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context

from src.db.models import Base
from src.db.connection import _get_database_url

config = context.config
if config.config_file_name is not None:
    config.set_main_option("sqlalchemy.url", _get_database_url())

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Запуск миграций в offline режиме."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Запуск миграций в online режиме."""
    from sqlalchemy import create_engine
    connectable = create_engine(
        _get_database_url(),
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
