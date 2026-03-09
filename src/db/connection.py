"""
Подключение к MySQL через SQLAlchemy.
"""
from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

# Загрузка .env при импорте
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if _env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_path)

# Формирование URL подключения
def _get_database_url() -> str:
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    user = os.environ.get("MYSQL_USER", "tbank_service")
    password = os.environ.get("MYSQL_PASSWORD", "")
    database = os.environ.get("MYSQL_DATABASE", "tbank_invoicing")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


_engine: Engine | None = None
_SessionLocal: sessionmaker | None = None


def get_engine() -> Engine:
    """Получение движка SQLAlchemy."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            _get_database_url(),
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=os.environ.get("SQL_ECHO", "").lower() in ("1", "true", "yes"),
        )
    return _engine


def get_session_factory() -> sessionmaker:
    """Фабрика сессий."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _SessionLocal


def get_session() -> Session:
    """Новая сессия БД."""
    return get_session_factory()()
