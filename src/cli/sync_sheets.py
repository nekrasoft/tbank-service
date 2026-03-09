"""
CLI: синхронизация работ из Google Sheets в MySQL.
Запуск: python -m src.cli.sync_sheets
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

# Загрузка .env
_env = Path(__file__).resolve().parent.parent.parent / ".env"
if _env.exists():
    from dotenv import load_dotenv
    load_dotenv(_env)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Запуск синхронизации Sheets → MySQL."""
    from src.sheets.sync import sync_sheets_to_mysql

    try:
        added = sync_sheets_to_mysql()
        logger.info("Синхронизация завершена. Добавлено работ: %s", added)
    except Exception as e:
        logger.error("Ошибка синхронизации: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
