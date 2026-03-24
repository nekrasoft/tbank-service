"""
CLI: синхронизация работ из Google Sheets в MySQL.
Запуск: python3 -m src.cli.sync_sheets
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
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


def _parse_date_arg(value: str) -> date:
    try:
        return datetime.strptime(value.strip(), "%d.%m.%Y").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Неверная дата '{value}', ожидается формат DD.MM.YYYY"
        ) from e


def main() -> None:
    """Запуск синхронизации Sheets → MySQL."""

    parser = argparse.ArgumentParser(description="Синхронизация работ из Google Sheets в MySQL")
    parser.add_argument(
        "--from-date",
        type=_parse_date_arg,
        default=None,
        help="Принудительно читать строки начиная с даты DD.MM.YYYY (для backfill выручки)",
    )
    args = parser.parse_args()

    from src.sheets.sync import sync_sheets_to_mysql

    try:
        added = sync_sheets_to_mysql(from_date=args.from_date)
        logger.info("Синхронизация завершена. Добавлено работ: %s", added)
    except Exception as e:
        logger.error("Ошибка синхронизации: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
