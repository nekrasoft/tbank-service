# tbank-service

Сервис интеграции с T-Bank для автоматического формирования и выставления счетов.

## Возможности

- Синхронизация выполненных работ из Google Sheets в MySQL
- Выставление счетов через T-Bank API
- Генерация акта (PDF)
- Отправка в Telegram бухгалтерам
- Cron: автоматическое выставление в последний день месяца
- Ручное выставление счёта по запросу

## Установка

```bash
pip install -r requirements.txt
cp .env.example .env
# Заполнить .env
```

## Настройка MySQL

```sql
CREATE DATABASE tbank_invoicing;
CREATE USER 'tbank_service'@'localhost' IDENTIFIED BY '...';
GRANT ALL ON tbank_invoicing.* TO 'tbank_service'@'localhost';
```

## Миграции

```bash
alembic upgrade head
```

## Использование

```bash
# Синхронизация работ из Google Sheets
python -m src.cli.sync_sheets

# Выставить счёт вручную
python -m src.cli.manual --counterparty "Алтай-Строй" --note "Ердякова 9"

# Cron (последний день месяца)
python -m src.cli.cron
```

## Структура

- `src/db/` — модели, репозитории, подключение к MySQL
- `src/sheets/` — чтение и синхронизация из Google Sheets
- `src/tbank/` — клиент T-Bank API
- `src/invoice/` — сборка счёта, генерация акта
- `src/notifications/` — отправка в Telegram
- `src/cli/` — точки входа (cron, manual, sync_sheets)
