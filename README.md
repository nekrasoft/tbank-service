# tbank-service

Сервис интеграции с T-Bank для автоматического формирования и выставления счетов.

## Возможности

- Синхронизация выполненных работ из Google Sheets в MySQL
- Выставление счетов через T-Bank API
- Генерация акта (PDF)
- Отправка в Telegram и MAX бухгалтерам
- Cron: автоматическое выставление в последний день месяца
- Ручное выставление счёта по запросу

## Установка

```bash
# Требуется Python 3.11+
python3 -m pip install -r requirements.txt
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
python3 -m src.cli.sync_sheets

# Выставить счёт вручную
python3 -m src.cli.manual --counterparty "Алтай-Строй"

# Cron (последний день месяца)
python3 -m src.cli.cron
```

## Override Email Для Отладки

- Если в `.env` задан `DEBUG_FORCE_EMAIL`, все счета отправляются на этот адрес.
- Если `DEBUG_FORCE_EMAIL` пустой, используется email контрагента из `counterparties.email`.

## Периодичность Выставления

В таблице `counterparties` используется поле `invoice_schedule`:

- `monthly` — выставление в последний день месяца после 21:00
- `2weeks` — выставление в последний день месяца и 15-го числа утром (08:00-11:59)
- `daily` — выставление в любой запуск крона при появлении новых работ

## Выручка Из Google Sheets

- Колонка `Выручка` синхронизируется в `works.revenue`.
- При сборке счёта приоритет такой:
  - если у работы есть `Выручка`, в сумму берётся она;
  - если `Выручка` пустая/невалидная, сумма считается по `prices`.

## Отбор Работ В Счёт По Дате

- По умолчанию берутся все невыставленные работы контрагента (`invoice_id IS NULL`) с верхней границей `work.date <= дата запуска`.
- Чтобы включить строгий период, задайте `INVOICE_STRICT_PERIOD=true`:
  - `monthly` — с 1-го числа текущего месяца
  - `2weeks` — с 1-го по 15-е или с 16-го по конец месяца
  - `daily` — только текущий день
- При `INVOICE_WARN_OUT_OF_PERIOD=true` в лог пишется предупреждение, если есть старые невыставленные работы до начала strict-периода.

## Структура

- `src/db/` — модели, репозитории, подключение к MySQL
- `src/sheets/` — чтение и синхронизация из Google Sheets
- `src/tbank/` — клиент T-Bank API
- `src/invoice/` — сборка счёта, генерация акта
- `src/notifications/` — отправка в Telegram и MAX
- `src/cli/` — точки входа (cron, manual, sync_sheets)
