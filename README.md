# tbank-service

Сервис интеграции с T-Bank для автоматического формирования и выставления счетов.

## Возможности

- Синхронизация работ и контрагентов из Google Sheets в MySQL
- Выставление счетов через T-Bank API
- Генерация акта (PDF)
- Отправка в Telegram и MAX бухгалтерам
- Создание сделки и задачи в Bitrix24 после выставления счёта (cron/manual)
- Разбиение одного контрагента на несколько счетов по правилам из `config/invoice_split_rules.json`
- Cron: автоматическое выставление в последний день месяца
- Ручное выставление счёта по запросу
- Импорт контрагентов в Bitrix24 CRM

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
# Синхронизация данных из Google Sheets (контрагенты + работы)
python3 -m src.cli.sync_sheets

# Backfill выручки/данных начиная с даты
python3 -m src.cli.sync_sheets --from-date 01.03.2026

# Выставить счёт вручную
python3 -m src.cli.manual --counterparty "Алтай-Строй"
# Выставить вручную, игнорируя окно invoice_schedule
python3 -m src.cli.manual --counterparty "Алтай-Строй" --ignore-schedule-window
# Выставить вручную с нижней границей по дате работ
python3 -m src.cli.manual --counterparty "Алтай-Строй" --from-date 01.03.2026
# Выставить вручную с диапазоном дат работ
python3 -m src.cli.manual --counterparty "Алтай-Строй" --from-date 25.03.2026 --to-date 31.03.2026
# Превью счёта без записи в БД и отправки в T-Bank
python3 -m src.cli.manual --counterparty "Алтай-Строй" --dry-run
# Превью, включая уже выставленные работы (если попадают в диапазон)
python3 -m src.cli.manual --counterparty "Алтай-Строй" --dry-run --dry-run-include-issued --from-date 01.03.2026 --to-date 31.03.2026
# Dry-run + реальное создание сделки/задачи в Bitrix24 (без T-Bank, чатов, Sheets и записи в БД)
python3 -m src.cli.manual --counterparty "Алтай-Строй" --dry-run --dry-run-bitrix

# Cron (последний день месяца)
python3 -m src.cli.cron

# Импорт контрагентов в Bitrix24 CRM
python3 -m src.cli.import_counterparties_to_bitrix24
```

## Импорт Контрагентов В Bitrix24

- Для импорта используется входящий вебхук `BITRIX24_WEBHOOK_URL` и метод `crm.company.add`.
- Скрипт читает все записи из таблицы `counterparties`, создаёт компании в Bitrix24 и заполняет реквизиты организации (`RQ_INN`, `RQ_KPP`) через `crm.requisite.*`.
- Повторный запуск идемпотентный: перед созданием выполняется поиск через `crm.company.list`, и уже существующие компании пропускаются.
- Для каждого контрагента сохраняется `company_id` Bitrix24 в локальное поле `counterparties.bitrix_company_id`.
- Чтобы заполнить/обновить это поле для существующих записей, запустите импорт повторно.
- Поиск выполняется в порядке:
  - `BITRIX24_COMPANY_INN_FIELD` (+ `BITRIX24_COMPANY_KPP_FIELD`, если задано)
  - `BITRIX24_COMPANY_SHORT_NAME_FIELD`
  - fallback по точному `TITLE`
- Для реквизитов используется `BITRIX24_REQUISITE_PRESET_ID` (если задан), иначе выбирается активный preset по `BITRIX24_REQUISITE_COUNTRY_ID` (по умолчанию `1`, РФ).
- Обязательный минимум для `.env`:

```env
BITRIX24_WEBHOOK_URL=https://<portal>.bitrix24.ru/rest/<user_id>/<code>
```

## Сделки И Задачи В Bitrix24 По Факту Выставления Счёта

- Для `python3 -m src.cli.cron` и `python3 -m src.cli.manual` можно включить создание сделки и задачи через отдельные webhook:

```env
BITRIX24_TASK_WEBHOOK_URL=https://<portal>.bitrix24.ru/rest/<user_id>/<code>
BITRIX24_DEAL_WEBHOOK_URL=https://<portal>.bitrix24.ru/rest/<user_id>/<code>
```

- Параметры сделки фиксированы:
  - Название: `[dd.mm.yyyy] Вывоз бункеров`
  - Стадия: `C102:FINAL_INVOICE`
  - Сумма (`OPPORTUNITY`): сумма по счёту
  - Клиент: `COMPANY_ID=counterparties.bitrix_company_id`
  - Тип: `TYPE_ID=SALE`
  - Источник: `SOURCE_ID=PARTNER`
  - Услуги: `UF_CRM_1640764372166=2558`
  - Адрес объекта: `ADDRESS=Киров`
  - Субъект: `UF_CRM_1640765412209=174`
  - Способ оплаты: `UF_CRM_AMO_586713=544`
  - Город: `UF_CRM_AMO_631688=Киров`
  - Направление: `UF_CRM_1680515310897=4818`
  - Товарные позиции (`crm.deal.productrows.set`): название `Услуга по вывозу мусора из контейнера 8м3`, количество/цена/сумма берутся из строк счёта.
- Параметры задачи фиксированы:
  - Название: `[Киров] Обработать счет №<NUM> (<SHORT_NAME>)`
  - Исполнитель: `31648`
  - Наблюдатели: `8`, `54`, `18`, `33036`
  - Важность: высокая (`PRIORITY=2`)
  - Требовать результат: включено
  - Теги: `киров`, `новый_счет`, `отправить в ЭДО`
  - Дедлайн: `+1 сутки` от момента создания
- Описание задачи в BBCode (жирный заголовок/блок), также добавляются `pdfUrl` и сумма счёта.
- При наличии `counterparties.bitrix_company_id` задача связывается с CRM-компанией через `UF_CRM_TASK` в формате `CO_<ID>`.
- После успешного создания сделки задача дополнительно связывается со сделкой через `UF_CRM_TASK` в формате `D_<DEAL_ID>`.
- Задача создаётся до отправки уведомления в MAX; если создана успешно, в сообщение MAX добавляется ссылка на задачу.

- Полезные флаги:
  - `--dry-run` — проверить список без отправки запросов
  - `--limit 10` — ограничить объём импорта
  - `--short-name "Алтай-Строй"` — импортировать только указанные `short_name` (флаг можно повторять)
  - `--stop-on-error` — остановиться на первой ошибке
- Опционально можно передавать данные в пользовательские поля Bitrix24 (`UF_CRM_*`) через env-переменные:
  - `BITRIX24_COMPANY_SHORT_NAME_FIELD`
  - `BITRIX24_COMPANY_INN_FIELD`
  - `BITRIX24_COMPANY_KPP_FIELD`
  - `BITRIX24_COMPANY_NOTE_FIELD`
  - `BITRIX24_COMPANY_INVOICE_SCHEDULE_FIELD`
- Настройки реквизитов:
  - `BITRIX24_REQUISITE_PRESET_ID` — зафиксировать preset реквизитов
  - `BITRIX24_REQUISITE_COUNTRY_ID` — страна для автоподбора preset (default `1`)

## Override Email Для Отладки

- Если в `.env` задан `DEBUG_FORCE_EMAIL`, все счета отправляются на этот адрес.
- Если `DEBUG_FORCE_EMAIL` пустой, используется email контрагента из `counterparties.email`.
- В `counterparties.email` можно указывать несколько адресов через запятую (например: `a@x.ru, b@y.ru`) — в T-Bank они отправляются как несколько контактов.

## Периодичность Выставления

В таблице `counterparties` используется поле `invoice_schedule`:

- `monthly` — выставление в последний день месяца после 22:00
- `2weeks` — выставление в последний день месяца и 15-го числа после 22:00
- `daily` — выставление в любой день после 22:00

## Разбиение На Несколько Счетов

- По умолчанию все невыставленные работы контрагента попадают в один счёт.
- Для точечного разбиения используйте `config/invoice_split_rules.json`.
- Правило задаётся на `short_name` контрагента и содержит список групп.
- Для каждой группы можно указать:
  - `note_contains_any` — список подстрок (в `works.note`, регистр не важен);
  - `default: true` — группа по умолчанию для всех работ, не попавших в другие группы.
- Пример (Инноград отдельно, остальное отдельно):

```json
{
  "counterparties": {
    "SHORT_NAME_КОНТРАГЕНТА": {
      "groups": [
        {
          "key": "innograd",
          "label": "Инноград",
          "note_contains_any": ["инноград"]
        },
        {
          "key": "other",
          "label": "Остальное",
          "default": true
        }
      ]
    }
  }
}
```

- При наличии нескольких групп `cron` и `manual` создают несколько счетов в одном запуске (по одному на группу).

## Выручка Из Google Sheets

- Колонка `Выручка` синхронизируется в `works.revenue`.
- При сборке счёта приоритет такой:
  - если у работы есть `Выручка`, в сумму берётся она;
  - если `Выручка` пустая/невалидная, сумма считается по `prices`.
- При формировании позиций счёта используется фактическое количество из `works.object_count`.
- Если внутри периода по одной и той же услуге меняется фактическая цена за единицу (`Выручка / Объект`),
  строки автоматически разбиваются на отдельные позиции счёта по моменту смены цены
  (название услуги одинаковое, `price`/`amount` разные).

## Синхронизация Контрагентов Из Google Sheets

- В том же Google-документе используется вкладка `Контрагенты`.
- Имя вкладки можно переопределить через `GOOGLE_COUNTERPARTIES_SHEET_NAME` (по умолчанию: `Контрагенты`).
- Ожидаемые колонки на вкладке:
  - `ИНН контрагента`
  - `КПП контрагента`
  - `Email`
  - `Сокращенное наименование`
  - `Наименование контрагента`
- При синхронизации выполняется upsert в таблицу `counterparties`:
  - новые контрагенты создаются;
  - для новых контрагентов `invoice_schedule` по умолчанию: `2weeks`;
  - существующие обновляются по `inn`/`short_name`;
  - поля `phone`, `note`, `invoice_schedule` не перезаписываются из Sheets.

## Отбор Работ В Счёт По Дате

- По умолчанию берутся все невыставленные работы контрагента (`invoice_id IS NULL`) с верхней границей `work.date <= дата запуска`.
- Чтобы включить строгий период, задайте `INVOICE_STRICT_PERIOD=true`:
  - `monthly` — с 1-го числа текущего месяца
  - `2weeks` — с 1-го по 15-е или с 16-го по конец месяца
  - `daily` — только текущий день
- При `INVOICE_WARN_OUT_OF_PERIOD=true` в лог пишется предупреждение, если есть старые невыставленные работы до начала strict-периода.
- Для `manual` в `strict_period` правая граница берётся концом периода (месяц/полумесяц), а не текущим моментом запуска.
- Для `manual` можно задать `--from-date DD.MM.YYYY` — это нижняя граница отбора работ в счёт.
- Для `manual` можно задать `--to-date DD.MM.YYYY` — это верхняя граница отбора работ в счёт.
- Для `manual` есть `--dry-run` — показывает превью счёта (позиции/сумму/комментарий) без каких-либо изменений.
- Для `manual --dry-run --dry-run-include-issued` в превью учитываются все работы в диапазоне, включая уже выставленные ранее.
- Для `manual` есть `--dry-run --dry-run-bitrix` — в dry-run дополнительно создаёт сделку/задачу в Bitrix24 для тестирования, но не делает запись в БД, отправку в T-Bank, Sheets и чат-уведомления.

## Структура

- `src/db/` — модели, репозитории, подключение к MySQL
- `src/sheets/` — чтение и синхронизация из Google Sheets
- `src/tbank/` — клиент T-Bank API
- `src/bitrix/` — клиент Bitrix24 CRM
- `src/invoice/` — сборка счёта, генерация акта
- `src/notifications/` — отправка в Telegram и MAX
- `src/cli/` — точки входа (`cron`, `manual`, `sync_sheets`, `import_counterparties_to_bitrix24`)
