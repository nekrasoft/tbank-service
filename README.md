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
- Отдельный cron: синк выписки T-Bank и автозачет оплат по выставленным счетам
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

# Cron оплаты: синк выписки и матчинг оплат к invoices
python3 -m src.cli.cron_payments
# Повторно наполнить лист "Безнал-Расходы" из уже сохраненных операций, например после очистки вкладки
python3 -m src.cli.cron_payments --force-cashless-expenses --cashless-expenses-from-date 2026-01-01
# Повторно наполнить лист "Безнал-Доходы" из уже сохраненных операций
python3 -m src.cli.cron_payments --force-cashless-incomes --cashless-incomes-from-date 2026-01-01

# Cron напоминаний клиентам о просрочке оплаты (email)
python3 -m src.cli.cron_invoice_reminders

# Импорт контрагентов в Bitrix24 CRM
python3 -m src.cli.import_counterparties_to_bitrix24
```

## Автозачет Оплат Из Выписки T-Bank

- Команда: `python3 -m src.cli.cron_payments`.
- Источник данных: `GET /api/v1/statement` (с пагинацией `nextCursor`).
- Синк идемпотентный:
  - каждая операция сохраняется в `tbank_statement_operations` (нормализованные поля + `raw_payload`);
  - дедупликация по `dedupe_key` (`account_number + operationId`, либо fallback hash).
- Для каждого счета в `TBANK_STATEMENT_ACCOUNT_NUMBERS` хранится состояние синка в `tbank_statement_sync_state`.
- Исходящие операции дополнительно выгружаются в Google Sheets во вкладку `Безнал-Расходы`.
  Повторная выгрузка контролируется полем `cashless_expense_sheet_synced_at` в `tbank_statement_operations`;
  перед добавлением cron сверяет уже существующие строки по банковским колонкам, чтобы не дублировать ручные строки.
  Если назначение платежа начинается с кода вида `1200-146-02`, cron заполняет `Структура` и `Операция`
  по справочникам `config/structure.json` и `config/operation.json`; проверки данных для этих колонок,
  а также формулы и форматирование `КСП` и `КСЗ` копируются из предыдущей строки.
- Если явного кода нет, fallback-правила из `config/cashless_expense_fallback_rules.json`
  пытаются определить аналитику по повторяющимся фрагментам назначения платежа.
- Входящие операции дополнительно выгружаются в Google Sheets во вкладку `Безнал-Доходы`.
  Повторная выгрузка контролируется полем `cashless_income_sheet_synced_at`.
  `Структура` заполняется дефолтным значением из `GOOGLE_CASHLESS_DEFAULT_STRUCTURE_CODE`
  или `default_structure_code` в `config/cashless_expense_fallback_rules.json`; формула и форматирование
  `КСП`, а также проверка данных `Структура` копируются из предыдущей строки.
  В колонку `Контрагент, имя` записывается `short_name` из `counterparties`, найденный по ИНН плательщика.
- Для повторной выгрузки расходов используйте `--force-cashless-expenses`; флаг игнорирует отметку
  `cashless_expense_sheet_synced_at`, но не отключает дедупликацию уже существующих строк в листе.
  Нижнюю дату можно задать через `--cashless-expenses-from-date YYYY-MM-DD` или `--from-date DD.MM.YYYY`.
- Для повторной выгрузки доходов используйте `--force-cashless-incomes`; флаг игнорирует отметку
  `cashless_income_sheet_synced_at`, но не отключает дедупликацию уже существующих строк в листе.
  Нижнюю дату можно задать через `--cashless-incomes-from-date YYYY-MM-DD`.
- Для счетов в `invoices` добавлены агрегаты оплаты:
  - `paid_amount` — суммарно зачтенные входящие платежи;
  - `paid_at` — дата закрытия счета (когда сумма достигла total).
- При успешной отправке счета в `invoices` также сохраняются:
  - `payment_link` — ссылка на оплату;
  - `recipient_emails_snapshot` — фактический список email получателей счета.
- Автоматический матчинг выполняется по приоритетам:
  - номер счета в `payPurpose/description` (самый надежный путь);
  - `payer.inn + сумма` (если кандидат уникальный);
  - `payer.name + сумма` (осторожный fallback при уникальном кандидате).
- Статусы счетов после пересчета:
  - `issued` — оплат нет;
  - `partially_paid` — оплачено частично;
  - `paid` — оплачено полностью (или с переплатой).

Минимум env для cron оплат:

```env
TBANK_STATEMENT_ACCOUNT_NUMBERS=4070...,4080...
TBANK_STATEMENT_INITIAL_LOOKBACK_DAYS=90
TBANK_STATEMENT_OVERLAP_MINUTES=180
TBANK_STATEMENT_PAGE_LIMIT=200
TBANK_STATEMENT_UNMATCHED_LIMIT=5000
GOOGLE_CASHLESS_EXPENSES_SHEET_NAME=Безнал-Расходы
GOOGLE_CASHLESS_INCOMES_SHEET_NAME=Безнал-Доходы
GOOGLE_CASHLESS_EXPENSE_SYNC_LIMIT=5000
GOOGLE_CASHLESS_INCOME_SYNC_LIMIT=5000
GOOGLE_CASHLESS_DEFAULT_STRUCTURE_CODE=1202
TBANK_STATEMENT_DEFAULT_ACCOUNT_LABEL=Благосервис ТБанк
# опционально для нескольких счетов:
# TBANK_STATEMENT_ACCOUNT_LABELS=4070...=Благосервис ТБанк,4080...=Благосервис Сбер
# опционально для первого бэкфилла:
# GOOGLE_CASHLESS_EXPENSE_SYNC_FROM_DATE=2026-01-01
# GOOGLE_CASHLESS_INCOME_SYNC_FROM_DATE=2026-01-01
```

## Напоминания О Просрочке Оплаты

- Команда: `python3 -m src.cli.cron_invoice_reminders`.
- Канал: отдельный email клиенту (не через T-Bank).
- Если у контрагента задан `email_accountant`, он добавляется в адресаты напоминаний и писем-благодарностей.
- Напоминания отправляются только по полностью неоплаченным счетам:
  - `status = issued`;
  - `paid_amount <= 0.01`;
  - `due_date <= today`.
- Для контрагента можно выключить рассылку через `counterparties.payment_reminders_enabled = false`
  (по умолчанию включено).
- Расписание задается смещениями в днях после `due_date`:
  - по умолчанию: `3,7,10,14` (`INVOICE_REMINDER_OFFSETS_DAYS`).
- Каждая попытка отправки фиксируется в `invoice_payment_reminders` со статусом:
  - `sent`, `failed`, `skipped`.
- Для превью без отправки и записи в БД:
  - `python3 -m src.cli.cron_invoice_reminders --dry-run`.

Минимум env для email-напоминаний:

```env
INVOICE_REMINDER_OFFSETS_DAYS=3,7,10,14
INVOICE_REMINDER_EMAIL_SMTP_HOST=smtp.example.com
INVOICE_REMINDER_EMAIL_SMTP_PORT=587
INVOICE_REMINDER_EMAIL_SMTP_USE_TLS=true
INVOICE_REMINDER_EMAIL_FROM=billing@example.com
INVOICE_REMINDER_EMAIL_FROM_NAME=БлагоСервис
INVOICE_REMINDER_EMAIL_COPY_TO_FROM=false
```

`python3 -m src.cli.cron_payments` также отправляет одно письмо-благодарность по счетам,
которые стали `paid` в текущий бизнес-день и еще не имеют
`invoices.payment_thank_email_sent_at`.

- Без просрочки в тексте используется фраза `Благодарим за оперативную оплату`.
- С просрочкой используется фраза `Благодарим за оплату`.
- SMTP-настройки используются те же: `INVOICE_REMINDER_EMAIL_*`.
- Если `INVOICE_REMINDER_EMAIL_COPY_TO_FROM=true`, reminder и thank-you письма дублируются скрытой копией на `INVOICE_REMINDER_EMAIL_FROM`.
- Бизнес-день считается в `APP_TIMEZONE` или `TZ`, по умолчанию `Europe/Moscow`.
- Лимит за запуск: `INVOICE_PAYMENT_THANK_EMAIL_LIMIT` (по умолчанию `5000`).

## Импорт Контрагентов В Bitrix24

- Для импорта используется входящий вебхук `BITRIX24_WEBHOOK_URL` и метод `crm.company.add`.
- Скрипт читает все записи из таблицы `counterparties`, создаёт компании в Bitrix24 и заполняет реквизиты организации (`RQ_INN`, `RQ_KPP`) через `crm.requisite.*`.
- Повторный запуск идемпотентный: перед созданием выполняется поиск через `crm.company.list`, и уже существующие компании повторно не создаются.
- Для каждого контрагента сохраняется `company_id` Bitrix24 в локальное поле `counterparties.bitrix_company_id`.
- Чтобы заполнить/обновить это поле для существующих записей, запустите импорт повторно.
- В `COMMENTS` карточки компании записывается служебный комментарий импорта (включая `contract`/строку договора); для уже существующих компаний `COMMENTS` также синхронизируется при импорте.
- Строка договора `contract` дополнительно записывается в поле компании `UF_CRM_1667795999022`.
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
  - Услуги (`UF_CRM_1640764372166`) по маппингу `operation_type` позиции счёта:
    - `container_pickup` (`Услуга по вывозу мусора из контейнера 8 м3`) → `2558`
    - `trip_removal` (`Услуги спецтехники (ломовоз) - вывоз и утилизация мусора объемом 30 м3`) → `2550`
  - Адрес объекта: `ADDRESS=Киров`
  - Субъект: `UF_CRM_1640765412209=174`
  - Способ оплаты: `UF_CRM_AMO_586713=544`
  - Город: `UF_CRM_AMO_631688=Киров`
  - Направление: `UF_CRM_1680515310897=4818`
  - Товарные позиции (`crm.deal.productrows.set`): название/количество/цена/сумма берутся из строк счёта.
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
- `DEBUG_FORCE_EMAIL` также применяется в `python3 -m src.cli.cron_invoice_reminders` и переопределяет получателя reminder-писем.

## Периодичность Выставления

В таблице `counterparties` используется поле `invoice_schedule`:

- `monthly` — выставление в последний день месяца после 22:00
- `2weeks` — выставление в последний день месяца и 15-го числа после 22:00
- `10days` — выставление в 10-й, 20-й и последний день месяца после 22:00
- `daily` — выставление в любой день после 22:00

## Разбиение На Несколько Счетов

- По умолчанию все невыставленные работы контрагента попадают в один счёт.
- Для точечного разбиения используйте `config/invoice_split_rules.json`.
- Правило задаётся на `short_name` контрагента и содержит список групп.
- Для каждой группы можно указать:
  - `note_contains_any` — список подстрок (в `works.note`, регистр не важен);
  - `email` — e-mail (или список e-mail) для отправки счета в T-Bank именно для этой группы;
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
          "email": "innograd-buh@example.ru",
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
- Если у группы задан `email`, для счета этой группы используется он; иначе берется `counterparties.email`.

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
  - `Email бухгалтера`
  - `Сокращенное наименование`
  - `Наименование контрагента`
  - `Договор` (например: `Договор №111 от 12.03.2025`)
  - `Напоминания по неоплаченным счетам` (опционально: `да`/`нет`, `true`/`false`, `1`/`0`)
- При синхронизации выполняется upsert в таблицу `counterparties`:
  - новые контрагенты создаются;
  - для новых контрагентов `invoice_schedule` по умолчанию: `2weeks`;
  - для новых контрагентов `payment_reminders_enabled` по умолчанию: `true`;
  - существующие обновляются по `inn`/`short_name`;
  - поле `email_accountant` синхронизируется из колонки `Email бухгалтера`;
  - поле `payment_reminders_enabled` меняется только если колонка напоминаний явно заполнена;
  - поле `contract` (строка договора) синхронизируется из Sheets;
  - поля `phone`, `note`, `invoice_schedule` не перезаписываются из Sheets.

## Отбор Работ В Счёт По Дате

- По умолчанию берутся все невыставленные работы контрагента (`invoice_id IS NULL`) с верхней границей `work.date <= дата запуска`.
- Чтобы включить строгий период, задайте `INVOICE_STRICT_PERIOD=true`:
  - `monthly` — с 1-го числа текущего месяца
  - `2weeks` — с 1-го по 15-е или с 16-го по конец месяца
  - `10days` — с 1-го по 10-е, с 11-го по 20-е или с 21-го по конец месяца
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
- `src/cli/` — точки входа (`cron`, `cron_payments`, `manual`, `sync_sheets`, `import_counterparties_to_bitrix24`)
