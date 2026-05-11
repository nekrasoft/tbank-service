# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`tbank-service` — Python 3.11+ сервис автоматизации выставления счетов через T-Bank API. Синхронизирует работы и контрагентов из Google Sheets в MySQL, генерирует счета и акты (PDF), создаёт сделки и задачи в Bitrix24, отправляет уведомления в Telegram и MAX.

## Common Commands

```bash
# Development setup
python3 -m pip install -r requirements.txt
cp .env.example .env  # Fill in your config

# Run tests
python3 -m pytest tests/ -v
python3 -m pytest tests/invoice/test_splitter.py -v  # single test file

# Database migrations (Alembic)
alembic upgrade head

# Sync data from Google Sheets (counterparties + works)
python3 -m src.cli.sync_sheets
python3 -m src.cli.sync_sheets --from-date 01.03.2026  # Backfill from date

# Manual invoice creation
python3 -m src.cli.manual --counterparty "Контрагент"
python3 -m src.cli.manual --counterparty "Контрагент" --dry-run  # Preview only
python3 -m src.cli.manual --counterparty "Контрагент" --from-date 01.03.2026 --to-date 31.03.2026

# Cron jobs
python3 -m src.cli.cron                    # Invoice generation (last day of month)
python3 -m src.cli.cron_payments           # Statement sync + payment matching
python3 -m src.cli.cron_invoice_reminders  # Email reminders for overdue invoices

# Bitrix24
python3 -m src.cli.import_counterparties_to_bitrix24
```

## Architecture

```
src/
├── cli/              # Entry points: cron, cron_payments, manual, sync_sheets
├── db/
│   ├── models.py     # SQLAlchemy models (works, invoices, counterparts, etc.)
│   ├── connection.py # MySQL connection + session management
│   └── repos/        # Data access layer (works.py, invoices.py, etc.)
├── sheets/           # Google Sheets reader/writer + sync logic
├── tbank/            # T-Bank API client (invoices, statement)
├── bitrix/           # Bitrix24 CRM client (companies, deals, tasks)
├── invoice/          # Invoice builder, PDF generator, splitting logic
└── notifications/    # Telegram, MAX, email notifications
```

**Database**: MySQL with Alembic migrations in `migrations/`

**Key configs** (in `config/`):
- `invoice_split_rules.json` — rules for splitting one counterparty into multiple invoices
- `structure.json`, `operation.json` — reference data for cashless expense categorization
- `cashless_expense_fallback_rules.json` — fallback logic for expense categorization

**Environment**: All settings via `.env` (see `.env.example`). Key integrations: MySQL, Google Sheets, T-Bank API, Bitrix24 webhooks, Telegram/MAX bots, SMTP for emails.

## Key Concepts

- **Invoice schedule**: `counterparties.invoice_schedule` controls generation frequency (`monthly`, `2weeks`, `10days`, `daily`)
- **Strict period**: `INVOICE_STRICT_PERIOD=true` enforces date boundaries per schedule
- **Payment matching**: `cron_payments` syncs T-Bank statement and auto-matches payments to invoices by invoice number, INN+amount, or name+amount
- **Email reminders**: Sent via separate SMTP channel (not T-Bank) at days 3,7,10,14 after due date
- **Idempotency**: All sync operations are idempotent — re-runs don't create duplicates
