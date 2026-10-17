from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from src.db.models import Work
from src.invoice.builder import build_invoice_items
from src.sheets.sync import _prepare_work_note_and_volume


def work(id, volume, **kwargs):
    return Work(id=id, date=date(2026, 10, 17), counterparty_name='Клиент',
                structure='ЮЛ - Вывоз мусора', operation='Поступление по основной деятельности',
                object_count='1', sheet_row_hash=str(id), billing_unit='м³',
                volume=Decimal(str(volume)), tariff=Decimal('1200'), revenue=Decimal(str(volume))*1200, **kwargs)


def test_volume_invoice_uses_cubes_and_snapshot_tariff():
    with patch('src.invoice.builder.prices_repo.get_by_counterparty_and_operation', return_value=SimpleNamespace(price=999, vat='None')):
        items = build_invoice_items(MagicMock(), [work(1, 30), work(2, 36)], counterparty_id=1)
    assert len(items) == 1
    assert items[0]['amount'] == 66
    assert items[0]['price'] == 1200
    assert items[0]['unit'] == 'м³'
    assert items[0]['amount'] * items[0]['price'] == 79200


def test_legacy_and_volume_with_same_price_are_separate():
    legacy = work(1, 30)
    legacy.billing_unit = None
    legacy.revenue = Decimal('1200')
    with patch('src.invoice.builder.prices_repo.get_by_counterparty_and_operation', return_value=None):
        items = build_invoice_items(MagicMock(), [legacy, work(2, 36)], counterparty_id=1)
    assert [(i['amount'], i['unit']) for i in items] == [(1, 'шт'), (36, 'м³')]


def test_missing_or_inconsistent_snapshot_blocks_invoice():
    for name, value in [('volume', None), ('tariff', None), ('revenue', None), ('revenue', Decimal('1'))]:
        row = work(1, 30)
        setattr(row, name, value)
        with pytest.raises(ValueError):
            build_invoice_items(MagicMock(), [row], counterparty_id=1)


def test_explicit_volume_takes_precedence_over_note():
    note, volume = _prepare_work_note_and_volume({'structure': 'ЮЛ - Вывоз мусора',
        'note': 'Объем: 30 м3, Площадка', 'billing_unit': 'м³', 'volume': '36,00'})
    assert volume == Decimal('36.00')
    assert note == 'Площадка'


def test_migration_is_idempotent_and_keeps_legacy_values():
    import importlib.util
    from pathlib import Path
    from sqlalchemy import create_engine, inspect, text
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    spec = importlib.util.spec_from_file_location('volume_migration', Path(__file__).parents[1] / 'migrations/versions/023_add_work_billing_tariff.py')
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    with create_engine('sqlite://').begin() as connection:
        connection.execute(text('CREATE TABLE works (id INTEGER PRIMARY KEY, revenue NUMERIC)'))
        connection.execute(text('INSERT INTO works VALUES (1, 36000)'))
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
            migration.upgrade()
        assert {'billing_unit', 'tariff'} <= {c['name'] for c in inspect(connection).get_columns('works')}
        assert connection.execute(text('SELECT revenue,billing_unit,tariff FROM works')).one() == (36000, None, None)


def test_sync_updates_new_fields_but_preserves_invoiced_work():
    from src.sheets.sync import sync_sheets_to_mysql
    session = MagicMock()
    row = {'date': '17.10.2026', 'counterparty_name': 'Клиент', 'structure': 'ЮЛ - Вывоз мусора',
           'operation': 'Поступление по основной деятельности', 'object_count': '1', 'sheet_row_hash': 'hash',
           'note': 'Объем: 36 м3, Площадка', 'volume': '36', 'billing_unit': 'м³', 'tariff': '1200', 'revenue': '43200'}
    for invoice_id in (None, 10):
        existing = work(1, 30)
        existing.invoice_id = invoice_id
        with patch('src.sheets.sync.get_session', return_value=session), \
             patch('src.sheets.sync.read_counterparties', return_value=[]), \
             patch('src.sheets.sync.read_works', return_value=[row]), \
             patch('src.sheets.sync.works_repo.get_max_date', return_value=None), \
             patch('src.sheets.sync.works_repo.get_by_hash', return_value=existing):
            assert sync_sheets_to_mysql() == 0
        assert existing.volume == Decimal('30' if invoice_id else '36')
        assert existing.revenue == Decimal('36000' if invoice_id else '43200')


def test_sheet_reader_keeps_trips_and_reads_numeric_billing_columns():
    from src.sheets.reader import read_works
    headers = ['Дата', 'Контрагент', 'Примечание', 'Структура', 'Операция', 'Объект',
               'Выручка', 'Объём, м³', 'Единица тарифа', 'Тариф на дату вывоза']
    values = ['17.10.2026', 'Клиент', 'Площадка [ВЫВОЗ:key]', 'ЮЛ - Вывоз мусора',
              'Поступление по основной деятельности', '1', '43200', '36', 'м³', '1200']
    client = MagicMock()
    sheet = client.open_by_key.return_value.get_worksheet_by_id.return_value
    sheet.get_all_records.side_effect = ValueError('Blank first header row')
    sheet.get_all_values.return_value = [[], headers, values]
    with patch('src.sheets.reader.get_sheets_client', return_value=client):
        rows = read_works(sheet_url='https://docs.google.com/spreadsheets/d/sheet/edit#gid=7')
    assert len(rows) == 1
    assert rows[0]['object_count'] == '1'
    assert (rows[0]['volume'], rows[0]['billing_unit'], rows[0]['tariff']) == ('36', 'м³', '1200')
