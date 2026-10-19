from hashlib import sha256
import importlib.util
from pathlib import Path
from types import SimpleNamespace

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text


def test_existing_photos_are_classified_and_tickets_stay_tickets():
    spec = importlib.util.spec_from_file_location('file_kind_migration', Path(__file__).parents[1] / 'migrations/versions/024_add_work_file_kind.py')
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    with create_engine('sqlite://').begin() as connection:
        connection.execute(text('CREATE TABLE works_files (file_token TEXT PRIMARY KEY)'))
        connection.execute(text('CREATE TABLE bunker_pickup_reports (id INTEGER PRIMARY KEY,submission_key TEXT)'))
        connection.execute(text('CREATE TABLE bunker_pickup_files (id INTEGER PRIMARY KEY,report_id INTEGER,kind TEXT)'))
        connection.execute(text("INSERT INTO bunker_pickup_reports VALUES (1,'submission-key')"))
        connection.execute(text("INSERT INTO bunker_pickup_files VALUES (1,1,'container_waybill'),(2,1,'site_photo'),(3,1,'site_photo')"))
        tokens = [sha256(f'submission-key:photo:{i}'.encode()).hexdigest() for i in range(2)]
        for token in ['wb_legacy', 'new-ticket', *tokens]:
            connection.execute(text('INSERT INTO works_files VALUES (:token)'), {'token': token})
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
            migration.upgrade()
        kinds = dict(connection.execute(text('SELECT file_token,kind FROM works_files')).all())
        assert kinds['wb_legacy'] == kinds['new-ticket'] == 'container_waybill'
        assert [kinds[token] for token in tokens] == ['site_photo', 'site_photo']


def test_manual_and_cron_keep_file_kind_without_removing_bitrix_photos():
    from src.cli.cron import _build_bitrix_task_file_attachments as cron_files
    from src.cli.manual import _build_bitrix_task_file_attachments as manual_files
    files = [SimpleNamespace(kind=kind, file_data=b'file') for kind in ('site_photo', 'container_waybill')]
    for build in (cron_files, manual_files):
        assert [file['kind'] for file in build(files)] == ['site_photo', 'container_waybill']
