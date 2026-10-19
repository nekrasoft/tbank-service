"""Distinguish tickets from site photos, including existing pickup attachments."""
from collections import Counter
from hashlib import sha256

from alembic import op
import sqlalchemy as sa

revision = '024'
down_revision = '023'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    if not inspector.has_table('works_files'):
        return
    if 'kind' not in {column['name'] for column in inspector.get_columns('works_files')}:
        op.add_column('works_files', sa.Column('kind', sa.String(20), nullable=False, server_default='container_waybill'))
    if not inspector.has_table('bunker_pickup_reports') or not inspector.has_table('bunker_pickup_files'):
        return
    if 'submission_key' not in {column['name'] for column in inspector.get_columns('bunker_pickup_reports')}:
        return
    last_id = 0
    while True:
        reports = connection.execute(sa.text(
            'SELECT id,submission_key FROM bunker_pickup_reports WHERE id > :id ORDER BY id LIMIT 200'
        ), {'id': last_id}).mappings().all()
        if not reports:
            break
        counts = Counter(row.report_id for row in connection.execute(sa.text(
            'SELECT report_id,kind FROM bunker_pickup_files WHERE report_id IN :ids'
        ).bindparams(sa.bindparam('ids', expanding=True)), {'ids': [report['id'] for report in reports]})
            if row.kind == 'site_photo')
        # Tokens follow pickup_submission.py's frozen submission key and zero-based photo position.
        for report in reports:
            if not report['submission_key']:
                continue
            for index in range(counts[report['id']]):
                token = sha256(f"{report['submission_key']}:photo:{index}".encode()).hexdigest()
                connection.execute(sa.text("UPDATE works_files SET kind='site_photo' WHERE file_token=:token"), {'token': token})
        last_id = reports[-1]['id']


def downgrade():
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table('works_files') and 'kind' in {column['name'] for column in inspector.get_columns('works_files')}:
        op.drop_column('works_files', 'kind')
