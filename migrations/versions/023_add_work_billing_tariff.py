"""Snapshot billing units and tariffs without changing historical works."""
from alembic import op
import sqlalchemy as sa

revision = '023'
down_revision = '022'
branch_labels = None
depends_on = None


def upgrade():
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table('works'):
        return
    columns = {c['name'] for c in inspector.get_columns('works')}
    for column in (sa.Column('billing_unit', sa.String(20), nullable=True), sa.Column('tariff', sa.Numeric(12, 2), nullable=True)):
        if column.name not in columns:
            op.add_column('works', column)


def downgrade():
    columns = {c['name'] for c in sa.inspect(op.get_bind()).get_columns('works')}
    for name in ('tariff', 'billing_unit'):
        if name in columns:
            op.drop_column('works', name)
