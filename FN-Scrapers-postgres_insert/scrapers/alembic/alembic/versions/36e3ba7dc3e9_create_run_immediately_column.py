"""Create run_immediately column

Revision ID: 36e3ba7dc3e9
Revises: 3b2b68b70826
Create Date: 2017-09-19 14:11:16.615152

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '36e3ba7dc3e9'
down_revision = '3b2b68b70826'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('schedules', sa.Column('run_immediately', sa.DateTime(timezone=True), nullable=True), schema='fnscrapers')


def downgrade():
    op.drop_column('schedules', 'run_immediately', schema='fnscrapers')
