"""Create average_good_duration column

Revision ID: aba6ba58c5a3
Revises: e9dc54a8d473
Create Date: 2017-11-16 13:57:53.859924

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'aba6ba58c5a3'
down_revision = '8c36f1091381'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('schedules', sa.Column('average_good_duration', postgresql.INTERVAL(), nullable=True), schema='fnscrapers')
    op.execute("UPDATE fnscrapers.schedules SET average_good_duration = last_good_end_at - last_good_start_at")


def downgrade():
    op.drop_column('schedules', 'average_good_duration', schema='fnscrapers')
