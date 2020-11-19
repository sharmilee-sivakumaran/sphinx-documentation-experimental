"""Add kill_immediately column

Revision ID: d14d027ecc5f
Revises: e9dc54a8d473
Create Date: 2018-03-02 15:36:52.018827

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd14d027ecc5f'
down_revision = 'e9dc54a8d473'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('schedules', sa.Column('kill_immediately', sa.Boolean, default=False), schema='fnscrapers')
    op.execute("""
               UPDATE fnscrapers.schedules SET kill_immediately = false WHERE kill_immediately is NULL;
               """)
    op.alter_column('schedules', 'kill_immediately', nullable=False, schema='fnscrapers')


def downgrade():
    op.drop_column('schedules', 'kill_immediately', schema='fnscrapers')
