"""Add last_start_at and last_end_at; rename other columns for consistency

Revision ID: 8c36f1091381
Revises: 36e3ba7dc3e9
Create Date: 2017-10-10 21:09:06.074256

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import text

# revision identifiers, used by Alembic.
revision = '8c36f1091381'
down_revision = '36e3ba7dc3e9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('schedules', sa.Column('last_end_at', sa.DateTime(timezone=True), nullable=True), schema='fnscrapers')
    op.add_column('schedules', sa.Column('last_start_at', sa.DateTime(timezone=True), nullable=True), schema='fnscrapers')

    op.alter_column('schedules', 'owner_started_at', new_column_name="owner_start_at", schema='fnscrapers')
    op.alter_column('schedules', 'last_started_at', new_column_name="last_good_start_at", schema='fnscrapers')
    op.alter_column('schedules', 'last_completed_at', new_column_name="last_good_end_at", schema='fnscrapers')

    op.drop_column('schedules', 'max_runtime_secs', schema='fnscrapers')
    op.add_column(
        'schedules',
        sa.Column('max_expected_duration', postgresql.INTERVAL, nullable=False, server_default=text("'2 days'::interval")),
        schema='fnscrapers')
    op.execute("ALTER TABLE fnscrapers.schedules ALTER COLUMN max_expected_duration DROP DEFAULT")

    op.execute("ALTER TABLE fnscrapers.schedules ADD COLUMN cooldown_duration INTERVAL")
    op.execute("UPDATE fnscrapers.schedules SET cooldown_duration = cooldown_time_secs * interval '1 second'")
    op.execute("ALTER TABLE fnscrapers.schedules ALTER COLUMN cooldown_duration SET NOT NULL")
    op.execute("ALTER TABLE fnscrapers.schedules ALTER COLUMN cooldown_duration DROP DEFAULT")
    op.execute("ALTER TABLE fnscrapers.schedules DROP COLUMN cooldown_time_secs")

def downgrade():
    op.alter_column('schedules', 'owner_start_at', new_column_name="owner_started_at", schema='fnscrapers')
    op.alter_column('schedules', 'last_good_start_at', new_column_name="last_started_at", schema='fnscrapers')
    op.alter_column('schedules', 'last_good_end_at', new_column_name="last_completed_at", schema='fnscrapers')

    op.drop_column('schedules', 'last_start_at', schema='fnscrapers')
    op.drop_column('schedules', 'last_end_at', schema='fnscrapers')

    op.drop_column('schedules', 'max_expected_duration', schema='fnscrapers')
    op.add_column(
        'schedules',
        sa.Column('max_runtime_secs', sa.BigInteger, nullable=False, server_default="{}".format(3600 * 48)),
        schema='fnscrapers')
    op.execute("ALTER TABLE fnscrapers.schedules ALTER COLUMN max_runtime_secs DROP DEFAULT")

    op.execute("ALTER TABLE fnscrapers.schedules ADD COLUMN cooldown_time_secs BIGINT")
    op.execute("UPDATE fnscrapers.schedules SET cooldown_time_secs = EXTRACT(EPOCH FROM cooldown_duration)")
    op.execute("ALTER TABLE fnscrapers.schedules ALTER COLUMN cooldown_time_secs SET NOT NULL")
    op.execute("ALTER TABLE fnscrapers.schedules ALTER COLUMN cooldown_time_secs DROP DEFAULT")
    op.execute("ALTER TABLE fnscrapers.schedules DROP COLUMN cooldown_duration")
