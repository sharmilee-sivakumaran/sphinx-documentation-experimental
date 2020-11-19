"""Add support for columns needed by updated scheduler

Revision ID: e9dc54a8d473
Revises: 8c36f1091381
Create Date: 2017-11-16 01:53:18.472719

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'e9dc54a8d473'
down_revision = 'aba6ba58c5a3'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('schedules', sa.Column('blackout_periods', postgresql.JSONB(), nullable=True), schema='fnscrapers')

    op.add_column('schedules', sa.Column('max_allowed_duration', postgresql.INTERVAL(), nullable=True), schema='fnscrapers')

    op.add_column('schedules', sa.Column('tz', sa.String(), nullable=True), schema='fnscrapers')
    op.execute("UPDATE fnscrapers.schedules SET tz = 'UTC'")
    op.execute("ALTER TABLE fnscrapers.schedules ALTER COLUMN tz SET NOT NULL")

    op.alter_column('schedules', 'cron_schedule',
               existing_type=sa.VARCHAR(),
               nullable=True,
               schema='fnscrapers')
    op.execute("UPDATE fnscrapers.schedules SET cron_schedule = NULL WHERE cron_schedule = '* * * * *'")

    op.add_column('schedules', sa.Column('cron_max_schedule_duration', postgresql.INTERVAL(), nullable=True), schema='fnscrapers')
    op.execute("UPDATE fnscrapers.schedules SET cron_max_schedule_duration = INTERVAL '4 hours' WHERE cron_schedule IS NOT NULL")

    op.alter_column('schedules', 'cooldown_duration',
                    existing_type=postgresql.INTERVAL(),
                    nullable=True,
                    schema='fnscrapers')
    op.execute("UPDATE fnscrapers.schedules SET cooldown_duration = NULL WHERE cron_schedule IS NOT NULL")

    op.add_column('schedules', sa.Column('scheduling_period', postgresql.INTERVAL(), nullable=True), schema='fnscrapers')
    op.execute("UPDATE fnscrapers.schedules SET scheduling_period = INTERVAL '24 hours' WHERE cron_schedule IS NULL")

    op.drop_column('schedules', 'start_times', schema='fnscrapers')


def downgrade():
    op.add_column('schedules', sa.Column('start_times', postgresql.JSONB(), autoincrement=False, nullable=True), schema='fnscrapers')
    op.execute("UPDATE fnscrapers.schedules SET start_times = 'null'")
    op.execute("ALTER TABLE fnscrapers.schedules ALTER COLUMN start_times SET NOT NULL")

    op.execute("UPDATE fnscrapers.schedules SET cooldown_duration = INTERVAL '4 hours' WHERE cooldown_duration IS NULL")
    op.alter_column('schedules', 'cooldown_duration',
               existing_type=postgresql.INTERVAL(),
               nullable=False,
               schema='fnscrapers')

    op.execute("UPDATE fnscrapers.schedules SET cron_schedule = '* * * * *' WHERE cron_schedule IS NULL")
    op.alter_column('schedules', 'cron_schedule',
                    existing_type=sa.VARCHAR(),
                    nullable=False,
                    schema='fnscrapers')

    op.drop_column('schedules', 'tz', schema='fnscrapers')
    op.drop_column('schedules', 'scheduling_period', schema='fnscrapers')
    op.drop_column('schedules', 'max_allowed_duration', schema='fnscrapers')
    op.drop_column('schedules', 'cron_max_schedule_duration', schema='fnscrapers')
    op.drop_column('schedules', 'blackout_periods', schema='fnscrapers')
