"""backfill generator jobs into job_runs

Revision ID: 20260409_0012
Revises: 20260409_0011
Create Date: 2026-04-09 05:55:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260409_0012"
down_revision = "20260409_0011"
branch_labels = None
depends_on = None

GENERATOR_JOB_KEY = "generator:adhoc"
GENERATOR_JOB_TYPE = "generator"


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            insert into job_definitions (
                job_key,
                job_type,
                enabled,
                schedule_type,
                schedule_json,
                payload_json,
                market_calendar,
                singleton_scope,
                created_at,
                updated_at
            )
            values (
                :job_key,
                :job_type,
                false,
                'manual',
                '{}'::jsonb,
                '{}'::jsonb,
                'NYSE',
                null,
                timezone('utc', now()),
                timezone('utc', now())
            )
            on conflict (job_key) do update
            set
                job_type = excluded.job_type,
                enabled = excluded.enabled,
                schedule_type = excluded.schedule_type,
                schedule_json = excluded.schedule_json,
                payload_json = excluded.payload_json,
                market_calendar = excluded.market_calendar,
                singleton_scope = excluded.singleton_scope,
                updated_at = excluded.updated_at
            """
        ).bindparams(job_key=GENERATOR_JOB_KEY, job_type=GENERATOR_JOB_TYPE)
    )
    op.execute(
        sa.text(
            """
            insert into job_runs (
                job_run_id,
                job_key,
                arq_job_id,
                job_type,
                status,
                scheduled_for,
                session_id,
                slot_at,
                retry_count,
                started_at,
                finished_at,
                heartbeat_at,
                worker_name,
                payload_json,
                result_json,
                error_text
            )
            select
                generator_job_id,
                :job_key,
                arq_job_id,
                :job_type,
                status,
                created_at,
                null,
                null,
                0,
                started_at,
                finished_at,
                coalesce(finished_at, started_at, created_at),
                null,
                coalesce(request_json, '{}'::jsonb)
                    || jsonb_build_object('job_key', :job_key, 'job_type', :job_type),
                result_json,
                error_text
            from generator_jobs
            on conflict (job_run_id) do nothing
            """
        ).bindparams(job_key=GENERATOR_JOB_KEY, job_type=GENERATOR_JOB_TYPE)
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            delete from job_runs
            where job_type = :job_type
              and job_key = :job_key
              and job_run_id in (
                  select generator_job_id from generator_jobs
              )
            """
        ).bindparams(job_key=GENERATOR_JOB_KEY, job_type=GENERATOR_JOB_TYPE)
    )
