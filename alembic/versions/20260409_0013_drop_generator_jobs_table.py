"""drop legacy generator_jobs table

Revision ID: 20260409_0013
Revises: 20260409_0012
Create Date: 2026-04-09 06:40:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260409_0013"
down_revision = "20260409_0012"
branch_labels = None
depends_on = None

GENERATOR_JOB_KEY = "generator:adhoc"
GENERATOR_JOB_TYPE = "generator"


def upgrade() -> None:
    op.drop_table("generator_jobs")


def downgrade() -> None:
    op.create_table(
        "generator_jobs",
        sa.Column("generator_job_id", sa.Text(), primary_key=True),
        sa.Column("arq_job_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
    )
    op.create_index("idx_generator_jobs_status_created", "generator_jobs", ["status", "created_at"], unique=False)
    op.create_index("idx_generator_jobs_symbol_created", "generator_jobs", ["symbol", "created_at"], unique=False)
    op.execute(
        sa.text(
            """
            insert into generator_jobs (
                generator_job_id,
                arq_job_id,
                symbol,
                status,
                created_at,
                started_at,
                finished_at,
                request_json,
                result_json,
                error_text
            )
            select
                job_run_id,
                arq_job_id,
                coalesce(payload_json->>'symbol', ''),
                status,
                scheduled_for,
                started_at,
                finished_at,
                payload_json - 'job_key' - 'job_type' - 'scheduled_for',
                result_json,
                error_text
            from job_runs
            where job_key = :job_key
              and job_type = :job_type
            """
        ).bindparams(job_key=GENERATOR_JOB_KEY, job_type=GENERATOR_JOB_TYPE)
    )
