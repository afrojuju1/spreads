"""add job orchestration tables

Revision ID: 20260407_0005
Revises: 20260407_0004
Create Date: 2026-04-07 23:45:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260407_0005"
down_revision = "20260407_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_definitions",
        sa.Column("job_key", sa.Text(), primary_key=True),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("schedule_type", sa.Text(), nullable=False),
        sa.Column("schedule_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("market_calendar", sa.Text(), nullable=False, server_default="NYSE"),
        sa.Column("singleton_scope", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_job_definitions_enabled_type",
        "job_definitions",
        ["enabled", "job_type"],
        unique=False,
    )

    op.create_table(
        "job_runs",
        sa.Column("job_run_id", sa.Text(), primary_key=True),
        sa.Column("job_key", sa.Text(), nullable=False),
        sa.Column("arq_job_id", sa.Text(), nullable=True),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_name", sa.Text(), nullable=True),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["job_key"], ["job_definitions.job_key"], ondelete="CASCADE"),
    )
    op.create_index(
        "idx_job_runs_job_key_scheduled_for",
        "job_runs",
        ["job_key", "scheduled_for"],
        unique=False,
    )
    op.create_index(
        "idx_job_runs_status_scheduled_for",
        "job_runs",
        ["status", "scheduled_for"],
        unique=False,
    )

    op.create_table(
        "job_leases",
        sa.Column("lease_key", sa.Text(), primary_key=True),
        sa.Column("job_run_id", sa.Text(), nullable=True),
        sa.Column("owner", sa.Text(), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["job_run_id"], ["job_runs.job_run_id"], ondelete="SET NULL"),
    )
    op.create_index(
        "idx_job_leases_expires_at",
        "job_leases",
        ["expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_job_leases_expires_at", table_name="job_leases")
    op.drop_table("job_leases")
    op.drop_index("idx_job_runs_status_scheduled_for", table_name="job_runs")
    op.drop_index("idx_job_runs_job_key_scheduled_for", table_name="job_runs")
    op.drop_table("job_runs")
    op.drop_index("idx_job_definitions_enabled_type", table_name="job_definitions")
    op.drop_table("job_definitions")
