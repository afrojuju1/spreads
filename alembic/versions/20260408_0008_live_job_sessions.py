"""add live session fields to job runs

Revision ID: 20260408_0008
Revises: 20260407_0007
Create Date: 2026-04-08 09:15:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260408_0008"
down_revision = "20260407_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("job_runs", sa.Column("session_id", sa.Text(), nullable=True))
    op.add_column("job_runs", sa.Column("slot_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "job_runs",
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index(
        "idx_job_runs_session_slot",
        "job_runs",
        ["session_id", "slot_at"],
        unique=False,
    )
    op.create_index(
        "ux_job_runs_job_key_session_slot",
        "job_runs",
        ["job_key", "session_id", "slot_at"],
        unique=True,
    )
    op.alter_column("job_runs", "retry_count", server_default=None)

    op.add_column("collector_cycles", sa.Column("job_run_id", sa.Text(), nullable=True))
    op.add_column("collector_cycles", sa.Column("session_id", sa.Text(), nullable=True))
    op.create_foreign_key(
        "fk_collector_cycles_job_run_id_job_runs",
        "collector_cycles",
        "job_runs",
        ["job_run_id"],
        ["job_run_id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_collector_cycles_session_id_generated_at",
        "collector_cycles",
        ["session_id", "generated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_collector_cycles_session_id_generated_at", table_name="collector_cycles")
    op.drop_constraint("fk_collector_cycles_job_run_id_job_runs", "collector_cycles", type_="foreignkey")
    op.drop_column("collector_cycles", "session_id")
    op.drop_column("collector_cycles", "job_run_id")

    op.drop_index("ux_job_runs_job_key_session_slot", table_name="job_runs")
    op.drop_index("idx_job_runs_session_slot", table_name="job_runs")
    op.drop_column("job_runs", "retry_count")
    op.drop_column("job_runs", "slot_at")
    op.drop_column("job_runs", "session_id")
