"""rename job run orchestration id

Revision ID: 20260709_0065
Revises: 20260708_0064
Create Date: 2026-07-09 02:50:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "20260709_0065"
down_revision = "20260708_0064"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("job_runs", "arq_job_id", new_column_name="orchestration_id")


def downgrade() -> None:
    op.alter_column("job_runs", "orchestration_id", new_column_name="arq_job_id")
