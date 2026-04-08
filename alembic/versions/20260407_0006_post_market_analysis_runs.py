"""add post market analysis runs

Revision ID: 20260407_0006
Revises: 20260407_0005
Create Date: 2026-04-07 20:35:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260407_0006"
down_revision = "20260407_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "post_market_analysis_runs",
        sa.Column("analysis_run_id", sa.Text(), nullable=False),
        sa.Column("job_run_id", sa.Text(), nullable=True),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("diagnostics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("recommendations_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("report_markdown", sa.Text(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["job_run_id"], ["job_runs.job_run_id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("analysis_run_id"),
        sa.UniqueConstraint("job_run_id"),
    )
    op.create_index(
        "idx_post_market_runs_label_session_completed",
        "post_market_analysis_runs",
        ["label", "session_date", "completed_at"],
        unique=False,
    )
    op.create_index(
        "idx_post_market_runs_status_created",
        "post_market_analysis_runs",
        ["status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_post_market_runs_status_created", table_name="post_market_analysis_runs")
    op.drop_index("idx_post_market_runs_label_session_completed", table_name="post_market_analysis_runs")
    op.drop_table("post_market_analysis_runs")
