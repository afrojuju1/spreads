"""drop post market analysis runs

Revision ID: 20260422_0035
Revises: 20260422_0034
Create Date: 2026-04-22 15:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260422_0035"
down_revision = "20260422_0034"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(index.get("name") == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    table_name = "post_market_analysis_runs"
    if not _has_table(table_name):
        return
    if _has_index(table_name, "idx_post_market_runs_status_created"):
        op.drop_index("idx_post_market_runs_status_created", table_name=table_name)
    if _has_index(table_name, "idx_post_market_runs_label_session_completed"):
        op.drop_index(
            "idx_post_market_runs_label_session_completed",
            table_name=table_name,
        )
    op.drop_table(table_name)


def downgrade() -> None:
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
        sa.Column(
            "diagnostics_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "recommendations_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
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
