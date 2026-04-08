"""add generator jobs

Revision ID: 20260407_0007
Revises: 20260407_0006
Create Date: 2026-04-07 21:55:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260407_0007"
down_revision = "20260407_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "generator_jobs",
        sa.Column("generator_job_id", sa.Text(), nullable=False),
        sa.Column("arq_job_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("request_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("result_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("generator_job_id"),
    )
    op.create_index("idx_generator_jobs_status_created", "generator_jobs", ["status", "created_at"], unique=False)
    op.create_index("idx_generator_jobs_symbol_created", "generator_jobs", ["symbol", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_generator_jobs_symbol_created", table_name="generator_jobs")
    op.drop_index("idx_generator_jobs_status_created", table_name="generator_jobs")
    op.drop_table("generator_jobs")
