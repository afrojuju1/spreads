"""remove job_definitions table

Revision ID: 20260421_0032
Revises: 20260420_0031
Create Date: 2026-04-21 11:45:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260421_0032"
down_revision = "20260420_0031"
branch_labels = None
depends_on = None


def _drop_job_runs_definition_fk() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for foreign_key in inspector.get_foreign_keys("job_runs"):
        if foreign_key.get("referred_table") != "job_definitions":
            continue
        name = foreign_key.get("name")
        if name:
            op.drop_constraint(name, "job_runs", type_="foreignkey")


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "job_definitions" not in inspector.get_table_names():
        return
    _drop_job_runs_definition_fk()
    op.drop_table("job_definitions")


def downgrade() -> None:
    op.create_table(
        "job_definitions",
        sa.Column("job_key", sa.Text(), primary_key=True),
        sa.Column("job_type", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("schedule_type", sa.Text(), nullable=False),
        sa.Column(
            "schedule_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "market_calendar",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'NYSE'"),
        ),
        sa.Column("singleton_scope", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.create_index(
        "idx_job_definitions_enabled_type",
        "job_definitions",
        ["enabled", "job_type"],
        unique=False,
    )
    op.execute(
        """
        INSERT INTO job_definitions (
            job_key,
            job_type,
            enabled,
            schedule_type,
            schedule_json,
            payload_json,
            market_calendar,
            singleton_scope
        )
        SELECT DISTINCT
            job_key,
            job_type,
            false,
            'manual',
            '{}'::jsonb,
            '{}'::jsonb,
            'NYSE',
            NULL
        FROM job_runs
        ON CONFLICT (job_key) DO NOTHING
        """
    )
    op.alter_column("job_definitions", "enabled", server_default=None)
    op.alter_column("job_definitions", "schedule_json", server_default=None)
    op.alter_column("job_definitions", "payload_json", server_default=None)
    op.alter_column("job_definitions", "market_calendar", server_default=None)
    op.alter_column("job_definitions", "created_at", server_default=None)
    op.alter_column("job_definitions", "updated_at", server_default=None)
    op.create_foreign_key(
        "fk_job_runs_job_key_job_definitions",
        "job_runs",
        "job_definitions",
        ["job_key"],
        ["job_key"],
        ondelete="CASCADE",
    )
