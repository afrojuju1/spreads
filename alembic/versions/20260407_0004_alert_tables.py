"""add alert tables

Revision ID: 20260407_0004
Revises: 20260407_0003
Create Date: 2026-04-07 23:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260407_0004"
down_revision = "20260407_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "alert_events",
        sa.Column("alert_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("cycle_id", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("alert_type", sa.Text(), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("delivery_target", sa.Text(), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("response_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_alert_events_session_label_created_at",
        "alert_events",
        ["session_date", "label", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_alert_events_symbol_created_at",
        "alert_events",
        ["symbol", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_alert_events_dedupe_created_at",
        "alert_events",
        ["dedupe_key", "created_at"],
        unique=False,
    )

    op.create_table(
        "alert_state",
        sa.Column("dedupe_key", sa.Text(), primary_key=True),
        sa.Column("last_alert_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_cycle_id", sa.Text(), nullable=False),
        sa.Column("last_alert_type", sa.Text(), nullable=False),
        sa.Column("state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("alert_state")
    op.drop_index("idx_alert_events_dedupe_created_at", table_name="alert_events")
    op.drop_index("idx_alert_events_symbol_created_at", table_name="alert_events")
    op.drop_index("idx_alert_events_session_label_created_at", table_name="alert_events")
    op.drop_table("alert_events")
