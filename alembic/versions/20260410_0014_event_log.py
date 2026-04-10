"""add canonical event log

Revision ID: 20260410_0014
Revises: 20260409_0013
Create Date: 2026-04-10 00:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260410_0014"
down_revision = "20260409_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "event_log",
        sa.Column("event_id", sa.Text(), primary_key=True),
        sa.Column("event_class", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_key", sa.Text(), nullable=False),
        sa.Column("session_date", sa.Date(), nullable=True),
        sa.Column("market_session", sa.Text(), nullable=True),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("schema_version", sa.Text(), nullable=False),
        sa.Column("producer_version", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Text(), nullable=True),
        sa.Column("causation_id", sa.Text(), nullable=True),
    )
    op.create_index("idx_event_log_occurred_at", "event_log", ["occurred_at"], unique=False)
    op.create_index("idx_event_log_class_occurred", "event_log", ["event_class", "occurred_at"], unique=False)
    op.create_index("idx_event_log_topic_occurred", "event_log", ["topic", "occurred_at"], unique=False)
    op.create_index(
        "idx_event_log_entity_occurred",
        "event_log",
        ["entity_type", "entity_key", "occurred_at"],
        unique=False,
    )
    op.create_index("idx_event_log_session_occurred", "event_log", ["session_date", "occurred_at"], unique=False)
    op.create_index(
        "idx_event_log_correlation_occurred",
        "event_log",
        ["correlation_id", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_event_log_correlation_occurred", table_name="event_log")
    op.drop_index("idx_event_log_session_occurred", table_name="event_log")
    op.drop_index("idx_event_log_entity_occurred", table_name="event_log")
    op.drop_index("idx_event_log_topic_occurred", table_name="event_log")
    op.drop_index("idx_event_log_class_occurred", table_name="event_log")
    op.drop_index("idx_event_log_occurred_at", table_name="event_log")
    op.drop_table("event_log")
