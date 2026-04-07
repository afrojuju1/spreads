"""add calendar event tables

Revision ID: 20260407_0002
Revises: 20260407_0001
Create Date: 2026-04-07 18:55:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260407_0002"
down_revision = "20260407_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "calendar_events",
        sa.Column("event_id", sa.Text(), primary_key=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("asset_scope", sa.Text(), nullable=True),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_confidence", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_calendar_events_symbol",
        "calendar_events",
        ["symbol", "scheduled_at"],
        unique=False,
    )
    op.create_index(
        "idx_calendar_events_asset_scope",
        "calendar_events",
        ["asset_scope", "scheduled_at"],
        unique=False,
    )

    op.create_table(
        "calendar_event_refresh_state",
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column("coverage_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("coverage_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source", "scope_key"),
    )


def downgrade() -> None:
    op.drop_table("calendar_event_refresh_state")
    op.drop_index("idx_calendar_events_asset_scope", table_name="calendar_events")
    op.drop_index("idx_calendar_events_symbol", table_name="calendar_events")
    op.drop_table("calendar_events")
