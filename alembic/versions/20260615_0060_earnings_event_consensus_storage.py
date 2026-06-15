"""add earnings event consensus storage

Revision ID: 20260615_0060
Revises: 20260612_0059
Create Date: 2026-06-15 14:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260615_0060"
down_revision = "20260612_0059"
branch_labels = None
depends_on = None


def _jsonb() -> postgresql.JSONB:
    return postgresql.JSONB(astext_type=sa.Text())


def upgrade() -> None:
    op.create_table(
        "provider_fetch_audit",
        sa.Column("audit_id", sa.Text(), primary_key=True),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("params_hash", sa.Text(), nullable=False),
        sa.Column("params_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("coverage_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("coverage_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("page_key", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("cache_hit", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("payload_hash", sa.Text(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("backoff_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_provider_fetch_audit_provider_endpoint_fetched",
        "provider_fetch_audit",
        ["provider", "endpoint", "fetched_at"],
        unique=False,
    )
    op.create_index(
        "idx_provider_fetch_audit_provider_params",
        "provider_fetch_audit",
        ["provider", "endpoint", "params_hash", "page_key"],
        unique=False,
    )
    op.create_index(
        "idx_provider_fetch_audit_window",
        "provider_fetch_audit",
        ["coverage_start", "coverage_end"],
        unique=False,
    )

    op.create_table(
        "earnings_event_consensus",
        sa.Column("consensus_id", sa.Text(), primary_key=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("event_date", sa.Date(), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_timing", sa.Text(), nullable=False),
        sa.Column("event_status", sa.Text(), nullable=False),
        sa.Column("primary_source", sa.Text(), nullable=True),
        sa.Column("supporting_sources_json", _jsonb(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("conflicting_sources_json", _jsonb(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("consensus_status", sa.Text(), nullable=False),
        sa.Column("source_confidence", sa.Text(), nullable=False),
        sa.Column("timing_confidence", sa.Text(), nullable=False),
        sa.Column("provider_payload_json", _jsonb(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stale_after", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("symbol", "event_date", name="uq_earnings_event_consensus_symbol_date"),
    )
    op.create_index(
        "idx_earnings_event_consensus_symbol_date",
        "earnings_event_consensus",
        ["symbol", "event_date"],
        unique=False,
    )
    op.create_index(
        "idx_earnings_event_consensus_stale_after",
        "earnings_event_consensus",
        ["stale_after"],
        unique=False,
    )
    op.create_index(
        "idx_earnings_event_consensus_status_date",
        "earnings_event_consensus",
        ["consensus_status", "event_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_earnings_event_consensus_status_date", table_name="earnings_event_consensus")
    op.drop_index("idx_earnings_event_consensus_stale_after", table_name="earnings_event_consensus")
    op.drop_index("idx_earnings_event_consensus_symbol_date", table_name="earnings_event_consensus")
    op.drop_table("earnings_event_consensus")
    op.drop_index("idx_provider_fetch_audit_window", table_name="provider_fetch_audit")
    op.drop_index("idx_provider_fetch_audit_provider_params", table_name="provider_fetch_audit")
    op.drop_index("idx_provider_fetch_audit_provider_endpoint_fetched", table_name="provider_fetch_audit")
    op.drop_table("provider_fetch_audit")
