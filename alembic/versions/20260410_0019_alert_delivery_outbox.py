"""refactor alert events into durable delivery outbox

Revision ID: 20260410_0019
Revises: 20260410_0018
Create Date: 2026-04-10 14:45:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260410_0019"
down_revision = "20260410_0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "alert_events",
        sa.Column("record_kind", sa.Text(), nullable=False, server_default="delivery"),
    )
    op.add_column(
        "alert_events",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("timezone('utc', now())")),
    )
    op.add_column("alert_events", sa.Column("session_id", sa.Text(), nullable=True))
    op.add_column("alert_events", sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("alert_events", sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("alert_events", sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("alert_events", sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("alert_events", sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("alert_events", sa.Column("planner_job_run_id", sa.Text(), nullable=True))
    op.add_column("alert_events", sa.Column("delivery_job_run_id", sa.Text(), nullable=True))
    op.add_column("alert_events", sa.Column("worker_name", sa.Text(), nullable=True))
    op.add_column(
        "alert_events",
        sa.Column("state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.alter_column("alert_events", "delivery_target", existing_type=sa.Text(), nullable=True)
    op.alter_column(
        "alert_events",
        "payload_json",
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        nullable=True,
    )

    op.execute(
        """
        UPDATE alert_events
        SET
            record_kind = 'delivery',
            updated_at = created_at,
            session_id = 'live:' || label || ':' || session_date::text,
            attempt_count = CASE
                WHEN status IN ('delivered', 'skipped', 'failed') THEN 1
                ELSE 0
            END,
            delivered_at = CASE
                WHEN status = 'delivered' THEN created_at
                ELSE NULL
            END,
            status = CASE
                WHEN status = 'skipped' THEN 'suppressed'
                WHEN status = 'failed' THEN 'dead_letter'
                ELSE status
            END
        """
    )

    op.alter_column("alert_events", "record_kind", server_default=None)
    op.alter_column("alert_events", "updated_at", server_default=None)
    op.alter_column("alert_events", "attempt_count", server_default=None)

    op.drop_index("idx_alert_events_dedupe_created_at", table_name="alert_events")
    op.drop_index("idx_alert_events_symbol_created_at", table_name="alert_events")
    op.drop_index("idx_alert_events_session_label_created_at", table_name="alert_events")
    op.create_index(
        "idx_alert_events_kind_session_label_created_at",
        "alert_events",
        ["record_kind", "session_date", "label", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_alert_events_kind_symbol_created_at",
        "alert_events",
        ["record_kind", "symbol", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_alert_events_kind_dedupe_target_created_at",
        "alert_events",
        ["record_kind", "dedupe_key", "delivery_target", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_alert_events_kind_status_next_attempt_at",
        "alert_events",
        ["record_kind", "status", "next_attempt_at"],
        unique=False,
    )
    op.create_index(
        "idx_alert_events_kind_status_claimed_at",
        "alert_events",
        ["record_kind", "status", "claimed_at"],
        unique=False,
    )

    op.drop_table("alert_state")


def downgrade() -> None:
    op.create_table(
        "alert_state",
        sa.Column("dedupe_key", sa.Text(), primary_key=True),
        sa.Column("last_alert_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_cycle_id", sa.Text(), nullable=False),
        sa.Column("last_alert_type", sa.Text(), nullable=False),
        sa.Column("state_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )

    op.execute("DELETE FROM alert_events WHERE record_kind = 'score_anchor'")
    op.execute(
        """
        UPDATE alert_events
        SET
            status = CASE
                WHEN status = 'suppressed' THEN 'skipped'
                WHEN status = 'dead_letter' THEN 'failed'
                ELSE status
            END
        """
    )

    op.drop_index("idx_alert_events_kind_status_claimed_at", table_name="alert_events")
    op.drop_index("idx_alert_events_kind_status_next_attempt_at", table_name="alert_events")
    op.drop_index("idx_alert_events_kind_dedupe_target_created_at", table_name="alert_events")
    op.drop_index("idx_alert_events_kind_symbol_created_at", table_name="alert_events")
    op.drop_index("idx_alert_events_kind_session_label_created_at", table_name="alert_events")
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

    op.alter_column(
        "alert_events",
        "payload_json",
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        nullable=False,
    )
    op.alter_column("alert_events", "delivery_target", existing_type=sa.Text(), nullable=False)
    op.drop_column("alert_events", "state_json")
    op.drop_column("alert_events", "worker_name")
    op.drop_column("alert_events", "delivery_job_run_id")
    op.drop_column("alert_events", "planner_job_run_id")
    op.drop_column("alert_events", "delivered_at")
    op.drop_column("alert_events", "next_attempt_at")
    op.drop_column("alert_events", "last_attempt_at")
    op.drop_column("alert_events", "claimed_at")
    op.drop_column("alert_events", "attempt_count")
    op.drop_column("alert_events", "session_id")
    op.drop_column("alert_events", "updated_at")
    op.drop_column("alert_events", "record_kind")
