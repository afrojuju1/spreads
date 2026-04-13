"""replace board/watchlist with canonical live opportunity selection

Revision ID: 20260412_0020
Revises: 20260410_0019
Create Date: 2026-04-12 16:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260412_0020"
down_revision = "20260410_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "collector_cycles",
        "selection_state_json",
        new_column_name="selection_memory_json",
    )

    op.alter_column(
        "collector_cycle_candidates",
        "bucket",
        new_column_name="selection_state",
        existing_type=sa.Text(),
    )
    op.alter_column(
        "collector_cycle_candidates",
        "position",
        new_column_name="selection_rank",
        existing_type=sa.Integer(),
    )
    op.add_column(
        "collector_cycle_candidates",
        sa.Column("state_reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "collector_cycle_candidates",
        sa.Column("origin", sa.Text(), nullable=True),
    )
    op.add_column(
        "collector_cycle_candidates",
        sa.Column("eligibility", sa.Text(), nullable=True),
    )

    op.execute(
        """
        UPDATE collector_cycle_candidates
        SET
            selection_state = CASE
                WHEN selection_state = 'board' THEN 'promotable'
                WHEN selection_state = 'watchlist' THEN 'monitor'
                WHEN selection_state = 'recovered' THEN 'monitor'
                ELSE 'monitor'
            END,
            state_reason = CASE
                WHEN selection_state = 'board' THEN 'legacy_board'
                WHEN selection_state = 'watchlist' THEN 'legacy_watchlist'
                WHEN selection_state = 'recovered' THEN 'history_recovery'
                ELSE 'legacy_candidate'
            END,
            origin = CASE
                WHEN selection_state = 'recovered' THEN 'history_recovery'
                ELSE 'live_scan'
            END,
            eligibility = CASE
                WHEN selection_state = 'recovered' THEN 'analysis_only'
                ELSE 'live'
            END
        """
    )

    op.alter_column(
        "collector_cycle_candidates",
        "state_reason",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.alter_column(
        "collector_cycle_candidates",
        "origin",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.alter_column(
        "collector_cycle_candidates",
        "eligibility",
        existing_type=sa.Text(),
        nullable=False,
    )

    op.alter_column(
        "signal_states",
        "active_bucket",
        new_column_name="active_selection_state",
        existing_type=sa.Text(),
    )
    op.alter_column(
        "signal_state_transitions",
        "active_bucket",
        new_column_name="active_selection_state",
        existing_type=sa.Text(),
    )

    op.alter_column(
        "opportunities",
        "classification",
        new_column_name="selection_state",
        existing_type=sa.Text(),
    )
    op.alter_column(
        "opportunities",
        "source_bucket",
        new_column_name="source_selection_state",
        existing_type=sa.Text(),
    )
    op.add_column(
        "opportunities",
        sa.Column("selection_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "opportunities",
        sa.Column("state_reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "opportunities",
        sa.Column("origin", sa.Text(), nullable=True),
    )
    op.add_column(
        "opportunities",
        sa.Column("eligibility", sa.Text(), nullable=True),
    )

    op.execute(
        """
        UPDATE opportunities
        SET
            selection_state = CASE
                WHEN selection_state = 'board' THEN 'promotable'
                ELSE 'monitor'
            END,
            source_selection_state = CASE
                WHEN source_selection_state = 'board' THEN 'promotable'
                WHEN source_selection_state = 'watchlist' THEN 'monitor'
                WHEN source_selection_state = 'recovered' THEN 'monitor'
                ELSE source_selection_state
            END,
            state_reason = COALESCE(selection_state, 'legacy_migration'),
            origin = 'live_scan',
            eligibility = 'live'
        """
    )

    op.execute(
        """
        UPDATE opportunities AS opportunity
        SET
            selection_state = candidate.selection_state,
            selection_rank = candidate.selection_rank,
            source_selection_state = candidate.selection_state,
            state_reason = candidate.state_reason,
            origin = candidate.origin,
            eligibility = candidate.eligibility
        FROM collector_cycle_candidates AS candidate
        WHERE opportunity.source_candidate_id = candidate.candidate_id
        """
    )

    op.alter_column(
        "opportunities",
        "state_reason",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.alter_column(
        "opportunities",
        "origin",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.alter_column(
        "opportunities",
        "eligibility",
        existing_type=sa.Text(),
        nullable=False,
    )

    op.execute(
        """
        UPDATE signal_states
        SET active_selection_state = CASE
            WHEN active_selection_state = 'board' THEN 'promotable'
            WHEN active_selection_state = 'watchlist' THEN 'monitor'
            WHEN active_selection_state = 'recovered' THEN 'monitor'
            ELSE active_selection_state
        END
        """
    )
    op.execute(
        """
        UPDATE signal_state_transitions
        SET active_selection_state = CASE
            WHEN active_selection_state = 'board' THEN 'promotable'
            WHEN active_selection_state = 'watchlist' THEN 'monitor'
            WHEN active_selection_state = 'recovered' THEN 'monitor'
            ELSE active_selection_state
        END
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE signal_state_transitions
        SET active_selection_state = CASE
            WHEN active_selection_state = 'promotable' THEN 'board'
            WHEN active_selection_state = 'monitor' THEN 'watchlist'
            ELSE active_selection_state
        END
        """
    )
    op.execute(
        """
        UPDATE signal_states
        SET active_selection_state = CASE
            WHEN active_selection_state = 'promotable' THEN 'board'
            WHEN active_selection_state = 'monitor' THEN 'watchlist'
            ELSE active_selection_state
        END
        """
    )
    op.alter_column(
        "signal_state_transitions",
        "active_selection_state",
        new_column_name="active_bucket",
        existing_type=sa.Text(),
    )
    op.alter_column(
        "signal_states",
        "active_selection_state",
        new_column_name="active_bucket",
        existing_type=sa.Text(),
    )

    op.execute(
        """
        UPDATE opportunities
        SET
            selection_state = CASE
                WHEN selection_state = 'promotable' THEN 'board'
                ELSE 'watchlist'
            END,
            source_selection_state = CASE
                WHEN source_selection_state = 'promotable' THEN 'board'
                WHEN source_selection_state = 'monitor' THEN 'watchlist'
                ELSE source_selection_state
            END
        """
    )
    op.drop_column("opportunities", "eligibility")
    op.drop_column("opportunities", "origin")
    op.drop_column("opportunities", "state_reason")
    op.drop_column("opportunities", "selection_rank")
    op.alter_column(
        "opportunities",
        "source_selection_state",
        new_column_name="source_bucket",
        existing_type=sa.Text(),
    )
    op.alter_column(
        "opportunities",
        "selection_state",
        new_column_name="classification",
        existing_type=sa.Text(),
    )

    op.execute(
        """
        UPDATE collector_cycle_candidates
        SET
            selection_state = CASE
                WHEN origin = 'history_recovery' THEN 'recovered'
                WHEN selection_state = 'promotable' THEN 'board'
                ELSE 'watchlist'
            END
        """
    )
    op.drop_column("collector_cycle_candidates", "eligibility")
    op.drop_column("collector_cycle_candidates", "origin")
    op.drop_column("collector_cycle_candidates", "state_reason")
    op.alter_column(
        "collector_cycle_candidates",
        "selection_rank",
        new_column_name="position",
        existing_type=sa.Integer(),
    )
    op.alter_column(
        "collector_cycle_candidates",
        "selection_state",
        new_column_name="bucket",
        existing_type=sa.Text(),
    )

    op.alter_column(
        "collector_cycles",
        "selection_memory_json",
        new_column_name="selection_state_json",
    )
