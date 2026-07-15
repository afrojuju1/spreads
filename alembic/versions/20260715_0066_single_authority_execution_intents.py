"""Cut execution lifecycle storage over to one intent authority.

Revision ID: 20260715_0066
Revises: 20260709_0065
Create Date: 2026-07-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260715_0066"
down_revision: str | None = "20260709_0065"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        DECLARE
            active_intents integer;
            active_attempts integer;
            open_positions integer;
            missing_approved_intents integer;
            nonapproved_current_intents integer;
            duplicate_attempt_links integer;
            missing_attempt_links integer;
            missing_successors integer;
            duplicate_successors integer;
            successor_cycles integer;
            missing_close_decisions integer;
        BEGIN
            SELECT count(*) INTO active_intents
            FROM execution_intents
            WHERE state IN ('pending', 'claimed', 'submitted', 'partially_filled');

            SELECT count(*) INTO active_attempts
            FROM execution_attempts
            WHERE status IN (
                'pending_submission', 'submit_unknown', 'working', 'accepted',
                'accepted_for_bidding', 'new', 'pending_new', 'submitted',
                'partially_filled', 'pending_cancel', 'pending_replace', 'replaced'
            );

            SELECT count(*) INTO open_positions
            FROM portfolio_positions
            WHERE status IN ('pending_open', 'partial_open', 'open', 'partial_close');

            SELECT count(*) INTO missing_approved_intents
            FROM trade_admissions a
            LEFT JOIN execution_intents i
              ON i.execution_intent_id = a.execution_intent_id
            WHERE a.admission_state = 'approved'
              AND i.execution_intent_id IS NULL;

            SELECT count(*) INTO nonapproved_current_intents
            FROM trade_admissions a
            JOIN execution_intents i
              ON i.execution_intent_id = a.execution_intent_id
            WHERE a.admission_state <> 'approved';

            SELECT count(*) INTO duplicate_attempt_links
            FROM (
                SELECT execution_attempt_id
                FROM execution_intents
                WHERE execution_attempt_id IS NOT NULL
                GROUP BY execution_attempt_id
                HAVING count(*) > 1
            ) duplicates;

            SELECT count(*) INTO missing_attempt_links
            FROM execution_intents i
            LEFT JOIN execution_attempts a
              ON a.execution_attempt_id = i.execution_attempt_id
            WHERE i.execution_attempt_id IS NOT NULL
              AND a.execution_attempt_id IS NULL;

            SELECT count(*) INTO missing_successors
            FROM execution_intents i
            LEFT JOIN execution_intents successor
              ON successor.execution_intent_id = i.superseded_by_id
            WHERE i.superseded_by_id IS NOT NULL
              AND successor.execution_intent_id IS NULL;

            SELECT count(*) INTO duplicate_successors
            FROM (
                SELECT superseded_by_id
                FROM execution_intents
                WHERE superseded_by_id IS NOT NULL
                GROUP BY superseded_by_id
                HAVING count(*) > 1
            ) duplicates;

            WITH RECURSIVE successor_chain AS (
                SELECT
                    execution_intent_id,
                    superseded_by_id,
                    ARRAY[execution_intent_id] AS path,
                    false AS cycle
                FROM execution_intents
                WHERE superseded_by_id IS NOT NULL
                UNION ALL
                SELECT
                    successor.execution_intent_id,
                    successor.superseded_by_id,
                    chain.path || successor.execution_intent_id,
                    successor.execution_intent_id = ANY(chain.path)
                FROM successor_chain chain
                JOIN execution_intents successor
                  ON successor.execution_intent_id = chain.superseded_by_id
                WHERE NOT chain.cycle
            )
            SELECT count(*) INTO successor_cycles
            FROM successor_chain
            WHERE cycle;

            SELECT count(*) INTO missing_close_decisions
            FROM trade_admissions a
            LEFT JOIN trade_close_decisions c
              ON c.close_decision_id = COALESCE(
                  a.evidence_json->>'close_decision_id',
                  a.evidence_json->'selected_close_decision'->>'close_decision_id'
              )
            WHERE a.admission_kind = 'position_close'
              AND c.close_decision_id IS NULL;

            IF active_intents > 0 OR active_attempts > 0 OR open_positions > 0 THEN
                RAISE EXCEPTION
                    'spr-t4z requires quiesced money path (active_intents=%, active_attempts=%, open_positions=%)',
                    active_intents, active_attempts, open_positions;
            END IF;
            IF missing_approved_intents > 0 OR nonapproved_current_intents > 0 THEN
                RAISE EXCEPTION
                    'spr-t4z admission mapping failed (missing_approved=%, nonapproved_with_current=%)',
                    missing_approved_intents, nonapproved_current_intents;
            END IF;
            IF duplicate_attempt_links > 0 OR missing_attempt_links > 0 THEN
                RAISE EXCEPTION
                    'spr-t4z attempt lineage failed (duplicate=%, missing=%)',
                    duplicate_attempt_links, missing_attempt_links;
            END IF;
            IF missing_successors > 0 OR duplicate_successors > 0 OR successor_cycles > 0 THEN
                RAISE EXCEPTION
                    'spr-t4z successor lineage failed (missing=%, duplicate=%, cycles=%)',
                    missing_successors, duplicate_successors, successor_cycles;
            END IF;
            IF missing_close_decisions > 0 THEN
                RAISE EXCEPTION 'spr-t4z found % close admissions without close decisions', missing_close_decisions;
            END IF;
        END $$;
        """
    )

    op.add_column("trade_admissions", sa.Column("source_object_type", sa.Text(), nullable=True))
    op.add_column("trade_admissions", sa.Column("source_object_id", sa.Text(), nullable=True))
    op.add_column("trade_admissions", sa.Column("close_decision_id", sa.Text(), nullable=True))

    op.add_column("execution_intents", sa.Column("admission_decision_id", sa.Text(), nullable=True))
    op.add_column("execution_intents", sa.Column("close_decision_id", sa.Text(), nullable=True))
    op.add_column("execution_intents", sa.Column("position_id", sa.Text(), nullable=True))
    op.add_column("execution_intents", sa.Column("intent_kind", sa.Text(), nullable=True))
    op.add_column("execution_intents", sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("execution_intents", sa.Column("workflow_id", sa.Text(), nullable=True))
    op.add_column("execution_intents", sa.Column("workflow_run_id", sa.Text(), nullable=True))
    op.add_column("execution_intents", sa.Column("supersedes_execution_intent_id", sa.Text(), nullable=True))
    op.add_column("execution_intents", sa.Column("state_version", sa.Integer(), nullable=True))

    op.add_column("execution_attempts", sa.Column("execution_intent_id", sa.Text(), nullable=True))

    op.execute(
        """
        UPDATE trade_admissions
        SET
            source_object_type = CASE
                WHEN admission_kind = 'position_close' THEN 'close_decision'
                ELSE 'trade_decision'
            END,
            source_object_id = CASE
                WHEN admission_kind = 'position_close' THEN COALESCE(
                    evidence_json->>'close_decision_id',
                    evidence_json->'selected_close_decision'->>'close_decision_id'
                )
                ELSE trade_decision_id
            END,
            close_decision_id = CASE
                WHEN admission_kind = 'position_close' THEN COALESCE(
                    evidence_json->>'close_decision_id',
                    evidence_json->'selected_close_decision'->>'close_decision_id'
                )
                ELSE NULL
            END
        """
    )
    op.execute(
        """
        UPDATE execution_intents i
        SET
            admission_decision_id = a.admission_decision_id,
            close_decision_id = CASE
                WHEN a.admission_kind = 'position_close' THEN a.close_decision_id
                ELSE NULL
            END
        FROM trade_admissions a
        WHERE a.admission_state = 'approved'
          AND a.execution_intent_id = i.execution_intent_id
        """
    )
    op.execute(
        """
        UPDATE execution_intents i
        SET
            position_id = i.strategy_position_id,
            intent_kind = CASE
                WHEN i.action_type IN ('open', 'buy', 'buy_to_open', 'sell_to_open') THEN 'open'
                WHEN i.action_type IN ('close', 'sell', 'sell_to_close', 'buy_to_close') THEN 'close'
                ELSE i.action_type
            END,
            claimed_at = CASE WHEN i.claim_token IS NOT NULL THEN i.updated_at ELSE NULL END,
            workflow_id = COALESCE(
                NULLIF(i.payload_json->>'workflow_id', ''),
                (
                    SELECT e.workflow_id
                    FROM engine_events e
                    WHERE e.execution_intent_id = i.execution_intent_id
                      AND e.workflow_id IS NOT NULL
                    ORDER BY e.recorded_at DESC, e.engine_event_id DESC
                    LIMIT 1
                )
            ),
            workflow_run_id = COALESCE(
                NULLIF(i.payload_json->>'workflow_run_id', ''),
                (
                    SELECT e.workflow_run_id
                    FROM engine_events e
                    WHERE e.execution_intent_id = i.execution_intent_id
                      AND e.workflow_run_id IS NOT NULL
                    ORDER BY e.recorded_at DESC, e.engine_event_id DESC
                    LIMIT 1
                )
            ),
            state_version = 1
        """
    )
    op.execute(
        """
        UPDATE execution_intents successor
        SET supersedes_execution_intent_id = predecessor.execution_intent_id
        FROM execution_intents predecessor
        WHERE predecessor.superseded_by_id = successor.execution_intent_id
        """
    )
    op.execute(
        """
        UPDATE execution_attempts attempt
        SET execution_intent_id = intent.execution_intent_id
        FROM execution_intents intent
        WHERE intent.execution_attempt_id = attempt.execution_attempt_id
        """
    )

    op.execute(
        """
        DO $$
        DECLARE
            missing_admission_source integer;
            missing_approved_lineage integer;
            missing_attempt_lineage integer;
            invalid_intent_kind integer;
        BEGIN
            SELECT count(*) INTO missing_admission_source
            FROM trade_admissions
            WHERE source_object_type IS NULL OR source_object_id IS NULL;

            SELECT count(*) INTO missing_approved_lineage
            FROM trade_admissions a
            LEFT JOIN execution_intents i
              ON i.admission_decision_id = a.admission_decision_id
            WHERE a.admission_state = 'approved'
              AND i.execution_intent_id IS NULL;

            SELECT count(*) INTO missing_attempt_lineage
            FROM execution_intents i
            JOIN execution_attempts a
              ON a.execution_attempt_id = i.execution_attempt_id
            WHERE a.execution_intent_id IS DISTINCT FROM i.execution_intent_id;

            SELECT count(*) INTO invalid_intent_kind
            FROM execution_intents
            WHERE intent_kind NOT IN ('open', 'close');

            IF missing_admission_source > 0 OR missing_approved_lineage > 0
               OR missing_attempt_lineage > 0 OR invalid_intent_kind > 0 THEN
                RAISE EXCEPTION
                    'spr-t4z backfill invariant failed (admission_source=%, approved_lineage=%, attempt_lineage=%, intent_kind=%)',
                    missing_admission_source, missing_approved_lineage,
                    missing_attempt_lineage, invalid_intent_kind;
            END IF;
        END $$;
        """
    )

    op.alter_column("trade_admissions", "source_object_type", nullable=False)
    op.alter_column("trade_admissions", "source_object_id", nullable=False)
    op.alter_column("execution_intents", "intent_kind", nullable=False)
    op.alter_column("execution_intents", "state_version", nullable=False)

    op.create_foreign_key(
        "trade_admissions_close_decision_id_fkey",
        "trade_admissions",
        "trade_close_decisions",
        ["close_decision_id"],
        ["close_decision_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "trade_admissions_position_id_fkey",
        "trade_admissions",
        "portfolio_positions",
        ["position_id"],
        ["position_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "execution_intents_admission_decision_id_fkey",
        "execution_intents",
        "trade_admissions",
        ["admission_decision_id"],
        ["admission_decision_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "execution_intents_close_decision_id_fkey",
        "execution_intents",
        "trade_close_decisions",
        ["close_decision_id"],
        ["close_decision_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "execution_intents_position_id_fkey",
        "execution_intents",
        "portfolio_positions",
        ["position_id"],
        ["position_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "execution_intents_supersedes_execution_intent_id_fkey",
        "execution_intents",
        "execution_intents",
        ["supersedes_execution_intent_id"],
        ["execution_intent_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "execution_attempts_execution_intent_id_fkey",
        "execution_attempts",
        "execution_intents",
        ["execution_intent_id"],
        ["execution_intent_id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_execution_intents_intent_kind",
        "execution_intents",
        "intent_kind IN ('open', 'close')",
    )
    op.create_check_constraint(
        "ck_execution_intents_state_version_positive",
        "execution_intents",
        "state_version > 0",
    )

    op.create_index("idx_trade_admissions_source", "trade_admissions", ["source_object_type", "source_object_id"])
    op.create_index("idx_trade_admissions_close_decision", "trade_admissions", ["close_decision_id"])
    op.create_index("idx_execution_intents_admission_decision", "execution_intents", ["admission_decision_id"])
    op.create_index("idx_execution_intents_close_decision", "execution_intents", ["close_decision_id"])
    op.create_index("idx_execution_intents_position", "execution_intents", ["position_id"])
    op.create_index("idx_execution_intents_workflow", "execution_intents", ["workflow_id"])
    op.create_index("idx_execution_intents_supersedes", "execution_intents", ["supersedes_execution_intent_id"])
    op.create_index(
        "ux_execution_intents_admission_decision",
        "execution_intents",
        ["admission_decision_id"],
        unique=True,
        postgresql_where=sa.text("admission_decision_id IS NOT NULL"),
    )
    op.create_index(
        "ux_execution_intents_supersedes",
        "execution_intents",
        ["supersedes_execution_intent_id"],
        unique=True,
        postgresql_where=sa.text("supersedes_execution_intent_id IS NOT NULL"),
    )
    op.create_index("idx_execution_attempts_execution_intent", "execution_attempts", ["execution_intent_id"])
    op.create_index(
        "ux_execution_attempts_execution_intent",
        "execution_attempts",
        ["execution_intent_id"],
        unique=True,
        postgresql_where=sa.text("execution_intent_id IS NOT NULL"),
    )

    op.drop_constraint("trade_admissions_execution_intent_id_fkey", "trade_admissions", type_="foreignkey")
    op.drop_constraint("trade_execution_attempts_execution_intent_id_fkey", "trade_execution_attempts", type_="foreignkey")
    op.drop_constraint("execution_intents_execution_attempt_id_fkey", "execution_intents", type_="foreignkey")
    op.drop_constraint("execution_intents_strategy_position_id_fkey", "execution_intents", type_="foreignkey")

    op.drop_index("idx_trade_admissions_intent", table_name="trade_admissions")
    op.drop_index("idx_execution_intents_execution_attempt", table_name="execution_intents")
    op.drop_index("idx_execution_intents_strategy_position", table_name="execution_intents")

    op.drop_column("trade_admissions", "execution_intent_id")
    op.drop_column("trade_admissions", "execution_attempt_id")
    op.drop_column("execution_intents", "execution_attempt_id")
    op.drop_column("execution_intents", "strategy_position_id")
    op.drop_column("execution_intents", "superseded_by_id")
    op.drop_column("execution_intents", "action_type")

    op.drop_table("execution_intent_events")
    op.drop_table("trade_execution_intents")


def downgrade() -> None:
    raise RuntimeError(
        "20260715_0066 is a clean authority cutover. Restore the verified pre-cutover Postgres backup instead of downgrading."
    )
