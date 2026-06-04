"""remove legacy execution and position lineage

Revision ID: 20260604_0051
Revises: 20260604_0050
Create Date: 2026-06-04 14:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260604_0051"
down_revision = "20260604_0050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("portfolio_positions", sa.Column("source_object_type", sa.Text(), nullable=True))
    op.add_column("portfolio_positions", sa.Column("source_object_id", sa.Text(), nullable=True))
    op.add_column(
        "portfolio_positions",
        sa.Column(
            "trade_signal_id",
            sa.Text(),
            sa.ForeignKey("trade_signals.trade_signal_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "portfolio_positions",
        sa.Column(
            "trade_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_decisions.trade_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "portfolio_positions",
        sa.Column(
            "admission_decision_id",
            sa.Text(),
            sa.ForeignKey("trade_admissions.admission_decision_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "idx_portfolio_positions_source_object",
        "portfolio_positions",
        ["source_object_type", "source_object_id"],
    )
    op.create_index("idx_portfolio_positions_trade_signal", "portfolio_positions", ["trade_signal_id"])
    op.create_index("idx_portfolio_positions_trade_decision", "portfolio_positions", ["trade_decision_id"])
    op.create_index("idx_portfolio_positions_admission_decision", "portfolio_positions", ["admission_decision_id"])

    op.execute(
        sa.text(
            """
            UPDATE portfolio_positions AS position
            SET source_object_type = attempt.source_object_type,
                source_object_id = attempt.source_object_id,
                trade_signal_id = attempt.trade_signal_id,
                trade_decision_id = attempt.trade_decision_id,
                admission_decision_id = attempt.admission_decision_id
            FROM execution_attempts AS attempt
            WHERE position.open_execution_attempt_id = attempt.execution_attempt_id
              AND (
                  position.source_object_type IS DISTINCT FROM attempt.source_object_type
                  OR position.source_object_id IS DISTINCT FROM attempt.source_object_id
                  OR position.trade_signal_id IS DISTINCT FROM attempt.trade_signal_id
                  OR position.trade_decision_id IS DISTINCT FROM attempt.trade_decision_id
                  OR position.admission_decision_id IS DISTINCT FROM attempt.admission_decision_id
              );

            UPDATE execution_attempts
            SET request_json = COALESCE(request_json, '{}'::jsonb)
                - 'pipeline_id'
                - 'opportunity_id'
                - 'candidate_id'
                - 'source_opportunity_id'
                - 'opportunity_decision_id'
                - 'risk_decision_id',
                candidate_json = COALESCE(candidate_json, '{}'::jsonb)
                - 'pipeline_id'
                - 'opportunity_id'
                - 'candidate_id'
                - 'source_opportunity_id'
                - 'opportunity_decision_id'
                - 'risk_decision_id';

            UPDATE portfolio_positions
            SET source_job_type = NULL
            WHERE source_job_type IN ('symbol_feed', 'finviz_direct');

            UPDATE portfolio_positions
            SET source_job_key = NULL
            WHERE source_job_key = 'finviz_direct'
               OR source_job_key LIKE 'symbol_feed:%';
            """
        )
    )

    op.execute(
        sa.text(
            """
            DROP INDEX IF EXISTS idx_execution_attempts_pipeline_requested;
            DROP INDEX IF EXISTS idx_execution_attempts_candidate_requested;
            DROP INDEX IF EXISTS idx_execution_attempts_opportunity_requested;
            DROP INDEX IF EXISTS idx_execution_attempts_risk_decision_requested;
            DROP INDEX IF EXISTS idx_portfolio_positions_pipeline_updated;
            DROP INDEX IF EXISTS idx_portfolio_positions_pipeline_status;

            ALTER TABLE execution_attempts DROP CONSTRAINT IF EXISTS fk_execution_attempts_opportunity_id;
            ALTER TABLE execution_attempts DROP CONSTRAINT IF EXISTS execution_attempts_opportunity_id_fkey;
            ALTER TABLE execution_attempts DROP CONSTRAINT IF EXISTS fk_execution_attempts_risk_decision_id;
            ALTER TABLE execution_attempts DROP CONSTRAINT IF EXISTS execution_attempts_risk_decision_id_fkey;
            ALTER TABLE portfolio_positions DROP CONSTRAINT IF EXISTS portfolio_positions_source_opportunity_id_fkey;

            ALTER TABLE execution_attempts DROP COLUMN IF EXISTS pipeline_id;
            ALTER TABLE execution_attempts DROP COLUMN IF EXISTS opportunity_id;
            ALTER TABLE execution_attempts DROP COLUMN IF EXISTS risk_decision_id;
            ALTER TABLE execution_attempts DROP COLUMN IF EXISTS candidate_id;
            ALTER TABLE portfolio_positions DROP COLUMN IF EXISTS pipeline_id;
            ALTER TABLE portfolio_positions DROP COLUMN IF EXISTS source_opportunity_id;

            DROP TABLE IF EXISTS risk_decisions CASCADE;
            """
        )
    )


def downgrade() -> None:
    pass
