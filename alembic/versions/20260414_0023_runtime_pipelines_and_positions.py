"""add runtime pipelines, cycles, and portfolio positions

Revision ID: 20260414_0023
Revises: 20260413_0022
Create Date: 2026-04-14 09:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260414_0023"
down_revision = "20260413_0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pipelines",
        sa.Column("pipeline_id", sa.Text(), primary_key=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("source_job_key", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("universe_label", sa.Text(), nullable=True),
        sa.Column("style_profile", sa.Text(), nullable=True),
        sa.Column("default_horizon_intent", sa.Text(), nullable=True),
        sa.Column("strategy_families_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("product_scope_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("policy_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ux_pipelines_label", "pipelines", ["label"], unique=True)
    op.create_index("idx_pipelines_enabled_updated", "pipelines", ["enabled", "updated_at"], unique=False)

    op.create_table(
        "pipeline_cycles",
        sa.Column("cycle_id", sa.Text(), primary_key=True),
        sa.Column(
            "pipeline_id",
            sa.Text(),
            sa.ForeignKey("pipelines.pipeline_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("market_date", sa.Date(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "job_run_id",
            sa.Text(),
            sa.ForeignKey("job_runs.job_run_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("universe_label", sa.Text(), nullable=True),
        sa.Column("strategy_mode", sa.Text(), nullable=True),
        sa.Column("legacy_profile", sa.Text(), nullable=True),
        sa.Column("greeks_source", sa.Text(), nullable=True),
        sa.Column("symbols_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("failures_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("selection_memory_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("summary_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )
    op.create_index(
        "idx_pipeline_cycles_pipeline_generated",
        "pipeline_cycles",
        ["pipeline_id", "generated_at"],
        unique=False,
    )
    op.create_index(
        "idx_pipeline_cycles_pipeline_market_date",
        "pipeline_cycles",
        ["pipeline_id", "market_date"],
        unique=False,
    )

    op.create_table(
        "portfolio_positions",
        sa.Column("position_id", sa.Text(), primary_key=True),
        sa.Column("pipeline_id", sa.Text(), nullable=False),
        sa.Column(
            "source_opportunity_id",
            sa.Text(),
            sa.ForeignKey("opportunities.opportunity_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "legacy_session_position_id",
            sa.Text(),
            sa.ForeignKey("session_positions.session_position_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "open_execution_attempt_id",
            sa.Text(),
            sa.ForeignKey("execution_attempts.execution_attempt_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("root_symbol", sa.Text(), nullable=False),
        sa.Column("strategy_family", sa.Text(), nullable=False),
        sa.Column("style_profile", sa.Text(), nullable=True),
        sa.Column("horizon_intent", sa.Text(), nullable=True),
        sa.Column("product_class", sa.Text(), nullable=True),
        sa.Column("market_date_opened", sa.Date(), nullable=False),
        sa.Column("market_date_closed", sa.Date(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("legs_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("economics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("strategy_metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("requested_quantity", sa.Integer(), nullable=False),
        sa.Column("opened_quantity", sa.Float(), nullable=False),
        sa.Column("remaining_quantity", sa.Float(), nullable=False),
        sa.Column("entry_value", sa.Float(), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0"),
        sa.Column("unrealized_pnl", sa.Float(), nullable=True),
        sa.Column("close_mark", sa.Float(), nullable=True),
        sa.Column("close_mark_source", sa.Text(), nullable=True),
        sa.Column("close_marked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_broker_status", sa.Text(), nullable=True),
        sa.Column("exit_policy_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("risk_policy_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_job_type", sa.Text(), nullable=True),
        sa.Column("source_job_key", sa.Text(), nullable=True),
        sa.Column("source_job_run_id", sa.Text(), nullable=True),
        sa.Column("last_exit_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_exit_reason", sa.Text(), nullable=True),
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconciliation_status", sa.Text(), nullable=True),
        sa.Column("reconciliation_note", sa.Text(), nullable=True),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "idx_portfolio_positions_pipeline_updated",
        "portfolio_positions",
        ["pipeline_id", "updated_at"],
        unique=False,
    )
    op.create_index(
        "idx_portfolio_positions_pipeline_status",
        "portfolio_positions",
        ["pipeline_id", "status"],
        unique=False,
    )
    op.create_index(
        "ux_portfolio_positions_open_attempt",
        "portfolio_positions",
        ["open_execution_attempt_id"],
        unique=True,
    )

    op.create_table(
        "position_closes",
        sa.Column("position_close_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "position_id",
            sa.Text(),
            sa.ForeignKey("portfolio_positions.position_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "execution_attempt_id",
            sa.Text(),
            sa.ForeignKey("execution_attempts.execution_attempt_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "legacy_session_position_id",
            sa.Text(),
            sa.ForeignKey("session_positions.session_position_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("closed_quantity", sa.Float(), nullable=False),
        sa.Column("exit_value", sa.Float(), nullable=True),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0"),
        sa.Column("broker_order_id", sa.Text(), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_position_closes_position_closed", "position_closes", ["position_id", "closed_at"], unique=False)
    op.create_index("ux_position_closes_execution_attempt", "position_closes", ["execution_attempt_id"], unique=True)

    op.add_column("execution_attempts", sa.Column("pipeline_id", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("market_date", sa.Date(), nullable=True))
    op.add_column("execution_attempts", sa.Column("position_id", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("root_symbol", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("strategy_family", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("style_profile", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("horizon_intent", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("product_class", sa.Text(), nullable=True))
    op.add_column("execution_attempts", sa.Column("requested_quantity", sa.Integer(), nullable=True))
    op.add_column("execution_attempts", sa.Column("requested_limit_price", sa.Float(), nullable=True))
    op.create_index(
        "idx_execution_attempts_pipeline_requested",
        "execution_attempts",
        ["pipeline_id", "requested_at"],
        unique=False,
    )
    op.create_index(
        "idx_execution_attempts_runtime_position_requested",
        "execution_attempts",
        ["position_id", "requested_at"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_execution_attempts_position_id",
        "execution_attempts",
        "portfolio_positions",
        ["position_id"],
        ["position_id"],
        ondelete="SET NULL",
    )

    op.add_column("opportunities", sa.Column("pipeline_id", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("market_date", sa.Date(), nullable=True))
    op.add_column("opportunities", sa.Column("cycle_id", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("root_symbol", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("style_profile", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("horizon_intent", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("product_class", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("expiration_date", sa.Date(), nullable=True))
    op.add_column("opportunities", sa.Column("side_bias", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("eligibility_state", sa.Text(), nullable=True))
    op.add_column("opportunities", sa.Column("promotion_score", sa.Float(), nullable=True))
    op.add_column("opportunities", sa.Column("execution_score", sa.Float(), nullable=True))
    op.add_column("opportunities", sa.Column("legs_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("opportunities", sa.Column("economics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("opportunities", sa.Column("strategy_metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("opportunities", sa.Column("order_payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("opportunities", sa.Column("evidence_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.create_index(
        "idx_opportunities_pipeline_market_lifecycle",
        "opportunities",
        ["pipeline_id", "market_date", "lifecycle_state"],
        unique=False,
    )

    op.execute(
        """
        INSERT INTO pipelines (
            pipeline_id,
            label,
            name,
            enabled,
            universe_label,
            style_profile,
            default_horizon_intent,
            strategy_families_json,
            product_scope_json,
            policy_json,
            created_at,
            updated_at
        )
        SELECT
            'pipeline:' || lower(label) AS pipeline_id,
            label,
            label,
            TRUE,
            max(universe_label) AS universe_label,
            CASE
                WHEN lower(max(profile)) = 'core' THEN 'carry'
                WHEN lower(max(profile)) = 'swing' THEN 'swing'
                ELSE 'active'
            END AS style_profile,
            CASE
                WHEN lower(max(profile)) = '0dte' THEN 'same_day'
                WHEN lower(max(profile)) IN ('weekly', 'micro') THEN 'short_dated'
                WHEN lower(max(profile)) = 'core' THEN 'carry'
                WHEN lower(max(profile)) = 'swing' THEN 'multi_day'
                ELSE 'short_dated'
            END AS default_horizon_intent,
            jsonb_build_array(max(strategy)) AS strategy_families_json,
            jsonb_build_object('legacy_labels', jsonb_build_array(label)) AS product_scope_json,
            jsonb_build_object('legacy_profile', max(profile)) AS policy_json,
            min(generated_at) AS created_at,
            max(generated_at) AS updated_at
        FROM collector_cycles
        GROUP BY label
        ON CONFLICT (pipeline_id) DO NOTHING
        """
    )

    op.execute(
        """
        INSERT INTO pipeline_cycles (
            cycle_id,
            pipeline_id,
            label,
            market_date,
            generated_at,
            job_run_id,
            universe_label,
            strategy_mode,
            legacy_profile,
            greeks_source,
            symbols_json,
            failures_json,
            selection_memory_json,
            summary_json
        )
        SELECT
            cycle_id,
            'pipeline:' || lower(label) AS pipeline_id,
            label,
            session_date AS market_date,
            generated_at,
            job_run_id,
            universe_label,
            strategy,
            profile,
            greeks_source,
            symbols_json,
            failures_json,
            selection_memory_json,
            jsonb_build_object(
                'candidate_count', (
                    SELECT count(*)
                    FROM collector_cycle_candidates candidate
                    WHERE candidate.cycle_id = collector_cycles.cycle_id
                ),
                'failure_count', jsonb_array_length(failures_json)
            ) AS summary_json
        FROM collector_cycles
        ON CONFLICT (cycle_id) DO NOTHING
        """
    )

    op.execute(
        """
        UPDATE opportunities
        SET
            pipeline_id = 'pipeline:' || lower(label),
            market_date = session_date,
            cycle_id = source_cycle_id,
            root_symbol = underlying_symbol,
            style_profile = CASE
                WHEN lower(profile) = 'core' THEN 'carry'
                WHEN lower(profile) = 'swing' THEN 'swing'
                ELSE 'active'
            END,
            horizon_intent = CASE
                WHEN lower(profile) = '0dte' THEN 'same_day'
                WHEN lower(profile) IN ('weekly', 'micro') THEN 'short_dated'
                WHEN lower(profile) = 'core' THEN 'carry'
                WHEN lower(profile) = 'swing' THEN 'multi_day'
                ELSE 'short_dated'
            END,
            product_class = CASE
                WHEN upper(underlying_symbol) IN ('SPY', 'QQQ', 'IWM', 'DIA') THEN 'index_etf_options'
                ELSE 'equity_options'
            END,
            side_bias = side,
            eligibility_state = eligibility,
            promotion_score = confidence,
            execution_score = confidence,
            legs_json = COALESCE(legs_json, '[]'::jsonb),
            economics_json = COALESCE(economics_json, '{}'::jsonb),
            strategy_metrics_json = COALESCE(strategy_metrics_json, risk_hints_json),
            order_payload_json = COALESCE(order_payload_json, execution_shape_json -> 'order_payload'),
            evidence_json = COALESCE(evidence_json, candidate_json)
        WHERE pipeline_id IS NULL
        """
    )

    op.execute(
        """
        INSERT INTO portfolio_positions (
            position_id,
            pipeline_id,
            source_opportunity_id,
            legacy_session_position_id,
            open_execution_attempt_id,
            root_symbol,
            strategy_family,
            style_profile,
            horizon_intent,
            product_class,
            market_date_opened,
            market_date_closed,
            status,
            legs_json,
            economics_json,
            strategy_metrics_json,
            requested_quantity,
            opened_quantity,
            remaining_quantity,
            entry_value,
            realized_pnl,
            unrealized_pnl,
            close_mark,
            close_mark_source,
            close_marked_at,
            last_broker_status,
            exit_policy_json,
            risk_policy_json,
            source_job_type,
            source_job_key,
            source_job_run_id,
            last_exit_evaluated_at,
            last_exit_reason,
            last_reconciled_at,
            reconciliation_status,
            reconciliation_note,
            opened_at,
            closed_at,
            created_at,
            updated_at
        )
        SELECT
            session_position_id AS position_id,
            'pipeline:' || lower(label) AS pipeline_id,
            NULL,
            session_position_id AS legacy_session_position_id,
            open_execution_attempt_id,
            underlying_symbol AS root_symbol,
            strategy AS strategy_family,
            CASE
                WHEN lower(label) LIKE '%core%' THEN 'carry'
                ELSE 'active'
            END AS style_profile,
            CASE
                WHEN lower(label) LIKE '0dte%' OR lower(label) LIKE '%_0dte_%' THEN 'same_day'
                WHEN lower(label) LIKE '%weekly%' THEN 'short_dated'
                WHEN lower(label) LIKE '%core%' THEN 'carry'
                ELSE 'short_dated'
            END AS horizon_intent,
            CASE
                WHEN upper(underlying_symbol) IN ('SPY', 'QQQ', 'IWM', 'DIA') THEN 'index_etf_options'
                ELSE 'equity_options'
            END AS product_class,
            session_date AS market_date_opened,
            CASE WHEN closed_at IS NULL THEN NULL ELSE session_date END AS market_date_closed,
            status,
            jsonb_build_array(
                jsonb_build_object('symbol', short_symbol, 'role', 'short'),
                jsonb_build_object('symbol', long_symbol, 'role', 'long')
            ) AS legs_json,
            jsonb_build_object(
                'entry_credit', entry_credit,
                'entry_notional', entry_notional,
                'max_profit', max_profit,
                'max_loss', max_loss
            ) AS economics_json,
            jsonb_build_object('width', width) AS strategy_metrics_json,
            requested_quantity,
            opened_quantity,
            remaining_quantity,
            entry_credit AS entry_value,
            realized_pnl,
            unrealized_pnl,
            close_mark,
            close_mark_source,
            close_marked_at,
            last_broker_status,
            exit_policy_json,
            risk_policy_json,
            source_job_type,
            source_job_key,
            source_job_run_id,
            last_exit_evaluated_at,
            last_exit_reason,
            last_reconciled_at,
            reconciliation_status,
            reconciliation_note,
            opened_at,
            closed_at,
            created_at,
            updated_at
        FROM session_positions
        ON CONFLICT (position_id) DO NOTHING
        """
    )

    op.execute(
        """
        UPDATE execution_attempts
        SET
            pipeline_id = 'pipeline:' || lower(label),
            market_date = session_date,
            position_id = session_position_id,
            root_symbol = underlying_symbol,
            strategy_family = strategy,
            requested_quantity = quantity,
            requested_limit_price = limit_price
        WHERE pipeline_id IS NULL
        """
    )

    op.execute(
        """
        INSERT INTO position_closes (
            position_id,
            execution_attempt_id,
            legacy_session_position_id,
            closed_quantity,
            exit_value,
            realized_pnl,
            broker_order_id,
            closed_at,
            created_at,
            updated_at
        )
        SELECT
            session_position_id AS position_id,
            execution_attempt_id,
            session_position_id AS legacy_session_position_id,
            closed_quantity,
            exit_debit AS exit_value,
            realized_pnl,
            broker_order_id,
            closed_at,
            created_at,
            updated_at
        FROM session_position_closes
        ON CONFLICT (execution_attempt_id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_index("idx_opportunities_pipeline_market_lifecycle", table_name="opportunities")
    op.drop_column("opportunities", "evidence_json")
    op.drop_column("opportunities", "order_payload_json")
    op.drop_column("opportunities", "strategy_metrics_json")
    op.drop_column("opportunities", "economics_json")
    op.drop_column("opportunities", "legs_json")
    op.drop_column("opportunities", "execution_score")
    op.drop_column("opportunities", "promotion_score")
    op.drop_column("opportunities", "eligibility_state")
    op.drop_column("opportunities", "side_bias")
    op.drop_column("opportunities", "expiration_date")
    op.drop_column("opportunities", "product_class")
    op.drop_column("opportunities", "horizon_intent")
    op.drop_column("opportunities", "style_profile")
    op.drop_column("opportunities", "root_symbol")
    op.drop_column("opportunities", "cycle_id")
    op.drop_column("opportunities", "market_date")
    op.drop_column("opportunities", "pipeline_id")

    op.drop_constraint("fk_execution_attempts_position_id", "execution_attempts", type_="foreignkey")
    op.drop_index("idx_execution_attempts_runtime_position_requested", table_name="execution_attempts")
    op.drop_index("idx_execution_attempts_pipeline_requested", table_name="execution_attempts")
    op.drop_column("execution_attempts", "requested_limit_price")
    op.drop_column("execution_attempts", "requested_quantity")
    op.drop_column("execution_attempts", "product_class")
    op.drop_column("execution_attempts", "horizon_intent")
    op.drop_column("execution_attempts", "style_profile")
    op.drop_column("execution_attempts", "strategy_family")
    op.drop_column("execution_attempts", "root_symbol")
    op.drop_column("execution_attempts", "position_id")
    op.drop_column("execution_attempts", "market_date")
    op.drop_column("execution_attempts", "pipeline_id")

    op.drop_index("ux_position_closes_execution_attempt", table_name="position_closes")
    op.drop_index("idx_position_closes_position_closed", table_name="position_closes")
    op.drop_table("position_closes")

    op.drop_index("ux_portfolio_positions_open_attempt", table_name="portfolio_positions")
    op.drop_index("idx_portfolio_positions_pipeline_status", table_name="portfolio_positions")
    op.drop_index("idx_portfolio_positions_pipeline_updated", table_name="portfolio_positions")
    op.drop_table("portfolio_positions")

    op.drop_index("idx_pipeline_cycles_pipeline_market_date", table_name="pipeline_cycles")
    op.drop_index("idx_pipeline_cycles_pipeline_generated", table_name="pipeline_cycles")
    op.drop_table("pipeline_cycles")

    op.drop_index("idx_pipelines_enabled_updated", table_name="pipelines")
    op.drop_index("ux_pipelines_label", table_name="pipelines")
    op.drop_table("pipelines")
