"""add strategy ledger read indexes

Revision ID: 20260611_0058
Revises: 20260608_0057
Create Date: 2026-06-11 20:25:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "20260611_0058"
down_revision = "20260608_0057"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "idx_trade_candidates_strategy_observed",
        "trade_candidates",
        ["trading_strategy_id", "observed_at"],
    )
    op.create_index(
        "idx_trade_signals_strategy_session_observed",
        "trade_signals",
        ["trading_strategy_id", "session_date", "observed_at"],
    )
    op.create_index(
        "idx_trade_admissions_session_decided",
        "trade_admissions",
        ["session_date", "decided_at"],
    )
    op.create_index(
        "idx_execution_attempts_strategy_market_requested",
        "execution_attempts",
        ["trading_strategy_id", "market_date", "requested_at"],
    )
    op.create_index(
        "idx_portfolio_positions_strategy_opened",
        "portfolio_positions",
        ["trading_strategy_id", "market_date_opened", "updated_at"],
    )
    op.create_index(
        "idx_portfolio_positions_strategy_closed",
        "portfolio_positions",
        ["trading_strategy_id", "market_date_closed", "updated_at"],
    )
    op.create_index(
        "idx_position_closes_closed_position",
        "position_closes",
        ["closed_at", "position_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_position_closes_closed_position", table_name="position_closes")
    op.drop_index("idx_portfolio_positions_strategy_closed", table_name="portfolio_positions")
    op.drop_index("idx_portfolio_positions_strategy_opened", table_name="portfolio_positions")
    op.drop_index("idx_execution_attempts_strategy_market_requested", table_name="execution_attempts")
    op.drop_index("idx_trade_admissions_session_decided", table_name="trade_admissions")
    op.drop_index("idx_trade_signals_strategy_session_observed", table_name="trade_signals")
    op.drop_index("idx_trade_candidates_strategy_observed", table_name="trade_candidates")
