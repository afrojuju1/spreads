"""replace active runtime strategy ownership columns

Revision ID: 20260603_0044
Revises: 20260603_0043
Create Date: 2026-06-03 16:55:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "20260603_0044"
down_revision = "20260603_0043"
branch_labels = None
depends_on = None


def _drop_index(index_name: str, table_name: str) -> None:
    op.execute(f"DROP INDEX IF EXISTS {index_name}")


def _rename_table_if_needed(old_name: str, new_name: str) -> None:
    op.execute(f"""
        DO $$
        BEGIN
            IF to_regclass('{old_name}') IS NOT NULL
               AND to_regclass('{new_name}') IS NULL THEN
                ALTER TABLE {old_name} RENAME TO {new_name};
            END IF;
        END
        $$;
        """)


def _rename_column_if_needed(table_name: str, old_name: str, new_name: str) -> None:
    op.execute(f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = '{table_name}' AND column_name = '{old_name}'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = '{table_name}' AND column_name = '{new_name}'
            ) THEN
                ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name};
            END IF;
        END
        $$;
        """)


def _create_strategy_run_fk_if_needed() -> None:
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables WHERE table_name = 'opportunities'
            )
            AND EXISTS (
                SELECT 1 FROM information_schema.tables WHERE table_name = 'strategy_runs'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'opportunities' AND column_name = 'strategy_run_id'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'opportunities_strategy_run_id_fkey'
            ) THEN
                ALTER TABLE opportunities
                ADD CONSTRAINT opportunities_strategy_run_id_fkey
                FOREIGN KEY (strategy_run_id)
                REFERENCES strategy_runs(strategy_run_id)
                ON DELETE SET NULL;
            END IF;
        END
        $$;
        """)


def upgrade() -> None:
    op.execute("ALTER TABLE automation_runs ADD COLUMN IF NOT EXISTS trading_strategy_id text")
    op.execute("""
        UPDATE automation_runs
        SET trading_strategy_id = COALESCE(
            NULLIF(strategy_config_id, ''),
            NULLIF(automation_id, ''),
            NULLIF(bot_id, '')
        )
        WHERE trading_strategy_id IS NULL OR trading_strategy_id = ''
        """)
    op.execute("ALTER TABLE automation_runs ALTER COLUMN trading_strategy_id SET NOT NULL")
    _drop_index("idx_automation_runs_bot_automation_started", "automation_runs")
    _drop_index("idx_automation_runs_cycle_automation", "automation_runs")
    _drop_index("idx_automation_runs_strategy_started", "automation_runs")
    _drop_index("idx_automation_runs_session_started", "automation_runs")
    _drop_index("idx_automation_runs_cycle_strategy", "automation_runs")
    op.execute("ALTER TABLE automation_runs DROP COLUMN IF EXISTS strategy_config_id")
    op.execute("ALTER TABLE automation_runs DROP COLUMN IF EXISTS automation_id")
    op.execute("ALTER TABLE automation_runs DROP COLUMN IF EXISTS bot_id")
    _rename_table_if_needed("automation_runs", "strategy_runs")
    _rename_column_if_needed("strategy_runs", "automation_run_id", "strategy_run_id")
    op.execute("CREATE INDEX IF NOT EXISTS idx_strategy_runs_strategy_started " "ON strategy_runs (trading_strategy_id, started_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_strategy_runs_session_started " "ON strategy_runs (session_date, started_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_strategy_runs_cycle_strategy " "ON strategy_runs (cycle_id, trading_strategy_id)")

    op.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS trading_strategy_id text")
    op.execute("""
        UPDATE opportunities
        SET trading_strategy_id = COALESCE(
            NULLIF(strategy_config_id, ''),
            NULLIF(strategy_id, ''),
            NULLIF(automation_id, ''),
            NULLIF(bot_id, '')
        )
        WHERE trading_strategy_id IS NULL OR trading_strategy_id = ''
        """)
    _drop_index("idx_opportunities_bot_automation_session", "opportunities")
    _drop_index("idx_opportunities_automation_run", "opportunities")
    _drop_index("idx_opportunities_strategy_run", "opportunities")
    op.execute("ALTER TABLE opportunities DROP CONSTRAINT IF EXISTS opportunities_automation_run_id_fkey")
    _rename_column_if_needed("opportunities", "automation_run_id", "strategy_run_id")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS strategy_id")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS strategy_config_id")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS automation_id")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS bot_id")
    op.execute("CREATE INDEX IF NOT EXISTS idx_opportunities_strategy_session " "ON opportunities (trading_strategy_id, session_date, updated_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_opportunities_strategy_run " "ON opportunities (strategy_run_id)")
    _create_strategy_run_fk_if_needed()

    op.execute("ALTER TABLE opportunity_decisions ADD COLUMN IF NOT EXISTS trading_strategy_id text")
    op.execute("""
        UPDATE opportunity_decisions
        SET trading_strategy_id = COALESCE(
            NULLIF(policy_ref_json ->> 'strategy_config_id', ''),
            NULLIF(policy_ref_json ->> 'strategy_id', ''),
            NULLIF(automation_id, ''),
            NULLIF(bot_id, '')
        )
        WHERE trading_strategy_id IS NULL OR trading_strategy_id = ''
        """)
    op.execute("ALTER TABLE opportunity_decisions ALTER COLUMN trading_strategy_id SET NOT NULL")
    _drop_index("idx_opportunity_decisions_bot_automation_decided", "opportunity_decisions")
    op.execute("ALTER TABLE opportunity_decisions DROP COLUMN IF EXISTS automation_id")
    op.execute("ALTER TABLE opportunity_decisions DROP COLUMN IF EXISTS bot_id")
    op.execute("CREATE INDEX IF NOT EXISTS idx_opportunity_decisions_strategy_decided " "ON opportunity_decisions (trading_strategy_id, decided_at)")

    op.execute("ALTER TABLE execution_intents ADD COLUMN IF NOT EXISTS trading_strategy_id text")
    op.execute("""
        UPDATE execution_intents
        SET trading_strategy_id = COALESCE(
            NULLIF(policy_ref_json ->> 'strategy_config_id', ''),
            NULLIF(policy_ref_json ->> 'strategy_id', ''),
            NULLIF(automation_id, ''),
            NULLIF(bot_id, '')
        )
        WHERE trading_strategy_id IS NULL OR trading_strategy_id = ''
        """)
    op.execute("ALTER TABLE execution_intents ALTER COLUMN trading_strategy_id SET NOT NULL")
    _drop_index("idx_execution_intents_bot_created", "execution_intents")
    op.execute("ALTER TABLE execution_intents DROP COLUMN IF EXISTS automation_id")
    op.execute("ALTER TABLE execution_intents DROP COLUMN IF EXISTS bot_id")
    op.execute("CREATE INDEX IF NOT EXISTS idx_execution_intents_strategy_created " "ON execution_intents (trading_strategy_id, created_at)")

    op.execute("ALTER TABLE execution_attempts ADD COLUMN IF NOT EXISTS trading_strategy_id text")
    op.execute("""
        UPDATE execution_attempts
        SET trading_strategy_id = COALESCE(
            NULLIF(strategy_config_id, ''),
            NULLIF(request_json ->> 'trading_strategy_id', ''),
            NULLIF(request_json ->> 'strategy_config_id', ''),
            NULLIF(automation_id, ''),
            NULLIF(bot_id, '')
        )
        WHERE trading_strategy_id IS NULL OR trading_strategy_id = ''
        """)
    _drop_index("idx_execution_attempts_bot_automation_requested", "execution_attempts")
    op.execute("ALTER TABLE execution_attempts DROP COLUMN IF EXISTS strategy_config_id")
    op.execute("ALTER TABLE execution_attempts DROP COLUMN IF EXISTS automation_id")
    op.execute("ALTER TABLE execution_attempts DROP COLUMN IF EXISTS bot_id")
    op.execute("CREATE INDEX IF NOT EXISTS idx_execution_attempts_strategy_requested " "ON execution_attempts (trading_strategy_id, requested_at)")

    op.execute("ALTER TABLE portfolio_positions ADD COLUMN IF NOT EXISTS trading_strategy_id text")
    op.execute("""
        UPDATE portfolio_positions
        SET trading_strategy_id = COALESCE(
            NULLIF(strategy_config_id, ''),
            NULLIF(strategy_id, ''),
            NULLIF(automation_id, ''),
            NULLIF(bot_id, '')
        )
        WHERE trading_strategy_id IS NULL OR trading_strategy_id = ''
        """)
    _drop_index("idx_portfolio_positions_bot_status", "portfolio_positions")
    _drop_index("idx_portfolio_positions_strategy_config_status", "portfolio_positions")
    op.execute("ALTER TABLE portfolio_positions DROP COLUMN IF EXISTS strategy_id")
    op.execute("ALTER TABLE portfolio_positions DROP COLUMN IF EXISTS strategy_config_id")
    op.execute("ALTER TABLE portfolio_positions DROP COLUMN IF EXISTS automation_id")
    op.execute("ALTER TABLE portfolio_positions DROP COLUMN IF EXISTS bot_id")
    op.execute("CREATE INDEX IF NOT EXISTS idx_portfolio_positions_strategy_status " "ON portfolio_positions (trading_strategy_id, status)")


def downgrade() -> None:
    raise NotImplementedError("Downgrading to the removed bot/automation ownership schema is not supported.")
