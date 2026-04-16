from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import unittest
from unittest.mock import patch

from core.services.automation_runtime import (
    resolve_entry_runtime,
    resolve_management_runtime,
)
from core.services.bootstrap_backtest import build_bootstrap_backtest
from core.services.management_planner import plan_position_management
from core.services.scanner import parse_args as parse_scanner_args
from core.services.strategy_builders import build_entry_runtime_candidates


class StrategyBuilderServiceTests(unittest.TestCase):
    def test_build_entry_runtime_candidates_filters_to_exact_widths(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )
        runtime = replace(
            runtime,
            automation=replace(runtime.automation, symbols=(runtime.symbols[0],)),
        )
        base_args = parse_scanner_args([])
        fake_market_slice = object()
        with (
            patch(
                "core.services.strategy_builders.build_symbol_market_slice",
                return_value=fake_market_slice,
            ) as build_slice,
            patch(
                "core.services.strategy_builders.build_candidates_from_market_slice",
                return_value=([object(), object()], None),
            ),
            patch(
                "core.services.strategy_builders._serialize_candidate",
                side_effect=[
                    {"underlying_symbol": runtime.symbols[0], "width": 2.0},
                    {"underlying_symbol": runtime.symbols[0], "width": 99.0},
                ],
            ),
        ):
            rows = build_entry_runtime_candidates(
                entry_runtimes=[runtime],
                base_scanner_args=base_args,
                client=object(),
                calendar_resolver=object(),
                greeks_provider=object(),
                per_runtime_limit=5,
            )

        owner_key = (runtime.bot_id, runtime.automation_id)
        self.assertEqual(build_slice.call_count, 1)
        self.assertEqual(len(rows[owner_key][runtime.symbols[0]]), 1)
        self.assertEqual(rows[owner_key][runtime.symbols[0]][0]["width"], 2.0)


class ManagementPlannerTests(unittest.TestCase):
    def test_plan_position_management_uses_management_recipe_refs(self) -> None:
        runtime = resolve_management_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_manage",
        )
        decision = plan_position_management(
            runtime=runtime,
            position={
                "position_id": "pos-1",
                "session_date": "2026-04-16",
                "entry_credit": 1.0,
                "close_mark": 0.45,
                "close_marked_at": "2026-04-16T13:59:00Z",
                "remaining_quantity": 1,
                "strategy_family": runtime.strategy_family,
            },
            flatten_due=False,
            now=datetime(2026, 4, 16, 14, 0, tzinfo=UTC),
        )

        self.assertTrue(decision["should_close"])
        self.assertEqual(decision["reason"], "profit_target")
        self.assertEqual(decision["recipe_ref"], "take_profit_50pct")


class BootstrapBacktestTests(unittest.TestCase):
    def test_build_bootstrap_backtest_summarizes_scoped_runtime_rows(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )

        class _SignalStore:
            def list_automation_runs(self, **_: object) -> list[dict[str, object]]:
                return [
                    {
                        "automation_run_id": "run-1",
                        "session_date": "2026-04-16",
                        "started_at": "2026-04-16T14:35:00Z",
                    }
                ]

            def list_opportunities(self, **_: object) -> list[dict[str, object]]:
                return [
                    {
                        "opportunity_id": "opp-1",
                        "underlying_symbol": runtime.symbols[0],
                        "strategy_family": runtime.strategy_family,
                        "execution_score": 88.0,
                        "selection_rank": 1,
                    }
                ]

            def list_opportunity_decisions(
                self, **_: object
            ) -> list[dict[str, object]]:
                return [
                    {
                        "opportunity_decision_id": "decision-1",
                        "opportunity_id": "opp-1",
                        "state": "selected",
                    }
                ]

        class _ExecutionStore:
            def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
                return [{"execution_intent_id": "intent-1"}]

            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return [
                    {
                        "position_id": "pos-1",
                        "realized_pnl": 12.5,
                        "unrealized_pnl": 0.0,
                    }
                ]

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()

        with patch(
            "core.services.bootstrap_backtest.evaluate_entry_controls",
            return_value=(True, None, {"open_position_count": 0}),
        ):
            payload = build_bootstrap_backtest(
                db_target="postgresql://example",
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=5,
                storage=_Storage(),
            )

        self.assertEqual(payload["aggregate"]["session_count"], 1)
        self.assertEqual(payload["aggregate"]["matched_selection_count"], 1)
        self.assertEqual(payload["aggregate"]["realized_pnl"], 12.5)
        self.assertEqual(
            payload["sessions"][0]["actual_selected_opportunity_id"], "opp-1"
        )


if __name__ == "__main__":
    unittest.main()
