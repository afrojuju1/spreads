from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import unittest
from unittest.mock import patch

from core.backtest import build_backtest_run, compare_backtest_runs
from core.domain.backtest_models import BacktestAggregate, BacktestRun, BacktestTarget
from core.services.automation_runtime import (
    resolve_entry_runtime,
    resolve_management_runtime,
)
from core.services.management_planner import plan_position_management
from core.services.scanners.config import parse_args as parse_scanner_args
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


class BacktestTests(unittest.TestCase):
    def test_build_backtest_run_summarizes_scoped_runtime_rows(self) -> None:
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
                        "short_symbol": "SPY240416P500",
                        "long_symbol": "SPY240416P498",
                        "expiration_date": "2026-04-16",
                        "execution_score": 88.0,
                        "selection_rank": 1,
                        "economics": {
                            "midpoint_credit": 1.0,
                            "natural_credit": 0.95,
                            "fill_ratio": 0.8,
                            "max_loss": 100.0,
                        },
                        "width": 2.0,
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
                return [{"execution_intent_id": "intent-1", "state": "submitted"}]

            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return [
                    {
                        "position_id": "pos-1",
                        "realized_pnl": 12.5,
                        "unrealized_pnl": 0.0,
                    }
                ]

        class _HistoryStore:
            def schema_ready(self) -> bool:
                return True

            def list_option_quote_events_window(
                self, **_: object
            ) -> list[dict[str, object]]:
                return [
                    {
                        "option_symbol": "SPY240416P500",
                        "bid": 0.95,
                        "ask": 1.00,
                        "midpoint": 0.975,
                        "captured_at": "2026-04-16T14:40:00Z",
                        "source": "test_quote",
                    },
                    {
                        "option_symbol": "SPY240416P498",
                        "bid": 0.55,
                        "ask": 0.60,
                        "midpoint": 0.575,
                        "captured_at": "2026-04-16T14:40:00Z",
                        "source": "test_quote",
                    },
                ]

            def list_option_trade_events_window(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()
                self.history = _HistoryStore()

        with patch(
            "core.backtest.service.evaluate_entry_controls",
            return_value=(True, None, {"open_position_count": 0}),
        ):
            run = build_backtest_run(
                db_target="postgresql://example",
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=5,
                storage=_Storage(),
            )

        self.assertEqual(run.kind, "run")
        self.assertEqual(run.engine_name, "backtest")
        self.assertEqual(run.aggregate.session_count, 1)
        self.assertEqual(run.aggregate.fidelity, "high")
        self.assertEqual(run.aggregate.matched_selection_count, 1)
        self.assertEqual(run.aggregate.modeled_fill_count, 1)
        self.assertEqual(run.aggregate.modeled_closed_count, 1)
        self.assertEqual(run.aggregate.modeled_realized_pnl, 55.0)
        self.assertEqual(run.aggregate.realized_pnl, 12.5)
        self.assertEqual(run.sessions[0].actual_selected_opportunity_id, "opp-1")
        self.assertEqual(run.sessions[0].fidelity, "high")
        self.assertEqual(run.sessions[0].modeled_fill_state, "filled")
        self.assertEqual(run.sessions[0].modeled_exit_state, "closed")
        self.assertEqual(run.sessions[0].modeled_exit_reason, "profit_target")

    def test_compare_backtest_runs_reports_metric_deltas(self) -> None:
        comparison = compare_backtest_runs(
            left_run=BacktestRun(
                id="left-run",
                kind="run",
                status="completed",
                engine_name="backtest",
                engine_version="v1",
                created_at=datetime(2026, 4, 16, 15, 0, tzinfo=UTC),
                started_at=datetime(2026, 4, 16, 15, 0, tzinfo=UTC),
                completed_at=datetime(2026, 4, 16, 15, 1, tzinfo=UTC),
                target=BacktestTarget(automation_id="left"),
                aggregate=BacktestAggregate(
                    session_count=3,
                    fidelity="high",
                    realized_pnl=12.5,
                ),
            ),
            right_run=BacktestRun(
                id="right-run",
                kind="run",
                status="completed",
                engine_name="backtest",
                engine_version="v1",
                created_at=datetime(2026, 4, 16, 15, 0, tzinfo=UTC),
                started_at=datetime(2026, 4, 16, 15, 0, tzinfo=UTC),
                completed_at=datetime(2026, 4, 16, 15, 1, tzinfo=UTC),
                target=BacktestTarget(automation_id="right"),
                aggregate=BacktestAggregate(
                    session_count=2,
                    fidelity="medium",
                    realized_pnl=7.5,
                ),
            ),
        )

        self.assertEqual(comparison.kind, "compare")
        self.assertEqual(comparison.comparison_metrics["session_count"]["delta"], 1.0)
        self.assertEqual(comparison.comparison_metrics["realized_pnl"]["delta"], 5.0)
        self.assertEqual(comparison.comparison_metrics["fidelity"]["left"], "high")
        self.assertEqual(comparison.comparison_metrics["fidelity"]["right"], "medium")

    def test_pre_feb_2024_without_recorded_data_is_unsupported(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )

        class _SignalStore:
            def list_automation_runs(self, **_: object) -> list[dict[str, object]]:
                return [
                    {
                        "automation_run_id": "run-1",
                        "session_date": "2024-01-31",
                        "started_at": "2024-01-31T14:35:00Z",
                    }
                ]

            def list_opportunities(self, **_: object) -> list[dict[str, object]]:
                return [
                    {
                        "opportunity_id": "opp-1",
                        "underlying_symbol": runtime.symbols[0],
                        "strategy_family": runtime.strategy_family,
                        "short_symbol": "SPY240131P500",
                        "long_symbol": "SPY240131P498",
                        "expiration_date": "2024-01-31",
                        "execution_score": 88.0,
                        "selection_rank": 1,
                        "economics": {
                            "midpoint_credit": 1.0,
                            "natural_credit": 0.95,
                            "fill_ratio": 0.8,
                            "max_loss": 100.0,
                        },
                        "width": 2.0,
                    }
                ]

            def list_opportunity_decisions(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

        class _ExecutionStore:
            def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

        class _HistoryStore:
            def schema_ready(self) -> bool:
                return True

            def list_option_quote_events_window(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

            def list_option_trade_events_window(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()
                self.history = _HistoryStore()

        with patch(
            "core.backtest.service.evaluate_entry_controls",
            return_value=(True, None, {"open_position_count": 0}),
        ):
            run = build_backtest_run(
                db_target="postgresql://example",
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=5,
                storage=_Storage(),
            )

        self.assertEqual(run.aggregate.fidelity, "unsupported")
        self.assertEqual(run.sessions[0].fidelity, "unsupported")
        self.assertEqual(
            run.sessions[0].fidelity_reason,
            "pre_2024_02_01_requires_recorded_repo_data",
        )

    def test_alpaca_fallback_marks_session_medium_fidelity(self) -> None:
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
                        "short_symbol": "SPY260416P500",
                        "long_symbol": "SPY260416P498",
                        "expiration_date": "2026-04-16",
                        "execution_score": 88.0,
                        "selection_rank": 1,
                        "economics": {
                            "midpoint_credit": 1.0,
                            "natural_credit": 0.95,
                            "fill_ratio": 0.8,
                            "max_loss": 100.0,
                        },
                        "width": 2.0,
                    }
                ]

            def list_opportunity_decisions(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

        class _ExecutionStore:
            def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

        class _HistoryStore:
            def schema_ready(self) -> bool:
                return True

            def list_option_quote_events_window(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

            def list_option_trade_events_window(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()
                self.history = _HistoryStore()

        with (
            patch(
                "core.backtest.service.evaluate_entry_controls",
                return_value=(True, None, {"open_position_count": 0}),
            ),
            patch("core.backtest.service._build_alpaca_client", return_value=object()),
            patch(
                "core.backtest.service._alpaca_daily_marks",
                return_value=(
                    [
                        {
                            "captured_at": "2026-04-16T20:00:00Z",
                            "close_mark": 0.45,
                            "source": "alpaca_bars",
                        }
                    ],
                    "alpaca_bars",
                ),
            ),
        ):
            run = build_backtest_run(
                db_target="postgresql://example",
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=5,
                storage=_Storage(),
            )

        self.assertEqual(run.aggregate.fidelity, "medium")
        self.assertEqual(run.sessions[0].fidelity, "medium")
        self.assertEqual(run.sessions[0].modeled_mark_source, "alpaca_bars")

    def test_synthetic_fallback_marks_session_reduced_fidelity(self) -> None:
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
                        "short_symbol": "SPY260416P500",
                        "long_symbol": "SPY260416P498",
                        "expiration_date": "2026-04-16",
                        "execution_score": 88.0,
                        "selection_rank": 1,
                        "economics": {
                            "midpoint_credit": 1.0,
                            "natural_credit": 0.95,
                            "fill_ratio": 0.8,
                            "max_loss": 100.0,
                        },
                        "width": 2.0,
                    }
                ]

            def list_opportunity_decisions(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

        class _ExecutionStore:
            def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

        class _HistoryStore:
            def schema_ready(self) -> bool:
                return True

            def list_option_quote_events_window(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

            def list_option_trade_events_window(
                self, **_: object
            ) -> list[dict[str, object]]:
                return []

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()
                self.history = _HistoryStore()

        with (
            patch(
                "core.backtest.service.evaluate_entry_controls",
                return_value=(True, None, {"open_position_count": 0}),
            ),
            patch("core.backtest.service._build_alpaca_client", return_value=None),
        ):
            run = build_backtest_run(
                db_target="postgresql://example",
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                limit=5,
                storage=_Storage(),
            )

        self.assertEqual(run.aggregate.fidelity, "reduced")
        self.assertEqual(run.sessions[0].fidelity, "reduced")
        self.assertEqual(run.sessions[0].modeled_mark_source, "synthetic_midpoint")


if __name__ == "__main__":
    unittest.main()
