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
from core.services.bots import build_collector_scope
from core.services.collections.config import (
    _apply_options_automation_overrides,
    build_collection_args,
    build_scanner_args,
)
from core.services.management_planner import plan_position_management
from core.services.opportunity_generation import build_runtime_opportunity_payload
from core.services.scanners.config import parse_args as parse_scanner_args
from core.services.signal_state import _build_opportunity_payload
from core.services.strategy_builders import (
    build_entry_runtime_candidates,
    build_runtime_scan_args,
)


class StrategyBuilderServiceTests(unittest.TestCase):
    def test_build_runtime_scan_args_uses_strategy_min_return_on_risk(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )
        base_args = parse_scanner_args([])

        args = build_runtime_scan_args(
            symbol=runtime.symbols[0],
            base_scanner_args=base_args,
            runtime=runtime,
        )

        self.assertEqual(args.min_return_on_risk, 0.13)

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


class CollectionConfigTests(unittest.TestCase):
    def test_build_collector_scope_includes_runtime_min_return_on_risk(self) -> None:
        scope = build_collector_scope(
            scanner_strategy="put_credit",
            scanner_profile="weekly",
        )

        self.assertEqual(scope["scanner_args"]["min_return_on_risk"], 0.13)

    def test_build_scanner_args_preserves_seeded_scanner_overrides(self) -> None:
        args = build_collection_args(
            {
                "strategy": "put_credit",
                "profile": "weekly",
                "symbols": "SPY",
                "min_dte": 5,
                "max_dte": 10,
                "min_return_on_risk": 0.13,
            }
        )

        scanner_args = build_scanner_args(args)

        self.assertEqual(scanner_args.min_dte, 5)
        self.assertEqual(scanner_args.max_dte, 10)
        self.assertEqual(scanner_args.min_return_on_risk, 0.13)

    def test_apply_options_automation_overrides_applies_scope_scanner_args(self) -> None:
        args = build_collection_args(
            {
                "strategy": "put_credit",
                "profile": "weekly",
                "options_automation_enabled": True,
            }
        )

        updated = _apply_options_automation_overrides(args)

        self.assertEqual(updated.min_return_on_risk, 0.13)
        self.assertEqual(updated.min_dte, 5)
        self.assertEqual(updated.max_dte, 10)


class OpportunityProjectionTests(unittest.TestCase):
    def test_build_runtime_opportunity_payload_preserves_candidate_blockers(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )
        candidate = {
            "underlying_symbol": runtime.symbols[0],
            "strategy": "put_credit",
            "profile": "weekly",
            "expiration_date": "2026-04-24",
            "short_symbol": "SPY260424P00700000",
            "long_symbol": "SPY260424P00698000",
            "short_strike": 700.0,
            "long_strike": 698.0,
            "width": 2.0,
            "midpoint_credit": 0.22,
            "natural_credit": 0.20,
            "max_profit": 22.0,
            "max_loss": 178.0,
            "return_on_risk": 0.1236,
            "fill_ratio": 0.91,
            "order_payload": {"limit_price": 0.22},
            "execution_blockers": ["return_on_risk_below_promotable_floor"],
            "scoring_blockers": ["calendar_risk_present"],
        }

        payload = build_runtime_opportunity_payload(
            runtime=runtime,
            label="explore_10_put_credit_weekly_auto",
            session_date="2026-04-17",
            generated_at="2026-04-17T17:25:24Z",
            cycle_id="cycle-1",
            automation_run_id="run-1",
            row={
                "selection_state": "monitor",
                "selection_rank": 1,
                "state_reason": "selected_monitor",
                "eligibility": "live",
                "candidate": candidate,
            },
            source_row=None,
        )

        self.assertEqual(
            payload["blockers"],
            ["calendar_risk_present", "return_on_risk_below_promotable_floor"],
        )

    def test_build_signal_opportunity_payload_preserves_candidate_blockers(self) -> None:
        candidate = {
            "underlying_symbol": "QQQ",
            "strategy": "put_credit",
            "profile": "weekly",
            "expiration_date": "2026-04-24",
            "short_symbol": "QQQ260424P00628000",
            "long_symbol": "QQQ260424P00625000",
            "short_strike": 628.0,
            "long_strike": 625.0,
            "width": 3.0,
            "midpoint_credit": 0.28,
            "natural_credit": 0.27,
            "max_profit": 28.0,
            "max_loss": 272.0,
            "return_on_risk": 0.1029,
            "fill_ratio": 0.96,
            "order_payload": {"limit_price": 0.28},
            "execution_blockers": ["return_on_risk_below_promotable_floor"],
        }

        payload = _build_opportunity_payload(
            label="explore_10_put_credit_weekly_auto",
            session_date="2026-04-17",
            generated_at="2026-04-17T19:00:20Z",
            cycle_id="cycle-collector",
            default_strategy="put_credit",
            default_profile="weekly",
            row={
                **candidate,
                "selection_state": "monitor",
                "selection_rank": 1,
                "state_reason": "selected_monitor",
                "origin": "live_scan",
                "eligibility": "live",
                "candidate": dict(candidate),
            },
        )

        self.assertEqual(
            payload["blockers"],
            ["return_on_risk_below_promotable_floor"],
        )


class BacktestTests(unittest.TestCase):
    def test_build_backtest_run_prefers_latest_non_empty_recent_session(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )

        class _SignalStore:
            def list_automation_runs(self, **_: object) -> list[dict[str, object]]:
                return [
                    {
                        "automation_run_id": "run-empty-2026-04-17",
                        "session_date": "2026-04-17",
                        "started_at": "2026-04-17T20:00:00Z",
                        "result": {"opportunity_count": 0},
                    },
                    {
                        "automation_run_id": "run-full-2026-04-17",
                        "session_date": "2026-04-17",
                        "started_at": "2026-04-17T19:55:00Z",
                        "result": {"opportunity_count": 1},
                    },
                    {
                        "automation_run_id": "run-full-2026-04-16",
                        "session_date": "2026-04-16",
                        "started_at": "2026-04-16T19:55:00Z",
                        "result": {"opportunity_count": 1},
                    },
                ]

            def list_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
                automation_run_id = str(kwargs.get("automation_run_id") or "")
                if automation_run_id == "run-full-2026-04-17":
                    return [
                        {
                            "opportunity_id": "opp-2026-04-17",
                            "underlying_symbol": runtime.symbols[0],
                            "strategy_family": runtime.strategy_family,
                            "short_symbol": "SPY260417P500",
                            "long_symbol": "SPY260417P498",
                            "expiration_date": "2026-04-17",
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
                if automation_run_id == "run-full-2026-04-16":
                    return [
                        {
                            "opportunity_id": "opp-2026-04-16",
                            "underlying_symbol": runtime.symbols[0],
                            "strategy_family": runtime.strategy_family,
                            "short_symbol": "SPY260416P500",
                            "long_symbol": "SPY260416P498",
                            "expiration_date": "2026-04-16",
                            "execution_score": 77.0,
                            "selection_rank": 1,
                            "economics": {
                                "midpoint_credit": 0.8,
                                "natural_credit": 0.75,
                                "fill_ratio": 0.8,
                                "max_loss": 120.0,
                            },
                            "width": 2.0,
                        }
                    ]
                return []

            def list_opportunity_decisions(
                self, **kwargs: object
            ) -> list[dict[str, object]]:
                scope_key = str(kwargs.get("scope_key") or "")
                if scope_key.endswith(":2026-04-17"):
                    return [
                        {
                            "opportunity_decision_id": "decision-2026-04-17",
                            "opportunity_id": "opp-2026-04-17",
                            "state": "selected",
                        }
                    ]
                if scope_key.endswith(":2026-04-16"):
                    return [
                        {
                            "opportunity_decision_id": "decision-2026-04-16",
                            "opportunity_id": "opp-2026-04-16",
                            "state": "selected",
                        }
                    ]
                return []

        class _ExecutionStore:
            def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
                return [{"execution_intent_id": "intent-1", "state": "submitted"}]

            def list_positions(self, **kwargs: object) -> list[dict[str, object]]:
                market_date = str(kwargs.get("market_date") or "")
                if market_date == "2026-04-17":
                    return [{"position_id": "pos-2026-04-17", "realized_pnl": 9.0}]
                if market_date == "2026-04-16":
                    return [{"position_id": "pos-2026-04-16", "realized_pnl": 4.0}]
                return []

        class _HistoryStore:
            def schema_ready(self) -> bool:
                return True

            def list_option_quote_events_window(
                self, **kwargs: object
            ) -> list[dict[str, object]]:
                captured_from = str(kwargs.get("captured_from") or "")
                if captured_from.startswith("2026-04-17"):
                    return [
                        {
                            "option_symbol": "SPY260417P500",
                            "bid": 0.95,
                            "ask": 1.00,
                            "midpoint": 0.975,
                            "captured_at": "2026-04-17T19:56:00Z",
                            "source": "test_quote",
                        },
                        {
                            "option_symbol": "SPY260417P498",
                            "bid": 0.55,
                            "ask": 0.60,
                            "midpoint": 0.575,
                            "captured_at": "2026-04-17T19:56:00Z",
                            "source": "test_quote",
                        },
                    ]
                if captured_from.startswith("2026-04-16"):
                    return [
                        {
                            "option_symbol": "SPY260416P500",
                            "bid": 0.75,
                            "ask": 0.80,
                            "midpoint": 0.775,
                            "captured_at": "2026-04-16T19:56:00Z",
                            "source": "test_quote",
                        },
                        {
                            "option_symbol": "SPY260416P498",
                            "bid": 0.45,
                            "ask": 0.50,
                            "midpoint": 0.475,
                            "captured_at": "2026-04-16T19:56:00Z",
                            "source": "test_quote",
                        },
                    ]
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
                limit=1,
                storage=_Storage(),
            )

        self.assertEqual(run.aggregate.session_count, 1)
        self.assertEqual(run.sessions[0].session_date, "2026-04-17")
        self.assertEqual(run.sessions[0].automation_run_id, "run-full-2026-04-17")
        self.assertEqual(
            run.sessions[0].modeled_selected_opportunity_id,
            "opp-2026-04-17",
        )
        self.assertEqual(
            run.sessions[0].actual_selected_opportunity_id,
            "opp-2026-04-17",
        )
        self.assertEqual(run.aggregate.matched_selection_count, 1)
        self.assertEqual(run.aggregate.modeled_fill_count, 1)

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
