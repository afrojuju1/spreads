from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from core.backtest import build_backtest_run, compare_backtest_runs
from core.domain.backtest_models import BacktestAggregate, BacktestRun, BacktestTarget
from core.domain.models import SymbolScanResult
from core.services.automation_runtimes import (
    get_automation_runtime_detail,
    list_automation_runtimes,
)
from core.services.automation_runtime import (
    resolve_entry_runtime,
    resolve_management_runtime,
)
from core.services.bots import build_discovery_run_scope
from core.services.discovery_runs.scanning import (
    build_raw_candidate_summary,
    build_symbol_strategy_candidates,
)
from core.services.discovery_runs.config import (
    _apply_options_automation_overrides,
    build_collection_args,
    build_scanner_args,
)
from core.services.discovery_runs.schedule import build_collection_schedule_summary
from core.services.management_planner import plan_position_management
from core.services.opportunities import list_opportunities
from core.services.opportunity_generation import build_runtime_opportunity_payload
from core.services.ops.jobs import build_jobs_overview
from core.services.positions import list_positions
from core.services.ranking_policy import evaluate_candidate_ranking_policy
from core.services.replay_filters import candidate_matches_filter
from core.services.scanners.config import (
    parse_args as parse_scanner_args,
    resolve_symbol_scan_args,
    resolve_ranking_builder_params,
)
from core.services.scanners.replay_artifacts import (
    deserialize_symbol_args,
    serialize_symbol_args,
)
from core.services.signal_state import _build_opportunity_payload
from core.services.runtime_candidate_filters import (
    build_runtime_candidate_filter,
    match_runtime_candidate,
)
from core.services.strategy_builders import (
    build_runtime_scan_args,
)


class StrategyBuilderServiceTests(unittest.TestCase):
    def test_short_put_etf_runtime_uses_focus_universe(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_etf_short_put_bot",
            automation_id="etf_short_put_entry",
        )

        self.assertEqual(runtime.strategy_family, "short_put")
        self.assertEqual(runtime.build_settings.short_delta_target, 0.12)
        self.assertEqual(runtime.build_settings.min_return_on_risk, 0.05)
        self.assertEqual(len(runtime.symbols), 12)
        self.assertEqual(runtime.symbols[:4], ("SPY", "QQQ", "IWM", "DIA"))

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

    def test_build_runtime_scan_args_uses_strategy_data_quality_overrides(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_iron_condor_bot",
            automation_id="index_iron_condor_entry",
        )
        base_args = parse_scanner_args([])

        args = build_runtime_scan_args(
            symbol=runtime.symbols[0],
            base_scanner_args=base_args,
            runtime=runtime,
        )

        self.assertEqual(args.min_short_vs_expected_move_ratio, -0.30)
        self.assertEqual(args.min_breakeven_vs_expected_move_ratio, -0.35)

    def test_runtime_candidate_filter_matches_runtime_gate_for_iron_condors(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_iron_condor_bot",
            automation_id="index_iron_condor_entry",
        )
        filter_payload = build_runtime_candidate_filter(runtime)
        passing_candidate = {
            "underlying_symbol": runtime.symbols[0],
            "strategy": "iron_condor",
            "days_to_expiration": runtime.build_settings.dte_min + 1,
            "short_delta": runtime.build_settings.short_delta_target,
            "width": runtime.build_settings.width_points[0],
            "short_open_interest": runtime.build_settings.min_open_interest + 100,
            "long_open_interest": runtime.build_settings.min_open_interest + 100,
            "short_relative_spread": 0.05,
            "long_relative_spread": 0.05,
            "return_on_risk": runtime.build_settings.min_return_on_risk + 0.05,
            "setup_status": "neutral",
            "side_balance_score": 0.55,
            "wing_symmetry_ratio": 1.0,
            "probability_of_profit": 0.80,
            "expected_value_dollars": 30.0,
            "slippage_adjusted_expected_value_dollars": 24.0,
            "entry_slippage_dollars": 8.0,
            "model_implied_volatility": 0.24,
        }
        failing_candidate = {
            **passing_candidate,
            "setup_status": "unfavorable",
        }

        runtime_match, runtime_reasons = match_runtime_candidate(
            passing_candidate, runtime
        )
        self.assertTrue(runtime_match)
        self.assertEqual(runtime_reasons, [])
        self.assertTrue(candidate_matches_filter(passing_candidate, filter_payload))
        self.assertIn("entry_recipe_refs", filter_payload)
        self.assertEqual(
            filter_payload["allowed_widths"],
            [float(value) for value in runtime.build_settings.width_points],
        )

        runtime_match, runtime_reasons = match_runtime_candidate(
            failing_candidate, runtime
        )
        self.assertFalse(runtime_match)
        self.assertIn("neutral_range_setup_unusable", runtime_reasons)
        self.assertFalse(candidate_matches_filter(failing_candidate, filter_payload))


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
    def test_replay_artifact_symbol_args_preserve_evaluation_context(self) -> None:
        args = parse_scanner_args(
            [
                "--symbol",
                "SPY",
                "--strategy",
                "iron_condor",
                "--profile",
                "weekly",
            ]
        )
        args.session_label = "test_session"
        args.evaluation_date = "2026-04-21"
        args.evaluation_timestamp = "2026-04-21T18:55:00+00:00"
        args.session_bucket_override = "midday"

        restored = deserialize_symbol_args(serialize_symbol_args(args))

        self.assertEqual(restored.session_label, "test_session")
        self.assertEqual(restored.evaluation_date, "2026-04-21")
        self.assertEqual(
            restored.evaluation_timestamp,
            "2026-04-21T18:55:00+00:00",
        )
        self.assertEqual(restored.session_bucket_override, "midday")

    def test_parse_scanner_args_allows_zero_width_single_leg_strategies(self) -> None:
        for strategy in ("long_call", "long_put", "short_call", "short_put"):
            with self.subTest(strategy=strategy):
                base_args = parse_scanner_args(
                    [
                        "--symbol",
                        "SPY",
                        "--strategy",
                        strategy,
                        "--profile",
                        "weekly",
                    ]
                )
                args, underlying_type = resolve_symbol_scan_args(
                    symbol="SPY",
                    base_args=base_args,
                )
                self.assertEqual(underlying_type, "etf_index_proxy")
                self.assertEqual(args.min_width, 0.0)
                self.assertEqual(args.max_width, 0.0)

    def test_weekly_short_call_uses_strategy_profile_override(self) -> None:
        short_call_args = parse_scanner_args(
            [
                "--symbol",
                "SPY",
                "--strategy",
                "short_call",
                "--profile",
                "weekly",
            ]
        )
        short_call, underlying_type = resolve_symbol_scan_args(
            symbol="SPY",
            base_args=short_call_args,
        )

        self.assertEqual(underlying_type, "etf_index_proxy")
        self.assertEqual(short_call.short_delta_min, 0.14)
        self.assertEqual(short_call.short_delta_max, 0.22)
        self.assertEqual(short_call.short_delta_target, 0.19)
        self.assertEqual(short_call.min_short_vs_expected_move_ratio, -0.10)
        self.assertEqual(short_call.min_breakeven_vs_expected_move_ratio, -0.05)

        short_put_args = parse_scanner_args(
            [
                "--symbol",
                "SPY",
                "--strategy",
                "short_put",
                "--profile",
                "weekly",
            ]
        )
        short_put, _ = resolve_symbol_scan_args(
            symbol="SPY",
            base_args=short_put_args,
        )

        self.assertEqual(short_put.short_delta_min, 0.08)
        self.assertEqual(short_put.short_delta_max, 0.16)
        self.assertEqual(short_put.short_delta_target, 0.12)
        self.assertEqual(short_put.min_short_vs_expected_move_ratio, -0.05)
        self.assertEqual(short_put.min_breakeven_vs_expected_move_ratio, -0.02)

    def test_build_symbol_strategy_candidates_carries_short_delta_target(self) -> None:
        scan_result = SymbolScanResult(
            symbol="SPY",
            underlying_type="etf_index_proxy",
            spot_price=500.0,
            args=parse_scanner_args(
                [
                    "--symbol",
                    "SPY",
                    "--strategy",
                    "put_credit",
                    "--short-delta-target",
                    "0.23",
                ]
            ),
            setup=None,
            candidates=[object()],
            run_id="run-1",
        )

        with patch(
            "core.services.discovery_runs.scanning.serialize_candidate",
            return_value={
                "underlying_symbol": "SPY",
                "strategy": "put_credit",
                "quality_score": 70.0,
            },
        ) as serialize_candidate:
            grouped = build_symbol_strategy_candidates(
                [scan_result],
                {("SPY", "put_credit"): "run-1"},
            )

        self.assertAlmostEqual(
            serialize_candidate.call_args.kwargs["short_delta_target"],
            0.23,
        )
        self.assertEqual(grouped["SPY"][0]["underlying_symbol"], "SPY")

    def test_resolve_ranking_builder_params_prefers_strategy_config_for_live_families(
        self,
    ) -> None:
        cases = {
            "call_credit": (0.60, 10.0, 8.0, 12.0, 0.18, 0.42),
            "put_credit": (0.60, 10.0, 8.0, 12.0, 0.18, 0.42),
            "iron_condor": (0.64, 12.0, 10.0, 16.0, 0.20, 0.48),
        }

        for strategy, expected in cases.items():
            source, params = resolve_ranking_builder_params(
                profile_name="weekly",
                strategy_family=strategy,
            )

            self.assertEqual(source, "strategy_config")
            self.assertEqual(
                (
                    params["ranking_min_probability_of_profit"],
                    params["ranking_min_expected_value_dollars"],
                    params["ranking_min_slippage_adjusted_expected_value_dollars"],
                    params["ranking_max_entry_slippage_dollars"],
                    params["ranking_min_model_implied_volatility"],
                    params["ranking_weight_probability_of_profit"],
                ),
                expected,
            )

    def test_resolve_ranking_builder_params_uses_explicit_profile_fallback_for_legacy_families(
        self,
    ) -> None:
        source, params = resolve_ranking_builder_params(
            profile_name="weekly",
            strategy_family="call_debit",
        )

        self.assertEqual(source, "profile_fallback")
        self.assertEqual(params["ranking_min_probability_of_profit"], 0.40)
        self.assertEqual(params["ranking_weight_probability_of_profit"], 0.28)

    def test_build_raw_candidate_summary_includes_ranking_vectors_and_blocked_exemplars(
        self,
    ) -> None:
        args = parse_scanner_args(
            [
                "--symbol",
                "QQQ",
                "--strategy",
                "call_credit",
                "--profile",
                "weekly",
            ]
        )
        scan_result = SymbolScanResult(
            symbol="QQQ",
            underlying_type="etf_index_proxy",
            spot_price=500.0,
            args=args,
            setup=None,
            candidates=[],
            run_id="run-1",
            diagnostics={
                "ranking_policy_status_counts": {"passed": 1, "blocked": 2},
                "ranking_policy_blocker_counts": {
                    "expected_value_dollars_below_floor": 2,
                },
                "ranking_policy_blocked_exemplars": [
                    {
                        "underlying_symbol": "QQQ",
                        "strategy": "call_credit",
                        "expiration_date": "2026-04-30",
                        "short_symbol": "QQQ260430C00663000",
                        "long_symbol": "QQQ260430C00667000",
                        "symbol_path": "QQQ260430C00663000 / QQQ260430C00667000",
                        "quality_score": 48.5,
                        "midpoint_credit": 0.72,
                        "return_on_risk": 0.21,
                        "setup_status": "neutral",
                        "ranking_policy": {"min_expected_value_dollars": 10.0},
                        "ranking_policy_status": "blocked",
                        "ranking_policy_blockers": [
                            "expected_value_dollars_below_floor"
                        ],
                        "ranking_policy_margin_to_pass": {
                            "expected_value_dollars": 3.8,
                        },
                        "probability_of_profit": 0.84,
                        "breakeven_touch_probability": 0.29,
                        "expected_value_dollars": 6.2,
                        "slippage_adjusted_expected_value_dollars": 3.1,
                        "entry_slippage_dollars": 4.0,
                        "model_implied_volatility": 0.20,
                    }
                ],
            },
        )

        summary = build_raw_candidate_summary(
            [scan_result],
            {
                "QQQ": [
                    {
                        "underlying_symbol": "QQQ",
                        "strategy": "call_credit",
                        "expiration_date": "2026-04-30",
                        "short_symbol": "QQQ260430C00663000",
                        "long_symbol": "QQQ260430C00667000",
                        "quality_score": 51.9,
                        "midpoint_credit": 0.78,
                        "return_on_risk": 0.24,
                        "setup_status": "neutral",
                        "structure_identity": "call_credit_spread|example",
                        "ranking_policy": {"min_expected_value_dollars": 10.0},
                        "ranking_policy_status": "passed",
                        "ranking_policy_blockers": [],
                        "probability_of_profit": 0.863,
                        "breakeven_touch_probability": 0.276,
                        "expected_value_dollars": 23.22,
                        "slippage_adjusted_expected_value_dollars": 14.22,
                        "entry_slippage_dollars": 9.0,
                        "model_implied_volatility": 0.204,
                    }
                ]
            },
        )

        top_candidate = summary["top_candidates"][0]
        blocked_candidate = summary["top_blocked_candidates"][0]
        self.assertEqual(top_candidate["ranking_policy_status"], "passed")
        self.assertAlmostEqual(top_candidate["probability_of_profit"], 0.863)
        self.assertAlmostEqual(
            top_candidate["slippage_adjusted_expected_value_dollars"],
            14.22,
        )
        self.assertEqual(blocked_candidate["ranking_policy_status"], "blocked")
        self.assertEqual(
            blocked_candidate["ranking_policy_blockers"],
            ["expected_value_dollars_below_floor"],
        )
        self.assertEqual(
            blocked_candidate["ranking_policy_margin_to_pass"],
            {"expected_value_dollars": 3.8},
        )
        self.assertAlmostEqual(blocked_candidate["expected_value_dollars"], 6.2)
        self.assertEqual(
            summary["ranking_policy_gate_summary"]["status_counts"],
            {"blocked": 2, "passed": 1},
        )

    def test_build_collection_schedule_summary_marks_session_complete_after_close(
        self,
    ) -> None:
        summary = build_collection_schedule_summary(
            now=datetime(2026, 4, 21, 23, 17, tzinfo=UTC),
            interval_seconds=300,
            session_start_offset_minutes=-5,
            session_end_offset_minutes=5,
        )

        self.assertEqual(summary["state"], "complete")
        self.assertEqual(summary["expected_last_slot_at"], "2026-04-21T20:05:00Z")
        self.assertEqual(summary["expected_current_slot_at"], "2026-04-21T20:05:00Z")

    def test_evaluate_candidate_ranking_policy_returns_margin_to_pass(self) -> None:
        evaluation = evaluate_candidate_ranking_policy(
            {
                "probability_of_profit": 0.58,
                "expected_value_dollars": 6.2,
                "slippage_adjusted_expected_value_dollars": 3.1,
                "entry_slippage_dollars": 13.5,
                "model_implied_volatility": 0.16,
            },
            policy_source={
                "ranking_policy": {
                    "min_probability_of_profit": 0.60,
                    "min_expected_value_dollars": 10.0,
                    "min_slippage_adjusted_expected_value_dollars": 8.0,
                    "max_entry_slippage_dollars": 12.0,
                    "min_model_implied_volatility": 0.18,
                }
            },
        )

        self.assertEqual(evaluation["status"], "blocked")
        self.assertEqual(
            evaluation["margin_to_pass"],
            {
                "probability_of_profit": 0.02,
                "expected_value_dollars": 3.8,
                "slippage_adjusted_expected_value_dollars": 4.9,
                "entry_slippage_dollars": 1.5,
                "model_implied_volatility": 0.02,
            },
        )

    def test_build_discovery_run_scope_includes_runtime_min_return_on_risk(self) -> None:
        scope = build_discovery_run_scope(
            scanner_strategy="put_credit",
            scanner_profile="weekly",
        )

        self.assertEqual(scope["scanner_args"]["min_return_on_risk"], 0.13)

    def test_build_discovery_run_scope_sets_short_delta_target_within_band(self) -> None:
        scope = build_discovery_run_scope(
            scanner_strategy="put_credit",
            scanner_profile="weekly",
        )

        self.assertEqual(scope["scanner_args"]["short_delta_min"], 0.18)
        self.assertEqual(scope["scanner_args"]["short_delta_max"], 0.28)
        self.assertAlmostEqual(scope["scanner_args"]["short_delta_target"], 0.23)

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
            cycle_id="cycle-discovery-run",
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


class RuntimeVisibilityTests(unittest.TestCase):
    def test_list_opportunities_excludes_expired_rows_by_default(self) -> None:
        class _SignalStore:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def list_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
                self.calls.append(dict(kwargs))
                rows = [
                    {
                        "opportunity_id": "opp-active",
                        "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                        "label": "explore_10_put_credit_weekly_auto",
                        "market_date": "2026-04-20",
                        "session_date": "2026-04-20",
                        "lifecycle_state": "ready",
                        "selection_state": "promotable",
                        "selection_rank": 1,
                        "underlying_symbol": "SPY",
                    },
                    {
                        "opportunity_id": "opp-expired",
                        "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                        "label": "explore_10_put_credit_weekly_auto",
                        "market_date": "2026-04-20",
                        "session_date": "2026-04-20",
                        "lifecycle_state": "expired",
                        "selection_state": "monitor",
                        "selection_rank": 2,
                        "underlying_symbol": "QQQ",
                    },
                ]
                lifecycle_state = kwargs.get("lifecycle_state")
                if lifecycle_state is not None:
                    return [
                        row
                        for row in rows
                        if row["lifecycle_state"] == lifecycle_state
                    ]
                if kwargs.get("active_only"):
                    return [
                        row
                        for row in rows
                        if row["lifecycle_state"] in ("candidate", "ready", "blocked")
                    ]
                return rows

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()

        storage = _Storage()

        payload = list_opportunities(
            db_target="postgresql://example",
            pipeline_id="pipeline:explore_10_put_credit_weekly_auto",
            market_date="2026-04-20",
            storage=storage,
        )

        self.assertEqual(
            [row["opportunity_id"] for row in payload["opportunities"]],
            ["opp-active"],
        )
        self.assertTrue(storage.signals.calls[0]["active_only"])
        self.assertIsNone(storage.signals.calls[0]["lifecycle_state"])

    def test_list_opportunities_can_include_expired_rows(self) -> None:
        class _SignalStore:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def list_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
                self.calls.append(dict(kwargs))
                return [
                    {
                        "opportunity_id": "opp-active",
                        "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                        "label": "explore_10_put_credit_weekly_auto",
                        "market_date": "2026-04-20",
                        "session_date": "2026-04-20",
                        "lifecycle_state": "ready",
                        "selection_state": "promotable",
                        "selection_rank": 1,
                        "underlying_symbol": "SPY",
                    },
                    {
                        "opportunity_id": "opp-expired",
                        "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                        "label": "explore_10_put_credit_weekly_auto",
                        "market_date": "2026-04-20",
                        "session_date": "2026-04-20",
                        "lifecycle_state": "expired",
                        "selection_state": "monitor",
                        "selection_rank": 2,
                        "underlying_symbol": "QQQ",
                    },
                ]

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()

        storage = _Storage()

        payload = list_opportunities(
            db_target="postgresql://example",
            pipeline_id="pipeline:explore_10_put_credit_weekly_auto",
            market_date="2026-04-20",
            include_expired=True,
            storage=storage,
        )

        self.assertEqual(
            [row["opportunity_id"] for row in payload["opportunities"]],
            ["opp-active", "opp-expired"],
        )
        self.assertFalse(storage.signals.calls[0]["active_only"])

    def test_list_opportunities_respects_explicit_lifecycle_filter(self) -> None:
        class _SignalStore:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def list_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
                self.calls.append(dict(kwargs))
                lifecycle_state = kwargs.get("lifecycle_state")
                return [
                    {
                        "opportunity_id": "opp-expired",
                        "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                        "label": "explore_10_put_credit_weekly_auto",
                        "market_date": "2026-04-20",
                        "session_date": "2026-04-20",
                        "lifecycle_state": lifecycle_state,
                        "selection_state": "monitor",
                        "selection_rank": 2,
                        "underlying_symbol": "QQQ",
                    }
                ]

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()

        storage = _Storage()

        payload = list_opportunities(
            db_target="postgresql://example",
            pipeline_id="pipeline:explore_10_put_credit_weekly_auto",
            market_date="2026-04-20",
            lifecycle_state="expired",
            storage=storage,
        )

        self.assertEqual(
            [row["opportunity_id"] for row in payload["opportunities"]],
            ["opp-expired"],
        )
        self.assertEqual(storage.signals.calls[0]["lifecycle_state"], "expired")
        self.assertFalse(storage.signals.calls[0]["active_only"])

    def test_list_automation_runtimes_summarizes_owner_scoped_state(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )

        class _SignalStore:
            def schema_ready(self) -> bool:
                return True

            def automation_runtime_schema_ready(self) -> bool:
                return True

            def list_automation_runs(self, **kwargs: object) -> list[dict[str, object]]:
                if (
                    kwargs.get("bot_id") == runtime.bot_id
                    and kwargs.get("automation_id") == runtime.automation_id
                ):
                    return [
                        {
                            "automation_run_id": "auto-run-1",
                            "bot_id": runtime.bot_id,
                            "automation_id": runtime.automation_id,
                            "strategy_config_id": runtime.strategy_config_id,
                            "cycle_id": "cycle-auto",
                            "label": "explore_10_put_credit_weekly_auto",
                            "session_date": "2026-04-17",
                            "started_at": "2026-04-17T17:25:00Z",
                            "status": "completed",
                            "result": {"opportunity_count": 1},
                        }
                    ]
                return []

            def list_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
                if (
                    kwargs.get("bot_id") == runtime.bot_id
                    and kwargs.get("automation_id") == runtime.automation_id
                ):
                    return [
                        {
                            "opportunity_id": "opp-runtime-1",
                            "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                            "label": "explore_10_put_credit_weekly_auto",
                            "market_date": "2026-04-17",
                            "session_date": "2026-04-17",
                            "cycle_id": "cycle-auto",
                            "bot_id": runtime.bot_id,
                            "automation_id": runtime.automation_id,
                            "automation_run_id": "auto-run-1",
                            "strategy_config_id": runtime.strategy_config_id,
                            "strategy_id": runtime.strategy_id,
                            "config_hash": runtime.config_hash,
                            "strategy_family": runtime.strategy_family,
                            "selection_state": "promotable",
                            "selection_rank": 1,
                            "lifecycle_state": "ready",
                            "eligibility_state": "live",
                            "underlying_symbol": runtime.symbols[0],
                            "candidate_json": {
                                "underlying_symbol": runtime.symbols[0],
                                "strategy": "put_credit",
                            },
                            "economics_json": {"midpoint_credit": 1.1},
                            "strategy_metrics_json": {"width": 2.0},
                            "legs_json": [],
                            "order_payload_json": {},
                            "evidence_json": {},
                        }
                    ]
                return []

            def list_opportunity_decisions(
                self, **kwargs: object
            ) -> list[dict[str, object]]:
                if (
                    kwargs.get("bot_id") == runtime.bot_id
                    and kwargs.get("automation_id") == runtime.automation_id
                ):
                    return [
                        {
                            "opportunity_decision_id": "decision-1",
                            "opportunity_id": "opp-runtime-1",
                            "bot_id": runtime.bot_id,
                            "automation_id": runtime.automation_id,
                            "state": "selected",
                            "decided_at": "2026-04-17T17:26:00Z",
                        }
                    ]
                return []

        class _ExecutionStore:
            def portfolio_schema_ready(self) -> bool:
                return True

            def intent_schema_ready(self) -> bool:
                return True

            def list_execution_intents(self, **kwargs: object) -> list[dict[str, object]]:
                if (
                    kwargs.get("bot_id") == runtime.bot_id
                    and kwargs.get("automation_id") == runtime.automation_id
                ):
                    return [
                        {
                            "execution_intent_id": "intent-1",
                            "bot_id": runtime.bot_id,
                            "automation_id": runtime.automation_id,
                            "action_type": "open",
                            "state": "filled",
                            "created_at": "2026-04-17T17:27:00Z",
                        }
                    ]
                return []

            def list_positions(self, **kwargs: object) -> list[dict[str, object]]:
                if (
                    kwargs.get("bot_id") == runtime.bot_id
                    and kwargs.get("automation_id") == runtime.automation_id
                ):
                    return [
                        {
                            "position_id": "pos-1",
                            "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                            "bot_id": runtime.bot_id,
                            "automation_id": runtime.automation_id,
                            "strategy_config_id": runtime.strategy_config_id,
                            "strategy_id": runtime.strategy_id,
                            "config_hash": runtime.config_hash,
                            "source_opportunity_id": "opp-runtime-1",
                            "open_execution_attempt_id": "attempt-1",
                            "root_symbol": runtime.symbols[0],
                            "strategy_family": runtime.strategy_family,
                            "market_date_opened": "2026-04-17",
                            "status": "open",
                            "legs": [],
                            "economics": {
                                "entry_credit": 1.1,
                                "entry_notional": 110.0,
                                "max_profit": 110.0,
                                "max_loss": 90.0,
                            },
                            "strategy_metrics": {"width": 2.0},
                            "requested_quantity": 1,
                            "opened_quantity": 1,
                            "remaining_quantity": 1,
                            "realized_pnl": 0.0,
                            "unrealized_pnl": 8.5,
                        }
                    ]
                return []

            def list_position_closes(self, **_: object) -> list[dict[str, object]]:
                return []

            def get_attempt(self, execution_attempt_id: str) -> dict[str, object] | None:
                return {"execution_attempt_id": execution_attempt_id, "status": "filled"}

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()

        storage = _Storage()
        with (
            patch(
                "core.services.automation_runtimes.resolve_entry_runtimes",
                return_value=[runtime],
            ),
            patch(
                "core.services.automation_runtimes.resolve_management_runtimes",
                return_value=[],
            ),
            patch(
                "core.services.automation_runtimes.build_bot_metrics",
                return_value={"daily_total_pnl": 8.5, "open_position_count": 1},
            ),
        ):
            listing = list_automation_runtimes(
                db_target="postgresql://example",
                market_date="2026-04-17",
                storage=storage,
            )
            detail = get_automation_runtime_detail(
                db_target="postgresql://example",
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                market_date="2026-04-17",
                storage=storage,
            )

        self.assertEqual(len(listing["automations"]), 1)
        summary = listing["automations"][0]
        self.assertEqual(summary["automation_type"], "entry")
        self.assertEqual(summary["opportunity_count"], 1)
        self.assertEqual(summary["decision_state_counts"]["selected"], 1)
        self.assertEqual(summary["entry_intent_count"], 1)
        self.assertEqual(summary["open_position_count"], 1)
        self.assertEqual(summary["latest_discovery"]["label"], "explore_10_put_credit_weekly_auto")
        self.assertEqual(
            summary["latest_discovery"]["pipeline_id"],
            "pipeline:explore_10_put_credit_weekly_auto",
        )
        self.assertEqual(detail["summary"]["opportunity_count"], 1)
        self.assertEqual(detail["opportunities"][0]["owner"]["automation_id"], runtime.automation_id)
        self.assertEqual(detail["positions"][0]["owner"]["bot_id"], runtime.bot_id)
        self.assertEqual(len(detail["automation_runs"]), 1)

    def test_owner_scoped_opportunities_and_positions_expose_lineage_blocks(self) -> None:
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )

        class _SignalStore:
            def schema_ready(self) -> bool:
                return True

            def list_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
                if kwargs.get("bot_id") != runtime.bot_id:
                    return []
                return [
                    {
                        "opportunity_id": "opp-runtime-2",
                        "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                        "label": "explore_10_put_credit_weekly_auto",
                        "market_date": "2026-04-17",
                        "session_date": "2026-04-17",
                        "cycle_id": "cycle-auto-2",
                        "bot_id": runtime.bot_id,
                        "automation_id": runtime.automation_id,
                        "strategy_config_id": runtime.strategy_config_id,
                        "strategy_id": runtime.strategy_id,
                        "config_hash": runtime.config_hash,
                        "strategy_family": runtime.strategy_family,
                        "selection_state": "monitor",
                        "selection_rank": 2,
                        "lifecycle_state": "candidate",
                        "eligibility_state": "live",
                        "underlying_symbol": runtime.symbols[0],
                        "candidate_json": {
                            "underlying_symbol": runtime.symbols[0],
                            "strategy": "put_credit",
                        },
                        "economics_json": {},
                        "strategy_metrics_json": {},
                        "legs_json": [],
                        "order_payload_json": {},
                        "evidence_json": {},
                    }
                ]

        class _ExecutionStore:
            def portfolio_schema_ready(self) -> bool:
                return True

            def list_positions(self, **kwargs: object) -> list[dict[str, object]]:
                if kwargs.get("automation_id") != runtime.automation_id:
                    return []
                return [
                    {
                        "position_id": "pos-2",
                        "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                        "bot_id": runtime.bot_id,
                        "automation_id": runtime.automation_id,
                        "strategy_config_id": runtime.strategy_config_id,
                        "strategy_id": runtime.strategy_id,
                        "config_hash": runtime.config_hash,
                        "source_opportunity_id": "opp-runtime-2",
                        "open_execution_attempt_id": "attempt-2",
                        "root_symbol": runtime.symbols[0],
                        "strategy_family": runtime.strategy_family,
                        "market_date_opened": "2026-04-17",
                        "status": "open",
                        "legs": [],
                        "economics": {},
                        "strategy_metrics": {},
                        "requested_quantity": 1,
                        "opened_quantity": 1,
                        "remaining_quantity": 1,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": 0.0,
                    }
                ]

            def list_position_closes(self, **_: object) -> list[dict[str, object]]:
                return []

            def get_attempt(self, execution_attempt_id: str) -> dict[str, object] | None:
                return {"execution_attempt_id": execution_attempt_id, "status": "filled"}

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()

        storage = _Storage()
        opportunities = list_opportunities(
            db_target="postgresql://example",
            bot_id=runtime.bot_id,
            automation_id=runtime.automation_id,
            strategy_config_id=runtime.strategy_config_id,
            storage=storage,
        )
        positions = list_positions(
            db_target="postgresql://example",
            bot_id=runtime.bot_id,
            automation_id=runtime.automation_id,
            strategy_config_id=runtime.strategy_config_id,
            storage=storage,
        )

        opportunity = opportunities["opportunities"][0]
        position = positions["positions"][0]
        self.assertEqual(opportunity["owner"]["owner_kind"], "automation")
        self.assertEqual(opportunity["discovery"]["label"], "explore_10_put_credit_weekly_auto")
        self.assertEqual(position["owner"]["automation_id"], runtime.automation_id)
        self.assertEqual(position["discovery"]["pipeline_id"], "pipeline:explore_10_put_credit_weekly_auto")
        self.assertEqual(positions["summary"]["bot_id"], runtime.bot_id)


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


class JobsOverviewTests(unittest.TestCase):
    def test_build_jobs_overview_excludes_idle_capture_from_degraded_count(self) -> None:
        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_latest_runs_by_job_keys(
                self,
                *,
                job_keys: list[str],
                statuses: list[str] | None = None,
            ) -> list[dict[str, object]]:
                return []

            def list_job_runs(
                self,
                *,
                job_type: str | None = None,
                status: str | None = None,
                limit: int = 25,
            ) -> list[dict[str, object]]:
                if status in {"queued", "running"}:
                    return []
                if job_type != "discovery_run":
                    return []
                return [
                    {
                        "job_run_id": "discovery_run:demo:20260421T200500Z",
                        "job_key": "discovery_run:demo",
                        "job_type": "discovery_run",
                        "status": "succeeded",
                        "scheduled_for": "2026-04-21T20:05:00Z",
                        "slot_at": "2026-04-21T20:05:00Z",
                        "started_at": "2026-04-21T20:05:03Z",
                        "finished_at": "2026-04-21T20:05:08Z",
                        "heartbeat_at": "2026-04-21T20:05:08Z",
                        "worker_name": "worker-discovery-1",
                        "payload": {},
                        "result": {"status": "completed"},
                        "quote_capture": {
                            "capture_status": "idle",
                            "expected_quote_symbol_count": 0,
                            "stream_quote_events_saved": 0,
                            "baseline_quote_events_saved": 0,
                            "recovery_quote_events_saved": 0,
                        },
                        "trade_capture": {
                            "capture_status": "idle",
                            "expected_trade_symbol_count": 0,
                            "stream_trade_events_saved": 0,
                            "total_trade_events_saved": 0,
                        },
                    }
                ]

            def get_lease(self, key: str) -> dict[str, object] | None:
                if key == "runtime:scheduler":
                    return {
                        "owner": "scheduler",
                        "expires_at": "2026-04-21T20:07:30Z",
                        "job_run_id": None,
                    }
                return None

            def list_active_leases(self, *, prefix: str) -> list[dict[str, object]]:
                if prefix != "runtime:worker:":
                    return []
                return [
                    {
                        "lease_key": "runtime:worker:runtime:1",
                        "owner": "runtime:1",
                        "expires_at": "2026-04-21T20:06:30Z",
                        "lease_state": {
                            "kind": "worker",
                            "lane": "runtime",
                            "queue_name": "arq:queue:runtime",
                            "settings_name": "RuntimeWorkerSettings",
                        },
                    },
                    {
                        "lease_key": "runtime:worker:discovery:1",
                        "owner": "discovery:1",
                        "expires_at": "2026-04-21T20:06:30Z",
                        "lease_state": {
                            "kind": "worker",
                            "lane": "discovery",
                            "queue_name": "arq:queue:discovery",
                            "settings_name": "DiscoveryWorkerSettings",
                        },
                    },
                ]

        storage = SimpleNamespace(jobs=_JobStore())
        with (
            patch("core.services.ops.jobs._utc_now", return_value="2026-04-21T20:06:00Z"),
            patch("core.services.ops.jobs.list_declared_job_rows", return_value=[]),
        ):
            payload = build_jobs_overview(
                storage=storage,
                job_type="discovery_run",
                limit=10,
            )

        self.assertEqual(payload["status"], "healthy")
        self.assertEqual(payload["summary"]["degraded_capture_count"], 0)
        self.assertEqual(payload["attention"], [])


if __name__ == "__main__":
    unittest.main()
