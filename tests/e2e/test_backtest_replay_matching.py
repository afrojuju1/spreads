from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from core.backtest.replay import (
    _cached_entry_runtimes,
    _candidate_identity,
    _candidate_match_key,
    _replay_comparison_mode,
    _upgrade_legacy_runtime_candidate_filter,
    build_replay_payload,
    build_replay_range_payload,
)

TEST_FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures"
RELAXED_LONG_VOL_CONFIG_ROOT = (
    TEST_FIXTURE_ROOT / "config_roots" / "relaxed_long_vol"
)


def _legacy_iron_condor_candidate() -> dict[str, object]:
    return {
        "strategy": "iron_condor",
        "expiration_date": "2026-04-30",
        "legs": [
            {
                "symbol": "SPY260430P00690000",
                "role": "short",
                "position_intent": "sell_to_open",
                "ratio_qty": "1",
                "expiration_date": "2026-04-30",
            },
            {
                "symbol": "SPY260430P00688000",
                "role": "long",
                "position_intent": "buy_to_open",
                "ratio_qty": "1",
                "expiration_date": "2026-04-30",
            },
        ],
        "short_symbol": "SPY260430P00690000",
        "long_symbol": "SPY260430P00688000",
        "width": 2.0,
        "midpoint_credit": 0.595,
        "natural_credit": 0.56,
        "breakeven": 689.405,
        "max_profit": 59.5,
        "max_loss": 140.5,
        "return_on_risk": 0.42348754448398573,
    }


def _full_iron_condor_candidate(
    *,
    short_call: str,
    long_call: str,
) -> dict[str, object]:
    return {
        "strategy": "iron_condor",
        "expiration_date": "2026-04-30",
        "legs": [
            {
                "symbol": "SPY260430P00690000",
                "role": "short",
                "position_intent": "sell_to_open",
                "ratio_qty": "1",
                "expiration_date": "2026-04-30",
            },
            {
                "symbol": "SPY260430P00688000",
                "role": "long",
                "position_intent": "buy_to_open",
                "ratio_qty": "1",
                "expiration_date": "2026-04-30",
            },
            {
                "symbol": short_call,
                "role": "short",
                "position_intent": "sell_to_open",
                "ratio_qty": "1",
                "expiration_date": "2026-04-30",
            },
            {
                "symbol": long_call,
                "role": "long",
                "position_intent": "buy_to_open",
                "ratio_qty": "1",
                "expiration_date": "2026-04-30",
            },
        ],
        "short_symbol": "SPY260430P00690000",
        "long_symbol": "SPY260430P00688000",
        "width": 2.0,
        "midpoint_credit": 0.595,
        "natural_credit": 0.56,
        "breakeven": 689.405,
        "max_profit": 59.5,
        "max_loss": 140.5,
        "return_on_risk": 0.42348754448398573,
    }


def _single_leg_candidate(strategy: str, symbol: str) -> dict[str, object]:
    role = "short" if strategy in {"short_call", "short_put"} else "long"
    position_intent = "sell_to_open" if role == "short" else "buy_to_open"
    return {
        "strategy": strategy,
        "expiration_date": "2026-04-30",
        "legs": [
            {
                "symbol": symbol,
                "role": role,
                "position_intent": position_intent,
                "ratio_qty": "1",
                "expiration_date": "2026-04-30",
            }
        ],
        "short_symbol": symbol,
        "long_symbol": symbol,
        "width": 0.0,
        "midpoint_credit": 1.25,
        "natural_credit": 1.3,
        "breakeven": 721.25,
        "max_profit": 80.0,
        "max_loss": 125.0,
        "return_on_risk": 0.64,
    }


class BacktestReplayMatchingTests(unittest.TestCase):
    def test_legacy_iron_condor_candidates_match_full_replay_rows(self) -> None:
        stored_candidate = _legacy_iron_condor_candidate()
        replayed_candidate = _full_iron_condor_candidate(
            short_call="SPY260430C00727000",
            long_call="SPY260430C00729000",
        )

        mode = _replay_comparison_mode(
            run={"strategy": "iron_condor"},
            stored_candidates=[stored_candidate],
        )

        self.assertEqual(mode, "legacy_iron_condor_compat")
        self.assertEqual(
            _candidate_match_key(stored_candidate, mode=mode),
            _candidate_match_key(replayed_candidate, mode=mode),
        )

    def test_current_iron_condor_replay_stays_on_full_identity(self) -> None:
        first_candidate = _full_iron_condor_candidate(
            short_call="SPY260430C00727000",
            long_call="SPY260430C00729000",
        )
        second_candidate = _full_iron_condor_candidate(
            short_call="SPY260430C00725000",
            long_call="SPY260430C00727000",
        )

        mode = _replay_comparison_mode(
            run={"strategy": "iron_condor"},
            stored_candidates=[first_candidate],
        )

        self.assertEqual(mode, "full_identity")
        self.assertNotEqual(
            _candidate_match_key(first_candidate, mode=mode),
            _candidate_match_key(second_candidate, mode=mode),
        )

    def test_single_leg_candidates_keep_full_identity_matching(self) -> None:
        for strategy, symbol in (
            ("long_call", "SPY260430C00720000"),
            ("long_put", "SPY260430P00690000"),
            ("short_call", "SPY260430C00725000"),
            ("short_put", "SPY260430P00685000"),
        ):
            with self.subTest(strategy=strategy):
                candidate = _single_leg_candidate(strategy, symbol)
                mode = _replay_comparison_mode(
                    run={"strategy": strategy},
                    stored_candidates=[candidate],
                )
                self.assertEqual(mode, "full_identity")
                self.assertEqual(
                    _candidate_match_key(candidate, mode=mode),
                    _candidate_identity(candidate),
                )

    def test_width_only_runtime_filters_upgrade_for_legacy_replay_artifacts(self) -> None:
        upgraded = _upgrade_legacy_runtime_candidate_filter(
            run={
                "symbol": "SPY",
                "strategy": "iron_condor",
                "profile": "weekly",
            },
            candidate_filter={"allowed_widths": [2.0, 3.0, 5.0]},
        )

        self.assertEqual(upgraded["symbols"], ["SPY"])
        self.assertEqual(upgraded["entry_recipe_refs"], ["neutral_range"])
        self.assertEqual(upgraded["allowed_widths"], [2.0, 3.0, 5.0])

    def test_width_only_runtime_filters_use_alternate_config_root_when_supplied(self) -> None:
        fake_runtime = SimpleNamespace(
            strategy_id="iron_condor",
            symbols=("SPY",),
            entry_recipe_refs=("neutral_range",),
            build_settings=SimpleNamespace(
                scanner_profile="weekly",
                width_points=(2.0, 3.0, 5.0),
            ),
        )
        _cached_entry_runtimes.cache_clear()
        try:
            with patch(
                "core.backtest.replay.resolve_entry_runtimes",
                return_value=[fake_runtime],
            ) as resolve_entry_runtimes_mock:
                upgraded = _upgrade_legacy_runtime_candidate_filter(
                    run={
                        "symbol": "SPY",
                        "strategy": "iron_condor",
                        "profile": "weekly",
                    },
                    candidate_filter={"allowed_widths": [2.0, 3.0, 5.0]},
                    config_root="/tmp/policy-compare",
                )
        finally:
            _cached_entry_runtimes.cache_clear()

        resolve_entry_runtimes_mock.assert_called_once_with(
            config_root="/tmp/policy-compare"
        )
        self.assertEqual(upgraded["symbols"], ["SPY"])
        self.assertEqual(upgraded["entry_recipe_refs"], ["neutral_range"])
        self.assertEqual(upgraded["allowed_widths"], [2.0, 3.0, 5.0])

    def test_build_replay_payload_forwards_config_root_to_run_replay_builder(self) -> None:
        storage = SimpleNamespace(history=SimpleNamespace())
        run = {"run_id": "run-1"}
        payload = {"status": "completed"}

        with (
            patch(
                "core.backtest.replay._resolve_target_run",
                return_value=run,
            ) as resolve_target_run_mock,
            patch(
                "core.backtest.replay._build_replay_payload_for_run",
                return_value=payload,
            ) as build_replay_payload_for_run_mock,
        ):
            result = build_replay_payload(
                db_target="",
                run_id="run-1",
                config_root="/tmp/policy-compare",
                calendar_confidence_policy="consensus",
                storage=storage,
            )

        resolve_target_run_mock.assert_called_once_with(
            history_store=storage.history,
            run_id="run-1",
            symbol=None,
            strategy=None,
            latest=False,
        )
        build_replay_payload_for_run_mock.assert_called_once_with(
            history_store=storage.history,
            run=run,
            config_root="/tmp/policy-compare",
            calendar_confidence_policy="consensus",
        )
        self.assertEqual(result, payload)

    def test_build_replay_range_payload_forwards_config_root_to_alpaca_builder(self) -> None:
        storage = SimpleNamespace()
        payload = {"status": "completed", "source": "alpaca"}

        with patch(
            "core.backtest.replay._build_alpaca_replay_range_payload",
            return_value=payload,
        ) as build_alpaca_replay_range_payload_mock:
            result = build_replay_range_payload(
                db_target="",
                bot_id="bot-1",
                automation_id="auto-1",
                start_date="2026-04-20",
                end_date="2026-04-23",
                source="alpaca",
                config_root="/tmp/policy-compare",
                calendar_confidence_policy="consensus",
                storage=storage,
            )

        build_alpaca_replay_range_payload_mock.assert_called_once_with(
            db_target="",
            bot_id="bot-1",
            automation_id="auto-1",
            start_date="2026-04-20",
            end_date="2026-04-23",
            limit=500,
            storage=storage,
            config_root="/tmp/policy-compare",
            sample_mode="intraday",
            calendar_confidence_policy="consensus",
        )
        self.assertEqual(result, payload)

    def test_build_replay_range_payload_includes_config_root_metadata(self) -> None:
        storage = SimpleNamespace(
            signals=SimpleNamespace(list_automation_runs=lambda **_: []),
            discovery=SimpleNamespace(),
            history=SimpleNamespace(),
        )

        payload = build_replay_range_payload(
            db_target="",
            bot_id="bot-1",
            automation_id="auto-1",
            start_date="2026-04-20",
            end_date="2026-04-23",
            config_root="/tmp/policy-compare",
            calendar_confidence_policy="off",
            storage=storage,
        )

        self.assertEqual(payload["config_root"], "/tmp/policy-compare")
        self.assertEqual(payload["target"]["config_root"], "/tmp/policy-compare")
        self.assertEqual(payload["target"]["calendar_confidence_policy"], "off")

    def test_alpaca_replay_range_attaches_config_root_to_scanner_args(self) -> None:
        storage = SimpleNamespace(signals=SimpleNamespace())
        runtime = SimpleNamespace(
            bot=SimpleNamespace(bot=SimpleNamespace()),
            symbols=("AAPL",),
            automation=SimpleNamespace(automation=SimpleNamespace(schedule={})),
            build_settings=SimpleNamespace(scanner_profile="weekly"),
            trigger_policy={},
        )
        captured_config_roots: list[str | None] = []

        def _record_market_slice_args(
            *,
            symbol: str,
            base_scanner_args: object,
            runtimes: object,
        ) -> object:
            captured_config_roots.append(getattr(base_scanner_args, "config_root", None))
            return SimpleNamespace(stock_feed="sip", feed="indicative")

        with (
            patch(
                "core.backtest.replay.resolve_entry_runtime",
                return_value=runtime,
            ),
            patch(
                "core.backtest.replay._alpaca_replay_dependencies",
                return_value=(SimpleNamespace(), object(), object()),
            ),
            patch(
                "core.backtest.replay._alpaca_trading_dates",
                return_value=["2026-04-24"],
            ),
            patch(
                "core.backtest.replay.build_market_slice_args",
                side_effect=_record_market_slice_args,
            ),
            patch(
                "core.backtest.replay.build_historical_symbol_session_data_from_alpaca",
                return_value=object(),
            ),
            patch(
                "core.backtest.replay.build_historical_symbol_market_slice_from_session_data",
                return_value=object(),
            ),
            patch(
                "core.backtest.replay.build_entry_runtime_symbol_candidates_from_market_slice",
                return_value={
                    "symbol": "AAPL",
                    "replay_details": {
                        "raw_candidate_count": 0,
                        "postprocess_candidate_count": 0,
                    },
                    "all_rows": [],
                    "rows": [],
                    "runtime_filter_reason_counts": {},
                },
            ),
            patch(
                "core.backtest.replay.select_live_opportunities",
                return_value={"opportunities": [], "symbol_candidates": {}},
            ),
            patch(
                "core.backtest.replay.evaluate_entry_controls",
                return_value=(True, None, {}),
            ),
            patch(
                "core.backtest.replay.plan_entry_selection",
                return_value={"selected": None, "eligible_opportunity_count": 0},
            ),
        ):
            payload = build_replay_range_payload(
                db_target="",
                bot_id="bot-1",
                automation_id="auto-1",
                start_date="2026-04-24",
                end_date="2026-04-24",
                limit=1,
                source="alpaca",
                config_root=str(RELAXED_LONG_VOL_CONFIG_ROOT),
                sample_mode="eod",
                storage=storage,
            )

        self.assertEqual(
            captured_config_roots,
            [str(RELAXED_LONG_VOL_CONFIG_ROOT)],
        )
        self.assertEqual(payload["config_root"], str(RELAXED_LONG_VOL_CONFIG_ROOT))


if __name__ == "__main__":
    unittest.main()
