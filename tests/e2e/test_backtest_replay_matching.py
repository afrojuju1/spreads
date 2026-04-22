from __future__ import annotations

import unittest

from core.backtest.replay import (
    _candidate_identity,
    _candidate_match_key,
    _replay_comparison_mode,
    _upgrade_legacy_runtime_candidate_filter,
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


if __name__ == "__main__":
    unittest.main()
