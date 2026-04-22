from __future__ import annotations

import unittest
from argparse import Namespace
from dataclasses import asdict

from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
)
from core.services.discovery_run_health.selection import build_selection_summary
from core.services.execution import _build_order_request, normalize_execution_policy
from core.services.opportunity_scoring import build_candidate_opportunity_score
from core.services.scanners.builders.single_legs import (
    build_short_calls,
    build_short_puts,
)


def _args() -> Namespace:
    return Namespace(
        profile="weekly",
        min_open_interest=100,
        max_relative_spread=0.25,
        short_delta_min=0.10,
        short_delta_max=0.25,
        short_delta_target=0.18,
        min_width=0.0,
        max_width=0.0,
        min_credit=0.20,
        min_return_on_risk=0.05,
        min_fill_ratio=0.70,
        min_short_vs_expected_move_ratio=-0.10,
        min_breakeven_vs_expected_move_ratio=-0.05,
    )


def _execution_policy() -> dict[str, object]:
    return normalize_execution_policy(
        {
            "enabled": True,
            "mode": "top_promotable",
            "pricing_mode": "midpoint",
            "quantity": 1,
        }
    )


class ShortSingleLegFlowE2ETests(unittest.TestCase):
    def test_short_call_scanner_order_and_score_flow(self) -> None:
        expiration = "2026-04-24"
        candidates = build_short_calls(
            symbol="SPY",
            spot_price=500.0,
            contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="SPY260424C510",
                        expiration_date=expiration,
                        strike_price=510.0,
                        open_interest=3200,
                        close_price=None,
                    ),
                ]
            },
            snapshots_by_expiration={
                expiration: {
                    "SPY260424C510": OptionSnapshot(
                        symbol="SPY260424C510",
                        bid=2.10,
                        ask=2.40,
                        bid_size=60,
                        ask_size=62,
                        midpoint=2.25,
                        delta=0.18,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.26,
                        last_trade_price=None,
                        daily_volume=1500,
                        greeks_source="alpaca",
                    ),
                }
            },
            expected_moves_by_expiration={
                expiration: ExpectedMoveEstimate(
                    expiration_date=expiration,
                    amount=4.0,
                    percent_of_spot=4.0 / 500.0,
                    reference_strike=500.0,
                )
            },
            args=_args(),
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.strategy, "short_call")
        self.assertEqual(candidate.order_payload["legs"][0]["side"], "sell")
        self.assertEqual(candidate.order_payload["limit_price"], "-2.25")
        self.assertAlmostEqual(candidate.return_on_risk, 1.0, places=4)

        candidate_payload = asdict(candidate)
        candidate_payload.update(
            {
                "quality_score": 79.0,
                "setup_score": 53.0,
                "setup_intraday_score": 50.0,
                "setup_status": "neutral",
                "data_status": "clean",
                "calendar_status": "clean",
                "earnings_phase": "clean",
            }
        )
        scorecard = build_candidate_opportunity_score(candidate_payload)
        self.assertEqual(scorecard["strategy_family"], "short_call")

        order_request, resolved_quantity, resolved_limit_price = _build_order_request(
            candidate={"candidate": candidate_payload},
            quantity=1,
            limit_price=candidate_payload["midpoint_credit"],
            execution_policy=_execution_policy(),
            client_order_id="test-short-call-open",
        )
        self.assertEqual(resolved_quantity, 1)
        self.assertEqual(resolved_limit_price, 2.25)
        self.assertEqual(order_request["limit_price"], "-2.25")
        self.assertEqual(order_request["legs"][0]["side"], "sell")

    def test_short_put_surfaces_in_selection_summary(self) -> None:
        expiration = "2026-04-24"
        candidates = build_short_puts(
            symbol="QQQ",
            spot_price=420.0,
            contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="QQQ260424P412",
                        expiration_date=expiration,
                        strike_price=412.0,
                        open_interest=2800,
                        close_price=None,
                    ),
                ]
            },
            snapshots_by_expiration={
                expiration: {
                    "QQQ260424P412": OptionSnapshot(
                        symbol="QQQ260424P412",
                        bid=1.70,
                        ask=1.90,
                        bid_size=58,
                        ask_size=59,
                        midpoint=1.80,
                        delta=-0.17,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.24,
                        last_trade_price=None,
                        daily_volume=1320,
                        greeks_source="alpaca",
                    ),
                }
            },
            expected_moves_by_expiration={
                expiration: ExpectedMoveEstimate(
                    expiration_date=expiration,
                    amount=5.0,
                    percent_of_spot=5.0 / 420.0,
                    reference_strike=420.0,
                )
            },
            args=_args(),
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        candidate_payload = asdict(candidate)
        candidate_payload.update(
            {
                "quality_score": 81.0,
                "setup_score": 54.0,
                "setup_intraday_score": 51.0,
                "setup_status": "neutral",
                "data_status": "clean",
                "calendar_status": "clean",
                "earnings_phase": "clean",
            }
        )
        scorecard = build_candidate_opportunity_score(candidate_payload)
        self.assertEqual(scorecard["strategy_family"], "short_put")

        summary = build_selection_summary(
            [
                {
                    "selection_state": "monitor",
                    "eligibility": "analysis_only",
                    "candidate": {
                        **candidate_payload,
                        "score_evidence": {
                            "signal_gate": scorecard["signal_gate"],
                        },
                    },
                }
            ]
        )
        self.assertEqual(summary["strategy_family_counts"]["short_put"], 1)
        self.assertEqual(summary["selection_state_counts"]["monitor"], 1)


if __name__ == "__main__":
    unittest.main()
