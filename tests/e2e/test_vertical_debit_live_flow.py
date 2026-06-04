from __future__ import annotations

import unittest
from argparse import Namespace
from dataclasses import asdict

from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
)
from core.services.execution import (
    _build_close_order_request,
    _build_order_request,
    normalize_execution_policy,
)
from core.services.opportunity_scoring import build_candidate_opportunity_score
from core.services.scanners.builders.verticals import build_vertical_spreads


def _args() -> Namespace:
    return Namespace(
        profile="weekly",
        min_open_interest=100,
        max_relative_spread=0.25,
        short_delta_min=0.15,
        short_delta_max=0.35,
        short_delta_target=0.25,
        min_width=1.0,
        max_width=10.0,
        min_credit=0.10,
        min_return_on_risk=0.05,
    )


def _execution_policy() -> dict[str, object]:
    return normalize_execution_policy(
        {
            "enabled": True,
            "mode": "top_promotable",
            "pricing_mode": "adaptive_credit",
            "quantity": 1,
            "min_credit_retention_pct": 0.95,
            "max_credit_concession": 0.10,
        }
    )


class VerticalDebitLiveFlowE2ETests(unittest.TestCase):
    def test_call_debit_scanner_scoring_and_execution_flow(self) -> None:
        expiration = "2026-04-24"
        candidates = build_vertical_spreads(
            symbol="AAPL",
            strategy="call_debit",
            spot_price=100.0,
            contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="AAPL260424C100",
                        expiration_date=expiration,
                        strike_price=100.0,
                        open_interest=1400,
                        close_price=None,
                    ),
                    OptionContract(
                        symbol="AAPL260424C105",
                        expiration_date=expiration,
                        strike_price=105.0,
                        open_interest=1200,
                        close_price=None,
                    ),
                ]
            },
            snapshots_by_expiration={
                expiration: {
                    "AAPL260424C100": OptionSnapshot(
                        symbol="AAPL260424C100",
                        bid=2.90,
                        ask=3.10,
                        bid_size=50,
                        ask_size=60,
                        midpoint=3.00,
                        delta=0.55,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.34,
                        last_trade_price=None,
                        daily_volume=800,
                        greeks_source="alpaca",
                    ),
                    "AAPL260424C105": OptionSnapshot(
                        symbol="AAPL260424C105",
                        bid=0.90,
                        ask=1.10,
                        bid_size=45,
                        ask_size=50,
                        midpoint=1.00,
                        delta=0.25,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.33,
                        last_trade_price=None,
                        daily_volume=650,
                        greeks_source="alpaca",
                    ),
                }
            },
            expected_moves_by_expiration={
                expiration: ExpectedMoveEstimate(
                    expiration_date=expiration,
                    amount=4.0,
                    percent_of_spot=0.04,
                    reference_strike=100.0,
                )
            },
            args=_args(),
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.strategy, "call_debit")
        self.assertEqual(candidate.short_symbol, "AAPL260424C105")
        self.assertEqual(candidate.long_symbol, "AAPL260424C100")
        self.assertAlmostEqual(candidate.midpoint_credit, 2.0, places=4)
        self.assertAlmostEqual(candidate.natural_credit, 2.2, places=4)
        self.assertAlmostEqual(candidate.fill_ratio, 2.0 / 2.2, places=4)
        self.assertAlmostEqual(candidate.max_profit, 300.0, places=4)
        self.assertAlmostEqual(candidate.max_loss, 200.0, places=4)
        self.assertAlmostEqual(candidate.return_on_risk, 1.5, places=4)
        self.assertEqual(candidate.order_payload["limit_price"], "2.00")

        candidate_payload = asdict(candidate)
        candidate_payload.update(
            {
                "quality_score": 88.0,
                "setup_score": 84.0,
                "setup_intraday_score": 86.0,
                "setup_status": "favorable",
                "data_status": "clean",
                "calendar_status": "clean",
                "earnings_phase": "pre_event_runup",
                "earnings_timing_confidence": "high",
                "options_bias_alignment": True,
                "debit_width_ratio": round(candidate.midpoint_credit / candidate.width, 4),
            }
        )
        scorecard = build_candidate_opportunity_score(candidate_payload)
        self.assertEqual(scorecard["strategy_family"], "call_debit_spread")
        self.assertEqual(scorecard["earnings_phase"], "pre_event_runup")
        self.assertEqual(scorecard["promotion_floor"], 72.0)
        self.assertTrue(scorecard["signal_gate"]["active"])
        self.assertTrue(scorecard["signal_gate"]["eligible"])

        live_candidate = {
            "underlying_symbol": candidate.underlying_symbol,
            "strategy": candidate.strategy,
            "expiration_date": candidate.expiration_date,
            "short_symbol": candidate.short_symbol,
            "long_symbol": candidate.long_symbol,
            "candidate": candidate_payload,
        }
        order_request, resolved_quantity, resolved_limit_price = _build_order_request(
            candidate=live_candidate,
            quantity=1,
            limit_price=None,
            execution_policy=_execution_policy(),
            client_order_id="test-open-order",
        )
        self.assertEqual(resolved_quantity, 1)
        self.assertGreater(resolved_limit_price, 0.0)
        self.assertEqual(order_request["limit_price"], "2.02")
        self.assertEqual(order_request["legs"][0]["position_intent"], "sell_to_open")
        self.assertEqual(order_request["legs"][1]["position_intent"], "buy_to_open")

        close_request, close_quantity, close_limit_price = _build_close_order_request(
            position={
                "strategy": candidate.strategy,
                "short_symbol": candidate.short_symbol,
                "long_symbol": candidate.long_symbol,
                "legs": order_request["legs"],
                "remaining_quantity": 1,
                "close_mark": 2.85,
            },
            quantity=1,
            limit_price=None,
            client_order_id="test-close-order",
        )
        self.assertEqual(close_quantity, 1)
        self.assertEqual(close_limit_price, 2.85)
        self.assertEqual(close_request["limit_price"], "-2.85")
        self.assertEqual(close_request["legs"][0]["position_intent"], "buy_to_close")
        self.assertEqual(close_request["legs"][1]["position_intent"], "sell_to_close")


if __name__ == "__main__":
    unittest.main()
