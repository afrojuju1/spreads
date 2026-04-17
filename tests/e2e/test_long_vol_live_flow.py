from __future__ import annotations

import unittest
from argparse import Namespace
from dataclasses import asdict
from typing import Any

from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
)
from core.services.execution import _build_order_request, normalize_execution_policy
from core.services.live_collector_health import build_selection_summary
from core.services.opportunity_scoring import build_candidate_opportunity_score
from core.services.scanners.service import (
    build_long_straddles,
    build_long_strangles,
)
from core.services.session_positions import sync_session_position_from_attempt


def _args() -> Namespace:
    return Namespace(
        profile="weekly",
        min_open_interest=100,
        max_relative_spread=0.25,
        short_delta_min=0.10,
        short_delta_max=0.25,
        short_delta_target=0.18,
        min_width=1.0,
        max_width=10.0,
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


class _InMemoryExecutionStore:
    def __init__(self) -> None:
        self.positions: dict[str, dict[str, Any]] = {}
        self.closes: dict[str, list[dict[str, Any]]] = {}
        self.attempt_links: dict[str, dict[str, Any]] = {}

    def portfolio_schema_ready(self) -> bool:
        return True

    def intent_schema_ready(self) -> bool:
        return False

    def create_position(self, *, position_id: str, **payload: Any) -> dict[str, Any]:
        row = {"position_id": position_id, **payload}
        self.positions[position_id] = row
        return dict(row)

    def update_position(self, *, position_id: str, **payload: Any) -> dict[str, Any]:
        row = self.positions[position_id]
        row.update(payload)
        return dict(row)

    def get_position(self, position_id: str) -> dict[str, Any] | None:
        row = self.positions.get(position_id)
        return None if row is None else dict(row)

    def get_position_by_open_attempt(
        self,
        open_execution_attempt_id: str,
    ) -> dict[str, Any] | None:
        for row in self.positions.values():
            if row.get("open_execution_attempt_id") == open_execution_attempt_id:
                return dict(row)
        return None

    def update_attempt(self, *, execution_attempt_id: str, **payload: Any) -> None:
        self.attempt_links.setdefault(execution_attempt_id, {}).update(payload)

    def upsert_position_close(
        self,
        *,
        position_id: str,
        execution_attempt_id: str,
        **payload: Any,
    ) -> None:
        rows = self.closes.setdefault(position_id, [])
        for row in rows:
            if row.get("execution_attempt_id") == execution_attempt_id:
                row.update(payload)
                return
        rows.append({"execution_attempt_id": execution_attempt_id, **payload})

    def list_position_closes(self, *, position_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.closes.get(position_id, [])]


class LongVolLiveFlowE2ETests(unittest.TestCase):
    def test_long_straddle_scanner_scoring_order_and_position_flow(self) -> None:
        expiration = "2026-04-24"
        candidates = build_long_straddles(
            symbol="NFLX",
            spot_price=900.0,
            call_contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="NFLX260424C900",
                        expiration_date=expiration,
                        strike_price=900.0,
                        open_interest=1800,
                        close_price=None,
                    ),
                ]
            },
            put_contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="NFLX260424P900",
                        expiration_date=expiration,
                        strike_price=900.0,
                        open_interest=1750,
                        close_price=None,
                    ),
                ]
            },
            call_snapshots_by_expiration={
                expiration: {
                    "NFLX260424C900": OptionSnapshot(
                        symbol="NFLX260424C900",
                        bid=4.80,
                        ask=5.20,
                        bid_size=40,
                        ask_size=42,
                        midpoint=5.00,
                        delta=0.51,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.54,
                        last_trade_price=None,
                        daily_volume=900,
                        greeks_source="alpaca",
                    ),
                }
            },
            put_snapshots_by_expiration={
                expiration: {
                    "NFLX260424P900": OptionSnapshot(
                        symbol="NFLX260424P900",
                        bid=4.70,
                        ask=5.10,
                        bid_size=38,
                        ask_size=39,
                        midpoint=4.90,
                        delta=-0.49,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.56,
                        last_trade_price=None,
                        daily_volume=880,
                        greeks_source="alpaca",
                    ),
                }
            },
            expected_moves_by_expiration={
                expiration: ExpectedMoveEstimate(
                    expiration_date=expiration,
                    amount=11.2,
                    percent_of_spot=11.2 / 900.0,
                    reference_strike=900.0,
                )
            },
            args=_args(),
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.strategy, "long_straddle")
        self.assertEqual(len(candidate.order_payload["legs"]), 2)
        self.assertTrue(all(leg["side"] == "buy" for leg in candidate.order_payload["legs"]))

        candidate_payload = asdict(candidate)
        candidate_payload.update(
            {
                "quality_score": 89.0,
                "setup_score": 52.0,
                "setup_intraday_score": 50.0,
                "setup_status": "neutral",
                "data_status": "clean",
                "calendar_status": "clean",
                "earnings_phase": "through_event",
                "earnings_timing_confidence": "high",
                "earnings_event_date": "2026-04-23",
                "pricing_signal": 0.72,
                "pricing_signal_subsignal_count": 2,
                "jump_risk_signal": 0.78,
                "jump_risk_signal_subsignal_count": 3,
                "modeled_move_vs_implied_move": 1.18,
                "modeled_move_vs_break_even_move": 1.12,
            }
        )
        scorecard = build_candidate_opportunity_score(candidate_payload)
        self.assertEqual(scorecard["strategy_family"], "long_straddle")
        self.assertTrue(scorecard["signal_gate"]["active"])
        self.assertTrue(scorecard["signal_gate"]["eligible"])

        order_request, resolved_quantity, resolved_limit_price = _build_order_request(
            candidate={"candidate": candidate_payload},
            quantity=1,
            limit_price=candidate_payload["midpoint_credit"],
            execution_policy=_execution_policy(),
            client_order_id="test-long-straddle-open",
        )
        self.assertEqual(resolved_quantity, 1)
        self.assertEqual(len(order_request["legs"]), 2)
        self.assertEqual(order_request["limit_price"], "9.90")
        self.assertEqual(resolved_limit_price, 9.9)

        store = _InMemoryExecutionStore()
        position = sync_session_position_from_attempt(
            execution_store=store,
            attempt={
                "execution_attempt_id": "attempt-long-straddle-open",
                "session_date": "2026-04-14",
                "market_date": "2026-04-14",
                "label": "explore_10_long_straddle_weekly_auto",
                "pipeline_id": "pipeline:explore_10_long_straddle_weekly_auto",
                "underlying_symbol": "NFLX",
                "strategy": "long_straddle",
                "strategy_family": "long_straddle",
                "expiration_date": expiration,
                "quantity": 1,
                "status": "filled",
                "requested_at": "2026-04-14T15:00:00Z",
                "submitted_at": "2026-04-14T15:00:01Z",
                "completed_at": "2026-04-14T15:00:05Z",
                "request": {"trade_intent": "open", "order": order_request},
                "candidate": candidate_payload,
                "orders": [
                    {
                        "broker_order_id": "broker-long-straddle-open",
                        "filled_qty": 1,
                        "filled_avg_price": 9.9,
                    }
                ],
                "fills": [
                    {"symbol": "NFLX260424P900", "price": 5.0, "quantity": 1},
                    {"symbol": "NFLX260424C900", "price": 4.9, "quantity": 1},
                ],
            },
        )
        self.assertIsNotNone(position)
        assert position is not None
        self.assertEqual(position["status"], "open")
        self.assertEqual(position["strategy_family"], "long_straddle")
        self.assertEqual(len(position["legs"]), 2)
        self.assertAlmostEqual(position["entry_value"], 9.9, places=4)
        self.assertAlmostEqual(position["economics"]["max_loss"], 990.0, places=2)

    def test_long_strangle_surfaces_in_selection_summary(self) -> None:
        expiration = "2026-04-24"
        candidates = build_long_strangles(
            symbol="AAPL",
            spot_price=210.0,
            call_contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="AAPL260424C215",
                        expiration_date=expiration,
                        strike_price=215.0,
                        open_interest=2600,
                        close_price=None,
                    ),
                ]
            },
            put_contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="AAPL260424P205",
                        expiration_date=expiration,
                        strike_price=205.0,
                        open_interest=2550,
                        close_price=None,
                    ),
                ]
            },
            call_snapshots_by_expiration={
                expiration: {
                    "AAPL260424C215": OptionSnapshot(
                        symbol="AAPL260424C215",
                        bid=1.40,
                        ask=1.60,
                        bid_size=55,
                        ask_size=55,
                        midpoint=1.50,
                        delta=0.18,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.39,
                        last_trade_price=None,
                        daily_volume=1300,
                        greeks_source="alpaca",
                    ),
                }
            },
            put_snapshots_by_expiration={
                expiration: {
                    "AAPL260424P205": OptionSnapshot(
                        symbol="AAPL260424P205",
                        bid=1.35,
                        ask=1.55,
                        bid_size=58,
                        ask_size=58,
                        midpoint=1.45,
                        delta=-0.17,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.38,
                        last_trade_price=None,
                        daily_volume=1280,
                        greeks_source="alpaca",
                    ),
                }
            },
            expected_moves_by_expiration={
                expiration: ExpectedMoveEstimate(
                    expiration_date=expiration,
                    amount=7.6,
                    percent_of_spot=7.6 / 210.0,
                    reference_strike=210.0,
                )
            },
            args=_args(),
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        candidate_payload = asdict(candidate)
        candidate_payload.update(
            {
                "quality_score": 84.0,
                "setup_score": 50.0,
                "setup_intraday_score": 49.0,
                "setup_status": "neutral",
                "data_status": "clean",
                "calendar_status": "clean",
                "earnings_phase": "through_event",
                "earnings_timing_confidence": "high",
                "earnings_event_date": "2026-04-23",
                "pricing_signal": 0.68,
                "pricing_signal_subsignal_count": 2,
                "jump_risk_signal": 0.75,
                "jump_risk_signal_subsignal_count": 3,
                "modeled_move_vs_break_even_move": 1.09,
            }
        )
        scorecard = build_candidate_opportunity_score(candidate_payload)
        self.assertEqual(scorecard["strategy_family"], "long_strangle")

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
        self.assertEqual(summary["strategy_family_counts"]["long_strangle"], 1)
        self.assertEqual(summary["earnings_phase_counts"]["through_event"], 1)
        self.assertEqual(summary["selection_state_counts"]["monitor"], 1)
        self.assertEqual(summary["timing_confidence_counts"]["high"], 1)
        self.assertEqual(summary["shadow_only_count"], 1)


if __name__ == "__main__":
    unittest.main()
