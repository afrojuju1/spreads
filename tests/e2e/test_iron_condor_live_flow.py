from __future__ import annotations

import unittest
from argparse import Namespace
from dataclasses import asdict
from typing import Any

from core.services.execution import (
    _build_close_order_request,
    _build_order_request,
    _validate_live_deployment_quality,
    normalize_execution_policy,
)
from core.services.opportunity_scoring import build_candidate_opportunity_score
from core.services.scanner import (
    ExpectedMoveEstimate,
    LiveOptionQuote,
    OptionContract,
    OptionSnapshot,
    build_iron_condors,
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


class _DummyQuoteClient:
    def __init__(self, quotes: dict[str, LiveOptionQuote]) -> None:
        self._quotes = quotes

    def get_latest_option_quotes(
        self,
        symbols: list[str],
        *,
        feed: str,
    ) -> dict[str, LiveOptionQuote]:
        return {
            symbol: self._quotes[symbol]
            for symbol in symbols
            if symbol in self._quotes
        }


class _InMemoryExecutionStore:
    def __init__(self) -> None:
        self.positions: dict[str, dict[str, Any]] = {}
        self.closes: dict[str, list[dict[str, Any]]] = {}
        self.attempt_links: dict[str, dict[str, Any]] = {}

    def portfolio_schema_ready(self) -> bool:
        return True

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


class IronCondorLiveFlowE2ETests(unittest.TestCase):
    def test_iron_condor_scanner_scoring_execution_and_position_sync(self) -> None:
        expiration = "2026-04-24"
        candidates = build_iron_condors(
            symbol="SPY",
            spot_price=510.0,
            call_contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="SPY260424C517",
                        expiration_date=expiration,
                        strike_price=517.0,
                        open_interest=2400,
                        close_price=None,
                    ),
                    OptionContract(
                        symbol="SPY260424C522",
                        expiration_date=expiration,
                        strike_price=522.0,
                        open_interest=1800,
                        close_price=None,
                    ),
                ]
            },
            put_contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="SPY260424P498",
                        expiration_date=expiration,
                        strike_price=498.0,
                        open_interest=1850,
                        close_price=None,
                    ),
                    OptionContract(
                        symbol="SPY260424P503",
                        expiration_date=expiration,
                        strike_price=503.0,
                        open_interest=2500,
                        close_price=None,
                    ),
                ]
            },
            call_snapshots_by_expiration={
                expiration: {
                    "SPY260424C517": OptionSnapshot(
                        symbol="SPY260424C517",
                        bid=0.95,
                        ask=1.05,
                        bid_size=60,
                        ask_size=55,
                        midpoint=1.00,
                        delta=0.19,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.34,
                        last_trade_price=None,
                        daily_volume=1400,
                        greeks_source="alpaca",
                    ),
                    "SPY260424C522": OptionSnapshot(
                        symbol="SPY260424C522",
                        bid=0.35,
                        ask=0.45,
                        bid_size=50,
                        ask_size=52,
                        midpoint=0.40,
                        delta=0.08,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.31,
                        last_trade_price=None,
                        daily_volume=900,
                        greeks_source="alpaca",
                    ),
                }
            },
            put_snapshots_by_expiration={
                expiration: {
                    "SPY260424P498": OptionSnapshot(
                        symbol="SPY260424P498",
                        bid=0.35,
                        ask=0.45,
                        bid_size=52,
                        ask_size=54,
                        midpoint=0.40,
                        delta=-0.08,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.30,
                        last_trade_price=None,
                        daily_volume=950,
                        greeks_source="alpaca",
                    ),
                    "SPY260424P503": OptionSnapshot(
                        symbol="SPY260424P503",
                        bid=0.95,
                        ask=1.05,
                        bid_size=62,
                        ask_size=58,
                        midpoint=1.00,
                        delta=-0.18,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.35,
                        last_trade_price=None,
                        daily_volume=1500,
                        greeks_source="alpaca",
                    ),
                }
            },
            expected_moves_by_expiration={
                expiration: ExpectedMoveEstimate(
                    expiration_date=expiration,
                    amount=6.0,
                    percent_of_spot=6.0 / 510.0,
                    reference_strike=510.0,
                )
            },
            args=_args(),
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.strategy, "iron_condor")
        self.assertEqual(candidate.short_symbol, "SPY260424P503")
        self.assertEqual(candidate.long_symbol, "SPY260424P498")
        self.assertEqual(candidate.secondary_short_symbol, "SPY260424C517")
        self.assertEqual(candidate.secondary_long_symbol, "SPY260424C522")
        self.assertAlmostEqual(candidate.midpoint_credit, 1.2, places=4)
        self.assertAlmostEqual(candidate.natural_credit, 1.0, places=4)
        self.assertAlmostEqual(candidate.max_profit, 120.0, places=4)
        self.assertAlmostEqual(candidate.max_loss, 380.0, places=4)
        self.assertAlmostEqual(candidate.return_on_risk, 1.2 / 3.8, places=4)
        self.assertEqual(len(candidate.order_payload["legs"]), 4)

        candidate_payload = asdict(candidate)
        candidate_payload.update(
            {
                "quality_score": 91.0,
                "setup_score": 63.0,
                "setup_intraday_score": 61.0,
                "setup_status": "neutral",
                "data_status": "clean",
                "calendar_status": "clean",
                "earnings_phase": "post_event_fresh",
                "earnings_timing_confidence": "high",
                "earnings_event_date": "2026-04-21",
                "setup_spot_vs_vwap_pct": 0.0004,
                "setup_intraday_return_pct": 0.0007,
                "setup_distance_to_session_extreme_pct": 0.011,
                "setup_opening_range_break_pct": 0.0002,
                "setup_latest_close": 510.2,
                "setup_opening_range_high": 510.9,
                "setup_opening_range_low": 509.8,
                "dominant_flow": "mixed",
                "post_event_confirmation_signal": 0.76,
                "post_event_confirmation_signal_subsignal_count": 3,
                "pricing_signal": 0.66,
                "pricing_signal_subsignal_count": 1,
                "neutral_regime_signal": 0.72,
                "residual_iv_richness": 0.65,
            }
        )
        scorecard = build_candidate_opportunity_score(candidate_payload)
        self.assertEqual(scorecard["strategy_family"], "iron_condor")
        self.assertTrue(scorecard["signal_gate"]["active"])
        self.assertTrue(scorecard["signal_gate"]["eligible"])

        live_quotes = {
            "SPY260424P503": LiveOptionQuote(
                symbol="SPY260424P503",
                bid=0.90,
                ask=1.00,
                bid_size=60,
                ask_size=60,
                timestamp="2026-04-14T15:00:00Z",
            ),
            "SPY260424P498": LiveOptionQuote(
                symbol="SPY260424P498",
                bid=0.30,
                ask=0.40,
                bid_size=55,
                ask_size=55,
                timestamp="2026-04-14T15:00:00Z",
            ),
            "SPY260424C517": LiveOptionQuote(
                symbol="SPY260424C517",
                bid=0.92,
                ask=1.02,
                bid_size=58,
                ask_size=58,
                timestamp="2026-04-14T15:00:00Z",
            ),
            "SPY260424C522": LiveOptionQuote(
                symbol="SPY260424C522",
                bid=0.28,
                ask=0.38,
                bid_size=52,
                ask_size=52,
                timestamp="2026-04-14T15:00:00Z",
            ),
        }
        quality_check = _validate_live_deployment_quality(
            candidate_payload=candidate_payload,
            client=_DummyQuoteClient(live_quotes),
        )
        self.assertTrue(quality_check["ok"])
        self.assertGreater(float(quality_check["live_quote"]["close_mark"]), 0.0)

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
            client_order_id="test-iron-condor-open",
        )
        self.assertEqual(resolved_quantity, 1)
        self.assertEqual(order_request["limit_price"], "-1.17")
        self.assertEqual(resolved_limit_price, 1.17)
        self.assertEqual(len(order_request["legs"]), 4)

        store = _InMemoryExecutionStore()
        open_attempt = {
            "execution_attempt_id": "attempt-open",
            "session_date": "2026-04-14",
            "market_date": "2026-04-14",
            "label": "explore_10_iron_condor_weekly_auto",
            "pipeline_id": "pipeline:explore_10_iron_condor_weekly_auto",
            "underlying_symbol": "SPY",
            "strategy": "iron_condor",
            "strategy_family": "iron_condor",
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
                    "broker_order_id": "broker-open",
                    "filled_qty": 1,
                    "filled_avg_price": -1.20,
                }
            ],
            "fills": [
                {"symbol": "SPY260424P503", "price": 1.05, "quantity": 1},
                {"symbol": "SPY260424P498", "price": 0.45, "quantity": 1},
                {"symbol": "SPY260424C517", "price": 1.00, "quantity": 1},
                {"symbol": "SPY260424C522", "price": 0.40, "quantity": 1},
            ],
        }
        position = sync_session_position_from_attempt(
            execution_store=store,
            attempt=open_attempt,
        )
        self.assertIsNotNone(position)
        assert position is not None
        self.assertEqual(position["status"], "open")
        self.assertEqual(len(position["legs"]), 4)
        self.assertAlmostEqual(position["entry_value"], 1.2, places=4)
        self.assertAlmostEqual(position["economics"]["max_loss"], 380.0, places=2)

        close_request, close_quantity, close_limit_price = _build_close_order_request(
            position={
                "strategy": "iron_condor",
                "legs": order_request["legs"],
                "remaining_quantity": 1,
                "close_mark": 0.85,
            },
            quantity=1,
            limit_price=None,
            client_order_id="test-iron-condor-close",
        )
        self.assertEqual(close_quantity, 1)
        self.assertEqual(close_limit_price, 0.85)
        self.assertEqual(close_request["limit_price"], "0.85")
        self.assertEqual(len(close_request["legs"]), 4)

        closed = sync_session_position_from_attempt(
            execution_store=store,
            attempt={
                "execution_attempt_id": "attempt-close",
                "position_id": position["position_id"],
                "quantity": 1,
                "status": "filled",
                "requested_at": "2026-04-14T15:30:00Z",
                "submitted_at": "2026-04-14T15:30:01Z",
                "completed_at": "2026-04-14T15:30:05Z",
                "request": {"trade_intent": "close", "order": close_request},
                "orders": [
                    {
                        "broker_order_id": "broker-close",
                        "filled_qty": 1,
                        "filled_avg_price": 0.85,
                    }
                ],
                "fills": [],
            },
        )
        self.assertEqual(closed["status"], "closed")
        self.assertAlmostEqual(closed["realized_pnl"], 35.0, places=2)
        self.assertAlmostEqual(closed["remaining_quantity"], 0.0, places=2)


if __name__ == "__main__":
    unittest.main()
