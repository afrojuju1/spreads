from __future__ import annotations

import unittest
from argparse import Namespace
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from core.domain.models import (
    ExpectedMoveEstimate,
    LiveOptionQuote,
    OptionContract,
    OptionSnapshot,
)
from core.services.automation_runtime import (
    resolve_entry_runtime,
    resolve_management_runtime,
)
from core.services.entry_recipes import evaluate_entry_recipes
from core.services.execution import (
    _build_close_order_request,
    _build_order_request,
    _validate_live_deployment_quality,
    normalize_execution_policy,
)
from core.services.management_planner import plan_position_management
from core.services.opportunity_scoring import build_candidate_opportunity_score
from core.services.scanners.builders.verticals import build_vertical_spreads
from core.services.session_positions import sync_session_position_from_attempt


def _args() -> Namespace:
    return Namespace(
        profile="weekly",
        min_open_interest=100,
        max_relative_spread=0.25,
        short_delta_min=0.10,
        short_delta_max=0.28,
        short_delta_target=0.18,
        min_width=1.0,
        max_width=10.0,
        min_credit=0.10,
        min_return_on_risk=0.13,
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


class CallCreditLiveFlowE2ETests(unittest.TestCase):
    def test_tactical_delta_fit_uses_strategy_short_delta_target(self) -> None:
        candidate_payload = {
            "underlying_symbol": "SPY",
            "strategy": "put_credit",
            "profile": "weekly",
            "days_to_expiration": 7,
            "quality_score": 65.0,
            "setup_score": 72.0,
            "setup_status": "favorable",
            "data_status": "clean",
            "calendar_status": "clean",
            "fill_ratio": 0.92,
            "short_delta": -0.23,
            "expected_move": 10.0,
            "short_vs_expected_move": 0.8,
            "earnings_phase": "clean",
        }

        default_scorecard = build_candidate_opportunity_score(candidate_payload)
        aligned_scorecard = build_candidate_opportunity_score(
            {
                **candidate_payload,
                "short_delta_target": 0.23,
            }
        )

        self.assertNotIn(
            "tactical_delta_fit_delta",
            default_scorecard["profile_score_components"],
        )
        self.assertAlmostEqual(
            aligned_scorecard["profile_score_components"]["tactical_delta_fit_delta"],
            1.5,
        )
        self.assertAlmostEqual(
            aligned_scorecard["profile_score_evidence"]["delta_fit_target"],
            0.23,
        )
        self.assertGreater(
            aligned_scorecard["promotion_score"],
            default_scorecard["promotion_score"],
        )

    def test_tactical_put_credit_penalizes_negative_economics(self) -> None:
        candidate_payload = {
            "underlying_symbol": "SPY",
            "strategy": "put_credit",
            "profile": "weekly",
            "days_to_expiration": 7,
            "quality_score": 57.4,
            "setup_score": 69.0,
            "setup_status": "favorable",
            "data_status": "clean",
            "calendar_status": "clean",
            "fill_ratio": 0.9,
            "short_delta": -0.22,
            "short_delta_target": 0.22,
            "expected_move": 10.0,
            "short_vs_expected_move": 0.8,
            "earnings_phase": "clean",
        }

        healthy_scorecard = build_candidate_opportunity_score(
            {
                **candidate_payload,
                "expected_value_dollars": 12.0,
                "slippage_adjusted_expected_value_dollars": 8.0,
            }
        )
        stressed_scorecard = build_candidate_opportunity_score(
            {
                **candidate_payload,
                "expected_value_dollars": -22.32,
                "slippage_adjusted_expected_value_dollars": -25.32,
            }
        )

        self.assertEqual(healthy_scorecard["state"], "monitor")
        self.assertEqual(stressed_scorecard["state"], "discarded")
        self.assertGreater(
            healthy_scorecard["promotion_score"],
            stressed_scorecard["promotion_score"],
        )
        self.assertIn(
            "tactical_expected_value_delta",
            healthy_scorecard["profile_score_components"],
        )
        self.assertIn(
            "tactical_slippage_adjusted_ev_delta",
            healthy_scorecard["profile_score_components"],
        )
        self.assertIn(
            "tactical_expected_value_penalty",
            stressed_scorecard["profile_score_components"],
        )
        self.assertIn(
            "tactical_slippage_adjusted_ev_penalty",
            stressed_scorecard["profile_score_components"],
        )

    def test_call_credit_scanner_scoring_execution_management_and_position_sync(
        self,
    ) -> None:
        entry_runtime = resolve_entry_runtime(
            bot_id="short_dated_index_call_credit_bot",
            automation_id="index_call_credit_entry",
        )
        management_runtime = resolve_management_runtime(
            bot_id="short_dated_index_call_credit_bot",
            automation_id="index_call_credit_manage",
        )
        self.assertEqual(entry_runtime.entry_recipe_refs, ("trend_resistance",))
        self.assertEqual(
            management_runtime.management_recipe_refs,
            ("take_profit_50pct", "max_loss_2x_credit", "expiry_day_exit"),
        )
        self.assertEqual(entry_runtime.build_settings.width_points, (2.0, 3.0, 5.0))
        self.assertEqual(entry_runtime.build_settings.min_return_on_risk, 0.13)

        expiration = "2026-04-24"
        candidates = build_vertical_spreads(
            symbol="SPY",
            strategy="call_credit",
            spot_price=100.0,
            contracts_by_expiration={
                expiration: [
                    OptionContract(
                        symbol="SPY260424C104",
                        expiration_date=expiration,
                        strike_price=104.0,
                        open_interest=2500,
                        close_price=None,
                    ),
                    OptionContract(
                        symbol="SPY260424C109",
                        expiration_date=expiration,
                        strike_price=109.0,
                        open_interest=2200,
                        close_price=None,
                    ),
                ]
            },
            snapshots_by_expiration={
                expiration: {
                    "SPY260424C104": OptionSnapshot(
                        symbol="SPY260424C104",
                        bid=1.25,
                        ask=1.35,
                        bid_size=75,
                        ask_size=70,
                        midpoint=1.30,
                        delta=0.24,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.28,
                        last_trade_price=None,
                        daily_volume=1500,
                        greeks_source="alpaca",
                    ),
                    "SPY260424C109": OptionSnapshot(
                        symbol="SPY260424C109",
                        bid=0.25,
                        ask=0.31,
                        bid_size=60,
                        ask_size=65,
                        midpoint=0.28,
                        delta=0.06,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.26,
                        last_trade_price=None,
                        daily_volume=1200,
                        greeks_source="alpaca",
                    ),
                }
            },
            expected_moves_by_expiration={
                expiration: ExpectedMoveEstimate(
                    expiration_date=expiration,
                    amount=3.0,
                    percent_of_spot=0.03,
                    reference_strike=100.0,
                )
            },
            args=_args(),
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.strategy, "call_credit")
        self.assertEqual(candidate.short_symbol, "SPY260424C104")
        self.assertEqual(candidate.long_symbol, "SPY260424C109")
        self.assertAlmostEqual(candidate.width, 5.0, places=4)
        self.assertAlmostEqual(candidate.midpoint_credit, 1.02, places=4)
        self.assertAlmostEqual(candidate.natural_credit, 0.94, places=4)
        self.assertAlmostEqual(candidate.fill_ratio, 0.94 / 1.02, places=4)
        self.assertAlmostEqual(candidate.max_profit, 102.0, places=4)
        self.assertAlmostEqual(candidate.max_loss, 398.0, places=4)
        self.assertAlmostEqual(candidate.return_on_risk, 1.02 / 3.98, places=4)
        self.assertEqual(candidate.order_payload["limit_price"], "-1.02")

        candidate_payload = asdict(candidate)
        candidate_payload.update(
            {
                "quality_score": 91.0,
                "setup_score": 84.0,
                "setup_intraday_score": 82.0,
                "setup_status": "favorable",
                "data_status": "clean",
                "calendar_status": "clean",
                "earnings_phase": "clean",
                "earnings_timing_confidence": "high",
                "options_bias_alignment": True,
                "dominant_flow": "mixed",
            }
        )

        scorecard = build_candidate_opportunity_score(candidate_payload)
        self.assertEqual(scorecard["strategy_family"], "call_credit_spread")
        self.assertEqual(scorecard["promotion_floor"], 72.0)
        self.assertEqual(scorecard["state"], "promotable")
        self.assertGreaterEqual(
            scorecard["promotion_score"],
            entry_runtime.trigger_policy["min_opportunity_score"],
        )
        self.assertFalse(scorecard["signal_gate"]["active"])
        self.assertTrue(scorecard["signal_gate"]["eligible"])

        entry_recipe_result = evaluate_entry_recipes(
            candidate_payload,
            entry_runtime.entry_recipe_refs,
        )
        self.assertTrue(entry_recipe_result.passed)

        live_quotes = {
            "SPY260424C104": LiveOptionQuote(
                symbol="SPY260424C104",
                bid=1.20,
                ask=1.30,
                bid_size=70,
                ask_size=70,
                timestamp="2026-04-14T15:00:00Z",
            ),
            "SPY260424C109": LiveOptionQuote(
                symbol="SPY260424C109",
                bid=0.20,
                ask=0.30,
                bid_size=60,
                ask_size=60,
                timestamp="2026-04-14T15:00:00Z",
            ),
        }
        quality_check = _validate_live_deployment_quality(
            candidate_payload=candidate_payload,
            client=_DummyQuoteClient(live_quotes),
        )
        self.assertTrue(quality_check["ok"])
        self.assertGreaterEqual(
            float(quality_check["live_quote"]["live_return_on_risk"]),
            entry_runtime.build_settings.min_return_on_risk or 0.0,
        )

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
            client_order_id="test-call-credit-open",
        )
        self.assertEqual(resolved_quantity, 1)
        self.assertEqual(resolved_limit_price, 1.01)
        self.assertEqual(order_request["limit_price"], "-1.01")
        self.assertEqual(order_request["legs"][0]["position_intent"], "sell_to_open")
        self.assertEqual(order_request["legs"][1]["position_intent"], "buy_to_open")

        store = _InMemoryExecutionStore()
        open_attempt = {
            "execution_attempt_id": "attempt-call-credit-open",
            "session_date": "2026-04-14",
            "market_date": "2026-04-14",
            "label": "explore_10_call_credit_weekly_auto",
            "underlying_symbol": "SPY",
            "strategy": "call_credit",
            "strategy_family": "call_credit_spread",
            "expiration_date": expiration,
            "quantity": 1,
            "status": "filled",
            "requested_at": "2026-04-14T15:00:00Z",
            "submitted_at": "2026-04-14T15:00:01Z",
            "completed_at": "2026-04-14T15:00:05Z",
            "request": {
                "trade_intent": "open",
                "bot_id": entry_runtime.bot_id,
                "automation_id": entry_runtime.automation_id,
                "strategy_config_id": entry_runtime.strategy_config_id,
                "strategy_id": entry_runtime.strategy_id,
                "config_hash": entry_runtime.config_hash,
                "execution_intent_id": "intent-call-credit-open-1",
                "order": order_request,
            },
            "candidate": candidate_payload,
            "orders": [
                {
                    "broker_order_id": "broker-call-credit-open",
                    "filled_qty": 1,
                    "filled_avg_price": -1.02,
                }
            ],
            "fills": [
                {"symbol": "SPY260424C104", "price": 1.30, "quantity": 1},
                {"symbol": "SPY260424C109", "price": 0.28, "quantity": 1},
            ],
        }
        position = sync_session_position_from_attempt(
            execution_store=store,
            attempt=open_attempt,
        )
        self.assertIsNotNone(position)
        assert position is not None
        self.assertEqual(position["status"], "open")
        self.assertEqual(position["bot_id"], entry_runtime.bot_id)
        self.assertEqual(position["automation_id"], entry_runtime.automation_id)
        self.assertEqual(position["strategy_config_id"], entry_runtime.strategy_config_id)
        self.assertEqual(position["opening_execution_intent_id"], "intent-call-credit-open-1")
        self.assertEqual(len(position["legs"]), 2)
        self.assertAlmostEqual(position["entry_value"], 1.02, places=4)
        self.assertAlmostEqual(position["economics"]["max_loss"], 398.0, places=2)

        management_decision = plan_position_management(
            runtime=management_runtime,
            position={
                **position,
                "close_mark": 0.50,
                "close_marked_at": "2026-04-14T15:29:00Z",
            },
            flatten_due=False,
            now=datetime(2026, 4, 14, 15, 30, tzinfo=UTC),
        )
        self.assertTrue(management_decision["should_close"])
        self.assertEqual(management_decision["reason"], "profit_target")
        self.assertEqual(management_decision["recipe_ref"], "take_profit_50pct")
        self.assertEqual(management_decision["limit_price"], 0.5)

        close_request, close_quantity, close_limit_price = _build_close_order_request(
            position={
                "strategy": candidate.strategy,
                "legs": order_request["legs"],
                "remaining_quantity": 1,
                "close_mark": management_decision["limit_price"],
            },
            quantity=1,
            limit_price=None,
            client_order_id="test-call-credit-close",
        )
        self.assertEqual(close_quantity, 1)
        self.assertEqual(close_limit_price, 0.5)
        self.assertEqual(close_request["limit_price"], "0.50")
        self.assertEqual(close_request["legs"][0]["position_intent"], "buy_to_close")
        self.assertEqual(close_request["legs"][1]["position_intent"], "sell_to_close")

        closed = sync_session_position_from_attempt(
            execution_store=store,
            attempt={
                "execution_attempt_id": "attempt-call-credit-close",
                "position_id": position["position_id"],
                "quantity": 1,
                "status": "filled",
                "requested_at": "2026-04-14T15:30:00Z",
                "submitted_at": "2026-04-14T15:30:01Z",
                "completed_at": "2026-04-14T15:30:05Z",
                "request": {"trade_intent": "close", "order": close_request},
                "orders": [
                    {
                        "broker_order_id": "broker-call-credit-close",
                        "filled_qty": 1,
                        "filled_avg_price": 0.47,
                    }
                ],
                "fills": [],
            },
        )
        self.assertEqual(closed["status"], "closed")
        self.assertAlmostEqual(closed["realized_pnl"], 55.0, places=2)
        self.assertAlmostEqual(closed["remaining_quantity"], 0.0, places=2)
        self.assertEqual(
            len(store.list_position_closes(position_id=position["position_id"])),
            1,
        )

    def test_open_position_sync_defaults_policy_fields_without_existing_metadata(
        self,
    ) -> None:
        store = _InMemoryExecutionStore()
        position = sync_session_position_from_attempt(
            execution_store=store,
            attempt={
                "execution_attempt_id": "attempt-call-credit-minimal-open",
                "session_date": "2026-04-14",
                "market_date": "2026-04-14",
                "label": "explore_10_call_credit_weekly_auto",
                "underlying_symbol": "SPY",
                "strategy": "call_credit",
                "strategy_family": "call_credit_spread",
                "expiration_date": "2026-04-24",
                "quantity": 1,
                "status": "filled",
                "request": {
                    "trade_intent": "open",
                    "order": {
                        "order_class": "mleg",
                        "qty": "1",
                        "type": "limit",
                        "limit_price": "-1.00",
                        "time_in_force": "day",
                        "legs": [
                            {
                                "symbol": "SPY260424C104",
                                "ratio_qty": "1",
                                "side": "sell",
                                "position_intent": "sell_to_open",
                            },
                            {
                                "symbol": "SPY260424C109",
                                "ratio_qty": "1",
                                "side": "buy",
                                "position_intent": "buy_to_open",
                            },
                        ],
                    },
                },
                "candidate": {
                    "width": 5.0,
                    "max_profit": 100.0,
                    "max_loss": 400.0,
                },
                "orders": [
                    {
                        "broker_order_id": "broker-call-credit-minimal-open",
                        "filled_qty": 1,
                        "filled_avg_price": -1.0,
                    }
                ],
            },
        )

        self.assertIsNotNone(position)
        assert position is not None
        self.assertEqual(position["style_profile"], "active")
        self.assertEqual(position["horizon_intent"], "short_dated")
        self.assertEqual(position["product_class"], "index_etf_options")


if __name__ == "__main__":
    unittest.main()
