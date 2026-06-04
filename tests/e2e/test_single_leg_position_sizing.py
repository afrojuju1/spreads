from __future__ import annotations

from datetime import UTC, datetime
import unittest
from unittest.mock import patch

from core.integrations.alpaca.client import AlpacaRequestError
from core.integrations.alpaca.errors import classify_alpaca_request_error
from core.services.execution import (
    _resolve_open_submission_quantity,
    normalize_execution_policy,
    run_execution_submit,
)
from core.services.risk_manager import (
    build_execution_admission_snapshot,
    build_candidate_position_sizing,
    evaluate_open_execution,
)


class SingleLegPositionSizingTests(unittest.TestCase):
    def test_normalize_execution_policy_marks_default_quantity_as_unconfigured(
        self,
    ) -> None:
        default_policy = normalize_execution_policy({"mode": "top_promotable"})
        explicit_policy = normalize_execution_policy(
            {"mode": "top_promotable", "quantity": 2}
        )

        self.assertFalse(default_policy["quantity_configured"])
        self.assertEqual(default_policy["quantity"], 1)
        self.assertTrue(explicit_policy["quantity_configured"])
        self.assertEqual(explicit_policy["quantity"], 2)

    def test_candidate_position_sizing_uses_trade_risk_budget_for_naked_shorts(
        self,
    ) -> None:
        sizing = build_candidate_position_sizing(
            candidate={
                "strategy": "short_call",
                "midpoint_credit": 2.5,
                "max_loss": 180.0,
            },
            limit_price=2.5,
            strategy_risk_budget=500.0,
        )

        self.assertTrue(sizing["applies"])
        self.assertEqual(sizing["recommended_quantity"], 2)
        self.assertEqual(sizing["recommended_max_loss"], 360.0)
        self.assertEqual(sizing["limiting_constraint"], "max_risk_per_trade")

    def test_live_submission_quantity_uses_strategy_budget_when_policy_default_is_one(
        self,
    ) -> None:
        class _ExecutionStore:
            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_session_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

            def list_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

        class _BrokerClient:
            def get_account(self) -> dict[str, object]:
                return {
                    "options_buying_power": "500000",
                }

            def list_positions(self) -> list[dict[str, object]]:
                return []

        with patch(
            "core.services.risk_manager.create_alpaca_client_from_env",
            return_value=_BrokerClient(),
        ):
            quantity, strategy_risk_budget = _resolve_open_submission_quantity(
                execution_store=_ExecutionStore(),
                session_id="live:test:2026-04-21",
                candidate={
                    "underlying_symbol": "SPY",
                    "strategy": "short_put",
                    "candidate": {
                        "midpoint_credit": 2.5,
                        "max_loss": 180.0,
                        "order_payload": {
                            "qty": "1",
                        },
                    },
                },
                explicit_quantity=None,
                limit_price=2.5,
                request_metadata={
                    "bot_id": "short_dated_etf_short_put_bot",
                    "automation_id": "etf_short_put_entry",
                    "strategy_config_id": "short_dated_etf_short_put",
                },
                risk_policy={"enabled": True, "allow_live": True},
                execution_policy=normalize_execution_policy({"mode": "top_promotable"}),
                bot_id="short_dated_etf_short_put_bot",
                automation_id="etf_short_put_entry",
                strategy_config_id="short_dated_etf_short_put",
            )

        self.assertEqual(quantity, 2)
        self.assertEqual(strategy_risk_budget, 500.0)

    def test_live_submission_quantity_caps_short_put_to_broker_buying_power(
        self,
    ) -> None:
        class _ExecutionStore:
            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_session_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

            def list_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

        class _BrokerClient:
            def get_account(self) -> dict[str, object]:
                return {
                    "options_buying_power": "75000",
                }

            def list_positions(self) -> list[dict[str, object]]:
                return []

        with patch(
            "core.services.risk_manager.create_alpaca_client_from_env",
            return_value=_BrokerClient(),
        ):
            quantity, strategy_risk_budget = _resolve_open_submission_quantity(
                execution_store=_ExecutionStore(),
                session_id="live:test:2026-04-21",
                candidate={
                    "underlying_symbol": "SPY",
                    "strategy": "short_put",
                    "candidate": {
                        "midpoint_credit": 2.5,
                        "legs": [
                            {
                                "symbol": "SPY260427P00500000",
                                "role": "short",
                                "expiration_date": "2026-04-27",
                            }
                        ],
                        "order_payload": {
                            "qty": "1",
                        },
                    },
                },
                explicit_quantity=None,
                limit_price=2.5,
                request_metadata={
                    "bot_id": "short_dated_etf_short_put_bot",
                    "automation_id": "etf_short_put_entry",
                    "strategy_config_id": "short_dated_etf_short_put",
                },
                risk_policy={"enabled": True, "allow_live": True},
                execution_policy=normalize_execution_policy({"mode": "top_promotable"}),
                bot_id="short_dated_etf_short_put_bot",
                automation_id="etf_short_put_entry",
                strategy_config_id="short_dated_etf_short_put",
            )

        self.assertEqual(quantity, 1)
        self.assertEqual(strategy_risk_budget, 500.0)

    def test_execution_admission_snapshot_reports_account_block_before_submit(
        self,
    ) -> None:
        class _ExecutionStore:
            def list_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

        class _BrokerClient:
            def get_account(self) -> dict[str, object]:
                return {
                    "options_buying_power": "40000",
                }

        with patch(
            "core.services.risk_manager.create_alpaca_client_from_env",
            return_value=_BrokerClient(),
        ):
            snapshot = build_execution_admission_snapshot(
                execution_store=_ExecutionStore(),
                candidate={
                    "underlying_symbol": "SPY",
                    "strategy": "short_put",
                    "candidate": {
                        "midpoint_credit": 2.5,
                        "legs": [
                            {
                                "symbol": "SPY260427P00500000",
                                "role": "short",
                                "position_intent": "sell_to_open",
                                "strike": 500.0,
                                "option_type": "put",
                            }
                        ],
                    },
                },
                limit_price=2.5,
                strategy_risk_budget=500.0,
            )

        self.assertEqual(snapshot["status"], "blocked")
        self.assertEqual(snapshot["reason"], "insufficient_broker_buying_power")
        self.assertEqual(snapshot["admissible_quantity"], 0)
        self.assertEqual(snapshot["required_buying_power"], 50000.0)
        self.assertEqual(snapshot["available_buying_power"], 40000.0)

    def test_execution_admission_snapshot_reports_unknown_when_broker_capacity_is_unavailable(
        self,
    ) -> None:
        class _ExecutionStore:
            def list_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

        with patch(
            "core.services.risk_manager.create_alpaca_client_from_env",
            side_effect=RuntimeError("missing alpaca creds"),
        ):
            snapshot = build_execution_admission_snapshot(
                execution_store=_ExecutionStore(),
                candidate={
                    "underlying_symbol": "SPY",
                    "strategy": "short_put",
                    "candidate": {
                        "midpoint_credit": 2.5,
                        "legs": [
                            {
                                "symbol": "SPY260427P00500000",
                                "role": "short",
                                "position_intent": "sell_to_open",
                                "strike": 500.0,
                                "option_type": "put",
                            }
                        ],
                    },
                },
                limit_price=2.5,
                strategy_risk_budget=500.0,
            )

        self.assertEqual(snapshot["status"], "unknown")
        self.assertEqual(snapshot["reason"], "broker_buying_power_unavailable")
        self.assertIsNone(snapshot["admissible_quantity"])

    def test_evaluate_open_execution_blocks_short_single_leg_above_trade_budget(
        self,
    ) -> None:
        class _ExecutionStore:
            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_session_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

            def list_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

        class _BrokerClient:
            def get_account(self) -> dict[str, object]:
                return {
                    "options_buying_power": "500000",
                }

            def list_positions(self) -> list[dict[str, object]]:
                return []

        with patch(
            "core.services.risk_manager._environment_reason",
            return_value=None,
        ), patch(
            "core.services.risk_manager.create_alpaca_client_from_env",
            return_value=_BrokerClient(),
        ):
            decision = evaluate_open_execution(
                execution_store=_ExecutionStore(),
                session_id="live:test:2026-04-21",
                candidate={
                    "underlying_symbol": "SPY",
                    "strategy": "short_put",
                    "candidate": {
                        "midpoint_credit": 2.5,
                        "max_loss": 180.0,
                    },
                },
                cycle={
                    "generated_at": datetime.now(UTC).isoformat().replace(
                        "+00:00",
                        "Z",
                    )
                },
                quantity=3,
                limit_price=2.5,
                risk_policy={
                    "enabled": True,
                    "allow_live": True,
                    "stale_quote_after_seconds": 86400,
                },
                strategy_risk_budget=500.0,
            )

        self.assertEqual(decision["status"], "blocked")
        self.assertIn(
            "strategy_risk_budget_exceeded",
            decision["reason_codes"],
        )
        self.assertEqual(decision["metrics"]["recommended_quantity"], 2)

    def test_evaluate_open_execution_blocks_explicit_quantity_above_broker_buying_power(
        self,
    ) -> None:
        class _ExecutionStore:
            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_session_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

            def list_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

        class _BrokerClient:
            def get_account(self) -> dict[str, object]:
                return {
                    "options_buying_power": "75000",
                }

            def list_positions(self) -> list[dict[str, object]]:
                return []

        with (
            patch(
                "core.services.risk_manager._environment_reason",
                return_value=None,
            ),
            patch(
                "core.services.risk_manager.create_alpaca_client_from_env",
                return_value=_BrokerClient(),
            ),
        ):
            decision = evaluate_open_execution(
                execution_store=_ExecutionStore(),
                session_id="live:test:2026-04-21",
                candidate={
                    "underlying_symbol": "SPY",
                    "strategy": "short_put",
                    "candidate": {
                        "midpoint_credit": 2.5,
                        "legs": [
                            {
                                "symbol": "SPY260427P00500000",
                                "role": "short",
                                "expiration_date": "2026-04-27",
                            }
                        ],
                    },
                },
                cycle={
                    "generated_at": datetime.now(UTC).isoformat().replace(
                        "+00:00",
                        "Z",
                    )
                },
                quantity=2,
                limit_price=2.5,
                risk_policy={
                    "enabled": True,
                    "allow_live": True,
                    "stale_quote_after_seconds": 86400,
                },
                strategy_risk_budget=500000.0,
            )

        self.assertEqual(decision["status"], "blocked")
        self.assertIn(
            "insufficient_broker_buying_power",
            decision["reason_codes"],
        )
        self.assertEqual(decision["metrics"]["recommended_quantity"], 1)

    def test_run_execution_submit_blocks_before_broker_submit_on_low_buying_power(
        self,
    ) -> None:
        class _ExecutionStore:
            def __init__(self) -> None:
                self.intent = {
                    "execution_intent_id": "intent-1",
                    "trading_strategy_id": "short_dated_etf_short_put",
                    "trade_decision_id": None,
                    "strategy_position_id": None,
                    "execution_attempt_id": "attempt-1",
                    "action_type": "open",
                    "slot_key": "entry:test",
                    "claim_token": None,
                    "policy_ref": {"strategy_id": "short_put"},
                    "config_hash": "cfg-1",
                    "state": "claimed",
                    "expires_at": None,
                    "superseded_by_id": None,
                    "payload": {},
                    "created_at": "2026-04-21T15:00:00Z",
                    "updated_at": "2026-04-21T15:00:00Z",
                }
                self.intent_events: list[dict[str, object]] = []
                self.attempt = {
                    "execution_attempt_id": "attempt-1",
                    "session_id": "live:test:2026-04-21",
                    "session_date": "2026-04-21",
                    "underlying_symbol": "SPY",
                    "strategy": "short_put",
                    "trade_intent": "open",
                    "status": "pending_submission",
                    "quantity": 2,
                    "limit_price": 2.5,
                    "requested_at": "2026-04-21T15:00:00Z",
                    "position_id": None,
                    "candidate": {
                        "strategy": "short_put",
                        "midpoint_credit": 2.5,
                        "legs": [
                            {
                                "symbol": "SPY260427P00500000",
                                "role": "short",
                                "expiration_date": "2026-04-27",
                            }
                        ],
                    },
                    "request": {
                        "execution_intent_id": "intent-1",
                        "order": {
                            "symbol": "SPY260427P00500000",
                            "side": "sell",
                            "position_intent": "sell_to_open",
                            "qty": "2",
                            "type": "limit",
                            "limit_price": "2.50",
                            "time_in_force": "day",
                        },
                        "execution_policy": {},
                        "exit_policy": {},
                    },
                }

            def schema_ready(self) -> bool:
                return True

            def get_attempt(self, execution_attempt_id: str) -> dict[str, object] | None:
                if execution_attempt_id != "attempt-1":
                    return None
                return dict(self.attempt)

            def update_attempt(
                self,
                *,
                execution_attempt_id: str,
                **changes: object,
            ) -> None:
                self.attempt.update(changes)

            def get_execution_intent(
                self,
                execution_intent_id: str,
            ) -> dict[str, object] | None:
                if execution_intent_id != "intent-1":
                    return None
                return dict(self.intent)

            def upsert_execution_intent(self, **payload: object) -> dict[str, object]:
                self.intent.update(payload)
                return dict(self.intent)

            def append_execution_intent_event(self, **payload: object) -> None:
                self.intent_events.append(dict(payload))

            def list_orders(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_fills(self, **_: object) -> list[dict[str, object]]:
                return []

            def intent_schema_ready(self) -> bool:
                return True

            def list_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return [dict(self.attempt)]

        class _Storage:
            def __init__(self) -> None:
                self.execution = _ExecutionStore()

        class _BrokerClient:
            def __init__(self) -> None:
                self.submitted = False

            def get_account(self) -> dict[str, object]:
                return {
                    "options_buying_power": "75000",
                }

            def submit_order(self, payload: dict[str, object]) -> dict[str, object]:
                self.submitted = True
                raise AssertionError("submit_order should not be called")

        broker_client = _BrokerClient()
        storage = _Storage()
        with (
            patch(
                "core.services.execution._validate_live_deployment_quality",
                return_value={"ok": True},
            ),
            patch(
                "core.services.execution.create_alpaca_client_from_env",
                return_value=broker_client,
            ),
        ):
            result = run_execution_submit(
                db_target="test",
                execution_attempt_id="attempt-1",
                storage=storage,
            )

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "insufficient_broker_buying_power")
        self.assertFalse(broker_client.submitted)
        self.assertEqual(storage.execution.attempt["status"], "failed")
        self.assertEqual(
            storage.execution.intent["payload"]["execution_admission"]["status"],
            "blocked",
        )
        self.assertEqual(
            storage.execution.intent["payload"]["execution_admission"]["reason"],
            "insufficient_broker_buying_power",
        )

    def test_classify_alpaca_request_error_marks_buying_power_rejections_terminal(
        self,
    ) -> None:
        error = AlpacaRequestError(
            "forbidden",
            status_code=403,
            url="https://paper-api.alpaca.markets/v2/orders",
            response_body=(
                '{"message":"insufficient options buying power '
                '(required: 191124, available: 8734.72)"}'
            ),
        )

        classified = classify_alpaca_request_error(error)

        self.assertEqual(classified["reason"], "insufficient_options_buying_power")
        self.assertTrue(classified["terminal"])

    def test_run_execution_submit_updates_intent_on_terminal_broker_rejection(
        self,
    ) -> None:
        class _ExecutionStore:
            def __init__(self) -> None:
                self.intent = {
                    "execution_intent_id": "intent-1",
                    "trading_strategy_id": "short_dated_etf_short_put",
                    "trade_decision_id": None,
                    "strategy_position_id": None,
                    "execution_attempt_id": "attempt-1",
                    "action_type": "open",
                    "slot_key": "entry:test",
                    "claim_token": None,
                    "policy_ref": {"strategy_id": "short_put"},
                    "config_hash": "cfg-1",
                    "state": "claimed",
                    "expires_at": None,
                    "superseded_by_id": None,
                    "payload": {},
                    "created_at": "2026-04-21T15:00:00Z",
                    "updated_at": "2026-04-21T15:00:00Z",
                }
                self.attempt = {
                    "execution_attempt_id": "attempt-1",
                    "session_id": "live:test:2026-04-21",
                    "session_date": "2026-04-21",
                    "underlying_symbol": "SPY",
                    "strategy": "short_put",
                    "trade_intent": "open",
                    "status": "pending_submission",
                    "quantity": 1,
                    "limit_price": 2.5,
                    "requested_at": "2026-04-21T15:00:00Z",
                    "position_id": None,
                    "candidate": {
                        "strategy": "short_put",
                        "midpoint_credit": 2.5,
                        "legs": [
                            {
                                "symbol": "SPY260427P00500000",
                                "role": "short",
                                "expiration_date": "2026-04-27",
                                "strike": 500.0,
                                "option_type": "put",
                            }
                        ],
                    },
                    "request": {
                        "execution_intent_id": "intent-1",
                        "order": {
                            "symbol": "SPY260427P00500000",
                            "side": "sell",
                            "position_intent": "sell_to_open",
                            "qty": "1",
                            "type": "limit",
                            "limit_price": "2.50",
                            "time_in_force": "day",
                        },
                        "execution_policy": {},
                        "exit_policy": {},
                    },
                }

            def schema_ready(self) -> bool:
                return True

            def get_attempt(self, execution_attempt_id: str) -> dict[str, object] | None:
                if execution_attempt_id != "attempt-1":
                    return None
                return dict(self.attempt)

            def update_attempt(
                self,
                *,
                execution_attempt_id: str,
                **changes: object,
            ) -> None:
                self.attempt.update(changes)

            def list_orders(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_fills(self, **_: object) -> list[dict[str, object]]:
                return []

            def intent_schema_ready(self) -> bool:
                return True

            def get_execution_intent(
                self,
                execution_intent_id: str,
            ) -> dict[str, object] | None:
                if execution_intent_id != "intent-1":
                    return None
                return dict(self.intent)

            def upsert_execution_intent(self, **payload: object) -> dict[str, object]:
                self.intent.update(payload)
                return dict(self.intent)

            def append_execution_intent_event(self, **_: object) -> None:
                pass

            def list_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return [dict(self.attempt)]

        class _Storage:
            def __init__(self) -> None:
                self.execution = _ExecutionStore()

        class _BrokerClient:
            def get_account(self) -> dict[str, object]:
                return {"options_buying_power": "500000"}

            def submit_order(self, payload: dict[str, object]) -> dict[str, object]:
                raise AlpacaRequestError(
                    "forbidden",
                    status_code=403,
                    url="https://paper-api.alpaca.markets/v2/orders",
                    response_body=(
                        '{"message":"insufficient options buying power '
                        '(required: 191124, available: 8734.72)"}'
                    ),
                )

        storage = _Storage()
        with (
            patch(
                "core.services.execution._validate_live_deployment_quality",
                return_value={"ok": True},
            ),
            patch(
                "core.services.execution.create_alpaca_client_from_env",
                return_value=_BrokerClient(),
            ),
        ):
            result = run_execution_submit(
                db_target="test",
                execution_attempt_id="attempt-1",
                storage=storage,
            )

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "insufficient_options_buying_power")
        self.assertEqual(
            storage.execution.intent["payload"]["execution_admission"]["status"],
            "blocked",
        )
        self.assertEqual(
            storage.execution.intent["payload"]["execution_admission"]["reason"],
            "insufficient_options_buying_power",
        )


if __name__ == "__main__":
    unittest.main()
