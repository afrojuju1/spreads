from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest
from unittest.mock import patch

from core.services.execution import (
    ExecutionAdmissionError,
    _validate_live_deployment_quality,
)
from core.services.execution.policy import _validate_open_timing_window
from core.services.execution_intents import submit_execution_intent


class ExecutionIntentAdmissionTests(unittest.TestCase):
    def test_paper_auto_enforces_weekly_live_return_floor(self) -> None:
        candidate = {
            "strategy_family": "call_debit_spread",
            "profile": "weekly",
            "width": 5.0,
            "legs": [
                {
                    "symbol": "NVDA260601C00220000",
                    "side": "buy",
                    "role": "long",
                    "position_intent": "buy_to_open",
                },
                {
                    "symbol": "NVDA260601C00225000",
                    "side": "sell",
                    "role": "short",
                    "position_intent": "sell_to_open",
                },
            ],
        }

        with patch(
            "core.services.execution.build_structure_quote_snapshot",
            return_value=({"midpoint_value": 4.9}, None),
        ):
            result = _validate_live_deployment_quality(
                candidate_payload=candidate,
                deployment_mode="paper_auto",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "live_return_on_risk_below_floor")
        self.assertEqual(
            result["live_quote"]["minimum_return_on_risk"],
            0.13,
        )

    def test_paper_auto_enforces_weekly_force_close_timing_window(self) -> None:
        current_time = datetime(2026, 5, 26, 18, 31, tzinfo=UTC)
        result = _validate_open_timing_window(
            exit_policy={
                "enabled": True,
                "force_close_at": (
                    current_time + timedelta(minutes=89)
                ).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
            current_time=current_time,
            profile="weekly",
            deployment_mode="paper_auto",
        )

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "insufficient_time_to_force_close")
        self.assertEqual(result["minimum_minutes_to_force_close"], 90.0)

    def test_submit_execution_intent_persists_structured_admission_failure(self) -> None:
        class _ExecutionStore:
            def __init__(self) -> None:
                self.intent = {
                    "execution_intent_id": "intent-1",
                    "bot_id": "short_dated_etf_short_put_bot",
                    "automation_id": "etf_short_put_entry",
                    "opportunity_decision_id": "decision-1",
                    "strategy_position_id": None,
                    "execution_attempt_id": None,
                    "action_type": "open",
                    "slot_key": "entry:test",
                    "claim_token": None,
                    "policy_ref": {"strategy_id": "short_put"},
                    "config_hash": "cfg-1",
                    "state": "pending",
                    "expires_at": None,
                    "superseded_by_id": None,
                    "payload": {},
                    "created_at": "2026-04-21T15:00:00Z",
                    "updated_at": "2026-04-21T15:00:00Z",
                }
                self.events: list[dict[str, object]] = []

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

            def append_execution_intent_event(self, **payload: object) -> None:
                self.events.append(dict(payload))

        class _SignalStore:
            def get_opportunity_decision(
                self,
                opportunity_decision_id: str,
            ) -> dict[str, object] | None:
                if opportunity_decision_id != "decision-1":
                    return None
                return {"opportunity_id": "opp-1"}

            def get_opportunity(self, opportunity_id: str) -> dict[str, object] | None:
                if opportunity_id != "opp-1":
                    return None
                return {
                    "opportunity_id": "opp-1",
                    "lifecycle_state": "ready",
                    "eligibility_state": "live",
                    "consumed_by_execution_attempt_id": None,
                }

        class _Storage:
            def __init__(self) -> None:
                self.execution = _ExecutionStore()
                self.signals = _SignalStore()

        storage = _Storage()
        admission = {
            "status": "blocked",
            "reason": "strategy_risk_budget_exceeded",
            "message": "Open execution exceeds strategy max_risk_per_trade.",
            "admissible_quantity": 1,
        }

        with patch(
            "core.services.execution_intents.submit_opportunity_execution",
            side_effect=ExecutionAdmissionError(
                "Open execution exceeds strategy max_risk_per_trade.",
                admission=admission,
            ),
        ):
            result = submit_execution_intent(
                db_target="postgresql://example",
                execution_intent_id="intent-1",
                storage=storage,
            )

        self.assertFalse(result["changed"])
        self.assertEqual(result["execution_intent"]["state"], "failed")
        self.assertEqual(
            result["execution_intent"]["payload"]["execution_admission"]["reason"],
            "strategy_risk_budget_exceeded",
        )
        self.assertEqual(
            storage.execution.events[-1]["payload"]["execution_admission"]["status"],
            "blocked",
        )


if __name__ == "__main__":
    unittest.main()
