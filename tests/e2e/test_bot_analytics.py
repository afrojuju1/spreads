from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from core.services.bot_analytics import _build_entry_decision_audit


class EntryDecisionAuditTests(unittest.TestCase):
    def test_repriced_selection_tracks_latest_filled_intent(self) -> None:
        decided_at = datetime(2026, 4, 20, 15, 0, tzinfo=UTC)
        audit = _build_entry_decision_audit(
            decisions=[
                {
                    "opportunity_decision_id": "decision-1",
                    "state": "selected",
                    "decided_at": decided_at,
                    "policy_ref": {"strategy_id": "put_credit"},
                    "payload": {
                        "opportunity": {
                            "opportunity_id": "opp-1",
                            "underlying_symbol": "SPY",
                            "strategy_family": "put_credit",
                        }
                    },
                    "reason_codes": ["selected_for_entry"],
                }
            ],
            intents=[
                {
                    "execution_intent_id": "intent-1",
                    "opportunity_decision_id": "decision-1",
                    "execution_attempt_id": "attempt-1",
                    "state": "canceled",
                    "created_at": decided_at + timedelta(seconds=1),
                    "payload": {"dispatch_status": "canceled_for_reprice"},
                    "policy_ref": {"strategy_id": "put_credit"},
                },
                {
                    "execution_intent_id": "intent-2",
                    "opportunity_decision_id": "decision-1",
                    "execution_attempt_id": "attempt-2",
                    "state": "filled",
                    "created_at": decided_at + timedelta(seconds=70),
                    "payload": {
                        "dispatch_status": "filled",
                        "reprice_count": 1,
                    },
                    "policy_ref": {"strategy_id": "put_credit"},
                },
            ],
            attempts=[
                {
                    "execution_attempt_id": "attempt-1",
                    "status": "canceled",
                    "submitted_at": decided_at + timedelta(seconds=4),
                    "completed_at": decided_at + timedelta(seconds=60),
                    "error_text": None,
                },
                {
                    "execution_attempt_id": "attempt-2",
                    "status": "filled",
                    "submitted_at": decided_at + timedelta(seconds=75),
                    "completed_at": decided_at + timedelta(seconds=88),
                    "error_text": None,
                },
            ],
            events=[
                {
                    "execution_intent_id": "intent-1",
                    "event_type": "replaced",
                    "event_at": decided_at + timedelta(seconds=70),
                    "payload": {"replacement_execution_intent_id": "intent-2"},
                },
                {
                    "execution_intent_id": "intent-2",
                    "event_type": "filled",
                    "event_at": decided_at + timedelta(seconds=88),
                    "payload": {},
                },
            ],
        )

        self.assertEqual(audit["summary"]["selected_count"], 1)
        self.assertEqual(audit["summary"]["intent_created_count"], 1)
        self.assertEqual(audit["summary"]["filled_count"], 1)
        self.assertEqual(audit["summary"]["repriced_count"], 1)
        self.assertEqual(audit["summary"]["no_intent_count"], 0)
        sample = audit["samples"][0]
        self.assertEqual(sample["outcome_bucket"], "filled")
        self.assertEqual(sample["intent_state"], "filled")
        self.assertEqual(sample["reprice_count"], 1)
        self.assertEqual(sample["execution_intent_id"], "intent-2")

    def test_no_intent_and_revoked_selection_are_counted_separately(self) -> None:
        decided_at = datetime(2026, 4, 20, 15, 0, tzinfo=UTC)
        audit = _build_entry_decision_audit(
            decisions=[
                {
                    "opportunity_decision_id": "decision-no-intent",
                    "state": "selected",
                    "decided_at": decided_at,
                    "policy_ref": {"strategy_id": "call_credit"},
                    "payload": {
                        "opportunity": {
                            "opportunity_id": "opp-no-intent",
                            "underlying_symbol": "QQQ",
                            "strategy_family": "call_credit",
                        }
                    },
                    "reason_codes": ["selected_for_entry"],
                },
                {
                    "opportunity_decision_id": "decision-revoked",
                    "state": "selected",
                    "decided_at": decided_at + timedelta(seconds=10),
                    "policy_ref": {"strategy_id": "put_credit"},
                    "payload": {
                        "opportunity": {
                            "opportunity_id": "opp-revoked",
                            "underlying_symbol": "SPY",
                            "strategy_family": "put_credit",
                        }
                    },
                    "reason_codes": ["selected_for_entry"],
                },
            ],
            intents=[
                {
                    "execution_intent_id": "intent-revoked",
                    "opportunity_decision_id": "decision-revoked",
                    "execution_attempt_id": None,
                    "state": "revoked",
                    "created_at": decided_at + timedelta(seconds=11),
                    "payload": {
                        "dispatch_status": "revoked",
                        "revoke_reason": "opportunity_inactive",
                    },
                    "policy_ref": {"strategy_id": "put_credit"},
                }
            ],
            attempts=[],
            events=[
                {
                    "execution_intent_id": "intent-revoked",
                    "event_type": "revoked",
                    "event_at": decided_at + timedelta(seconds=12),
                    "payload": {"reason": "opportunity_inactive"},
                }
            ],
        )

        self.assertEqual(audit["summary"]["selected_count"], 2)
        self.assertEqual(audit["summary"]["intent_created_count"], 1)
        self.assertEqual(audit["summary"]["no_intent_count"], 1)
        self.assertEqual(audit["summary"]["revoked_count"], 1)
        self.assertEqual(
            audit["summary"]["terminal_reason_counts"],
            {
                "intent_not_created": 1,
                "opportunity_inactive": 1,
            },
        )
        self.assertEqual(audit["samples"][0]["outcome_bucket"], "no_intent")
        self.assertEqual(audit["samples"][1]["outcome_bucket"], "revoked")


if __name__ == "__main__":
    unittest.main()
