from __future__ import annotations

import os
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from spreads.services.execution import normalize_execution_policy
from spreads.services.risk_manager import evaluate_open_execution


class _ExecutionStore:
    def list_positions(self, **_: object) -> list[dict[str, object]]:
        return []

    def list_session_attempts_by_status(self, **_: object) -> list[dict[str, object]]:
        return []


def _candidate() -> dict[str, object]:
    return {
        "underlying_symbol": "MSFT",
        "strategy": "call_debit",
        "candidate": {
            "midpoint_credit": 1.2,
            "max_loss": 250.0,
        },
    }


def _cycle() -> dict[str, object]:
    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


class DeploymentPolicyTests(unittest.TestCase):
    def test_normalize_execution_policy_maps_legacy_allow_live_to_live_auto(self) -> None:
        policy = normalize_execution_policy(
            {
                "execution_policy": {
                    "enabled": True,
                    "mode": "top_promotable",
                },
                "risk_policy": {
                    "allow_live": True,
                },
            }
        )

        self.assertEqual(policy["deployment_mode"], "live_auto")
        self.assertTrue(policy["enabled"])

    def test_explicit_paper_auto_blocks_live_even_with_legacy_allow_live(self) -> None:
        with patch(
            "spreads.services.risk_manager._current_trading_environment",
            return_value="live",
        ), patch.dict(os.environ, {"SPREADS_ALLOW_LIVE_TRADING": "true"}, clear=False):
            decision = evaluate_open_execution(
                execution_store=_ExecutionStore(),
                session_id="live:explore_10_call_debit_weekly_auto:2026-04-15",
                candidate=_candidate(),
                cycle=_cycle(),
                quantity=1,
                limit_price=1.2,
                risk_policy={"enabled": True, "allow_live": True},
                execution_policy={
                    "enabled": True,
                    "deployment_mode": "paper_auto",
                    "mode": "top_promotable",
                },
            )

        self.assertEqual(decision["status"], "blocked")
        self.assertIn("live_environment_blocked", decision["reason_codes"])

    def test_legacy_live_allowance_still_permits_live_when_enabled(self) -> None:
        with patch(
            "spreads.services.risk_manager._current_trading_environment",
            return_value="live",
        ), patch.dict(os.environ, {"SPREADS_ALLOW_LIVE_TRADING": "true"}, clear=False):
            decision = evaluate_open_execution(
                execution_store=_ExecutionStore(),
                session_id="live:explore_10_call_debit_weekly_auto:2026-04-15",
                candidate=_candidate(),
                cycle=_cycle(),
                quantity=1,
                limit_price=1.2,
                risk_policy={"enabled": True, "allow_live": True},
                execution_policy={
                    "enabled": True,
                    "mode": "top_promotable",
                },
            )

        self.assertEqual(decision["status"], "approved")
        self.assertEqual(decision["policy"]["allow_live"], True)


if __name__ == "__main__":
    unittest.main()
