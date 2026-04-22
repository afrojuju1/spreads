from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.backtest.service import _simulate_entry_execution
from core.domain.opportunity_models import Opportunity, OpportunityLeg
from core.services.execution import (
    _resolve_open_submission_quantity,
    normalize_execution_policy,
)
from core.services.opportunity_execution_plan import build_execution_plan
from core.services.risk_manager import (
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

    def test_execution_plan_carries_recommended_contracts_for_naked_shorts(
        self,
    ) -> None:
        opportunity = Opportunity(
            opportunity_id="opp-short-call",
            cycle_id="cycle-1",
            session_id="live:test:2026-04-21",
            candidate_id=1,
            symbol="TSLA",
            legacy_strategy="short_call",
            expiration_date="2026-04-27",
            structure_identity="short_call|TSLA260427C00420000",
            style_profile="tactical",
            strategy_family="short_call",
            regime_snapshot_id="regime-1",
            strategy_intent_id="intent-1",
            horizon_intent_id="horizon-1",
            discovery_score=78.0,
            promotion_score=81.0,
            rank=1,
            state="promotable",
            state_reason="selected",
            expected_edge_value=0.18,
            max_loss=180.0,
            capital_usage=180.0,
            product_class="equity",
            evidence={},
            legs=[
                OpportunityLeg(
                    leg_index=0,
                    symbol="TSLA260427C00420000",
                    side="sell",
                    role="short",
                    position_intent="sell_to_open",
                )
            ],
        )

        plan = build_execution_plan([opportunity])
        decision = plan["allocation_decisions"][0]
        intent = plan["execution_intents"][0]

        self.assertEqual(decision.allocation_state, "allocated")
        self.assertEqual(decision.budget_impact["recommended_contracts"], 5)
        self.assertEqual(decision.budget_impact["max_loss"], 900.0)
        self.assertEqual(intent.evidence["recommended_quantity"], 5)

    def test_backtest_entry_execution_scales_short_single_leg_quantity(self) -> None:
        runtime = SimpleNamespace(
            build_settings=SimpleNamespace(
                risk_defaults={
                    "max_risk_per_trade": 500.0,
                }
            ),
            automation=SimpleNamespace(
                strategy_config=SimpleNamespace(management_recipe_refs=())
            ),
        )
        opportunity = {
            "underlying_symbol": "TSLA",
            "strategy_family": "short_put",
            "economics": {
                "midpoint_credit": 2.3,
                "natural_credit": 2.1,
                "fill_ratio": 0.8,
                "max_loss": 120.0,
            },
        }

        execution = _simulate_entry_execution(
            runtime=runtime,
            opportunity=opportunity,
            session_date="2026-04-21",
        )

        self.assertIsNotNone(execution)
        self.assertTrue(execution["filled"])
        self.assertEqual(execution["position"]["remaining_quantity"], 4.0)
        self.assertEqual(execution["position"]["max_loss"], 480.0)
        self.assertEqual(
            execution["position_sizing"]["recommended_quantity"],
            4,
        )

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

        with patch(
            "core.services.risk_manager._environment_reason",
            return_value=None,
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
                cycle={"generated_at": "2026-04-21T15:00:00Z"},
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


if __name__ == "__main__":
    unittest.main()
