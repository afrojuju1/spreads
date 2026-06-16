from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.money import premium_float
from core.services.trading_engine.risk_runtime import close_intent_id, close_slot_key
from core.value_coercion import as_text, utc_expiry_iso

from .shared import issue_pending_execution_intent


def _close_source_payload(*, kind: str, decision: Mapping[str, Any], close_decision: Mapping[str, Any]) -> dict[str, Any]:
    details = dict(decision.get("decision_details") or {}) if isinstance(decision.get("decision_details"), Mapping) else {}
    exit_context: dict[str, Any] = {}
    for key in (
        "mark",
        "effective_mark",
        "entry_value",
        "profit_target_mark",
        "stop_mark",
    ):
        rounded = premium_float(details.get(key))
        if rounded is not None:
            exit_context[key] = rounded
    for key in ("mark_state", "force_close_at", "quote_spread_state", "underlying_invalidation_state"):
        text = as_text(details.get(key))
        if text is not None:
            exit_context[key] = text

    payload: dict[str, Any] = {
        "kind": kind,
        "source_object_type": "close_decision",
        "source_object_id": close_decision.get("close_decision_id"),
        "reason": as_text(decision.get("reason")),
        "decision_source": as_text(decision.get("decision_source")),
        "recipe_ref": as_text(decision.get("recipe_ref")),
        "limit_price_source": as_text(decision.get("limit_price_source")),
        "close_decision": dict(close_decision),
    }
    if exit_context:
        payload["exit_context"] = exit_context
    return payload


def issue_close_execution_intent(
    execution_store: Any,
    *,
    position: Mapping[str, Any],
    runtime: Any,
    decision: Mapping[str, Any],
    close_decision: Mapping[str, Any],
    close_admission: Mapping[str, Any],
) -> dict[str, Any]:
    position_id = str(position["position_id"])
    close_execution_policy = runtime.strategy.execution.execution_policy_for_action("close")
    close_repricing_policy = dict(close_execution_policy.get("repricing_policy") or {})
    close_executor_profile = runtime.strategy.execution.executor_profile_snapshot("close")
    return issue_pending_execution_intent(
        execution_store,
        execution_intent_id=close_intent_id(position_id=position_id, trading_strategy_id=runtime.trading_strategy_id),
        trading_strategy_id=runtime.trading_strategy_id,
        strategy_position_id=position_id,
        execution_attempt_id=None,
        action_type="close",
        slot_key=close_slot_key(position_id),
        claim_token=None,
        policy_ref={
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_structure": runtime.trade_structure,
            "routine": "manage",
        },
        config_hash=runtime.config_hash,
        state="pending",
        expires_at=utc_expiry_iso(
            minutes=int(close_execution_policy["submit_ttl_minutes"]),
            minimum_seconds=60,
        ),
        superseded_by_id=None,
        payload={
            "position_id": position_id,
            "limit_price": decision.get("limit_price"),
            "limit_price_source": decision.get("limit_price_source"),
            "reason": decision.get("reason"),
            "recipe_ref": decision.get("recipe_ref"),
            "close_decision": dict(close_decision),
            "close_admission": dict(close_admission),
            "decision_source": decision.get("decision_source"),
            "decision_details": dict(decision.get("decision_details") or {}),
            "source": _close_source_payload(
                kind="management_runtime_exit",
                decision=decision,
                close_decision=close_decision,
            ),
            "execution_mode": runtime.strategy.execution.mode,
            "approval_mode": runtime.strategy.execution.approval,
            "execution_runtime": runtime.strategy.execution.runtime,
            "executor_profile": close_executor_profile,
            "execution_policy": close_execution_policy,
            "repricing_policy": close_repricing_policy,
            "validation_provenance": "natural_strategy",
        },
        created_event_payload={
            "position_id": position_id,
            "reason": decision.get("reason"),
            "recipe_ref": decision.get("recipe_ref"),
            "close_decision_id": close_decision.get("close_decision_id"),
            "close_decision_state": close_decision.get("decision_state"),
            "close_admission_state": close_admission.get("admission_state"),
            "limit_price": decision.get("limit_price"),
            "execution_runtime": runtime.strategy.execution.runtime,
            "executor_profile_id": close_executor_profile.get("executor_profile_id"),
            "submit_ttl_minutes": close_execution_policy.get("submit_ttl_minutes"),
        },
    )


__all__ = ["issue_close_execution_intent"]
