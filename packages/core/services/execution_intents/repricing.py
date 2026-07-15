from __future__ import annotations

from typing import Any

from core.db.decorators import with_storage
from core.money import option_contract_notional
from core.services.admission_lifecycle import normalize_lifecycle_admission
from core.services.execution.attempts import _get_attempt_payload
from core.value_coercion import as_text, coerce_int, utc_now_iso

from .shared import (
    _intent_kind,
    _intent_payload,
    _next_reprice_limit,
    _reprice_count,
    _transition_intent,
    issue_pending_execution_intent,
)


def _replacement_intent_id(*, execution_intent_id: str, reprice_count: int) -> str:
    return f"{execution_intent_id}:reprice:{reprice_count}"


def _replacement_admission(
    *,
    intent: dict[str, Any],
    attempt: dict[str, Any],
    replacement_intent_id: str,
    next_limit: float,
    reprice_count: int,
) -> dict[str, Any]:
    payload = _intent_payload(intent)
    requested_quantity = coerce_int(payload.get("quantity")) or coerce_int(attempt.get("quantity")) or 1
    intent_kind = _intent_kind(intent, attempt)
    return normalize_lifecycle_admission(
        {
            "status": "approved",
            "reason": "bounded_reprice_approved",
            "message": "Replacement price remains within the approved executor policy.",
        },
        admission_kind=f"{intent_kind}_reprice",
        source_object_type="execution_intent",
        source_object_id=replacement_intent_id,
        session_date=as_text(attempt.get("market_date") or attempt.get("session_date")),
        requested_quantity=requested_quantity,
        requested_notional=option_contract_notional(next_limit, requested_quantity),
        policy_snapshot=dict(intent.get("policy_ref") or {}),
        capability_snapshot={
            "previous_admission_decision_id": as_text(intent.get("admission_decision_id")),
            "previous_execution_attempt_id": as_text(attempt.get("execution_attempt_id")),
            "bounded_by_executor_policy": True,
        },
        metrics={
            "previous_limit_price": attempt.get("requested_limit_price") or attempt.get("limit_price"),
            "replacement_limit_price": next_limit,
            "reprice_count": reprice_count,
        },
        evidence={
            "supersedes_execution_intent_id": str(intent["execution_intent_id"]),
            "replacement_execution_intent_id": replacement_intent_id,
            "close_decision_id": as_text(intent.get("close_decision_id")),
            "position_id": as_text(intent.get("position_id")),
        },
        decided_at=utc_now_iso(),
    )


def _create_replacement_intent(
    execution_store: Any,
    *,
    intent: dict[str, Any],
    attempt: dict[str, Any],
) -> dict[str, Any] | None:
    next_limit = _next_reprice_limit(intent, attempt)
    if next_limit is None:
        return _transition_intent(
            execution_store,
            intent,
            state="failed",
            transition_reason="reprice_exhausted",
            execution_attempt_id=as_text(attempt.get("execution_attempt_id")),
            event_payload={"execution_attempt_id": attempt.get("execution_attempt_id")},
        )

    existing_successor = execution_store.get_successor_execution_intent(str(intent["execution_intent_id"]))
    if existing_successor is not None:
        return dict(existing_successor)

    payload = _intent_payload(intent)
    reprice_count = _reprice_count(intent) + 1
    replacement_id = _replacement_intent_id(
        execution_intent_id=str(intent["execution_intent_id"]),
        reprice_count=reprice_count,
    )
    original_limit_price = payload.get("original_limit_price")
    if original_limit_price in (None, ""):
        original_limit_price = attempt.get("requested_limit_price") or attempt.get("limit_price")
    replacement_payload = {
        **payload,
        "limit_price": next_limit,
        "original_limit_price": original_limit_price,
        "previous_limit_price": attempt.get("requested_limit_price") or attempt.get("limit_price"),
        "reprice_count": reprice_count,
        "supersedes_execution_intent_id": str(intent["execution_intent_id"]),
        "previous_execution_attempt_id": as_text(attempt.get("execution_attempt_id")),
    }
    admission = _replacement_admission(
        intent=intent,
        attempt=attempt,
        replacement_intent_id=replacement_id,
        next_limit=next_limit,
        reprice_count=reprice_count,
    )
    replacement = issue_pending_execution_intent(
        execution_store,
        admission=admission,
        execution_intent_id=replacement_id,
        trading_strategy_id=str(intent["trading_strategy_id"]),
        trade_signal_id=as_text(intent.get("trade_signal_id")),
        trade_decision_id=as_text(intent.get("trade_decision_id")),
        close_decision_id=as_text(intent.get("close_decision_id")),
        position_id=as_text(intent.get("position_id")),
        intent_kind=str(intent["intent_kind"]),
        slot_key=str(intent["slot_key"]),
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        expires_at=as_text(intent.get("expires_at")),
        supersedes_execution_intent_id=str(intent["execution_intent_id"]),
        payload=replacement_payload,
        created_event_payload={
            "reprice_count": reprice_count,
            "limit_price": next_limit,
            "supersedes_execution_intent_id": str(intent["execution_intent_id"]),
        },
    )
    _transition_intent(
        execution_store,
        intent,
        state="superseded",
        transition_reason="replacement_intent_created",
        execution_attempt_id=as_text(attempt.get("execution_attempt_id")),
        event_payload={
            "replacement_execution_intent_id": replacement_id,
            "next_limit_price": next_limit,
        },
    )
    return replacement


@with_storage()
def create_repriced_execution_intent(
    *,
    db_target: str,
    execution_intent_id: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    _ = db_target
    execution_store = storage.execution
    if not execution_store.intent_schema_ready() or not execution_store.schema_ready():
        raise RuntimeError("Execution intent and attempt schema are required for repricing.")
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        raise ValueError(f"Unknown execution_intent_id: {execution_intent_id}")
    existing = execution_store.get_successor_execution_intent(execution_intent_id)
    if existing is not None:
        return {
            "status": "exists",
            "changed": False,
            "execution_intent_id": execution_intent_id,
            "execution_attempt_id": execution_attempt_id,
            "replacement_execution_intent_id": existing["execution_intent_id"],
            "replacement_intent": dict(existing),
        }
    attempt = _get_attempt_payload(execution_store, execution_attempt_id)
    replacement = _create_replacement_intent(
        execution_store,
        intent=dict(intent),
        attempt=attempt,
    )
    if replacement is None or str(replacement.get("execution_intent_id")) == execution_intent_id:
        return {
            "status": "reprice_exhausted",
            "changed": True,
            "execution_intent_id": execution_intent_id,
            "execution_attempt_id": execution_attempt_id,
            "replacement_execution_intent_id": None,
        }
    return {
        "status": "replacement_created",
        "changed": True,
        "execution_intent_id": execution_intent_id,
        "execution_attempt_id": execution_attempt_id,
        "replacement_execution_intent_id": replacement["execution_intent_id"],
        "replacement_intent": replacement,
    }
