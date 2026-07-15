from __future__ import annotations

from typing import Any

from core.money import repriced_limit_price
from core.services.option_structures import (
    net_premium_kind,
    normalize_strategy_family,
)
from core.services.trading_lifecycle import (
    ExecutionIntentState,
    LifecycleObject,
    require_lifecycle_transition,
    validate_lifecycle_transition,
    normalize_lifecycle_state,
)
from core.value_coercion import (
    as_mapping,
    as_text,
    coerce_float,
    coerce_int,
    utc_now,
)
from core.storage.serializers import parse_datetime

AUTO_EXECUTION_MODES = {"paper", "live"}
ACTIVE_INTENT_STATES = {
    ExecutionIntentState.PENDING.value,
    ExecutionIntentState.CLAIMED.value,
    ExecutionIntentState.SUBMITTED.value,
    ExecutionIntentState.PARTIALLY_FILLED.value,
}
OPEN_POSITION_STATES = {"open", "partial_open", "partial_close"}
WORKING_REPRICE_ATTEMPT_STATUSES = {
    "accepted",
    "accepted_for_bidding",
    "new",
    "pending_new",
    "replaced",
}
TERMINAL_INTENT_STATES = {
    ExecutionIntentState.FILLED.value,
    ExecutionIntentState.FAILED.value,
    ExecutionIntentState.CANCELED.value,
    ExecutionIntentState.REVOKED.value,
    ExecutionIntentState.EXPIRED.value,
    ExecutionIntentState.SUPERSEDED.value,
}


def _intent_payload(intent: dict[str, Any]) -> dict[str, Any]:
    payload = intent.get("payload")
    if isinstance(payload, dict):
        return dict(payload)
    payload_json = intent.get("payload_json")
    if isinstance(payload_json, dict):
        return dict(payload_json)
    return {}


def normalize_execution_intent_state(value: Any) -> str:
    return normalize_lifecycle_state(
        LifecycleObject.EXECUTION_INTENT,
        value,
    ).value


def validate_execution_intent_transition(
    from_state: Any,
    to_state: Any,
):
    return validate_lifecycle_transition(
        LifecycleObject.EXECUTION_INTENT,
        None if as_text(from_state) is None else normalize_execution_intent_state(from_state),
        normalize_execution_intent_state(to_state),
    )


def require_execution_intent_transition(
    from_state: Any,
    to_state: Any,
) -> str:
    normalized_to = normalize_execution_intent_state(to_state)
    require_lifecycle_transition(
        LifecycleObject.EXECUTION_INTENT,
        None if as_text(from_state) is None else normalize_execution_intent_state(from_state),
        normalized_to,
    )
    return normalized_to


def _attempt_request(attempt: dict[str, Any]) -> dict[str, Any]:
    payload = attempt.get("request")
    return dict(payload) if isinstance(payload, dict) else {}


def _intent_kind(intent: dict[str, Any], attempt: dict[str, Any] | None = None) -> str:
    intent_kind = str(intent.get("intent_kind") or "").strip().lower()
    request = {} if attempt is None else _attempt_request(attempt)
    payload = _intent_payload(intent)
    trade_intent = (
        str(request.get("trade_intent") or payload.get("trade_intent") or (None if attempt is None else attempt.get("trade_intent")) or "open")
        .strip()
        .lower()
    )
    if intent_kind in {"open", "close"}:
        return intent_kind
    if trade_intent in {"open", "close"}:
        return trade_intent
    return intent_kind or "open"


def _transition_intent(
    execution_store: Any,
    intent: dict[str, Any],
    *,
    state: str,
    transition_reason: str,
    event_payload: dict[str, Any] | None = None,
    engine_event_type: str = "engine.state_transitioned",
    claim_token: str | None = None,
    claimed_at: str | None = None,
    workflow_id: str | None = None,
    workflow_run_id: str | None = None,
    execution_attempt_id: str | None = None,
) -> dict[str, Any]:
    current_state = as_text(intent.get("state"))
    if current_state is None:
        raise ValueError("Execution intent is missing its lifecycle state.")
    resolved_state = require_execution_intent_transition(current_state, state)
    return execution_store.transition_execution_intent(
        execution_intent_id=str(intent["execution_intent_id"]),
        expected_state=current_state,
        expected_version=int(intent.get("state_version") or 0),
        to_state=resolved_state,
        transition_reason=transition_reason,
        event_payload=event_payload,
        engine_event_type=engine_event_type,
        claim_token=claim_token,
        claimed_at=claimed_at,
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
        execution_attempt_id=execution_attempt_id,
    )


def issue_pending_execution_intent(
    execution_store: Any,
    *,
    admission: dict[str, Any],
    execution_intent_id: str,
    trading_strategy_id: str,
    position_id: str | None,
    close_decision_id: str | None,
    intent_kind: str,
    slot_key: str,
    policy_ref: dict[str, Any],
    config_hash: str,
    expires_at: str | None,
    payload: dict[str, Any] | None = None,
    created_event_payload: dict[str, Any] | None = None,
    supersedes_execution_intent_id: str | None = None,
    trade_signal_id: str | None = None,
    trade_decision_id: str | None = None,
) -> dict[str, Any]:
    created_at = utc_now().isoformat().replace("+00:00", "Z")
    admission_decision_id = str(admission["admission_decision_id"])
    handoff = execution_store.persist_admission_intent_handoff(
        admission=admission,
        execution_intent={
            "execution_intent_id": execution_intent_id,
            "trading_strategy_id": trading_strategy_id,
            "trade_signal_id": trade_signal_id,
            "trade_decision_id": trade_decision_id,
            "admission_decision_id": admission_decision_id,
            "close_decision_id": close_decision_id,
            "position_id": position_id,
            "intent_kind": intent_kind,
            "slot_key": slot_key,
            "claim_token": None,
            "claimed_at": None,
            "workflow_id": None,
            "workflow_run_id": None,
            "policy_ref": policy_ref,
            "config_hash": config_hash,
            "state": "pending",
            "expires_at": expires_at,
            "supersedes_execution_intent_id": supersedes_execution_intent_id,
            "state_version": 1,
            "payload": {} if payload is None else dict(payload),
            "created_at": created_at,
            "updated_at": created_at,
        },
        created_event_payload=created_event_payload,
    )
    intent = handoff.get("execution_intent")
    if not isinstance(intent, dict):
        raise RuntimeError(f"Approved admission {admission_decision_id} did not create an execution intent")
    return intent


def sync_execution_intent_from_attempt(
    execution_store: Any,
    *,
    intent: dict[str, Any],
    attempt: dict[str, Any],
    state: str,
    event_type: str,
    event_payload: dict[str, Any] | None = None,
    payload_updates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution_attempt_id = as_text(attempt.get("execution_attempt_id"))
    return _transition_intent(
        execution_store,
        intent,
        state=state,
        transition_reason=event_type,
        execution_attempt_id=execution_attempt_id,
        event_payload={
            "attempt_status": str(attempt.get("status") or ""),
            **({} if event_payload is None else dict(event_payload)),
            **({} if payload_updates is None else dict(payload_updates)),
        },
    )


def _attempt_state(attempt: dict[str, Any] | None) -> str:
    if attempt is None:
        return "claimed"
    status = str(attempt.get("status") or "").strip().lower()
    if status in {"partially_filled"}:
        return "partially_filled"
    if status in {"filled"}:
        return "filled"
    if status in {"canceled", "cancelled"}:
        return "canceled"
    if status in {"failed", "rejected"}:
        return "failed"
    if status in {"expired", "revoked"}:
        return status
    if status in {"new", "accepted", "pending_new", "submitted"}:
        return "submitted"
    return "claimed"


def _reprice_count(intent: dict[str, Any]) -> int:
    payload = _intent_payload(intent)
    try:
        return int(payload.get("reprice_count") or 0)
    except (TypeError, ValueError):
        return 0


def _submitted_age_seconds(attempt: dict[str, Any]) -> float | None:
    submitted_at = parse_datetime(as_text(attempt.get("submitted_at")))
    if submitted_at is None:
        submitted_at = parse_datetime(as_text(attempt.get("requested_at")))
    if submitted_at is None:
        return None
    return max((utc_now() - submitted_at).total_seconds(), 0.0)


def _repricing_policy(intent: dict[str, Any], attempt: dict[str, Any]) -> dict[str, Any]:
    payload = _intent_payload(intent)
    request = _attempt_request(attempt)
    exit_policy = as_mapping(request.get("exit_policy")) or as_mapping(payload.get("exit_policy"))
    execution_policy = as_mapping(request.get("execution_policy")) or as_mapping(payload.get("execution_policy"))
    policy = (
        as_mapping(request.get("repricing_policy"))
        or as_mapping(payload.get("repricing_policy"))
        or as_mapping(payload.get("repricing"))
        or as_mapping(execution_policy.get("repricing_policy"))
        or as_mapping(execution_policy.get("repricing"))
        or as_mapping(exit_policy.get("repricing"))
    )
    if not policy:
        return {}
    lifecycle_action = as_text(execution_policy.get("stale_order_action"))
    if lifecycle_action is not None and "stale_order_action" not in policy:
        return {**policy, "stale_order_action": lifecycle_action}
    return policy


def _policy_enabled(policy: dict[str, Any]) -> bool:
    value = policy.get("enabled")
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _next_reprice_limit(intent: dict[str, Any], attempt: dict[str, Any]) -> float | None:
    request = _attempt_request(attempt)
    candidate = request.get("candidate") if isinstance(request.get("candidate"), dict) else {}
    execution_policy = request.get("execution_policy") if isinstance(request.get("execution_policy"), dict) else {}
    current_limit = coerce_float(attempt.get("requested_limit_price"))
    if current_limit is None:
        current_limit = coerce_float(attempt.get("limit_price"))
    if current_limit is None:
        return None
    policy = _repricing_policy(intent, attempt)
    if not policy or not _policy_enabled(policy):
        return None
    intent_kind = _intent_kind(intent, attempt)
    max_reprices = coerce_int(policy.get("max_reprices", policy.get("max_reprice_count")))
    if max_reprices is None:
        max_reprices = 3
    if _reprice_count(intent) >= max(max_reprices, 0):
        return None
    natural_value = coerce_float(candidate.get("natural_credit") or candidate.get("natural_debit") or candidate.get("natural_value"))
    max_credit_concession = max(
        coerce_float(
            policy.get(
                "max_concession",
                policy.get(
                    "max_credit_concession",
                    execution_policy.get("max_credit_concession"),
                ),
            )
        )
        or 0.02,
        0.0,
    )
    step = max(
        coerce_float(policy.get("price_step", policy.get("step"))) or 0.01,
        0.01,
    )
    original_limit = coerce_float(
        _intent_payload(intent).get(
            "original_limit_price",
            request.get("original_limit_price"),
        )
    )
    if original_limit is None:
        original_limit = current_limit
    premium_kind = net_premium_kind(normalize_strategy_family(attempt.get("strategy_family") or attempt.get("strategy")))
    if intent_kind == "close":
        if premium_kind == "credit":
            premium_kind = "debit"
        elif premium_kind == "debit":
            premium_kind = "credit"
    return repriced_limit_price(
        current_limit=current_limit,
        original_limit=original_limit,
        step=step,
        max_concession=max_credit_concession,
        premium_kind=premium_kind,
        natural_value=natural_value,
    )
