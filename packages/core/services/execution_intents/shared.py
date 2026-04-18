from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.services.option_structures import (
    net_premium_kind,
    normalize_strategy_family,
)
from core.storage.serializers import parse_datetime

AUTO_EXECUTION_MODES = {"paper", "live"}
ACTIVE_INTENT_STATES = {"pending", "claimed", "submitted", "partially_filled"}
OPEN_POSITION_STATES = {"open", "partial_open", "partial_close"}
WORKING_REPRICE_ATTEMPT_STATUSES = {
    "accepted",
    "accepted_for_bidding",
    "new",
    "pending_new",
    "replaced",
}
TERMINAL_INTENT_STATES = {"failed", "canceled", "revoked", "expired"}
_UNCHANGED = object()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _intent_payload(intent: dict[str, Any]) -> dict[str, Any]:
    payload = intent.get("payload")
    if isinstance(payload, dict):
        return dict(payload)
    payload_json = intent.get("payload_json")
    if isinstance(payload_json, dict):
        return dict(payload_json)
    return {}


def _attempt_request(attempt: dict[str, Any]) -> dict[str, Any]:
    payload = attempt.get("request")
    return dict(payload) if isinstance(payload, dict) else {}


def _intent_action_type(
    intent: dict[str, Any], attempt: dict[str, Any] | None = None
) -> str:
    action_type = str(intent.get("action_type") or "").strip().lower()
    if action_type:
        return action_type
    request = {} if attempt is None else _attempt_request(attempt)
    trade_intent = (
        str(
            request.get("trade_intent")
            or (None if attempt is None else attempt.get("trade_intent"))
            or "open"
        )
        .strip()
        .lower()
    )
    return "close" if trade_intent == "close" else "open"


def _update_intent(
    execution_store: Any,
    intent: dict[str, Any],
    *,
    state: str | object = _UNCHANGED,
    payload_updates: dict[str, Any] | None = None,
    payload: dict[str, Any] | object = _UNCHANGED,
    opportunity_decision_id: str | None | object = _UNCHANGED,
    strategy_position_id: str | None | object = _UNCHANGED,
    execution_attempt_id: str | None | object = _UNCHANGED,
    claim_token: str | None | object = _UNCHANGED,
    expires_at: str | None | object = _UNCHANGED,
    superseded_by_id: str | None | object = _UNCHANGED,
    updated_at: str | None = None,
) -> dict[str, Any]:
    resolved_payload = (
        _intent_payload(intent)
        if payload is _UNCHANGED
        else dict(payload or {})
    )
    if payload_updates:
        resolved_payload.update(payload_updates)
    return execution_store.upsert_execution_intent(
        execution_intent_id=str(intent["execution_intent_id"]),
        bot_id=str(intent["bot_id"]),
        automation_id=str(intent["automation_id"]),
        opportunity_decision_id=(
            _as_text(intent.get("opportunity_decision_id"))
            if opportunity_decision_id is _UNCHANGED
            else opportunity_decision_id
        ),
        strategy_position_id=(
            _as_text(intent.get("strategy_position_id"))
            if strategy_position_id is _UNCHANGED
            else strategy_position_id
        ),
        execution_attempt_id=(
            _as_text(intent.get("execution_attempt_id"))
            if execution_attempt_id is _UNCHANGED
            else execution_attempt_id
        ),
        action_type=str(intent["action_type"]),
        slot_key=str(intent["slot_key"]),
        claim_token=(
            _as_text(intent.get("claim_token"))
            if claim_token is _UNCHANGED
            else claim_token
        ),
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        state=(
            str(intent.get("state") or "")
            if state is _UNCHANGED
            else str(state)
        ),
        expires_at=(
            _as_text(intent.get("expires_at"))
            if expires_at is _UNCHANGED
            else expires_at
        ),
        superseded_by_id=(
            _as_text(intent.get("superseded_by_id"))
            if superseded_by_id is _UNCHANGED
            else superseded_by_id
        ),
        payload=resolved_payload,
        created_at=str(intent["created_at"]),
        updated_at=updated_at or _utc_now(),
    )


def _append_event(
    execution_store: Any,
    *,
    execution_intent_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> None:
    execution_store.append_execution_intent_event(
        execution_intent_id=execution_intent_id,
        event_type=event_type,
        event_at=_utc_now(),
        payload=payload,
    )


def issue_pending_execution_intent(
    execution_store: Any,
    *,
    execution_intent_id: str,
    bot_id: str,
    automation_id: str,
    opportunity_decision_id: str | None,
    strategy_position_id: str | None,
    action_type: str,
    slot_key: str,
    policy_ref: dict[str, Any],
    config_hash: str,
    expires_at: str | None,
    payload: dict[str, Any] | None = None,
    created_event_payload: dict[str, Any] | None = None,
    claim_token: str | None = None,
    execution_attempt_id: str | None = None,
    superseded_by_id: str | None = None,
    state: str = "pending",
) -> dict[str, Any]:
    created_at = _utc_now()
    intent = execution_store.upsert_execution_intent(
        execution_intent_id=execution_intent_id,
        bot_id=bot_id,
        automation_id=automation_id,
        opportunity_decision_id=opportunity_decision_id,
        strategy_position_id=strategy_position_id,
        execution_attempt_id=execution_attempt_id,
        action_type=action_type,
        slot_key=slot_key,
        claim_token=claim_token,
        policy_ref=policy_ref,
        config_hash=config_hash,
        state=state,
        expires_at=expires_at,
        superseded_by_id=superseded_by_id,
        payload={} if payload is None else dict(payload),
        created_at=created_at,
        updated_at=created_at,
    )
    _append_event(
        execution_store,
        execution_intent_id=str(intent["execution_intent_id"]),
        event_type="created",
        payload=None if created_event_payload is None else dict(created_event_payload),
    )
    return intent


def link_execution_intent_position(
    execution_store: Any,
    *,
    intent: dict[str, Any],
    position_id: str,
    execution_attempt_id: str | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    return _update_intent(
        execution_store,
        intent,
        strategy_position_id=position_id,
        execution_attempt_id=(
            _UNCHANGED if execution_attempt_id is None else execution_attempt_id
        ),
        payload_updates={"strategy_position_id": position_id},
        updated_at=updated_at,
    )


def sync_execution_intent_from_attempt(
    execution_store: Any,
    *,
    intent: dict[str, Any],
    attempt: dict[str, Any],
    state: str,
    event_type: str,
    event_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution_attempt_id = _as_text(attempt.get("execution_attempt_id"))
    strategy_position_id = _as_text(attempt.get("position_id")) or _as_text(
        intent.get("strategy_position_id")
    )
    updated_at = _utc_now()
    updated = _update_intent(
        execution_store,
        intent,
        state=state,
        strategy_position_id=(
            _UNCHANGED if strategy_position_id is None else strategy_position_id
        ),
        execution_attempt_id=(
            _UNCHANGED if execution_attempt_id is None else execution_attempt_id
        ),
        payload_updates={
            "dispatch_status": state,
            "execution_attempt_id": execution_attempt_id,
            "attempt_status": str(attempt.get("status") or ""),
            **(
                {}
                if strategy_position_id is None
                else {"strategy_position_id": strategy_position_id}
            ),
        },
        updated_at=updated_at,
    )
    _append_event(
        execution_store,
        execution_intent_id=str(intent["execution_intent_id"]),
        event_type=event_type,
        payload=None if event_payload is None else dict(event_payload),
    )
    return updated


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
    return "claimed"


def _reprice_count(intent: dict[str, Any]) -> int:
    payload = _intent_payload(intent)
    try:
        return int(payload.get("reprice_count") or 0)
    except (TypeError, ValueError):
        return 0


def _submitted_age_seconds(attempt: dict[str, Any]) -> float | None:
    submitted_at = parse_datetime(_as_text(attempt.get("submitted_at")))
    if submitted_at is None:
        submitted_at = parse_datetime(_as_text(attempt.get("requested_at")))
    if submitted_at is None:
        return None
    return max((datetime.now(UTC) - submitted_at).total_seconds(), 0.0)


def _next_reprice_limit(
    intent: dict[str, Any], attempt: dict[str, Any]
) -> float | None:
    request = _attempt_request(attempt)
    candidate = (
        request.get("candidate") if isinstance(request.get("candidate"), dict) else {}
    )
    execution_policy = (
        request.get("execution_policy")
        if isinstance(request.get("execution_policy"), dict)
        else {}
    )
    current_limit = _coerce_float(attempt.get("requested_limit_price"))
    if current_limit is None:
        current_limit = _coerce_float(attempt.get("limit_price"))
    if current_limit is None:
        return None
    natural_value = _coerce_float(
        candidate.get("natural_credit")
        or candidate.get("natural_debit")
        or candidate.get("natural_value")
    )
    max_credit_concession = max(
        _coerce_float(execution_policy.get("max_credit_concession")) or 0.02,
        0.0,
    )
    step = 0.01
    premium_kind = net_premium_kind(
        normalize_strategy_family(
            attempt.get("strategy_family") or attempt.get("strategy")
        )
    )
    if _intent_action_type(intent, attempt) == "close":
        if premium_kind == "credit":
            premium_kind = "debit"
        elif premium_kind == "debit":
            premium_kind = "credit"
    if premium_kind == "debit":
        ceiling = current_limit + max_credit_concession
        target = min(round(current_limit + step, 2), round(ceiling, 2))
        if natural_value is not None:
            target = min(target, round(max(natural_value, current_limit), 2))
        if target <= current_limit:
            return None
        return target

    floor = round(max(current_limit - max_credit_concession, 0.01), 2)
    target = max(round(current_limit - step, 2), floor)
    if natural_value is not None:
        target = min(target, round(current_limit - step, 2))
    if target >= current_limit:
        return None
    return max(target, 0.01)
