from __future__ import annotations

from typing import Any
from uuid import uuid4

from core.services.alpaca import create_alpaca_client_from_env
from core.services.execution import refresh_execution_attempt

from .maintenance import _opportunity_is_active_for_intent, _position_is_active_for_intent
from .shared import (
    WORKING_REPRICE_ATTEMPT_STATUSES,
    _append_event,
    _as_text,
    _intent_action_type,
    issue_pending_execution_intent,
    _intent_payload,
    _next_reprice_limit,
    _reprice_count,
    _submitted_age_seconds,
    _update_intent,
    _utc_now,
)


def _replacement_intent_id() -> str:
    return f"execution_intent:{uuid4().hex}"


def _create_replacement_intent(
    execution_store: Any,
    *,
    intent: dict[str, Any],
    attempt: dict[str, Any],
) -> dict[str, Any] | None:
    next_limit = _next_reprice_limit(intent, attempt)
    if next_limit is None:
        updated = _update_intent(
            execution_store,
            intent,
            state="failed",
            payload_updates={"dispatch_status": "reprice_exhausted"},
            updated_at=_utc_now(),
        )
        _append_event(
            execution_store,
            execution_intent_id=str(intent["execution_intent_id"]),
            event_type="reprice_exhausted",
            payload={"execution_attempt_id": attempt.get("execution_attempt_id")},
        )
        return updated
    now = _utc_now()
    replacement_id = _replacement_intent_id()
    payload = _intent_payload(intent)
    payload.update(
        {
            "limit_price": next_limit,
            "reprice_count": _reprice_count(intent) + 1,
            "dispatch_status": "pending",
            "supersedes_execution_intent_id": str(intent["execution_intent_id"]),
            "previous_execution_attempt_id": _as_text(attempt.get("execution_attempt_id")),
        }
    )
    issue_pending_execution_intent(
        execution_store,
        execution_intent_id=replacement_id,
        bot_id=str(intent["bot_id"]),
        automation_id=str(intent["automation_id"]),
        opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
        strategy_position_id=_as_text(intent.get("strategy_position_id")),
        execution_attempt_id=None,
        action_type=str(intent["action_type"]),
        slot_key=str(intent["slot_key"]),
        claim_token=None,
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        state="pending",
        expires_at=_as_text(intent.get("expires_at")),
        superseded_by_id=None,
        payload=payload,
        created_event_payload={
            "reprice_count": payload.get("reprice_count"),
            "limit_price": next_limit,
            "replaces_execution_intent_id": str(intent["execution_intent_id"]),
        },
    )
    updated = _update_intent(
        execution_store,
        intent,
        state="canceled",
        execution_attempt_id=_as_text(attempt.get("execution_attempt_id")),
        superseded_by_id=replacement_id,
        payload_updates={
            "dispatch_status": "canceled_for_reprice",
            "replacement_execution_intent_id": replacement_id,
        },
        updated_at=now,
    )
    _append_event(
        execution_store,
        execution_intent_id=str(intent["execution_intent_id"]),
        event_type="replaced",
        payload={
            "replacement_execution_intent_id": replacement_id,
            "next_limit_price": next_limit,
        },
    )
    return updated


def _manage_submitted_open_intents(
    *,
    db_target: str,
    storage: Any,
    execution_store: Any,
    limit: int,
    stale_after_seconds: int = 60,
) -> dict[str, Any]:
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            states=["submitted"],
            limit=max(int(limit), 1) * 5,
        )
    ]
    reviewed = 0
    repriced = 0
    canceled = 0
    refreshed = 0
    results: list[dict[str, Any]] = []
    client = create_alpaca_client_from_env()
    for intent in intents:
        if reviewed >= max(int(limit), 1):
            break
        reviewed += 1
        action_type = _intent_action_type(intent)
        execution_attempt_id = _as_text(intent.get("execution_attempt_id"))
        if execution_attempt_id is None:
            continue
        refreshed_result = refresh_execution_attempt(
            db_target=db_target,
            execution_attempt_id=execution_attempt_id,
            storage=storage,
        )
        refreshed_attempt = (
            refreshed_result.get("attempt")
            if isinstance(refreshed_result.get("attempt"), dict)
            else None
        )
        if refreshed_attempt is None:
            continue
        refreshed += 1
        status = str(refreshed_attempt.get("status") or "").strip().lower()
        if status in {
            "filled",
            "failed",
            "canceled",
            "cancelled",
            "expired",
            "rejected",
        }:
            if status in {"canceled", "cancelled"}:
                replacement = _create_replacement_intent(
                    execution_store,
                    intent=intent,
                    attempt=refreshed_attempt,
                )
                if replacement is not None:
                    repriced += 1
                    results.append(
                        {
                            "execution_intent_id": str(intent["execution_intent_id"]),
                            "status": "replaced",
                            "replacement_execution_intent_id": replacement.get(
                                "superseded_by_id"
                            ),
                        }
                    )
            continue
        if status not in WORKING_REPRICE_ATTEMPT_STATUSES:
            continue
        if action_type == "open":
            active, inactive_reason = _opportunity_is_active_for_intent(
                storage.signals,
                intent,
                execution_attempt_id=execution_attempt_id,
            )
        else:
            active, inactive_reason = _position_is_active_for_intent(
                execution_store,
                intent,
            )
        age_seconds = _submitted_age_seconds(refreshed_attempt)
        if age_seconds is None or age_seconds < float(max(stale_after_seconds, 1)):
            continue
        broker_order_id = _as_text(refreshed_attempt.get("broker_order_id"))
        if broker_order_id is None:
            continue
        client.cancel_order(broker_order_id)
        canceled += 1
        _append_event(
            execution_store,
            execution_intent_id=str(intent["execution_intent_id"]),
            event_type="cancel_requested_for_reprice",
            payload={
                "execution_attempt_id": execution_attempt_id,
                "broker_order_id": broker_order_id,
                "age_seconds": age_seconds,
            },
        )
        post_cancel = refresh_execution_attempt(
            db_target=db_target,
            execution_attempt_id=execution_attempt_id,
            storage=storage,
        )
        post_cancel_attempt = (
            post_cancel.get("attempt")
            if isinstance(post_cancel.get("attempt"), dict)
            else refreshed_attempt
        )
        post_cancel_status = str(post_cancel_attempt.get("status") or "").strip().lower()
        if not active and post_cancel_status in {"canceled", "cancelled"}:
            updated = _update_intent(
                execution_store,
                intent,
                state="revoked",
                execution_attempt_id=execution_attempt_id,
                payload_updates={
                    "dispatch_status": "revoked",
                    "revoke_reason": inactive_reason,
                },
                updated_at=_utc_now(),
            )
            _append_event(
                execution_store,
                execution_intent_id=str(intent["execution_intent_id"]),
                event_type="revoked",
                payload={
                    "reason": inactive_reason,
                    "execution_attempt_id": execution_attempt_id,
                },
            )
            results.append(
                {
                    "execution_intent_id": str(intent["execution_intent_id"]),
                    "status": updated.get("state"),
                    "reason": inactive_reason,
                }
            )
            continue
        if post_cancel_status in {"canceled", "cancelled"}:
            replacement = _create_replacement_intent(
                execution_store,
                intent=intent,
                attempt=post_cancel_attempt,
            )
            if replacement is not None:
                repriced += 1
                results.append(
                    {
                        "execution_intent_id": str(intent["execution_intent_id"]),
                        "status": "replaced",
                        "replacement_execution_intent_id": replacement.get(
                            "superseded_by_id"
                        ),
                    }
                )
                continue
        results.append(
            {
                "execution_intent_id": str(intent["execution_intent_id"]),
                "status": post_cancel_status,
                "execution_attempt_id": execution_attempt_id,
            }
        )
    return {
        "reviewed": reviewed,
        "refreshed": refreshed,
        "cancel_requested": canceled,
        "repriced": repriced,
        "results": results,
    }
