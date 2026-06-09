from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.services.execution_lifecycle import (
    PENDING_SUBMISSION_STATUS,
    SUBMIT_UNKNOWN_STATUS,
)
from core.services.execution_lifecycle import project_execution_attempt_lifecycle
from core.services.runtime_identity import (
    build_live_run_scope_id,
)
from core.value_coercion import (
    as_mapping,
    as_text,
    utc_now_iso,
)
from .alpaca_adapter import create_alpaca_order_adapter
from .attempts import (
    _get_attempt_payload,
    _publish_execution_attempt_event,
    _reconcile_submit_unknown_attempt,
    _require_execution_schema,
    _sync_attempt_state,
    _sync_linked_execution_intent,
)
from .shared import (
    _is_terminal_status,
)


def _linked_intent_payload(*, execution_store: Any, attempt: dict[str, Any]) -> dict[str, Any] | None:
    if not execution_store.intent_schema_ready():
        return None
    request = as_mapping(attempt.get("request"))
    execution_intent_id = as_text(request.get("execution_intent_id"))
    if execution_intent_id is None:
        execution_intent_id = as_text(attempt.get("execution_intent_id"))
    if execution_intent_id is None:
        return None
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        return {
            "execution_intent_id": execution_intent_id,
            "state": "missing",
        }
    payload = as_mapping(intent.get("payload"))
    return {
        "execution_intent_id": intent.get("execution_intent_id"),
        "state": intent.get("state"),
        "action_type": intent.get("action_type"),
        "trading_strategy_id": intent.get("trading_strategy_id"),
        "execution_attempt_id": intent.get("execution_attempt_id"),
        "updated_at": intent.get("updated_at"),
        "validation_provenance": payload.get("validation_provenance"),
        "execution_mode": payload.get("execution_mode"),
        "approval_mode": payload.get("approval_mode"),
    }


def _operator_attempt_summary(attempt: dict[str, Any]) -> dict[str, Any]:
    lifecycle = as_mapping(attempt.get("execution_attempt_lifecycle"))
    if not lifecycle:
        lifecycle = project_execution_attempt_lifecycle(
            attempt,
            now=datetime.now(UTC),
        )
    orders = [dict(row) for row in attempt.get("orders") or [] if isinstance(row, dict)]
    fills = [dict(row) for row in attempt.get("fills") or [] if isinstance(row, dict)]
    return {
        "execution_attempt_id": attempt.get("execution_attempt_id"),
        "session_id": attempt.get("session_id"),
        "trading_strategy_id": attempt.get("trading_strategy_id") or as_mapping(attempt.get("request")).get("trading_strategy_id"),
        "underlying_symbol": attempt.get("underlying_symbol"),
        "trade_intent": attempt.get("trade_intent"),
        "status": attempt.get("status"),
        "lifecycle_state": lifecycle.get("lifecycle_state") or attempt.get("lifecycle_state"),
        "lifecycle_phase": lifecycle.get("phase") or attempt.get("lifecycle_phase"),
        "next_action": lifecycle.get("next_action") or attempt.get("next_action"),
        "broker_order_id": attempt.get("broker_order_id"),
        "client_order_id": attempt.get("client_order_id"),
        "order_count": len(orders),
        "fill_count": len(fills),
        "requested_at": attempt.get("requested_at"),
        "submitted_at": attempt.get("submitted_at"),
        "completed_at": attempt.get("completed_at"),
        "error_text": attempt.get("error_text"),
    }


def _operator_result(
    *,
    action: str,
    changed: bool,
    message: str,
    execution_store: Any,
    attempt: dict[str, Any],
) -> dict[str, Any]:
    linked_intent = _linked_intent_payload(execution_store=execution_store, attempt=attempt)
    return {
        "action": action,
        "changed": changed,
        "message": message,
        "summary": _operator_attempt_summary(attempt),
        "linked_intent": linked_intent,
        "attempt": attempt,
    }


@with_storage()
def inspect_execution_attempt(
    *,
    db_target: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    payload = _get_attempt_payload(execution_store, execution_attempt_id)
    return _operator_result(
        action="inspect",
        changed=False,
        message=f"Loaded execution {execution_attempt_id}.",
        execution_store=execution_store,
        attempt=payload,
    )


@with_storage()
def refresh_live_session_execution(
    *,
    db_target: str,
    session_id: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    if str(attempt["session_id"]) != session_id:
        raise ValueError(f"Execution {execution_attempt_id} does not belong to session {session_id}")
    if as_text(attempt.get("broker_order_id")) is None and str(attempt.get("status") or "") == PENDING_SUBMISSION_STATUS:
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        return {
            "action": "refresh",
            "changed": False,
            "message": "Execution is still queued for broker submission.",
            "summary": _operator_attempt_summary(payload),
            "linked_intent": _linked_intent_payload(execution_store=execution_store, attempt=payload),
            "attempt": payload,
        }
    if as_text(attempt.get("broker_order_id")) is None and str(attempt.get("status") or "") == SUBMIT_UNKNOWN_STATUS:
        client_order_id = as_text(attempt.get("client_order_id"))
        if client_order_id is None:
            payload = _get_attempt_payload(execution_store, execution_attempt_id)
            message = "Execution submit outcome is uncertain and cannot be reconciled because the client order id is missing."
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=payload,
                event_type="submit_unknown_unresolved",
                message=message,
            )
            return {
                "action": "refresh",
                "changed": False,
                "message": message,
                "summary": _operator_attempt_summary(payload),
                "linked_intent": _linked_intent_payload(execution_store=execution_store, attempt=payload),
                "attempt": payload,
            }
        adapter = create_alpaca_order_adapter()
        reconciled_attempt = _reconcile_submit_unknown_attempt(
            execution_store=execution_store,
            attempt=attempt,
            client=adapter.client,
        )
        if reconciled_attempt is None:
            payload = _get_attempt_payload(execution_store, execution_attempt_id)
            message = f"Execution submit outcome is uncertain and no broker order has been found yet for client_order_id {client_order_id}."
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=payload,
                event_type="submit_unknown_unresolved",
                message=message,
            )
            return {
                "action": "refresh",
                "changed": False,
                "message": message,
                "summary": _operator_attempt_summary(payload),
                "linked_intent": _linked_intent_payload(execution_store=execution_store, attempt=payload),
                "attempt": payload,
            }
        message = f"Reconciled execution {execution_attempt_id} via client_order_id {client_order_id}: {reconciled_attempt['status']}."
        _publish_execution_attempt_event(reconciled_attempt, message=message)
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=reconciled_attempt,
            event_type="reconciled",
            message=message,
        )
        return {
            "action": "refresh",
            "changed": True,
            "message": message,
            "summary": _operator_attempt_summary(reconciled_attempt),
            "linked_intent": _linked_intent_payload(execution_store=execution_store, attempt=reconciled_attempt),
            "attempt": reconciled_attempt,
        }
    broker_order_id = as_text(attempt.get("broker_order_id"))
    if broker_order_id is None:
        raise ValueError("Execution does not have a broker order id to refresh")

    adapter = create_alpaca_order_adapter()
    order_snapshot = adapter.get_order_snapshot(broker_order_id, nested=True)
    payload = _sync_attempt_state(
        execution_store=execution_store,
        attempt=dict(attempt),
        client=adapter.client,
        order_snapshot=order_snapshot,
    )
    message = f"Refreshed execution {execution_attempt_id}: {payload['status']}."
    _publish_execution_attempt_event(payload, message=message)
    _sync_linked_execution_intent(
        execution_store=execution_store,
        attempt=payload,
        event_type="refreshed",
        message=message,
    )
    return {
        "action": "refresh",
        "changed": True,
        "message": message,
        "summary": _operator_attempt_summary(payload),
        "linked_intent": _linked_intent_payload(execution_store=execution_store, attempt=payload),
        "attempt": payload,
    }


@with_storage()
def refresh_execution_attempt(
    *,
    db_target: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    session_id = as_text(attempt.get("session_id"))
    if session_id is None:
        label = as_text(attempt.get("label"))
        market_date = as_text(attempt.get("market_date")) or as_text(attempt.get("session_date"))
        if label is None or market_date is None:
            raise ValueError("Execution attempt is missing session compatibility fields")
        session_id = build_live_run_scope_id(label, market_date)
    return refresh_live_session_execution(
        db_target=db_target,
        session_id=session_id,
        execution_attempt_id=execution_attempt_id,
        storage=storage,
    )


@with_storage()
def cancel_execution_attempt(
    *,
    db_target: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")

    status = str(attempt.get("status") or "").strip().lower()
    if _is_terminal_status(status):
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        return {
            "action": "cancel",
            "changed": False,
            "message": f"Execution is already terminal: {payload['status']}.",
            "summary": _operator_attempt_summary(payload),
            "linked_intent": _linked_intent_payload(execution_store=execution_store, attempt=payload),
            "attempt": payload,
        }

    broker_order_id = as_text(attempt.get("broker_order_id"))
    if broker_order_id is None:
        if status != PENDING_SUBMISSION_STATUS:
            raise ValueError("Execution does not have a broker order id to cancel")
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="canceled",
            completed_at=utc_now_iso(),
            position_id=as_text(attempt.get("position_id")),
        )
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        message = f"Canceled queued execution {execution_attempt_id} before broker submit."
        _publish_execution_attempt_event(payload, message=message)
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=payload,
            state="canceled",
            event_type="canceled",
            message=message,
        )
        return {
            "action": "cancel",
            "changed": True,
            "message": message,
            "summary": _operator_attempt_summary(payload),
            "linked_intent": _linked_intent_payload(execution_store=execution_store, attempt=payload),
            "attempt": payload,
        }

    adapter = create_alpaca_order_adapter()
    order_snapshot = adapter.request_cancel(broker_order_id)
    execution_store.update_attempt(
        execution_attempt_id=execution_attempt_id,
        status="pending_cancel",
        position_id=as_text(attempt.get("position_id")),
    )
    if order_snapshot is None:
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
    else:
        payload = _sync_attempt_state(
            execution_store=execution_store,
            attempt=dict(attempt),
            client=adapter.client,
            order_snapshot=order_snapshot,
        )
    message = f"Requested cancel for execution {execution_attempt_id}: {payload['status']}."
    _publish_execution_attempt_event(payload, message=message)
    _sync_linked_execution_intent(
        execution_store=execution_store,
        attempt=payload,
        event_type="cancel_requested",
        message=message,
    )
    return {
        "action": "cancel",
        "changed": True,
        "message": message,
        "summary": _operator_attempt_summary(payload),
        "linked_intent": _linked_intent_payload(execution_store=execution_store, attempt=payload),
        "attempt": payload,
    }
