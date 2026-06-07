from __future__ import annotations

from typing import Any

from core.db.decorators import with_storage
from core.services.execution_lifecycle import (
    PENDING_SUBMISSION_STATUS,
    SUBMIT_UNKNOWN_STATUS,
)
from core.services.runtime_identity import (
    build_live_run_scope_id,
)
from core.services.value_coercion import (
    as_text as _as_text,
    utc_now_iso as _utc_now,
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
    if _as_text(attempt.get("broker_order_id")) is None and str(attempt.get("status") or "") == PENDING_SUBMISSION_STATUS:
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        return {
            "action": "refresh",
            "changed": False,
            "message": "Execution is still queued for broker submission.",
            "attempt": payload,
        }
    if _as_text(attempt.get("broker_order_id")) is None and str(attempt.get("status") or "") == SUBMIT_UNKNOWN_STATUS:
        client_order_id = _as_text(attempt.get("client_order_id"))
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
            "attempt": reconciled_attempt,
        }
    broker_order_id = _as_text(attempt.get("broker_order_id"))
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
    session_id = _as_text(attempt.get("session_id"))
    if session_id is None:
        label = _as_text(attempt.get("label"))
        market_date = _as_text(attempt.get("market_date")) or _as_text(attempt.get("session_date"))
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
            "attempt": payload,
        }

    broker_order_id = _as_text(attempt.get("broker_order_id"))
    if broker_order_id is None:
        if status != PENDING_SUBMISSION_STATUS:
            raise ValueError("Execution does not have a broker order id to cancel")
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="canceled",
            completed_at=_utc_now(),
            position_id=_as_text(attempt.get("position_id")),
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
            "attempt": payload,
        }

    adapter = create_alpaca_order_adapter()
    order_snapshot = adapter.request_cancel(broker_order_id)
    execution_store.update_attempt(
        execution_attempt_id=execution_attempt_id,
        status="pending_cancel",
        position_id=_as_text(attempt.get("position_id")),
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
        "attempt": payload,
    }
