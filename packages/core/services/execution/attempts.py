from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.events.bus import publish_global_event_sync
from core.integrations.alpaca.client import AlpacaClient, AlpacaRequestError
from core.observability.logging import log_event
from core.services.session_positions import (
    CLOSE_TRADE_INTENT,
    OPEN_TRADE_INTENT,
    sync_session_position_from_attempt,
)
from core.services.execution_lifecycle import (
    is_terminal_execution_attempt_status,
    project_execution_attempt_lifecycle,
)
from core.value_coercion import (
    as_text,
    coerce_float,
    utc_now_iso,
)

from .shared import (
    BROKER_NAME,
    EXECUTION_SCHEMA_MESSAGE,
    _order_intent_key,
    _resolve_completed_at,
)

logger = logging.getLogger(__name__)


def _log_attempt_failure(event: str, *, attempt: Mapping[str, Any], exc: Exception, **fields: Any) -> None:
    log_event(
        logger,
        logging.WARNING,
        event,
        exc_info=True,
        execution_attempt_id=attempt.get("execution_attempt_id"),
        session_id=attempt.get("session_id"),
        session_date=attempt.get("session_date"),
        underlying_symbol=attempt.get("underlying_symbol"),
        broker_order_id=attempt.get("broker_order_id"),
        client_order_id=attempt.get("client_order_id"),
        status=attempt.get("status"),
        error=str(exc),
        **fields,
    )


def _require_execution_schema(execution_store: Any) -> None:
    if not execution_store.schema_ready():
        raise RuntimeError(EXECUTION_SCHEMA_MESSAGE)


def _require_position_schema(execution_store: Any) -> None:
    if not execution_store.portfolio_schema_ready():
        raise RuntimeError(EXECUTION_SCHEMA_MESSAGE)


def _attempt_payload_with_lifecycle(
    attempt_payload: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    attempt_context = as_text(attempt_payload.get("attempt_context"))
    lifecycle = project_execution_attempt_lifecycle(
        attempt_payload,
        now=now,
    )
    execution_attempt_id = str(attempt_payload["execution_attempt_id"])
    payload = {
        **attempt_payload,
        "attempt_context": attempt_context,
        "order_intent_id": execution_attempt_id,
        "order_intent_key": _order_intent_key(execution_attempt_id),
        "execution_attempt_lifecycle": lifecycle,
        "lifecycle_state": lifecycle.get("lifecycle_state"),
        "lifecycle_phase": lifecycle.get("phase"),
        "broker_order_state": lifecycle.get("broker_order_state"),
        "next_action": lifecycle.get("next_action"),
        "stale": bool(lifecycle.get("stale")),
    }
    payload.pop("bucket", None)
    return payload


@with_storage()
def list_session_execution_attempts(
    *,
    db_target: str,
    session_id: str,
    limit: int = 20,
    execution_store: Any | None = None,
    storage: Any | None = None,
) -> list[dict[str, Any]]:
    resolved_execution_store = execution_store if execution_store is not None else storage.execution
    if not resolved_execution_store.schema_ready():
        return []
    activities = resolved_execution_store.list_attempt_activities(session_id=session_id, limit=limit)
    now = datetime.now(UTC)
    return [_attempt_payload_with_lifecycle(activity.to_payload(), now=now) for activity in activities]


def _get_attempt_payload(execution_store: Any, execution_attempt_id: str) -> dict[str, Any]:
    activity = execution_store.get_attempt_activity(execution_attempt_id)
    if activity is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    return _attempt_payload_with_lifecycle(
        activity.to_payload(),
        now=datetime.now(UTC),
    )


def _flatten_order_snapshot(
    order: dict[str, Any],
    *,
    parent_broker_order_id: str | None = None,
) -> list[dict[str, Any]]:
    broker_order_id = as_text(order.get("id"))
    if broker_order_id is None:
        raise ValueError("Broker order payload is missing an id")
    updated_at = as_text(order.get("updated_at")) or as_text(order.get("filled_at")) or as_text(order.get("submitted_at")) or utc_now_iso()
    symbol = as_text(order.get("symbol"))
    side = as_text(order.get("side"))
    rows = [
        {
            "broker": BROKER_NAME,
            "broker_order_id": broker_order_id,
            "parent_broker_order_id": parent_broker_order_id,
            "client_order_id": as_text(order.get("client_order_id")),
            "order_status": str(order.get("status") or "unknown"),
            "order_type": as_text(order.get("type")),
            "time_in_force": as_text(order.get("time_in_force")),
            "order_class": as_text(order.get("order_class")),
            "side": side,
            "symbol": symbol,
            "leg_symbol": symbol if parent_broker_order_id is not None else None,
            "leg_side": side if parent_broker_order_id is not None else None,
            "position_intent": as_text(order.get("position_intent")),
            "quantity": coerce_float(order.get("qty")),
            "limit_price": coerce_float(order.get("limit_price")),
            "filled_qty": coerce_float(order.get("filled_qty")),
            "filled_avg_price": coerce_float(order.get("filled_avg_price")),
            "submitted_at": as_text(order.get("submitted_at")),
            "updated_at": updated_at,
            "order": order,
        }
    ]
    for leg in order.get("legs") or []:
        if isinstance(leg, dict):
            rows.extend(_flatten_order_snapshot(leg, parent_broker_order_id=broker_order_id))
    return rows


def _sync_fill_rows(
    *,
    client: AlpacaClient,
    session_date: str,
    persisted_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    broker_order_ids = {str(order["broker_order_id"]) for order in persisted_orders}
    if not broker_order_ids:
        return []
    order_lookup = {str(order["broker_order_id"]): order for order in persisted_orders}
    activities = client.list_account_activities(activity_type="FILL", date=session_date)
    rows: list[dict[str, Any]] = []
    for activity in activities:
        broker_fill_id = as_text(activity.get("id"))
        broker_order_id = as_text(activity.get("order_id"))
        symbol = as_text(activity.get("symbol"))
        filled_at = as_text(activity.get("transaction_time"))
        quantity = coerce_float(activity.get("qty"))
        if (
            broker_fill_id is None
            or broker_order_id is None
            or broker_order_id not in broker_order_ids
            or symbol is None
            or filled_at is None
            or quantity is None
        ):
            continue
        matching_order = order_lookup.get(broker_order_id)
        rows.append(
            {
                "execution_order_id": None if matching_order is None else matching_order.get("execution_order_id"),
                "broker": BROKER_NAME,
                "broker_fill_id": broker_fill_id,
                "broker_order_id": broker_order_id,
                "symbol": symbol,
                "side": as_text(activity.get("side")),
                "fill_type": as_text(activity.get("type")),
                "quantity": quantity,
                "cumulative_quantity": coerce_float(activity.get("cum_qty")),
                "remaining_quantity": coerce_float(activity.get("leaves_qty")),
                "price": coerce_float(activity.get("price")),
                "filled_at": filled_at,
                "fill": activity,
            }
        )
    return rows


def _sync_attempt_state(
    *,
    execution_store: Any,
    attempt: dict[str, Any],
    client: AlpacaClient,
    order_snapshot: dict[str, Any],
) -> dict[str, Any]:
    order_rows = _flatten_order_snapshot(order_snapshot)
    persisted_orders = [
        dict(row)
        for row in execution_store.upsert_orders(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=order_rows,
        )
    ]
    try:
        fill_rows = _sync_fill_rows(
            client=client,
            session_date=str(attempt["session_date"]),
            persisted_orders=persisted_orders,
        )
    except Exception as exc:
        _log_attempt_failure(
            "execution_fill_sync_failed",
            attempt=attempt,
            exc=exc,
            broker_order_id=as_text(order_snapshot.get("id")) or attempt.get("broker_order_id"),
        )
        fill_rows = []
    if fill_rows:
        execution_store.upsert_fills(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=fill_rows,
        )

    status = str(order_snapshot.get("status") or attempt.get("status") or "unknown").lower()
    completed_at = _resolve_completed_at(order_snapshot) if is_terminal_execution_attempt_status(status) else None
    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        status=status,
        broker_order_id=as_text(order_snapshot.get("id")),
        client_order_id=as_text(order_snapshot.get("client_order_id")),
        submitted_at=as_text(order_snapshot.get("submitted_at")) or str(attempt["requested_at"]),
        completed_at=completed_at,
        error_text=None,
    )
    payload = _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))
    try:
        sync_session_position_from_attempt(
            execution_store=execution_store,
            attempt=payload,
        )
    except Exception as exc:
        _log_attempt_failure("execution_position_sync_failed", attempt=payload, exc=exc)
        raise
    return _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))


def _sync_equity_attempt_state(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    client: Any,
    order_snapshot: dict[str, Any],
) -> dict[str, Any]:
    order_rows = _flatten_order_snapshot(order_snapshot)
    persisted_orders = [
        dict(row)
        for row in execution_store.upsert_orders(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=order_rows,
        )
    ]
    try:
        fill_rows = _sync_fill_rows(
            client=client,
            session_date=str(attempt["session_date"]),
            persisted_orders=persisted_orders,
        )
    except Exception as exc:
        _log_attempt_failure(
            "execution_fill_sync_failed",
            attempt=attempt,
            exc=exc,
            broker_order_id=as_text(order_snapshot.get("id")) or attempt.get("broker_order_id"),
        )
        fill_rows = []
    if fill_rows:
        execution_store.upsert_fills(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=fill_rows,
        )

    status = str(order_snapshot.get("status") or attempt.get("status") or "unknown").lower()
    completed_at = _resolve_completed_at(order_snapshot) if is_terminal_execution_attempt_status(status) else None
    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        status=status,
        broker_order_id=as_text(order_snapshot.get("id")),
        client_order_id=as_text(order_snapshot.get("client_order_id")),
        submitted_at=as_text(order_snapshot.get("submitted_at")) or str(attempt["requested_at"]),
        completed_at=completed_at,
        error_text=None,
    )
    payload = _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))
    request = dict(payload.get("request") or {})
    should_sync_position = str(request.get("trade_intent") or "") == OPEN_TRADE_INTENT
    should_sync_position = should_sync_position or as_text(request.get("position_id")) is not None
    if should_sync_position:
        try:
            sync_session_position_from_attempt(
                execution_store=execution_store,
                attempt=payload,
            )
            payload = _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))
        except Exception as exc:
            _log_attempt_failure("execution_position_sync_failed", attempt=payload, exc=exc)
    return payload


def _publish_execution_attempt_event(attempt: dict[str, Any], *, message: str) -> None:
    try:
        publish_global_event_sync(
            topic="execution.attempt.updated",
            event_class="broker_event",
            entity_type="execution_attempt",
            entity_id=str(attempt["execution_attempt_id"]),
            payload={
                **attempt,
                "message": message,
            },
            timestamp=attempt.get("completed_at") or attempt.get("submitted_at") or attempt.get("requested_at") or utc_now_iso(),
            source="execution",
            session_date=as_text(attempt.get("session_date")),
            correlation_id=as_text(attempt.get("session_id")),
            causation_id=as_text(attempt.get("broker_order_id")),
        )
    except Exception as exc:
        _log_attempt_failure("execution_attempt_event_publish_failed", attempt=attempt, exc=exc, message=message)


def _linked_execution_intent_id(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    if not isinstance(request, Mapping):
        return None
    return as_text(request.get("execution_intent_id"))


def _intent_state_from_attempt_status(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"partially_filled"}:
        return "partially_filled"
    if normalized in {"filled"}:
        return "filled"
    if normalized in {"canceled", "cancelled"}:
        return "canceled"
    if normalized in {"expired", "revoked"}:
        return normalized
    if normalized in {"failed", "rejected"}:
        return "failed"
    if normalized in {"new", "accepted", "pending_new", "submitted"}:
        return "submitted"
    return "claimed"


def _sync_linked_execution_intent(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    state: str | None = None,
    event_type: str,
    message: str,
    payload_updates: dict[str, Any] | None = None,
) -> None:
    from core.services.execution_intents.shared import (
        sync_execution_intent_from_attempt,
        validate_execution_intent_transition,
    )

    execution_intent_id = _linked_execution_intent_id(attempt)
    if execution_intent_id is None or not execution_store.intent_schema_ready():
        return
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        return
    resolved_state = state or _intent_state_from_attempt_status(str(attempt.get("status") or ""))
    transition = validate_execution_intent_transition(
        intent.get("state"),
        resolved_state,
    )
    if not transition.allowed:
        resolved_state = str(intent.get("state") or "")
    lifecycle = attempt.get("execution_attempt_lifecycle")
    if not isinstance(lifecycle, Mapping):
        lifecycle = project_execution_attempt_lifecycle(
            attempt,
            now=datetime.now(UTC),
        )
    sync_execution_intent_from_attempt(
        execution_store,
        intent=dict(intent),
        attempt=dict(attempt),
        state=resolved_state,
        event_type=event_type,
        event_payload={
            "execution_attempt_id": as_text(attempt.get("execution_attempt_id")),
            "message": message,
            "attempt_status": str(attempt.get("status") or ""),
            "execution_attempt_lifecycle": dict(lifecycle),
        },
        payload_updates={
            "execution_attempt_lifecycle": dict(lifecycle),
            **({} if payload_updates is None else payload_updates),
        },
    )


def _submission_message(attempt: dict[str, Any], *, queued: bool) -> str:
    symbol_path = str(attempt.get("symbol_path") or "n/a")
    if str(attempt.get("trade_intent") or OPEN_TRADE_INTENT) == CLOSE_TRADE_INTENT:
        prefix = "Queued close for" if queued else "Submitted close for"
        return f"{prefix} {attempt['underlying_symbol']} {symbol_path}."
    prefix = "Queued" if queued else "Submitted"
    return f"{prefix} {attempt['underlying_symbol']} {attempt['strategy']} {symbol_path}."


def _queue_execution_attempt(
    *,
    job_store: Any,
    execution_store: Any,
    attempt: dict[str, Any],
) -> dict[str, Any]:
    _ = job_store
    execution_attempt_id = str(attempt["execution_attempt_id"])
    queued_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
    _publish_execution_attempt_event(
        queued_attempt,
        message=_submission_message(queued_attempt, queued=True),
    )
    return queued_attempt


def _reconcile_submit_unknown_attempt(
    *,
    execution_store: Any,
    attempt: Mapping[str, Any],
    client: AlpacaClient,
) -> dict[str, Any] | None:
    client_order_id = as_text(attempt.get("client_order_id"))
    if client_order_id is None:
        return None
    try:
        order_snapshot = client.get_order_by_client_order_id(
            client_order_id,
            nested=True,
        )
    except AlpacaRequestError as exc:
        if exc.status_code == 404:
            return None
        raise
    return _sync_attempt_state(
        execution_store=execution_store,
        attempt=dict(attempt),
        client=client,
        order_snapshot=order_snapshot,
    )
