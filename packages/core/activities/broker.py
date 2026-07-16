from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from core.engine import EngineAggregateType, EngineEvent, EngineEventType
from core.services.execution.attempts import _get_attempt_payload
from core.services.execution.submit import submit_execution_attempt_to_broker
from core.services.execution.sync import cancel_execution_attempt, refresh_execution_attempt
from core.services.execution_intents.attempt_planner import ensure_execution_attempt_for_intent
from core.services.execution_intents.repricing import create_repriced_execution_intent
from core.runtime.config import default_database_url
from core.value_coercion import as_mapping, as_text, coerce_int
from core.workflows.contracts import LifecycleActivityResult


def _database_url() -> str:
    return default_database_url()


def _bounded_text(value: Any, *, limit: int = 512) -> str | None:
    text = as_text(value)
    return None if text is None else text[:limit]


def _positive_int(value: Any, default: int) -> int:
    parsed = coerce_int(value)
    return default if parsed is None or parsed <= 0 else parsed


def _compact_lifecycle_result(
    result: Mapping[str, Any],
    *,
    attempt: Mapping[str, Any] | None = None,
    execution_intent_id: str | None = None,
    execution_attempt_id: str | None = None,
) -> dict[str, Any]:
    attempt_payload = dict(attempt or as_mapping(result.get("attempt")))
    request = as_mapping(attempt_payload.get("request"))
    execution_policy = as_mapping(request.get("execution_policy"))
    repricing_policy = as_mapping(request.get("repricing_policy"))
    if not repricing_policy:
        repricing_policy = as_mapping(execution_policy.get("repricing_policy"))
    stale_order_action = str(
        execution_policy.get("stale_order_action")
        or repricing_policy.get("stale_order_action")
        or "cancel_and_reprice"
    ).strip().lower()
    if stale_order_action not in {"cancel_and_reprice", "fail_closed", "leave_working"}:
        stale_order_action = "cancel_and_reprice"
    resolved_intent_id = (
        as_text(result.get("execution_intent_id"))
        or as_text(attempt_payload.get("execution_intent_id"))
        or as_text(request.get("execution_intent_id"))
        or execution_intent_id
    )
    resolved_attempt_id = (
        as_text(result.get("execution_attempt_id"))
        or as_text(attempt_payload.get("execution_attempt_id"))
        or execution_attempt_id
    )
    attempt_status = as_text(result.get("attempt_status")) or as_text(attempt_payload.get("status"))
    if resolved_intent_id is None:
        raise ValueError("Lifecycle Activity result is missing execution_intent_id")
    return LifecycleActivityResult(
        status=_bounded_text(result.get("status") or result.get("action") or attempt_status or "ok", limit=64) or "ok",
        execution_intent_id=resolved_intent_id,
        execution_attempt_id=resolved_attempt_id,
        replacement_execution_intent_id=as_text(result.get("replacement_execution_intent_id")),
        position_id=(
            as_text(result.get("position_id"))
            or as_text(attempt_payload.get("position_id"))
            or as_text(request.get("position_id"))
        ),
        attempt_status=_bounded_text(attempt_status, limit=64),
        requested_at=attempt_payload.get("requested_at"),
        submitted_at=attempt_payload.get("submitted_at"),
        completed_at=attempt_payload.get("completed_at"),
        reason=_bounded_text(result.get("reason")),
        submit_ttl_seconds=min(_positive_int(execution_policy.get("submit_ttl_minutes"), 5) * 60, 86_400),
        stale_after_seconds=min(_positive_int(repricing_policy.get("stale_after_seconds"), 75), 86_400),
        stale_order_action=stale_order_action,
    ).to_payload()


def _execution_attempt_id(payload: Mapping[str, Any]) -> str:
    execution_attempt_id = as_text(payload.get("execution_attempt_id"))
    if execution_attempt_id is None:
        raise ValueError("Broker activity payload is missing execution_attempt_id")
    return execution_attempt_id


def _append_activity_event(
    *,
    db_target: str,
    event_type: str,
    attempt: Mapping[str, Any],
    workflow_id: str | None,
    payload: dict[str, Any],
) -> None:
    from core.storage.factory import build_storage_context

    with build_storage_context(db_target) as storage:
        engine_events = getattr(storage, "engine_events", None)
        if engine_events is None or not engine_events.schema_ready():
            return
        request = attempt.get("request") if isinstance(attempt.get("request"), Mapping) else {}
        execution_attempt_id = str(attempt["execution_attempt_id"])
        execution_intent_id = as_text(request.get("execution_intent_id")) or as_text(attempt.get("execution_intent_id"))
        engine_events.append_engine_event(
            EngineEvent(
                event_type=event_type,
                aggregate_type=EngineAggregateType.EXECUTION_ATTEMPT,
                aggregate_id=execution_attempt_id,
                lifecycle_object="execution_attempt",
                to_state=as_text(attempt.get("status")),
                trading_strategy_id=as_text(attempt.get("trading_strategy_id")) or as_text(request.get("trading_strategy_id")),
                trade_signal_id=as_text(request.get("trade_signal_id")),
                trade_decision_id=as_text(request.get("trade_decision_id")),
                execution_intent_id=execution_intent_id,
                execution_attempt_id=execution_attempt_id,
                broker_order_id=as_text(attempt.get("broker_order_id")),
                position_id=as_text(attempt.get("position_id")) or as_text(request.get("position_id")),
                workflow_id=workflow_id,
                correlation_id=execution_intent_id or execution_attempt_id,
                idempotency_key=(
                    f"{event_type}:{execution_attempt_id}:"
                    f"{as_text(attempt.get('broker_order_id')) or 'local'}:{as_text(attempt.get('status')) or 'unknown'}"
                ),
                payload=payload,
                occurred_at=datetime.now(UTC),
            )
        )


def _load_attempt(*, db_target: str, execution_attempt_id: str) -> dict[str, Any]:
    from core.storage.factory import build_storage_context

    with build_storage_context(db_target) as storage:
        return _get_attempt_payload(storage.execution, execution_attempt_id)


@activity.defn(name="ensure_execution_attempt_for_intent")
async def ensure_execution_attempt_for_intent_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url()
    execution_intent_id = as_text(payload.get("execution_intent_id"))
    if execution_intent_id is None:
        raise ValueError("Broker activity payload is missing execution_intent_id")
    result = await asyncio.to_thread(
        ensure_execution_attempt_for_intent,
        db_target=database_url,
        execution_intent_id=execution_intent_id,
        workflow_id=as_text(payload.get("workflow_id")),
    )
    return _compact_lifecycle_result(result, execution_intent_id=execution_intent_id)


@activity.defn(name="submit_execution_attempt_to_broker")
async def submit_execution_attempt_to_broker_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url()
    execution_attempt_id = _execution_attempt_id(payload)
    workflow_id = as_text(payload.get("workflow_id"))
    attempt_before = await asyncio.to_thread(_load_attempt, db_target=database_url, execution_attempt_id=execution_attempt_id)
    await asyncio.to_thread(
        _append_activity_event,
        db_target=database_url,
        event_type=EngineEventType.BROKER_SUBMISSION_REQUESTED,
        attempt=attempt_before,
        workflow_id=workflow_id,
        payload={"status": "submit_activity_started"},
    )
    activity.heartbeat()
    try:
        result = await asyncio.to_thread(
            submit_execution_attempt_to_broker,
            db_target=database_url,
            execution_attempt_id=execution_attempt_id,
            heartbeat=None,
        )
    except Exception:
        latest_attempt = await asyncio.to_thread(_load_attempt, db_target=database_url, execution_attempt_id=execution_attempt_id)
        event_type = (
            EngineEventType.BROKER_SUBMISSION_UNKNOWN
            if str(latest_attempt.get("status") or "").strip().lower() == "submit_unknown"
            else EngineEventType.STATE_TRANSITIONED
        )
        await asyncio.to_thread(
            _append_activity_event,
            db_target=database_url,
            event_type=event_type,
            attempt=latest_attempt,
            workflow_id=workflow_id,
            payload={"status": "submit_activity_failed"},
        )
        raise
    activity.heartbeat()
    attempt = dict(result.get("attempt") or await asyncio.to_thread(_load_attempt, db_target=database_url, execution_attempt_id=execution_attempt_id))
    await asyncio.to_thread(
        _append_activity_event,
        db_target=database_url,
        event_type=EngineEventType.BROKER_ORDER_OBSERVED,
        attempt=attempt,
        workflow_id=workflow_id,
        payload={"status": result.get("status"), "message": result.get("message"), "reason": result.get("reason")},
    )
    return _compact_lifecycle_result(result, attempt=attempt, execution_attempt_id=execution_attempt_id)


@activity.defn(name="refresh_execution_attempt")
async def refresh_execution_attempt_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url()
    execution_attempt_id = _execution_attempt_id(payload)
    result = await asyncio.to_thread(
        refresh_execution_attempt,
        db_target=database_url,
        execution_attempt_id=execution_attempt_id,
    )
    attempt = dict(result.get("attempt") or {})
    if attempt:
        await asyncio.to_thread(
            _append_activity_event,
            db_target=database_url,
            event_type=EngineEventType.BROKER_ORDER_OBSERVED,
            attempt=attempt,
            workflow_id=as_text(payload.get("workflow_id")),
            payload={"status": "refresh_activity_completed", "changed": result.get("changed")},
        )
    return _compact_lifecycle_result(result, attempt=attempt, execution_attempt_id=execution_attempt_id)


@activity.defn(name="cancel_execution_attempt")
async def cancel_execution_attempt_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url()
    execution_attempt_id = _execution_attempt_id(payload)
    result = await asyncio.to_thread(
        cancel_execution_attempt,
        db_target=database_url,
        execution_attempt_id=execution_attempt_id,
    )
    attempt = dict(result.get("attempt") or {})
    if attempt:
        await asyncio.to_thread(
            _append_activity_event,
            db_target=database_url,
            event_type=EngineEventType.STATE_TRANSITIONED,
            attempt=attempt,
            workflow_id=as_text(payload.get("workflow_id")),
            payload={"status": "cancel_activity_completed", "changed": result.get("changed")},
        )
    return _compact_lifecycle_result(result, attempt=attempt, execution_attempt_id=execution_attempt_id)


@activity.defn(name="create_repriced_execution_intent")
async def create_repriced_execution_intent_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url()
    execution_intent_id = as_text(payload.get("execution_intent_id"))
    if execution_intent_id is None:
        raise ValueError("Broker activity payload is missing execution_intent_id")
    execution_attempt_id = _execution_attempt_id(payload)
    result = await asyncio.to_thread(
        create_repriced_execution_intent,
        db_target=database_url,
        execution_intent_id=execution_intent_id,
        execution_attempt_id=execution_attempt_id,
    )
    return _compact_lifecycle_result(
        result,
        execution_intent_id=execution_intent_id,
        execution_attempt_id=execution_attempt_id,
    )


__all__ = [
    "cancel_execution_attempt_activity",
    "create_repriced_execution_intent_activity",
    "ensure_execution_attempt_for_intent_activity",
    "refresh_execution_attempt_activity",
    "submit_execution_attempt_to_broker_activity",
]
