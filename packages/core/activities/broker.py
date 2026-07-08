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
from core.value_coercion import as_text


def _database_url(payload: Mapping[str, Any]) -> str:
    database_url = as_text(payload.get("database_url")) or as_text(payload.get("db_target"))
    if database_url is None:
        raise ValueError("Broker activity payload is missing database_url")
    return database_url


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
    database_url = _database_url(payload)
    execution_intent_id = as_text(payload.get("execution_intent_id"))
    if execution_intent_id is None:
        raise ValueError("Broker activity payload is missing execution_intent_id")
    return await asyncio.to_thread(
        ensure_execution_attempt_for_intent,
        db_target=database_url,
        execution_intent_id=execution_intent_id,
        workflow_id=as_text(payload.get("workflow_id")),
    )


@activity.defn(name="submit_execution_attempt_to_broker")
async def submit_execution_attempt_to_broker_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url(payload)
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
    return result


@activity.defn(name="refresh_execution_attempt")
async def refresh_execution_attempt_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url(payload)
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
    return result


@activity.defn(name="cancel_execution_attempt")
async def cancel_execution_attempt_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url(payload)
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
    return result


@activity.defn(name="create_repriced_execution_intent")
async def create_repriced_execution_intent_activity(payload: dict[str, Any]) -> dict[str, Any]:
    database_url = _database_url(payload)
    execution_intent_id = as_text(payload.get("execution_intent_id"))
    if execution_intent_id is None:
        raise ValueError("Broker activity payload is missing execution_intent_id")
    execution_attempt_id = _execution_attempt_id(payload)
    return await asyncio.to_thread(
        create_repriced_execution_intent,
        db_target=database_url,
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
