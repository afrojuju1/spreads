from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from arq import create_pool

from core.db.decorators import with_storage
from core.events.bus import publish_global_event_sync
from core.integrations.alpaca.client import AlpacaClient, AlpacaRequestError
from core.jobs.registry import (
    EXECUTION_SUBMIT_ADHOC_JOB_KEY,
    EXECUTION_SUBMIT_JOB_TYPE,
    get_job_spec,
)
from core.runtime.config import default_redis_url
from core.runtime.redis import build_redis_settings
from core.services.session_positions import (
    CLOSE_TRADE_INTENT,
    OPEN_TRADE_INTENT,
    sync_session_position_from_attempt,
)
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    utc_now_iso as _utc_now,
)

from .shared import (
    BROKER_NAME,
    EXECUTION_SCHEMA_MESSAGE,
    _deprecated_bucket,
    _execution_submit_job_run_id,
    _is_terminal_status,
    _normalize_attempt_context,
    _order_intent_key,
    _resolve_completed_at,
)


def _require_execution_schema(execution_store: Any) -> None:
    if not execution_store.schema_ready():
        raise RuntimeError(EXECUTION_SCHEMA_MESSAGE)


def _require_position_schema(execution_store: Any) -> None:
    if not execution_store.portfolio_schema_ready():
        raise RuntimeError(EXECUTION_SCHEMA_MESSAGE)


def _ensure_execution_submit_job_definition(job_store: Any) -> None:
    job_store.upsert_job_definition(
        job_key=EXECUTION_SUBMIT_ADHOC_JOB_KEY,
        job_type=EXECUTION_SUBMIT_JOB_TYPE,
        enabled=False,
        schedule_type="manual",
        schedule={},
        payload={},
        singleton_scope=None,
    )


def _enqueue_ad_hoc_job(
    *,
    job_type: str,
    job_key: str,
    job_run_id: str,
    arq_job_id: str,
    payload: dict[str, Any],
) -> Any:
    spec = get_job_spec(job_type)
    if spec is None:
        raise RuntimeError(f"Job type is not registered: {job_type}")

    async def _enqueue() -> Any:
        redis = await create_pool(build_redis_settings(default_redis_url()))
        try:
            return await redis.enqueue_job(
                spec.task_name,
                job_key,
                job_run_id,
                payload,
                arq_job_id,
                _job_id=arq_job_id,
                _queue_name=spec.queue_name,
            )
        finally:
            await redis.aclose()

    return asyncio.run(_enqueue())


def _attach_attempt_details(
    *,
    execution_store: Any,
    attempts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not attempts:
        return []
    attempt_ids = [str(item["execution_attempt_id"]) for item in attempts]
    orders = execution_store.list_orders(execution_attempt_ids=attempt_ids)
    fills = execution_store.list_fills(execution_attempt_ids=attempt_ids)
    orders_by_attempt: dict[str, list[dict[str, Any]]] = {}
    fills_by_attempt: dict[str, list[dict[str, Any]]] = {}

    for order in orders:
        orders_by_attempt.setdefault(str(order["execution_attempt_id"]), []).append(
            dict(order)
        )
    for fill in fills:
        fills_by_attempt.setdefault(str(fill["execution_attempt_id"]), []).append(
            dict(fill)
        )

    payloads: list[dict[str, Any]] = []
    for attempt in attempts:
        attempt_context = _normalize_attempt_context(
            attempt.get("attempt_context", attempt.get("bucket"))
        )
        payloads.append(
            {
                **attempt,
                "attempt_context": attempt_context,
                "bucket": _deprecated_bucket(attempt_context),
                "order_intent_id": str(attempt["execution_attempt_id"]),
                "order_intent_key": _order_intent_key(
                    str(attempt["execution_attempt_id"])
                ),
                "orders": orders_by_attempt.get(
                    str(attempt["execution_attempt_id"]), []
                ),
                "fills": fills_by_attempt.get(str(attempt["execution_attempt_id"]), []),
            }
        )
    return payloads


@with_storage()
def list_session_execution_attempts(
    *,
    db_target: str,
    session_id: str,
    limit: int = 20,
    execution_store: Any | None = None,
    storage: Any | None = None,
) -> list[dict[str, Any]]:
    resolved_execution_store = (
        execution_store if execution_store is not None else storage.execution
    )
    if not resolved_execution_store.schema_ready():
        return []
    attempts = list(
        resolved_execution_store.list_attempts(session_id=session_id, limit=limit)
    )
    return _attach_attempt_details(
        execution_store=resolved_execution_store, attempts=attempts
    )


def _get_attempt_payload(
    execution_store: Any, execution_attempt_id: str
) -> dict[str, Any]:
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    return _attach_attempt_details(
        execution_store=execution_store, attempts=[dict(attempt)]
    )[0]


def _flatten_order_snapshot(
    order: dict[str, Any],
    *,
    parent_broker_order_id: str | None = None,
) -> list[dict[str, Any]]:
    broker_order_id = _as_text(order.get("id"))
    if broker_order_id is None:
        raise ValueError("Broker order payload is missing an id")
    updated_at = (
        _as_text(order.get("updated_at"))
        or _as_text(order.get("filled_at"))
        or _as_text(order.get("submitted_at"))
        or _utc_now()
    )
    symbol = _as_text(order.get("symbol"))
    side = _as_text(order.get("side"))
    rows = [
        {
            "broker": BROKER_NAME,
            "broker_order_id": broker_order_id,
            "parent_broker_order_id": parent_broker_order_id,
            "client_order_id": _as_text(order.get("client_order_id")),
            "order_status": str(order.get("status") or "unknown"),
            "order_type": _as_text(order.get("type")),
            "time_in_force": _as_text(order.get("time_in_force")),
            "order_class": _as_text(order.get("order_class")),
            "side": side,
            "symbol": symbol,
            "leg_symbol": symbol if parent_broker_order_id is not None else None,
            "leg_side": side if parent_broker_order_id is not None else None,
            "position_intent": _as_text(order.get("position_intent")),
            "quantity": _coerce_float(order.get("qty")),
            "limit_price": _coerce_float(order.get("limit_price")),
            "filled_qty": _coerce_float(order.get("filled_qty")),
            "filled_avg_price": _coerce_float(order.get("filled_avg_price")),
            "submitted_at": _as_text(order.get("submitted_at")),
            "updated_at": updated_at,
            "order": order,
        }
    ]
    for leg in order.get("legs") or []:
        if isinstance(leg, dict):
            rows.extend(
                _flatten_order_snapshot(leg, parent_broker_order_id=broker_order_id)
            )
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
        broker_fill_id = _as_text(activity.get("id"))
        broker_order_id = _as_text(activity.get("order_id"))
        symbol = _as_text(activity.get("symbol"))
        filled_at = _as_text(activity.get("transaction_time"))
        quantity = _coerce_float(activity.get("qty"))
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
                "execution_order_id": None
                if matching_order is None
                else matching_order.get("execution_order_id"),
                "broker": BROKER_NAME,
                "broker_fill_id": broker_fill_id,
                "broker_order_id": broker_order_id,
                "symbol": symbol,
                "side": _as_text(activity.get("side")),
                "fill_type": _as_text(activity.get("type")),
                "quantity": quantity,
                "cumulative_quantity": _coerce_float(activity.get("cum_qty")),
                "remaining_quantity": _coerce_float(activity.get("leaves_qty")),
                "price": _coerce_float(activity.get("price")),
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
    except Exception:
        fill_rows = []
    if fill_rows:
        execution_store.upsert_fills(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=fill_rows,
        )

    status = str(
        order_snapshot.get("status") or attempt.get("status") or "unknown"
    ).lower()
    completed_at = (
        _resolve_completed_at(order_snapshot) if _is_terminal_status(status) else None
    )
    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        status=status,
        broker_order_id=_as_text(order_snapshot.get("id")),
        client_order_id=_as_text(order_snapshot.get("client_order_id")),
        submitted_at=_as_text(order_snapshot.get("submitted_at"))
        or str(attempt["requested_at"]),
        completed_at=completed_at,
        error_text=None,
    )
    payload = _get_attempt_payload(
        execution_store, str(attempt["execution_attempt_id"])
    )
    sync_session_position_from_attempt(
        execution_store=execution_store,
        attempt=payload,
    )
    return _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))


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
            timestamp=attempt.get("completed_at")
            or attempt.get("submitted_at")
            or attempt.get("requested_at")
            or _utc_now(),
            source="execution",
            session_date=_as_text(attempt.get("session_date")),
            correlation_id=_as_text(attempt.get("session_id")),
            causation_id=_as_text(attempt.get("broker_order_id")),
        )
    except Exception:
        pass


def _linked_execution_intent_id(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    if not isinstance(request, Mapping):
        return None
    return _as_text(request.get("execution_intent_id"))


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
) -> None:
    execution_intent_id = _linked_execution_intent_id(attempt)
    if execution_intent_id is None or not execution_store.intent_schema_ready():
        return
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        return
    resolved_state = state or _intent_state_from_attempt_status(
        str(attempt.get("status") or "")
    )
    updated_at = _utc_now()
    strategy_position_id = _as_text(attempt.get("position_id")) or _as_text(
        intent.get("strategy_position_id")
    )
    execution_store.upsert_execution_intent(
        execution_intent_id=str(intent["execution_intent_id"]),
        bot_id=str(intent["bot_id"]),
        automation_id=str(intent["automation_id"]),
        opportunity_decision_id=_as_text(intent.get("opportunity_decision_id")),
        strategy_position_id=strategy_position_id,
        execution_attempt_id=_as_text(attempt.get("execution_attempt_id")),
        action_type=str(intent["action_type"]),
        slot_key=str(intent["slot_key"]),
        claim_token=_as_text(intent.get("claim_token")),
        policy_ref=dict(intent.get("policy_ref") or {}),
        config_hash=str(intent.get("config_hash") or ""),
        state=resolved_state,
        expires_at=_as_text(intent.get("expires_at")),
        superseded_by_id=_as_text(intent.get("superseded_by_id")),
        payload={
            **dict(intent.get("payload") or {}),
            "dispatch_status": resolved_state,
            "execution_attempt_id": _as_text(attempt.get("execution_attempt_id")),
            "attempt_status": str(attempt.get("status") or ""),
            **(
                {}
                if strategy_position_id is None
                else {"strategy_position_id": strategy_position_id}
            ),
        },
        created_at=str(intent["created_at"]),
        updated_at=updated_at,
    )
    execution_store.append_execution_intent_event(
        execution_intent_id=execution_intent_id,
        event_type=event_type,
        event_at=updated_at,
        payload={
            "execution_attempt_id": _as_text(attempt.get("execution_attempt_id")),
            "message": message,
            "attempt_status": str(attempt.get("status") or ""),
        },
    )


def _publish_risk_decision_event(risk_decision: dict[str, Any]) -> None:
    try:
        publish_global_event_sync(
            topic="risk.decision.recorded",
            event_class="risk_event",
            entity_type="risk_decision",
            entity_id=str(risk_decision["risk_decision_id"]),
            payload=risk_decision,
            timestamp=risk_decision.get("decided_at") or _utc_now(),
            source="execution",
            session_date=_as_text(risk_decision.get("session_date")),
            correlation_id=_as_text(risk_decision.get("opportunity_id"))
            or _as_text(risk_decision.get("session_id")),
            causation_id=_as_text(risk_decision.get("candidate_id")),
        )
    except Exception:
        pass


def _submission_message(attempt: dict[str, Any], *, queued: bool) -> str:
    if str(attempt.get("trade_intent") or OPEN_TRADE_INTENT) == CLOSE_TRADE_INTENT:
        prefix = "Queued close for" if queued else "Submitted close for"
        return (
            f"{prefix} {attempt['underlying_symbol']} "
            f"{attempt['short_symbol']} / {attempt['long_symbol']}."
        )
    prefix = "Queued" if queued else "Submitted"
    return (
        f"{prefix} {attempt['underlying_symbol']} {attempt['strategy']} "
        f"{attempt['short_symbol']} / {attempt['long_symbol']}."
    )


def _queue_execution_attempt(
    *,
    job_store: Any,
    execution_store: Any,
    attempt: dict[str, Any],
) -> dict[str, Any]:
    _ensure_execution_submit_job_definition(job_store)
    execution_attempt_id = str(attempt["execution_attempt_id"])
    job_run_id = _execution_submit_job_run_id(execution_attempt_id)
    scheduled_for = datetime.now(UTC)
    payload = {
        "execution_attempt_id": execution_attempt_id,
        "session_id": str(attempt["session_id"]),
        "trade_intent": str(attempt["trade_intent"]),
        "job_key": EXECUTION_SUBMIT_ADHOC_JOB_KEY,
        "job_type": EXECUTION_SUBMIT_JOB_TYPE,
        "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
    }
    job_run, _ = job_store.create_job_run(
        job_run_id=job_run_id,
        job_key=EXECUTION_SUBMIT_ADHOC_JOB_KEY,
        arq_job_id=job_run_id,
        job_type=EXECUTION_SUBMIT_JOB_TYPE,
        status="queued",
        scheduled_for=scheduled_for,
        session_id=str(attempt["session_id"]),
        payload=payload,
    )
    try:
        enqueued = _enqueue_ad_hoc_job(
            job_type=EXECUTION_SUBMIT_JOB_TYPE,
            job_key=EXECUTION_SUBMIT_ADHOC_JOB_KEY,
            job_run_id=job_run_id,
            arq_job_id=job_run_id,
            payload=payload,
        )
    except Exception as exc:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=datetime.now(UTC),
            error_text=str(exc),
        )
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="failed",
            completed_at=_utc_now(),
            error_text=str(exc),
            position_id=_as_text(attempt.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message=f"Execution queueing failed before submission: {exc}",
        )
        raise RuntimeError(f"Execution queueing failed: {exc}") from exc
    if enqueued is None:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=datetime.now(UTC),
            error_text="Execution submit job was not enqueued.",
        )
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="failed",
            completed_at=_utc_now(),
            error_text="Execution submit job was not enqueued.",
            position_id=_as_text(attempt.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message="Execution queueing failed before submission: job was not enqueued.",
        )
        raise RuntimeError("Execution queueing failed: job was not enqueued.")
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
    client_order_id = _as_text(attempt.get("client_order_id"))
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
