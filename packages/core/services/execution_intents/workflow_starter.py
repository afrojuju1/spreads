from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy

from core.db.decorators import with_storage
from core.engine import EngineAggregateType, EngineEvent, EngineEventType, close_lifecycle_workflow_id, trade_lifecycle_workflow_id
from core.jobs.adhoc import enqueue_ad_hoc_job
from core.jobs.registry import EXECUTION_LIFECYCLE_START_ADHOC_JOB_KEY, EXECUTION_LIFECYCLE_START_JOB_TYPE
from core.runtime.config import default_temporal_address, default_temporal_namespace, default_temporal_task_queue
from core.services.alpaca import create_alpaca_client_from_env, resolve_trading_environment
from core.services.execution_intents.maintenance import _auto_execution_gate
from core.services.execution_intents.shared import _append_event, _intent_action_type, _intent_payload, _update_intent
from core.storage.serializers import parse_datetime
from core.value_coercion import as_text, utc_now_iso
from core.workflows.close_lifecycle import CloseLifecycleWorkflow
from core.workflows.contracts import CloseLifecycleWorkflowInput, TradeLifecycleWorkflowInput
from core.workflows.trade_lifecycle import TradeLifecycleWorkflow

PRE_WORKFLOW_START_EXPIRE_REASON = "workflow_start_window_elapsed"


def _workflow_kind(intent: dict[str, Any]) -> str:
    if as_text(intent.get("strategy_position_id")) is not None or _intent_action_type(intent) == "close":
        return "close"
    return "trade"


def _workflow_id(intent: dict[str, Any], *, workflow_kind: str) -> str:
    execution_intent_id = str(intent["execution_intent_id"])
    if workflow_kind == "close":
        position_id = as_text(intent.get("strategy_position_id")) or as_text(_intent_payload(intent).get("position_id")) or execution_intent_id
        return close_lifecycle_workflow_id(position_id, execution_intent_id)
    return trade_lifecycle_workflow_id(execution_intent_id)


async def _start_temporal_lifecycle_workflow(
    *,
    client: Client,
    intent: dict[str, Any],
    database_url: str,
    workflow_kind: str,
    workflow_id: str,
    task_queue: str,
) -> dict[str, Any]:
    execution_intent_id = str(intent["execution_intent_id"])
    requested_at = datetime.now(UTC)
    if workflow_kind == "close":
        position_id = as_text(intent.get("strategy_position_id")) or as_text(_intent_payload(intent).get("position_id"))
        if position_id is None:
            raise ValueError(f"Close lifecycle intent {execution_intent_id} is missing strategy_position_id")
        request = CloseLifecycleWorkflowInput(
            database_url=database_url,
            position_id=position_id,
            execution_intent_id=execution_intent_id,
            workflow_id=workflow_id,
            correlation_id=execution_intent_id,
            requested_at=requested_at,
            payload=_intent_payload(intent),
        )
        handle = await client.start_workflow(
            CloseLifecycleWorkflow.run,
            request.to_payload(),
            id=workflow_id,
            task_queue=task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
    else:
        request = TradeLifecycleWorkflowInput(
            database_url=database_url,
            execution_intent_id=execution_intent_id,
            workflow_id=workflow_id,
            correlation_id=execution_intent_id,
            requested_at=requested_at,
            payload=_intent_payload(intent),
        )
        handle = await client.start_workflow(
            TradeLifecycleWorkflow.run,
            request.to_payload(),
            id=workflow_id,
            task_queue=task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
    return {
        "workflow_id": handle.id,
        "workflow_run_id": getattr(handle, "first_execution_run_id", None),
        "workflow_kind": workflow_kind,
        "task_queue": task_queue,
        "started_at": requested_at.isoformat().replace("+00:00", "Z"),
    }


async def _connect_temporal(*, temporal_address: str, temporal_namespace: str) -> Client:
    return await Client.connect(temporal_address, namespace=temporal_namespace)


def _append_engine_event(
    storage: Any,
    *,
    intent: dict[str, Any],
    event_type: str,
    workflow_id: str,
    from_state: str | None,
    to_state: str | None,
    payload: dict[str, Any],
) -> None:
    engine_events = getattr(storage, "engine_events", None)
    if engine_events is None or not engine_events.schema_ready():
        return
    execution_intent_id = str(intent["execution_intent_id"])
    engine_events.append_engine_event(
        EngineEvent(
            event_type=event_type,
            aggregate_type=EngineAggregateType.EXECUTION_INTENT,
            aggregate_id=execution_intent_id,
            lifecycle_object="execution_intent",
            from_state=from_state,
            to_state=to_state,
            trading_strategy_id=as_text(intent.get("trading_strategy_id")),
            trade_signal_id=as_text(intent.get("trade_signal_id")),
            trade_decision_id=as_text(intent.get("trade_decision_id")),
            execution_intent_id=execution_intent_id,
            workflow_id=workflow_id,
            correlation_id=execution_intent_id,
            idempotency_key=f"{event_type}:{execution_intent_id}:{workflow_id}:{to_state or 'event'}",
            payload=payload,
            occurred_at=datetime.now(UTC),
        )
    )


def request_execution_lifecycle_start(
    *,
    job_store: Any,
    limit: int = 25,
    requested_by: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if job_store is None:
        return None
    if hasattr(job_store, "schema_ready") and not job_store.schema_ready():
        return None
    required_methods = ("create_job_run", "update_job_run_status")
    if any(not hasattr(job_store, method_name) for method_name in required_methods):
        return None
    scheduled_for = datetime.now(UTC)
    job_run_id = f"{EXECUTION_LIFECYCLE_START_ADHOC_JOB_KEY}:{uuid4().hex}"
    payload: dict[str, Any] = {
        "limit": max(int(limit), 1),
        "job_key": EXECUTION_LIFECYCLE_START_ADHOC_JOB_KEY,
        "job_type": EXECUTION_LIFECYCLE_START_JOB_TYPE,
        "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
        "singleton_scope": "global",
    }
    if requested_by:
        payload["requested_by"] = dict(requested_by)

    job_run, _ = job_store.create_job_run(
        job_run_id=job_run_id,
        job_key=EXECUTION_LIFECYCLE_START_ADHOC_JOB_KEY,
        arq_job_id=job_run_id,
        job_type=EXECUTION_LIFECYCLE_START_JOB_TYPE,
        status="queued",
        scheduled_for=scheduled_for,
        payload=payload,
    )
    try:
        enqueued = enqueue_ad_hoc_job(
            job_type=EXECUTION_LIFECYCLE_START_JOB_TYPE,
            job_key=EXECUTION_LIFECYCLE_START_ADHOC_JOB_KEY,
            job_run_id=job_run_id,
            arq_job_id=job_run_id,
            payload=payload,
        )
    except Exception as exc:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=scheduled_for,
            error_text=str(exc),
        )
        return {"status": "failed", "job_run_id": job_run_id, "error": str(exc)}
    if enqueued is None:
        message = "Execution lifecycle start job was not enqueued."
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=scheduled_for,
            error_text=message,
        )
        return {"status": "failed", "job_run_id": job_run_id, "error": message}
    return {
        "status": "queued",
        "job_run_id": str(job_run["job_run_id"]),
        "job_key": EXECUTION_LIFECYCLE_START_ADHOC_JOB_KEY,
    }


@with_storage()
def start_pending_execution_lifecycle_workflows(
    *,
    db_target: str,
    limit: int = 25,
    storage: Any | None = None,
) -> dict[str, Any]:
    database_url = str(getattr(storage, "database_url", db_target))
    execution_store = storage.execution
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}

    batch_limit = max(int(limit), 1)
    temporal_address = default_temporal_address()
    temporal_namespace = default_temporal_namespace()
    task_queue = default_temporal_task_queue()
    client = create_alpaca_client_from_env()
    trading_environment = resolve_trading_environment(client.trading_base_url)
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            states=["pending"],
            limit=batch_limit * 5,
        )
    ]
    intents.sort(key=lambda row: parse_datetime(as_text(row.get("created_at"))) or datetime.min.replace(tzinfo=UTC))
    started = 0
    skipped = 0
    expired = 0
    failed = 0
    reviewed = 0
    temporal_client: Client | None = None
    results: list[dict[str, Any]] = []
    for intent in intents:
        if reviewed >= batch_limit:
            break
        reviewed += 1
        execution_intent_id = str(intent["execution_intent_id"])
        current_state = as_text(intent.get("state"))
        expires_at = parse_datetime(as_text(intent.get("expires_at")))
        if expires_at is not None and expires_at <= datetime.now(UTC):
            updated = _update_intent(
                execution_store,
                intent,
                state="expired",
                payload_updates={
                    "workflow_start_status": "expired",
                    "expire_reason": PRE_WORKFLOW_START_EXPIRE_REASON,
                },
                updated_at=utc_now_iso(),
            )
            _append_event(
                execution_store,
                execution_intent_id=execution_intent_id,
                event_type="expired",
                payload={"reason": PRE_WORKFLOW_START_EXPIRE_REASON},
            )
            _append_engine_event(
                storage,
                intent=intent,
                event_type=EngineEventType.STATE_TRANSITIONED,
                workflow_id=_workflow_id(intent, workflow_kind=_workflow_kind(intent)),
                from_state=current_state,
                to_state="expired",
                payload={"reason": PRE_WORKFLOW_START_EXPIRE_REASON},
            )
            expired += 1
            results.append({"execution_intent_id": execution_intent_id, "status": "expired", "intent": updated})
            continue

        allowed, reason = _auto_execution_gate(intent=intent, trading_environment=trading_environment)
        if not allowed:
            skipped += 1
            results.append({"execution_intent_id": execution_intent_id, "status": "pending", "reason": reason})
            continue

        workflow_kind = _workflow_kind(intent)
        workflow_id = _workflow_id(intent, workflow_kind=workflow_kind)
        if temporal_client is None:
            try:
                temporal_client = asyncio.run(_connect_temporal(temporal_address=temporal_address, temporal_namespace=temporal_namespace))
            except Exception as exc:
                skipped += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "pending",
                        "reason": "temporal_unavailable",
                        "workflow_id": workflow_id,
                        "error": str(exc),
                    }
                )
                continue
        try:
            start_result = asyncio.run(
                _start_temporal_lifecycle_workflow(
                    client=temporal_client,
                    intent=intent,
                    database_url=database_url,
                    workflow_kind=workflow_kind,
                    workflow_id=workflow_id,
                    task_queue=task_queue,
                )
            )
        except Exception as exc:
            failed += 1
            _append_event(
                execution_store,
                execution_intent_id=execution_intent_id,
                event_type="workflow_start_failed",
                payload={
                    "workflow_id": workflow_id,
                    "workflow_kind": workflow_kind,
                    "temporal_address": temporal_address,
                    "temporal_namespace": temporal_namespace,
                    "task_queue": task_queue,
                    "error": str(exc),
                },
            )
            results.append(
                {
                    "execution_intent_id": execution_intent_id,
                    "status": "pending",
                    "reason": "temporal_workflow_start_failed",
                    "workflow_id": workflow_id,
                    "error": str(exc),
                }
            )
            continue

        claim_token = as_text(intent.get("claim_token")) or workflow_id
        claimed = _update_intent(
            execution_store,
            intent,
            state="claimed",
            claim_token=claim_token,
            payload_updates={
                "workflow_id": workflow_id,
                "workflow_kind": workflow_kind,
                "workflow_run_id": start_result.get("workflow_run_id"),
                "workflow_start_status": "started",
                "workflow_task_queue": task_queue,
                "workflow_started_at": start_result["started_at"],
            },
            updated_at=utc_now_iso(),
        )
        _append_event(
            execution_store,
            execution_intent_id=execution_intent_id,
            event_type="workflow_started",
            payload=start_result,
        )
        _append_engine_event(
            storage,
            intent=claimed,
            event_type=EngineEventType.WORKFLOW_STARTED,
            workflow_id=workflow_id,
            from_state=current_state,
            to_state="claimed",
            payload=start_result,
        )
        started += 1
        results.append(
            {
                "execution_intent_id": execution_intent_id,
                "status": "workflow_started",
                "workflow_id": workflow_id,
                "workflow_kind": workflow_kind,
                "intent": claimed,
            }
        )

    return {
        "status": "ok",
        "trading_environment": trading_environment,
        "temporal_address": temporal_address,
        "temporal_namespace": temporal_namespace,
        "task_queue": task_queue,
        "reviewed": reviewed,
        "started": started,
        "skipped": skipped,
        "expired": expired,
        "failed": failed,
        "results": results,
    }


__all__ = [
    "request_execution_lifecycle_start",
    "start_pending_execution_lifecycle_workflows",
]
