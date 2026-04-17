from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from arq import create_pool

from core.db.decorators import with_storage
from core.jobs.registry import (
    OPTIONS_AUTOMATION_EXECUTE_ADHOC_JOB_KEY,
    OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE,
    get_job_spec,
)
from core.runtime.config import default_redis_url
from core.runtime.redis import build_redis_settings
from core.services.alpaca import (
    create_alpaca_client_from_env,
    resolve_trading_environment,
)
from core.services.execution import (
    submit_opportunity_execution,
    submit_position_close_by_id,
)
from core.storage.serializers import parse_datetime

from .maintenance import (
    _auto_execution_gate,
    _backfill_strategy_position_links,
    _cleanup_slot_conflicts,
    _cleanup_stale_automation_opportunities,
    _cleanup_terminal_intent_history,
    _intent_execution_policy,
    _intent_exit_policy,
    _opportunity_is_active_for_intent,
    _position_is_active_for_intent,
)
from .repricing import _manage_submitted_open_intents
from .shared import (
    _append_event,
    _as_text,
    _attempt_state,
    _intent_action_type,
    _intent_payload,
    _update_intent,
    _utc_now,
)

PRE_DISPATCH_EXPIRE_REASON = "dispatch_window_elapsed"


def _ensure_options_automation_execute_job_definition(job_store: Any) -> None:
    job_store.upsert_job_definition(
        job_key=OPTIONS_AUTOMATION_EXECUTE_ADHOC_JOB_KEY,
        job_type=OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE,
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


def request_options_automation_dispatch(
    *,
    job_store: Any,
    limit: int = 25,
    requested_by: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if job_store is None:
        return None
    if hasattr(job_store, "schema_ready") and not job_store.schema_ready():
        return None
    required_methods = (
        "upsert_job_definition",
        "create_job_run",
        "update_job_run_status",
    )
    if any(not hasattr(job_store, method_name) for method_name in required_methods):
        return None

    _ensure_options_automation_execute_job_definition(job_store)
    scheduled_for = datetime.now(UTC)
    job_run_id = f"{OPTIONS_AUTOMATION_EXECUTE_ADHOC_JOB_KEY}:{uuid4().hex}"
    payload: dict[str, Any] = {
        "limit": max(int(limit), 1),
        "job_key": OPTIONS_AUTOMATION_EXECUTE_ADHOC_JOB_KEY,
        "job_type": OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE,
        "scheduled_for": scheduled_for.isoformat().replace("+00:00", "Z"),
        "singleton_scope": "global",
    }
    if requested_by:
        payload["requested_by"] = dict(requested_by)

    job_run, _ = job_store.create_job_run(
        job_run_id=job_run_id,
        job_key=OPTIONS_AUTOMATION_EXECUTE_ADHOC_JOB_KEY,
        arq_job_id=job_run_id,
        job_type=OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE,
        status="queued",
        scheduled_for=scheduled_for,
        payload=payload,
    )
    try:
        enqueued = _enqueue_ad_hoc_job(
            job_type=OPTIONS_AUTOMATION_EXECUTE_JOB_TYPE,
            job_key=OPTIONS_AUTOMATION_EXECUTE_ADHOC_JOB_KEY,
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
        return {
            "status": "failed",
            "job_run_id": job_run_id,
            "error": str(exc),
        }
    if enqueued is None:
        job_store.update_job_run_status(
            job_run_id=job_run_id,
            status="failed",
            expected_arq_job_id=job_run_id,
            finished_at=scheduled_for,
            error_text="Options automation execute job was not enqueued.",
        )
        return {
            "status": "failed",
            "job_run_id": job_run_id,
            "error": "Options automation execute job was not enqueued.",
        }
    return {
        "status": "queued",
        "job_run_id": str(job_run["job_run_id"]),
        "job_key": OPTIONS_AUTOMATION_EXECUTE_ADHOC_JOB_KEY,
    }


def _intent_target_is_active(
    *,
    intent: dict[str, Any],
    execution_store: Any,
    signal_store: Any,
) -> tuple[bool, str | None]:
    action_type = _intent_action_type(intent)
    if _as_text(intent.get("strategy_position_id")) is not None or action_type == "close":
        return _position_is_active_for_intent(execution_store, intent)
    if _as_text(intent.get("opportunity_decision_id")) is not None or action_type == "open":
        return _opportunity_is_active_for_intent(signal_store, intent)
    return False, "source_reference_missing"


@with_storage()
def submit_execution_intent(
    *,
    db_target: str,
    execution_intent_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    signal_store = storage.signals
    if not execution_store.intent_schema_ready():
        raise ValueError("Execution intent tables are not available yet.")
    intent = execution_store.get_execution_intent(execution_intent_id)
    if intent is None:
        raise ValueError(f"Unknown execution_intent_id: {execution_intent_id}")
    state = str(intent.get("state") or "")
    if state not in {"pending", "claimed"}:
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": "Execution intent is no longer pending submission.",
            "execution_intent": intent,
        }

    target_active, inactive_reason = _intent_target_is_active(
        intent=dict(intent),
        execution_store=execution_store,
        signal_store=signal_store,
    )
    if not target_active:
        revoked_intent = _update_intent(
            execution_store,
            dict(intent),
            state="revoked",
            payload_updates={
                "dispatch_status": "revoked",
                "revoke_reason": inactive_reason,
            },
            updated_at=_utc_now(),
        )
        _append_event(
            execution_store,
            execution_intent_id=execution_intent_id,
            event_type="revoked",
            payload={"reason": inactive_reason},
        )
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": (
                "Execution intent target is no longer active."
                if inactive_reason is None
                else f"Execution intent target is no longer active: {inactive_reason}"
            ),
            "execution_intent": revoked_intent,
        }

    claim_token = str(intent.get("claim_token") or uuid4().hex)
    claimed_intent = _update_intent(
        execution_store,
        dict(intent),
        state="claimed",
        payload_updates={"dispatch_status": "claimed"},
        updated_at=_utc_now(),
    )
    if not _as_text(claimed_intent.get("claim_token")):
        claimed_intent = execution_store.upsert_execution_intent(
            execution_intent_id=str(claimed_intent["execution_intent_id"]),
            bot_id=str(claimed_intent["bot_id"]),
            automation_id=str(claimed_intent["automation_id"]),
            opportunity_decision_id=_as_text(
                claimed_intent.get("opportunity_decision_id")
            ),
            strategy_position_id=_as_text(claimed_intent.get("strategy_position_id")),
            execution_attempt_id=_as_text(claimed_intent.get("execution_attempt_id")),
            action_type=str(claimed_intent["action_type"]),
            slot_key=str(claimed_intent["slot_key"]),
            claim_token=claim_token,
            policy_ref=dict(claimed_intent.get("policy_ref") or {}),
            config_hash=str(claimed_intent.get("config_hash") or ""),
            state="claimed",
            expires_at=_as_text(claimed_intent.get("expires_at")),
            superseded_by_id=_as_text(claimed_intent.get("superseded_by_id")),
            payload=_intent_payload(claimed_intent),
            created_at=str(claimed_intent["created_at"]),
            updated_at=_utc_now(),
        )
    _append_event(
        execution_store,
        execution_intent_id=execution_intent_id,
        event_type="claimed",
        payload={"claim_token": claim_token},
    )

    source_intent = dict(claimed_intent)
    payload = _intent_payload(source_intent)
    policy_ref = dict(source_intent.get("policy_ref") or {})
    execution_policy = _intent_execution_policy(source_intent)
    exit_policy = _intent_exit_policy(source_intent)

    try:
        if source_intent.get("opportunity_decision_id"):
            decision = signal_store.get_opportunity_decision(
                str(source_intent["opportunity_decision_id"])
            )
            if decision is None:
                raise ValueError(
                    f"Missing opportunity decision for execution intent {execution_intent_id}"
                )
            request_metadata = {
                "execution_intent_id": execution_intent_id,
                "bot_id": source_intent.get("bot_id"),
                "automation_id": source_intent.get("automation_id"),
                "strategy_config_id": policy_ref.get("strategy_config_id"),
                "strategy_id": policy_ref.get("strategy_id"),
                "config_hash": source_intent.get("config_hash"),
            }
            if execution_policy is not None:
                request_metadata["execution_policy"] = execution_policy
            if exit_policy is not None:
                request_metadata["exit_policy"] = exit_policy
            result = submit_opportunity_execution(
                db_target=db_target,
                opportunity_id=str(decision["opportunity_id"]),
                limit_price=(
                    None
                    if payload.get("limit_price") in (None, "")
                    else float(payload["limit_price"])
                ),
                request_metadata=request_metadata,
                storage=storage,
            )
        elif source_intent.get("strategy_position_id"):
            result = submit_position_close_by_id(
                db_target=db_target,
                position_id=str(source_intent["strategy_position_id"]),
                limit_price=(
                    None
                    if payload.get("limit_price") in (None, "")
                    else float(payload["limit_price"])
                ),
                request_metadata={
                    "execution_intent_id": execution_intent_id,
                    "bot_id": source_intent.get("bot_id"),
                    "automation_id": source_intent.get("automation_id"),
                    "strategy_config_id": policy_ref.get("strategy_config_id"),
                    "strategy_id": policy_ref.get("strategy_id"),
                    "config_hash": source_intent.get("config_hash"),
                },
                storage=storage,
            )
        else:
            raise ValueError(
                f"Execution intent {execution_intent_id} is missing its source reference"
            )
    except Exception as exc:
        failed_intent = _update_intent(
            execution_store,
            dict(claimed_intent),
            state="failed",
            payload_updates={"dispatch_status": "failed", "error": str(exc)},
            updated_at=_utc_now(),
        )
        _append_event(
            execution_store,
            execution_intent_id=execution_intent_id,
            event_type="failed",
            payload={"error": str(exc)},
        )
        return {
            "action": "submit_execution_intent",
            "changed": False,
            "message": str(exc),
            "execution_intent": failed_intent,
        }

    attempt = result.get("attempt") if isinstance(result.get("attempt"), dict) else None
    linked_attempt_id = (
        None if attempt is None else _as_text(attempt.get("execution_attempt_id"))
    )
    next_state = _attempt_state(attempt)
    linked_intent = _update_intent(
        execution_store,
        dict(claimed_intent),
        state=next_state,
        execution_attempt_id=linked_attempt_id,
        payload_updates={
            "dispatch_status": next_state,
            **(
                {}
                if linked_attempt_id is None
                else {"execution_attempt_id": linked_attempt_id}
            ),
        },
        updated_at=_utc_now(),
    )
    _append_event(
        execution_store,
        execution_intent_id=execution_intent_id,
        event_type="queued_for_submission"
        if linked_attempt_id is not None
        else "submit_noop",
        payload={
            "execution_attempt_id": linked_attempt_id,
            "attempt_status": None if attempt is None else attempt.get("status"),
            "changed": bool(result.get("changed", False)),
        },
    )
    return {
        "action": "submit_execution_intent",
        "changed": True,
        "result": result,
        "execution_intent": linked_intent,
    }


@with_storage()
def dispatch_pending_execution_intents(
    *,
    db_target: str,
    limit: int = 25,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.intent_schema_ready():
        return {"status": "skipped", "reason": "execution_intent_schema_unavailable"}

    batch_limit = max(int(limit), 1)
    market_date = datetime.now(UTC).date().isoformat()
    client = create_alpaca_client_from_env()
    trading_environment = resolve_trading_environment(client.trading_base_url)
    opportunity_cleanup = _cleanup_stale_automation_opportunities(
        signal_store=storage.signals,
        job_store=storage.jobs,
        market_date=market_date,
        limit=batch_limit,
    )
    intent_cleanup = _cleanup_terminal_intent_history(
        execution_store,
        limit=batch_limit,
    )
    position_linkage = _backfill_strategy_position_links(
        execution_store,
        limit=batch_limit,
    )
    slot_cleanup = _cleanup_slot_conflicts(
        execution_store,
        limit=batch_limit,
    )
    active_management = _manage_submitted_open_intents(
        db_target=db_target,
        storage=storage,
        execution_store=execution_store,
        limit=batch_limit,
    )
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            states=["pending"],
            limit=batch_limit * 5,
        )
    ]
    intents.sort(
        key=lambda row: parse_datetime(_as_text(row.get("created_at")))
        or datetime.min.replace(tzinfo=UTC)
    )
    submitted = 0
    skipped = 0
    expired = 0
    failed = 0
    reviewed = 0
    results: list[dict[str, Any]] = []
    for intent in intents:
        if reviewed >= batch_limit:
            break
        reviewed += 1
        execution_intent_id = str(intent["execution_intent_id"])
        expires_at = parse_datetime(_as_text(intent.get("expires_at")))
        if expires_at is not None and expires_at <= datetime.now(UTC):
            updated = _update_intent(
                execution_store,
                intent,
                state="expired",
                payload_updates={
                    "dispatch_status": "expired",
                    "expire_reason": PRE_DISPATCH_EXPIRE_REASON,
                },
                updated_at=_utc_now(),
            )
            _append_event(
                execution_store,
                execution_intent_id=execution_intent_id,
                event_type="expired",
                payload={"reason": PRE_DISPATCH_EXPIRE_REASON},
            )
            expired += 1
            results.append(
                {
                    "execution_intent_id": execution_intent_id,
                    "status": "expired",
                    "reason": PRE_DISPATCH_EXPIRE_REASON,
                    "intent": updated,
                }
            )
            continue

        allowed, reason = _auto_execution_gate(
            intent=intent,
            trading_environment=trading_environment,
        )
        if not allowed:
            if reason in {"bot_entry_cutoff_reached", "bot_live_disabled"}:
                updated = _update_intent(
                    execution_store,
                    intent,
                    state="revoked",
                    payload_updates={
                        "dispatch_status": "revoked",
                        "revoke_reason": reason,
                    },
                    updated_at=_utc_now(),
                )
                _append_event(
                    execution_store,
                    execution_intent_id=execution_intent_id,
                    event_type="revoked",
                    payload={"reason": reason},
                )
                skipped += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "revoked",
                        "intent": updated,
                    }
                )
            elif reason == "paper_execution_requires_paper_environment":
                updated = _update_intent(
                    execution_store,
                    intent,
                    state="failed",
                    payload_updates={"dispatch_status": reason},
                    updated_at=_utc_now(),
                )
                _append_event(
                    execution_store,
                    execution_intent_id=execution_intent_id,
                    event_type="failed",
                    payload={
                        "reason": reason,
                        "trading_environment": trading_environment,
                    },
                )
                failed += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "failed",
                        "intent": updated,
                    }
                )
            else:
                skipped += 1
                results.append(
                    {
                        "execution_intent_id": execution_intent_id,
                        "status": "pending",
                        "reason": reason,
                    }
                )
            continue

        result = submit_execution_intent(
            db_target=db_target,
            execution_intent_id=execution_intent_id,
            storage=storage,
        )
        final_intent = (
            result.get("execution_intent")
            if isinstance(result.get("execution_intent"), dict)
            else None
        )
        final_state = (
            None if final_intent is None else str(final_intent.get("state") or "")
        )
        if final_state == "failed":
            failed += 1
        elif final_state == "expired":
            expired += 1
        elif final_state in {"revoked", "pending", "claimed"}:
            skipped += 1
        else:
            submitted += 1
        results.append(
            {
                "execution_intent_id": execution_intent_id,
                "status": final_state or "submitted",
                "result": result,
            }
        )

    return {
        "status": "ok",
        "trading_environment": trading_environment,
        "opportunity_cleanup": opportunity_cleanup,
        "intent_cleanup": intent_cleanup,
        "position_linkage": position_linkage,
        "slot_cleanup": slot_cleanup,
        "active_management": active_management,
        "reviewed": reviewed,
        "submitted": submitted,
        "skipped": skipped,
        "expired": expired,
        "failed": failed,
        "results": results[:25],
    }


__all__ = [
    "PRE_DISPATCH_EXPIRE_REASON",
    "dispatch_pending_execution_intents",
    "request_options_automation_dispatch",
    "submit_execution_intent",
]
