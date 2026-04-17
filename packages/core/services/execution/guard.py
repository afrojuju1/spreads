from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.integrations.alpaca.client import AlpacaClient
from core.services.alpaca import create_alpaca_client_from_env
from core.services.candidate_policy import resolve_candidate_profile
from core.services.execution_lifecycle import (
    PENDING_SUBMISSION_GRACE_SECONDS,
    PENDING_SUBMISSION_RUNNING_STALE_AFTER_SECONDS,
    SUBMIT_UNKNOWN_STATUS,
    classify_open_execution_attempt,
    resolve_execution_attempt_source_job,
    resolve_execution_submit_job_run_id,
)
from core.services.session_positions import OPEN_TRADE_INTENT
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)

from .attempts import (
    _get_attempt_payload,
    _publish_execution_attempt_event,
    _sync_attempt_state,
)
from .policy import _attempt_exit_policy, _validate_open_timing_window
from .shared import OPEN_STATUSES, _is_terminal_status


def _execution_submit_job_run(
    storage: Any, execution_attempt_id: str
) -> Mapping[str, Any] | None:
    job_store = getattr(storage, "jobs", None)
    if job_store is None or (
        hasattr(job_store, "schema_ready") and not job_store.schema_ready()
    ):
        return None
    try:
        return job_store.get_job_run(
            resolve_execution_submit_job_run_id(execution_attempt_id)
        )
    except Exception:
        return None


def _source_job_definition(
    storage: Any, attempt: Mapping[str, Any]
) -> Mapping[str, Any] | None:
    source_job = resolve_execution_attempt_source_job(attempt)
    source_job_key = _as_text(source_job.get("job_key"))
    job_store = getattr(storage, "jobs", None)
    if (
        source_job_key is None
        or job_store is None
        or (hasattr(job_store, "schema_ready") and not job_store.schema_ready())
    ):
        return None
    try:
        return job_store.get_job_definition(source_job_key)
    except Exception:
        return None


def _attempt_request(attempt: Mapping[str, Any]) -> Mapping[str, Any]:
    request = attempt.get("request")
    return request if isinstance(request, Mapping) else {}


def _evaluate_open_attempt_guard(
    *,
    storage: Any,
    attempt: Mapping[str, Any],
    current_time: datetime,
) -> dict[str, Any]:
    lifecycle = classify_open_execution_attempt(
        attempt,
        now=current_time,
        submit_job=_execution_submit_job_run(
            storage, str(attempt.get("execution_attempt_id") or "")
        ),
        source_job_definition=_source_job_definition(storage, attempt),
        submission_grace_seconds=PENDING_SUBMISSION_GRACE_SECONDS,
        running_submit_stale_after_seconds=PENDING_SUBMISSION_RUNNING_STALE_AFTER_SECONDS,
    )
    request = _attempt_request(attempt)
    execution_policy = (
        request.get("execution_policy")
        if isinstance(request.get("execution_policy"), Mapping)
        else {}
    )
    timing_gate = _validate_open_timing_window(
        exit_policy=_attempt_exit_policy(attempt),
        current_time=current_time,
        profile=resolve_candidate_profile(dict(attempt.get("candidate") or {})),
        deployment_mode=str(execution_policy.get("deployment_mode") or ""),
    )
    if str(lifecycle.get("phase") or "") == "submit_unknown":
        return {
            **dict(timing_gate),
            "lifecycle": lifecycle,
            "age_seconds": lifecycle.get("age_seconds"),
            "stale_after_seconds": lifecycle.get("working_stale_after_seconds"),
        }
    if not timing_gate["allowed"]:
        return {
            **dict(timing_gate),
            "lifecycle": lifecycle,
            "intervention": "cancel_order"
            if _as_text(attempt.get("broker_order_id"))
            else "fail_unsubmitted",
        }

    intervention = _as_text(lifecycle.get("intervention"))
    if intervention is None:
        return {
            **dict(timing_gate),
            "lifecycle": lifecycle,
            "age_seconds": lifecycle.get("age_seconds"),
            "stale_after_seconds": lifecycle.get("working_stale_after_seconds"),
        }

    reason = (
        "stale_auto_open_attempt"
        if intervention == "cancel_order"
        else (
            "submit_outcome_uncertain"
            if intervention == "mark_submit_unknown"
            else "stale_pending_submission"
        )
    )
    return {
        "allowed": False,
        "reason": reason,
        "message": _as_text(lifecycle.get("note")),
        "force_close_at": timing_gate.get("force_close_at"),
        "age_seconds": lifecycle.get("age_seconds"),
        "stale_after_seconds": lifecycle.get("working_stale_after_seconds"),
        "queue_grace_seconds": lifecycle.get("submission_grace_seconds"),
        "submit_job_status": lifecycle.get("submit_job_status"),
        "intervention": intervention,
        "lifecycle": lifecycle,
    }


def _guard_intervention_message(
    guard_decision: Mapping[str, Any],
    *,
    submitted: bool,
) -> str:
    reason = _as_text(guard_decision.get("reason"))
    direct_message = _as_text(guard_decision.get("message"))
    if reason in {"stale_pending_submission", "submit_outcome_uncertain"}:
        return direct_message or "Execution needs operator reconciliation."
    if reason == "stale_auto_open_attempt":
        age_seconds = _coerce_float(guard_decision.get("age_seconds"))
        stale_after_seconds = _coerce_int(guard_decision.get("stale_after_seconds"))
        age_fragment = (
            "" if age_seconds is None else f" after {int(round(age_seconds))}s"
        )
        threshold_fragment = (
            ""
            if stale_after_seconds is None
            else f" (stale threshold {stale_after_seconds}s)"
        )
        if submitted:
            return (
                "Canceled automatic open execution because the order remained pending"
                f"{age_fragment}{threshold_fragment}."
            )
        return (
            "Automatic open execution expired before broker submission because it remained pending"
            f"{age_fragment}{threshold_fragment}."
        )
    if reason == "insufficient_time_to_force_close":
        minimum_minutes = _coerce_float(
            guard_decision.get("minimum_minutes_to_force_close")
        )
        minutes_remaining = _coerce_float(guard_decision.get("minutes_to_force_close"))
        threshold_fragment = (
            ""
            if minimum_minutes is None
            else f" below the {minimum_minutes:.1f}-minute threshold"
        )
        remaining_fragment = (
            ""
            if minutes_remaining is None
            else f" with {minutes_remaining:.1f} minutes remaining"
        )
        if submitted:
            return (
                "Canceled open execution because remaining time to force-close fell"
                f"{threshold_fragment}{remaining_fragment}."
            )
        return (
            "Open execution expired before broker submission because remaining time to"
            f" force-close fell{threshold_fragment}{remaining_fragment}."
        )

    if submitted:
        return (
            "Canceled open execution because the exit force-close window "
            "started before the order completed."
        )
    return (
        "Open execution expired because the exit force-close window started "
        "before broker submission."
    )


@with_storage()
def run_open_execution_guard(
    *,
    db_target: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.schema_ready():
        return {
            "status": "skipped",
            "reason": "execution_schema_unavailable",
        }

    open_attempts = [
        dict(attempt)
        for attempt in execution_store.list_attempts_by_status(
            statuses=sorted(OPEN_STATUSES),
            trade_intent=OPEN_TRADE_INTENT,
            limit=200,
        )
    ]
    if not open_attempts:
        return {
            "status": "ok",
            "open_attempt_count": 0,
            "evaluated": 0,
            "canceled": 0,
            "failed_unsubmitted": 0,
            "submit_unknown": 0,
            "terminal_synced": 0,
            "skipped": 0,
            "failure_count": 0,
            "decisions": [],
            "failures": [],
        }

    now = datetime.now(UTC)
    client: AlpacaClient | None = None
    evaluated = 0
    canceled = 0
    failed_unsubmitted = 0
    submit_unknown = 0
    terminal_synced = 0
    skipped = 0
    failures: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    for attempt in open_attempts:
        execution_attempt_id = str(attempt["execution_attempt_id"])
        guard_decision = _evaluate_open_attempt_guard(
            storage=storage,
            attempt=attempt,
            current_time=now,
        )
        if guard_decision["allowed"]:
            skipped += 1
            continue

        evaluated += 1
        broker_order_id = _as_text(attempt.get("broker_order_id"))
        position_id = _as_text(attempt.get("position_id"))
        intervention = _as_text(guard_decision.get("intervention"))
        if intervention == "mark_submit_unknown":
            message = _guard_intervention_message(
                guard_decision,
                submitted=False,
            )
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status=SUBMIT_UNKNOWN_STATUS,
                error_text=message,
                position_id=position_id,
            )
            uncertain_attempt = _get_attempt_payload(
                execution_store, execution_attempt_id
            )
            _publish_execution_attempt_event(
                uncertain_attempt,
                message=message,
            )
            submit_unknown += 1
            decisions.append(
                {
                    "execution_attempt_id": execution_attempt_id,
                    "symbol": str(attempt.get("underlying_symbol") or ""),
                    "action": "marked_submit_unknown",
                    "status": str(uncertain_attempt.get("status") or ""),
                    "reason": str(guard_decision["reason"] or ""),
                    "age_seconds": guard_decision.get("age_seconds"),
                    "stale_after_seconds": guard_decision.get("stale_after_seconds"),
                    "submit_job_status": guard_decision.get("submit_job_status"),
                }
            )
            continue
        if broker_order_id is None:
            message = _guard_intervention_message(
                guard_decision,
                submitted=False,
            )
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=_utc_now(),
                error_text=message,
                position_id=position_id,
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=message,
            )
            failed_unsubmitted += 1
            decisions.append(
                {
                    "execution_attempt_id": execution_attempt_id,
                    "symbol": str(attempt.get("underlying_symbol") or ""),
                    "action": "failed_unsubmitted",
                    "status": str(failed_attempt.get("status") or ""),
                    "reason": str(guard_decision["reason"] or ""),
                    "age_seconds": guard_decision.get("age_seconds"),
                    "stale_after_seconds": guard_decision.get("stale_after_seconds"),
                }
            )
            continue

        try:
            if client is None:
                client = create_alpaca_client_from_env()
            order_snapshot = client.get_order(broker_order_id, nested=True)
            synced_attempt = _sync_attempt_state(
                execution_store=execution_store,
                attempt=attempt,
                client=client,
                order_snapshot=order_snapshot,
            )
            current_status = str(synced_attempt.get("status") or "").lower()
            if _is_terminal_status(current_status):
                terminal_synced += 1
                decisions.append(
                    {
                        "execution_attempt_id": execution_attempt_id,
                        "symbol": str(synced_attempt.get("underlying_symbol") or ""),
                        "action": "synced_terminal",
                        "status": current_status,
                        "reason": str(guard_decision["reason"] or ""),
                        "age_seconds": guard_decision.get("age_seconds"),
                        "stale_after_seconds": guard_decision.get(
                            "stale_after_seconds"
                        ),
                    }
                )
                continue

            if current_status != "pending_cancel":
                client.cancel_order(broker_order_id)
                try:
                    cancel_snapshot = client.get_order(broker_order_id, nested=True)
                    synced_attempt = _sync_attempt_state(
                        execution_store=execution_store,
                        attempt=synced_attempt,
                        client=client,
                        order_snapshot=cancel_snapshot,
                    )
                except Exception:
                    execution_store.update_attempt(
                        execution_attempt_id=execution_attempt_id,
                        status="pending_cancel",
                        position_id=position_id,
                    )
                    synced_attempt = _get_attempt_payload(
                        execution_store, execution_attempt_id
                    )
                canceled += 1
                _publish_execution_attempt_event(
                    synced_attempt,
                    message=_guard_intervention_message(
                        guard_decision,
                        submitted=True,
                    ),
                )
                decisions.append(
                    {
                        "execution_attempt_id": execution_attempt_id,
                        "symbol": str(synced_attempt.get("underlying_symbol") or ""),
                        "action": "cancel_requested",
                        "status": str(synced_attempt.get("status") or ""),
                        "reason": str(guard_decision["reason"] or ""),
                        "age_seconds": guard_decision.get("age_seconds"),
                        "stale_after_seconds": guard_decision.get(
                            "stale_after_seconds"
                        ),
                    }
                )
                continue

            decisions.append(
                {
                    "execution_attempt_id": execution_attempt_id,
                    "symbol": str(synced_attempt.get("underlying_symbol") or ""),
                    "action": "already_pending_cancel",
                    "status": current_status,
                    "reason": str(guard_decision["reason"] or ""),
                    "age_seconds": guard_decision.get("age_seconds"),
                    "stale_after_seconds": guard_decision.get("stale_after_seconds"),
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "execution_attempt_id": execution_attempt_id,
                    "symbol": str(attempt.get("underlying_symbol") or ""),
                    "error": str(exc),
                }
            )

    return {
        "status": "degraded" if failures else "ok",
        "open_attempt_count": len(open_attempts),
        "evaluated": evaluated,
        "canceled": canceled,
        "failed_unsubmitted": failed_unsubmitted,
        "submit_unknown": submit_unknown,
        "terminal_synced": terminal_synced,
        "skipped": skipped,
        "failure_count": len(failures),
        "decisions": decisions[:25],
        "failures": failures[:25],
    }
