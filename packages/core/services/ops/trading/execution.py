from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any


from core.jobs.specs import get_declared_job_row
from core.services.execution.shared import OPEN_STATUSES
from core.services.execution_lifecycle import (
    is_open_execution_attempt_status,
    project_execution_attempt_lifecycle,
    resolve_execution_attempt_source_job,
)
from core.value_coercion import (
    as_text,
)

from core.services.ops.shared import (
    _attention,
    _sorted_by_activity,
)


from core.services.ops.trading.models import (
    _ExecutionProjection,
)

def _load_execution_attempt_activity_context(
    *,
    job_store: Any,
    attempts: list[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any] | None]:
    source_definitions: dict[str, Mapping[str, Any] | None] = {}
    if job_store is None or (hasattr(job_store, "schema_ready") and not job_store.schema_ready()):
        return source_definitions

    for attempt in attempts:
        source_job = resolve_execution_attempt_source_job(attempt)
        source_job_key = as_text(source_job.get("job_key"))
        if source_job_key is None or source_job_key in source_definitions:
            continue
        source_definitions[source_job_key] = get_declared_job_row(source_job_key)
    return source_definitions


def _execution_attempt_lifecycle(
    *,
    attempt: Mapping[str, Any],
    now: datetime,
    source_definitions: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, Any]:
    if not is_open_execution_attempt_status(attempt.get("status")):
        return {}
    source_job = resolve_execution_attempt_source_job(attempt)
    source_job_key = as_text(source_job.get("job_key"))
    source_definition = None if source_job_key is None else source_definitions.get(source_job_key)
    attached_lifecycle = attempt.get("execution_attempt_lifecycle")
    if isinstance(attached_lifecycle, Mapping):
        return dict(attached_lifecycle)
    return project_execution_attempt_lifecycle(
        attempt,
        now=now,
        source_job_definition=source_definition,
    )


def _summarize_execution_attempt(
    attempt: Mapping[str, Any],
    *,
    lifecycle: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    lifecycle_payload = dict(lifecycle or {})
    return {
        "execution_attempt_id": attempt.get("execution_attempt_id"),
        "session_id": attempt.get("session_id"),
        "label": attempt.get("label"),
        "underlying_symbol": attempt.get("underlying_symbol"),
        "strategy": attempt.get("strategy"),
        "trade_intent": attempt.get("trade_intent"),
        "status": attempt.get("status"),
        "lifecycle_state": lifecycle_payload.get("lifecycle_state"),
        "requested_at": attempt.get("requested_at"),
        "submitted_at": attempt.get("submitted_at"),
        "completed_at": attempt.get("completed_at"),
        "broker_order_id": attempt.get("broker_order_id"),
        "broker_order_state": lifecycle_payload.get("broker_order_state"),
        "broker_order_state_counts": lifecycle_payload.get("broker_order_state_counts"),
        "source_kind": lifecycle_payload.get("source_kind"),
        "lifecycle_phase": lifecycle_payload.get("phase"),
        "lifecycle_note": lifecycle_payload.get("note"),
        "age_seconds": lifecycle_payload.get("age_seconds"),
        "queue_age_seconds": lifecycle_payload.get("queue_age_seconds"),
        "stale_after_seconds": lifecycle_payload.get("working_stale_after_seconds"),
        "submission_grace_seconds": lifecycle_payload.get("submission_grace_seconds"),
        "broker_activity_id": lifecycle_payload.get("broker_activity_id"),
        "stale": bool(lifecycle_payload.get("stale")),
        "next_action": lifecycle_payload.get("next_action"),
        "blocks_capacity": bool(lifecycle_payload.get("blocks_capacity")),
        "occupies_position_slot": bool(lifecycle_payload.get("occupies_position_slot")),
    }

def _project_execution(
    *,
    storage: Any,
    market_date: str,
    now: datetime,
) -> _ExecutionProjection:
    execution_store = storage.execution
    job_store = getattr(storage, "jobs", None)
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    if execution_store.schema_ready():
        open_execution_attempts = [
            dict(row)
            for row in execution_store.list_attempts_by_status(
                statuses=sorted(OPEN_STATUSES),
                limit=200,
            )
        ]
    else:
        open_execution_attempts = []
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="execution_schema_unavailable",
                message="Execution attempts storage is not available yet.",
            )
        )

    source_definitions = _load_execution_attempt_activity_context(
        job_store=job_store,
        attempts=open_execution_attempts,
    )
    summarized_open_execution_attempts = [
        _summarize_execution_attempt(
            row,
            lifecycle=_execution_attempt_lifecycle(
                attempt=row,
                now=now,
                source_definitions=source_definitions,
            ),
        )
        for row in _sorted_by_activity(open_execution_attempts)
    ]
    stale_open_execution_count = sum(1 for row in summarized_open_execution_attempts if bool(row.get("stale")))
    submit_unknown_execution_count = sum(1 for row in summarized_open_execution_attempts if str(row.get("lifecycle_phase") or "") == "submit_unknown")
    capacity_blocked_underlyings = sorted(
        {
            str(row.get("underlying_symbol") or "")
            for row in summarized_open_execution_attempts
            if bool(row.get("blocks_capacity")) and as_text(row.get("underlying_symbol"))
        }
    )
    if execution_store.schema_has_tables(
        "trade_admissions",
        "trade_decisions",
        "trade_execution_intents",
        "execution_intents",
    ):
        approved_admission_intent_gaps = execution_store.list_approved_admissions_missing_execution_intents(
            session_date=market_date,
            limit=25,
        )
    else:
        approved_admission_intent_gaps = []
    approved_admission_intent_gap_ids = [
        str(row["admission"]["execution_intent_id"])
        for row in approved_admission_intent_gaps
        if isinstance(row.get("admission"), dict) and as_text(row["admission"].get("execution_intent_id")) is not None
    ]
    approved_admission_intent_gap_count = len(approved_admission_intent_gaps)
    execution_health_status = (
        "degraded" if stale_open_execution_count or submit_unknown_execution_count or approved_admission_intent_gap_count else "healthy"
    )
    if approved_admission_intent_gap_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="high",
                code="approved_admission_intent_missing",
                message=(f"{approved_admission_intent_gap_count} approved admission(s) are missing current execution intent rows."),
            )
        )
    if submit_unknown_execution_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="high",
                code="broker_submission_unknown",
                message=f"{submit_unknown_execution_count} open execution attempt(s) have uncertain broker submission outcomes and still block capacity.",
            )
        )
    elif stale_open_execution_count:
        statuses.append("degraded")
        attention.append(
            _attention(
                severity="medium",
                code="stale_open_executions_present",
                message=f"{stale_open_execution_count} open execution attempt(s) are stale and need operator review.",
            )
        )

    return _ExecutionProjection(
        open_execution_attempts=open_execution_attempts,
        summarized_open_execution_attempts=summarized_open_execution_attempts,
        stale_open_execution_count=stale_open_execution_count,
        submit_unknown_execution_count=submit_unknown_execution_count,
        approved_admission_intent_gap_count=approved_admission_intent_gap_count,
        approved_admission_intent_gap_ids=approved_admission_intent_gap_ids,
        capacity_blocked_underlyings=capacity_blocked_underlyings,
        execution_health_status=execution_health_status,
        statuses=tuple(statuses),
        attention=attention,
    )
