from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from core.storage.serializers import parse_datetime

PENDING_SUBMISSION_STATUS = "pending_submission"
SUBMIT_UNKNOWN_STATUS = "submit_unknown"
AUTO_OPEN_ATTEMPT_STALE_AFTER_FALLBACK_SECONDS = 300
AUTO_OPEN_ATTEMPT_STALE_AFTER_MIN_SECONDS = 120
PENDING_SUBMISSION_GRACE_SECONDS = 60
PENDING_SUBMISSION_RUNNING_STALE_AFTER_SECONDS = 120
TERMINAL_ATTEMPT_STATUSES = frozenset(
    {
        "canceled",
        "done_for_day",
        "expired",
        "failed",
        "filled",
        "rejected",
    }
)
OPEN_ATTEMPT_STATUSES = frozenset(
    {
        PENDING_SUBMISSION_STATUS,
        SUBMIT_UNKNOWN_STATUS,
        "accepted",
        "accepted_for_bidding",
        "calculated",
        "held",
        "new",
        "partially_filled",
        "pending_cancel",
        "pending_new",
        "pending_replace",
        "replaced",
        "stopped",
        "suspended",
    }
)
OPEN_ATTEMPT_STATUS_LIST = tuple(sorted(OPEN_ATTEMPT_STATUSES))
BROKER_WORKING_ATTEMPT_STATUSES = frozenset(
    status
    for status in OPEN_ATTEMPT_STATUSES
    if status not in {PENDING_SUBMISSION_STATUS, SUBMIT_UNKNOWN_STATUS}
)


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _seconds_since(value: Any, *, now: datetime) -> float | None:
    parsed = parse_datetime(value)
    if parsed is None:
        return None
    return round(max((now - parsed).total_seconds(), 0.0), 3)


def normalize_execution_attempt_status(value: Any) -> str:
    return str(value or "").strip().lower()


def is_terminal_execution_attempt_status(value: Any) -> bool:
    return normalize_execution_attempt_status(value) in TERMINAL_ATTEMPT_STATUSES


def is_open_execution_attempt_status(value: Any) -> bool:
    return normalize_execution_attempt_status(value) in OPEN_ATTEMPT_STATUSES


def resolve_execution_attempt_request(attempt: Mapping[str, Any]) -> Mapping[str, Any]:
    request = attempt.get("request")
    return request if isinstance(request, Mapping) else {}


def resolve_execution_attempt_source(attempt: Mapping[str, Any]) -> dict[str, Any]:
    payload = resolve_execution_attempt_request(attempt).get("source")
    return dict(payload) if isinstance(payload, Mapping) else {}


def resolve_execution_attempt_source_job(attempt: Mapping[str, Any]) -> dict[str, Any]:
    payload = resolve_execution_attempt_request(attempt).get("source_job")
    return dict(payload) if isinstance(payload, Mapping) else {}


def resolve_execution_submit_job_run_id(execution_attempt_id: str) -> str:
    return f"execution_submit:{execution_attempt_id}"


def resolve_execution_attempt_primary_order(
    attempt: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    orders = attempt.get("orders")
    if not isinstance(orders, list):
        return None
    primary = next(
        (
            order
            for order in orders
            if isinstance(order, Mapping)
            and not _as_text(order.get("parent_broker_order_id"))
        ),
        None,
    )
    if primary is not None:
        return primary
    return next((order for order in orders if isinstance(order, Mapping)), None)


def resolve_execution_attempt_filled_quantity(
    attempt: Mapping[str, Any],
    *,
    primary_order: Mapping[str, Any] | None = None,
) -> float:
    primary = (
        primary_order
        if primary_order is not None
        else resolve_execution_attempt_primary_order(attempt)
    )
    primary_value = (
        None if primary is None else _coerce_float(primary.get("filled_qty"))
    )
    if primary_value is not None and primary_value > 0:
        return primary_value

    order_values: list[float] = []
    for order in attempt.get("orders") or []:
        if not isinstance(order, Mapping):
            continue
        filled = _coerce_float(order.get("filled_qty"))
        if filled is not None and filled > 0:
            order_values.append(filled)
    if order_values:
        return max(order_values)

    fill_values: list[float] = []
    for fill in attempt.get("fills") or []:
        if not isinstance(fill, Mapping):
            continue
        cumulative = _coerce_float(fill.get("cumulative_quantity"))
        quantity = _coerce_float(fill.get("quantity"))
        candidate = (
            cumulative if cumulative is not None and cumulative > 0 else quantity
        )
        if candidate is not None and candidate > 0:
            fill_values.append(candidate)
    if fill_values:
        return max(fill_values)
    return 0.0


def resolve_open_attempt_working_stale_after_seconds(
    attempt: Mapping[str, Any],
    *,
    source_job_definition: Mapping[str, Any] | None = None,
) -> float | None:
    source = resolve_execution_attempt_source(attempt)
    if _as_text(source.get("kind")) != "auto_session_execution":
        return None

    stale_after_seconds = AUTO_OPEN_ATTEMPT_STALE_AFTER_FALLBACK_SECONDS
    payload = (
        source_job_definition.get("payload")
        if isinstance(source_job_definition, Mapping)
        else {}
    )
    interval_seconds = (
        _coerce_int(payload.get("interval_seconds"))
        if isinstance(payload, Mapping)
        else None
    )
    if interval_seconds is not None and interval_seconds > 0:
        stale_after_seconds = max(
            interval_seconds * 2,
            AUTO_OPEN_ATTEMPT_STALE_AFTER_MIN_SECONDS,
        )
    return float(stale_after_seconds)


def classify_open_execution_attempt(
    attempt: Mapping[str, Any],
    *,
    now: datetime,
    submit_job: Mapping[str, Any] | None = None,
    source_job_definition: Mapping[str, Any] | None = None,
    submission_grace_seconds: int = PENDING_SUBMISSION_GRACE_SECONDS,
    running_submit_stale_after_seconds: int = PENDING_SUBMISSION_RUNNING_STALE_AFTER_SECONDS,
) -> dict[str, Any]:
    status = normalize_execution_attempt_status(attempt.get("status"))
    broker_order_id = _as_text(attempt.get("broker_order_id"))
    source = resolve_execution_attempt_source(attempt)
    source_kind = _as_text(source.get("kind")) or "unknown"
    filled_quantity = resolve_execution_attempt_filled_quantity(attempt)
    requested_quantity = max(_coerce_float(attempt.get("quantity")) or 0.0, 0.0)
    pending_quantity = max(
        requested_quantity - min(filled_quantity, requested_quantity), 0.0
    )
    linked_position_id = _as_text(attempt.get("position_id"))
    occupies_position_slot = linked_position_id is None and filled_quantity <= 0
    submit_job_status = normalize_execution_attempt_status(
        None if not isinstance(submit_job, Mapping) else submit_job.get("status")
    )
    queue_age_seconds = _seconds_since(attempt.get("requested_at"), now=now)
    submitted_age_seconds = _seconds_since(
        _as_text(attempt.get("submitted_at")) or _as_text(attempt.get("requested_at")),
        now=now,
    )
    submit_job_age_seconds = _seconds_since(
        None
        if not isinstance(submit_job, Mapping)
        else submit_job.get("scheduled_for"),
        now=now,
    )
    submit_job_heartbeat_age_seconds = _seconds_since(
        None
        if not isinstance(submit_job, Mapping)
        else (
            submit_job.get("heartbeat_at")
            or submit_job.get("started_at")
            or submit_job.get("scheduled_for")
        ),
        now=now,
    )
    working_stale_after_seconds = resolve_open_attempt_working_stale_after_seconds(
        attempt,
        source_job_definition=source_job_definition,
    )

    lifecycle = {
        "phase": "unknown",
        "source_kind": source_kind,
        "status": status,
        "age_seconds": submitted_age_seconds,
        "queue_age_seconds": queue_age_seconds,
        "submit_job_run_id": resolve_execution_submit_job_run_id(
            str(attempt.get("execution_attempt_id") or "")
        ),
        "submit_job_status": None if not submit_job_status else submit_job_status,
        "submit_job_age_seconds": submit_job_age_seconds,
        "submit_job_heartbeat_age_seconds": submit_job_heartbeat_age_seconds,
        "submission_grace_seconds": int(max(submission_grace_seconds, 1)),
        "working_stale_after_seconds": working_stale_after_seconds,
        "stale": False,
        "blocks_capacity": is_open_execution_attempt_status(status),
        "occupies_position_slot": occupies_position_slot,
        "filled_quantity": round(filled_quantity, 4),
        "pending_quantity": round(pending_quantity, 4),
        "next_action": "observe",
        "intervention": None,
        "note": None,
    }

    if status == PENDING_SUBMISSION_STATUS and broker_order_id is None:
        lifecycle["phase"] = "queued_local"
        lifecycle["age_seconds"] = queue_age_seconds
        lifecycle["note"] = "Execution is queued locally for broker submission."
        lifecycle["next_action"] = "wait_for_submit_job"
        grace_seconds = float(max(submission_grace_seconds, 1))
        if queue_age_seconds is None or queue_age_seconds <= grace_seconds:
            return lifecycle
        if submit_job_status in {"failed", "skipped"} or submit_job is None:
            lifecycle["stale"] = True
            lifecycle["next_action"] = "fail_unsubmitted"
            lifecycle["intervention"] = "fail_unsubmitted"
            lifecycle["note"] = (
                "Execution remained queued locally past the submission grace window "
                "and the submit job did not complete successfully."
            )
            return lifecycle
        if submit_job_status == "queued":
            lifecycle["stale"] = True
            lifecycle["next_action"] = "fail_unsubmitted"
            lifecycle["intervention"] = "fail_unsubmitted"
            lifecycle["note"] = (
                "Execution remained queued locally past the submission grace window "
                "without reaching a worker."
            )
            return lifecycle
        if submit_job_status == "running":
            heartbeat_stale_after = float(max(running_submit_stale_after_seconds, 1))
            if (
                submit_job_heartbeat_age_seconds is not None
                and submit_job_heartbeat_age_seconds > heartbeat_stale_after
            ):
                lifecycle["phase"] = "submit_unknown"
                lifecycle["stale"] = True
                lifecycle["next_action"] = "reconcile_broker"
                lifecycle["intervention"] = "mark_submit_unknown"
                lifecycle["note"] = (
                    "Execution submit outcome is uncertain because the submit job heartbeat "
                    "is stale and broker submission may have happened."
                )
            return lifecycle
        if submit_job_status == "succeeded":
            lifecycle["phase"] = "submit_unknown"
            lifecycle["stale"] = True
            lifecycle["next_action"] = "reconcile_broker"
            lifecycle["intervention"] = "mark_submit_unknown"
            lifecycle["note"] = (
                "Execution submit outcome is uncertain because the submit job completed "
                "without a reconciled broker order on the attempt."
            )
            return lifecycle
        lifecycle["stale"] = True
        lifecycle["next_action"] = "fail_unsubmitted"
        lifecycle["intervention"] = "fail_unsubmitted"
        lifecycle["note"] = (
            "Execution remained queued locally past the submission grace window "
            "and requires cleanup."
        )
        return lifecycle

    if status == SUBMIT_UNKNOWN_STATUS:
        lifecycle["phase"] = "submit_unknown"
        lifecycle["stale"] = True
        lifecycle["note"] = (
            "Execution submit outcome is uncertain and needs broker reconciliation."
        )
        lifecycle["next_action"] = "reconcile_broker"
        return lifecycle

    if status == "pending_cancel":
        lifecycle["phase"] = "canceling"
        lifecycle["note"] = "Execution is waiting for broker cancel confirmation."
        lifecycle["next_action"] = "wait_for_cancel_confirmation"
        if (
            working_stale_after_seconds is not None
            and submitted_age_seconds is not None
            and submitted_age_seconds > working_stale_after_seconds
        ):
            lifecycle["stale"] = True
            lifecycle["next_action"] = "escalate"
            lifecycle["note"] = (
                "Execution cancel request is stale and needs operator review."
            )
        return lifecycle

    if status == "partially_filled":
        lifecycle["phase"] = "partial_open"
        lifecycle["occupies_position_slot"] = False
        lifecycle["note"] = (
            "Execution is partially filled and linked to position ownership."
        )
        lifecycle["next_action"] = "manage_partial_open"
        return lifecycle

    lifecycle["phase"] = "working_fresh"
    lifecycle["note"] = "Execution is working at the broker."
    lifecycle["next_action"] = "wait_for_broker_update"
    if (
        working_stale_after_seconds is not None
        and submitted_age_seconds is not None
        and submitted_age_seconds > working_stale_after_seconds
    ):
        lifecycle["phase"] = "working_stale"
        lifecycle["stale"] = True
        if source_kind == "auto_session_execution":
            lifecycle["next_action"] = "cancel_order"
            lifecycle["intervention"] = "cancel_order"
            lifecycle["note"] = (
                "Automatic open execution remained pending past its stale-order window."
            )
        else:
            lifecycle["next_action"] = "escalate"
            lifecycle["note"] = (
                "Manual open execution remained pending past its review window."
            )
    return lifecycle


__all__ = [
    "AUTO_OPEN_ATTEMPT_STALE_AFTER_FALLBACK_SECONDS",
    "AUTO_OPEN_ATTEMPT_STALE_AFTER_MIN_SECONDS",
    "BROKER_WORKING_ATTEMPT_STATUSES",
    "OPEN_ATTEMPT_STATUSES",
    "OPEN_ATTEMPT_STATUS_LIST",
    "PENDING_SUBMISSION_GRACE_SECONDS",
    "PENDING_SUBMISSION_RUNNING_STALE_AFTER_SECONDS",
    "PENDING_SUBMISSION_STATUS",
    "SUBMIT_UNKNOWN_STATUS",
    "TERMINAL_ATTEMPT_STATUSES",
    "classify_open_execution_attempt",
    "is_open_execution_attempt_status",
    "is_terminal_execution_attempt_status",
    "normalize_execution_attempt_status",
    "resolve_execution_submit_job_run_id",
    "resolve_execution_attempt_filled_quantity",
    "resolve_execution_attempt_primary_order",
    "resolve_execution_attempt_request",
    "resolve_execution_attempt_source",
    "resolve_execution_attempt_source_job",
    "resolve_open_attempt_working_stale_after_seconds",
]
