from __future__ import annotations

from collections.abc import Mapping
from typing import Any

PENDING_SUBMISSION_STATUS = "pending_submission"
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
    primary_value = None if primary is None else _coerce_float(primary.get("filled_qty"))
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
        candidate = cumulative if cumulative is not None and cumulative > 0 else quantity
        if candidate is not None and candidate > 0:
            fill_values.append(candidate)
    if fill_values:
        return max(fill_values)
    return 0.0


__all__ = [
    "OPEN_ATTEMPT_STATUSES",
    "OPEN_ATTEMPT_STATUS_LIST",
    "PENDING_SUBMISSION_STATUS",
    "TERMINAL_ATTEMPT_STATUSES",
    "is_open_execution_attempt_status",
    "is_terminal_execution_attempt_status",
    "normalize_execution_attempt_status",
    "resolve_execution_attempt_filled_quantity",
    "resolve_execution_attempt_primary_order",
    "resolve_execution_attempt_request",
    "resolve_execution_attempt_source",
    "resolve_execution_attempt_source_job",
]
