from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any

ACTIVE_CLOSE_ATTEMPT_STATUSES = {
    "accepted",
    "accepted_for_bidding",
    "calculated",
    "held",
    "new",
    "partially_filled",
    "pending_cancel",
    "pending_new",
    "pending_replace",
    "pending_submission",
    "replaced",
    "stopped",
    "submitted",
    "suspended",
}
FAILED_CLOSE_ATTEMPT_STATUSES = {"failed", "rejected", "submit_unknown"}
PENDING_CLOSE_INTENT_STATES = {"pending", "claimed", "dispatching"}
STALE_RECONCILIATION_REASONS = {
    "broker_reconciliation_stale",
    "broker_sync_stale",
    "broker_sync_in_flight",
}
INTENT_MISMATCH_REASONS = {
    "broker_position_intent_conflict",
    "close_intent_mismatch",
    "position_intent_mismatch",
    "intent_mismatch",
}


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


def _round_money(value: Any) -> float | None:
    parsed = _coerce_float(value)
    return None if parsed is None else round(parsed, 2)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _is_close_attempt(row: Mapping[str, Any]) -> bool:
    if str(row.get("trade_intent") or "").strip().lower() == "close":
        return True
    for order in _list(row.get("orders")):
        if not isinstance(order, Mapping):
            continue
        intent = str(order.get("position_intent") or "").strip().lower()
        if intent in {"sell_to_close", "buy_to_close"}:
            return True
    return False


def _is_close_intent(row: Mapping[str, Any]) -> bool:
    action_type = str(row.get("action_type") or "").strip().lower()
    if action_type == "close":
        return True
    payload = _mapping(row.get("payload"))
    trade_intent = str(payload.get("trade_intent") or row.get("trade_intent") or "")
    if trade_intent.strip().lower() == "close":
        return True
    position_intent = str(
        payload.get("position_intent") or row.get("position_intent") or ""
    )
    return position_intent.strip().lower() in {"sell_to_close", "buy_to_close"}


def _attempt_activity_at(row: Mapping[str, Any]) -> str:
    return str(
        row.get("completed_at")
        or row.get("submitted_at")
        or row.get("requested_at")
        or row.get("updated_at")
        or ""
    )


def _position_activity_at(row: Mapping[str, Any]) -> str:
    return str(row.get("closed_at") or row.get("opened_at") or row.get("updated_at") or "")


def _latest_close(closes: list[Any]) -> dict[str, Any] | None:
    rows = [dict(row) for row in closes if isinstance(row, Mapping)]
    if not rows:
        return None
    rows.sort(
        key=lambda row: str(row.get("closed_at") or row.get("updated_at") or ""),
        reverse=True,
    )
    row = rows[0]
    return {
        "execution_attempt_id": row.get("execution_attempt_id"),
        "broker_order_id": row.get("broker_order_id"),
        "closed_quantity": row.get("closed_quantity"),
        "exit_value": _round_money(row.get("exit_value")),
        "realized_pnl": _round_money(row.get("realized_pnl")),
        "closed_at": row.get("closed_at"),
    }


def _compact_attempt(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "execution_attempt_id": row.get("execution_attempt_id"),
        "position_id": row.get("position_id"),
        "root_symbol": row.get("root_symbol") or row.get("underlying_symbol"),
        "strategy_family": row.get("strategy_family") or row.get("strategy"),
        "status": row.get("status"),
        "requested_at": row.get("requested_at"),
        "submitted_at": row.get("submitted_at"),
        "completed_at": row.get("completed_at"),
        "broker_order_id": row.get("broker_order_id"),
        "error_text": row.get("error_text"),
        "order_statuses": row.get("order_statuses"),
        "filled_qty": row.get("filled_qty"),
        "avg_fill_price": row.get("avg_fill_price"),
    }


def _compact_intent(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = _mapping(row.get("payload"))
    return {
        "execution_intent_id": row.get("execution_intent_id"),
        "execution_attempt_id": row.get("execution_attempt_id"),
        "state": row.get("state"),
        "action_type": row.get("action_type"),
        "position_id": row.get("strategy_position_id") or payload.get("position_id"),
        "symbol": payload.get("symbol") or payload.get("underlying_symbol"),
        "limit_price": payload.get("limit_price"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "expires_at": row.get("expires_at"),
    }


def _position_close_proof(row: Mapping[str, Any]) -> dict[str, Any]:
    closes = _list(row.get("closes"))
    latest_close = _latest_close(closes)
    return {
        "position_id": row.get("position_id"),
        "root_symbol": row.get("root_symbol") or row.get("underlying_symbol"),
        "strategy_family": row.get("strategy_family") or row.get("strategy"),
        "status": row.get("status") or row.get("position_status"),
        "remaining_quantity": row.get("remaining_quantity"),
        "last_exit_reason": row.get("last_exit_reason"),
        "reconciliation_status": row.get("reconciliation_status"),
        "close_count": len([item for item in closes if isinstance(item, Mapping)]),
        "latest_close": latest_close,
    }


def _reason_count(rows: list[Mapping[str, Any]], reasons: set[str]) -> int:
    total = 0
    for row in rows:
        reason_counts = _mapping(row.get("reason_counts"))
        for reason in reasons:
            total += int(reason_counts.get(reason) or 0)
    return total


def _attempt_error_matches(row: Mapping[str, Any], needles: tuple[str, ...]) -> bool:
    text = str(row.get("error_text") or "").strip().lower()
    return bool(text) and all(needle in text for needle in needles)


def build_close_lifecycle_summary(
    *,
    attempts: list[dict[str, Any]],
    intents: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    recent_direct_runs: list[dict[str, Any]] | None = None,
    limit: int = 8,
) -> dict[str, Any]:
    close_attempts = [row for row in attempts if _is_close_attempt(row)]
    close_attempts.sort(key=_attempt_activity_at, reverse=True)
    status_counts = dict(
        sorted(
            Counter(
                str(row.get("status") or "unknown").strip().lower()
                for row in close_attempts
            ).items()
        )
    )

    active_attempts = [
        row
        for row in close_attempts
        if str(row.get("status") or "").strip().lower() in ACTIVE_CLOSE_ATTEMPT_STATUSES
    ]
    failed_attempts = [
        row
        for row in close_attempts
        if str(row.get("status") or "").strip().lower()
        in FAILED_CLOSE_ATTEMPT_STATUSES
        or _as_text(row.get("error_text")) is not None
    ]
    failed_attempts.sort(key=_attempt_activity_at, reverse=True)

    close_intents = [row for row in intents if _is_close_intent(row)]
    close_intents.sort(
        key=lambda row: str(row.get("updated_at") or row.get("created_at") or ""),
        reverse=True,
    )
    pending_intents = [
        row
        for row in close_intents
        if str(row.get("state") or "").strip().lower() in PENDING_CLOSE_INTENT_STATES
    ]

    stale_position_count = sum(
        1
        for row in positions
        if str(row.get("last_exit_reason") or "").strip().lower()
        in STALE_RECONCILIATION_REASONS
    )
    direct_runs = [row for row in list(recent_direct_runs or []) if isinstance(row, Mapping)]
    stale_decision_count = _reason_count(direct_runs, STALE_RECONCILIATION_REASONS)
    intent_mismatch_decision_count = _reason_count(direct_runs, INTENT_MISMATCH_REASONS)
    intent_mismatch_attempt_count = sum(
        1
        for row in close_attempts
        if _attempt_error_matches(row, ("intent", "mismatch"))
        or _attempt_error_matches(row, ("position", "conflict"))
    )

    proof_rows = [_position_close_proof(row) for row in positions]
    proof_rows.sort(
        key=lambda row: str(
            _mapping(row.get("latest_close")).get("closed_at")
            or row.get("status")
            or ""
        ),
        reverse=True,
    )
    latest_filled_closes = [
        row for row in proof_rows if _mapping(row.get("latest_close"))
    ]
    latest_filled_closes.sort(
        key=lambda row: str(_mapping(row.get("latest_close")).get("closed_at") or ""),
        reverse=True,
    )

    active_count = len(active_attempts) + len(pending_intents)
    failed_count = len(failed_attempts)
    anomaly_count = (
        failed_count
        + max(stale_position_count, stale_decision_count)
        + intent_mismatch_decision_count
        + intent_mismatch_attempt_count
    )
    return {
        "status": "degraded" if active_count or failed_count else "healthy",
        "recent_close_attempt_count": len(close_attempts),
        "close_attempt_status_counts": status_counts,
        "active_close_attempt_count": len(active_attempts),
        "pending_close_intent_count": len(pending_intents),
        "failed_close_attempt_count": failed_count,
        "stale_reconciliation_skip_count": max(
            stale_position_count, stale_decision_count
        ),
        "intent_mismatch_reject_count": (
            intent_mismatch_decision_count + intent_mismatch_attempt_count
        ),
        "anomaly_count": anomaly_count,
        "latest_failure": None
        if not failed_attempts
        else _compact_attempt(failed_attempts[0]),
        "active_close_attempts": [_compact_attempt(row) for row in active_attempts[:limit]],
        "pending_close_intents": [_compact_intent(row) for row in pending_intents[:limit]],
        "recent_close_attempts": [_compact_attempt(row) for row in close_attempts[:limit]],
        "latest_filled_closes": latest_filled_closes[:limit],
        "position_close_proof": proof_rows[:limit],
    }


__all__ = ["build_close_lifecycle_summary"]
