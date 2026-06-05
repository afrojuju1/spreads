from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import re
from typing import Any

from core.services.trading_lifecycle import (
    CloseDecisionState,
    LifecycleObject,
    TradingPositionState,
    is_terminal_lifecycle_state,
    normalize_lifecycle_state,
)

BLOCKED_CLOSE_REASONS = {
    "awaiting_broker_reconciliation",
    "broker_reconciliation_stale",
    "close_already_open",
    "close_intent_already_open",
    "close_symbols_missing",
    "close_validation_blocked",
    "execution_intent_schema_unavailable",
    "ambiguous_management_runtime",
    "management_runtime_required_for_close_intent",
    "missing_management_owner",
    "no_management_runtime",
    "no_remaining_quantity",
    "position_not_open",
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


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


def _safe_component(value: Any) -> str:
    rendered = str(value or "").strip()
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", rendered) or "unknown"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def normalize_position_lifecycle_state(position: Mapping[str, Any]) -> str:
    raw_status = _as_text(position.get("position_status")) or _as_text(position.get("status"))
    if raw_status is not None:
        try:
            return normalize_lifecycle_state(
                LifecycleObject.POSITION,
                raw_status,
            ).value
        except ValueError:
            pass
    opened_quantity = _coerce_float(position.get("opened_quantity")) or 0.0
    remaining_quantity = _coerce_float(position.get("remaining_quantity")) or 0.0
    if opened_quantity <= 0:
        return TradingPositionState.PENDING_OPEN.value
    if remaining_quantity <= 0:
        return TradingPositionState.CLOSED.value
    if remaining_quantity < opened_quantity:
        return TradingPositionState.PARTIAL_CLOSE.value
    return TradingPositionState.OPEN.value


def _close_attempt_is_active(row: Mapping[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    return status in {
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


def _close_intent_is_pending(row: Mapping[str, Any]) -> bool:
    return str(row.get("state") or "").strip().lower() in {"pending", "claimed", "dispatching"}


def build_position_lifecycle(
    position: Mapping[str, Any],
    *,
    closes: list[Any] | None = None,
    close_attempts: list[Any] | None = None,
    close_intents: list[Any] | None = None,
) -> dict[str, Any]:
    state = normalize_position_lifecycle_state(position)
    close_rows = [dict(row) for row in _list(closes) if isinstance(row, Mapping)]
    close_attempt_rows = [dict(row) for row in _list(close_attempts) if isinstance(row, Mapping)]
    close_intent_rows = [dict(row) for row in _list(close_intents) if isinstance(row, Mapping)]
    active_close_attempt_count = sum(1 for row in close_attempt_rows if _close_attempt_is_active(row))
    pending_close_intent_count = sum(1 for row in close_intent_rows if _close_intent_is_pending(row))
    active_close_count = active_close_attempt_count + pending_close_intent_count
    reconciliation_state = _as_text(position.get("reconciliation_status")) or _as_text(position.get("reconciliation_state"))
    remaining_quantity = _coerce_float(position.get("remaining_quantity")) or 0.0
    opened_quantity = _coerce_float(position.get("opened_quantity")) or 0.0

    if state == TradingPositionState.CLOSED.value:
        next_action = "none"
    elif active_close_count > 0:
        next_action = "wait_for_active_close"
    elif reconciliation_state not in {None, "matched"}:
        next_action = "reconcile_broker"
    elif remaining_quantity <= 0:
        next_action = "recalculate_position"
    else:
        next_action = "evaluate_close"

    return {
        "object_type": LifecycleObject.POSITION.value,
        "position_id": position.get("position_id"),
        "lifecycle_state": state,
        "terminal": is_terminal_lifecycle_state(LifecycleObject.POSITION, state),
        "opened_quantity": opened_quantity,
        "remaining_quantity": remaining_quantity,
        "closed_quantity": max(opened_quantity - remaining_quantity, 0.0),
        "close_count": len(close_rows),
        "active_close_attempt_count": active_close_attempt_count,
        "pending_close_intent_count": pending_close_intent_count,
        "active_close_count": active_close_count,
        "one_active_close_policy": "one_active_close_per_position",
        "close_allowed": next_action == "evaluate_close",
        "reconciliation_state": reconciliation_state,
        "next_action": next_action,
        "updated_at": _as_text(position.get("updated_at")),
    }


def _close_decision_state(decision: Mapping[str, Any]) -> str:
    reason = str(decision.get("reason") or "").strip().lower()
    if bool(decision.get("should_close")):
        return CloseDecisionState.CLOSE_SELECTED.value
    if reason in BLOCKED_CLOSE_REASONS:
        return CloseDecisionState.BLOCKED.value
    if reason in {"", "unknown"}:
        return CloseDecisionState.UNKNOWN.value
    return CloseDecisionState.HOLD.value


def build_close_decision_lifecycle(
    *,
    position: Mapping[str, Any],
    decision: Mapping[str, Any],
    decision_source: str | None = None,
    decided_at: str | None = None,
) -> dict[str, Any]:
    decided_at_value = decided_at or _utc_now()
    position_id = _as_text(position.get("position_id")) or "unknown"
    reason = _as_text(decision.get("reason")) or "unknown"
    state = _close_decision_state(decision)
    details = _mapping(decision.get("decision_details"))
    policy = _mapping(details.get("policy"))
    blockers = [reason] if state in {CloseDecisionState.BLOCKED.value, CloseDecisionState.UNKNOWN.value} else []
    metrics = {
        key: details.get(key)
        for key in (
            "mark",
            "effective_mark",
            "entry_value",
            "profit_target_mark",
            "stop_mark",
        )
        if details.get(key) is not None
    }
    evidence = {
        "decision_source": decision_source or _as_text(decision.get("decision_source")),
        "recipe_ref": _as_text(decision.get("recipe_ref")),
        "limit_price": decision.get("limit_price"),
        "limit_price_source": _as_text(decision.get("limit_price_source")),
        "mark_state": _as_text(details.get("mark_state")),
        "force_close_at": _as_text(details.get("force_close_at")),
        "management_recipe_refs": [str(value) for value in decision.get("management_recipe_refs") or [] if str(value or "").strip()],
    }
    return {
        "close_decision_id": ("close_decision:" f"{_safe_component(position_id)}:{_safe_component(decided_at_value)}:{_safe_component(reason)}"),
        "object_type": LifecycleObject.CLOSE_DECISION.value,
        "position_id": position_id,
        "decision_state": state,
        "lifecycle_state": state,
        "reason": reason,
        "reason_codes": [reason],
        "blockers": blockers,
        "quantity_to_close": _coerce_float(position.get("remaining_quantity")),
        "limit_source": _as_text(decision.get("limit_price_source")),
        "limit_price": _coerce_float(decision.get("limit_price")),
        "mark_source": _as_text(position.get("close_mark_source")),
        "policy_snapshot": policy,
        "metrics": metrics,
        "evidence": evidence,
        "decided_at": decided_at_value,
    }


__all__ = [
    "BLOCKED_CLOSE_REASONS",
    "build_close_decision_lifecycle",
    "build_position_lifecycle",
    "normalize_position_lifecycle_state",
]
