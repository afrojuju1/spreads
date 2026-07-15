from __future__ import annotations

from typing import Any

from core.value_coercion import as_text

from .shared import AUTO_EXECUTION_MODES, OPEN_POSITION_STATES, _intent_payload


def _auto_execution_gate(
    *,
    intent: dict[str, Any],
    trading_environment: str,
) -> tuple[bool, str | None]:
    payload = _intent_payload(intent)
    approval_mode = str(payload.get("approval_mode") or "manual").strip().lower()
    execution_mode = str(payload.get("execution_mode") or "paper").strip().lower()
    if approval_mode != "auto":
        return False, "manual_approval_required"
    if execution_mode not in AUTO_EXECUTION_MODES:
        return False, "unsupported_execution_mode"
    if execution_mode == "paper" and trading_environment != "paper":
        return False, "paper_execution_requires_paper_environment"
    return True, None


def _position_is_active_for_intent(
    execution_store: Any,
    intent: dict[str, Any],
) -> tuple[bool, str | None]:
    position_id = as_text(intent.get("position_id")) or as_text(_intent_payload(intent).get("position_id"))
    if position_id is None:
        return False, "position_missing"
    position = execution_store.get_position(position_id)
    if position is None:
        return False, "position_missing"
    if str(position.get("status") or "") not in OPEN_POSITION_STATES:
        return False, "position_closed"
    return True, None
