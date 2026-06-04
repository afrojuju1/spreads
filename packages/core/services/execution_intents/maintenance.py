from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.deployment_policy import (
    DEPLOYMENT_MODE_LIVE_AUTO,
    DEPLOYMENT_MODE_PAPER_AUTO,
)
from core.services.trading_strategies import load_active_trading_strategies
from core.storage.serializers import parse_datetime

from .shared import (
    ACTIVE_INTENT_STATES,
    AUTO_EXECUTION_MODES,
    OPEN_POSITION_STATES,
    TERMINAL_INTENT_STATES,
    _append_event,
    _as_text,
    _intent_payload,
    _update_intent,
    _utc_now,
    link_execution_intent_position,
    normalize_execution_intent_state,
)


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


def _intent_execution_policy(intent: dict[str, Any]) -> dict[str, Any] | None:
    payload = _intent_payload(intent)
    approval_mode = str(payload.get("approval_mode") or "manual").strip().lower()
    execution_mode = str(payload.get("execution_mode") or "paper").strip().lower()
    if approval_mode != "auto":
        return None
    if execution_mode == "paper":
        return {"deployment_mode": DEPLOYMENT_MODE_PAPER_AUTO}
    if execution_mode == "live":
        return {"deployment_mode": DEPLOYMENT_MODE_LIVE_AUTO}
    return None


def _intent_exit_policy(intent: dict[str, Any]) -> dict[str, Any] | None:
    payload = _intent_payload(intent)
    exit_policy = payload.get("exit_policy")
    return dict(exit_policy) if isinstance(exit_policy, dict) else None


def _position_is_active_for_intent(
    execution_store: Any,
    intent: dict[str, Any],
) -> tuple[bool, str | None]:
    payload = _intent_payload(intent)
    strategy_position_id = _as_text(intent.get("strategy_position_id")) or _as_text(payload.get("position_id"))
    if strategy_position_id is None:
        return False, "position_missing"
    position = execution_store.get_position(strategy_position_id)
    if position is None:
        return False, "position_missing"
    status = str(position.get("status") or "")
    if status not in OPEN_POSITION_STATES:
        return False, "position_closed"
    return True, None


def _cleanup_slot_conflicts(
    execution_store: Any,
    *,
    limit: int,
) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in execution_store.list_execution_intents(
            limit=max(int(limit), 1) * 20,
        )
    ]
    slots: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        slot_key = str(row.get("slot_key") or "")
        if not slot_key:
            continue
        slots.setdefault(slot_key, []).append(row)

    revoked = 0
    results: list[dict[str, Any]] = []
    for slot_key, intents in slots.items():
        intents.sort(
            key=lambda row: parse_datetime(_as_text(row.get("created_at"))) or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        anchor_id: str | None = None
        for intent in intents:
            state = normalize_execution_intent_state(intent.get("state"))
            intent_id = str(intent["execution_intent_id"])
            if anchor_id is None and state in ACTIVE_INTENT_STATES.union({"filled"}):
                anchor_id = intent_id
                continue
            if anchor_id is None:
                continue
            if state not in {"pending", "claimed"}:
                continue
            if _as_text(intent.get("execution_attempt_id")):
                continue
            updated = _update_intent(
                execution_store,
                intent,
                state="revoked",
                execution_attempt_id=None,
                superseded_by_id=anchor_id,
                payload_updates={
                    "dispatch_status": "revoked",
                    "revoked_by_execution_intent_id": anchor_id,
                },
                updated_at=_utc_now(),
            )
            _append_event(
                execution_store,
                execution_intent_id=intent_id,
                event_type="revoked",
                payload={
                    "reason": "slot_conflict",
                    "anchor_execution_intent_id": anchor_id,
                },
            )
            revoked += 1
            results.append(
                {
                    "execution_intent_id": intent_id,
                    "slot_key": slot_key,
                    "status": updated.get("state"),
                    "anchor_execution_intent_id": anchor_id,
                }
            )
    return {"revoked": revoked, "results": results[:25]}


def _backfill_strategy_position_links(execution_store: Any, *, limit: int) -> dict[str, Any]:
    linked = 0
    results: list[dict[str, Any]] = []
    positions = [dict(row) for row in execution_store.list_positions(limit=max(int(limit), 1) * 10)]
    for position in positions:
        position_id = str(position.get("position_id") or "")
        open_execution_attempt_id = _as_text(position.get("open_execution_attempt_id"))
        if not position_id or open_execution_attempt_id is None:
            continue
        attempt = execution_store.get_attempt(open_execution_attempt_id)
        if attempt is None:
            continue
        request = attempt.get("request") if isinstance(attempt.get("request"), dict) else {}
        execution_intent_id = _as_text(request.get("execution_intent_id"))
        if execution_intent_id is None:
            continue
        intent = execution_store.get_execution_intent(execution_intent_id)
        if intent is None:
            continue
        if _as_text(intent.get("strategy_position_id")) == position_id:
            continue
        updated = link_execution_intent_position(
            execution_store,
            intent=dict(intent),
            position_id=position_id,
            execution_attempt_id=_as_text(intent.get("execution_attempt_id")),
            updated_at=_utc_now(),
        )
        linked += 1
        results.append(
            {
                "execution_intent_id": str(intent["execution_intent_id"]),
                "strategy_position_id": position_id,
                "state": updated.get("state"),
            }
        )
    return {"linked": linked, "results": results[:25]}


def _active_trading_strategy_ids() -> set[str]:
    return set(load_active_trading_strategies().keys())


def _cleanup_terminal_intent_history(
    execution_store: Any,
    *,
    limit: int,
    older_than_minutes: int = 15,
) -> dict[str, Any]:
    threshold = datetime.now(UTC) - timedelta(minutes=max(older_than_minutes, 1))
    retained = 0
    results: list[dict[str, Any]] = []
    intents = [dict(row) for row in execution_store.list_execution_intents(limit=max(int(limit), 1) * 25)]
    for intent in intents:
        if retained >= max(int(limit), 1):
            break
        state = normalize_execution_intent_state(intent.get("state"))
        if state not in TERMINAL_INTENT_STATES:
            continue
        created_at = parse_datetime(_as_text(intent.get("created_at")))
        if created_at is None or created_at >= threshold:
            continue
        execution_intent_id = str(intent["execution_intent_id"])
        retained += 1
        results.append(
            {
                "execution_intent_id": execution_intent_id,
                "state": state,
                "slot_key": intent.get("slot_key"),
            }
        )
    return {"deleted": 0, "retained": retained, "results": results[:25]}


def _cleanup_inactive_strategy_intents(
    *,
    execution_store: Any,
    limit: int,
    older_than_minutes: int = 15,
) -> dict[str, Any]:
    active_strategy_ids = _active_trading_strategy_ids()
    threshold = datetime.now(UTC) - timedelta(minutes=max(older_than_minutes, 1))
    revoked = 0
    results: list[dict[str, Any]] = []
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(
            states=sorted(ACTIVE_INTENT_STATES),
            limit=max(int(limit), 1) * 25,
        )
    ]
    for intent in intents:
        if revoked >= max(int(limit), 1):
            break
        trading_strategy_id = str(intent.get("trading_strategy_id") or "")
        if trading_strategy_id in active_strategy_ids:
            continue
        created_at = parse_datetime(_as_text(intent.get("created_at")))
        if created_at is None or created_at >= threshold:
            continue
        execution_intent_id = str(intent["execution_intent_id"])
        if _as_text(intent.get("execution_attempt_id")):
            continue
        updated = _update_intent(
            execution_store,
            intent,
            state="revoked",
            payload_updates={
                "dispatch_status": "revoked",
                "revoke_reason": "inactive_trading_strategy",
            },
            updated_at=_utc_now(),
        )
        _append_event(
            execution_store,
            execution_intent_id=execution_intent_id,
            event_type="revoked",
            payload={"reason": "inactive_trading_strategy"},
        )
        revoked += 1
        results.append(
            {
                "execution_intent_id": execution_intent_id,
                "trading_strategy_id": trading_strategy_id,
                "state": updated.get("state"),
            }
        )
    return {"revoked": revoked, "results": results[:25]}
