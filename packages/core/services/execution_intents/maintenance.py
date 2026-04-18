from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.bots import bot_time_reached, load_bots
from core.services.deployment_policy import (
    DEPLOYMENT_MODE_LIVE_AUTO,
    DEPLOYMENT_MODE_PAPER_AUTO,
)
from core.services.live_pipelines import resolve_live_collector_label
from core.storage.serializers import parse_datetime

from .shared import (
    ACTIVE_INTENT_STATES,
    AUTO_EXECUTION_MODES,
    OPEN_POSITION_STATES,
    TERMINAL_INTENT_STATES,
    _append_event,
    _as_text,
    _intent_action_type,
    _intent_payload,
    _update_intent,
    _utc_now,
    link_execution_intent_position,
)


def _auto_execution_gate(
    *,
    intent: dict[str, Any],
    trading_environment: str,
) -> tuple[bool, str | None]:
    payload = _intent_payload(intent)
    approval_mode = str(payload.get("approval_mode") or "manual").strip().lower()
    execution_mode = str(payload.get("execution_mode") or "paper").strip().lower()
    action_type = _intent_action_type(intent)
    bot_id = _as_text(intent.get("bot_id"))
    bot = None if bot_id is None else load_bots().get(bot_id)
    if approval_mode != "auto":
        return False, "manual_approval_required"
    if execution_mode not in AUTO_EXECUTION_MODES:
        return False, "unsupported_execution_mode"
    if action_type == "open" and bot is not None:
        if bot.cancel_pending_entries_after_et and bot_time_reached(
            bot,
            time_value=bot.cancel_pending_entries_after_et,
        ):
            return False, "bot_entry_cutoff_reached"
        if execution_mode == "live" and not bot.live_enabled:
            return False, "bot_live_disabled"
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


def _resolve_intent_opportunity_id(
    signal_store: Any,
    intent: dict[str, Any],
) -> str | None:
    opportunity_decision_id = _as_text(intent.get("opportunity_decision_id"))
    if opportunity_decision_id:
        decision = signal_store.get_opportunity_decision(opportunity_decision_id)
        if decision is not None:
            return _as_text(decision.get("opportunity_id"))
    return _as_text(_intent_payload(intent).get("opportunity_id"))


def _opportunity_is_active_for_intent(
    signal_store: Any,
    intent: dict[str, Any],
    *,
    execution_attempt_id: str | None = None,
) -> tuple[bool, str | None]:
    opportunity_id = _resolve_intent_opportunity_id(signal_store, intent)
    if opportunity_id is None:
        return False, "opportunity_missing"
    opportunity = signal_store.get_opportunity(opportunity_id)
    if opportunity is None:
        return False, "opportunity_missing"
    lifecycle_state = str(opportunity.get("lifecycle_state") or "")
    eligibility_state = str(opportunity.get("eligibility_state") or "")
    if lifecycle_state not in {"candidate", "ready", "blocked"}:
        return False, "opportunity_inactive"
    if eligibility_state != "live":
        return False, "opportunity_not_live"
    consumed = _as_text(opportunity.get("consumed_by_execution_attempt_id"))
    if execution_attempt_id and consumed not in {None, "", execution_attempt_id}:
        return False, "opportunity_consumed_elsewhere"
    return True, None


def _position_is_active_for_intent(
    execution_store: Any,
    intent: dict[str, Any],
) -> tuple[bool, str | None]:
    strategy_position_id = _as_text(intent.get("strategy_position_id"))
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
            key=lambda row: parse_datetime(_as_text(row.get("created_at")))
            or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        anchor_id: str | None = None
        for intent in intents:
            state = str(intent.get("state") or "")
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


def _backfill_strategy_position_links(
    execution_store: Any, *, limit: int
) -> dict[str, Any]:
    linked = 0
    results: list[dict[str, Any]] = []
    positions = [
        dict(row)
        for row in execution_store.list_positions(limit=max(int(limit), 1) * 10)
    ]
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


def _active_options_automation_labels(job_store: Any) -> set[str]:
    if job_store is None or not job_store.schema_ready():
        return set()
    labels: set[str] = set()
    for definition in job_store.list_job_definitions(
        enabled_only=True, job_type="live_collector"
    ):
        payload = dict(definition.get("payload") or {})
        if not bool(payload.get("options_automation_enabled", False)):
            continue
        labels.add(resolve_live_collector_label(payload))
    return labels


def _cleanup_terminal_intent_history(
    execution_store: Any,
    *,
    limit: int,
    older_than_minutes: int = 15,
) -> dict[str, Any]:
    threshold = datetime.now(UTC) - timedelta(minutes=max(older_than_minutes, 1))
    retained = 0
    results: list[dict[str, Any]] = []
    intents = [
        dict(row)
        for row in execution_store.list_execution_intents(limit=max(int(limit), 1) * 25)
    ]
    for intent in intents:
        if retained >= max(int(limit), 1):
            break
        state = str(intent.get("state") or "")
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


def _cleanup_stale_automation_opportunities(
    *,
    signal_store: Any,
    job_store: Any,
    market_date: str,
    limit: int,
    older_than_minutes: int = 15,
) -> dict[str, Any]:
    if not signal_store.schema_ready():
        return {"deleted": 0, "terminalized": 0, "results": []}
    active_labels = _active_options_automation_labels(job_store)
    threshold = datetime.now(UTC) - timedelta(minutes=max(older_than_minutes, 1))
    terminalized = 0
    results: list[dict[str, Any]] = []
    opportunities = [
        dict(row)
        for row in signal_store.list_opportunities(
            market_date=market_date,
            runtime_owned=True,
            limit=max(int(limit), 1) * 50,
        )
    ]
    for opportunity in opportunities:
        if terminalized >= max(int(limit), 1):
            break
        opportunity_id = str(opportunity["opportunity_id"])
        label = str(opportunity.get("label") or "")
        lifecycle_state = str(opportunity.get("lifecycle_state") or "")
        updated_at = parse_datetime(_as_text(opportunity.get("updated_at")))
        if updated_at is None or updated_at >= threshold:
            continue
        if _as_text(opportunity.get("consumed_by_execution_attempt_id")):
            continue
        if lifecycle_state == "expired":
            continue
        if label in active_labels:
            continue
        expired = signal_store.expire_opportunity(
            opportunity_id,
            expired_at=_utc_now(),
            reason_code="expired_inactive_automation_label",
        )
        if expired is None:
            continue
        terminalized += 1
        results.append(
            {
                "opportunity_id": opportunity_id,
                "label": label,
                "previous_lifecycle_state": lifecycle_state,
                "lifecycle_state": expired.get("lifecycle_state"),
            }
        )
    return {"deleted": 0, "terminalized": terminalized, "results": results[:25]}
