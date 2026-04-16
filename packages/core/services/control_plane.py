from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import uuid4

from core.db.decorators import with_storage
from core.events.bus import publish_global_event_sync
from core.services.exit_manager import normalize_exit_policy
from core.services.risk_manager import normalize_risk_policy, resolve_execution_kill_switch_reason
from core.services.value_coercion import (
    as_text as _as_text,
    utc_now_iso as _utc_now,
)

CONTROL_SCHEMA_MESSAGE = "Control plane storage is not available. Run database migrations."
CONTROL_STATE_ID = "global"
CONTROL_SCOPE_GLOBAL = "global"
CONTROL_MODE_NORMAL = "normal"
CONTROL_MODE_DEGRADED = "degraded"
CONTROL_MODE_HALTED = "halted"
CONTROL_MODES = {
    CONTROL_MODE_NORMAL,
    CONTROL_MODE_DEGRADED,
    CONTROL_MODE_HALTED,
}
OPEN_ACTIVITY_MANUAL = "manual_open_execution"
OPEN_ACTIVITY_AUTO = "auto_open_execution"
POLICY_FAMILIES = {
    "risk_policy",
    "execution_policy",
    "exit_policy",
}

def _operator_action_id() -> str:
    return f"operator_action:{uuid4().hex}"


def _policy_rollout_id() -> str:
    return f"policy_rollout:{uuid4().hex}"


def _policy_version_token(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:12]


def _default_control_record() -> dict[str, Any]:
    now = _utc_now()
    return {
        "control_state_id": CONTROL_STATE_ID,
        "mode": CONTROL_MODE_NORMAL,
        "reason_code": None,
        "note": None,
        "source_kind": "default",
        "triggered_by_action_id": None,
        "effective_at": now,
        "updated_at": now,
        "metadata": {},
    }


def _normalize_control_mode(mode: str) -> str:
    normalized = str(mode).strip().lower()
    if normalized not in CONTROL_MODES:
        raise ValueError(f"Unsupported control mode: {mode}")
    return normalized


def _build_policy_ref(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "family": str(row["family"]),
        "key": str(row["policy_rollout_id"]),
        "version": str(row["version_token"]),
        "source_kind": str(row["source_kind"]),
        "policy_rollout_id": str(row["policy_rollout_id"]),
        "operator_action_id": _as_text(row.get("operator_action_id")),
        "effective_at": _as_text(row.get("effective_at")),
    }


def _normalize_rollout_policy(family: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    if family == "risk_policy":
        return normalize_risk_policy(payload)
    if family == "exit_policy":
        return normalize_exit_policy(payload)
    if family == "execution_policy":
        from core.services.execution import normalize_execution_policy

        return normalize_execution_policy(payload)
    raise ValueError(f"Unsupported policy family: {family}")


@with_storage()
def get_active_policy_rollout_map(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, dict[str, Any]]:
    control_store = getattr(storage, "control", None)
    if control_store is None or not control_store.schema_ready():
        return {}
    rollouts = control_store.list_active_policy_rollouts()
    return {str(row["family"]): dict(row) for row in rollouts}


@with_storage()
def get_control_state_snapshot(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    control_store = getattr(storage, "control", None)
    record = (
        _default_control_record()
        if control_store is None or not control_store.schema_ready()
        else dict(control_store.get_control_state(CONTROL_STATE_ID) or _default_control_record())
    )
    active_rollouts = get_active_policy_rollout_map(storage=storage)
    configured_mode = _normalize_control_mode(str(record.get("mode") or CONTROL_MODE_NORMAL))
    effective_mode = configured_mode
    reason_code = _as_text(record.get("reason_code"))
    note = _as_text(record.get("note"))
    source_kind = _as_text(record.get("source_kind")) or "default"
    blockers: list[dict[str, Any]] = []
    kill_switch_reason = resolve_execution_kill_switch_reason()
    if kill_switch_reason is not None:
        effective_mode = CONTROL_MODE_HALTED
        reason_code = "kill_switch_enabled"
        note = kill_switch_reason
        source_kind = "environment"
        blockers.append(
            {
                "code": "kill_switch_enabled",
                "category": "kill_switch",
                "note": kill_switch_reason,
            }
        )
    return {
        "control_state_id": CONTROL_STATE_ID,
        "mode": effective_mode,
        "configured_mode": configured_mode,
        "reason_code": reason_code,
        "note": note,
        "source_kind": source_kind,
        "configured_source_kind": _as_text(record.get("source_kind")) or "default",
        "triggered_by_action_id": _as_text(record.get("triggered_by_action_id")),
        "effective_at": _as_text(record.get("effective_at")) or _utc_now(),
        "updated_at": _as_text(record.get("updated_at")) or _utc_now(),
        "metadata": dict(record.get("metadata") or {}),
        "blockers": blockers,
        "active_policy_refs": {
            family: _build_policy_ref(row)
            for family, row in active_rollouts.items()
        },
    }


def assess_open_activity_gate(
    *,
    activity_kind: str,
    storage: Any,
) -> dict[str, Any]:
    control = get_control_state_snapshot(storage=storage)
    blockers = list(control.get("blockers") or [])
    if blockers:
        return {
            "allowed": False,
            "decision": "blocked",
            "reason": str(blockers[0]["code"]),
            "message": str(blockers[0]["note"]),
            "block_category": str(blockers[0]["category"]),
            "control": control,
        }
    mode = str(control["mode"])
    if mode == CONTROL_MODE_HALTED:
        return {
            "allowed": False,
            "decision": "blocked",
            "reason": "control_mode_halted",
            "message": "Open execution is blocked because control mode is halted.",
            "block_category": "control_mode",
            "control": control,
        }
    if mode == CONTROL_MODE_DEGRADED and activity_kind == OPEN_ACTIVITY_AUTO:
        return {
            "allowed": False,
            "decision": "skipped",
            "reason": "control_mode_degraded",
            "message": "Automatic execution is suppressed because control mode is degraded.",
            "block_category": "control_mode",
            "control": control,
        }
    return {
        "allowed": True,
        "decision": "pass",
        "reason": None,
        "message": None,
        "block_category": None,
        "control": control,
    }


def publish_control_gate_event(
    *,
    db_target: str | None,
    decision: dict[str, Any],
    activity_kind: str,
    session_id: str | None,
    session_date: str | None,
    label: str | None,
    candidate_id: int | None = None,
    cycle_id: str | None = None,
) -> None:
    if decision.get("allowed"):
        return
    control = dict(decision.get("control") or {})
    payload = {
        "activity_kind": activity_kind,
        "decision": decision.get("decision"),
        "reason": decision.get("reason"),
        "message": decision.get("message"),
        "block_category": decision.get("block_category"),
        "session_id": session_id,
        "session_date": session_date,
        "label": label,
        "candidate_id": candidate_id,
        "cycle_id": cycle_id,
        "control": control,
    }
    publish_global_event_sync(
        topic="control.execution.skipped" if decision.get("decision") == "skipped" else "control.execution.blocked",
        entity_type="control_state",
        entity_id=CONTROL_STATE_ID,
        payload=payload,
        timestamp=_utc_now(),
        source="control_plane",
        session_date=session_date,
        correlation_id=_as_text(control.get("triggered_by_action_id")) or session_id,
        causation_id=cycle_id or (None if candidate_id is None else str(candidate_id)),
        database_url=db_target,
    )


@with_storage()
def set_control_mode(
    *,
    db_target: str,
    mode: str,
    reason_code: str,
    note: str | None = None,
    source_kind: str = "api",
    actor_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    control_store = getattr(storage, "control", None)
    if control_store is None or not control_store.schema_ready():
        raise RuntimeError(CONTROL_SCHEMA_MESSAGE)
    normalized_mode = _normalize_control_mode(mode)
    normalized_reason_code = _as_text(reason_code)
    if normalized_reason_code is None:
        raise ValueError("reason_code is required")
    normalized_note = _as_text(note)
    requested_metadata = dict(metadata or {})
    current = dict(control_store.get_control_state(CONTROL_STATE_ID) or _default_control_record())
    if (
        str(current.get("mode") or CONTROL_MODE_NORMAL) == normalized_mode
        and _as_text(current.get("reason_code")) == normalized_reason_code
        and _as_text(current.get("note")) == normalized_note
    ):
        return {
            "action": "set_mode",
            "changed": False,
            "message": f"Control mode is already {normalized_mode}.",
            "control": get_control_state_snapshot(storage=storage),
        }
    now = _utc_now()
    action_id = _operator_action_id()
    resulting_state = {
        "mode": normalized_mode,
        "reason_code": normalized_reason_code,
        "note": normalized_note,
        "effective_at": now,
        "metadata": requested_metadata,
    }
    operator_action = control_store.append_operator_action(
        operator_action_id=action_id,
        action_kind="set_mode",
        source_kind=source_kind,
        actor_id=actor_id,
        target_scope=CONTROL_SCOPE_GLOBAL,
        requested_payload={
            "mode": normalized_mode,
            "reason_code": normalized_reason_code,
            "note": normalized_note,
            "metadata": requested_metadata,
        },
        resulting_state=resulting_state,
        note=normalized_note,
        correlation_id=action_id,
        causation_id=_as_text(current.get("triggered_by_action_id")),
        occurred_at=now,
    )
    control_state = control_store.upsert_control_state(
        control_state_id=CONTROL_STATE_ID,
        mode=normalized_mode,
        reason_code=normalized_reason_code,
        note=normalized_note,
        source_kind=source_kind,
        triggered_by_action_id=str(operator_action["operator_action_id"]),
        effective_at=now,
        updated_at=now,
        metadata=requested_metadata,
    )
    snapshot = get_control_state_snapshot(storage=storage)
    publish_global_event_sync(
        topic="control.mode.updated",
        entity_type="control_state",
        entity_id=CONTROL_STATE_ID,
        payload={
            "action": "set_mode",
            "operator_action": dict(operator_action),
            "control": snapshot,
            "previous_mode": str(current.get("mode") or CONTROL_MODE_NORMAL),
        },
        timestamp=str(control_state["effective_at"]),
        source="control_plane",
        correlation_id=str(operator_action["operator_action_id"]),
        causation_id=str(operator_action["operator_action_id"]),
        database_url=db_target,
    )
    return {
        "action": "set_mode",
        "changed": True,
        "message": f"Control mode set to {normalized_mode}.",
        "control": snapshot,
        "operator_action": dict(operator_action),
    }


@with_storage()
def create_policy_rollout(
    *,
    db_target: str,
    family: str,
    policy: dict[str, Any],
    note: str | None = None,
    source_kind: str = "internal",
    metadata: dict[str, Any] | None = None,
    operator_action_id: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    control_store = getattr(storage, "control", None)
    if control_store is None or not control_store.schema_ready():
        raise RuntimeError(CONTROL_SCHEMA_MESSAGE)
    normalized_family = str(family).strip()
    if normalized_family not in POLICY_FAMILIES:
        raise ValueError(f"Unsupported policy family: {family}")
    normalized_policy = _normalize_rollout_policy(normalized_family, policy)
    rollout = control_store.create_policy_rollout(
        policy_rollout_id=_policy_rollout_id(),
        family=normalized_family,
        scope_kind=CONTROL_SCOPE_GLOBAL,
        scope_key=None,
        status="active",
        version_token=_policy_version_token(normalized_policy),
        policy=normalized_policy,
        note=_as_text(note),
        source_kind=source_kind,
        operator_action_id=operator_action_id,
        effective_at=_utc_now(),
        ended_at=None,
        metadata=dict(metadata or {}),
    )
    return dict(rollout)
