from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import re
from typing import Any

from core.services.trading_lifecycle import AdmissionState, LifecycleObject


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _safe_component(value: Any) -> str:
    rendered = str(value or "").strip()
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", rendered) or "unknown"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_text(value: Any) -> list[str]:
    if not isinstance(value, list):
        rendered = _as_text(value)
        return [] if rendered is None else [rendered]
    rendered_values: list[str] = []
    for item in value:
        rendered = _as_text(item)
        if rendered is not None and rendered not in rendered_values:
            rendered_values.append(rendered)
    return rendered_values


def normalize_admission_state(status: Any) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"admissible", "approved", "ok", "pass", "passed"}:
        return AdmissionState.APPROVED.value
    if normalized in {"blocked", "rejected", "denied"}:
        return AdmissionState.BLOCKED.value
    return AdmissionState.UNKNOWN.value


def legacy_admission_status(admission_state: Any) -> str:
    state = normalize_admission_state(admission_state)
    if state == AdmissionState.APPROVED.value:
        return "admissible"
    return state


def normalize_lifecycle_admission(
    snapshot: Mapping[str, Any] | None,
    *,
    admission_kind: str,
    source_object_type: str | None = None,
    source_object_id: str | None = None,
    account_id: str | None = None,
    session_date: str | None = None,
    requested_quantity: int | None = None,
    requested_notional: float | None = None,
    max_loss: float | None = None,
    policy_snapshot: Mapping[str, Any] | None = None,
    capability_snapshot: Mapping[str, Any] | None = None,
    metrics: Mapping[str, Any] | None = None,
    evidence: Mapping[str, Any] | None = None,
    reason_codes: list[str] | None = None,
    blockers: list[str] | None = None,
    decided_at: str | None = None,
) -> dict[str, Any]:
    raw = _mapping(snapshot)
    decided_at_value = decided_at or _as_text(raw.get("evaluated_at")) or _utc_now()
    state = normalize_admission_state(raw.get("admission_state") or raw.get("status"))
    resolved_reason_codes = _list_text(reason_codes if reason_codes is not None else raw.get("reason_codes"))
    reason = _as_text(raw.get("reason"))
    if reason is not None and reason not in resolved_reason_codes:
        resolved_reason_codes.append(reason)
    if not resolved_reason_codes and state == AdmissionState.APPROVED.value:
        resolved_reason_codes.append("approved")
    resolved_blockers = _list_text(blockers if blockers is not None else raw.get("blockers"))
    if state in {AdmissionState.BLOCKED.value, AdmissionState.UNKNOWN.value} and reason is not None and reason not in resolved_blockers:
        resolved_blockers.append(reason)

    source_id = source_object_id or _as_text(raw.get("source_object_id")) or _as_text(raw.get("execution_intent_id"))
    admission_id = (
        f"trade_admission:{_safe_component(admission_kind)}:"
        f"{_safe_component(source_object_type)}:{_safe_component(source_id)}"
    )
    normalized = {
        **raw,
        "admission_decision_id": _as_text(raw.get("admission_decision_id")) or admission_id,
        "object_type": LifecycleObject.ADMISSION.value,
        "admission_kind": admission_kind,
        "admission_state": state,
        "lifecycle_state": state,
        "status": legacy_admission_status(state),
        "source_object_type": source_object_type or _as_text(raw.get("source_object_type")),
        "source_object_id": source_id,
        "account_id": account_id or _as_text(raw.get("account_id")),
        "session_date": session_date or _as_text(raw.get("session_date")),
        "requested_quantity": requested_quantity if requested_quantity is not None else raw.get("requested_quantity"),
        "requested_notional": requested_notional if requested_notional is not None else raw.get("requested_notional"),
        "max_loss": max_loss if max_loss is not None else raw.get("max_loss"),
        "reason_codes": resolved_reason_codes,
        "blockers": resolved_blockers,
        "policy_snapshot": dict(policy_snapshot) if policy_snapshot is not None else _mapping(raw.get("policy_snapshot") or raw.get("policy")),
        "capability_snapshot": dict(capability_snapshot) if capability_snapshot is not None else _mapping(raw.get("capability_snapshot")),
        "metrics": dict(metrics) if metrics is not None else _mapping(raw.get("metrics")),
        "evidence": dict(evidence) if evidence is not None else _mapping(raw.get("evidence")),
        "decided_at": decided_at_value,
        "evaluated_at": decided_at_value,
    }
    return normalized


def admission_allows_attempt(admission: Mapping[str, Any]) -> bool:
    return normalize_admission_state(admission.get("admission_state") or admission.get("status")) == AdmissionState.APPROVED.value


__all__ = [
    "admission_allows_attempt",
    "legacy_admission_status",
    "normalize_admission_state",
    "normalize_lifecycle_admission",
]
