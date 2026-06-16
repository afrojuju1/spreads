from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.trading_lifecycle import AdmissionState, LifecycleObject
from core.value_coercion import as_mapping, as_text as _as_text, safe_component, unique_text_list, utc_now_iso as _utc_now


def normalize_admission_state(status: Any) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"admissible", "approved", "ok", "pass", "passed"}:
        return AdmissionState.APPROVED.value
    if normalized in {"blocked", "rejected", "denied"}:
        return AdmissionState.BLOCKED.value
    return AdmissionState.UNKNOWN.value


def admission_status(admission_state: Any) -> str:
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
    raw = as_mapping(snapshot)
    decided_at_value = decided_at or _as_text(raw.get("evaluated_at")) or _utc_now()
    state = normalize_admission_state(raw.get("admission_state") or raw.get("status"))
    resolved_reason_codes = unique_text_list(reason_codes if reason_codes is not None else raw.get("reason_codes"), accept_scalar=True)
    reason = _as_text(raw.get("reason"))
    if reason is not None and reason not in resolved_reason_codes:
        resolved_reason_codes.append(reason)
    if not resolved_reason_codes and state == AdmissionState.APPROVED.value:
        resolved_reason_codes.append("approved")
    resolved_blockers = unique_text_list(blockers if blockers is not None else raw.get("blockers"), accept_scalar=True)
    if state in {AdmissionState.BLOCKED.value, AdmissionState.UNKNOWN.value} and reason is not None and reason not in resolved_blockers:
        resolved_blockers.append(reason)

    source_id = source_object_id or _as_text(raw.get("source_object_id")) or _as_text(raw.get("execution_intent_id"))
    admission_id = f"trade_admission:{safe_component(admission_kind)}:" f"{safe_component(source_object_type)}:{safe_component(source_id)}"
    return {
        **raw,
        "admission_decision_id": _as_text(raw.get("admission_decision_id")) or admission_id,
        "object_type": LifecycleObject.ADMISSION.value,
        "admission_kind": admission_kind,
        "admission_state": state,
        "lifecycle_state": state,
        "status": admission_status(state),
        "source_object_type": source_object_type or _as_text(raw.get("source_object_type")),
        "source_object_id": source_id,
        "account_id": account_id or _as_text(raw.get("account_id")),
        "session_date": session_date or _as_text(raw.get("session_date")),
        "requested_quantity": requested_quantity if requested_quantity is not None else raw.get("requested_quantity"),
        "requested_notional": requested_notional if requested_notional is not None else raw.get("requested_notional"),
        "max_loss": max_loss if max_loss is not None else raw.get("max_loss"),
        "reason_codes": resolved_reason_codes,
        "blockers": resolved_blockers,
        "policy_snapshot": dict(policy_snapshot) if policy_snapshot is not None else as_mapping(raw.get("policy_snapshot") or raw.get("policy")),
        "capability_snapshot": dict(capability_snapshot) if capability_snapshot is not None else as_mapping(raw.get("capability_snapshot")),
        "metrics": dict(metrics) if metrics is not None else as_mapping(raw.get("metrics")),
        "evidence": dict(evidence) if evidence is not None else as_mapping(raw.get("evidence")),
        "decided_at": decided_at_value,
        "evaluated_at": decided_at_value,
    }


def admission_allows_attempt(admission: Mapping[str, Any]) -> bool:
    return normalize_admission_state(admission.get("admission_state") or admission.get("status")) == AdmissionState.APPROVED.value


__all__ = [
    "admission_allows_attempt",
    "admission_status",
    "normalize_admission_state",
    "normalize_lifecycle_admission",
]
