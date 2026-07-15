from __future__ import annotations

from typing import Any

from core.services.admission_lifecycle import admission_allows_attempt, normalize_lifecycle_admission
from core.services.candidate_identity import resolve_candidate_identity
from core.services.trading_engine.entry_signals import (
    candidate_payload,
    quality_evidence_summary,
)
from core.services.trading_strategy_runtime_models import EntryRuntime
from core.value_coercion import utc_now_iso as _utc_now


def _persist_trade_admission_handoff(
    *,
    execution_store: Any,
    runtime: EntryRuntime,
    market_date: str,
    policy_ref: dict[str, Any],
    trade_signal_id: str,
    trade_decision_id: str,
    execution_intent_id: str,
    slot_key: str,
    admission_snapshot: dict[str, Any],
    signal: dict[str, Any],
    expires_at: str,
    execution_intent_payload: dict[str, Any] | None = None,
    execution_intent_created_event_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate = candidate_payload(signal)
    candidate_identity = str(signal.get("candidate_identity") or resolve_candidate_identity(candidate, strategy=candidate.get("strategy"))).strip()
    normalized = normalize_lifecycle_admission(
        admission_snapshot,
        admission_kind="entry_open",
        source_object_type="trade_decision",
        source_object_id=trade_decision_id,
        session_date=market_date,
        requested_quantity=1,
        requested_notional=admission_snapshot.get("required_buying_power"),
        max_loss=admission_snapshot.get("required_buying_power"),
        policy_snapshot=policy_ref,
        metrics={
            "admissible_quantity": admission_snapshot.get("admissible_quantity"),
            "required_buying_power": admission_snapshot.get("required_buying_power"),
            "available_buying_power": admission_snapshot.get("available_buying_power"),
            "protection_admission": dict(admission_snapshot.get("protection_admission") or {}),
            "portfolio_admission": dict(admission_snapshot.get("portfolio_admission") or {}),
        },
        evidence={
            "trade_signal_id": trade_signal_id,
            "trade_decision_id": trade_decision_id,
            "proposed_execution_intent_id": execution_intent_id,
            "slot_key": slot_key,
            "underlying_symbol": signal.get("underlying_symbol"),
            "candidate_identity": candidate_identity,
            "admission_boundary": admission_snapshot.get("admission_boundary"),
            "capacity_admission_kind": admission_snapshot.get("capacity_admission_kind"),
            "capacity_admission_status": admission_snapshot.get("capacity_admission_status"),
            "protection_admission_status": admission_snapshot.get("protection_admission_status"),
            "protection_admission_reason": admission_snapshot.get("protection_admission_reason"),
            "portfolio_admission_status": admission_snapshot.get("portfolio_admission_status"),
            "portfolio_admission_reason": admission_snapshot.get("portfolio_admission_reason"),
            "execution_readiness_status": admission_snapshot.get("execution_readiness_status"),
            "execution_readiness_reason": admission_snapshot.get("execution_readiness_reason"),
            "capacity_admission": dict(admission_snapshot.get("capacity_admission") or {}),
            "protection_admission": dict(admission_snapshot.get("protection_admission") or {}),
            "portfolio_admission": dict(admission_snapshot.get("portfolio_admission") or {}),
            "execution_readiness": dict(admission_snapshot.get("execution_readiness") or {}),
            **quality_evidence_summary(signal),
        },
    )
    now = _utc_now()
    current_execution_intent: dict[str, Any] | None = None
    current_created_event: dict[str, Any] | None = None
    if admission_allows_attempt(normalized):
        if execution_intent_payload is None or execution_intent_created_event_payload is None:
            raise RuntimeError("Approved admission is missing a prebuilt execution intent payload.")
        current_payload = {
            **execution_intent_payload,
            "admission_decision_id": normalized["admission_decision_id"],
            "execution_admission": normalized,
        }
        current_execution_intent = {
            "execution_intent_id": execution_intent_id,
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_signal_id": trade_signal_id,
            "trade_decision_id": trade_decision_id,
            "admission_decision_id": normalized["admission_decision_id"],
            "close_decision_id": None,
            "position_id": None,
            "intent_kind": "open",
            "slot_key": slot_key,
            "claim_token": None,
            "claimed_at": None,
            "workflow_id": None,
            "workflow_run_id": None,
            "policy_ref": policy_ref,
            "config_hash": runtime.config_hash,
            "state": "pending",
            "expires_at": expires_at,
            "supersedes_execution_intent_id": None,
            "state_version": 1,
            "payload": current_payload,
            "created_at": now,
            "updated_at": now,
        }
        current_created_event = {
            **execution_intent_created_event_payload,
            "admission_decision_id": normalized["admission_decision_id"],
        }
    handoff = execution_store.persist_admission_intent_handoff(
        admission={
            "admission_decision_id": str(normalized["admission_decision_id"]),
            "source_object_type": str(normalized["source_object_type"]),
            "source_object_id": str(normalized["source_object_id"]),
            "trade_signal_id": trade_signal_id,
            "trade_decision_id": trade_decision_id,
            "close_decision_id": None,
            "position_id": None,
            "admission_kind": str(normalized["admission_kind"]),
            "admission_state": str(normalized["admission_state"]),
            "account_id": None,
            "session_date": market_date,
            "requested_quantity": normalized.get("requested_quantity"),
            "requested_notional": normalized.get("requested_notional"),
            "max_loss": normalized.get("max_loss"),
            "policy_snapshot": dict(normalized.get("policy_snapshot") or {}),
            "capability_snapshot": dict(normalized.get("capability_snapshot") or {}),
            "metrics": dict(normalized.get("metrics") or {}),
            "reason_codes": list(normalized.get("reason_codes") or []),
            "blockers": list(normalized.get("blockers") or []),
            "evidence": dict(normalized.get("evidence") or {}),
            "note": normalized.get("message") or normalized.get("reason"),
            "decided_at": str(normalized["decided_at"]),
        },
        execution_intent=current_execution_intent,
        created_event_payload=current_created_event,
    )
    admission = dict(handoff["admission"])
    return {
        "admission": {
            **dict(normalized),
            "admission_decision_id": admission["admission_decision_id"],
            **(
                {"execution_intent_id": execution_intent_id}
                if handoff.get("execution_intent") is not None
                else {}
            ),
        },
        "execution_intent": handoff.get("execution_intent"),
    }


__all__ = ["_persist_trade_admission_handoff"]
