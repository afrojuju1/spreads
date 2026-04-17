from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .shared import (
    _resolve_profile,
    normalize_capture_status,
    normalize_tradeability_state,
)

TRADEABILITY_STATE_LIVE_READY = "live_ready"
TRADEABILITY_STATE_DEGRADED_QUOTES = "degraded_quotes"
TRADEABILITY_STATE_RECOVERY_ONLY = "recovery_only"
TRADEABILITY_STATE_RESEARCH_ONLY = "research_only"

NON_HEALTHY_QUOTE_CAPTURE_STATUSES = frozenset(
    {"empty", "baseline_only", "recovery_only"}
)
DEGRADED_QUOTE_CAPTURE_STATUSES = frozenset({"empty", "baseline_only"})
RECOVERY_QUOTE_CAPTURE_STATUSES = frozenset({"recovery_only"})
CAPTURE_HISTORY_BLOCK_THRESHOLD = 3


def build_live_action_gate(
    *,
    profile: str | None,
    label: str | None = None,
    quote_capture: Mapping[str, Any] | None,
) -> dict[str, Any]:
    normalized_profile = _resolve_profile(profile, label=label) or ""
    capture = quote_capture if isinstance(quote_capture, Mapping) else {}
    capture_status = str(capture.get("capture_status") or "").strip().lower()

    if normalized_profile == "0dte" and capture_status in {
        "empty",
        "baseline_only",
        "recovery_only",
    }:
        reason_code = {
            "empty": "quote_capture_empty",
            "baseline_only": "quote_capture_baseline_only",
            "recovery_only": "quote_capture_recovery_only",
        }[capture_status]
        return {
            "status": "blocked",
            "reason_code": reason_code,
            "message": (
                "0DTE live actions are blocked because quote capture did not finish healthy "
                f"({capture_status})."
            ),
            "allow_alerts": False,
            "allow_auto_execution": False,
            "tradeability_state": (
                TRADEABILITY_STATE_RECOVERY_ONLY
                if capture_status == "recovery_only"
                else TRADEABILITY_STATE_DEGRADED_QUOTES
            ),
        }

    return {
        "status": "pass",
        "reason_code": None,
        "message": "Live actions are allowed.",
        "allow_alerts": True,
        "allow_auto_execution": True,
        "tradeability_state": TRADEABILITY_STATE_LIVE_READY,
    }


def build_capture_history_gate(
    recent_capture_statuses: Sequence[str] | None,
    *,
    minimum_consecutive_degraded_slots: int = CAPTURE_HISTORY_BLOCK_THRESHOLD,
) -> dict[str, Any] | None:
    threshold = max(int(minimum_consecutive_degraded_slots), 1)
    normalized_statuses = [
        status
        for status in (
            normalize_capture_status(value)
            for value in list(recent_capture_statuses or [])
        )
        if status is not None
    ]
    if not normalized_statuses:
        return None

    degraded_streak: list[str] = []
    for status in normalized_statuses:
        if status not in NON_HEALTHY_QUOTE_CAPTURE_STATUSES:
            break
        degraded_streak.append(status)
    if len(degraded_streak) < threshold:
        return None

    state = (
        TRADEABILITY_STATE_RECOVERY_ONLY
        if degraded_streak[0] in RECOVERY_QUOTE_CAPTURE_STATUSES
        else TRADEABILITY_STATE_DEGRADED_QUOTES
    )
    joined_statuses = ", ".join(degraded_streak)
    return {
        "status": "blocked",
        "reason_code": "quote_capture_degraded_history",
        "message": (
            "Live actions are blocked after "
            f"{len(degraded_streak)} consecutive non-healthy quote-capture slots "
            f"({joined_statuses})."
        ),
        "allow_alerts": False,
        "allow_auto_execution": False,
        "tradeability_state": state,
        "consecutive_non_healthy_capture_slots": len(degraded_streak),
        "recent_capture_statuses": degraded_streak,
    }


def build_tradeability_summary(
    *,
    capture_status: Any = None,
    live_action_gate: Mapping[str, Any] | None = None,
    slot_health: Mapping[str, Any] | None = None,
    has_live_opportunities: bool | None = None,
    has_analysis_only_opportunities: bool | None = None,
) -> dict[str, Any]:
    normalized_capture_status = normalize_capture_status(capture_status)
    gate = {} if not isinstance(live_action_gate, Mapping) else dict(live_action_gate)
    gate_reason = str(gate.get("reason_code") or "").strip().lower() or None
    gate_message = str(gate.get("message") or "").strip() or None
    gate_tradeability_state = normalize_tradeability_state(
        gate.get("tradeability_state")
    )
    recovery = {} if not isinstance(slot_health, Mapping) else dict(slot_health)
    recovery_state = str(recovery.get("recovery_state") or "").strip().lower()
    gap_active = bool(recovery.get("gap_active"))

    if gap_active or (recovery_state and recovery_state != "clear"):
        return {
            "state": TRADEABILITY_STATE_RECOVERY_ONLY,
            "reason_code": gate_reason or "collector_gap_active",
            "message": gate_message
            or "Collector recovery is active, so the label is limited to recovery-only mode.",
            "capture_status": normalized_capture_status,
            "blocked": True,
        }

    if gate_tradeability_state in {
        TRADEABILITY_STATE_RECOVERY_ONLY,
        TRADEABILITY_STATE_DEGRADED_QUOTES,
    }:
        return {
            "state": gate_tradeability_state,
            "reason_code": gate_reason,
            "message": gate_message,
            "capture_status": normalized_capture_status,
            "blocked": True,
        }

    if normalized_capture_status in RECOVERY_QUOTE_CAPTURE_STATUSES:
        return {
            "state": TRADEABILITY_STATE_RECOVERY_ONLY,
            "reason_code": gate_reason or "quote_capture_recovery_only",
            "message": gate_message
            or "Quote capture only recovered stale data, so the label is recovery-only.",
            "capture_status": normalized_capture_status,
            "blocked": True,
        }

    if normalized_capture_status in DEGRADED_QUOTE_CAPTURE_STATUSES:
        return {
            "state": TRADEABILITY_STATE_DEGRADED_QUOTES,
            "reason_code": gate_reason or f"quote_capture_{normalized_capture_status}",
            "message": gate_message
            or "Quote capture is degraded, so live actions stay paused for this label.",
            "capture_status": normalized_capture_status,
            "blocked": True,
        }

    if has_live_opportunities:
        return {
            "state": TRADEABILITY_STATE_LIVE_READY,
            "reason_code": "live_opportunities_available",
            "message": "Healthy quote capture and live-eligible opportunities are available.",
            "capture_status": normalized_capture_status,
            "blocked": False,
        }

    if has_analysis_only_opportunities:
        return {
            "state": TRADEABILITY_STATE_RESEARCH_ONLY,
            "reason_code": "analysis_only_recovery",
            "message": "Only analysis-only recovery opportunities are available right now.",
            "capture_status": normalized_capture_status,
            "blocked": True,
        }

    if normalized_capture_status == "healthy":
        return {
            "state": TRADEABILITY_STATE_RESEARCH_ONLY,
            "reason_code": "no_live_opportunities",
            "message": "Quote capture is healthy, but no live-ready opportunities are currently available.",
            "capture_status": normalized_capture_status,
            "blocked": True,
        }

    if normalized_capture_status is None:
        return {
            "state": TRADEABILITY_STATE_RESEARCH_ONLY,
            "reason_code": "awaiting_capture",
            "message": "Tradeability is waiting for a completed capture slot.",
            "capture_status": None,
            "blocked": True,
        }

    return {
        "state": TRADEABILITY_STATE_RESEARCH_ONLY,
        "reason_code": gate_reason or f"capture_{normalized_capture_status}",
        "message": gate_message
        or "Tradeability is limited until a healthy live capture slot completes.",
        "capture_status": normalized_capture_status,
        "blocked": True,
    }


__all__ = [
    "CAPTURE_HISTORY_BLOCK_THRESHOLD",
    "TRADEABILITY_STATE_DEGRADED_QUOTES",
    "TRADEABILITY_STATE_LIVE_READY",
    "TRADEABILITY_STATE_RECOVERY_ONLY",
    "TRADEABILITY_STATE_RESEARCH_ONLY",
    "build_capture_history_gate",
    "build_live_action_gate",
    "build_tradeability_summary",
]
