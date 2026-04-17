from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.db.decorators import with_storage
from core.services.selection_terms import (
    MONITOR_SELECTION_STATE,
    PROMOTABLE_SELECTION_STATE,
)
from core.services.uoa_state import get_latest_uoa_state, get_uoa_state_for_cycle
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)

from .shared import OpsLookupError, _attention

UOA_ROOT_LIMIT = 5
UOA_CONTRACT_LIMIT = 8
UOA_CANDIDATE_LIMIT = 8
UOA_EVENT_LIMIT = 20


def _count_rows_from_mapping(
    mapping: Mapping[str, Any] | None,
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    if not isinstance(mapping, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for key, value in mapping.items():
        count = _coerce_int(value)
        if count is None or count <= 0:
            continue
        rows.append({"name": str(key), "count": count})
    rows.sort(key=lambda row: (-int(row["count"]), str(row["name"])))
    return rows[:limit]


def _summarize_uoa_root(root: Mapping[str, Any]) -> dict[str, Any]:
    quote_context = (
        root.get("quote_context")
        if isinstance(root.get("quote_context"), Mapping)
        else {}
    )
    current = root.get("current") if isinstance(root.get("current"), Mapping) else {}
    return {
        "underlying_symbol": root.get("underlying_symbol"),
        "root_score": _coerce_float(root.get("root_score")),
        "scoreable_premium": _coerce_float(root.get("scoreable_premium")),
        "scoreable_trade_count": _coerce_int(root.get("scoreable_trade_count")),
        "scoreable_contract_count": _coerce_int(root.get("scoreable_contract_count")),
        "dominant_flow": root.get("dominant_flow") or current.get("dominant_flow"),
        "dominant_flow_ratio": _coerce_float(
            root.get("dominant_flow_ratio") or current.get("dominant_flow_ratio")
        ),
        "supporting_volume_oi_ratio": _coerce_float(
            root.get("supporting_volume_oi_ratio")
            or current.get("supporting_volume_oi_ratio")
        ),
        "max_volume_oi_ratio": _coerce_float(
            root.get("max_volume_oi_ratio") or current.get("max_volume_oi_ratio")
        ),
        "decision_state": root.get("decision_state"),
        "decision_score": _coerce_float(root.get("decision_score")),
        "quality_state": quote_context.get("quality_state"),
        "liquid_contract_count": _coerce_int(
            quote_context.get("liquid_contract_count")
        ),
        "reason_codes": list(root.get("reason_codes") or []),
        "explanation": _as_text(root.get("explanation")),
    }


def _summarize_uoa_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "option_symbol": contract.get("option_symbol"),
        "underlying_symbol": contract.get("underlying_symbol"),
        "strategy": contract.get("strategy"),
        "option_type": contract.get("option_type"),
        "dte": _coerce_int(contract.get("dte")),
        "percent_otm": _coerce_float(contract.get("percent_otm")),
        "scoreable_premium": _coerce_float(contract.get("scoreable_premium")),
        "scoreable_trade_count": _coerce_int(contract.get("scoreable_trade_count")),
        "contract_score": _coerce_float(contract.get("contract_score")),
        "quality_score": _coerce_float(contract.get("quality_score")),
        "quality_state": contract.get("quality_state"),
        "midpoint": _coerce_float(contract.get("midpoint")),
        "spread_pct": _coerce_float(contract.get("spread_pct")),
        "volume": _coerce_int(contract.get("volume")),
        "open_interest": _coerce_int(contract.get("open_interest")),
        "volume_oi_ratio": _coerce_float(contract.get("volume_oi_ratio")),
        "implied_volatility": _coerce_float(contract.get("implied_volatility")),
        "delta": _coerce_float(contract.get("delta")),
        "quote_age_seconds": _coerce_float(contract.get("quote_age_seconds")),
    }


def _summarize_uoa_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    payload = (
        candidate.get("candidate")
        if isinstance(candidate.get("candidate"), Mapping)
        else {}
    )
    return {
        "candidate_id": candidate.get("candidate_id"),
        "selection_state": candidate.get("selection_state"),
        "selection_rank": _coerce_int(candidate.get("selection_rank")),
        "underlying_symbol": candidate.get("underlying_symbol"),
        "strategy": candidate.get("strategy") or payload.get("strategy"),
        "short_symbol": candidate.get("short_symbol") or payload.get("short_symbol"),
        "long_symbol": candidate.get("long_symbol") or payload.get("long_symbol"),
        "expiration_date": candidate.get("expiration_date")
        or payload.get("expiration_date"),
        "dte": _coerce_int(payload.get("days_to_expiration")),
        "quality_score": _coerce_float(
            candidate.get("quality_score") or payload.get("quality_score")
        ),
        "midpoint_credit": _coerce_float(
            candidate.get("midpoint_credit") or payload.get("midpoint_credit")
        ),
        "max_loss": _coerce_float(payload.get("max_loss")),
        "return_on_risk": _coerce_float(payload.get("return_on_risk")),
        "underlying_price": _coerce_float(payload.get("underlying_price")),
        "setup_status": payload.get("setup_status"),
    }


def _summarize_uoa_event(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": event.get("generated_at"),
        "symbol": event.get("symbol"),
        "event_type": event.get("event_type"),
        "message": event.get("message"),
    }


def _uoa_quote_status(
    quote_capture: Mapping[str, Any],
    quote_overview: Mapping[str, Any],
) -> tuple[str, str | None]:
    capture_status = str(quote_capture.get("capture_status") or "").strip().lower()
    expected_count = (
        _coerce_int(
            quote_capture.get("expected_quote_symbol_count")
            or quote_overview.get("expected_contract_count")
        )
        or 0
    )
    total_saved = _coerce_int(quote_capture.get("total_quote_events_saved")) or 0
    fresh_count = _coerce_int(quote_overview.get("fresh_contract_count")) or 0
    liquid_count = _coerce_int(quote_overview.get("liquid_contract_count")) or 0

    if capture_status == "healthy":
        if expected_count > 0 and fresh_count <= 0:
            return "degraded", "Expected UOA contracts have no fresh quotes."
        if expected_count > 0 and liquid_count <= 0:
            return "degraded", "Expected UOA contracts have no liquid quotes."
        return "healthy", None
    if capture_status == "idle":
        return "idle", None
    if capture_status == "baseline_only":
        return "degraded", "UOA quote capture fell back to baseline-only quotes."
    if capture_status == "recovery_only":
        return "degraded", "UOA quote capture relied only on recovery quotes."
    if capture_status == "empty":
        if expected_count > 0 or total_saved > 0:
            return "degraded", "Expected UOA contracts produced no quote capture."
        return "idle", None
    return "unknown", None


def _uoa_trade_status(
    trade_capture: Mapping[str, Any],
    uoa_overview: Mapping[str, Any],
) -> tuple[str, str | None]:
    capture_status = str(trade_capture.get("capture_status") or "").strip().lower()
    expected_count = _coerce_int(trade_capture.get("expected_trade_symbol_count")) or 0
    raw_trade_count = _coerce_int(uoa_overview.get("raw_trade_count")) or 0
    scoreable_trade_count = _coerce_int(uoa_overview.get("scoreable_trade_count")) or 0
    excluded_trade_count = _coerce_int(uoa_overview.get("excluded_trade_count")) or 0

    if capture_status == "healthy":
        return "healthy", None
    if capture_status == "idle":
        return "idle", None
    if capture_status == "baseline_only" and raw_trade_count > 0:
        return (
            "degraded",
            "UOA trade capture missed websocket trades and used baseline-only data.",
        )
    if capture_status == "empty":
        if raw_trade_count <= 0 and scoreable_trade_count <= 0 and expected_count > 0:
            return "idle", None
        if excluded_trade_count > 0 and scoreable_trade_count <= 0:
            return "idle", None
    return "unknown", None


def _uoa_activity_status(
    uoa_overview: Mapping[str, Any],
    decision_overview: Mapping[str, Any],
) -> str:
    summary_status = str(uoa_overview.get("summary_status") or "").strip().lower()
    decision_status = (
        str(decision_overview.get("decision_status") or "").strip().lower()
    )
    if summary_status == "active" or decision_status == "active":
        return "healthy"
    if summary_status in {
        "empty",
        "captured_no_scoreable_trades",
    } and decision_status in {"", "empty"}:
        return "idle"
    return "unknown"


def _uoa_overall_status(
    *,
    cycle_present: bool,
    quote_status: str,
    trade_status: str,
    activity_status: str,
) -> str:
    if not cycle_present:
        return "degraded"
    if "degraded" in {quote_status, trade_status, activity_status}:
        return "degraded"
    if activity_status == "healthy":
        return "healthy"
    if activity_status == "idle":
        return "idle"
    if quote_status == "healthy" or trade_status == "healthy":
        return "healthy"
    return "unknown"


def _build_uoa_payload(
    *,
    state: Mapping[str, Any],
    generated_at: str,
    view: str,
    requested_label: str | None = None,
) -> dict[str, Any]:
    cycle = state.get("cycle") if isinstance(state.get("cycle"), Mapping) else {}
    job_run = state.get("job_run") if isinstance(state.get("job_run"), Mapping) else {}
    quote_capture = dict(state.get("quote_capture") or {})
    trade_capture = dict(state.get("trade_capture") or {})
    uoa_summary = (
        state.get("uoa_summary")
        if isinstance(state.get("uoa_summary"), Mapping)
        else {}
    )
    uoa_overview = (
        uoa_summary.get("overview")
        if isinstance(uoa_summary.get("overview"), Mapping)
        else {}
    )
    uoa_quote_summary = (
        state.get("uoa_quote_summary")
        if isinstance(state.get("uoa_quote_summary"), Mapping)
        else {}
    )
    uoa_quote_overview = (
        uoa_quote_summary.get("overview")
        if isinstance(uoa_quote_summary.get("overview"), Mapping)
        else {}
    )
    uoa_decisions = (
        state.get("uoa_decisions")
        if isinstance(state.get("uoa_decisions"), Mapping)
        else {}
    )
    uoa_decision_overview = (
        uoa_decisions.get("overview")
        if isinstance(uoa_decisions.get("overview"), Mapping)
        else {}
    )
    opportunities = list(state.get("opportunities") or [])
    promotable_candidates = [
        _summarize_uoa_candidate(row)
        for row in opportunities[:UOA_CANDIDATE_LIMIT]
        if isinstance(row, Mapping)
        and str(row.get("selection_state") or "") == "promotable"
    ]
    monitor_candidates = [
        _summarize_uoa_candidate(row)
        for row in opportunities[:UOA_CANDIDATE_LIMIT]
        if isinstance(row, Mapping)
        and str(row.get("selection_state") or "") == "monitor"
    ]
    cycle_events = [
        _summarize_uoa_event(row)
        for row in list(state.get("cycle_events") or [])[:UOA_EVENT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_roots = [
        _summarize_uoa_root(row)
        for row in list(uoa_summary.get("top_roots") or [])[:UOA_ROOT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_contracts = [
        _summarize_uoa_contract(row)
        for row in list(uoa_summary.get("top_contracts") or [])[:UOA_CONTRACT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_monitor_roots = [
        _summarize_uoa_root(row)
        for row in list(uoa_decisions.get("top_monitor_roots") or [])[:UOA_ROOT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_promotable_roots = [
        _summarize_uoa_root(row)
        for row in list(uoa_decisions.get("top_promotable_roots") or [])[
            :UOA_ROOT_LIMIT
        ]
        if isinstance(row, Mapping)
    ]
    top_high_roots = [
        _summarize_uoa_root(row)
        for row in list(uoa_decisions.get("top_high_roots") or [])[:UOA_ROOT_LIMIT]
        if isinstance(row, Mapping)
    ]
    top_exclusion_reasons = _count_rows_from_mapping(
        uoa_overview.get("excluded_reason_counts")
        if isinstance(uoa_overview, Mapping)
        else None
    )
    top_conditions = _count_rows_from_mapping(
        uoa_overview.get("condition_counts")
        if isinstance(uoa_overview, Mapping)
        else None
    )

    attention: list[dict[str, str]] = []
    cycle_present = bool(cycle)
    if not cycle_present:
        attention.append(
            _attention(
                severity="high",
                code="uoa_cycle_missing",
                message="Collector cycle payload is missing for the selected UOA state.",
            )
        )

    quote_status, quote_message = _uoa_quote_status(quote_capture, uoa_quote_overview)
    if quote_message is not None:
        attention.append(
            _attention(
                severity="medium",
                code="uoa_quote_capture_unhealthy",
                message=quote_message,
            )
        )

    trade_status, trade_message = _uoa_trade_status(trade_capture, uoa_overview)
    if trade_message is not None:
        attention.append(
            _attention(
                severity="medium",
                code="uoa_trade_capture_unhealthy",
                message=trade_message,
            )
        )

    raw_trade_count = _coerce_int(uoa_overview.get("raw_trade_count")) or 0
    scoreable_trade_count = _coerce_int(uoa_overview.get("scoreable_trade_count")) or 0
    excluded_trade_count = _coerce_int(uoa_overview.get("excluded_trade_count")) or 0
    if excluded_trade_count >= 3 and excluded_trade_count > scoreable_trade_count:
        attention.append(
            _attention(
                severity="medium",
                code="uoa_exclusions_dominate",
                message="Most captured UOA trades were excluded from scoring.",
            )
        )

    activity_status = _uoa_activity_status(uoa_overview, uoa_decision_overview)
    overall_status = _uoa_overall_status(
        cycle_present=cycle_present,
        quote_status=quote_status,
        trade_status=trade_status,
        activity_status=activity_status,
    )

    return {
        "status": overall_status,
        "generated_at": generated_at,
        "summary": {
            "view": view,
            "label": cycle.get("label") or requested_label,
            "cycle_id": cycle.get("cycle_id"),
            "session_id": cycle.get("session_id") or job_run.get("session_id"),
            "session_date": cycle.get("session_date"),
            "job_run_id": job_run.get("job_run_id"),
            "job_key": job_run.get("job_key"),
            "slot_at": job_run.get("slot_at"),
            "quote_capture_status": quote_capture.get("capture_status"),
            "trade_capture_status": trade_capture.get("capture_status"),
            "uoa_summary_status": uoa_overview.get("summary_status"),
            "decision_status": uoa_decision_overview.get("decision_status"),
            "observed_contract_count": uoa_overview.get("observed_contract_count"),
            "scoreable_root_count": uoa_overview.get("scoreable_root_count"),
            "raw_trade_count": raw_trade_count,
            "scoreable_trade_count": scoreable_trade_count,
            "excluded_trade_count": excluded_trade_count,
            "root_count": uoa_decision_overview.get("root_count"),
            "monitor_count": uoa_decision_overview.get("monitor_count"),
            "promotable_count": uoa_decision_overview.get("promotable_count"),
            "high_count": uoa_decision_overview.get("high_count"),
            "top_decision_symbol": uoa_decision_overview.get("top_decision_symbol"),
            "top_decision_state": uoa_decision_overview.get("top_decision_state"),
            "top_decision_score": uoa_decision_overview.get("top_decision_score"),
            "selected_promotable_count": int(
                dict(state.get("selection_counts") or {}).get(
                    PROMOTABLE_SELECTION_STATE
                )
                or 0
            ),
            "selected_monitor_count": int(
                dict(state.get("selection_counts") or {}).get(MONITOR_SELECTION_STATE)
                or 0
            ),
            "event_count": len(list(state.get("cycle_events") or [])),
        },
        "attention": attention,
        "details": {
            "view": view,
            "job_run": dict(job_run),
            "cycle": dict(cycle),
            "quote_capture": quote_capture,
            "trade_capture": trade_capture,
            "uoa_overview": dict(uoa_overview),
            "uoa_quote_overview": dict(uoa_quote_overview),
            "uoa_decision_overview": dict(uoa_decision_overview),
            "top_roots": top_roots,
            "top_contracts": top_contracts,
            "top_monitor_roots": top_monitor_roots,
            "top_promotable_roots": top_promotable_roots,
            "top_high_roots": top_high_roots,
            "promotable_candidates": promotable_candidates,
            "monitor_candidates": monitor_candidates,
            "cycle_events": cycle_events,
            "selection_memory": dict(cycle.get("selection_memory") or {}),
            "selection_counts": dict(state.get("selection_counts") or {}),
            "top_exclusion_reasons": top_exclusion_reasons,
            "top_conditions": top_conditions,
        },
    }


@with_storage()
def build_uoa_overview(
    *,
    db_target: str | None = None,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    try:
        state = get_latest_uoa_state(
            db_target=db_target,
            label=label,
            storage=storage,
        )
    except ValueError as exc:
        raise OpsLookupError(str(exc)) from exc
    return _build_uoa_payload(
        state=state,
        generated_at=generated_at,
        view="latest",
        requested_label=label,
    )


@with_storage()
def build_uoa_cycle_view(
    *,
    cycle_id: str,
    db_target: str | None = None,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now()
    try:
        state = get_uoa_state_for_cycle(
            db_target=db_target,
            cycle_id=cycle_id,
            label=label,
            storage=storage,
        )
    except ValueError as exc:
        raise OpsLookupError(str(exc)) from exc
    return _build_uoa_payload(
        state=state,
        generated_at=generated_at,
        view="cycle",
        requested_label=label,
    )
