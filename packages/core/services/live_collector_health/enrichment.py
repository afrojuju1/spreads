from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.selection_terms import normalize_uoa_decision_state

from .capture import build_quote_capture_summary, build_trade_capture_summary
from .selection import normalize_selection_summary
from .shared import (
    _normalize_text_list,
    _read_float,
    _read_int,
    _read_text,
    _strategy_from_opportunity_id,
    _symbol_from_opportunity_id,
)
from .tradeability import build_live_action_gate

AUTO_EXECUTION_SKIPPED_REASONS = frozenset(
    {
        "execution_disabled",
        "no_live_opportunity",
        "no_allocated_opportunity",
        "live_opportunity_store_unavailable",
        "selected_opportunity_missing",
        "selected_opportunity_missing_candidate",
    }
)


def build_auto_execution_summary(
    auto_execution: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(auto_execution, Mapping):
        return None
    execution_plan = (
        auto_execution.get("execution_plan")
        if isinstance(auto_execution.get("execution_plan"), Mapping)
        else {}
    )
    selected_intent = (
        execution_plan.get("selected_execution_intent")
        if isinstance(execution_plan.get("selected_execution_intent"), Mapping)
        else {}
    )
    selected_decision = (
        execution_plan.get("selected_allocation_decision")
        if isinstance(execution_plan.get("selected_allocation_decision"), Mapping)
        else {}
    )
    top_decision = (
        execution_plan.get("top_allocation_decision")
        if isinstance(execution_plan.get("top_allocation_decision"), Mapping)
        else {}
    )
    reason = _read_text(auto_execution.get("reason"))
    changed = bool(auto_execution.get("changed"))
    if changed:
        status = "submitted"
    elif reason is None:
        status = "skipped"
    elif reason in AUTO_EXECUTION_SKIPPED_REASONS:
        status = "skipped"
    else:
        status = "blocked"
    decision = selected_decision or top_decision
    execution_blockers = _normalize_text_list(
        decision.get("rejection_codes") if isinstance(decision, Mapping) else None
    )
    if not execution_blockers and reason is not None and not changed:
        execution_blockers = [reason]
    return _with_auto_execution_target_fallbacks(
        {
            "status": status,
            "changed": changed,
            "reason": reason,
            "message": _read_text(auto_execution.get("message")),
            "selected_opportunity_id": _read_text(
                auto_execution.get("selected_opportunity_id")
            ),
            "selected_candidate_id": _read_int(auto_execution, "selected_candidate_id")
            or None,
            "selected_symbol": _read_text(selected_intent.get("symbol"))
            or _read_text(execution_plan.get("top_symbol")),
            "selected_strategy_family": _read_text(
                selected_intent.get("strategy_family")
            )
            or _read_text(execution_plan.get("top_strategy_family")),
            "allocation_score": _read_float(decision, "allocation_score"),
            "decision_reason": _read_text(
                decision.get("allocation_reason")
                if isinstance(decision, Mapping)
                else None
            ),
            "execution_blockers": execution_blockers,
            "candidate_count": _read_int(execution_plan, "candidate_count"),
            "allocation_count": _read_int(execution_plan, "allocation_count"),
            "execution_intent_count": _read_int(
                execution_plan, "execution_intent_count"
            ),
            "top_opportunity_id": _read_text(execution_plan.get("top_opportunity_id")),
        }
    )


def _with_auto_execution_target_fallbacks(summary: dict[str, Any]) -> dict[str, Any]:
    selected_opportunity_id = summary.get("selected_opportunity_id") or summary.get(
        "top_opportunity_id"
    )
    if summary.get("selected_symbol") in (None, ""):
        summary["selected_symbol"] = _symbol_from_opportunity_id(
            selected_opportunity_id
        )
    if summary.get("selected_strategy_family") in (None, ""):
        summary["selected_strategy_family"] = _strategy_from_opportunity_id(
            selected_opportunity_id
        )
    return summary


def _normalize_uoa_root(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    decision_state = normalize_uoa_decision_state(payload.get("decision_state"))
    if decision_state is not None:
        payload["decision_state"] = decision_state
    return payload


def normalize_uoa_decisions_payload(
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    source = {} if not isinstance(payload, Mapping) else dict(payload)
    overview = (
        {}
        if not isinstance(source.get("overview"), Mapping)
        else dict(source.get("overview"))
    )
    normalized_overview = {
        key: value
        for key, value in overview.items()
        if key not in {"watchlist_count", "board_count"}
    }
    normalized_overview.setdefault("monitor_count", overview.get("watchlist_count"))
    normalized_overview.setdefault("promotable_count", overview.get("board_count"))
    normalized_top_decision_state = normalize_uoa_decision_state(
        normalized_overview.get("top_decision_state")
    )
    if normalized_top_decision_state is not None:
        normalized_overview["top_decision_state"] = normalized_top_decision_state
    roots = [
        _normalize_uoa_root(item)
        for item in list(source.get("roots") or [])
        if isinstance(item, Mapping)
    ]
    top_monitor_roots = [
        _normalize_uoa_root(item)
        for item in list(
            source.get("top_monitor_roots", source.get("top_watchlist_roots")) or []
        )
        if isinstance(item, Mapping)
    ]
    top_promotable_roots = [
        _normalize_uoa_root(item)
        for item in list(
            source.get("top_promotable_roots", source.get("top_board_roots")) or []
        )
        if isinstance(item, Mapping)
    ]
    top_high_roots = [
        _normalize_uoa_root(item)
        for item in list(source.get("top_high_roots") or [])
        if isinstance(item, Mapping)
    ]
    return {
        **{
            key: value
            for key, value in source.items()
            if key not in {"overview", "top_watchlist_roots", "top_board_roots"}
        },
        "overview": normalized_overview,
        "roots": roots,
        "top_monitor_roots": top_monitor_roots,
        "top_promotable_roots": top_promotable_roots,
        "top_high_roots": top_high_roots,
    }


def enrich_live_collector_result(
    result: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if result is None:
        return None
    enriched = dict(result)
    quote_capture = build_quote_capture_summary(
        expected_quote_symbols=enriched.get("expected_quote_symbols"),
        total_quote_events_saved=_read_int(enriched, "quote_events_saved"),
        baseline_quote_events_saved=_read_int(enriched, "baseline_quote_events_saved"),
        stream_quote_events_saved=_read_int(enriched, "stream_quote_events_saved"),
        websocket_quote_events_saved=_read_int(
            enriched, "websocket_quote_events_saved"
        ),
        recovery_quote_events_saved=_read_int(enriched, "recovery_quote_events_saved"),
    )
    trade_capture = build_trade_capture_summary(
        expected_trade_symbols=enriched.get("expected_trade_symbols"),
        total_trade_events_saved=_read_int(enriched, "trade_events_saved"),
        stream_trade_events_saved=_read_int(enriched, "stream_trade_events_saved"),
        websocket_trade_events_saved=_read_int(
            enriched, "websocket_trade_events_saved"
        ),
    )
    enriched["stream_quote_events_saved"] = quote_capture["stream_quote_events_saved"]
    enriched["websocket_quote_events_saved"] = quote_capture[
        "websocket_quote_events_saved"
    ]
    enriched["stream_trade_events_saved"] = trade_capture["stream_trade_events_saved"]
    enriched["websocket_trade_events_saved"] = trade_capture[
        "websocket_trade_events_saved"
    ]
    enriched["quote_capture"] = quote_capture
    enriched["trade_capture"] = trade_capture
    enriched["uoa_decisions"] = normalize_uoa_decisions_payload(
        enriched.get("uoa_decisions")
        if isinstance(enriched.get("uoa_decisions"), Mapping)
        else None
    )
    enriched["selection_summary"] = normalize_selection_summary(
        enriched.get("selection_summary")
        if isinstance(enriched.get("selection_summary"), Mapping)
        else None
    )
    enriched["auto_execution_summary"] = build_auto_execution_summary(
        enriched.get("auto_execution")
        if isinstance(enriched.get("auto_execution"), Mapping)
        else None
    )
    enriched["live_action_gate"] = dict(
        enriched.get("live_action_gate")
        or build_live_action_gate(
            profile=str(enriched.get("profile") or ""),
            label=str(enriched.get("label") or ""),
            quote_capture=quote_capture,
        )
    )
    return enriched


def enrich_live_collector_job_run_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    enriched = dict(payload)
    if enriched.get("job_type") != "live_collector":
        return enriched
    result = enrich_live_collector_result(
        enriched.get("result") if isinstance(enriched.get("result"), Mapping) else None
    )
    if result is None:
        return enriched
    enriched["result"] = result
    enriched["quote_capture"] = result["quote_capture"]
    enriched["trade_capture"] = result["trade_capture"]
    enriched["uoa_summary"] = result.get("uoa_summary") or {}
    enriched["uoa_quote_summary"] = result.get("uoa_quote_summary") or {}
    enriched["uoa_decisions"] = result.get("uoa_decisions") or {}
    enriched["selection_summary"] = result.get("selection_summary") or {}
    enriched["raw_candidate_summary"] = result.get("raw_candidate_summary") or {}
    enriched["auto_execution_summary"] = result.get("auto_execution_summary")
    enriched["capture_status"] = result["quote_capture"]["capture_status"]
    run_payload = (
        enriched.get("payload") if isinstance(enriched.get("payload"), Mapping) else {}
    )
    enriched["live_action_gate"] = dict(
        result.get("live_action_gate")
        or build_live_action_gate(
            profile=str(run_payload.get("profile") or result.get("profile") or ""),
            label=str(
                run_payload.get("label")
                or result.get("label")
                or enriched.get("label")
                or ""
            ),
            quote_capture=result.get("quote_capture"),
        )
    )
    return enriched


__all__ = [
    "build_auto_execution_summary",
    "enrich_live_collector_job_run_payload",
    "enrich_live_collector_result",
    "normalize_uoa_decisions_payload",
]
