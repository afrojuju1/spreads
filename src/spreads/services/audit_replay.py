from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from spreads.db.decorators import with_storage
from spreads.services.pipelines import (
    DEFAULT_ANALYSIS_PROFIT_TARGET,
    DEFAULT_ANALYSIS_STOP_MULTIPLE,
    get_pipeline_detail,
)
from spreads.services.runtime_identity import build_pipeline_id, parse_live_session_id
from spreads.services.selection_terms import promotable_monitor_pnl_spread
from spreads.storage.serializers import parse_datetime

DEFAULT_TIMELINE_LIMIT = 500
DEFAULT_EVENT_SCAN_LIMIT = 5000
MARKET_QUOTE_CAPTURE_TOPIC = "market.quote.captured"


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _confidence_from_quality_score(value: Any) -> float | None:
    score = _coerce_float(value)
    if score is None:
        return None
    normalized = (score - 55.0) / 27.0
    return round(max(0.0, min(normalized, 1.0)), 4)


def _timeline_sort_key(item: Mapping[str, Any]) -> tuple[Any, str, str, str]:
    timestamp = parse_datetime(_as_text(item.get("at")) or "1970-01-01T00:00:00Z")
    return (
        timestamp,
        str(item.get("topic") or ""),
        str(item.get("entity_type") or ""),
        str(item.get("entity_id") or ""),
    )


def _detail_subset(payload: Mapping[str, Any]) -> dict[str, Any]:
    detail_keys = (
        "session_id",
        "session_date",
        "label",
        "cycle_id",
        "candidate_id",
        "underlying_symbol",
        "strategy",
        "state",
        "from_state",
        "to_state",
        "lifecycle_state",
        "selection_state",
        "trade_intent",
        "status",
        "decision_kind",
        "mode",
        "reason",
        "reason_code",
        "message",
        "opportunity_id",
        "signal_state_id",
        "risk_decision_id",
        "execution_attempt_id",
        "broker_order_id",
        "job_run_id",
        "job_type",
    )
    details = {key: payload[key] for key in detail_keys if payload.get(key) is not None}
    if payload.get("reason_codes"):
        details["reason_codes"] = list(payload["reason_codes"])
    if payload.get("blockers"):
        details["blockers"] = list(payload["blockers"])
    return details


def _event_summary(event: Mapping[str, Any]) -> str:
    topic = str(event.get("topic") or "")
    payload = dict(event.get("payload") or {})
    entity_key = _as_text(event.get("entity_key"))
    underlying_symbol = _as_text(payload.get("underlying_symbol"))
    status = _as_text(payload.get("status"))
    if topic == "signal.transition.recorded":
        symbol = underlying_symbol or entity_key or "signal"
        from_state = _as_text(payload.get("from_state")) or "unknown"
        to_state = _as_text(payload.get("to_state")) or "unknown"
        return f"{symbol} signal transitioned from {from_state} to {to_state}."
    if topic == "signal.state.updated":
        symbol = underlying_symbol or entity_key or "signal"
        state = _as_text(payload.get("state")) or "updated"
        return f"{symbol} signal is {state}."
    if topic == "opportunity.lifecycle.updated":
        symbol = underlying_symbol or entity_key or "opportunity"
        lifecycle_state = _as_text(payload.get("lifecycle_state")) or "updated"
        return f"{symbol} opportunity is {lifecycle_state}."
    if topic == "risk.decision.recorded":
        symbol = underlying_symbol or entity_key or "risk decision"
        decision_kind = _as_text(payload.get("decision_kind")) or "decision"
        return f"{symbol} {decision_kind} is {status or 'recorded'}."
    if topic == "execution.attempt.updated":
        symbol = underlying_symbol or entity_key or "execution"
        trade_intent = _as_text(payload.get("trade_intent")) or "order"
        return f"{trade_intent} execution for {symbol} is {status or 'updated'}."
    if topic == "control.execution.blocked" or topic == "control.execution.skipped":
        return _as_text(payload.get("message")) or topic
    if topic == "control.mode.updated":
        control = (
            payload.get("control")
            if isinstance(payload.get("control"), Mapping)
            else {}
        )
        mode = (
            _as_text(control.get("mode")) or _as_text(payload.get("mode")) or "updated"
        )
        reason_code = _as_text(control.get("reason_code")) or _as_text(
            payload.get("reason_code")
        )
        return f"Control mode set to {mode}{'' if reason_code is None else f' ({reason_code})'}."
    if topic == "job.run.updated":
        job_type = _as_text(payload.get("job_type")) or "job"
        return f"{job_type} run is {status or 'updated'}."
    if topic == "live.collector.degraded":
        return _as_text(payload.get("message")) or "Live collector slot degraded."
    if topic == "broker.sync.updated":
        return f"Broker sync is {status or 'updated'}."
    if topic == "market.quote.captured":
        symbol = (
            _as_text(payload.get("option_symbol"))
            or _as_text(payload.get("symbol"))
            or entity_key
            or "quote"
        )
        return f"{symbol} quote captured."
    if topic == "market.trade.captured":
        symbol = (
            _as_text(payload.get("option_symbol"))
            or _as_text(payload.get("symbol"))
            or entity_key
            or "trade"
        )
        return f"{symbol} trade captured."
    if topic == "uoa.summary.updated":
        overview = (
            payload.get("overview")
            if isinstance(payload.get("overview"), Mapping)
            else {}
        )
        cycle_id = _as_text(payload.get("cycle_id")) or entity_key or "unknown_cycle"
        scoreable_trades = int(overview.get("scoreable_trade_count") or 0)
        scoreable_contracts = int(overview.get("scoreable_contract_count") or 0)
        scoreable_roots = int(overview.get("scoreable_root_count") or 0)
        return (
            f"UOA summary updated for cycle {cycle_id}: "
            f"{scoreable_trades} scoreable trades across "
            f"{scoreable_contracts} contracts / {scoreable_roots} roots."
        )
    if topic == "uoa.decision.updated":
        overview = (
            payload.get("overview")
            if isinstance(payload.get("overview"), Mapping)
            else {}
        )
        cycle_id = _as_text(payload.get("cycle_id")) or entity_key or "unknown_cycle"
        monitor_count = int(
            overview.get("monitor_count", overview.get("watchlist_count")) or 0
        )
        promotable_count = int(
            overview.get("promotable_count", overview.get("board_count")) or 0
        )
        high_count = int(overview.get("high_count") or 0)
        return (
            f"UOA decisions updated for cycle {cycle_id}: "
            f"{monitor_count} monitor / {promotable_count} promotable / {high_count} high."
        )
    return _as_text(payload.get("message")) or topic or "event recorded"


def _normalize_event_item(event: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(event.get("payload") or {})
    return {
        "at": _as_text(event.get("occurred_at")),
        "kind": str(event.get("event_class") or "event"),
        "topic": str(event.get("topic") or ""),
        "source": _as_text(event.get("source")),
        "entity_type": str(event.get("entity_type") or ""),
        "entity_id": str(event.get("entity_key") or ""),
        "correlation_id": _as_text(event.get("correlation_id")),
        "causation_id": _as_text(event.get("causation_id")),
        "summary": _event_summary(event),
        "details": _detail_subset(payload),
    }


def _collapse_market_quote_events(
    events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    grouped: dict[tuple[str | None, str | None, str | None], dict[str, Any]] = {}
    ordered_groups: list[tuple[str | None, str | None, str | None]] = []
    normalized_items: list[dict[str, Any]] = []
    collapsed_count = 0

    for event in events:
        if str(event.get("topic") or "") != MARKET_QUOTE_CAPTURE_TOPIC:
            normalized_items.append(_normalize_event_item(event))
            continue

        payload = dict(event.get("payload") or {})
        group_key = (
            _as_text(event.get("occurred_at")),
            _as_text(payload.get("cycle_id")),
            _as_text(payload.get("session_id")),
        )
        entry = grouped.get(group_key)
        symbol = (
            _as_text(payload.get("option_symbol"))
            or _as_text(payload.get("symbol"))
            or _as_text(event.get("entity_key"))
        )
        if entry is None:
            entry = {
                "at": _as_text(event.get("occurred_at")),
                "kind": str(event.get("event_class") or "market_event"),
                "topic": MARKET_QUOTE_CAPTURE_TOPIC,
                "source": _as_text(event.get("source")),
                "entity_type": "option_quote_batch",
                "entity_id": _as_text(payload.get("cycle_id"))
                or str(event.get("entity_key") or ""),
                "correlation_id": _as_text(event.get("correlation_id")),
                "causation_id": _as_text(event.get("causation_id")),
                "details": {
                    "session_id": _as_text(payload.get("session_id")),
                    "session_date": _as_text(payload.get("session_date")),
                    "label": _as_text(payload.get("label")),
                    "cycle_id": _as_text(payload.get("cycle_id")),
                    "quote_count": 0,
                    "symbols_sample": [],
                },
            }
            grouped[group_key] = entry
            ordered_groups.append(group_key)
        entry["details"]["quote_count"] += 1
        if (
            symbol
            and symbol not in entry["details"]["symbols_sample"]
            and len(entry["details"]["symbols_sample"]) < 5
        ):
            entry["details"]["symbols_sample"].append(symbol)
        collapsed_count += 1

    for group_key in ordered_groups:
        entry = grouped[group_key]
        quote_count = int(entry["details"]["quote_count"])
        cycle_id = _as_text(entry["details"].get("cycle_id")) or "unknown_cycle"
        symbol_preview = list(entry["details"]["symbols_sample"])
        preview_text = ", ".join(symbol_preview)
        if quote_count == 1:
            entry["summary"] = (
                f"Captured 1 option quote for cycle {cycle_id}"
                f"{'' if not preview_text else f' ({preview_text})'}."
            )
        else:
            entry["summary"] = (
                f"Captured {quote_count} option quotes for cycle {cycle_id}"
                f"{'' if not preview_text else f' ({preview_text})'}."
            )
        normalized_items.append(entry)
    return normalized_items, collapsed_count


def _normalize_collector_event(event: Mapping[str, Any]) -> dict[str, Any]:
    details = {
        "label": _as_text(event.get("label")),
        "session_date": _as_text(event.get("session_date")),
        "cycle_id": _as_text(event.get("cycle_id")),
        "symbol": _as_text(event.get("symbol")),
        "event_type": _as_text(event.get("event_type")),
    }
    return {
        "at": _as_text(event.get("generated_at")),
        "kind": "collector_event",
        "topic": f"collector.{_as_text(event.get('event_type')) or 'updated'}",
        "source": "collector",
        "entity_type": "collector_cycle_event",
        "entity_id": str(event.get("event_id") or ""),
        "correlation_id": _as_text(event.get("cycle_id")),
        "causation_id": None,
        "summary": _as_text(event.get("message")) or "Collector event recorded.",
        "details": details,
    }


def _apply_timeline_limit(
    items: list[dict[str, Any]],
    *,
    timeline_limit: int,
) -> tuple[list[dict[str, Any]], bool, int]:
    if timeline_limit <= 0:
        return [], bool(items), len(items)
    if len(items) <= timeline_limit:
        return items, False, 0
    if timeline_limit < 3:
        return items[-timeline_limit:], True, max(0, len(items) - timeline_limit)
    head_count = max(1, timeline_limit // 2)
    tail_count = max(1, timeline_limit - head_count - 1)
    if head_count + tail_count >= timeline_limit:
        tail_count = max(1, timeline_limit - head_count - 1)
    hidden_count = max(0, len(items) - head_count - tail_count)
    if hidden_count <= 0:
        return items[:timeline_limit], True, max(0, len(items) - timeline_limit)
    marker = {
        "at": items[head_count]["at"],
        "kind": "meta",
        "topic": "replay.timeline.truncated",
        "source": "audit_replay",
        "entity_type": "replay",
        "entity_id": "timeline",
        "correlation_id": None,
        "causation_id": None,
        "summary": f"{hidden_count} timeline item(s) omitted from the middle of the replay.",
        "details": {
            "hidden_count": hidden_count,
            "timeline_limit": timeline_limit,
        },
    }
    return items[:head_count] + [marker] + items[-tail_count:], True, hidden_count


def _current_cycle_summary(
    current_cycle: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if current_cycle is None:
        return None
    opportunities = list(current_cycle.get("opportunities") or [])
    selection_counts = dict(current_cycle.get("selection_counts") or {})
    return {
        "cycle_id": _as_text(current_cycle.get("cycle_id")),
        "generated_at": _as_text(current_cycle.get("generated_at")),
        "job_run_id": _as_text(current_cycle.get("job_run_id")),
        "strategy": _as_text(current_cycle.get("strategy")),
        "profile": _as_text(current_cycle.get("profile")),
        "universe_label": _as_text(current_cycle.get("universe_label")),
        "selection_memory": dict(current_cycle.get("selection_memory") or {}),
        "promotable_count": int(selection_counts.get("promotable") or 0),
        "monitor_count": int(selection_counts.get("monitor") or 0),
        "opportunity_count": len(opportunities),
    }


def _summarize_opportunity(opportunity: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "opportunity_id": str(opportunity.get("opportunity_id") or ""),
        "underlying_symbol": _as_text(opportunity.get("underlying_symbol")),
        "selection_state": _as_text(opportunity.get("selection_state")),
        "selection_rank": opportunity.get("selection_rank"),
        "lifecycle_state": _as_text(opportunity.get("lifecycle_state")),
        "confidence": opportunity.get("confidence"),
        "reason_codes": list(opportunity.get("reason_codes") or []),
        "blockers": list(opportunity.get("blockers") or []),
        "consumed_by_execution_attempt_id": _as_text(
            opportunity.get("consumed_by_execution_attempt_id")
        ),
        "updated_at": _as_text(opportunity.get("updated_at")),
    }


def _selected_opportunities_for_explanation(
    *,
    current_cycle: Mapping[str, Any] | None,
    opportunities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if isinstance(current_cycle, Mapping):
        current_rows = [
            dict(row)
            for row in list(current_cycle.get("opportunities") or [])
            if isinstance(row, Mapping)
        ]
        if current_rows:
            ledger_by_candidate_id = {
                int(row["source_candidate_id"]): row
                for row in opportunities
                if row.get("source_candidate_id") not in (None, "")
            }
            merged_rows = []
            for row in current_rows:
                candidate_id = row.get("candidate_id")
                ledger_row = (
                    {}
                    if candidate_id in (None, "")
                    else dict(ledger_by_candidate_id.get(int(candidate_id)) or {})
                )
                merged_row = {
                    **ledger_row,
                    **row,
                }
                if merged_row.get("lifecycle_state") in (None, ""):
                    merged_row["lifecycle_state"] = (
                        "ready"
                        if str(merged_row.get("selection_state") or "") == "promotable"
                        else "candidate"
                    )
                if merged_row.get("confidence") in (None, ""):
                    merged_row["confidence"] = _confidence_from_quality_score(
                        merged_row.get("quality_score")
                    )
                if not merged_row.get("reason_codes") and merged_row.get(
                    "state_reason"
                ):
                    merged_row["reason_codes"] = [str(merged_row["state_reason"])]
                merged_rows.append(merged_row)
            selection_priority = {
                "promotable": 0,
                "monitor": 1,
            }
            return sorted(
                merged_rows,
                key=lambda row: (
                    selection_priority.get(str(row.get("selection_state") or ""), 2),
                    0 if str(row.get("eligibility") or "live") == "live" else 1,
                    int(row.get("selection_rank") or 10_000),
                    str(row.get("underlying_symbol") or ""),
                ),
            )

    return sorted(
        opportunities,
        key=lambda row: (
            0 if _as_text(row.get("consumed_by_execution_attempt_id")) else 1,
            0 if str(row.get("lifecycle_state") or "") == "ready" else 1,
            parse_datetime(_as_text(row.get("updated_at")) or "1970-01-01T00:00:00Z"),
        ),
    )[:5]


def _summarize_risk_decision(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "risk_decision_id": str(decision.get("risk_decision_id") or ""),
        "status": _as_text(decision.get("status")),
        "decision_kind": _as_text(decision.get("decision_kind")),
        "trade_intent": _as_text(decision.get("trade_intent")),
        "underlying_symbol": _as_text(decision.get("underlying_symbol")),
        "note": _as_text(decision.get("note")),
        "reason_codes": list(decision.get("reason_codes") or []),
        "blockers": list(decision.get("blockers") or []),
        "opportunity_id": _as_text(decision.get("opportunity_id")),
        "execution_attempt_id": _as_text(decision.get("execution_attempt_id")),
        "decided_at": _as_text(decision.get("decided_at")),
    }


def _summarize_execution(attempt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "execution_attempt_id": str(attempt.get("execution_attempt_id") or ""),
        "trade_intent": _as_text(attempt.get("trade_intent")),
        "status": _as_text(attempt.get("status")),
        "underlying_symbol": _as_text(attempt.get("underlying_symbol")),
        "strategy": _as_text(attempt.get("strategy")),
        "opportunity_id": _as_text(attempt.get("opportunity_id")),
        "risk_decision_id": _as_text(attempt.get("risk_decision_id")),
        "session_position_id": _as_text(attempt.get("session_position_id")),
        "broker_order_id": _as_text(attempt.get("broker_order_id")),
        "requested_at": _as_text(attempt.get("requested_at")),
        "submitted_at": _as_text(attempt.get("submitted_at")),
        "completed_at": _as_text(attempt.get("completed_at")),
        "error_text": _as_text(attempt.get("error_text")),
        "order_count": len(list(attempt.get("orders") or [])),
        "fill_count": len(list(attempt.get("fills") or [])),
    }


def _summarize_slot_run(run: Mapping[str, Any]) -> dict[str, Any]:
    quote_capture = (
        run.get("quote_capture")
        if isinstance(run.get("quote_capture"), Mapping)
        else {}
    )
    trade_capture = (
        run.get("trade_capture")
        if isinstance(run.get("trade_capture"), Mapping)
        else {}
    )
    stream_quote_events_saved = quote_capture.get(
        "stream_quote_events_saved",
        quote_capture.get("websocket_quote_events_saved"),
    )
    stream_trade_events_saved = trade_capture.get(
        "stream_trade_events_saved",
        trade_capture.get("websocket_trade_events_saved"),
    )
    return {
        "job_run_id": _as_text(run.get("job_run_id")),
        "slot_at": _as_text(run.get("slot_at")),
        "scheduled_for": _as_text(run.get("scheduled_for")),
        "status": _as_text(run.get("status")),
        "capture_status": _as_text(run.get("capture_status")),
        "started_at": _as_text(run.get("started_at")),
        "finished_at": _as_text(run.get("finished_at")),
        "quote_capture": {
            "capture_status": _as_text(quote_capture.get("capture_status")),
            "stream_quote_events_saved": stream_quote_events_saved,
            "websocket_quote_events_saved": stream_quote_events_saved,
            "baseline_quote_events_saved": quote_capture.get(
                "baseline_quote_events_saved"
            ),
            "recovery_quote_events_saved": quote_capture.get(
                "recovery_quote_events_saved"
            ),
        },
        "trade_capture": {
            "capture_status": _as_text(trade_capture.get("capture_status")),
            "total_trade_events_saved": trade_capture.get("total_trade_events_saved"),
            "stream_trade_events_saved": stream_trade_events_saved,
            "websocket_trade_events_saved": stream_trade_events_saved,
        },
    }


def _summarize_alert(alert: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "alert_id": _as_text(alert.get("alert_id")),
        "created_at": _as_text(alert.get("created_at")),
        "label": _as_text(alert.get("label")),
        "symbol": _as_text(alert.get("symbol")),
        "alert_type": _as_text(alert.get("alert_type")),
        "delivery_target": _as_text(alert.get("delivery_target")),
        "status": _as_text(alert.get("status")),
    }


def _promotable_monitor_pnl_spread(summary: Mapping[str, Any] | None) -> float | None:
    if not isinstance(summary, Mapping):
        return None
    outcomes = summary.get("outcomes")
    if not isinstance(outcomes, Mapping):
        return None
    averages = outcomes.get(
        "average_estimated_pnl_by_selection_state",
        outcomes.get("average_estimated_pnl_by_bucket"),
    )
    return (
        None
        if not isinstance(averages, Mapping)
        else promotable_monitor_pnl_spread(averages)
    )


def _summarize_post_market(analysis: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(analysis, Mapping):
        return {
            "overall_verdict": None,
            "promotable_monitor_pnl_spread": None,
            "recommendations": [],
        }
    diagnostics = (
        analysis.get("diagnostics")
        if isinstance(analysis.get("diagnostics"), Mapping)
        else {}
    )
    return {
        "overall_verdict": _as_text(diagnostics.get("overall_verdict")),
        "promotable_monitor_pnl_spread": _promotable_monitor_pnl_spread(analysis),
        "recommendations": list(analysis.get("recommendations") or []),
    }


def _resolve_audit_scope(
    *,
    session_id: str,
    storage: Any,
) -> dict[str, str]:
    resolved = parse_live_session_id(session_id)
    if resolved is not None:
        return {
            "session_id": session_id,
            "label": resolved["label"],
            "session_date": resolved["session_date"],
            "pipeline_id": build_pipeline_id(resolved["label"]),
        }

    latest_cycle = storage.collector.get_latest_session_cycle(session_id)
    if latest_cycle is None:
        raise ValueError(f"Unknown session_id: {session_id}")
    label = str(latest_cycle["label"])
    session_date = str(latest_cycle["session_date"])
    return {
        "session_id": session_id,
        "label": label,
        "session_date": session_date,
        "pipeline_id": build_pipeline_id(label),
    }


@with_storage()
def build_audit_replay(
    *,
    db_target: str,
    session_id: str,
    timeline_limit: int = DEFAULT_TIMELINE_LIMIT,
    event_scan_limit: int = DEFAULT_EVENT_SCAN_LIMIT,
    storage: Any | None = None,
) -> dict[str, Any]:
    scope = _resolve_audit_scope(session_id=session_id, storage=storage)
    session = get_pipeline_detail(
        db_target=db_target,
        pipeline_id=scope["pipeline_id"],
        market_date=scope["session_date"],
        profit_target=DEFAULT_ANALYSIS_PROFIT_TARGET,
        stop_multiple=DEFAULT_ANALYSIS_STOP_MULTIPLE,
        storage=storage,
    )
    label = str(scope["label"])
    session_date = str(scope["session_date"])
    analysis = (
        session.get("analysis") if isinstance(session.get("analysis"), Mapping) else {}
    )
    event_store = storage.events
    signal_store = storage.signals

    raw_events: list[dict[str, Any]] = []
    if event_store.schema_ready():
        raw_events = [
            dict(row)
            for row in event_store.list_events(
                label=label,
                session_date=session_date,
                limit=event_scan_limit,
            )
        ]

    signal_states: list[dict[str, Any]] = []
    signal_transitions: list[dict[str, Any]] = []
    opportunities: list[dict[str, Any]] = []
    if signal_store.schema_ready():
        signal_states = [
            dict(row)
            for row in signal_store.list_signal_states(
                label=label,
                session_date=session_date,
                limit=200,
            )
        ]
        signal_transitions = [
            dict(row)
            for row in signal_store.list_signal_transitions(
                label=label,
                session_date=session_date,
                limit=500,
            )
        ]
        opportunities = [
            dict(row)
            for row in signal_store.list_opportunities(
                label=label,
                session_date=session_date,
                limit=200,
            )
        ]

    timeline_items, collapsed_quote_count = _collapse_market_quote_events(raw_events)
    timeline_items.extend(
        _normalize_collector_event(dict(event)) for event in session.get("events") or []
    )
    timeline_items.sort(key=_timeline_sort_key)
    limited_timeline, timeline_truncated, hidden_count = _apply_timeline_limit(
        timeline_items,
        timeline_limit=timeline_limit,
    )

    control_items = [
        item
        for item in timeline_items
        if str(item.get("topic") or "").startswith("control.")
    ]
    prioritized_opportunities = _selected_opportunities_for_explanation(
        current_cycle=session.get("current_cycle"),
        opportunities=opportunities,
    )

    state_summary = {
        "status": session.get("status"),
        "updated_at": session.get("updated_at"),
        "control_snapshot": dict(session.get("control") or {}),
        "current_cycle": _current_cycle_summary(session.get("current_cycle")),
        "counts": {
            "timeline_items": len(timeline_items),
            "events_scanned": len(raw_events),
            "collector_events": len(list(session.get("events") or [])),
            "signal_states": len(signal_states),
            "signal_transitions": len(signal_transitions),
            "opportunities": len(opportunities),
            "risk_decisions": len(list(session.get("risk_decisions") or [])),
            "executions": len(list(session.get("executions") or [])),
            "alerts": len(list(session.get("alerts") or [])),
        },
        "signals": signal_states,
        "opportunities": opportunities,
        "risk_decisions": list(session.get("risk_decisions") or []),
        "executions": list(session.get("executions") or []),
        "portfolio": dict(session.get("portfolio") or {}),
    }
    slot_runs = [
        _summarize_slot_run(dict(row))
        for row in list(session.get("slot_runs") or [])[:10]
        if isinstance(row, Mapping)
    ]
    alerts = [
        _summarize_alert(dict(row))
        for row in list(session.get("alerts") or [])[:25]
        if isinstance(row, Mapping)
    ]

    return {
        "session": {
            "session_id": scope["session_id"],
            "label": label,
            "session_date": session_date,
            "status": session.get("status"),
            "updated_at": session.get("updated_at"),
            "risk_status": session.get("risk_status"),
            "risk_note": session.get("risk_note"),
            "reconciliation_status": session.get("reconciliation_status"),
            "reconciliation_note": session.get("reconciliation_note"),
        },
        "timeline": limited_timeline,
        "timeline_stats": {
            "timeline_limit": timeline_limit,
            "timeline_item_count": len(timeline_items),
            "returned_timeline_item_count": len(limited_timeline),
            "timeline_truncated": timeline_truncated,
            "omitted_timeline_item_count": hidden_count,
            "event_scan_limit": event_scan_limit,
            "event_scan_limit_hit": len(raw_events) >= event_scan_limit,
            "collapsed_market_quote_event_count": collapsed_quote_count,
            "timeline_window": {
                "started_at": None
                if not timeline_items
                else timeline_items[0].get("at"),
                "ended_at": None
                if not timeline_items
                else timeline_items[-1].get("at"),
            },
        },
        "state_summary": state_summary,
        "slot_runs": slot_runs,
        "alerts": alerts,
        "post_market": _summarize_post_market(analysis),
        "explanations": {
            "selected_opportunities": [
                _summarize_opportunity(row) for row in prioritized_opportunities[:10]
            ],
            "risk_decisions": [
                _summarize_risk_decision(dict(row))
                for row in list(session.get("risk_decisions") or [])[:10]
            ],
            "execution_outcomes": [
                _summarize_execution(dict(row))
                for row in list(session.get("executions") or [])[:10]
            ],
            "control_actions": [
                {
                    "at": item.get("at"),
                    "topic": item.get("topic"),
                    "summary": item.get("summary"),
                    "details": dict(item.get("details") or {}),
                }
                for item in control_items[-10:]
            ],
        },
    }
