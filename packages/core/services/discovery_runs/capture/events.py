from __future__ import annotations

from typing import Any

from core.events.bus import build_global_event
from core.storage.event_repository import EventRepository


def _source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        source = str(row.get("source") or "unknown")
        counts[source] = counts.get(source, 0) + 1
    return dict(sorted(counts.items()))


def record_market_capture_summary_event(
    *,
    event_store: EventRepository,
    cycle_id: str,
    generated_at: str,
    label: str,
    profile: str,
    session_date: str,
    session_id: str | None,
    job_run_id: str | None,
    quote_capture: dict[str, Any],
    trade_capture: dict[str, Any],
    expected_quote_symbols: list[str],
    expected_trade_symbols: list[str],
    quote_records: list[dict[str, Any]],
    trade_records: list[dict[str, Any]],
    stream_quote_error: str | None,
    stream_trade_error: str | None,
) -> int:
    if not event_store.schema_ready():
        return 0
    payload = {
        "cycle_id": cycle_id,
        "label": label,
        "profile": profile,
        "quote_capture": dict(quote_capture),
        "trade_capture": dict(trade_capture),
        "expected_quote_symbol_count": len(expected_quote_symbols),
        "expected_trade_symbol_count": len(expected_trade_symbols),
        "expected_quote_symbols_sample": list(expected_quote_symbols[:10]),
        "expected_trade_symbols_sample": list(expected_trade_symbols[:10]),
        "quote_source_counts": _source_counts(quote_records),
        "trade_source_counts": _source_counts(trade_records),
        **({} if stream_quote_error is None else {"stream_quote_error": stream_quote_error}),
        **({} if stream_trade_error is None else {"stream_trade_error": stream_trade_error}),
        **({} if session_id is None else {"session_id": session_id}),
        **({} if job_run_id is None else {"job_run_id": job_run_id}),
    }
    envelope = build_global_event(
        topic="market.capture.summary",
        event_class="market_event",
        event_type="market_capture.summary",
        entity_type="market_capture_cycle",
        entity_id=cycle_id,
        payload=payload,
        timestamp=generated_at,
        source="discovery_capture",
        session_date=session_date,
        market_session="regular",
        correlation_id=cycle_id,
        causation_id=job_run_id,
    )
    event_store.create_events([envelope])
    return 1


def record_uoa_summary_event(
    *,
    event_store: EventRepository,
    cycle_id: str,
    generated_at: str,
    label: str,
    profile: str,
    session_date: str,
    session_id: str | None,
    job_run_id: str | None,
    summary: dict[str, Any],
) -> int:
    if not event_store.schema_ready():
        return 0
    overview = dict(summary.get("overview") or {})
    if (
        not overview
        and not summary.get("top_contracts")
        and not summary.get("top_roots")
    ):
        return 0
    payload = {
        "cycle_id": cycle_id,
        "label": label,
        "profile": profile,
        "overview": overview,
        "top_contracts": [
            dict(item) for item in (summary.get("top_contracts") or [])[:3]
        ],
        "top_roots": [dict(item) for item in (summary.get("top_roots") or [])[:3]],
        **({} if session_id is None else {"session_id": session_id}),
        **({} if job_run_id is None else {"job_run_id": job_run_id}),
    }
    envelope = build_global_event(
        topic="uoa.summary.updated",
        event_class="market_event",
        event_type="uoa.trade_summary.updated",
        entity_type="uoa_cycle",
        entity_id=cycle_id,
        payload=payload,
        timestamp=str(overview.get("last_trade_at") or generated_at),
        source="uoa_scanner",
        session_date=session_date,
        market_session="regular",
        correlation_id=cycle_id,
        causation_id=job_run_id,
    )
    event_store.create_events([envelope])
    return 1


def record_uoa_decision_event(
    *,
    event_store: EventRepository,
    cycle_id: str,
    generated_at: str,
    label: str,
    profile: str,
    session_date: str,
    session_id: str | None,
    job_run_id: str | None,
    decisions: dict[str, Any],
) -> int:
    if not event_store.schema_ready():
        return 0
    overview = dict(decisions.get("overview") or {})
    if not overview:
        return 0
    payload = {
        "cycle_id": cycle_id,
        "label": label,
        "profile": profile,
        "overview": overview,
        "top_monitor_roots": [
            dict(item) for item in (decisions.get("top_monitor_roots") or [])[:3]
        ],
        "top_promotable_roots": [
            dict(item) for item in (decisions.get("top_promotable_roots") or [])[:3]
        ],
        "top_high_roots": [
            dict(item) for item in (decisions.get("top_high_roots") or [])[:3]
        ],
        **({} if session_id is None else {"session_id": session_id}),
        **({} if job_run_id is None else {"job_run_id": job_run_id}),
    }
    envelope = build_global_event(
        topic="uoa.decision.updated",
        event_class="market_event",
        event_type="uoa.root_decision.updated",
        entity_type="uoa_cycle",
        entity_id=cycle_id,
        payload=payload,
        timestamp=generated_at,
        source="uoa_scanner",
        session_date=session_date,
        market_session="regular",
        correlation_id=cycle_id,
        causation_id=job_run_id,
    )
    event_store.create_events([envelope])
    return 1


__all__ = [
    "record_market_capture_summary_event",
    "record_uoa_decision_event",
    "record_uoa_summary_event",
]
