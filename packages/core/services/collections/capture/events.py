from __future__ import annotations

from typing import Any

from core.events.bus import build_global_event
from core.storage.event_repository import EventRepository


def record_quote_market_events(
    *,
    event_store: EventRepository,
    cycle_id: str,
    label: str,
    profile: str,
    session_date: str,
    session_id: str | None,
    job_run_id: str | None,
    quotes: list[dict[str, Any]],
) -> int:
    if not quotes or not event_store.schema_ready():
        return 0
    envelopes = [
        build_global_event(
            topic="market.quote.captured",
            event_class="market_event",
            event_type="option_quote.captured",
            entity_type="option_quote",
            entity_id=str(quote["option_symbol"]),
            payload={
                **dict(quote),
                "cycle_id": cycle_id,
                "label": label,
                "profile": profile,
                **({} if session_id is None else {"session_id": session_id}),
                **({} if job_run_id is None else {"job_run_id": job_run_id}),
            },
            timestamp=str(quote["captured_at"]),
            source=str(quote.get("source") or "option_quote_capture"),
            session_date=session_date,
            market_session="regular",
            correlation_id=cycle_id,
            causation_id=job_run_id,
        )
        for quote in quotes
        if quote.get("option_symbol") and quote.get("captured_at")
    ]
    if not envelopes:
        return 0
    event_store.create_events(envelopes)
    return len(envelopes)


def record_trade_market_events(
    *,
    event_store: EventRepository,
    cycle_id: str,
    label: str,
    profile: str,
    session_date: str,
    session_id: str | None,
    job_run_id: str | None,
    trades: list[dict[str, Any]],
) -> int:
    if not trades or not event_store.schema_ready():
        return 0
    envelopes = [
        build_global_event(
            topic="market.trade.captured",
            event_class="market_event",
            event_type="option_trade.captured",
            entity_type="option_trade",
            entity_id=str(trade["option_symbol"]),
            payload={
                **dict(trade),
                "cycle_id": cycle_id,
                "label": label,
                "profile": profile,
                **({} if session_id is None else {"session_id": session_id}),
                **({} if job_run_id is None else {"job_run_id": job_run_id}),
            },
            timestamp=str(trade["captured_at"]),
            source=str(trade.get("source") or "option_trade_capture"),
            session_date=session_date,
            market_session="regular",
            correlation_id=cycle_id,
            causation_id=job_run_id,
        )
        for trade in trades
        if trade.get("option_symbol") and trade.get("captured_at")
    ]
    if not envelopes:
        return 0
    event_store.create_events(envelopes)
    return len(envelopes)


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
    "record_quote_market_events",
    "record_trade_market_events",
    "record_uoa_decision_event",
    "record_uoa_summary_event",
]
