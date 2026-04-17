from __future__ import annotations

import argparse
import time as time_module
from datetime import UTC, datetime, timedelta
from typing import Any

from core.events.bus import build_global_event
from core.integrations.alpaca.client import AlpacaClient
from core.services.live_collector_health import (
    build_quote_capture_summary,
    build_trade_capture_summary,
)
from core.services.live_recovery import refresh_live_session_capture_targets
from core.services.option_quote_records import (
    build_quote_records,
    build_quote_symbol_metadata,
)
from core.services.option_structures import candidate_legs, legs_identity_key
from core.services.option_trade_records import build_trade_symbol_metadata
from core.services.runtime_identity import build_live_run_scope_id
from core.services.uoa_quote_summary import build_uoa_quote_summary
from core.services.uoa_root_decisions import build_uoa_root_decisions
from core.services.uoa_trade_baselines import build_uoa_trade_baselines
from core.services.uoa_trade_summary import build_uoa_trade_summary
from core.storage.event_repository import EventRepository
from core.storage.run_history_repository import RunHistoryRepository

from .models import LiveCaptureSnapshot, LiveTickContext

MARKET_RECORDER_SOURCE = "market_recorder"
MARKET_RECORDER_POLL_SECONDS = 25.0
MARKET_RECORDER_WAIT_GRACE_SECONDS = 10.0
MARKET_RECORDER_QUERY_POLL_SECONDS = 2.0


def _capture_candidate_identity(candidate: dict[str, Any]) -> tuple[str, str, str]:
    return (
        legs_identity_key(
            strategy=candidate.get("strategy"),
            legs=candidate_legs(candidate),
        ),
        str(candidate.get("underlying_symbol") or ""),
        str(candidate.get("expiration_date") or ""),
    )


def _opportunity_is_live_capture_eligible(opportunity: dict[str, Any]) -> bool:
    eligibility = (
        str(
            opportunity.get("eligibility_state")
            or opportunity.get("eligibility")
            or "live"
        )
        .strip()
        .lower()
    )
    return eligibility == "live"


def build_capture_candidates(
    *,
    promotable_candidates: list[dict[str, Any]],
    monitor_candidates: list[dict[str, Any]],
    opportunities: list[dict[str, Any]],
    monitor_limit: int,
) -> list[dict[str, Any]]:
    capture_candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add_candidate(candidate: dict[str, Any]) -> None:
        identity = _capture_candidate_identity(candidate)
        if not all(identity) or identity in seen:
            return
        seen.add(identity)
        capture_candidates.append(dict(candidate))

    for candidate in promotable_candidates:
        add_candidate(candidate)
    for candidate in monitor_candidates[: max(monitor_limit, 0)]:
        add_candidate(candidate)

    target_count = max(
        len(promotable_candidates) + max(monitor_limit, 0),
        max(monitor_limit, 0),
    )
    if len(capture_candidates) >= target_count:
        return capture_candidates

    ranked_opportunities = sorted(
        (
            dict(item)
            for item in opportunities
            if isinstance(item, dict)
            and str(item.get("selection_state") or "") in {"promotable", "monitor"}
            and _opportunity_is_live_capture_eligible(item)
        ),
        key=lambda item: (
            int(item.get("selection_rank") or 999_999),
            str(item.get("selection_state") or ""),
        ),
    )
    for row in ranked_opportunities:
        candidate_payload = row.get("candidate")
        if isinstance(candidate_payload, dict) and candidate_payload:
            add_candidate(dict(candidate_payload))
        else:
            add_candidate(row)
        if len(capture_candidates) >= target_count:
            break

    return capture_candidates


def build_preselection_capture_candidates(
    symbol_candidates: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    capture_candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for rows in symbol_candidates.values():
        for candidate in rows:
            if not isinstance(candidate, dict):
                continue
            identity = _capture_candidate_identity(candidate)
            if not all(identity) or identity in seen:
                continue
            seen.add(identity)
            capture_candidates.append(dict(candidate))
    capture_candidates.sort(
        key=lambda candidate: float(candidate.get("quality_score") or 0.0),
        reverse=True,
    )
    return capture_candidates


def capture_live_option_market_state(
    *,
    args: argparse.Namespace,
    scanner_args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
    event_store: EventRepository,
    recovery_store: Any | None,
    label: str,
    cycle_id: str,
    generated_at: str,
    session_date: str,
    tick_context: LiveTickContext | None,
    capture_candidates: list[dict[str, Any]],
) -> LiveCaptureSnapshot:
    quote_event_count = 0
    baseline_quote_event_count = 0
    stream_quote_event_count = 0
    recovery_quote_event_count = 0
    trade_event_count = 0
    stream_trade_event_count = 0
    latest_quote_records: list[dict[str, Any]] = []
    stream_quote_records: list[dict[str, Any]] = []
    recovery_quote_records: list[dict[str, Any]] = []
    stream_trade_records: list[dict[str, Any]] = []
    stream_quote_error: str | None = None
    stream_trade_error: str | None = None
    contract_metadata_by_symbol = build_quote_symbol_metadata(capture_candidates)
    expected_quote_symbols = list(contract_metadata_by_symbol.keys())
    expected_trade_symbols = list(
        build_trade_symbol_metadata(capture_candidates).keys()
    )
    expected_uoa_roots = sorted(
        {
            str(candidate.get("underlying_symbol") or "").strip()
            for candidate in capture_candidates
            if str(candidate.get("underlying_symbol") or "").strip()
        }
    )
    if capture_candidates:
        if recovery_store is not None:
            try:
                refresh_live_session_capture_targets(
                    recovery_store=recovery_store,
                    session_id=(
                        tick_context.session_id
                        if tick_context is not None
                        else build_live_run_scope_id(label, session_date)
                    ),
                    session_date=session_date,
                    label=label,
                    profile=args.profile,
                    promotable_candidates=[],
                    monitor_candidates=[],
                    capture_candidates=capture_candidates,
                    feed=scanner_args.feed,
                    data_base_url=getattr(scanner_args, "data_base_url", None),
                    session_end_offset_minutes=int(
                        getattr(args, "session_end_offset_minutes", 0)
                    ),
                )
            except Exception as exc:
                print(f"Preselection capture target refresh unavailable: {exc}")
        try:
            latest_quote_records = collect_latest_quote_records(
                client=client,
                candidates=capture_candidates,
                feed=scanner_args.feed,
            )
            baseline_quote_event_count = history_store.save_option_quote_events(
                cycle_id=cycle_id,
                label=label,
                profile=args.profile,
                quotes=latest_quote_records,
            )
            quote_event_count += baseline_quote_event_count
            try:
                _record_quote_market_events(
                    event_store=event_store,
                    cycle_id=cycle_id,
                    label=label,
                    profile=args.profile,
                    session_date=session_date,
                    session_id=None
                    if tick_context is None
                    else tick_context.session_id,
                    job_run_id=None
                    if tick_context is None
                    else tick_context.job_run_id,
                    quotes=latest_quote_records,
                )
            except Exception as exc:
                print(f"Live latest quote event normalization unavailable: {exc}")
        except Exception as exc:
            print(f"Live latest quote capture unavailable: {exc}")
        trade_storage_ready = history_store.schema_has_tables("option_trade_events")
        if args.trade_capture_seconds > 0 and not trade_storage_ready:
            print(
                "Option trade capture unavailable: option_trade_events table is missing."
            )
        stream_quote_duration_seconds = float(max(args.quote_capture_seconds, 0))
        stream_trade_duration_seconds = (
            float(args.trade_capture_seconds) if trade_storage_ready else 0.0
        )
        if stream_quote_duration_seconds > 0 or stream_trade_duration_seconds > 0:
            if recovery_store is None or not recovery_store.schema_ready():
                stream_quote_error = (
                    "Market recorder capture unavailable: recovery schema is not ready."
                )
                stream_trade_error = stream_quote_error
                print(
                    f"Live stream market-data capture unavailable: {stream_quote_error}"
                )
            else:
                capture_response = collect_recorded_market_data_records(
                    history_store=history_store,
                    label=label,
                    profile=args.profile,
                    expected_quote_symbols=expected_quote_symbols,
                    expected_trade_symbols=expected_trade_symbols,
                    captured_from=generated_at,
                    wait_timeout_seconds=(
                        MARKET_RECORDER_POLL_SECONDS
                        + max(
                            float(stream_quote_duration_seconds),
                            float(stream_trade_duration_seconds),
                        )
                        + MARKET_RECORDER_WAIT_GRACE_SECONDS
                    ),
                )
                stream_quote_records = [
                    dict(item)
                    for item in (capture_response.get("quotes") or [])
                    if isinstance(item, dict)
                ]
                stream_trade_records = [
                    dict(item)
                    for item in (capture_response.get("trades") or [])
                    if isinstance(item, dict)
                ]
                stream_quote_error = (
                    None
                    if capture_response.get("quote_error") in (None, "")
                    else str(capture_response.get("quote_error"))
                )
                stream_trade_error = (
                    None
                    if capture_response.get("trade_error") in (None, "")
                    else str(capture_response.get("trade_error"))
                )
                if stream_quote_error:
                    print(
                        f"Live stream quote capture unavailable: {stream_quote_error}"
                    )
                if stream_trade_error:
                    print(
                        f"Live stream trade capture unavailable: {stream_trade_error}"
                    )
                if stream_quote_records:
                    stream_quote_event_count = len(stream_quote_records)
                    quote_event_count += stream_quote_event_count
                    try:
                        _record_quote_market_events(
                            event_store=event_store,
                            cycle_id=cycle_id,
                            label=label,
                            profile=args.profile,
                            session_date=session_date,
                            session_id=None
                            if tick_context is None
                            else tick_context.session_id,
                            job_run_id=None
                            if tick_context is None
                            else tick_context.job_run_id,
                            quotes=stream_quote_records,
                        )
                    except Exception as exc:
                        print(
                            f"Live stream quote event normalization unavailable: {exc}"
                        )
                if stream_trade_records:
                    stream_trade_event_count = len(stream_trade_records)
                    trade_event_count += stream_trade_event_count
                    try:
                        _record_trade_market_events(
                            event_store=event_store,
                            cycle_id=cycle_id,
                            label=label,
                            profile=args.profile,
                            session_date=session_date,
                            session_id=None
                            if tick_context is None
                            else tick_context.session_id,
                            job_run_id=None
                            if tick_context is None
                            else tick_context.job_run_id,
                            trades=stream_trade_records,
                        )
                    except Exception as exc:
                        print(
                            f"Live stream trade event normalization unavailable: {exc}"
                        )
        if quote_event_count == 0:
            try:
                recovery_quote_records = collect_latest_quote_records(
                    client=client,
                    candidates=capture_candidates,
                    feed=scanner_args.feed,
                    attempts=3,
                    retry_delay_seconds=2.0,
                    source="alpaca_latest_quote_recovery",
                )
                recovered_quote_event_count = history_store.save_option_quote_events(
                    cycle_id=cycle_id,
                    label=label,
                    profile=args.profile,
                    quotes=recovery_quote_records,
                )
                quote_event_count += recovered_quote_event_count
                try:
                    _record_quote_market_events(
                        event_store=event_store,
                        cycle_id=cycle_id,
                        label=label,
                        profile=args.profile,
                        session_date=session_date,
                        session_id=None
                        if tick_context is None
                        else tick_context.session_id,
                        job_run_id=None
                        if tick_context is None
                        else tick_context.job_run_id,
                        quotes=recovery_quote_records,
                    )
                except Exception as exc:
                    print(f"Live recovery quote event normalization unavailable: {exc}")
                recovery_quote_event_count = recovered_quote_event_count
            except Exception as exc:
                print(f"Live quote recovery unavailable: {exc}")
    quote_capture = build_quote_capture_summary(
        expected_quote_symbols=expected_quote_symbols,
        total_quote_events_saved=quote_event_count,
        baseline_quote_events_saved=baseline_quote_event_count,
        stream_quote_events_saved=stream_quote_event_count,
        recovery_quote_events_saved=recovery_quote_event_count,
    )
    trade_capture = build_trade_capture_summary(
        expected_trade_symbols=expected_trade_symbols,
        total_trade_events_saved=trade_event_count,
        stream_trade_events_saved=stream_trade_event_count,
    )
    reactive_quote_records = [
        *latest_quote_records,
        *stream_quote_records,
        *recovery_quote_records,
    ]
    uoa_summary = build_uoa_trade_summary(
        as_of=generated_at,
        expected_trade_symbols=expected_trade_symbols,
        contract_metadata_by_symbol=contract_metadata_by_symbol,
        trades=stream_trade_records,
        top_contracts_limit=max(len(expected_trade_symbols), 10),
        top_roots_limit=max(len(expected_uoa_roots), 10),
    )
    uoa_quote_summary = build_uoa_quote_summary(
        as_of=generated_at,
        expected_quote_symbols=expected_quote_symbols,
        contract_metadata_by_symbol=contract_metadata_by_symbol,
        quotes=reactive_quote_records,
    )
    uoa_baselines = build_uoa_trade_baselines(
        history_store=history_store,
        label=label,
        session_date=session_date,
        as_of=generated_at,
        underlyings=expected_uoa_roots,
    )
    uoa_decisions = build_uoa_root_decisions(
        uoa_summary=uoa_summary,
        baselines_by_symbol=uoa_baselines,
        quote_summary=uoa_quote_summary,
        capture_window_seconds=float(max(args.trade_capture_seconds, 1)),
    )
    if expected_trade_symbols or stream_trade_records:
        try:
            _record_uoa_summary_event(
                event_store=event_store,
                cycle_id=cycle_id,
                generated_at=generated_at,
                label=label,
                profile=args.profile,
                session_date=session_date,
                session_id=None if tick_context is None else tick_context.session_id,
                job_run_id=None if tick_context is None else tick_context.job_run_id,
                summary=uoa_summary,
            )
        except Exception as exc:
            print(f"UOA summary event publish unavailable: {exc}")
        try:
            _record_uoa_decision_event(
                event_store=event_store,
                cycle_id=cycle_id,
                generated_at=generated_at,
                label=label,
                profile=args.profile,
                session_date=session_date,
                session_id=None if tick_context is None else tick_context.session_id,
                job_run_id=None if tick_context is None else tick_context.job_run_id,
                decisions=uoa_decisions,
            )
        except Exception as exc:
            print(f"UOA decision event publish unavailable: {exc}")
    return LiveCaptureSnapshot(
        candidates=list(capture_candidates),
        contract_metadata_by_symbol=contract_metadata_by_symbol,
        expected_quote_symbols=expected_quote_symbols,
        expected_trade_symbols=expected_trade_symbols,
        expected_uoa_roots=expected_uoa_roots,
        quote_event_count=quote_event_count,
        baseline_quote_event_count=baseline_quote_event_count,
        stream_quote_event_count=stream_quote_event_count,
        recovery_quote_event_count=recovery_quote_event_count,
        trade_event_count=trade_event_count,
        stream_trade_event_count=stream_trade_event_count,
        latest_quote_records=latest_quote_records,
        stream_quote_records=stream_quote_records,
        recovery_quote_records=recovery_quote_records,
        stream_trade_records=stream_trade_records,
        reactive_quote_records=reactive_quote_records,
        quote_capture=quote_capture,
        trade_capture=trade_capture,
        uoa_summary=uoa_summary,
        uoa_quote_summary=uoa_quote_summary,
        uoa_decisions=uoa_decisions,
        stream_quote_error=stream_quote_error,
        stream_trade_error=stream_trade_error,
    )


def collect_latest_quote_records(
    *,
    client: AlpacaClient,
    candidates: list[dict[str, Any]],
    feed: str,
    attempts: int = 1,
    retry_delay_seconds: float = 0.0,
    source: str = "alpaca_latest_quote",
) -> list[dict[str, Any]]:
    if not candidates:
        return []

    symbol_metadata = build_quote_symbol_metadata(candidates)
    stream_symbols = list(symbol_metadata.keys())
    max_attempts = max(int(attempts), 1)
    for attempt in range(max_attempts):
        latest_quotes = client.get_latest_option_quotes(stream_symbols, feed=feed)
        if latest_quotes:
            latest_captured_at = (
                datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
            )
            return build_quote_records(
                captured_at=latest_captured_at,
                symbol_metadata=symbol_metadata,
                quotes=list(latest_quotes.values()),
                source=source,
            )
        if attempt < max_attempts - 1 and retry_delay_seconds > 0:
            time_module.sleep(retry_delay_seconds)
    return []


def collect_recorded_market_data_records(
    *,
    history_store: RunHistoryRepository,
    label: str,
    profile: str,
    expected_quote_symbols: list[str],
    expected_trade_symbols: list[str],
    captured_from: str,
    wait_timeout_seconds: float,
    poll_interval_seconds: float = MARKET_RECORDER_QUERY_POLL_SECONDS,
) -> dict[str, Any]:
    normalized_quote_symbols = sorted(
        {
            str(symbol or "").strip()
            for symbol in expected_quote_symbols
            if str(symbol or "").strip()
        }
    )
    normalized_trade_symbols = sorted(
        {
            str(symbol or "").strip()
            for symbol in expected_trade_symbols
            if str(symbol or "").strip()
        }
    )
    if not normalized_quote_symbols and not normalized_trade_symbols:
        return {
            "quotes": [],
            "trades": [],
            "quote_error": None,
            "trade_error": None,
            "quote_complete": True,
        }

    deadline = datetime.now(UTC) + timedelta(
        seconds=max(float(wait_timeout_seconds), 0.0)
    )
    quote_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    missing_quote_symbols = list(normalized_quote_symbols)

    while True:
        captured_to = (
            datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        )
        if normalized_quote_symbols:
            quote_records = history_store.list_option_quote_events_window(
                option_symbols=normalized_quote_symbols,
                captured_from=captured_from,
                captured_to=captured_to,
                label=label,
                profile=profile,
                sources=MARKET_RECORDER_SOURCE,
            )
        if normalized_trade_symbols:
            trade_records = history_store.list_option_trade_events_window(
                option_symbols=normalized_trade_symbols,
                captured_from=captured_from,
                captured_to=captured_to,
                label=label,
                profile=profile,
                sources=MARKET_RECORDER_SOURCE,
            )
        covered_quote_symbols = {
            str(row.get("option_symbol") or "").strip()
            for row in quote_records
            if str(row.get("option_symbol") or "").strip()
        }
        missing_quote_symbols = [
            symbol
            for symbol in normalized_quote_symbols
            if symbol not in covered_quote_symbols
        ]
        if not missing_quote_symbols or datetime.now(UTC) >= deadline:
            break
        time_module.sleep(max(float(poll_interval_seconds), 0.2))

    quote_complete = not missing_quote_symbols
    quote_error = None
    if missing_quote_symbols:
        quote_error = (
            "Market recorder did not cover "
            f"{len(missing_quote_symbols)}/{len(normalized_quote_symbols)} expected quote symbols before timeout."
        )
    return {
        "quotes": [dict(row) for row in quote_records],
        "trades": [dict(row) for row in trade_records],
        "quote_error": quote_error,
        "trade_error": None,
        "quote_complete": quote_complete,
    }


def _record_quote_market_events(
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


def _record_trade_market_events(
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


def _record_uoa_summary_event(
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


def _record_uoa_decision_event(
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
