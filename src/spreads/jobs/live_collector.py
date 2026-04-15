#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time as time_module
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from spreads.common import env_or_die, load_local_env
from spreads.alerts.dispatcher import dispatch_cycle_alerts
from spreads.events.bus import build_global_event
from spreads.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from spreads.integrations.calendar_events import build_calendar_event_resolver
from spreads.integrations.greeks import build_local_greeks_provider
from spreads.runtime.config import default_database_url
from spreads.services.execution import submit_auto_session_execution
from spreads.services.live_selection import (
    read_previous_selection,
    select_live_opportunities,
)
from spreads.services.live_collector_health import (
    CAPTURE_HISTORY_BLOCK_THRESHOLD,
    build_capture_history_gate,
    build_live_action_gate,
    build_quote_capture_summary,
    build_selection_summary,
    build_trade_capture_summary,
    enrich_live_collector_job_run_payload,
)
from spreads.services.live_recovery import (
    LIVE_SLOT_STATUS_MISSED,
    load_session_slot_health,
    merge_live_action_gate_with_recovery,
    refresh_live_session_capture_targets,
    resolve_live_slot_stale_after_seconds,
)
from spreads.services.live_pipelines import build_live_snapshot_label
from spreads.services.option_structures import candidate_legs, legs_identity_key
from spreads.services.option_market_data_capture import (
    request_option_market_data_capture,
)
from spreads.services.option_quote_records import (
    build_quote_records,
    build_quote_symbol_metadata,
)
from spreads.services.uoa_quote_summary import build_uoa_quote_summary
from spreads.services.uoa_root_decisions import build_uoa_root_decisions
from spreads.services.candidate_history_recovery import (
    recover_session_candidates_from_history,
)
from spreads.services.uoa_trade_baselines import build_uoa_trade_baselines
from spreads.services.option_trade_records import build_trade_symbol_metadata
from spreads.services.signal_state import sync_live_collector_signal_layer
from spreads.services.uoa_trade_summary import build_uoa_trade_summary
from spreads.services.scanner import (
    NEW_YORK,
    SpreadCandidate,
    SymbolScanResult,
    UniverseScanFailure,
    merge_strategy_candidates,
    parse_args as parse_scanner_args,
    resolve_symbols,
    scan_symbol_across_strategies,
    sort_candidates_for_display,
)
from spreads.storage.alert_repository import AlertRepository
from spreads.storage.factory import (
    build_storage_context,
)
from spreads.storage.collector_repository import CollectorRepository
from spreads.storage.event_repository import EventRepository
from spreads.storage.run_history_repository import RunHistoryRepository
from spreads.storage.signal_repository import SignalRepository

WATCHLIST_PER_STRATEGY = 3
WATCHLIST_TOP = 12
WATCHLIST_QUOTE_CAPTURE_TOP = 6
MARKET_RECORDER_SOURCE = "market_recorder"
MARKET_RECORDER_POLL_SECONDS = 25.0
MARKET_RECORDER_WAIT_GRACE_SECONDS = 10.0
MARKET_RECORDER_QUERY_POLL_SECONDS = 2.0


@dataclass(frozen=True)
class LiveTickContext:
    job_run_id: str
    session_id: str
    slot_at: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll the options scanner intraday and persist live opportunity cycles, selection state, and quote events to Postgres."
    )
    parser.add_argument(
        "--universe",
        default="0dte_core",
        help="Universe preset to scan. Default: 0dte_core",
    )
    parser.add_argument("--symbols", help="Optional comma-separated symbol list.")
    parser.add_argument(
        "--symbols-file", help="Optional file containing one symbol per line."
    )
    parser.add_argument(
        "--strategy",
        default="combined",
        choices=(
            "call_credit",
            "put_credit",
            "call_debit",
            "put_debit",
            "long_straddle",
            "long_strangle",
            "iron_condor",
            "combined",
        ),
        help="Strategy mode. Default: combined",
    )
    parser.add_argument(
        "--profile",
        default="0dte",
        choices=("0dte", "micro", "weekly", "swing", "core"),
        help="Scanner profile. Default: 0dte",
    )
    parser.add_argument(
        "--greeks-source",
        default="auto",
        choices=("alpaca", "local", "auto"),
        help="Greeks source mode. Default: auto",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Maximum promotable opportunities to keep per cycle. Default: 10",
    )
    parser.add_argument(
        "--per-symbol-top",
        type=int,
        default=1,
        help="Maximum spreads to keep per symbol before live ranking. Default: 1",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=300,
        help="Polling interval in seconds. Default: 300",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of polling cycles to run. Default: 1",
    )
    parser.add_argument(
        "--quote-capture-seconds",
        type=int,
        default=20,
        help="Seconds of live option quote capture per cycle. Default: 20",
    )
    parser.add_argument(
        "--trade-capture-seconds",
        type=int,
        default=10,
        help="Seconds of live option trade capture per cycle. Default: 10",
    )
    parser.add_argument(
        "--allow-off-hours",
        action="store_true",
        help="Run even outside regular market hours.",
    )
    parser.add_argument(
        "--session-start-offset-minutes",
        type=int,
        default=0,
        help="Minutes relative to the 9:30 ET open when collection can begin. Default: 0",
    )
    parser.add_argument(
        "--session-end-offset-minutes",
        type=int,
        default=0,
        help="Minutes relative to the 4:00 ET close when collection should stop. Default: 0",
    )
    parser.add_argument(
        "--history-db",
        default=default_database_url(),
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def build_collection_args(
    overrides: dict[str, Any] | None = None,
) -> argparse.Namespace:
    args = parse_args([])
    for key, value in (overrides or {}).items():
        setattr(args, key, value)
    return args


def collection_window_is_open(
    *,
    now: datetime | None = None,
    session_start_offset_minutes: int = 0,
    session_end_offset_minutes: int = 0,
) -> bool:
    current = datetime.now(NEW_YORK) if now is None else now.astimezone(NEW_YORK)
    if current.weekday() >= 5:
        return False
    session_start = current.replace(
        hour=9, minute=30, second=0, microsecond=0
    ) + timedelta(minutes=session_start_offset_minutes)
    session_end = current.replace(
        hour=16, minute=0, second=0, microsecond=0
    ) + timedelta(minutes=session_end_offset_minutes)
    return session_start <= current <= session_end


def build_scanner_args(args: argparse.Namespace) -> argparse.Namespace:
    scanner_args = parse_scanner_args([])
    scanner_args.symbol = None
    scanner_args.symbols = args.symbols
    scanner_args.symbols_file = args.symbols_file
    scanner_args.universe = args.universe
    scanner_args.strategy = args.strategy
    scanner_args.profile = args.profile
    scanner_args.greeks_source = args.greeks_source
    scanner_args.top = args.top
    scanner_args.per_symbol_top = args.per_symbol_top
    scanner_args.output = None
    scanner_args.json = False
    scanner_args.show_order_json = False
    scanner_args.stream_live_quotes = False
    return scanner_args


def run_universe_cycle(
    *,
    scanner_args: argparse.Namespace,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    history_store: RunHistoryRepository,
) -> tuple[
    list[str],
    str,
    list[SymbolScanResult],
    list[UniverseScanFailure],
    list[SpreadCandidate],
]:
    symbols, universe_label = resolve_symbols(scanner_args)
    scanner_args.session_label = build_live_snapshot_label(
        universe_label=universe_label,
        strategy=scanner_args.strategy,
        profile=scanner_args.profile,
        greeks_source=scanner_args.greeks_source,
    )
    scan_results: list[SymbolScanResult] = []
    failures: list[UniverseScanFailure] = []
    selected_candidates: list[SpreadCandidate] = []

    for symbol in symbols:
        strategy_results, symbol_failures = scan_symbol_across_strategies(
            symbol=symbol,
            base_args=scanner_args,
            client=client,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            history_store=history_store,
        )
        failures.extend(symbol_failures)
        if not strategy_results:
            continue
        scan_results.extend(strategy_results)
        symbol_selected_candidates = merge_strategy_candidates(
            strategy_results,
            per_strategy_top=scanner_args.per_symbol_top,
        )[: scanner_args.per_symbol_top]
        selected_candidates.extend(symbol_selected_candidates)

    selected_candidates = sort_candidates_for_display(selected_candidates)
    selected_candidates = selected_candidates[: scanner_args.top]
    return symbols, universe_label, scan_results, failures, selected_candidates


def build_cycle_id(label: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{label}"


def serialize_candidate(
    candidate: SpreadCandidate, run_id: str | None
) -> dict[str, Any]:
    payload = asdict(candidate)
    payload["run_id"] = run_id
    return payload


def build_symbol_strategy_candidates(
    scan_results: list[SymbolScanResult],
    run_ids: dict[tuple[str, str], str],
    *,
    max_per_strategy: int = 1,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for result in scan_results:
        if not result.candidates:
            continue
        for candidate in result.candidates[: max(max_per_strategy, 1)]:
            payload = serialize_candidate(
                candidate,
                run_ids.get((result.symbol, result.args.strategy)),
            )
            grouped.setdefault(result.symbol, []).append(payload)
    for symbol in grouped:
        grouped[symbol].sort(
            key=lambda candidate: candidate["quality_score"], reverse=True
        )
    return grouped


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


def collect_websocket_market_data_records(
    *,
    args: argparse.Namespace,
    candidates: list[dict[str, Any]],
    feed: str,
) -> dict[str, Any]:
    quote_duration_seconds = float(max(getattr(args, "quote_capture_seconds", 0), 0))
    trade_duration_seconds = float(max(getattr(args, "trade_capture_seconds", 0), 0))
    if not candidates or (quote_duration_seconds <= 0 and trade_duration_seconds <= 0):
        return {
            "quotes": [],
            "trades": [],
            "quote_error": None,
            "trade_error": None,
        }
    return request_option_market_data_capture(
        candidates=candidates,
        feed=feed,
        quote_duration_seconds=quote_duration_seconds,
        trade_duration_seconds=trade_duration_seconds,
        data_base_url=getattr(args, "data_base_url", None),
    )


def print_cycle_summary(
    *,
    generated_at: str,
    label: str,
    promotable_candidates: list[dict[str, Any]],
    monitor_candidates: list[dict[str, Any]],
    events: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    failures: list[UniverseScanFailure],
    quote_event_count: int,
    trade_event_count: int,
    uoa_summary: dict[str, Any],
    uoa_decisions: dict[str, Any],
    auto_execution: dict[str, Any] | None,
) -> None:
    print(f"[{generated_at}] {label}")
    print(f"Promotable opportunities: {len(promotable_candidates)}")
    print(f"Monitor opportunities: {len(monitor_candidates)}")
    print(f"Events: {len(events)}")
    print(f"Alerts: {len(alerts)}")
    print(f"Quote events saved: {quote_event_count}")
    print(f"Trade events saved: {trade_event_count}")
    uoa_overview = dict(uoa_summary.get("overview") or {})
    if int(uoa_overview.get("scoreable_trade_count") or 0) > 0:
        print(
            "UOA scoreable flow: "
            f"{int(uoa_overview.get('scoreable_trade_count') or 0)} trades across "
            f"{int(uoa_overview.get('scoreable_contract_count') or 0)} contracts "
            f"(${float(uoa_overview.get('scoreable_premium') or 0.0):,.0f} premium)"
        )
    uoa_decision_overview = dict(uoa_decisions.get("overview") or {})
    if int(uoa_decision_overview.get("root_count") or 0) > 0:
        print(
            "UOA decisions: "
            f"{int(uoa_decision_overview.get('monitor_count') or 0)} monitor / "
            f"{int(uoa_decision_overview.get('promotable_count') or 0)} promotable / "
            f"{int(uoa_decision_overview.get('high_count') or 0)} high"
        )
    if auto_execution is not None:
        print(f"Auto execution: {auto_execution.get('message')}")
    if failures:
        print(f"Failures: {len(failures)}")
    if promotable_candidates:
        print("Promotable:")
        for index, candidate in enumerate(promotable_candidates, start=1):
            print(
                f"- {index}. {candidate['underlying_symbol']} "
                f"[{candidate['strategy']}] "
                f"{candidate['short_strike']:.2f}/{candidate['long_strike']:.2f} "
                f"score {candidate['quality_score']:.1f} "
                f"mid {candidate['midpoint_credit']:.2f}"
            )
    if events:
        print("Events:")
        for event in events:
            print(f"- {event['message']}")
    if alerts:
        print("Alerts:")
        for alert in alerts:
            payload = alert.get("payload", {})
            print(
                f"- {payload.get('symbol')} {payload.get('alert_type')} "
                f"status={alert.get('status')} cycle={payload.get('cycle_id')}"
            )
    print()


def _resolve_collection_reference_time(slot_at: str | datetime | None) -> datetime:
    if isinstance(slot_at, datetime):
        return slot_at
    if isinstance(slot_at, str) and slot_at:
        normalized = (
            slot_at.replace("Z", "+00:00") if slot_at.endswith("Z") else slot_at
        )
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.now(UTC)


def _session_date_for_generated_at(generated_at: str) -> str:
    normalized = (
        generated_at.replace("Z", "+00:00")
        if generated_at.endswith("Z")
        else generated_at
    )
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(NEW_YORK).date().isoformat()


def _build_skipped_tick_result(
    *,
    tick_context: LiveTickContext,
    status: str,
    reason: str,
    message: str,
    slot_status: str | None = None,
) -> dict[str, Any]:
    result = {
        "status": status,
        "reason": reason,
        "message": message,
        "iterations_completed": 0,
        "cycle_ids": [],
        "alerts_sent": 0,
        "quote_events_saved": 0,
        "baseline_quote_events_saved": 0,
        "stream_quote_events_saved": 0,
        "recovery_quote_events_saved": 0,
        "expected_quote_symbols": [],
        "trade_events_saved": 0,
        "stream_trade_events_saved": 0,
        "expected_trade_symbols": [],
        "signal_states_upserted": 0,
        "signal_transitions_recorded": 0,
        "opportunities_upserted": 0,
        "opportunities_expired": 0,
        "quote_capture": build_quote_capture_summary(
            expected_quote_symbols=[],
            total_quote_events_saved=0,
            baseline_quote_events_saved=0,
            stream_quote_events_saved=0,
            recovery_quote_events_saved=0,
        ),
        "trade_capture": build_trade_capture_summary(
            expected_trade_symbols=[],
            total_trade_events_saved=0,
            stream_trade_events_saved=0,
        ),
        "uoa_summary": build_uoa_trade_summary(
            expected_trade_symbols=[],
            trades=[],
        ),
        "uoa_quote_summary": build_uoa_quote_summary(
            as_of=tick_context.slot_at,
            expected_quote_symbols=[],
            quotes=[],
        ),
        "uoa_decisions": build_uoa_root_decisions(
            uoa_summary={},
            baselines_by_symbol={},
            quote_summary={},
            capture_window_seconds=0,
        ),
        "session_id": tick_context.session_id,
        "slot_at": tick_context.slot_at,
    }
    if slot_status is not None:
        result["slot_status"] = slot_status
    return result


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


def _run_collection_cycle(
    args: argparse.Namespace,
    *,
    tick_context: LiveTickContext | None,
    scanner_args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
    alert_store: AlertRepository,
    job_store: Any,
    collector_store: CollectorRepository,
    event_store: EventRepository,
    signal_store: SignalRepository,
    recovery_store: Any | None,
    calendar_resolver: Any,
    greeks_provider: Any,
    emit_output: bool,
    heartbeat: Callable[[], None] | None = None,
) -> dict[str, Any]:
    generated_at = (
        datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    session_date = _session_date_for_generated_at(generated_at)
    if heartbeat is not None:
        heartbeat()
    symbols, universe_label, scan_results, failures, _raw_top_candidates = (
        run_universe_cycle(
            scanner_args=scanner_args,
            client=client,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            history_store=history_store,
        )
    )
    label = build_live_snapshot_label(
        universe_label=universe_label,
        strategy=args.strategy,
        profile=args.profile,
        greeks_source=args.greeks_source,
    )
    cycle_id = build_cycle_id(label)
    latest_live_collector_run = job_store.get_latest_live_collector_run(
        label=label,
        status="succeeded",
    )
    signal_cycle_context = {}
    if latest_live_collector_run is not None:
        latest_run_payload = enrich_live_collector_job_run_payload(latest_live_collector_run)
        signal_cycle_context = {
            "uoa_decisions": dict(latest_run_payload.get("uoa_decisions") or {}),
            "uoa_quote_summary": dict(
                latest_run_payload.get("uoa_quote_summary") or {}
            ),
            "selection_summary": dict(
                latest_run_payload.get("selection_summary") or {}
            ),
        }
    run_ids = {
        (result.symbol, result.args.strategy): result.run_id for result in scan_results
    }
    symbol_strategy_candidates = build_symbol_strategy_candidates(
        scan_results,
        run_ids,
        max_per_strategy=WATCHLIST_PER_STRATEGY,
    )
    previous_promotable, previous_selection_memory = read_previous_selection(
        collector_store, label
    )
    recovered_payloads: list[dict[str, Any]] = []
    selection = select_live_opportunities(
        label=label,
        cycle_id=cycle_id,
        generated_at=generated_at,
        symbol_candidates=symbol_strategy_candidates,
        previous_promotable=previous_promotable,
        previous_selection_memory=previous_selection_memory,
        top_promotable=args.top,
        top_monitor=WATCHLIST_TOP,
        profile=args.profile,
        signal_cycle_context=signal_cycle_context,
    )
    symbol_strategy_candidates = dict(selection.get("symbol_candidates") or {})
    promotable_payloads = list(selection["promotable_candidates"])
    monitor_payloads = list(selection["monitor_candidates"])
    if args.profile == "0dte" and not promotable_payloads and not monitor_payloads:
        recovered_payloads = recover_session_candidates_from_history(
            history_store=history_store,
            session_date=session_date,
            session_label=label,
            generated_at=generated_at,
            top=WATCHLIST_TOP,
            max_per_strategy=WATCHLIST_PER_STRATEGY,
        )
        selection = select_live_opportunities(
            label=label,
            cycle_id=cycle_id,
            generated_at=generated_at,
            symbol_candidates=symbol_strategy_candidates,
            previous_promotable=previous_promotable,
            previous_selection_memory=previous_selection_memory,
            top_promotable=args.top,
            top_monitor=WATCHLIST_TOP,
            profile=args.profile,
            recovered_candidates=recovered_payloads,
            signal_cycle_context=signal_cycle_context,
        )
        symbol_strategy_candidates = dict(selection.get("symbol_candidates") or {})
        promotable_payloads = list(selection["promotable_candidates"])
        monitor_payloads = list(selection["monitor_candidates"])
    opportunities = list(selection["opportunities"])
    selection_memory = dict(selection["selection_memory"])
    events = list(selection["events"])
    selection_summary = build_selection_summary(opportunities)
    collector_store.save_cycle(
        cycle_id=cycle_id,
        label=label,
        generated_at=generated_at,
        job_run_id=None if tick_context is None else tick_context.job_run_id,
        session_id=None if tick_context is None else tick_context.session_id,
        universe_label=universe_label,
        strategy=args.strategy,
        profile=args.profile,
        greeks_source=args.greeks_source,
        symbols=symbols,
        failures=[asdict(failure) for failure in failures],
        selection_memory=selection_memory,
        opportunities=opportunities,
        events=events,
    )
    signal_sync = {
        "signal_states_upserted": 0,
        "signal_transitions_recorded": 0,
        "opportunities_upserted": 0,
        "opportunities_expired": 0,
    }
    try:
        signal_sync = sync_live_collector_signal_layer(
            signal_store=signal_store,
            label=label,
            session_date=session_date,
            generated_at=generated_at,
            cycle_id=cycle_id,
            strategy=args.strategy,
            profile=args.profile,
            symbols=symbols,
            symbol_candidates=symbol_strategy_candidates,
            selection_memory=selection_memory,
            failures=[asdict(failure) for failure in failures],
            persisted_opportunities=collector_store.list_cycle_candidates(cycle_id),
        )
    except Exception as exc:
        print(f"Signal-state sync unavailable: {exc}")
    if heartbeat is not None:
        heartbeat()
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
    quote_candidates = build_capture_candidates(
        promotable_candidates=promotable_payloads,
        monitor_candidates=monitor_payloads,
        opportunities=opportunities,
        monitor_limit=WATCHLIST_QUOTE_CAPTURE_TOP,
    )
    contract_metadata_by_symbol = build_quote_symbol_metadata(quote_candidates)
    expected_quote_symbols = list(contract_metadata_by_symbol.keys())
    expected_trade_symbols = list(build_trade_symbol_metadata(quote_candidates).keys())
    expected_uoa_roots = sorted(
        {
            str(candidate.get("underlying_symbol") or "").strip()
            for candidate in quote_candidates
            if str(candidate.get("underlying_symbol") or "").strip()
        }
    )
    capture_targets: dict[str, list[dict[str, Any]]] = {
        "promotable": [],
        "monitor": [],
    }
    recorder_capture_requested_at: str | None = None
    if tick_context is not None and recovery_store is not None:
        requested_at = (
            datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        )
        try:
            target_refresh = refresh_live_session_capture_targets(
                recovery_store=recovery_store,
                session_id=tick_context.session_id,
                session_date=session_date,
                label=label,
                profile=args.profile,
                promotable_candidates=promotable_payloads,
                monitor_candidates=monitor_payloads,
                capture_candidates=quote_candidates,
                data_base_url=getattr(scanner_args, "data_base_url", None),
                session_end_offset_minutes=int(
                    getattr(args, "session_end_offset_minutes", 0)
                ),
            )
            capture_targets = {
                str(reason): [dict(row) for row in rows if isinstance(row, dict)]
                for reason, rows in dict(
                    target_refresh.get("capture_targets") or {}
                ).items()
            }
            recorder_capture_requested_at = requested_at
        except Exception as exc:
            print(f"Capture target refresh unavailable: {exc}")
    if quote_candidates:
        try:
            latest_quote_records = collect_latest_quote_records(
                client=client,
                candidates=quote_candidates,
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
            try:
                using_market_recorder = recorder_capture_requested_at is not None
                if using_market_recorder:
                    capture_response = collect_recorded_market_data_records(
                        history_store=history_store,
                        label=label,
                        profile=args.profile,
                        expected_quote_symbols=expected_quote_symbols,
                        expected_trade_symbols=expected_trade_symbols,
                        captured_from=recorder_capture_requested_at,
                        wait_timeout_seconds=max(
                            stream_quote_duration_seconds,
                            stream_trade_duration_seconds,
                            1.0,
                        )
                        + MARKET_RECORDER_POLL_SECONDS
                        + MARKET_RECORDER_WAIT_GRACE_SECONDS,
                    )
                else:
                    capture_args = argparse.Namespace(**vars(args))
                    capture_args.quote_capture_seconds = stream_quote_duration_seconds
                    capture_args.trade_capture_seconds = stream_trade_duration_seconds
                    capture_response = collect_websocket_market_data_records(
                        args=capture_args,
                        candidates=quote_candidates,
                        feed=scanner_args.feed,
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
                        f"Live {'recorder' if using_market_recorder else 'stream'} quote capture unavailable: {stream_quote_error}"
                    )
                if stream_trade_error:
                    print(
                        f"Live {'recorder' if using_market_recorder else 'stream'} trade capture unavailable: {stream_trade_error}"
                    )
                if stream_quote_records:
                    if using_market_recorder:
                        if bool(capture_response.get("quote_complete", True)):
                            stream_quote_event_count = len(stream_quote_records)
                    else:
                        stream_quote_event_count = (
                            history_store.save_option_quote_events(
                                cycle_id=cycle_id,
                                label=label,
                                profile=args.profile,
                                quotes=stream_quote_records,
                            )
                        )
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
                            f"Live {'recorder' if using_market_recorder else 'stream'} quote event normalization unavailable: {exc}"
                        )
                if stream_trade_records:
                    if using_market_recorder:
                        stream_trade_event_count = len(stream_trade_records)
                    else:
                        stream_trade_event_count = (
                            history_store.save_option_trade_events(
                                cycle_id=cycle_id,
                                label=label,
                                profile=args.profile,
                                trades=stream_trade_records,
                            )
                        )
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
                            f"Live {'recorder' if using_market_recorder else 'stream'} trade event normalization unavailable: {exc}"
                        )
            except Exception as exc:
                print(
                    "Live "
                    f"{'recorder' if recorder_capture_requested_at is not None else 'stream'} "
                    f"market-data capture unavailable: {exc}"
                )
        if quote_event_count == 0:
            try:
                recovery_quote_records = collect_latest_quote_records(
                    client=client,
                    candidates=quote_candidates,
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
    live_action_gate = build_live_action_gate(
        profile=args.profile,
        quote_capture=quote_capture,
    )
    slot_recovery = None
    if tick_context is not None and recovery_store is not None:
        try:
            slot_recovery = load_session_slot_health(
                recovery_store=recovery_store,
                session_id=tick_context.session_id,
            )
            merged_gate = merge_live_action_gate_with_recovery(
                base_gate=live_action_gate,
                slot_health=slot_recovery,
            )
            if merged_gate is not None:
                live_action_gate = merged_gate
        except Exception as exc:
            print(f"Live recovery gate unavailable: {exc}")
    if tick_context is not None:
        try:
            recent_runs = [
                enrich_live_collector_job_run_payload(row)
                for row in job_store.list_job_runs(
                    job_type="live_collector",
                    session_id=tick_context.session_id,
                    limit=CAPTURE_HISTORY_BLOCK_THRESHOLD + 3,
                )
            ]
            recent_capture_statuses = [quote_capture["capture_status"]]
            recent_capture_statuses.extend(
                str(row.get("capture_status") or "").strip().lower()
                for row in recent_runs
                if str(row.get("job_run_id") or "") != tick_context.job_run_id
                and str(row.get("status") or "") == "succeeded"
                and str(row.get("capture_status") or "").strip()
            )
            history_gate = build_capture_history_gate(recent_capture_statuses)
            if (
                history_gate is not None
                and str(live_action_gate.get("status") or "") != "blocked"
            ):
                live_action_gate = history_gate
        except Exception as exc:
            print(f"Live capture history gate unavailable: {exc}")
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
    if heartbeat is not None:
        heartbeat()
    auto_execution: dict[str, Any] | None = None
    if tick_context is not None and bool(live_action_gate.get("allow_auto_execution")):
        try:
            auto_execution = submit_auto_session_execution(
                db_target=args.history_db,
                session_id=tick_context.session_id,
                cycle_id=cycle_id,
                policy=getattr(args, "execution_policy", None),
                job_run_id=tick_context.job_run_id,
                reactive_quote_records=reactive_quote_records,
            )
        except Exception as exc:
            auto_execution = {
                "action": "auto_submit",
                "changed": False,
                "reason": "execution_error",
                "message": f"Automatic execution failed: {exc}",
                "error": str(exc),
            }
            print(f"Automatic execution unavailable: {exc}")
        if heartbeat is not None:
            heartbeat()
    elif tick_context is not None:
        auto_execution = {
            "action": "auto_submit",
            "changed": False,
            "reason": live_action_gate.get("reason_code"),
            "message": live_action_gate.get("message"),
            "gate": dict(live_action_gate),
        }
    alerts: list[dict[str, Any]] = []
    if bool(live_action_gate.get("allow_alerts")):
        try:
            alerts = dispatch_cycle_alerts(
                collector_store=collector_store,
                alert_store=alert_store,
                job_store=job_store,
                cycle_id=cycle_id,
                label=label,
                generated_at=generated_at,
                strategy_mode=args.strategy,
                profile=args.profile,
                promotable_candidates=promotable_payloads,
                events=events,
                uoa_decisions=uoa_decisions,
                session_id=None if tick_context is None else tick_context.session_id,
                planner_job_run_id=None
                if tick_context is None
                else tick_context.job_run_id,
            )
        except Exception as exc:
            print(f"Alert dispatch unavailable: {exc}")
    if emit_output:
        print_cycle_summary(
            generated_at=generated_at,
            label=label,
            promotable_candidates=promotable_payloads,
            monitor_candidates=monitor_payloads,
            events=events,
            alerts=alerts,
            failures=failures,
            quote_event_count=quote_event_count,
            trade_event_count=trade_event_count,
            uoa_summary=uoa_summary,
            uoa_decisions=uoa_decisions,
            auto_execution=auto_execution,
        )
    return {
        "cycle_id": cycle_id,
        "generated_at": generated_at,
        "label": label,
        "profile": args.profile,
        "alerts_sent": len(alerts),
        "quote_events_saved": quote_event_count,
        "baseline_quote_events_saved": baseline_quote_event_count,
        "stream_quote_events_saved": stream_quote_event_count,
        "websocket_quote_events_saved": stream_quote_event_count,
        "recovery_quote_events_saved": recovery_quote_event_count,
        "expected_quote_symbols": expected_quote_symbols,
        "stream_quote_error": stream_quote_error,
        "trade_events_saved": trade_event_count,
        "stream_trade_events_saved": stream_trade_event_count,
        "websocket_trade_events_saved": stream_trade_event_count,
        "expected_trade_symbols": expected_trade_symbols,
        "stream_trade_error": stream_trade_error,
        "promotable_opportunity_count": len(promotable_payloads),
        "monitor_opportunity_count": len(monitor_payloads),
        "signal_states_upserted": int(signal_sync["signal_states_upserted"]),
        "signal_transitions_recorded": int(signal_sync["signal_transitions_recorded"]),
        "opportunities_upserted": int(signal_sync["opportunities_upserted"]),
        "opportunities_expired": int(signal_sync["opportunities_expired"]),
        "quote_capture": quote_capture,
        "trade_capture": trade_capture,
        "live_action_gate": live_action_gate,
        "slot_recovery": slot_recovery,
        "capture_targets": capture_targets,
        "uoa_summary": uoa_summary,
        "uoa_quote_summary": uoa_quote_summary,
        "uoa_decisions": uoa_decisions,
        "selection_summary": selection_summary,
        "auto_execution": auto_execution,
    }


def run_collection_tick(
    args: argparse.Namespace,
    *,
    tick_context: LiveTickContext,
    heartbeat: Callable[[], None] | None = None,
    emit_output: bool = True,
) -> dict[str, Any]:
    scanner_args = build_scanner_args(args)
    reference_time = _resolve_collection_reference_time(tick_context.slot_at)
    if not args.allow_off_hours and not collection_window_is_open(
        now=reference_time,
        session_start_offset_minutes=int(
            getattr(args, "session_start_offset_minutes", 0)
        ),
        session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
    ):
        if emit_output:
            print("Scheduled slot is outside the collection window. Skipping.")
        return _build_skipped_tick_result(
            tick_context=tick_context,
            status="skipped",
            reason="market_closed",
            message="Scheduled slot is outside the collection window.",
        )

    with build_storage_context(args.history_db) as storage:
        recovery_store = storage.recovery
        stale_after_seconds = resolve_live_slot_stale_after_seconds(
            int(getattr(args, "interval_seconds", 0))
        )
        now = datetime.now(UTC)
        slot_at = _resolve_collection_reference_time(tick_context.slot_at)
        if now > slot_at + timedelta(seconds=stale_after_seconds):
            session_date = str(
                getattr(args, "session_date", "")
                or _session_date_for_generated_at(tick_context.slot_at)
            )
            label = str(
                getattr(args, "label", "")
                or build_live_snapshot_label(
                    universe_label=args.universe,
                    strategy=args.strategy,
                    profile=args.profile,
                    greeks_source=args.greeks_source,
                )
            )
            message = "Scheduled live slot is stale and will be marked missed instead of replayed."
            if recovery_store.schema_ready():
                recovery_store.upsert_live_session_slot(
                    job_key=str(getattr(args, "job_key", "") or "live_collector"),
                    session_id=tick_context.session_id,
                    session_date=session_date,
                    label=label,
                    slot_at=tick_context.slot_at,
                    scheduled_for=tick_context.slot_at,
                    status=LIVE_SLOT_STATUS_MISSED,
                    job_run_id=tick_context.job_run_id,
                    recovery_note=message,
                    finished_at=now.isoformat().replace("+00:00", "Z"),
                    updated_at=now.isoformat().replace("+00:00", "Z"),
                )
            if emit_output:
                print(message)
            return _build_skipped_tick_result(
                tick_context=tick_context,
                status="skipped",
                reason="stale_slot",
                message=message,
                slot_status=LIVE_SLOT_STATUS_MISSED,
            )

        key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
        secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
        client = AlpacaClient(
            key_id=key_id,
            secret_key=secret_key,
            trading_base_url=infer_trading_base_url(
                key_id, scanner_args.trading_base_url
            ),
            data_base_url=scanner_args.data_base_url,
        )
        calendar_resolver = build_calendar_event_resolver(
            key_id=key_id,
            secret_key=secret_key,
            data_base_url=scanner_args.data_base_url,
            database_url=args.history_db,
        )
        greeks_provider = build_local_greeks_provider()
        try:
            if heartbeat is not None:
                heartbeat()
            cycle_result = _run_collection_cycle(
                args,
                tick_context=tick_context,
                scanner_args=scanner_args,
                client=client,
                history_store=storage.history,
                alert_store=storage.alerts,
                job_store=storage.jobs,
                collector_store=storage.collector,
                event_store=storage.events,
                signal_store=storage.signals,
                recovery_store=recovery_store,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                emit_output=emit_output,
                heartbeat=heartbeat,
            )
        finally:
            calendar_resolver.store.close()

    return {
        "status": "completed",
        "iterations_completed": 1,
        "cycle_ids": [cycle_result["cycle_id"]],
        "alerts_sent": cycle_result["alerts_sent"],
        "quote_events_saved": cycle_result["quote_events_saved"],
        "baseline_quote_events_saved": cycle_result["baseline_quote_events_saved"],
        "stream_quote_events_saved": cycle_result["stream_quote_events_saved"],
        "websocket_quote_events_saved": cycle_result["stream_quote_events_saved"],
        "recovery_quote_events_saved": cycle_result["recovery_quote_events_saved"],
        "expected_quote_symbols": list(cycle_result["expected_quote_symbols"]),
        "trade_events_saved": cycle_result["trade_events_saved"],
        "stream_trade_events_saved": cycle_result["stream_trade_events_saved"],
        "websocket_trade_events_saved": cycle_result["stream_trade_events_saved"],
        "expected_trade_symbols": list(cycle_result["expected_trade_symbols"]),
        "promotable_opportunity_count": cycle_result["promotable_opportunity_count"],
        "monitor_opportunity_count": cycle_result["monitor_opportunity_count"],
        "signal_states_upserted": cycle_result["signal_states_upserted"],
        "signal_transitions_recorded": cycle_result["signal_transitions_recorded"],
        "opportunities_upserted": cycle_result["opportunities_upserted"],
        "opportunities_expired": cycle_result["opportunities_expired"],
        "quote_capture": dict(cycle_result["quote_capture"]),
        "trade_capture": dict(cycle_result["trade_capture"]),
        "uoa_summary": dict(cycle_result["uoa_summary"]),
        "uoa_quote_summary": dict(cycle_result["uoa_quote_summary"]),
        "uoa_decisions": dict(cycle_result["uoa_decisions"]),
        "selection_summary": dict(cycle_result["selection_summary"]),
        "auto_execution": cycle_result["auto_execution"],
        "label": cycle_result["label"],
        "session_id": tick_context.session_id,
        "slot_at": tick_context.slot_at,
    }


def run_collection(
    args: argparse.Namespace,
    *,
    heartbeat: Callable[[], None] | None = None,
    emit_output: bool = True,
) -> dict[str, Any]:
    scanner_args = build_scanner_args(args)

    if not args.allow_off_hours and not collection_window_is_open(
        now=_resolve_collection_reference_time(None),
        session_start_offset_minutes=int(
            getattr(args, "session_start_offset_minutes", 0)
        ),
        session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
    ):
        if emit_output:
            print("Market is closed. Use --allow-off-hours to collect cycles anyway.")
        return {
            "status": "skipped",
            "reason": "market_closed",
            "iterations_completed": 0,
            "cycle_ids": [],
            "alerts_sent": 0,
            "quote_events_saved": 0,
            "baseline_quote_events_saved": 0,
            "stream_quote_events_saved": 0,
            "recovery_quote_events_saved": 0,
            "expected_quote_symbols": [],
            "trade_events_saved": 0,
            "stream_trade_events_saved": 0,
            "expected_trade_symbols": [],
            "signal_states_upserted": 0,
            "signal_transitions_recorded": 0,
            "opportunities_upserted": 0,
            "opportunities_expired": 0,
            "quote_capture": build_quote_capture_summary(
                expected_quote_symbols=[],
                total_quote_events_saved=0,
                baseline_quote_events_saved=0,
                stream_quote_events_saved=0,
                recovery_quote_events_saved=0,
            ),
            "trade_capture": build_trade_capture_summary(
                expected_trade_symbols=[],
                total_trade_events_saved=0,
                stream_trade_events_saved=0,
            ),
            "uoa_summary": build_uoa_trade_summary(
                expected_trade_symbols=[], trades=[]
            ),
            "uoa_quote_summary": build_uoa_quote_summary(
                as_of=datetime.now(UTC).isoformat(),
                expected_quote_symbols=[],
                quotes=[],
            ),
            "uoa_decisions": build_uoa_root_decisions(
                uoa_summary={},
                baselines_by_symbol={},
                quote_summary={},
                capture_window_seconds=0,
            ),
            "selection_summary": build_selection_summary([]),
        }

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, scanner_args.trading_base_url),
        data_base_url=scanner_args.data_base_url,
    )
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=scanner_args.data_base_url,
        database_url=args.history_db,
    )
    greeks_provider = build_local_greeks_provider()
    cycle_ids: list[str] = []
    total_alerts = 0
    total_quote_events = 0
    total_baseline_quote_events = 0
    total_stream_quote_events = 0
    total_recovery_quote_events = 0
    total_trade_events = 0
    total_stream_trade_events = 0
    total_signal_states = 0
    total_signal_transitions = 0
    total_opportunities = 0
    total_opportunities_expired = 0
    last_label: str | None = None
    last_uoa_summary = build_uoa_trade_summary(expected_trade_symbols=[], trades=[])
    last_uoa_quote_summary = build_uoa_quote_summary(
        as_of=datetime.now(UTC).isoformat(),
        expected_quote_symbols=[],
        quotes=[],
    )
    last_uoa_decisions = build_uoa_root_decisions(
        uoa_summary={},
        baselines_by_symbol={},
        quote_summary={},
        capture_window_seconds=0,
    )
    last_selection_summary = build_selection_summary([])
    iterations_completed = 0
    try:
        with build_storage_context(args.history_db) as storage:
            for iteration in range(args.iterations):
                iteration_started_at = time_module.monotonic()
                if heartbeat is not None:
                    heartbeat()
                if not args.allow_off_hours and not collection_window_is_open(
                    now=datetime.now(UTC),
                    session_start_offset_minutes=int(
                        getattr(args, "session_start_offset_minutes", 0)
                    ),
                    session_end_offset_minutes=int(
                        getattr(args, "session_end_offset_minutes", 0)
                    ),
                ):
                    if emit_output:
                        print("Market closed during collection window. Stopping.")
                    break
                cycle_result = _run_collection_cycle(
                    args,
                    tick_context=None,
                    scanner_args=scanner_args,
                    client=client,
                    history_store=storage.history,
                    alert_store=storage.alerts,
                    job_store=storage.jobs,
                    collector_store=storage.collector,
                    event_store=storage.events,
                    signal_store=storage.signals,
                    recovery_store=storage.recovery,
                    calendar_resolver=calendar_resolver,
                    greeks_provider=greeks_provider,
                    emit_output=emit_output,
                    heartbeat=heartbeat,
                )
                cycle_ids.append(cycle_result["cycle_id"])
                total_alerts += int(cycle_result["alerts_sent"])
                total_quote_events += int(cycle_result["quote_events_saved"])
                total_baseline_quote_events += int(
                    cycle_result["baseline_quote_events_saved"]
                )
                total_stream_quote_events += int(
                    cycle_result["stream_quote_events_saved"]
                )
                total_recovery_quote_events += int(
                    cycle_result["recovery_quote_events_saved"]
                )
                total_trade_events += int(cycle_result["trade_events_saved"])
                total_stream_trade_events += int(
                    cycle_result["stream_trade_events_saved"]
                )
                total_signal_states += int(cycle_result["signal_states_upserted"])
                total_signal_transitions += int(
                    cycle_result["signal_transitions_recorded"]
                )
                total_opportunities += int(cycle_result["opportunities_upserted"])
                total_opportunities_expired += int(
                    cycle_result["opportunities_expired"]
                )
                iterations_completed += 1
                last_label = str(cycle_result["label"])
                last_uoa_summary = dict(cycle_result["uoa_summary"])
                last_uoa_quote_summary = dict(cycle_result["uoa_quote_summary"])
                last_uoa_decisions = dict(cycle_result["uoa_decisions"])
                last_selection_summary = dict(cycle_result["selection_summary"])
                if iteration < args.iterations - 1:
                    elapsed_seconds = time_module.monotonic() - iteration_started_at
                    sleep_seconds = max(
                        float(max(args.interval_seconds, 1)) - elapsed_seconds, 0.0
                    )
                    if sleep_seconds > 0:
                        time_module.sleep(sleep_seconds)
                    if heartbeat is not None:
                        heartbeat()
    finally:
        calendar_resolver.store.close()

    return {
        "status": "completed",
        "iterations_completed": iterations_completed,
        "cycle_ids": cycle_ids,
        "alerts_sent": total_alerts,
        "quote_events_saved": total_quote_events,
        "baseline_quote_events_saved": total_baseline_quote_events,
        "stream_quote_events_saved": total_stream_quote_events,
        "websocket_quote_events_saved": total_stream_quote_events,
        "recovery_quote_events_saved": total_recovery_quote_events,
        "trade_events_saved": total_trade_events,
        "stream_trade_events_saved": total_stream_trade_events,
        "websocket_trade_events_saved": total_stream_trade_events,
        "signal_states_upserted": total_signal_states,
        "signal_transitions_recorded": total_signal_transitions,
        "opportunities_upserted": total_opportunities,
        "opportunities_expired": total_opportunities_expired,
        "quote_capture": build_quote_capture_summary(
            expected_quote_symbols=[],
            total_quote_events_saved=total_quote_events,
            baseline_quote_events_saved=total_baseline_quote_events,
            stream_quote_events_saved=total_stream_quote_events,
            recovery_quote_events_saved=total_recovery_quote_events,
        ),
        "trade_capture": build_trade_capture_summary(
            expected_trade_symbols=[],
            total_trade_events_saved=total_trade_events,
            stream_trade_events_saved=total_stream_trade_events,
        ),
        "uoa_summary": last_uoa_summary,
        "uoa_quote_summary": last_uoa_quote_summary,
        "uoa_decisions": last_uoa_decisions,
        "selection_summary": last_selection_summary,
        "label": last_label,
    }


def main(argv: list[str] | None = None) -> int:
    load_local_env()
    args = parse_args(argv)
    run_collection(args, emit_output=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
