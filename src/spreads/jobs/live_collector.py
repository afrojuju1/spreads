#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time as time_module
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from spreads.common import env_or_die, load_local_env
from spreads.alerts.dispatcher import dispatch_cycle_alerts
from spreads.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from spreads.integrations.calendar_events import build_calendar_event_resolver
from spreads.integrations.greeks import build_local_greeks_provider
from spreads.runtime.config import default_database_url
from spreads.services.execution import submit_auto_session_execution
from spreads.services.live_collector_health import build_quote_capture_summary
from spreads.services.live_pipelines import build_live_snapshot_label
from spreads.services.option_quote_capture import request_option_quote_capture
from spreads.services.option_quote_records import build_quote_records, build_quote_symbol_metadata
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
    build_alert_repository,
    build_collector_repository,
    build_history_store,
)
from spreads.storage.collector_repository import CollectorRepository
from spreads.storage.run_history_repository import RunHistoryRepository

BOARD_SCORE_FLOOR = 65.0
BOARD_STRONG_SCORE = 82.0
BOARD_WINNER_GAP = 6.0
BOARD_SIDE_SWITCH_MARGIN = 10.0
BOARD_REPLACEMENT_MARGIN = 5.0
BOARD_CONFIRMATION_CYCLES = 2
BOARD_HOLD_TOLERANCE = 3.0
WATCHLIST_SCORE_FLOOR = 55.0
WATCHLIST_PER_STRATEGY = 3
WATCHLIST_PER_SYMBOL = 2
WATCHLIST_TOP = 12
WATCHLIST_QUOTE_CAPTURE_TOP = 6


@dataclass(frozen=True)
class LiveTickContext:
    job_run_id: str
    session_id: str
    slot_at: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll the options scanner intraday and persist live idea cycles, board/watchlist state, and quote events to Postgres."
    )
    parser.add_argument(
        "--universe",
        default="0dte_core",
        help="Universe preset to scan. Default: 0dte_core",
    )
    parser.add_argument("--symbols", help="Optional comma-separated symbol list.")
    parser.add_argument("--symbols-file", help="Optional file containing one symbol per line.")
    parser.add_argument(
        "--strategy",
        default="combined",
        choices=("call_credit", "put_credit", "combined"),
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
        help="Maximum board entries to keep per cycle. Default: 10",
    )
    parser.add_argument(
        "--per-symbol-top",
        type=int,
        default=1,
        help="Maximum spreads to keep per symbol before board ranking. Default: 1",
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


def build_collection_args(overrides: dict[str, Any] | None = None) -> argparse.Namespace:
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
    session_start = current.replace(hour=9, minute=30, second=0, microsecond=0) + timedelta(
        minutes=session_start_offset_minutes
    )
    session_end = current.replace(hour=16, minute=0, second=0, microsecond=0) + timedelta(
        minutes=session_end_offset_minutes
    )
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
) -> tuple[list[str], str, list[SymbolScanResult], list[UniverseScanFailure], list[SpreadCandidate]]:
    symbols, universe_label = resolve_symbols(scanner_args)
    scanner_args.session_label = build_live_snapshot_label(
        universe_label=universe_label,
        strategy=scanner_args.strategy,
        profile=scanner_args.profile,
        greeks_source=scanner_args.greeks_source,
    )
    scan_results: list[SymbolScanResult] = []
    failures: list[UniverseScanFailure] = []
    board_candidates: list[SpreadCandidate] = []

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
        symbol_board_candidates = merge_strategy_candidates(
            strategy_results,
            per_strategy_top=scanner_args.per_symbol_top,
        )[: scanner_args.per_symbol_top]
        board_candidates.extend(symbol_board_candidates)

    board_candidates = sort_candidates_for_display(board_candidates)
    board_candidates = board_candidates[: scanner_args.top]
    return symbols, universe_label, scan_results, failures, board_candidates


def build_cycle_id(label: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{label}"


def serialize_candidate(candidate: SpreadCandidate, run_id: str | None) -> dict[str, Any]:
    payload = asdict(candidate)
    payload["run_id"] = run_id
    return payload

def read_previous_cycle_state(
    collector_store: CollectorRepository,
    label: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    latest_cycle = collector_store.get_latest_cycle(label)
    if latest_cycle is None:
        return {}, {}

    board = collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="board")
    previous: dict[str, dict[str, Any]] = {}
    for candidate in board:
        payload = dict(candidate["candidate"])
        symbol = payload.get("underlying_symbol")
        if symbol:
            previous[str(symbol)] = payload

    raw_state = latest_cycle["selection_state"] or {}
    selection_state = {
        str(symbol): state
        for symbol, state in raw_state.items()
        if isinstance(symbol, str) and isinstance(state, dict)
    }
    return previous, selection_state


def candidate_identity(candidate: dict[str, Any]) -> str:
    return (
        f"{candidate['strategy']}|{candidate['short_symbol']}|{candidate['long_symbol']}"
    )


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
        grouped[symbol].sort(key=lambda candidate: candidate["quality_score"], reverse=True)
    return grouped


def evaluate_pending_candidate(
    *,
    symbol: str,
    winner: dict[str, Any],
    previous_state: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    winner_id = candidate_identity(winner)
    pending_id = previous_state.get("pending_identity")
    pending_count = int(previous_state.get("pending_count", 0))
    if pending_id == winner_id:
        pending_count += 1
    else:
        pending_count = 1
    state = {
        "pending_identity": winner_id,
        "pending_strategy": winner["strategy"],
        "pending_count": pending_count,
    }
    return pending_count >= BOARD_CONFIRMATION_CYCLES, state


def select_board_candidates(
    *,
    symbol_candidates: dict[str, list[dict[str, Any]]],
    previous_board: dict[str, dict[str, Any]],
    previous_state: dict[str, dict[str, Any]],
    top: int,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    next_state: dict[str, dict[str, Any]] = {}

    for symbol in sorted(symbol_candidates):
        options = sorted(
            symbol_candidates.get(symbol, []),
            key=lambda candidate: candidate["quality_score"],
            reverse=True,
        )
        viable = [candidate for candidate in options if candidate["quality_score"] >= BOARD_SCORE_FLOOR]
        winner = viable[0] if viable else None
        runner_up = viable[1] if len(viable) > 1 else None
        winner_gap = None
        if winner is not None and runner_up is not None:
            winner_gap = winner["quality_score"] - runner_up["quality_score"]

        previous = previous_board.get(symbol)
        state = previous_state.get(symbol, {})

        accepted: dict[str, Any] | None = None
        state_update: dict[str, Any] = {}

        if previous is None:
            if winner is not None:
                if (
                    winner["quality_score"] >= BOARD_STRONG_SCORE
                    or runner_up is None
                    or (winner_gap is not None and winner_gap >= BOARD_WINNER_GAP)
                ):
                    accepted = winner
                else:
                    confirmed, state_update = evaluate_pending_candidate(
                        symbol=symbol,
                        winner=winner,
                        previous_state=state,
                    )
                    if confirmed:
                        accepted = winner
                        state_update = {}
            if accepted is not None:
                state_update.update(
                    {
                        "accepted_identity": candidate_identity(accepted),
                        "accepted_strategy": accepted["strategy"],
                        "accepted_score": accepted["quality_score"],
                    }
                )
                selected.append(accepted)
            next_state[symbol] = state_update
            continue

        previous_id = candidate_identity(previous)
        previous_match = next((candidate for candidate in options if candidate_identity(candidate) == previous_id), None)
        previous_same_side = next(
            (candidate for candidate in options if candidate["strategy"] == previous["strategy"]),
            None,
        )
        current_anchor = previous_match or previous_same_side

        if current_anchor is not None and current_anchor["quality_score"] >= BOARD_SCORE_FLOOR - BOARD_HOLD_TOLERANCE:
            accepted = current_anchor
        elif winner is not None:
            if winner["strategy"] == previous["strategy"]:
                accepted = winner
            else:
                confirmed, state_update = evaluate_pending_candidate(
                    symbol=symbol,
                    winner=winner,
                    previous_state=state,
                )
                if confirmed:
                    accepted = winner
                    state_update = {}
        else:
            accepted = None

        if winner is not None and accepted is not None:
            accepted_id = candidate_identity(accepted)
            winner_id = candidate_identity(winner)
            if winner_id != accepted_id:
                same_side = winner["strategy"] == accepted["strategy"]
                score_gap = winner["quality_score"] - accepted["quality_score"]
                if same_side:
                    if score_gap >= BOARD_REPLACEMENT_MARGIN:
                        confirmed, state_update = evaluate_pending_candidate(
                            symbol=symbol,
                            winner=winner,
                            previous_state=state,
                        )
                        if confirmed:
                            accepted = winner
                            state_update = {}
                    else:
                        accepted = accepted
                else:
                    if (
                        score_gap >= BOARD_SIDE_SWITCH_MARGIN
                        and (winner_gap is None or winner_gap >= BOARD_WINNER_GAP)
                    ):
                        confirmed, state_update = evaluate_pending_candidate(
                            symbol=symbol,
                            winner=winner,
                            previous_state=state,
                        )
                        if confirmed:
                            accepted = winner
                            state_update = {}
                    else:
                        accepted = accepted

        if accepted is not None:
            accepted_score = accepted["quality_score"]
            if accepted_score < BOARD_SCORE_FLOOR:
                accepted = None
            else:
                state_update.update(
                    {
                        "accepted_identity": candidate_identity(accepted),
                        "accepted_strategy": accepted["strategy"],
                        "accepted_score": accepted_score,
                    }
                )
                if state_update.get("pending_identity") == state_update["accepted_identity"]:
                    state_update.pop("pending_identity", None)
                    state_update.pop("pending_strategy", None)
                    state_update.pop("pending_count", None)
                selected.append(accepted)

        next_state[symbol] = state_update

    selected.sort(
        key=lambda candidate: (
            candidate["quality_score"],
            candidate["return_on_risk"],
            candidate["midpoint_credit"],
            min(candidate["short_open_interest"], candidate["long_open_interest"]),
        ),
        reverse=True,
    )
    return selected[:top], next_state


def select_watchlist_candidates(
    *,
    symbol_candidates: dict[str, list[dict[str, Any]]],
    board_candidates: list[dict[str, Any]],
    top: int,
) -> list[dict[str, Any]]:
    accepted_ids = {candidate_identity(candidate) for candidate in board_candidates}
    watchlist: list[dict[str, Any]] = []

    for symbol in sorted(symbol_candidates):
        kept = 0
        for candidate in sorted(
            symbol_candidates.get(symbol, []),
            key=lambda item: item["quality_score"],
            reverse=True,
        ):
            if candidate_identity(candidate) in accepted_ids:
                continue
            if candidate["quality_score"] < WATCHLIST_SCORE_FLOOR:
                continue
            watchlist.append(candidate)
            kept += 1
            if kept >= WATCHLIST_PER_SYMBOL:
                break

    watchlist.sort(
        key=lambda candidate: (
            candidate["quality_score"],
            candidate["return_on_risk"],
            candidate["midpoint_credit"],
            min(candidate["short_open_interest"], candidate["long_open_interest"]),
        ),
        reverse=True,
    )
    return watchlist[:top]


def summarize_candidate(candidate: dict[str, Any]) -> str:
    return (
        f"{candidate['strategy']} {candidate['short_strike']:.2f}/{candidate['long_strike']:.2f} "
        f"score {candidate['quality_score']:.1f}"
    )


def build_events(
    *,
    label: str,
    cycle_id: str,
    generated_at: str,
    previous: dict[str, dict[str, Any]],
    current: dict[str, dict[str, Any]],
    score_delta_threshold: float = 5.0,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    all_symbols = sorted(set(previous) | set(current))
    for symbol in all_symbols:
        prev = previous.get(symbol)
        curr = current.get(symbol)
        if prev is None and curr is not None:
            message = f"{symbol} new idea: {summarize_candidate(curr)}"
            events.append(
                {
                    "generated_at": generated_at,
                    "cycle_id": cycle_id,
                    "label": label,
                    "symbol": symbol,
                    "event_type": "new",
                    "message": message,
                    "previous": None,
                    "current": curr,
                }
            )
            continue
        if prev is not None and curr is None:
            message = f"{symbol} dropped from board: {summarize_candidate(prev)}"
            events.append(
                {
                    "generated_at": generated_at,
                    "cycle_id": cycle_id,
                    "label": label,
                    "symbol": symbol,
                    "event_type": "dropped",
                    "message": message,
                    "previous": prev,
                    "current": None,
                }
            )
            continue
        if prev is None or curr is None:
            continue

        previous_identity = (prev["strategy"], prev["short_symbol"], prev["long_symbol"])
        current_identity = (curr["strategy"], curr["short_symbol"], curr["long_symbol"])
        if prev["strategy"] != curr["strategy"]:
            message = f"{symbol} side flipped: {summarize_candidate(prev)} -> {summarize_candidate(curr)}"
            event_type = "side_flip"
        elif previous_identity != current_identity:
            message = f"{symbol} idea replaced: {summarize_candidate(prev)} -> {summarize_candidate(curr)}"
            event_type = "replaced"
        else:
            score_change = curr["quality_score"] - prev["quality_score"]
            if abs(score_change) < score_delta_threshold:
                continue
            direction = "up" if score_change > 0 else "down"
            message = (
                f"{symbol} score {direction}: "
                f"{prev['quality_score']:.1f} -> {curr['quality_score']:.1f} "
                f"for {summarize_candidate(curr)}"
            )
            event_type = f"score_{direction}"

        events.append(
            {
                "generated_at": generated_at,
                "cycle_id": cycle_id,
                "label": label,
                "symbol": symbol,
                "event_type": event_type,
                "message": message,
                "previous": prev,
                "current": curr,
            }
        )
    return events


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
            latest_captured_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
            return build_quote_records(
                captured_at=latest_captured_at,
                symbol_metadata=symbol_metadata,
                quotes=list(latest_quotes.values()),
                source=source,
            )
        if attempt < max_attempts - 1 and retry_delay_seconds > 0:
            time_module.sleep(retry_delay_seconds)
    return []


def collect_websocket_quote_records(
    *,
    args: argparse.Namespace,
    candidates: list[dict[str, Any]],
    feed: str,
) -> list[dict[str, Any]]:
    if args.quote_capture_seconds <= 0 or not candidates:
        return []
    return request_option_quote_capture(
        candidates=candidates,
        feed=feed,
        duration_seconds=float(args.quote_capture_seconds),
        data_base_url=getattr(args, "data_base_url", None),
    )

def print_cycle_summary(
    *,
    generated_at: str,
    label: str,
    board_candidates: list[dict[str, Any]],
    watchlist_candidates: list[dict[str, Any]],
    events: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    failures: list[UniverseScanFailure],
    quote_event_count: int,
    auto_execution: dict[str, Any] | None,
) -> None:
    print(f"[{generated_at}] {label}")
    print(f"Board entries: {len(board_candidates)}")
    print(f"Watchlist entries: {len(watchlist_candidates)}")
    print(f"Events: {len(events)}")
    print(f"Alerts: {len(alerts)}")
    print(f"Quote events saved: {quote_event_count}")
    if auto_execution is not None:
        print(f"Auto execution: {auto_execution.get('message')}")
    if failures:
        print(f"Failures: {len(failures)}")
    if board_candidates:
        print("Board:")
        for index, candidate in enumerate(board_candidates, start=1):
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
        normalized = slot_at.replace("Z", "+00:00") if slot_at.endswith("Z") else slot_at
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.now(UTC)


def _run_collection_cycle(
    args: argparse.Namespace,
    *,
    tick_context: LiveTickContext | None,
    scanner_args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
    alert_store: AlertRepository,
    collector_store: CollectorRepository,
    calendar_resolver: Any,
    greeks_provider: Any,
    emit_output: bool,
    heartbeat: Callable[[], None] | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    if heartbeat is not None:
        heartbeat()
    symbols, universe_label, scan_results, failures, _ = run_universe_cycle(
        scanner_args=scanner_args,
        client=client,
        calendar_resolver=calendar_resolver,
        greeks_provider=greeks_provider,
        history_store=history_store,
    )
    label = build_live_snapshot_label(
        universe_label=universe_label,
        strategy=args.strategy,
        profile=args.profile,
        greeks_source=args.greeks_source,
    )
    cycle_id = build_cycle_id(label)
    run_ids = {(result.symbol, result.args.strategy): result.run_id for result in scan_results}
    symbol_strategy_candidates = build_symbol_strategy_candidates(
        scan_results,
        run_ids,
        max_per_strategy=WATCHLIST_PER_STRATEGY,
    )
    previous_map, previous_selection_state = read_previous_cycle_state(collector_store, label)
    board_payloads, selection_state = select_board_candidates(
        symbol_candidates=symbol_strategy_candidates,
        previous_board=previous_map,
        previous_state=previous_selection_state,
        top=args.top,
    )
    watchlist_payloads = select_watchlist_candidates(
        symbol_candidates=symbol_strategy_candidates,
        board_candidates=board_payloads,
        top=WATCHLIST_TOP,
    )
    current_map = {payload["underlying_symbol"]: payload for payload in board_payloads}
    events = build_events(
        label=label,
        cycle_id=cycle_id,
        generated_at=generated_at,
        previous=previous_map,
        current=current_map,
    )
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
        selection_state=selection_state,
        board_candidates=board_payloads,
        watchlist_candidates=watchlist_payloads,
        events=events,
    )
    if heartbeat is not None:
        heartbeat()
    auto_execution: dict[str, Any] | None = None
    if tick_context is not None:
        try:
            auto_execution = submit_auto_session_execution(
                db_target=args.history_db,
                session_id=tick_context.session_id,
                cycle_id=cycle_id,
                policy=getattr(args, "execution_policy", None),
                job_run_id=tick_context.job_run_id,
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
    alerts: list[dict[str, Any]] = []
    try:
        alerts = dispatch_cycle_alerts(
            collector_store=collector_store,
            alert_store=alert_store,
            cycle_id=cycle_id,
            label=label,
            generated_at=generated_at,
            strategy_mode=args.strategy,
            profile=args.profile,
            board_candidates=board_payloads,
            events=events,
        )
    except Exception as exc:
        print(f"Alert dispatch unavailable: {exc}")
    quote_event_count = 0
    baseline_quote_event_count = 0
    websocket_quote_event_count = 0
    recovery_quote_event_count = 0
    quote_candidates = board_payloads + watchlist_payloads[:WATCHLIST_QUOTE_CAPTURE_TOP]
    expected_quote_symbols = list(build_quote_symbol_metadata(quote_candidates).keys())
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
        except Exception as exc:
            print(f"Live latest quote capture unavailable: {exc}")
        try:
            websocket_quote_records = collect_websocket_quote_records(
                args=args,
                candidates=quote_candidates,
                feed=scanner_args.feed,
            )
            websocket_quote_event_count = history_store.save_option_quote_events(
                cycle_id=cycle_id,
                label=label,
                profile=args.profile,
                quotes=websocket_quote_records,
            )
            quote_event_count += websocket_quote_event_count
        except Exception as exc:
            print(f"Live websocket quote capture unavailable: {exc}")
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
                recovery_quote_event_count = recovered_quote_event_count
                quote_event_count += recovered_quote_event_count
            except Exception as exc:
                print(f"Live quote recovery unavailable: {exc}")
    quote_capture = build_quote_capture_summary(
        expected_quote_symbols=expected_quote_symbols,
        total_quote_events_saved=quote_event_count,
        baseline_quote_events_saved=baseline_quote_event_count,
        websocket_quote_events_saved=websocket_quote_event_count,
        recovery_quote_events_saved=recovery_quote_event_count,
    )
    if heartbeat is not None:
        heartbeat()
    if emit_output:
        print_cycle_summary(
            generated_at=generated_at,
            label=label,
            board_candidates=board_payloads,
            watchlist_candidates=watchlist_payloads,
            events=events,
            alerts=alerts,
            failures=failures,
            quote_event_count=quote_event_count,
            auto_execution=auto_execution,
        )
    return {
        "cycle_id": cycle_id,
        "generated_at": generated_at,
        "label": label,
        "alerts_sent": len(alerts),
        "quote_events_saved": quote_event_count,
        "baseline_quote_events_saved": baseline_quote_event_count,
        "websocket_quote_events_saved": websocket_quote_event_count,
        "recovery_quote_events_saved": recovery_quote_event_count,
        "expected_quote_symbols": expected_quote_symbols,
        "board_candidate_count": len(board_payloads),
        "watchlist_candidate_count": len(watchlist_payloads),
        "quote_capture": quote_capture,
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
        session_start_offset_minutes=int(getattr(args, "session_start_offset_minutes", 0)),
        session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
    ):
        if emit_output:
            print("Scheduled slot is outside the collection window. Skipping.")
        return {
            "status": "skipped",
            "reason": "market_closed",
            "iterations_completed": 0,
            "cycle_ids": [],
            "alerts_sent": 0,
            "quote_events_saved": 0,
            "baseline_quote_events_saved": 0,
            "websocket_quote_events_saved": 0,
            "recovery_quote_events_saved": 0,
            "expected_quote_symbols": [],
            "quote_capture": build_quote_capture_summary(
                expected_quote_symbols=[],
                total_quote_events_saved=0,
                baseline_quote_events_saved=0,
                websocket_quote_events_saved=0,
                recovery_quote_events_saved=0,
            ),
            "session_id": tick_context.session_id,
            "slot_at": tick_context.slot_at,
        }

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, scanner_args.trading_base_url),
        data_base_url=scanner_args.data_base_url,
    )
    history_store = build_history_store(args.history_db)
    alert_store = build_alert_repository(args.history_db)
    collector_store = build_collector_repository(args.history_db)
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
            history_store=history_store,
            alert_store=alert_store,
            collector_store=collector_store,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            emit_output=emit_output,
            heartbeat=heartbeat,
        )
    finally:
        alert_store.close()
        collector_store.close()
        history_store.close()
        calendar_resolver.store.close()

    return {
        "status": "completed",
        "iterations_completed": 1,
        "cycle_ids": [cycle_result["cycle_id"]],
        "alerts_sent": cycle_result["alerts_sent"],
        "quote_events_saved": cycle_result["quote_events_saved"],
        "baseline_quote_events_saved": cycle_result["baseline_quote_events_saved"],
        "websocket_quote_events_saved": cycle_result["websocket_quote_events_saved"],
        "recovery_quote_events_saved": cycle_result["recovery_quote_events_saved"],
        "expected_quote_symbols": list(cycle_result["expected_quote_symbols"]),
        "board_candidate_count": cycle_result["board_candidate_count"],
        "watchlist_candidate_count": cycle_result["watchlist_candidate_count"],
        "quote_capture": dict(cycle_result["quote_capture"]),
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
        session_start_offset_minutes=int(getattr(args, "session_start_offset_minutes", 0)),
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
            "websocket_quote_events_saved": 0,
            "recovery_quote_events_saved": 0,
            "expected_quote_symbols": [],
            "quote_capture": build_quote_capture_summary(
                expected_quote_symbols=[],
                total_quote_events_saved=0,
                baseline_quote_events_saved=0,
                websocket_quote_events_saved=0,
                recovery_quote_events_saved=0,
            ),
        }

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, scanner_args.trading_base_url),
        data_base_url=scanner_args.data_base_url,
    )
    history_store = build_history_store(args.history_db)
    alert_store = build_alert_repository(args.history_db)
    collector_store = build_collector_repository(args.history_db)
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
    total_websocket_quote_events = 0
    total_recovery_quote_events = 0
    last_label: str | None = None
    iterations_completed = 0
    try:
        for iteration in range(args.iterations):
            iteration_started_at = time_module.monotonic()
            if heartbeat is not None:
                heartbeat()
            if not args.allow_off_hours and not collection_window_is_open(
                now=datetime.now(UTC),
                session_start_offset_minutes=int(getattr(args, "session_start_offset_minutes", 0)),
                session_end_offset_minutes=int(getattr(args, "session_end_offset_minutes", 0)),
            ):
                if emit_output:
                    print("Market closed during collection window. Stopping.")
                break
            cycle_result = _run_collection_cycle(
                args,
                tick_context=None,
                scanner_args=scanner_args,
                client=client,
                history_store=history_store,
                alert_store=alert_store,
                collector_store=collector_store,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                emit_output=emit_output,
                heartbeat=heartbeat,
            )
            cycle_ids.append(cycle_result["cycle_id"])
            total_alerts += int(cycle_result["alerts_sent"])
            total_quote_events += int(cycle_result["quote_events_saved"])
            total_baseline_quote_events += int(cycle_result["baseline_quote_events_saved"])
            total_websocket_quote_events += int(cycle_result["websocket_quote_events_saved"])
            total_recovery_quote_events += int(cycle_result["recovery_quote_events_saved"])
            iterations_completed += 1
            last_label = str(cycle_result["label"])
            if iteration < args.iterations - 1:
                elapsed_seconds = time_module.monotonic() - iteration_started_at
                sleep_seconds = max(float(max(args.interval_seconds, 1)) - elapsed_seconds, 0.0)
                if sleep_seconds > 0:
                    time_module.sleep(sleep_seconds)
                if heartbeat is not None:
                    heartbeat()
    finally:
        alert_store.close()
        collector_store.close()
        history_store.close()
        calendar_resolver.store.close()

    return {
        "status": "completed",
        "iterations_completed": iterations_completed,
        "cycle_ids": cycle_ids,
        "alerts_sent": total_alerts,
        "quote_events_saved": total_quote_events,
        "baseline_quote_events_saved": total_baseline_quote_events,
        "websocket_quote_events_saved": total_websocket_quote_events,
        "recovery_quote_events_saved": total_recovery_quote_events,
        "quote_capture": build_quote_capture_summary(
            expected_quote_symbols=[],
            total_quote_events_saved=total_quote_events,
            baseline_quote_events_saved=total_baseline_quote_events,
            websocket_quote_events_saved=total_websocket_quote_events,
            recovery_quote_events_saved=total_recovery_quote_events,
        ),
        "label": last_label,
    }


def main() -> int:
    load_local_env()
    args = parse_args()
    run_collection(args, emit_output=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
