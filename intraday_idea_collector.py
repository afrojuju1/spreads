#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import time as time_module
from dataclasses import asdict
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

from calendar_events import build_calendar_event_resolver
from credit_spread_scanner import (
    NEW_YORK,
    AlpacaClient,
    AlpacaOptionQuoteStreamer,
    SpreadCandidate,
    SymbolScanResult,
    UniverseScanFailure,
    env_or_die,
    infer_trading_base_url,
    load_local_env,
    merge_strategy_candidates,
    parse_args as parse_scanner_args,
    resolve_symbols,
    scan_symbol_across_strategies,
    sort_candidates_for_display,
)
from greeks import build_local_greeks_provider
from scanner_history import DEFAULT_HISTORY_DB_PATH, RunHistoryStore

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll the options scanner intraday and persist live idea snapshots plus event logs."
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
        "--output-dir",
        default=str(Path("outputs") / "live_ideas"),
        help="Directory for live idea snapshots and logs. Default: outputs/live_ideas",
    )
    parser.add_argument(
        "--history-db",
        default=str(DEFAULT_HISTORY_DB_PATH),
        help=argparse.SUPPRESS,
    )
    return parser.parse_args()


def market_is_open(now: datetime | None = None) -> bool:
    current = datetime.now(NEW_YORK) if now is None else now.astimezone(NEW_YORK)
    if current.weekday() >= 5:
        return False
    current_time = current.time()
    return time(9, 30) <= current_time < time(16, 0)


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
    history_store: RunHistoryStore,
) -> tuple[list[str], str, list[SymbolScanResult], list[UniverseScanFailure], list[SpreadCandidate]]:
    symbols, universe_label = resolve_symbols(scanner_args)
    scanner_args.session_label = snapshot_label(universe_label, scanner_args)
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


def snapshot_label(universe_label: str, args: argparse.Namespace) -> str:
    return f"{universe_label}_{args.strategy}_{args.profile}_{args.greeks_source}".lower()


def serialize_candidate(candidate: SpreadCandidate, run_id: str | None) -> dict[str, Any]:
    payload = asdict(candidate)
    payload["run_id"] = run_id
    return payload


def latest_snapshot_path(output_dir: Path, label: str) -> Path:
    return output_dir / f"latest_{label}.json"


def event_log_path(output_dir: Path, label: str) -> Path:
    return output_dir / f"events_{label}.jsonl"


def read_previous_snapshot(path: Path) -> dict[str, dict[str, Any]]:
    return read_previous_snapshot_state(path)[0]


def read_previous_snapshot_state(path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not path.exists():
        return {}, {}
    payload = json.loads(path.read_text())
    board = payload.get("board_candidates", [])
    previous: dict[str, dict[str, Any]] = {}
    for candidate in board:
        symbol = candidate.get("underlying_symbol")
        if symbol:
            previous[str(symbol)] = candidate
    raw_state = payload.get("selection_state", {})
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


def build_quote_symbol_metadata(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        for leg_role, option_symbol in (
            ("short", candidate["short_symbol"]),
            ("long", candidate["long_symbol"]),
        ):
            metadata[option_symbol] = {
                "underlying_symbol": candidate["underlying_symbol"],
                "strategy": candidate["strategy"],
                "leg_role": leg_role,
            }
    return metadata


def collect_live_quote_records(
    *,
    args: argparse.Namespace,
    client: AlpacaClient,
    candidates: list[dict[str, Any]],
    feed: str,
) -> list[dict[str, Any]]:
    if args.quote_capture_seconds <= 0 or not candidates:
        return []

    stream_symbols = list(build_quote_symbol_metadata(candidates).keys())
    streamer = AlpacaOptionQuoteStreamer(
        key_id=client.headers["APCA-API-KEY-ID"],
        secret_key=client.headers["APCA-API-SECRET-KEY"],
        data_base_url=client.data_base_url,
        feed=feed,
    )
    quote_events = streamer.collect_quote_events(
        stream_symbols,
        duration_seconds=float(args.quote_capture_seconds),
    )
    if not quote_events:
        return []

    captured_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    symbol_metadata = build_quote_symbol_metadata(candidates)
    records: list[dict[str, Any]] = []
    for quote in quote_events:
        metadata = symbol_metadata.get(quote.symbol, {})
        records.append(
            {
                "captured_at": captured_at,
                "underlying_symbol": metadata.get("underlying_symbol"),
                "strategy": metadata.get("strategy"),
                "option_symbol": quote.symbol,
                "leg_role": metadata.get("leg_role", "unknown"),
                "bid": quote.bid,
                "ask": quote.ask,
                "midpoint": quote.midpoint,
                "bid_size": quote.bid_size,
                "ask_size": quote.ask_size,
                "quote_timestamp": quote.timestamp,
                "source": "alpaca_websocket",
            }
        )
    return records


def write_events(path: Path, events: list[dict[str, Any]]) -> None:
    if not events:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")))
            handle.write("\n")


def write_snapshot(
    *,
    output_dir: Path,
    label: str,
    cycle_id: str,
    generated_at: str,
    args: argparse.Namespace,
    symbols: list[str],
    board_candidates: list[dict[str, Any]],
    watchlist_candidates: list[dict[str, Any]],
    selection_state: dict[str, dict[str, Any]],
    failures: list[UniverseScanFailure],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir = output_dir / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshots_dir / f"{cycle_id}.json"
    payload = {
        "cycle_id": cycle_id,
        "generated_at": generated_at,
        "label": label,
        "symbols": symbols,
        "strategy": args.strategy,
        "profile": args.profile,
        "greeks_source": args.greeks_source,
        "board_candidates": board_candidates,
        "watchlist_candidates": watchlist_candidates,
        "selection_state": selection_state,
        "failures": [asdict(failure) for failure in failures],
    }
    snapshot_path.write_text(json.dumps(payload, indent=2))
    shutil.copyfile(snapshot_path, latest_snapshot_path(output_dir, label))
    return snapshot_path


def print_cycle_summary(
    *,
    generated_at: str,
    label: str,
    board_candidates: list[dict[str, Any]],
    watchlist_candidates: list[dict[str, Any]],
    events: list[dict[str, Any]],
    failures: list[UniverseScanFailure],
    snapshot_path: Path,
    quote_event_count: int,
) -> None:
    print(f"[{generated_at}] {label}")
    print(f"Board entries: {len(board_candidates)}")
    print(f"Watchlist entries: {len(watchlist_candidates)}")
    print(f"Events: {len(events)}")
    print(f"Quote events saved: {quote_event_count}")
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
    print(f"Snapshot: {snapshot_path}")
    print()


def main() -> int:
    load_local_env()
    args = parse_args()
    scanner_args = build_scanner_args(args)

    if not args.allow_off_hours and not market_is_open():
        print("Market is closed. Use --allow-off-hours to collect snapshots anyway.")
        return 0

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, scanner_args.trading_base_url),
        data_base_url=scanner_args.data_base_url,
    )
    history_store = RunHistoryStore(args.history_db)
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=scanner_args.data_base_url,
    )
    greeks_provider = build_local_greeks_provider()
    output_dir = Path(args.output_dir)

    try:
        for iteration in range(args.iterations):
            if not args.allow_off_hours and not market_is_open():
                print("Market closed during collection window. Stopping.")
                break

            generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
            symbols, universe_label, scan_results, failures, board_candidates = run_universe_cycle(
                scanner_args=scanner_args,
                client=client,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                history_store=history_store,
            )
            label = snapshot_label(universe_label, args)
            cycle_id = build_cycle_id(label)
            run_ids = {(result.symbol, result.args.strategy): result.run_id for result in scan_results}
            symbol_strategy_candidates = build_symbol_strategy_candidates(
                scan_results,
                run_ids,
                max_per_strategy=WATCHLIST_PER_STRATEGY,
            )
            previous_map, previous_selection_state = read_previous_snapshot_state(
                latest_snapshot_path(output_dir, label)
            )
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
            snapshot_path = write_snapshot(
                output_dir=output_dir,
                label=label,
                cycle_id=cycle_id,
                generated_at=generated_at,
                args=args,
                symbols=symbols,
                board_candidates=board_payloads,
                watchlist_candidates=watchlist_payloads,
                selection_state=selection_state,
                failures=failures,
            )
            write_events(event_log_path(output_dir, label), events)
            quote_event_count = 0
            quote_candidates = board_payloads + watchlist_payloads[:WATCHLIST_QUOTE_CAPTURE_TOP]
            if quote_candidates:
                try:
                    quote_records = collect_live_quote_records(
                        args=args,
                        client=client,
                        candidates=quote_candidates,
                        feed=scanner_args.feed,
                    )
                    quote_event_count = history_store.save_option_quote_events(
                        cycle_id=cycle_id,
                        label=label,
                        profile=args.profile,
                        quotes=quote_records,
                    )
                except Exception as exc:
                    print(f"Live quote capture unavailable: {exc}")
            print_cycle_summary(
                generated_at=generated_at,
                label=label,
                board_candidates=board_payloads,
                watchlist_candidates=watchlist_payloads,
                events=events,
                failures=failures,
                snapshot_path=snapshot_path,
                quote_event_count=quote_event_count,
            )
            if iteration < args.iterations - 1:
                time_module.sleep(max(args.interval_seconds, 1))
    finally:
        history_store.close()
        calendar_resolver.store.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
