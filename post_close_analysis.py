#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any

from credit_spread_scanner import (
    NEW_YORK,
    AlpacaClient,
    env_or_die,
    infer_trading_base_url,
    load_local_env,
    summarize_replay,
)
from scanner_history import DEFAULT_HISTORY_DB_PATH
from scanner_history import RunHistoryStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize intraday scanner runs, board events, and persisted option quote data."
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_HISTORY_DB_PATH),
        help="SQLite history database. Default: outputs/run_history/scanner_history.sqlite",
    )
    parser.add_argument(
        "--date",
        default="today",
        help="Session date in YYYY-MM-DD format. Default: today",
    )
    parser.add_argument(
        "--label",
        default="0dte_core_combined_0dte_auto",
        help="Collector label for live idea events and quote rows.",
    )
    parser.add_argument(
        "--events-log",
        help="Optional explicit JSONL event log path.",
    )
    parser.add_argument(
        "--output",
        help="Optional markdown output path. Default: outputs/analysis/post_close_<date>_<label>.md",
    )
    parser.add_argument(
        "--replay-profit-target",
        type=float,
        default=0.5,
        help="Profit target used for replay verdicts. Default: 0.5",
    )
    parser.add_argument(
        "--replay-stop-multiple",
        type=float,
        default=2.0,
        help="Stop multiple used for replay verdicts. Default: 2.0",
    )
    return parser.parse_args()


def resolve_date(value: str) -> str:
    if value == "today":
        from datetime import date

        return date.today().isoformat()
    return value


def default_output_path(session_date: str, label: str) -> Path:
    safe_label = label.lower().replace(" ", "_")
    return Path("outputs") / "analysis" / f"post_close_{session_date}_{safe_label}.md"


def default_event_log_path(label: str) -> Path:
    return Path("outputs") / "live_ideas" / f"events_{label}.jsonl"


def load_events(path: Path, session_date: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for raw_line in path.read_text().splitlines():
        if not raw_line.strip():
            continue
        payload = json.loads(raw_line)
        generated_at = str(payload.get("generated_at", ""))
        if generated_at.startswith(session_date):
            events.append(payload)
    return events


def load_top_runs(
    conn: sqlite3.Connection,
    session_date: str,
    label: str,
) -> list[sqlite3.Row]:
    run_columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(scan_runs)").fetchall()
    }
    setup_json_select = "r.setup_json" if "setup_json" in run_columns else "NULL AS setup_json"
    session_filter = ""
    parameters: list[str] = [f"{session_date}%"]
    if "session_label" in run_columns:
        session_filter = "AND r.session_label = ?"
        parameters.append(label)
    query = """
        SELECT
            r.run_id,
            r.generated_at,
            r.symbol,
            r.strategy,
            r.profile,
            r.spot_price,
            r.candidate_count,
            r.setup_status,
            r.setup_score,
            {setup_json_select},
            c.short_symbol,
            c.long_symbol,
            c.short_strike,
            c.long_strike,
            c.midpoint_credit,
            c.quality_score,
            c.calendar_status,
            c.expected_move,
            c.short_vs_expected_move
        FROM scan_runs r
        LEFT JOIN scan_candidates c
            ON c.run_id = r.run_id AND c.rank = 1
        WHERE r.generated_at LIKE ?
        {session_filter}
        ORDER BY r.generated_at ASC
    """
    return conn.execute(
        query.format(setup_json_select=setup_json_select, session_filter=session_filter),
        parameters,
    ).fetchall()


def load_quote_rows(conn: sqlite3.Connection, session_date: str, label: str) -> list[sqlite3.Row]:
    query = """
        SELECT *
        FROM option_quote_events
        WHERE captured_at LIKE ? AND label = ?
        ORDER BY quote_id ASC
    """
    return conn.execute(query, (f"{session_date}%", label)).fetchall()


def parse_setup_json(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def summarize_runs(rows: list[sqlite3.Row]) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        grouped[(row["symbol"], row["strategy"])].append(row)

    summaries: dict[tuple[str, str], dict[str, Any]] = {}
    nonzero_rows = [row for row in rows if row["candidate_count"] > 0 and row["quality_score"] is not None]
    overview = {
        "run_count": len(rows),
        "candidate_run_count": len(nonzero_rows),
        "first_run_at": None if not rows else rows[0]["generated_at"],
        "last_run_at": None if not rows else rows[-1]["generated_at"],
    }

    for key, group in grouped.items():
        candidate_rows = [row for row in group if row["candidate_count"] > 0 and row["quality_score"] is not None]
        best_row = None if not candidate_rows else max(candidate_rows, key=lambda row: row["quality_score"])
        latest_row = group[-1]
        latest_setup = parse_setup_json(latest_row["setup_json"])
        summaries[key] = {
            "symbol": key[0],
            "strategy": key[1],
            "run_count": len(group),
            "idea_count": len(candidate_rows),
            "first_seen": group[0]["generated_at"],
            "last_seen": group[-1]["generated_at"],
            "avg_best_score": None if not candidate_rows else mean(row["quality_score"] for row in candidate_rows),
            "best_row": best_row,
            "latest_idea_row": None if not candidate_rows else candidate_rows[-1],
            "latest_row": latest_row,
            "latest_setup": latest_setup,
        }
    return summaries, overview


def summarize_quotes(rows: list[sqlite3.Row]) -> tuple[dict[tuple[str, str], dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    by_symbol_strategy: dict[tuple[str, str], list[sqlite3.Row]] = defaultdict(list)
    by_option_symbol: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        by_symbol_strategy[(row["underlying_symbol"], row["strategy"])].append(row)
        by_option_symbol[row["option_symbol"]].append(row)

    summaries: dict[tuple[str, str], dict[str, Any]] = {}
    for key, group in by_symbol_strategy.items():
        summaries[key] = {
            "quote_events": len(group),
            "unique_legs": len({row["option_symbol"] for row in group}),
            "first_quote_at": group[0]["captured_at"],
            "last_quote_at": group[-1]["captured_at"],
        }

    leg_summaries: list[dict[str, Any]] = []
    for option_symbol, group in by_option_symbol.items():
        midpoints = [row["midpoint"] for row in group]
        leg_summaries.append(
            {
                "option_symbol": option_symbol,
                "underlying_symbol": group[0]["underlying_symbol"],
                "strategy": group[0]["strategy"],
                "leg_role": group[0]["leg_role"],
                "event_count": len(group),
                "first_quote_at": group[0]["captured_at"],
                "last_quote_at": group[-1]["captured_at"],
                "midpoint_min": min(midpoints),
                "midpoint_max": max(midpoints),
            }
        )
    leg_summaries.sort(key=lambda item: item["event_count"], reverse=True)

    overview = {
        "quote_event_count": len(rows),
        "first_quote_at": None if not rows else rows[0]["captured_at"],
        "last_quote_at": None if not rows else rows[-1]["captured_at"],
        "tracked_leg_count": len(by_option_symbol),
    }
    return summaries, leg_summaries, overview


def render_event_summary(events: list[dict[str, Any]]) -> list[str]:
    if not events:
        return ["No board event log was available for this session."]
    type_counts = Counter(str(event.get("event_type", "unknown")) for event in events)
    symbol_counts = Counter(str(event.get("symbol", "unknown")) for event in events)
    lines = [
        f"- Event count: {len(events)}",
        "- By type: "
        + ", ".join(f"{event_type}={count}" for event_type, count in sorted(type_counts.items())),
        "- Most active symbols: "
        + ", ".join(f"{symbol}={count}" for symbol, count in symbol_counts.most_common(5)),
    ]
    side_flips = [event for event in events if event.get("event_type") == "side_flip"]
    if side_flips:
        lines.append("- Side flips:")
        for event in side_flips[-5:]:
            lines.append(f"  - {event['generated_at']} {event['message']}")
    return lines


def render_symbol_summaries(
    run_summaries: dict[tuple[str, str], dict[str, Any]],
    quote_summaries: dict[tuple[str, str], dict[str, Any]],
) -> list[str]:
    lines: list[str] = []
    for key in sorted(run_summaries):
        summary = run_summaries[key]
        best_row = summary["best_row"]
        latest_setup = summary["latest_setup"] or {}
        quote_summary = quote_summaries.get(key)
        header = f"### {summary['symbol']} {summary['strategy']}"
        lines.append(header)
        lines.append(
            f"- Runs: {summary['run_count']} | idea runs: {summary['idea_count']} | first: {summary['first_seen']} | last: {summary['last_seen']}"
        )
        if best_row is None:
            lines.append("- Best idea: none surfaced in this session")
        else:
            lines.append(
                "- Best idea: "
                f"{best_row['short_strike']:.2f}/{best_row['long_strike']:.2f} "
                f"score {best_row['quality_score']:.1f} mid {best_row['midpoint_credit']:.2f} "
                f"at {best_row['generated_at']}"
            )
            lines.append(
                f"- Average best score: {summary['avg_best_score']:.1f} | calendar: {best_row['calendar_status']}"
            )
        if latest_setup:
            lines.append(
                "- Latest setup: "
                f"{latest_setup.get('status', 'unknown')} score {latest_setup.get('score', 0):.1f} "
                f"(daily {latest_setup.get('daily_score', 'n/a')}, intraday {latest_setup.get('intraday_score', 'n/a')})"
            )
            reasons = latest_setup.get("reasons") or []
            if reasons:
                lines.append("- Latest setup reasons: " + "; ".join(reasons[:3]))
        if quote_summary:
            lines.append(
                "- Quote coverage: "
                f"{quote_summary['quote_events']} quote events across {quote_summary['unique_legs']} legs "
                f"from {quote_summary['first_quote_at']} to {quote_summary['last_quote_at']}"
            )
        lines.append("")
    return lines


def render_leg_summaries(leg_summaries: list[dict[str, Any]]) -> list[str]:
    if not leg_summaries:
        return ["No websocket quote rows were captured for this session."]
    lines = ["| Option | Sym | Side | Role | Events | Mid Min | Mid Max | Last Quote |", "|---|---|---|---|---:|---:|---:|---|"]
    for item in leg_summaries[:10]:
        lines.append(
            f"| {item['option_symbol']} | {item['underlying_symbol']} | {item['strategy']} | {item['leg_role']} | "
            f"{item['event_count']} | {item['midpoint_min']:.2f} | {item['midpoint_max']:.2f} | {item['last_quote_at']} |"
        )
    return lines


def build_replay_client() -> AlpacaClient:
    load_local_env()
    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    return AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, None),
        data_base_url="https://data.alpaca.markets",
    )


def load_latest_idea_outcomes(
    *,
    db_path: Path,
    run_summaries: dict[tuple[str, str], dict[str, Any]],
    profit_target: float,
    stop_multiple: float,
) -> dict[tuple[str, str], dict[str, Any]]:
    outcomes: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        client = build_replay_client()
    except Exception as exc:
        for key in run_summaries:
            outcomes[key] = {"status": "unavailable", "reason": str(exc)}
        return outcomes

    history_store = RunHistoryStore(db_path)
    try:
        for key, summary in run_summaries.items():
            latest_idea_row = summary.get("latest_idea_row")
            if latest_idea_row is None:
                outcomes[key] = {"status": "no_idea"}
                continue

            run_id = latest_idea_row["run_id"]
            run_payload = history_store.get_run(run_id)
            candidates = history_store.list_candidates(run_id)
            if not run_payload or not candidates:
                outcomes[key] = {"status": "missing_run"}
                continue

            top_candidate = candidates[:1]
            generated_at = datetime.fromisoformat(run_payload["generated_at"].replace("Z", "+00:00"))
            run_date = generated_at.astimezone(NEW_YORK).date()
            replay_end = max(
                [
                    run_date + timedelta(days=3),
                    *[datetime.fromisoformat(f"{candidate['expiration_date']}T00:00:00+00:00").date() for candidate in top_candidate],
                ]
            )
            bars = client.get_daily_bars(
                run_payload["symbol"],
                start=(run_date - timedelta(days=2)).isoformat(),
                end=replay_end.isoformat(),
                stock_feed=run_payload["filters"].get("stock_feed", "sip"),
            )
            option_symbols = sorted(
                {
                    *[candidate["short_symbol"] for candidate in top_candidate],
                    *[candidate["long_symbol"] for candidate in top_candidate],
                }
            )
            option_bars = client.get_option_bars(
                option_symbols,
                start=run_date.isoformat(),
                end=replay_end.isoformat(),
            )
            replay_summaries, replay_rows = summarize_replay(
                run_payload=run_payload,
                candidates=top_candidate,
                bars=bars,
                option_bars=option_bars,
                profit_target=profit_target,
                stop_multiple=stop_multiple,
            )
            rows_by_horizon = {
                row["horizon"]: row
                for row in replay_rows
                if row.get("status") == "available"
            }
            entry_row = rows_by_horizon.get("entry")
            expiry_row = rows_by_horizon.get("expiry")

            if expiry_row is not None:
                if expiry_row["estimated_pnl"] is not None and expiry_row["estimated_pnl"] > 0:
                    verdict = "profitable by expiry"
                elif expiry_row.get("estimated_stop_hit"):
                    verdict = "stop-loss outcome by expiry"
                elif expiry_row.get("closed_past_breakeven"):
                    verdict = "loss by expiry"
                else:
                    verdict = "expired but unresolved"
                outcomes[key] = {
                    "status": "available",
                    "verdict": verdict,
                    "run_id": run_id,
                    "candidate": top_candidate[0],
                    "entry_row": entry_row,
                    "expiry_row": expiry_row,
                    "replay_summaries": replay_summaries,
                }
                continue

            if entry_row is not None:
                if entry_row.get("closed_past_breakeven"):
                    verdict = "in danger at close"
                elif entry_row.get("closed_past_short_strike"):
                    verdict = "tested at close but still live"
                elif entry_row["estimated_pnl"] is not None and entry_row["estimated_pnl"] > 0:
                    verdict = "up and still in play at close"
                else:
                    verdict = "down but still in play at close"
                outcomes[key] = {
                    "status": "available",
                    "verdict": verdict,
                    "run_id": run_id,
                    "candidate": top_candidate[0],
                    "entry_row": entry_row,
                    "expiry_row": None,
                    "replay_summaries": replay_summaries,
                }
                continue

            outcomes[key] = {
                "status": "pending",
                "run_id": run_id,
                "candidate": top_candidate[0],
                "replay_summaries": replay_summaries,
            }
    finally:
        history_store.close()

    return outcomes


def render_outcome_summaries(outcomes: dict[tuple[str, str], dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for key in sorted(outcomes):
        outcome = outcomes[key]
        symbol, strategy = key
        lines.append(f"### {symbol} {strategy}")
        status = outcome.get("status")
        if status == "no_idea":
            lines.append("- No surfaced idea to evaluate.")
            lines.append("")
            continue
        if status == "unavailable":
            lines.append(f"- Outcome replay unavailable: {outcome.get('reason', 'unknown error')}")
            lines.append("")
            continue
        if status == "missing_run":
            lines.append("- Stored run payload was not available.")
            lines.append("")
            continue
        candidate = outcome.get("candidate", {})
        lines.append(
            f"- Latest idea: {candidate.get('short_strike', 0):.2f}/{candidate.get('long_strike', 0):.2f} "
            f"exp {candidate.get('expiration_date')} score {candidate.get('quality_score', 0):.1f} "
            f"credit {candidate.get('midpoint_credit', 0):.2f}"
        )
        if status == "pending":
            lines.append("- Outcome: replay data is still pending for this idea.")
            lines.append("")
            continue

        lines.append(f"- Outcome: {outcome.get('verdict', 'unknown')}")
        entry_row = outcome.get("entry_row")
        if entry_row is not None:
            lines.append(
                f"- Close state: PnL {entry_row.get('estimated_pnl', 0):.0f}, "
                f"past short = {'yes' if entry_row.get('closed_past_short_strike') else 'no'}, "
                f"past breakeven = {'yes' if entry_row.get('closed_past_breakeven') else 'no'}"
            )
        expiry_row = outcome.get("expiry_row")
        if expiry_row is not None:
            lines.append(
                f"- Expiry state: PnL {expiry_row.get('estimated_pnl', 0):.0f}, "
                f"exit = {expiry_row.get('exit_reason')}, "
                f"PT = {'yes' if expiry_row.get('estimated_profit_target_hit') else 'no'}, "
                f"stop = {'yes' if expiry_row.get('estimated_stop_hit') else 'no'}"
            )
        lines.append("")
    return lines


def build_report(
    *,
    session_date: str,
    label: str,
    run_overview: dict[str, Any],
    quote_overview: dict[str, Any],
    run_summaries: dict[tuple[str, str], dict[str, Any]],
    quote_summaries: dict[tuple[str, str], dict[str, Any]],
    leg_summaries: list[dict[str, Any]],
    events: list[dict[str, Any]],
    outcomes: dict[tuple[str, str], dict[str, Any]],
) -> str:
    lines = [
        f"# Post-Close Analysis: {session_date}",
        "",
        f"- Label: `{label}`",
        f"- Scan runs: {run_overview['run_count']} total, {run_overview['candidate_run_count']} with surfaced ideas",
        f"- Scan window: {run_overview['first_run_at']} -> {run_overview['last_run_at']}",
        f"- Quote events: {quote_overview['quote_event_count']} across {quote_overview['tracked_leg_count']} legs",
        f"- Quote window: {quote_overview['first_quote_at']} -> {quote_overview['last_quote_at']}",
        "",
        "## Board Events",
        "",
        *render_event_summary(events),
        "",
        "## Symbol Breakdown",
        "",
        *render_symbol_summaries(run_summaries, quote_summaries),
        "## Idea Outcomes",
        "",
        *render_outcome_summaries(outcomes),
        "## Most Tracked Legs",
        "",
        *render_leg_summaries(leg_summaries),
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    session_date = resolve_date(args.date)
    db_path = Path(args.db)
    events_path = Path(args.events_log) if args.events_log else default_event_log_path(args.label)
    output_path = Path(args.output) if args.output else default_output_path(session_date, args.label)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        run_rows = load_top_runs(conn, session_date, args.label)
        quote_rows = load_quote_rows(conn, session_date, args.label)
    finally:
        conn.close()

    run_summaries, run_overview = summarize_runs(run_rows)
    quote_summaries, leg_summaries, quote_overview = summarize_quotes(quote_rows)
    events = load_events(events_path, session_date)
    outcomes = load_latest_idea_outcomes(
        db_path=db_path,
        run_summaries=run_summaries,
        profit_target=args.replay_profit_target,
        stop_multiple=args.replay_stop_multiple,
    )
    report = build_report(
        session_date=session_date,
        label=args.label,
        run_overview=run_overview,
        quote_overview=quote_overview,
        run_summaries=run_summaries,
        quote_summaries=quote_summaries,
        leg_summaries=leg_summaries,
        events=events,
        outcomes=outcomes,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
