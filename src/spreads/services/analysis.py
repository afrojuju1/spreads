#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from statistics import mean
from typing import Any, Mapping

from spreads.common import env_or_die, load_local_env
from spreads.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from spreads.services.replay import summarize_replay
from spreads.services.scanner import NEW_YORK
from spreads.storage import (
    build_collector_repository,
    build_history_store,
    default_database_url,
)
from spreads.storage.collector_repository import CollectorRepository
from spreads.storage.run_history_repository import RunHistoryRepository

MAX_EVENTS = 5000
SCORE_BUCKETS = (
    (85.0, "85+"),
    (75.0, "75-84"),
    (65.0, "65-74"),
    (55.0, "55-64"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize persisted intraday collector sessions and replay spread outcomes."
    )
    parser.add_argument(
        "--db",
        default=default_database_url(),
        help="Database target. Default: SPREADS_DATABASE_URL / DATABASE_URL / local Docker Postgres.",
    )
    parser.add_argument(
        "--date",
        default="today",
        help="Session date in YYYY-MM-DD format using the New York trading date. Default: today",
    )
    parser.add_argument(
        "--label",
        default="explore_10_combined_0dte_auto",
        help="Collector label to summarize.",
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
        return datetime.now(NEW_YORK).date().isoformat()
    return value


def parse_setup_json(value: dict[str, Any] | str | None) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    try:
        import json

        return json.loads(value)
    except Exception:
        return None


def summarize_runs(rows: list[Mapping[str, Any]]) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["symbol"]), str(row["strategy"]))].append(row)

    summaries: dict[tuple[str, str], dict[str, Any]] = {}
    nonzero_rows = [row for row in rows if int(row["candidate_count"]) > 0 and row["quality_score"] is not None]
    overview = {
        "run_count": len(rows),
        "candidate_run_count": len(nonzero_rows),
        "first_run_at": None if not rows else rows[0]["generated_at"],
        "last_run_at": None if not rows else rows[-1]["generated_at"],
    }

    for key, group in grouped.items():
        candidate_rows = [row for row in group if int(row["candidate_count"]) > 0 and row["quality_score"] is not None]
        best_row = None if not candidate_rows else max(candidate_rows, key=lambda row: float(row["quality_score"]))
        latest_row = group[-1]
        latest_setup = parse_setup_json(latest_row["setup_json"])
        summaries[key] = {
            "symbol": key[0],
            "strategy": key[1],
            "run_count": len(group),
            "idea_count": len(candidate_rows),
            "first_seen": group[0]["generated_at"],
            "last_seen": group[-1]["generated_at"],
            "avg_best_score": None if not candidate_rows else mean(float(row["quality_score"]) for row in candidate_rows),
            "best_row": best_row,
            "latest_idea_row": None if not candidate_rows else candidate_rows[-1],
            "latest_row": latest_row,
            "latest_setup": latest_setup,
        }
    return summaries, overview


def summarize_quotes(
    rows: list[Mapping[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    by_symbol_strategy: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    by_option_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol_strategy[(str(row["underlying_symbol"]), str(row["strategy"]))].append(row)
        by_option_symbol[str(row["option_symbol"])].append(row)

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
        midpoints = [float(row["midpoint"]) for row in group]
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


def summarize_events(events: list[Mapping[str, Any]]) -> dict[str, Any]:
    type_counts = Counter(str(event["event_type"]) for event in events)
    symbol_counts = Counter(str(event["symbol"]) for event in events)
    return {
        "event_count": len(events),
        "by_type": dict(sorted(type_counts.items())),
        "most_active_symbols": dict(symbol_counts.most_common(10)),
        "recent_events": [dict(event) for event in events[-10:]],
    }


def build_symbol_breakdown(
    run_summaries: dict[tuple[str, str], dict[str, Any]],
    quote_summaries: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(run_summaries):
        summary = run_summaries[key]
        best_row = summary["best_row"]
        latest_setup = summary["latest_setup"] or {}
        row = {
            "symbol": summary["symbol"],
            "strategy": summary["strategy"],
            "run_count": summary["run_count"],
            "idea_count": summary["idea_count"],
            "first_seen": summary["first_seen"],
            "last_seen": summary["last_seen"],
            "avg_best_score": summary["avg_best_score"],
            "best_idea": None
            if best_row is None
            else {
                "short_strike": best_row["short_strike"],
                "long_strike": best_row["long_strike"],
                "quality_score": best_row["quality_score"],
                "midpoint_credit": best_row["midpoint_credit"],
                "generated_at": best_row["generated_at"],
                "calendar_status": best_row["calendar_status"],
            },
            "latest_setup": latest_setup,
            "quote_coverage": quote_summaries.get(key),
        }
        rows.append(row)
    return rows


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


def candidate_identity(candidate: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(candidate["underlying_symbol"]),
        str(candidate["strategy"]),
        str(candidate["expiration_date"]),
        str(candidate["short_symbol"]),
        str(candidate["long_symbol"]),
    )


def score_bucket_label(score: float | None) -> str:
    if score is None:
        return "unknown"
    for threshold, label in SCORE_BUCKETS:
        if score >= threshold:
            return label
    return "<55"


def build_session_outcomes(
    *,
    history_store: RunHistoryRepository,
    collector_store: CollectorRepository,
    session_date: str,
    label: str,
    profit_target: float,
    stop_multiple: float,
) -> dict[str, Any]:
    session_candidates = collector_store.list_session_candidates(label=label, session_date=session_date)

    grouped: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in session_candidates:
        key = candidate_identity(row)
        state = grouped.setdefault(
            key,
            {
                "identity": {
                    "underlying_symbol": row["underlying_symbol"],
                    "strategy": row["strategy"],
                    "expiration_date": row["expiration_date"],
                    "short_symbol": row["short_symbol"],
                    "long_symbol": row["long_symbol"],
                },
                "first_seen": row["generated_at"],
                "latest_seen": row["generated_at"],
                "first_board": None,
                "first_watchlist": None,
                "latest": row,
                "occurrence_count": 0,
            },
        )
        state["occurrence_count"] += 1
        state["latest_seen"] = row["generated_at"]
        state["latest"] = row
        if row["bucket"] == "board" and state["first_board"] is None:
            state["first_board"] = row
        if row["bucket"] == "watchlist" and state["first_watchlist"] is None:
            state["first_watchlist"] = row

    try:
        client = build_replay_client()
        replay_client_error = None
    except Exception as exc:
        client = None
        replay_client_error = str(exc)

    run_cache: dict[str, tuple[Mapping[str, Any] | None, list[Mapping[str, Any]]]] = {}
    bars_cache: dict[tuple[str, str, str, str], Any] = {}
    option_bars_cache: dict[tuple[tuple[str, ...], str, str], Any] = {}

    def find_matching_candidate(
        candidates: list[Mapping[str, Any]],
        entry_candidate: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        for candidate in candidates:
            if (
                candidate["strategy"] == entry_candidate["strategy"]
                and candidate["expiration_date"] == entry_candidate["expiration_date"]
                and candidate["short_symbol"] == entry_candidate["short_symbol"]
                and candidate["long_symbol"] == entry_candidate["long_symbol"]
            ):
                return candidate
        return None

    def replay_outcome(entry_row: Mapping[str, Any]) -> dict[str, Any]:
        if client is None:
            return {
                "status": "unavailable",
                "reason": replay_client_error,
                "verdict": "replay unavailable",
                "outcome_bucket": "unavailable",
                "still_in_play": False,
                "estimated_close_pnl": None,
                "estimated_expiry_pnl": None,
                "profit_target_hit": False,
                "stop_hit": False,
            }

        run_id = str(entry_row["run_id"])
        cached = run_cache.get(run_id)
        if cached is None:
            run_payload = history_store.get_run(run_id)
            candidates = history_store.list_candidates(run_id)
            cached = (run_payload, candidates)
            run_cache[run_id] = cached
        run_payload, stored_candidates = cached
        if run_payload is None:
            return {
                "status": "missing_run",
                "verdict": "stored run missing",
                "outcome_bucket": "unavailable",
                "still_in_play": False,
                "estimated_close_pnl": None,
                "estimated_expiry_pnl": None,
                "profit_target_hit": False,
                "stop_hit": False,
            }

        target_candidate = find_matching_candidate(stored_candidates, entry_row)
        if target_candidate is None:
            return {
                "status": "missing_candidate",
                "verdict": "stored candidate missing",
                "outcome_bucket": "unavailable",
                "still_in_play": False,
                "estimated_close_pnl": None,
                "estimated_expiry_pnl": None,
                "profit_target_hit": False,
                "stop_hit": False,
            }

        generated_at = datetime.fromisoformat(str(run_payload["generated_at"]).replace("Z", "+00:00"))
        run_date = generated_at.astimezone(NEW_YORK).date()
        expiry_date = date.fromisoformat(str(target_candidate["expiration_date"]))
        replay_end = max(run_date + timedelta(days=3), expiry_date)
        stock_feed = str(run_payload["filters"].get("stock_feed", "sip"))

        bars_key = (str(run_payload["symbol"]), run_date.isoformat(), replay_end.isoformat(), stock_feed)
        if bars_key not in bars_cache:
            bars_cache[bars_key] = client.get_daily_bars(
                str(run_payload["symbol"]),
                start=(run_date - timedelta(days=2)).isoformat(),
                end=replay_end.isoformat(),
                stock_feed=stock_feed,
            )

        option_symbols = tuple(sorted({str(target_candidate["short_symbol"]), str(target_candidate["long_symbol"])}))
        option_bars_key = (option_symbols, run_date.isoformat(), replay_end.isoformat())
        if option_bars_key not in option_bars_cache:
            option_bars_cache[option_bars_key] = client.get_option_bars(
                list(option_symbols),
                start=run_date.isoformat(),
                end=replay_end.isoformat(),
            )

        _, replay_rows = summarize_replay(
            run_payload=run_payload,
            candidates=[target_candidate],
            bars=bars_cache[bars_key],
            option_bars=option_bars_cache[option_bars_key],
            profit_target=profit_target,
            stop_multiple=stop_multiple,
        )
        rows_by_horizon = {
            row["horizon"]: row
            for row in replay_rows
            if row.get("status") == "available"
        }
        entry_horizon = rows_by_horizon.get("entry")
        expiry_horizon = rows_by_horizon.get("expiry")

        if expiry_horizon is not None:
            expiry_pnl = expiry_horizon.get("estimated_pnl")
            if expiry_pnl is not None and expiry_pnl > 0:
                verdict = "profitable by expiry"
                outcome_bucket = "win"
            elif expiry_horizon.get("estimated_stop_hit"):
                verdict = "stop-loss outcome by expiry"
                outcome_bucket = "loss"
            elif expiry_horizon.get("closed_past_breakeven") or (expiry_pnl is not None and expiry_pnl < 0):
                verdict = "loss by expiry"
                outcome_bucket = "loss"
            else:
                verdict = "expired but unresolved"
                outcome_bucket = "loss"
            return {
                "status": "available",
                "verdict": verdict,
                "outcome_bucket": outcome_bucket,
                "still_in_play": False,
                "estimated_close_pnl": None if entry_horizon is None else entry_horizon.get("estimated_pnl"),
                "estimated_expiry_pnl": expiry_pnl,
                "profit_target_hit": bool(expiry_horizon.get("estimated_profit_target_hit")),
                "stop_hit": bool(expiry_horizon.get("estimated_stop_hit")),
                "entry_row": entry_horizon,
                "expiry_row": expiry_horizon,
            }

        if entry_horizon is not None:
            if entry_horizon.get("closed_past_breakeven"):
                verdict = "in danger at close"
            elif entry_horizon.get("closed_past_short_strike"):
                verdict = "tested at close but still live"
            elif entry_horizon.get("estimated_pnl") is not None and entry_horizon.get("estimated_pnl", 0) > 0:
                verdict = "up and still in play at close"
            else:
                verdict = "down but still in play at close"
            return {
                "status": "available",
                "verdict": verdict,
                "outcome_bucket": "still_open",
                "still_in_play": True,
                "estimated_close_pnl": entry_horizon.get("estimated_pnl"),
                "estimated_expiry_pnl": None,
                "profit_target_hit": bool(entry_horizon.get("estimated_profit_target_hit")),
                "stop_hit": bool(entry_horizon.get("estimated_stop_hit")),
                "entry_row": entry_horizon,
                "expiry_row": None,
            }

        return {
            "status": "pending",
            "verdict": "replay data pending",
            "outcome_bucket": "still_open",
            "still_in_play": True,
            "estimated_close_pnl": None,
            "estimated_expiry_pnl": None,
            "profit_target_hit": False,
            "stop_hit": False,
            "entry_row": None,
            "expiry_row": None,
        }

    ideas: list[dict[str, Any]] = []
    for state in grouped.values():
        entry = state["first_board"] or state["first_watchlist"]
        latest = state["latest"]
        classification = "board" if state["first_board"] is not None else "watchlist"
        outcome = replay_outcome(entry)
        ideas.append(
            {
                **state["identity"],
                "classification": classification,
                "first_seen": entry["generated_at"],
                "latest_seen": latest["generated_at"],
                "entry_run_id": entry["run_id"],
                "entry_cycle_id": entry["cycle_id"],
                "latest_score": latest["quality_score"],
                "score_bucket": score_bucket_label(float(latest["quality_score"])),
                "occurrence_count": state["occurrence_count"],
                "replay_status": outcome["status"],
                "replay_verdict": outcome["verdict"],
                "outcome_bucket": outcome["outcome_bucket"],
                "estimated_close_pnl": outcome["estimated_close_pnl"],
                "estimated_expiry_pnl": outcome["estimated_expiry_pnl"],
                "profit_target_hit": outcome["profit_target_hit"],
                "stop_hit": outcome["stop_hit"],
                "still_in_play": outcome["still_in_play"],
                "entry_candidate": dict(entry["candidate"]),
                "latest_candidate": dict(latest["candidate"]),
            }
        )

    ideas.sort(
        key=lambda item: (
            0 if item["classification"] == "board" else 1,
            -float(item["latest_score"]),
            item["first_seen"],
        )
    )

    counts_by_bucket = Counter(item["classification"] for item in ideas)
    outcome_counts_by_bucket: dict[str, dict[str, int]] = {}
    average_estimated_pnl_by_bucket: dict[str, float | None] = {}
    for bucket in ("board", "watchlist"):
        bucket_items = [item for item in ideas if item["classification"] == bucket]
        outcome_counts_by_bucket[bucket] = dict(Counter(item["outcome_bucket"] for item in bucket_items))
        pnl_values = [
            item["estimated_expiry_pnl"] if item["estimated_expiry_pnl"] is not None else item["estimated_close_pnl"]
            for item in bucket_items
            if (item["estimated_expiry_pnl"] if item["estimated_expiry_pnl"] is not None else item["estimated_close_pnl"])
            is not None
        ]
        average_estimated_pnl_by_bucket[bucket] = None if not pnl_values else mean(float(value) for value in pnl_values)

    def aggregate_by(field: str) -> dict[str, dict[str, Any]]:
        grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in ideas:
            grouped_rows[str(item[field])].append(item)
        output: dict[str, dict[str, Any]] = {}
        for key, rows in grouped_rows.items():
            output[key] = {
                "count": len(rows),
                "board_count": sum(1 for row in rows if row["classification"] == "board"),
                "watchlist_count": sum(1 for row in rows if row["classification"] == "watchlist"),
                "outcomes": dict(Counter(row["outcome_bucket"] for row in rows)),
                "average_latest_score": mean(float(row["latest_score"]) for row in rows),
            }
        return dict(sorted(output.items()))

    return {
        "session_date": session_date,
        "label": label,
        "idea_count": len(ideas),
        "counts_by_bucket": dict(counts_by_bucket),
        "outcome_counts_by_bucket": outcome_counts_by_bucket,
        "average_estimated_pnl_by_bucket": average_estimated_pnl_by_bucket,
        "by_symbol": aggregate_by("underlying_symbol"),
        "by_strategy": aggregate_by("strategy"),
        "by_score_bucket": aggregate_by("score_bucket"),
        "ideas": ideas,
    }


def build_session_summary(
    *,
    db_target: str,
    session_date: str,
    label: str,
    profit_target: float,
    stop_multiple: float,
) -> dict[str, Any]:
    history_store = build_history_store(db_target)
    collector_store = build_collector_repository(db_target)
    try:
        run_rows = history_store.list_session_top_runs(session_date=session_date, session_label=label)
        quote_rows = history_store.list_session_quote_events(session_date=session_date, label=label)
        cycles = collector_store.list_cycles(label, session_date=session_date, limit=5000)
        events = collector_store.list_events(label, session_date, limit=MAX_EVENTS, ascending=True)
        latest_cycle = cycles[0] if cycles else None
        latest_cycle_payload = None
        if latest_cycle is not None:
            board = collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="board")
            watchlist = collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="watchlist")
            latest_cycle_payload = {
                **latest_cycle.to_dict(),
                "board_candidates": [candidate.to_dict() for candidate in board],
                "watchlist_candidates": [candidate.to_dict() for candidate in watchlist],
            }
        outcomes = build_session_outcomes(
            history_store=history_store,
            collector_store=collector_store,
            session_date=session_date,
            label=label,
            profit_target=profit_target,
            stop_multiple=stop_multiple,
        )
    finally:
        collector_store.close()
        history_store.close()

    run_summaries, run_overview = summarize_runs(run_rows)
    quote_summaries, leg_summaries, quote_overview = summarize_quotes(quote_rows)
    event_overview = summarize_events(events)
    symbol_breakdown = build_symbol_breakdown(run_summaries, quote_summaries)

    return {
        "session_date": session_date,
        "label": label,
        "cycle_count": len(cycles),
        "latest_cycle": latest_cycle_payload,
        "run_overview": run_overview,
        "quote_overview": quote_overview,
        "event_overview": event_overview,
        "symbol_breakdown": symbol_breakdown,
        "leg_summaries": leg_summaries[:10],
        "outcomes": outcomes,
    }


def render_event_summary(event_overview: Mapping[str, Any]) -> list[str]:
    if not event_overview["event_count"]:
        return ["No collector events were persisted for this session."]
    lines = [
        f"- Event count: {event_overview['event_count']}",
        "- By type: "
        + ", ".join(
            f"{event_type}={count}"
            for event_type, count in sorted(event_overview["by_type"].items())
        ),
        "- Most active symbols: "
        + ", ".join(
            f"{symbol}={count}"
            for symbol, count in list(event_overview["most_active_symbols"].items())[:5]
        ),
    ]
    recent_side_flips = [
        event
        for event in event_overview["recent_events"]
        if event.get("event_type") == "side_flip"
    ]
    if recent_side_flips:
        lines.append("- Recent side flips:")
        for event in recent_side_flips[-5:]:
            lines.append(f"  - {event['generated_at']} {event['message']}")
    return lines


def render_symbol_summaries(symbol_breakdown: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for summary in symbol_breakdown:
        header = f"### {summary['symbol']} {summary['strategy']}"
        lines.append(header)
        lines.append(
            f"- Runs: {summary['run_count']} | idea runs: {summary['idea_count']} | first: {summary['first_seen']} | last: {summary['last_seen']}"
        )
        best_idea = summary["best_idea"]
        if best_idea is None:
            lines.append("- Best idea: none surfaced in this session")
        else:
            lines.append(
                "- Best idea: "
                f"{best_idea['short_strike']:.2f}/{best_idea['long_strike']:.2f} "
                f"score {best_idea['quality_score']:.1f} mid {best_idea['midpoint_credit']:.2f} "
                f"at {best_idea['generated_at']}"
            )
            lines.append(
                f"- Calendar status: {best_idea['calendar_status']} | average best score: {summary['avg_best_score']:.1f}"
            )
        latest_setup = summary["latest_setup"] or {}
        if latest_setup:
            lines.append(
                "- Latest setup: "
                f"{latest_setup.get('status', 'unknown')} score {latest_setup.get('score', 0):.1f} "
                f"(daily {latest_setup.get('daily_score', 'n/a')}, intraday {latest_setup.get('intraday_score', 'n/a')})"
            )
            reasons = latest_setup.get("reasons") or []
            if reasons:
                lines.append("- Latest setup reasons: " + "; ".join(reasons[:3]))
        quote_coverage = summary.get("quote_coverage")
        if quote_coverage:
            lines.append(
                "- Quote coverage: "
                f"{quote_coverage['quote_events']} quote events across {quote_coverage['unique_legs']} legs "
                f"from {quote_coverage['first_quote_at']} to {quote_coverage['last_quote_at']}"
            )
        lines.append("")
    return lines


def render_leg_summaries(leg_summaries: list[dict[str, Any]]) -> list[str]:
    if not leg_summaries:
        return ["No websocket quote rows were captured for this session."]
    lines = [
        "| Option | Sym | Side | Role | Events | Mid Min | Mid Max | Last Quote |",
        "|---|---|---|---|---:|---:|---:|---|",
    ]
    for item in leg_summaries:
        lines.append(
            f"| {item['option_symbol']} | {item['underlying_symbol']} | {item['strategy']} | {item['leg_role']} | "
            f"{item['event_count']} | {item['midpoint_min']:.2f} | {item['midpoint_max']:.2f} | {item['last_quote_at']} |"
        )
    return lines


def render_outcome_summaries(outcomes: Mapping[str, Any]) -> list[str]:
    ideas = list(outcomes["ideas"])
    if not ideas:
        return ["No persisted board or watchlist ideas were available for this session."]

    lines = [
        f"- Idea count: {outcomes['idea_count']}",
        "- Classification counts: "
        + ", ".join(
            f"{bucket}={count}" for bucket, count in sorted(outcomes["counts_by_bucket"].items())
        ),
        "- Average estimated PnL: "
        + ", ".join(
            f"{bucket}={value:.0f}" if value is not None else f"{bucket}=n/a"
            for bucket, value in sorted(outcomes["average_estimated_pnl_by_bucket"].items())
        ),
        "",
    ]
    for idea in ideas[:20]:
        lines.append(
            f"### {idea['underlying_symbol']} {idea['strategy']} {idea['short_symbol']} / {idea['long_symbol']}"
        )
        lines.append(
            f"- Classification: {idea['classification']} | first seen: {idea['first_seen']} | latest score: {idea['latest_score']:.1f} ({idea['score_bucket']})"
        )
        lines.append(
            f"- Outcome: {idea['replay_verdict']} | bucket: {idea['outcome_bucket']} | still in play: {'yes' if idea['still_in_play'] else 'no'}"
        )
        close_pnl = "n/a" if idea["estimated_close_pnl"] is None else f"{idea['estimated_close_pnl']:.0f}"
        expiry_pnl = "n/a" if idea["estimated_expiry_pnl"] is None else f"{idea['estimated_expiry_pnl']:.0f}"
        lines.append(
            f"- Estimated PnL: close {close_pnl} | expiry {expiry_pnl} | PT {'yes' if idea['profit_target_hit'] else 'no'} | stop {'yes' if idea['stop_hit'] else 'no'}"
        )
        lines.append("")
    return lines


def render_session_summary_markdown(summary: Mapping[str, Any]) -> str:
    run_overview = summary["run_overview"]
    quote_overview = summary["quote_overview"]
    latest_cycle = summary["latest_cycle"]
    lines = [
        f"# Post-Close Analysis: {summary['session_date']}",
        "",
        f"- Label: `{summary['label']}`",
        f"- Collector cycles: {summary['cycle_count']}",
        f"- Scan runs: {run_overview['run_count']} total, {run_overview['candidate_run_count']} with surfaced ideas",
        f"- Scan window: {run_overview['first_run_at']} -> {run_overview['last_run_at']}",
        f"- Quote events: {quote_overview['quote_event_count']} across {quote_overview['tracked_leg_count']} legs",
        f"- Quote window: {quote_overview['first_quote_at']} -> {quote_overview['last_quote_at']}",
    ]
    if latest_cycle is not None:
        lines.extend(
            [
                f"- Latest cycle: {latest_cycle['generated_at']}",
                f"- Latest board/watchlist sizes: {len(latest_cycle['board_candidates'])}/{len(latest_cycle['watchlist_candidates'])}",
            ]
        )
    lines.extend(
        [
            "",
            "## Collector Events",
            "",
            *render_event_summary(summary["event_overview"]),
            "",
            "## Symbol Breakdown",
            "",
            *render_symbol_summaries(summary["symbol_breakdown"]),
            "## Accepted Vs Watchlist Outcomes",
            "",
            *render_outcome_summaries(summary["outcomes"]),
            "## Most Tracked Legs",
            "",
            *render_leg_summaries(summary["leg_summaries"]),
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    session_date = resolve_date(args.date)

    summary = build_session_summary(
        db_target=args.db,
        session_date=session_date,
        label=args.label,
        profit_target=args.replay_profit_target,
        stop_multiple=args.replay_stop_multiple,
    )
    report = render_session_summary_markdown(summary)
    print(report, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
