from __future__ import annotations

from collections import Counter, defaultdict
from statistics import mean
from typing import Any, Mapping

from core.db.decorators import with_storage
from core.services.selection_summary import live_selection_counts
from core.storage.collector_repository import CollectorRepository
from core.storage.run_history_repository import RunHistoryRepository

from .outcomes import build_session_outcomes
from .tuning import build_signal_tuning

MAX_EVENTS = 5000


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


def summarize_runs(
    rows: list[Mapping[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["symbol"]), str(row["strategy"]))].append(row)

    summaries: dict[tuple[str, str], dict[str, Any]] = {}
    nonzero_rows = [
        row
        for row in rows
        if int(row["candidate_count"]) > 0 and row["quality_score"] is not None
    ]
    overview = {
        "run_count": len(rows),
        "candidate_run_count": len(nonzero_rows),
        "first_run_at": None if not rows else rows[0]["generated_at"],
        "last_run_at": None if not rows else rows[-1]["generated_at"],
    }

    for key, group in grouped.items():
        candidate_rows = [
            row
            for row in group
            if int(row["candidate_count"]) > 0 and row["quality_score"] is not None
        ]
        best_row = (
            None
            if not candidate_rows
            else max(candidate_rows, key=lambda row: float(row["quality_score"]))
        )
        latest_row = group[-1]
        latest_setup = parse_setup_json(latest_row["setup_json"])
        summaries[key] = {
            "symbol": key[0],
            "strategy": key[1],
            "run_count": len(group),
            "idea_count": len(candidate_rows),
            "first_seen": group[0]["generated_at"],
            "last_seen": group[-1]["generated_at"],
            "avg_best_score": (
                None
                if not candidate_rows
                else mean(float(row["quality_score"]) for row in candidate_rows)
            ),
            "best_row": best_row,
            "latest_idea_row": None if not candidate_rows else candidate_rows[-1],
            "latest_row": latest_row,
            "latest_setup": latest_setup,
        }
    return summaries, overview


def summarize_quotes(
    rows: list[Mapping[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    by_symbol_strategy: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(
        list
    )
    by_option_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol_strategy[
            (str(row["underlying_symbol"]), str(row["strategy"]))
        ].append(row)
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


@with_storage()
def build_session_summary(
    *,
    db_target: str,
    session_date: str,
    label: str,
    profit_target: float,
    stop_multiple: float,
    storage: Any | None = None,
) -> dict[str, Any]:
    history_store: RunHistoryRepository = storage.history
    collector_store: CollectorRepository = storage.collector

    run_rows = history_store.list_session_top_runs(
        session_date=session_date,
        session_label=label,
    )
    quote_rows = history_store.list_session_quote_events(
        session_date=session_date,
        label=label,
    )
    cycles = collector_store.list_cycles(label, session_date=session_date, limit=5000)
    events = collector_store.list_events(
        label,
        session_date,
        limit=MAX_EVENTS,
        ascending=True,
    )
    latest_cycle = cycles[0] if cycles else None
    latest_cycle_payload = None
    if latest_cycle is not None:
        opportunities = collector_store.list_cycle_candidates(latest_cycle["cycle_id"])
        latest_cycle_payload = {
            **latest_cycle,
            "opportunities": list(opportunities),
            "selection_counts": live_selection_counts(opportunities),
        }
    outcomes = build_session_outcomes(
        history_store=history_store,
        collector_store=collector_store,
        session_date=session_date,
        label=label,
        profit_target=profit_target,
        stop_multiple=stop_multiple,
    )
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
        "tuning": build_signal_tuning(outcomes),
    }
