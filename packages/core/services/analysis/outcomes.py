from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from statistics import mean
from typing import Any, Mapping

from core.common import env_or_die, load_local_env
from core.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from core.services.analysis_helpers import (
    candidate_identity,
    candidate_session_phase,
    score_bucket_label,
)
from core.services.market_dates import NEW_YORK
from core.services.scanners.service import summarize_replay
from core.services.selection_terms import (
    MONITOR_SELECTION_STATE,
    PROMOTABLE_SELECTION_STATE,
    normalize_selection_state,
    selection_state_counts,
    selection_state_rank,
)
from core.storage.collector_repository import CollectorRepository
from core.storage.run_history_repository import RunHistoryRepository

from .tuning import (
    classify_opening_range_regime,
    classify_session_extreme_regime,
    classify_trend_regime,
    classify_vwap_regime,
)


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


def build_session_outcomes(
    *,
    history_store: RunHistoryRepository,
    collector_store: CollectorRepository,
    session_date: str,
    label: str,
    profit_target: float,
    stop_multiple: float,
) -> dict[str, Any]:
    session_candidates = collector_store.list_session_candidates(
        label=label,
        session_date=session_date,
    )

    grouped: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in session_candidates:
        selection_state = normalize_selection_state(
            row.get("selection_state", row.get("bucket"))
        )
        if selection_state is None:
            continue
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
                "first_promotable": None,
                "first_monitor": None,
                "latest": row,
                "occurrence_count": 0,
            },
        )
        state["occurrence_count"] += 1
        state["latest_seen"] = row["generated_at"]
        state["latest"] = row
        if (
            selection_state == PROMOTABLE_SELECTION_STATE
            and state["first_promotable"] is None
        ):
            state["first_promotable"] = row
        if (
            selection_state == MONITOR_SELECTION_STATE
            and state["first_monitor"] is None
        ):
            state["first_monitor"] = row

    try:
        client = build_replay_client()
        replay_client_error = None
    except Exception as exc:
        client = None
        replay_client_error = str(exc)

    run_cache: dict[str, tuple[Mapping[str, Any] | None, list[Mapping[str, Any]]]] = {}
    bars_cache: dict[tuple[str, str, str, str], Any] = {}
    option_bars_cache: dict[tuple[tuple[str, ...], str, str], Any] = {}

    def load_run_bundle(
        run_id: str,
    ) -> tuple[Mapping[str, Any] | None, list[Mapping[str, Any]]]:
        cached = run_cache.get(run_id)
        if cached is None:
            run_payload = history_store.get_run(run_id)
            candidates = history_store.list_candidates(run_id)
            cached = (run_payload, candidates)
            run_cache[run_id] = cached
        return cached

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
        run_payload, stored_candidates = load_run_bundle(run_id)
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

        generated_at = datetime.fromisoformat(
            str(run_payload["generated_at"]).replace("Z", "+00:00")
        )
        run_date = generated_at.astimezone(NEW_YORK).date()
        expiry_date = date.fromisoformat(str(target_candidate["expiration_date"]))
        replay_end = max(run_date + timedelta(days=3), expiry_date)
        stock_feed = str(run_payload["filters"].get("stock_feed", "sip"))

        bars_key = (
            str(run_payload["symbol"]),
            run_date.isoformat(),
            replay_end.isoformat(),
            stock_feed,
        )
        if bars_key not in bars_cache:
            bars_cache[bars_key] = client.get_daily_bars(
                str(run_payload["symbol"]),
                start=(run_date - timedelta(days=2)).isoformat(),
                end=replay_end.isoformat(),
                stock_feed=stock_feed,
            )

        option_symbols = tuple(
            sorted(
                {
                    str(target_candidate["short_symbol"]),
                    str(target_candidate["long_symbol"]),
                }
            )
        )
        option_bars_key = (
            option_symbols,
            run_date.isoformat(),
            replay_end.isoformat(),
        )
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
            elif expiry_horizon.get("closed_past_breakeven") or (
                expiry_pnl is not None and expiry_pnl < 0
            ):
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
                "estimated_close_pnl": (
                    None
                    if entry_horizon is None
                    else entry_horizon.get("estimated_pnl")
                ),
                "estimated_expiry_pnl": expiry_pnl,
                "profit_target_hit": bool(
                    expiry_horizon.get("estimated_profit_target_hit")
                ),
                "stop_hit": bool(expiry_horizon.get("estimated_stop_hit")),
                "entry_row": entry_horizon,
                "expiry_row": expiry_horizon,
            }

        if entry_horizon is not None:
            if entry_horizon.get("closed_past_breakeven"):
                verdict = "in danger at close"
            elif entry_horizon.get("closed_past_short_strike"):
                verdict = "tested at close but still live"
            elif (
                entry_horizon.get("estimated_pnl") is not None
                and entry_horizon.get("estimated_pnl", 0) > 0
            ):
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
                "profit_target_hit": bool(
                    entry_horizon.get("estimated_profit_target_hit")
                ),
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
        entry = state["first_promotable"] or state["first_monitor"]
        latest = state["latest"]
        selection_state = (
            PROMOTABLE_SELECTION_STATE
            if state["first_promotable"] is not None
            else MONITOR_SELECTION_STATE
        )
        outcome = replay_outcome(entry)
        entry_run_payload, _ = load_run_bundle(str(entry["run_id"]))
        entry_setup = None if entry_run_payload is None else (entry_run_payload.get("setup") or {})
        entry_candidate = dict(entry["candidate"])
        latest_candidate = dict(latest["candidate"])
        setup_status = (
            entry_candidate.get("setup_status")
            or (entry_setup.get("status") if entry_setup else None)
            or "unknown"
        )
        calendar_status = entry_candidate.get("calendar_status") or "unknown"
        greeks_source = entry_candidate.get("greeks_source") or "unknown"
        ideas.append(
            {
                **state["identity"],
                "selection_state": selection_state,
                "first_seen": state["first_seen"],
                "entry_seen": entry["generated_at"],
                "latest_seen": latest["generated_at"],
                "entry_run_id": entry["run_id"],
                "entry_cycle_id": entry["cycle_id"],
                "first_promotable_seen": (
                    None
                    if state["first_promotable"] is None
                    else state["first_promotable"]["generated_at"]
                ),
                "first_monitor_seen": (
                    None
                    if state["first_monitor"] is None
                    else state["first_monitor"]["generated_at"]
                ),
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
                "entry_candidate": entry_candidate,
                "latest_candidate": latest_candidate,
                "entry_setup": entry_setup,
                "setup_status": setup_status,
                "calendar_status": calendar_status,
                "greeks_source": greeks_source,
                "session_phase": candidate_session_phase(entry_candidate),
                "vwap_regime": classify_vwap_regime(
                    entry_setup,
                    str(entry["strategy"]),
                ),
                "trend_regime": classify_trend_regime(
                    entry_setup,
                    str(entry["strategy"]),
                ),
                "opening_range_regime": classify_opening_range_regime(
                    entry_setup,
                    str(entry["strategy"]),
                ),
                "session_extreme_regime": classify_session_extreme_regime(
                    entry_setup
                ),
            }
        )

    ideas.sort(
        key=lambda item: (
            selection_state_rank(item.get("selection_state")),
            -float(item["latest_score"]),
            item["first_seen"],
        )
    )

    counts_by_selection_state = selection_state_counts(ideas)
    outcome_counts_by_selection_state: dict[str, dict[str, int]] = {}
    average_estimated_pnl_by_selection_state: dict[str, float | None] = {}
    for selection_state in (PROMOTABLE_SELECTION_STATE, MONITOR_SELECTION_STATE):
        state_items = [
            item for item in ideas if item["selection_state"] == selection_state
        ]
        outcome_counts_by_selection_state[selection_state] = dict(
            Counter(item["outcome_bucket"] for item in state_items)
        )
        pnl_values = [
            (
                item["estimated_expiry_pnl"]
                if item["estimated_expiry_pnl"] is not None
                else item["estimated_close_pnl"]
            )
            for item in state_items
            if (
                item["estimated_expiry_pnl"]
                if item["estimated_expiry_pnl"] is not None
                else item["estimated_close_pnl"]
            )
            is not None
        ]
        average_estimated_pnl_by_selection_state[selection_state] = (
            None if not pnl_values else mean(float(value) for value in pnl_values)
        )

    def aggregate_by(field: str) -> dict[str, dict[str, Any]]:
        grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in ideas:
            grouped_rows[str(item[field])].append(item)
        output: dict[str, dict[str, Any]] = {}
        for key, rows in grouped_rows.items():
            output[key] = {
                "count": len(rows),
                "promotable_count": sum(
                    1
                    for row in rows
                    if row["selection_state"] == PROMOTABLE_SELECTION_STATE
                ),
                "monitor_count": sum(
                    1
                    for row in rows
                    if row["selection_state"] == MONITOR_SELECTION_STATE
                ),
                "outcomes": dict(Counter(row["outcome_bucket"] for row in rows)),
                "average_latest_score": mean(
                    float(row["latest_score"]) for row in rows
                ),
            }
        return dict(sorted(output.items()))

    return {
        "session_date": session_date,
        "label": label,
        "idea_count": len(ideas),
        "counts_by_selection_state": counts_by_selection_state,
        "outcome_counts_by_selection_state": outcome_counts_by_selection_state,
        "average_estimated_pnl_by_selection_state": (
            average_estimated_pnl_by_selection_state
        ),
        "by_symbol": aggregate_by("underlying_symbol"),
        "by_strategy": aggregate_by("strategy"),
        "by_score_bucket": aggregate_by("score_bucket"),
        "ideas": ideas,
    }
