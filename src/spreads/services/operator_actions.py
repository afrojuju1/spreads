from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any, Mapping
from uuid import uuid4

from spreads.alerts.dispatcher import resolve_session_date, send_or_skip_alert
from spreads.db.decorators import with_storage
from spreads.events.bus import publish_global_event_sync
from spreads.services.live_pipelines import build_live_session_id

MANUAL_ALERT_TYPE = "manual_generator_idea"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _selector_identity(*, strategy: str, short_symbol: str, long_symbol: str) -> tuple[str, str, str]:
    return (strategy, short_symbol, long_symbol)


def _candidate_identity(candidate: dict[str, Any]) -> tuple[str, str, str]:
    return _selector_identity(
        strategy=str(candidate["strategy"]),
        short_symbol=str(candidate["short_symbol"]),
        long_symbol=str(candidate["long_symbol"]),
    )


def _candidate_summary(candidate: dict[str, Any]) -> str:
    return (
        f"{candidate['strategy']} {float(candidate['short_strike']):.2f}/{float(candidate['long_strike']):.2f} "
        f"score {float(candidate['quality_score']):.1f}"
    )


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[float, float, float, float]:
    return (
        float(candidate.get("quality_score") or 0.0),
        float(candidate.get("return_on_risk") or 0.0),
        float(candidate.get("midpoint_credit") or 0.0),
        min(
            float(candidate.get("short_open_interest") or 0.0),
            float(candidate.get("long_open_interest") or 0.0),
        ),
    )


def _build_manual_cycle_id(label: str, bucket: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{label}_manual_{bucket}_{uuid4().hex[:8]}".lower()


def _get_generator_result(job: Mapping[str, Any]) -> dict[str, Any]:
    result = job["result"]
    if not isinstance(result, dict):
        raise ValueError("Generator job does not have a completed result")
    return result


def _resolve_candidate_run_id(
    job: Mapping[str, Any],
    result: dict[str, Any],
    candidate: dict[str, Any],
) -> str:
    for run in result.get("strategy_runs") or []:
        if not isinstance(run, dict):
            continue
        if str(run.get("strategy")) != str(candidate.get("strategy")):
            continue
        run_id = str(run.get("run_id") or "").strip()
        if run_id:
            return run_id
    return str(job["generator_job_id"])


def _find_job_candidate(
    job: Mapping[str, Any],
    *,
    strategy: str,
    short_symbol: str,
    long_symbol: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    result = _get_generator_result(job)
    target_identity = _selector_identity(
        strategy=strategy,
        short_symbol=short_symbol,
        long_symbol=long_symbol,
    )
    for row in result.get("top_candidates") or []:
        if not isinstance(row, dict):
            continue
        candidate = dict(row)
        if _candidate_identity(candidate) != target_identity:
            continue
        candidate.setdefault("underlying_symbol", job["symbol"])
        candidate["run_id"] = str(candidate.get("run_id") or _resolve_candidate_run_id(job, result, candidate))
        return candidate, result
    raise ValueError("Selected candidate was not found in the persisted generator result")


@with_storage()
def create_manual_generator_alert(
    *,
    job: Mapping[str, Any],
    live_label: str,
    strategy: str,
    short_symbol: str,
    long_symbol: str,
    db_target: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    candidate, result = _find_job_candidate(
        job,
        strategy=strategy,
        short_symbol=short_symbol,
        long_symbol=long_symbol,
    )
    collector_store = storage.collector
    alert_store = storage.alerts
    latest_cycle = collector_store.get_latest_cycle(live_label)
    if latest_cycle is None:
        raise ValueError(f"No live cycle is available for '{live_label}'")

    created_at = _utc_now()
    payload = {
        "created_at": created_at,
        "session_date": resolve_session_date(created_at),
        "label": latest_cycle["label"],
        "cycle_id": latest_cycle["cycle_id"],
        "symbol": str(candidate["underlying_symbol"]),
        "alert_type": MANUAL_ALERT_TYPE,
        "strategy_mode": str(job["request"].get("strategy") or result.get("strategy") or candidate["strategy"]),
        "profile": str(job["request"].get("profile") or result.get("profile") or latest_cycle["profile"]),
        "candidate": candidate,
        "description": f"{candidate['underlying_symbol']} manual generator alert: {_candidate_summary(candidate)}",
        "source": {
            "generator_job_id": job["generator_job_id"],
            "kind": "manual_generator_action",
        },
    }
    record = send_or_skip_alert(
        webhook_url=os.environ.get("SPREADS_DISCORD_WEBHOOK_URL") or os.environ.get("DISCORD_WEBHOOK_URL"),
        alert_store=alert_store,
        payload=payload,
        dedupe_key=(
            f"manual_generator_alert|{latest_cycle['label']}|{job['generator_job_id']}|"
            f"{candidate['strategy']}|{candidate['short_symbol']}|{candidate['long_symbol']}"
        ),
        dedupe_state={
            "generator_job_id": job["generator_job_id"],
            "strategy": candidate["strategy"],
            "short_symbol": candidate["short_symbol"],
            "long_symbol": candidate["long_symbol"],
            "kind": "manual_generator_action",
        },
    )
    try:
        publish_global_event_sync(
            topic="alert.event.created",
            event_class="control_event",
            entity_type="alert_event",
            entity_id=str(record["alert_id"]),
            payload={
                **record,
                "session_id": build_live_session_id(record["label"], record["session_date"]),
            },
            timestamp=record["created_at"],
            source="operator_actions",
            session_date=_as_text(record.get("session_date")),
            correlation_id=_as_text(job.get("generator_job_id")),
        )
    except Exception:
        pass
    return {
        "action": "create_alert",
        "changed": True,
        "message": f"Manual alert {record['status']} for {candidate['underlying_symbol']} on {latest_cycle['label']}.",
        "live_label": latest_cycle["label"],
        "session_id": latest_cycle.get("session_id"),
        "alert": record,
    }


@with_storage()
def apply_generator_live_action(
    *,
    job: Mapping[str, Any],
    live_label: str,
    bucket: str,
    strategy: str,
    short_symbol: str,
    long_symbol: str,
    db_target: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    if bucket not in {"board", "watchlist"}:
        raise ValueError("Live action bucket must be 'board' or 'watchlist'")

    candidate, _ = _find_job_candidate(
        job,
        strategy=strategy,
        short_symbol=short_symbol,
        long_symbol=long_symbol,
    )
    collector_store = storage.collector
    latest_cycle = collector_store.get_latest_cycle(live_label)
    if latest_cycle is None:
        raise ValueError(f"No live cycle is available for '{live_label}'")

    board_candidates = [
        dict(row["candidate"])
        for row in collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="board")
    ]
    watchlist_candidates = [
        dict(row["candidate"])
        for row in collector_store.list_cycle_candidates(latest_cycle["cycle_id"], bucket="watchlist")
    ]
    target_identity = _candidate_identity(candidate)
    board_match = next((row for row in board_candidates if _candidate_identity(row) == target_identity), None)
    watchlist_match = next((row for row in watchlist_candidates if _candidate_identity(row) == target_identity), None)

    if bucket == "board" and board_match is not None:
        return {
            "action": "promote_live",
            "changed": False,
            "message": f"{candidate['underlying_symbol']} is already on the live board for {latest_cycle['label']}.",
            "live_label": latest_cycle["label"],
            "bucket": "board",
            "cycle_id": latest_cycle["cycle_id"],
        }
    if bucket == "watchlist" and board_match is not None:
        return {
            "action": "promote_live",
            "changed": False,
            "message": f"{candidate['underlying_symbol']} is already on the live board for {latest_cycle['label']}.",
            "live_label": latest_cycle["label"],
            "bucket": "board",
            "cycle_id": latest_cycle["cycle_id"],
        }
    if bucket == "watchlist" and watchlist_match is not None:
        return {
            "action": "promote_live",
            "changed": False,
            "message": f"{candidate['underlying_symbol']} is already on the live watchlist for {latest_cycle['label']}.",
            "live_label": latest_cycle["label"],
            "bucket": "watchlist",
            "cycle_id": latest_cycle["cycle_id"],
        }

    symbol = str(candidate["underlying_symbol"])
    generated_at = _utc_now()
    cycle_id = _build_manual_cycle_id(latest_cycle["label"], bucket)
    previous_candidate: dict[str, Any] | None = None
    selection_state = dict(latest_cycle["selection_state"] or {})

    if bucket == "board":
        previous_candidate = next(
            (row for row in board_candidates if str(row.get("underlying_symbol")) == symbol),
            None,
        )
        if previous_candidate is None:
            previous_candidate = next(
                (row for row in watchlist_candidates if str(row.get("underlying_symbol")) == symbol),
                None,
            )
        board_candidates = [
            row
            for row in board_candidates
            if str(row.get("underlying_symbol")) != symbol and _candidate_identity(row) != target_identity
        ]
        watchlist_candidates = [
            row for row in watchlist_candidates if str(row.get("underlying_symbol")) != symbol
        ]
        board_candidates.append(candidate)
        board_candidates.sort(key=_candidate_sort_key, reverse=True)
        current_symbol_state = selection_state.get(symbol)
        next_symbol_state = dict(current_symbol_state) if isinstance(current_symbol_state, dict) else {}
        next_symbol_state.update(
            {
                "accepted_identity": f"{candidate['strategy']}|{candidate['short_symbol']}|{candidate['long_symbol']}",
                "accepted_strategy": candidate["strategy"],
                "accepted_score": float(candidate["quality_score"]),
            }
        )
        next_symbol_state.pop("pending_identity", None)
        next_symbol_state.pop("pending_strategy", None)
        next_symbol_state.pop("pending_count", None)
        selection_state[symbol] = next_symbol_state

        if watchlist_match is not None:
            event_type = "manual_watchlist_promoted"
            message = f"{symbol} manually promoted from watchlist to board: {_candidate_summary(candidate)}"
        elif previous_candidate is not None:
            event_type = "manual_board_replaced"
            message = (
                f"{symbol} manually replaced on board: "
                f"{_candidate_summary(previous_candidate)} -> {_candidate_summary(candidate)}"
            )
        else:
            event_type = "manual_board_added"
            message = f"{symbol} manually added to board: {_candidate_summary(candidate)}"
    else:
        watchlist_candidates = [
            row for row in watchlist_candidates if _candidate_identity(row) != target_identity
        ]
        watchlist_candidates.append(candidate)
        watchlist_candidates.sort(key=_candidate_sort_key, reverse=True)
        event_type = "manual_watchlist_added"
        message = f"{symbol} manually added to watchlist: {_candidate_summary(candidate)}"

    symbols = list(latest_cycle["symbols"])
    if symbol not in symbols:
        symbols.append(symbol)

    event_payload = {
        "generated_at": generated_at,
        "cycle_id": cycle_id,
        "label": latest_cycle["label"],
        "symbol": symbol,
        "event_type": event_type,
        "message": message,
        "previous": previous_candidate,
        "current": candidate,
    }

    collector_store.save_cycle(
        cycle_id=cycle_id,
        label=latest_cycle["label"],
        generated_at=generated_at,
        job_run_id=latest_cycle.get("job_run_id"),
        session_id=latest_cycle.get("session_id"),
        universe_label=latest_cycle["universe_label"],
        strategy=latest_cycle["strategy"],
        profile=latest_cycle["profile"],
        greeks_source=latest_cycle["greeks_source"],
        symbols=symbols,
        failures=list(latest_cycle["failures"]),
        selection_state=selection_state,
        board_candidates=board_candidates,
        watchlist_candidates=watchlist_candidates,
        events=[event_payload],
    )
    response = {
        "action": "promote_live",
        "changed": True,
        "message": message,
        "live_label": latest_cycle["label"],
        "session_date": latest_cycle["session_date"],
        "session_id": latest_cycle.get("session_id"),
        "bucket": bucket,
        "cycle_id": cycle_id,
        "event_type": event_type,
        "symbol": symbol,
        "generated_at": generated_at,
        "board_count": len(board_candidates),
        "watchlist_count": len(watchlist_candidates),
    }
    try:
        publish_global_event_sync(
            topic="live.cycle.updated",
            event_class="control_event",
            entity_type="collector_cycle",
            entity_id=cycle_id,
            payload=response,
            timestamp=generated_at,
            source="operator_actions",
            session_date=_as_text(latest_cycle.get("session_date")),
            correlation_id=_as_text(latest_cycle.get("session_id")),
        )
    except Exception:
        pass
    return response
