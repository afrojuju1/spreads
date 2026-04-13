from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping
from uuid import uuid4

from spreads.alerts.dispatcher import resolve_session_date
from spreads.db.decorators import with_storage
from spreads.events.bus import publish_global_event_sync
from spreads.services.alert_delivery import plan_alert_delivery

MANUAL_ALERT_TYPE = "manual_generator_idea"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


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


def _build_manual_cycle_id(label: str, target_state: str) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}_{label}_manual_{target_state}_{uuid4().hex[:8]}".lower()


def _canonical_opportunity_row(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    candidate = payload.get("candidate")
    payload["candidate"] = dict(candidate) if isinstance(candidate, Mapping) else {}
    return payload


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
    job_store = storage.jobs
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
    record, created = plan_alert_delivery(
        alert_store=alert_store,
        job_store=job_store,
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
        session_id=_as_text(latest_cycle.get("session_id")),
        planner_job_run_id=None,
        source="operator_actions",
        correlation_id=_as_text(job.get("generator_job_id")),
    )
    return {
        "action": "create_alert",
        "changed": created,
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
    target_state: str,
    strategy: str,
    short_symbol: str,
    long_symbol: str,
    db_target: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    if target_state not in {"promotable", "monitor"}:
        raise ValueError("Live action target_state must be 'promotable' or 'monitor'")

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

    live_opportunities = [
        _canonical_opportunity_row(row)
        for row in collector_store.list_cycle_candidates(latest_cycle["cycle_id"])
    ]
    target_identity = _candidate_identity(candidate)
    current_match = next(
        (
            row
            for row in live_opportunities
            if _candidate_identity(dict(row.get("candidate") or {})) == target_identity
            and str(row.get("eligibility") or "live") == "live"
        ),
        None,
    )

    if current_match is not None and str(current_match.get("selection_state")) == target_state:
        return {
            "action": "promote_live",
            "changed": False,
            "message": (
                f"{candidate['underlying_symbol']} is already marked {target_state} "
                f"for {latest_cycle['label']}."
            ),
            "live_label": latest_cycle["label"],
            "target_state": target_state,
            "cycle_id": latest_cycle["cycle_id"],
        }

    symbol = str(candidate["underlying_symbol"])
    generated_at = _utc_now()
    cycle_id = _build_manual_cycle_id(latest_cycle["label"], target_state)
    selection_memory = dict(latest_cycle.get("selection_memory") or {})
    previous_candidate: dict[str, Any] | None = None
    retained: list[dict[str, Any]] = []
    for row in live_opportunities:
        if str(row.get("eligibility") or "live") != "live":
            retained.append(row)
            continue
        row_candidate = dict(row.get("candidate") or {})
        row_identity = _candidate_identity(row_candidate)
        row_symbol = str(row_candidate.get("underlying_symbol") or "")
        if target_state == "promotable":
            if row_symbol == symbol:
                if previous_candidate is None:
                    previous_candidate = row_candidate
                continue
        elif row_identity == target_identity:
            if previous_candidate is None:
                previous_candidate = row_candidate
            continue
        retained.append(row)

    if target_state == "promotable":
        current_symbol_state = selection_memory.get(symbol)
        next_symbol_state = (
            dict(current_symbol_state) if isinstance(current_symbol_state, dict) else {}
        )
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
        selection_memory[symbol] = next_symbol_state
    new_row = {
        **candidate,
        "candidate": dict(candidate),
        "selection_state": target_state,
        "selection_rank": 0,
        "state_reason": f"manual_{target_state}",
        "origin": "manual_override",
        "eligibility": "live",
    }
    retained.append(new_row)

    promotable_rows = [
        row for row in retained if str(row.get("selection_state")) == "promotable"
    ]
    monitor_rows = [
        row for row in retained if str(row.get("selection_state")) == "monitor"
    ]
    analysis_only_rows = [
        row for row in retained if str(row.get("eligibility") or "live") != "live"
    ]
    promotable_rows.sort(
        key=lambda row: _candidate_sort_key(dict(row.get("candidate") or row)),
        reverse=True,
    )
    monitor_rows.sort(
        key=lambda row: _candidate_sort_key(dict(row.get("candidate") or row)),
        reverse=True,
    )
    opportunities: list[dict[str, Any]] = []
    next_rank = 1
    for row in [*promotable_rows, *monitor_rows, *analysis_only_rows]:
        row["selection_rank"] = next_rank
        opportunities.append(row)
        next_rank += 1

    if target_state == "promotable":
        if current_match is not None and str(current_match.get("selection_state")) == "monitor":
            event_type = "manual_promotable_promoted"
            message = f"{symbol} manually promoted to promotable: {_candidate_summary(candidate)}"
        elif previous_candidate is not None:
            event_type = "manual_promotable_replaced"
            message = (
                f"{symbol} manually replaced as promotable: "
                f"{_candidate_summary(previous_candidate)} -> {_candidate_summary(candidate)}"
            )
        else:
            event_type = "manual_promotable_added"
            message = f"{symbol} manually marked promotable: {_candidate_summary(candidate)}"
    else:
        event_type = "manual_monitor_added"
        message = f"{symbol} manually marked monitor: {_candidate_summary(candidate)}"

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
        selection_memory=selection_memory,
        opportunities=opportunities,
        events=[event_payload],
    )
    response = {
        "action": "promote_live",
        "changed": True,
        "message": message,
        "live_label": latest_cycle["label"],
        "session_date": latest_cycle["session_date"],
        "session_id": latest_cycle.get("session_id"),
        "target_state": target_state,
        "cycle_id": cycle_id,
        "event_type": event_type,
        "symbol": symbol,
        "generated_at": generated_at,
        "promotable_count": len(promotable_rows),
        "monitor_count": len(monitor_rows),
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
