from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.candidate_history_recovery import (
    recover_session_candidates_from_history,
)
from core.services.live_pipelines import parse_live_run_scope_id

from .shared import RECOVERY_PER_STRATEGY, RECOVERY_TOP, _as_text


class OpportunityReplayLookupError(LookupError):
    pass


def _resolve_target(
    *,
    storage: Any,
    session_id: str | None,
    label: str | None,
    session_date: str | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    collector_store = storage.collector
    post_market_store = storage.post_market

    if session_id:
        cycle = collector_store.get_latest_session_cycle(session_id)
        if cycle is None:
            parsed = parse_live_run_scope_id(session_id)
            if parsed is not None:
                label = parsed["label"]
                session_date = parsed["market_date"]
                cycles = collector_store.list_cycles(
                    label, session_date=session_date, limit=1
                )
                cycle = cycles[0] if cycles else None
        if cycle is None:
            raise OpportunityReplayLookupError(f"Unknown session id: {session_id}")
        label = str(cycle["label"])
        session_date = str(cycle["session_date"])
        analysis_run = post_market_store.get_latest_run(
            label=label,
            session_date=session_date,
            succeeded_only=True,
        )
        return dict(cycle), None if analysis_run is None else dict(analysis_run)

    if label is not None:
        if session_date is None:
            cycle = collector_store.get_latest_cycle(label)
        else:
            cycles = collector_store.list_cycles(
                label, session_date=session_date, limit=1
            )
            cycle = cycles[0] if cycles else None
        if cycle is None:
            target = label if session_date is None else f"{label} on {session_date}"
            raise OpportunityReplayLookupError(
                f"No stored collector cycle found for {target}."
            )
        session_date = str(cycle["session_date"])
        analysis_run = post_market_store.get_latest_run(
            label=str(cycle["label"]),
            session_date=session_date,
            succeeded_only=True,
        )
        return dict(cycle), None if analysis_run is None else dict(analysis_run)

    latest_runs = post_market_store.list_runs(status="succeeded", limit=1)
    if not latest_runs:
        raise OpportunityReplayLookupError(
            "No succeeded post-market analysis runs are available."
        )
    analysis_run = dict(latest_runs[0])
    label = str(analysis_run["label"])
    session_date = str(analysis_run["session_date"])
    cycles = collector_store.list_cycles(label, session_date=session_date, limit=1)
    if not cycles:
        raise OpportunityReplayLookupError(
            f"No stored collector cycle found for latest succeeded analysis target {label} on {session_date}."
        )
    return dict(cycles[0]), analysis_run


def _wrap_recovered_candidate_rows(
    *,
    cycle_id: str,
    recovered_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, payload in enumerate(recovered_candidates, start=1):
        rows.append(
            {
                "candidate_id": -index,
                "cycle_id": cycle_id,
                "legacy_selection_state": "recovered",
                "position": index,
                "run_id": payload.get("run_id"),
                "underlying_symbol": payload.get("underlying_symbol"),
                "strategy": payload.get("strategy"),
                "expiration_date": payload.get("expiration_date"),
                "short_symbol": payload.get("short_symbol"),
                "long_symbol": payload.get("long_symbol"),
                "quality_score": payload.get("quality_score"),
                "midpoint_credit": payload.get("midpoint_credit"),
                "candidate": payload,
            }
        )
    return rows


def _load_cycle_candidates(
    *,
    storage: Any,
    cycle: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    persisted_rows = [
        dict(row)
        for row in storage.collector.list_cycle_candidates(str(cycle["cycle_id"]))
    ]
    warnings: list[str] = []
    if persisted_rows:
        return persisted_rows, warnings

    recovered_candidates = recover_session_candidates_from_history(
        history_store=storage.history,
        session_date=str(cycle["session_date"]),
        session_label=str(cycle["label"]),
        generated_at=str(cycle["generated_at"]),
        top=RECOVERY_TOP,
        max_per_strategy=RECOVERY_PER_STRATEGY,
    )
    if not recovered_candidates:
        return [], warnings

    warnings.append(
        f"Collector cycle {cycle['cycle_id']} has no stored candidates; replay recovered {len(recovered_candidates)} candidates from scan history."
    )
    return _wrap_recovered_candidate_rows(
        cycle_id=str(cycle["cycle_id"]),
        recovered_candidates=recovered_candidates,
    ), warnings


def _resolve_recent_analysis_targets(
    *,
    storage: Any,
    recent: int,
    label: str | None = None,
) -> list[dict[str, Any]]:
    if recent <= 0:
        raise ValueError("--recent must be greater than 0.")
    seen: set[tuple[str, str]] = set()
    targets: list[dict[str, Any]] = []
    fetched = storage.post_market.list_runs(
        status="succeeded",
        label=label,
        limit=max(recent * 5, recent),
    )
    for run in fetched:
        run_label = _as_text(run.get("label"))
        session_date = _as_text(run.get("session_date"))
        if run_label is None or session_date is None:
            continue
        key = (run_label, session_date)
        if key in seen:
            continue
        seen.add(key)
        targets.append({"label": run_label, "session_date": session_date})
    return targets
