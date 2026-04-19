from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from datetime import UTC, date, datetime, time
import os
from typing import Any, Mapping

from core.common import env_or_die, load_local_env
from core.db.decorators import with_storage
from core.integrations.calendar_events import build_calendar_event_resolver
from core.integrations.greeks import build_local_greeks_provider
from core.services.alpaca import create_alpaca_client_from_env
from core.services.automation_runtime import resolve_entry_runtime
from core.services.bot_analytics import evaluate_entry_controls
from core.services.entry_planner import plan_entry_selection, score_opportunity
from core.services.live_selection import select_live_opportunities
from core.services.replay_filters import candidate_matches_filter
from core.services.scanners.config import (
    apply_scan_evaluation_context,
    parse_args as parse_scanner_args,
)
from core.services.scanners.historical import (
    ALPACA_OPTIONS_HISTORY_START,
    build_historical_symbol_market_slice_from_alpaca,
)
from core.services.option_structures import candidate_legs, legs_identity_key
from core.services.scanners.replay_artifacts import (
    deserialize_calendar_decisions_by_expiration,
    deserialize_market_slice,
    deserialize_setup_context,
    deserialize_symbol_args,
    load_scan_replay_artifact,
)
from core.services.scanners.runtime import (
    build_raw_candidates_from_market_slice,
    postprocess_market_slice_candidates,
)
from core.services.strategy_builders import (
    build_entry_runtime_candidates_from_market_slices,
    build_market_slice_args,
    runtime_owner_key,
)
from core.services.market_dates import NEW_YORK


_COMPARABLE_FIELDS = (
    "width",
    "short_strike",
    "long_strike",
    "midpoint_credit",
    "natural_credit",
    "breakeven",
    "max_profit",
    "max_loss",
    "return_on_risk",
    "expected_move",
    "short_vs_expected_move",
    "calendar_status",
    "setup_status",
)


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _values_differ(left: Any, right: Any, *, tolerance: float = 1e-4) -> bool:
    left_float = _as_float(left)
    right_float = _as_float(right)
    if left_float is not None and right_float is not None:
        return abs(left_float - right_float) > tolerance
    return left != right


def _candidate_identity(candidate: Mapping[str, Any]) -> str:
    return legs_identity_key(
        strategy=candidate.get("strategy"),
        legs=candidate_legs(candidate),
    )


def _candidate_summary(candidate: Mapping[str, Any], *, rank: int | None = None) -> dict[str, Any]:
    payload = dict(candidate)
    return {
        "rank": rank,
        "identity": _candidate_identity(payload),
        "underlying_symbol": payload.get("underlying_symbol"),
        "strategy": payload.get("strategy"),
        "expiration_date": payload.get("expiration_date"),
        "short_symbol": payload.get("short_symbol"),
        "long_symbol": payload.get("long_symbol"),
        "width": payload.get("width"),
        "midpoint_credit": payload.get("midpoint_credit"),
        "return_on_risk": payload.get("return_on_risk"),
        "quality_score": payload.get("quality_score"),
        "calendar_status": payload.get("calendar_status"),
        "setup_status": payload.get("setup_status"),
    }


def _resolve_target_run(
    *,
    history_store: Any,
    run_id: str | None,
    symbol: str | None,
    strategy: str | None,
    latest: bool,
) -> dict[str, Any]:
    if run_id:
        run = history_store.get_run(run_id)
    elif latest:
        if not symbol:
            raise ValueError("--latest requires --symbol")
        run = history_store.get_latest_run(symbol.upper(), strategy=strategy)
    else:
        raise ValueError("Provide --run-id or use --latest with --symbol")
    if not run:
        target = run_id or symbol or "unknown"
        raise ValueError(f"No stored scan run found for target: {target}")
    return dict(run)


def _build_base_replay_payload(
    *,
    run: Mapping[str, Any],
    artifact_path: str,
) -> dict[str, Any]:
    return {
        "status": "completed",
        "fidelity": "high",
        "run": {
            "run_id": run.get("run_id"),
            "generated_at": run.get("generated_at"),
            "symbol": run.get("symbol"),
            "strategy": run.get("strategy"),
            "profile": run.get("profile"),
            "artifact_path": artifact_path or None,
        },
        "summary": {},
        "stored_top": [],
        "replayed_top": [],
        "stored_only": [],
        "replayed_only": [],
        "rank_changes": [],
        "field_drifts": [],
    }


def _build_replay_payload_for_run(
    *,
    history_store: Any,
    run: Mapping[str, Any],
) -> dict[str, Any]:
    resolved_run = dict(run)
    stored_candidates = [
        dict(row) for row in history_store.list_candidates(resolved_run["run_id"])
    ]
    artifact_path = str(resolved_run.get("output_path") or "").strip()
    base_payload = _build_base_replay_payload(
        run=resolved_run,
        artifact_path=artifact_path,
    )
    if not artifact_path:
        base_payload["status"] = "unsupported"
        base_payload["fidelity"] = "unsupported"
        base_payload["reason"] = "missing_replay_artifact"
        return base_payload

    try:
        artifact = load_scan_replay_artifact(artifact_path)
    except FileNotFoundError:
        base_payload["status"] = "unsupported"
        base_payload["fidelity"] = "unsupported"
        base_payload["reason"] = "replay_artifact_file_missing"
        return base_payload
    market_slice = deserialize_market_slice(dict(artifact["market_slice"]))
    setup_context = deserialize_setup_context(artifact.get("setup_context"))
    symbol_args = deserialize_symbol_args(artifact.get("symbol_args"))
    calendar_decisions_by_expiration = deserialize_calendar_decisions_by_expiration(
        artifact.get("calendar_decisions_by_expiration")
    )
    candidate_filter = dict(artifact.get("candidate_filter") or {})

    replayed_candidates = postprocess_market_slice_candidates(
        market_slice=market_slice,
        symbol_args=symbol_args,
        raw_candidates=build_raw_candidates_from_market_slice(
            market_slice=market_slice,
            symbol_args=symbol_args,
        ),
        setup_context=setup_context,
        calendar_decisions_by_expiration=calendar_decisions_by_expiration,
    )
    replayed_rows = [
        dict(asdict(candidate))
        for candidate in replayed_candidates
        if candidate_matches_filter(dict(asdict(candidate)), candidate_filter)
    ]

    stored_rank_by_identity: dict[str, int] = {}
    stored_by_identity: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(stored_candidates, start=1):
        identity = _candidate_identity(row)
        stored_rank_by_identity[identity] = index
        stored_by_identity[identity] = dict(row)

    replayed_rank_by_identity: dict[str, int] = {}
    replayed_by_identity: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(replayed_rows, start=1):
        identity = _candidate_identity(row)
        replayed_rank_by_identity[identity] = index
        replayed_by_identity[identity] = dict(row)

    common_identities = sorted(
        set(stored_by_identity).intersection(replayed_by_identity)
    )
    stored_only = sorted(set(stored_by_identity).difference(replayed_by_identity))
    replayed_only = sorted(set(replayed_by_identity).difference(stored_by_identity))

    rank_changes: list[dict[str, Any]] = []
    field_drifts: list[dict[str, Any]] = []
    for identity in common_identities:
        stored_rank = stored_rank_by_identity[identity]
        replayed_rank = replayed_rank_by_identity[identity]
        if stored_rank != replayed_rank:
            rank_changes.append(
                {
                    "identity": identity,
                    "stored_rank": stored_rank,
                    "replayed_rank": replayed_rank,
                }
            )
        stored_row = stored_by_identity[identity]
        replayed_row = replayed_by_identity[identity]
        for field in _COMPARABLE_FIELDS:
            if _values_differ(stored_row.get(field), replayed_row.get(field)):
                field_drifts.append(
                    {
                        "identity": identity,
                        "field": field,
                        "stored": stored_row.get(field),
                        "replayed": replayed_row.get(field),
                    }
                )

    base_payload["summary"] = {
        "exact_match": not stored_only
        and not replayed_only
        and not rank_changes
        and not field_drifts,
        "stored_candidate_count": len(stored_candidates),
        "replayed_candidate_count": len(replayed_rows),
        "matched_candidate_count": len(common_identities),
        "stored_only_count": len(stored_only),
        "replayed_only_count": len(replayed_only),
        "rank_change_count": len(rank_changes),
        "field_drift_count": len(field_drifts),
    }
    base_payload["stored_top"] = [
        _candidate_summary(row, rank=index)
        for index, row in enumerate(stored_candidates[:10], start=1)
    ]
    base_payload["replayed_top"] = [
        _candidate_summary(row, rank=index)
        for index, row in enumerate(replayed_rows[:10], start=1)
    ]
    base_payload["stored_only"] = [
        _candidate_summary(
            stored_by_identity[identity],
            rank=stored_rank_by_identity[identity],
        )
        for identity in stored_only[:25]
    ]
    base_payload["replayed_only"] = [
        _candidate_summary(
            replayed_by_identity[identity],
            rank=replayed_rank_by_identity[identity],
        )
        for identity in replayed_only[:25]
    ]
    base_payload["rank_changes"] = rank_changes[:50]
    base_payload["field_drifts"] = field_drifts[:200]
    return base_payload


def _replay_run_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    run = dict(payload.get("run") or {})
    summary = dict(payload.get("summary") or {})
    return {
        "run_id": run.get("run_id"),
        "generated_at": run.get("generated_at"),
        "symbol": run.get("symbol"),
        "strategy": run.get("strategy"),
        "profile": run.get("profile"),
        "status": payload.get("status"),
        "fidelity": payload.get("fidelity"),
        "reason": payload.get("reason"),
        "exact_match": summary.get("exact_match"),
        "stored_candidate_count": summary.get("stored_candidate_count"),
        "replayed_candidate_count": summary.get("replayed_candidate_count"),
        "matched_candidate_count": summary.get("matched_candidate_count"),
        "stored_only_count": summary.get("stored_only_count"),
        "replayed_only_count": summary.get("replayed_only_count"),
        "rank_change_count": summary.get("rank_change_count"),
        "field_drift_count": summary.get("field_drift_count"),
        "artifact_path": run.get("artifact_path"),
    }


def _cycle_replay_status(run_summaries: list[dict[str, Any]]) -> tuple[str, bool | None]:
    if not run_summaries:
        return "no_scan_runs", None
    if any(str(row.get("status") or "") == "unsupported" for row in run_summaries):
        return "unsupported", None
    if all(row.get("exact_match") is True for row in run_summaries):
        return "exact_match", True
    return "mismatch", False


def _market_close_timestamp(session_day: date) -> datetime:
    return datetime.combine(session_day, time(16, 0), tzinfo=NEW_YORK).astimezone(UTC)


def _render_utc(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _opportunity_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = (
        dict(row.get("candidate"))
        if isinstance(row.get("candidate"), Mapping)
        else dict(row)
    )
    return {
        **_candidate_summary(candidate, rank=row.get("selection_rank")),
        "opportunity_id": row.get("opportunity_id"),
        "underlying_symbol": candidate.get("underlying_symbol"),
        "selection_state": row.get("selection_state"),
        "eligibility": row.get("eligibility"),
        "promotion_score": row.get("promotion_score"),
        "execution_score": row.get("execution_score"),
    }


def _candidate_sort_key(row: Mapping[str, Any]) -> tuple[float, float, str]:
    return (
        float(row.get("quality_score") or 0.0),
        float(row.get("return_on_risk") or 0.0),
        str(row.get("underlying_symbol") or ""),
    )


def _with_opportunity_ids(
    opportunities: list[dict[str, Any]],
    *,
    bot_id: str,
    automation_id: str,
    session_date: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(opportunities, start=1):
        payload = dict(row)
        if payload.get("opportunity_id") in (None, ""):
            payload["opportunity_id"] = (
                f"alpaca_opportunity:{bot_id}:{automation_id}:{session_date}:{index}:"
                f"{_candidate_identity(payload)}"
            )
        rows.append(payload)
    return rows


def _alpaca_replay_dependencies(*, db_target: str) -> tuple[Any, Any, Any]:
    load_local_env()
    client = create_alpaca_client_from_env(request_timeout_seconds=45.0)
    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    data_base_url = os.environ.get("ALPACA_DATA_BASE_URL", client.data_base_url)
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=data_base_url,
        database_url=db_target or None,
    )
    greeks_provider = build_local_greeks_provider()
    return client, calendar_resolver, greeks_provider


def _alpaca_trading_dates(
    *,
    client: Any,
    anchor_symbol: str,
    start_date: str,
    end_date: str,
    stock_feed: str,
    limit: int,
) -> list[str]:
    bars = client.get_daily_bars(
        anchor_symbol,
        start=start_date,
        end=end_date,
        stock_feed=stock_feed,
    )
    session_dates = sorted(
        {
            datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00"))
            .astimezone(NEW_YORK)
            .date()
            .isoformat()
            for bar in bars
        }
    )
    return session_dates[: max(int(limit), 1)]


def _alpaca_cycle_status(
    *,
    selected: Mapping[str, Any] | None,
    opportunities: list[dict[str, Any]],
    candidate_count: int,
    controls_allowed: bool,
    symbol_failures: list[dict[str, Any]],
) -> str:
    if selected is not None:
        return "selected"
    if opportunities and not controls_allowed:
        return "blocked"
    if opportunities:
        return "monitor_only"
    if candidate_count > 0:
        return "candidates_only"
    if symbol_failures:
        return "unsupported"
    return "no_candidates"


def _build_alpaca_replay_range_payload(
    *,
    db_target: str,
    bot_id: str,
    automation_id: str,
    start_date: str,
    end_date: str,
    limit: int,
    storage: Any,
) -> dict[str, Any]:
    runtime = resolve_entry_runtime(bot_id=bot_id, automation_id=automation_id)
    if not runtime.symbols:
        return {
            "status": "no_cycles",
            "source": "alpaca",
            "target": {
                "bot_id": bot_id,
                "automation_id": automation_id,
                "start_date": start_date,
                "end_date": end_date,
                "cycle_limit": max(int(limit), 1),
                "sample_mode": "market_close",
            },
            "summary": {"cycle_count": 0},
            "cycles": [],
        }

    client, calendar_resolver, greeks_provider = _alpaca_replay_dependencies(
        db_target=db_target
    )
    scanner_args = parse_scanner_args([])
    anchor_symbol = str(runtime.symbols[0]).upper()
    session_dates = _alpaca_trading_dates(
        client=client,
        anchor_symbol=anchor_symbol,
        start_date=max(start_date, ALPACA_OPTIONS_HISTORY_START.isoformat()),
        end_date=end_date,
        stock_feed=str(getattr(scanner_args, "stock_feed", "sip") or "sip"),
        limit=limit,
    )

    cycle_rows: list[dict[str, Any]] = []
    cycle_status_counts: Counter[str] = Counter()
    exact_match_run_count = 0
    mismatch_run_count = 0
    unsupported_run_count = 0
    total_candidate_count = 0
    total_opportunity_count = 0
    selected_cycle_count = 0
    cycles_with_candidates_count = 0
    cycles_with_opportunities_count = 0

    for session_date in session_dates:
        session_day = date.fromisoformat(session_date)
        as_of = _market_close_timestamp(session_day)
        generated_at = _render_utc(as_of)
        session_scanner_args = apply_scan_evaluation_context(
            parse_scanner_args([]),
            evaluation_timestamp=as_of,
            evaluation_date=session_date,
        )
        market_slices_by_symbol: dict[str, Any] = {}
        symbol_failures: list[dict[str, Any]] = []
        for symbol in runtime.symbols:
            normalized_symbol = str(symbol).upper()
            try:
                market_slice_args = build_market_slice_args(
                    symbol=normalized_symbol,
                    base_scanner_args=session_scanner_args,
                    runtimes=[runtime],
                )
                apply_scan_evaluation_context(
                    market_slice_args,
                    evaluation_timestamp=as_of,
                    evaluation_date=session_date,
                )
                market_slices_by_symbol[normalized_symbol] = (
                    build_historical_symbol_market_slice_from_alpaca(
                        symbol=normalized_symbol,
                        symbol_args=market_slice_args,
                        client=client,
                        greeks_provider=greeks_provider,
                        as_of=as_of,
                    )
                )
            except Exception as exc:
                symbol_failures.append(
                    {
                        "symbol": normalized_symbol,
                        "error": str(exc).splitlines()[0],
                    }
                )

        runtime_candidate_rows_by_owner = (
            build_entry_runtime_candidates_from_market_slices(
                entry_runtimes=[runtime],
                base_scanner_args=session_scanner_args,
                calendar_resolver=calendar_resolver,
                market_slices_by_symbol=market_slices_by_symbol,
                per_runtime_limit=6,
                history_store=None,
                session_label=f"alpaca_replay:{bot_id}:{automation_id}",
            )
            if market_slices_by_symbol
            else {}
        )
        owner_candidates = {
            str(symbol): [dict(candidate) for candidate in rows]
            for symbol, rows in dict(
                runtime_candidate_rows_by_owner.get(runtime_owner_key(runtime)) or {}
            ).items()
        }
        selection = select_live_opportunities(
            label=f"alpaca_replay:{bot_id}:{automation_id}",
            cycle_id=f"alpaca:{bot_id}:{automation_id}:{session_date}",
            generated_at=generated_at,
            symbol_candidates=owner_candidates,
            previous_promotable={},
            previous_selection_memory={},
            top_promotable=max(int(getattr(session_scanner_args, "top", 10) or 10), 1),
            top_monitor=max(int(getattr(session_scanner_args, "top", 10) or 10), 1),
            profile=runtime.build_settings.scanner_profile,
        )
        opportunities = _with_opportunity_ids(
            [dict(row) for row in list(selection.get("opportunities") or [])],
            bot_id=bot_id,
            automation_id=automation_id,
            session_date=session_date,
        )
        controls_allowed, controls_reason, bot_metrics = evaluate_entry_controls(
            storage=storage,
            bot=runtime.bot.bot,
            market_date=session_date,
        )
        plan = plan_entry_selection(
            opportunities=opportunities,
            controls_allowed=controls_allowed,
            controls_reason=controls_reason,
            bot_metrics=bot_metrics,
            min_score=float(runtime.trigger_policy.get("min_opportunity_score") or 0.0),
        )
        selected = (
            dict(plan["selected"])
            if isinstance(plan.get("selected"), Mapping)
            else None
        )

        flattened_candidates = [
            dict(candidate)
            for rows in owner_candidates.values()
            for candidate in rows
        ]
        flattened_candidates.sort(key=_candidate_sort_key, reverse=True)
        candidate_count = len(flattened_candidates)
        opportunity_count = len(opportunities)
        cycle_status = _alpaca_cycle_status(
            selected=selected,
            opportunities=opportunities,
            candidate_count=candidate_count,
            controls_allowed=controls_allowed,
            symbol_failures=symbol_failures,
        )
        cycle_status_counts[cycle_status] += 1
        total_candidate_count += candidate_count
        total_opportunity_count += opportunity_count
        if candidate_count > 0:
            cycles_with_candidates_count += 1
        if opportunity_count > 0:
            cycles_with_opportunities_count += 1
        if selected is not None:
            selected_cycle_count += 1

        cycle_rows.append(
            {
                "session_date": session_date,
                "started_at": generated_at,
                "completed_at": generated_at,
                "automation_run_id": None,
                "cycle_id": f"alpaca:{bot_id}:{automation_id}:{session_date}",
                "label": f"alpaca_replay:{bot_id}:{automation_id}",
                "status": cycle_status,
                "exact_match": None,
                "fidelity": "reduced",
                "fidelity_reason": "alpaca_option_bars_with_synthetic_quotes_and_local_greeks",
                "trigger_type": "alpaca_historical_market_close",
                "candidate_symbol_count": len(owner_candidates),
                "candidate_count": candidate_count,
                "opportunity_count": opportunity_count,
                "collector_candidate_count": candidate_count,
                "scan_run_count": 0,
                "controls_allowed": controls_allowed,
                "controls_reason": controls_reason,
                "selected": None if selected is None else _opportunity_summary(selected),
                "top_opportunities": [
                    _opportunity_summary(row) for row in opportunities[:10]
                ],
                "top_candidates": [
                    _candidate_summary(row, rank=index)
                    for index, row in enumerate(flattened_candidates[:10], start=1)
                ],
                "symbol_failures": symbol_failures,
                "symbol_summaries": [
                    {
                        "symbol": symbol,
                        "candidate_count": len(rows),
                        "top_candidate": (
                            None
                            if not rows
                            else _candidate_summary(rows[0], rank=1)
                        ),
                    }
                    for symbol, rows in sorted(owner_candidates.items())
                ],
                "runs": [],
            }
        )

    return {
        "status": "completed" if cycle_rows else "no_cycles",
        "source": "alpaca",
        "target": {
            "bot_id": bot_id,
            "automation_id": automation_id,
            "start_date": start_date,
            "end_date": end_date,
            "cycle_limit": max(int(limit), 1),
            "sample_mode": "market_close",
            "fidelity": "reduced",
        },
        "summary": {
            "cycle_count": len(cycle_rows),
            "cycle_with_candidates_count": cycles_with_candidates_count,
            "cycle_with_opportunities_count": cycles_with_opportunities_count,
            "scan_run_count": 0,
            "candidate_count": total_candidate_count,
            "opportunity_count": total_opportunity_count,
            "selected_cycle_count": selected_cycle_count,
            "exact_match_cycle_count": 0,
            "mismatch_cycle_count": 0,
            "unsupported_cycle_count": int(cycle_status_counts.get("unsupported", 0)),
            "no_scan_run_cycle_count": 0,
            "exact_match_run_count": exact_match_run_count,
            "mismatch_run_count": mismatch_run_count,
            "unsupported_run_count": unsupported_run_count,
            "cycle_status_counts": dict(sorted(cycle_status_counts.items())),
            "run_status_counts": {},
            "run_fidelity_counts": {"reduced": len(cycle_rows)} if cycle_rows else {},
        },
        "cycles": cycle_rows,
        "coverage": {
            "priority_ladder": [
                "alpaca_historical_option_bars",
                "inactive_option_contract_metadata",
                "local_bsm_greeks",
            ],
            "alpaca_options_history_supported_from": ALPACA_OPTIONS_HISTORY_START.isoformat(),
            "quote_reconstruction": "synthetic_from_bar_range",
        },
    }


@with_storage()
def build_replay_payload(
    *,
    db_target: str,
    run_id: str | None = None,
    symbol: str | None = None,
    strategy: str | None = None,
    latest: bool = False,
    storage: Any | None = None,
) -> dict[str, Any]:
    history_store = storage.history
    run = _resolve_target_run(
        history_store=history_store,
        run_id=run_id,
        symbol=symbol,
        strategy=strategy,
        latest=latest,
    )
    return _build_replay_payload_for_run(history_store=history_store, run=run)


@with_storage()
def build_replay_range_payload(
    *,
    db_target: str,
    bot_id: str,
    automation_id: str,
    start_date: str,
    end_date: str,
    limit: int = 500,
    source: str = "stored",
    storage: Any | None = None,
) -> dict[str, Any]:
    normalized_source = str(source or "stored").strip().lower()
    if normalized_source == "alpaca":
        return _build_alpaca_replay_range_payload(
            db_target=db_target,
            bot_id=bot_id,
            automation_id=automation_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            storage=storage,
        )
    if normalized_source != "stored":
        raise ValueError(f"Unsupported replay source: {source}")

    signal_store = storage.signals
    collector_store = storage.collector
    history_store = storage.history
    cycle_limit = max(int(limit), 1)
    automation_runs = [
        dict(row)
        for row in signal_store.list_automation_runs(
            bot_id=bot_id,
            automation_id=automation_id,
            start_date=start_date,
            end_date=end_date,
            limit=cycle_limit,
        )
    ]
    ordered_runs = sorted(
        automation_runs,
        key=lambda row: (
            str(row.get("session_date") or ""),
            str(row.get("started_at") or ""),
            str(row.get("automation_run_id") or ""),
        ),
    )
    cycle_rows: list[dict[str, Any]] = []
    run_status_counts: Counter[str] = Counter()
    run_fidelity_counts: Counter[str] = Counter()
    cycle_status_counts: Counter[str] = Counter()
    exact_match_run_count = 0
    mismatch_run_count = 0
    unsupported_run_count = 0

    for automation_run in ordered_runs:
        cycle_id = str(automation_run.get("cycle_id") or "").strip()
        collector_candidates = (
            []
            if not cycle_id
            else [
                dict(row)
                for row in collector_store.list_cycle_candidates(cycle_id)
            ]
        )
        unique_run_ids: list[str] = []
        seen_run_ids: set[str] = set()
        for candidate in collector_candidates:
            run_id = str(candidate.get("run_id") or "").strip()
            if not run_id or run_id in seen_run_ids:
                continue
            seen_run_ids.add(run_id)
            unique_run_ids.append(run_id)

        run_rows: list[dict[str, Any]] = []
        for run_id in unique_run_ids:
            run_record = history_store.get_run(run_id)
            if run_record is None:
                summary_row = {
                    "run_id": run_id,
                    "generated_at": None,
                    "symbol": None,
                    "strategy": None,
                    "profile": None,
                    "status": "unsupported",
                    "fidelity": "unsupported",
                    "reason": "scan_run_missing",
                    "exact_match": None,
                    "stored_candidate_count": None,
                    "replayed_candidate_count": None,
                    "matched_candidate_count": None,
                    "stored_only_count": None,
                    "replayed_only_count": None,
                    "rank_change_count": None,
                    "field_drift_count": None,
                    "artifact_path": None,
                }
            else:
                replay_payload = _build_replay_payload_for_run(
                    history_store=history_store,
                    run=dict(run_record),
                )
                summary_row = _replay_run_summary(replay_payload)
            run_rows.append(summary_row)
            run_status_counts[str(summary_row.get("status") or "unknown")] += 1
            run_fidelity_counts[str(summary_row.get("fidelity") or "unknown")] += 1
            if summary_row.get("status") == "unsupported":
                unsupported_run_count += 1
            elif summary_row.get("exact_match") is True:
                exact_match_run_count += 1
            else:
                mismatch_run_count += 1

        cycle_status, cycle_exact_match = _cycle_replay_status(run_rows)
        cycle_status_counts[cycle_status] += 1
        result_payload = dict(automation_run.get("result") or {})
        cycle_rows.append(
            {
                "session_date": automation_run.get("session_date"),
                "started_at": automation_run.get("started_at"),
                "completed_at": automation_run.get("completed_at"),
                "automation_run_id": automation_run.get("automation_run_id"),
                "cycle_id": automation_run.get("cycle_id"),
                "label": automation_run.get("label"),
                "status": cycle_status,
                "exact_match": cycle_exact_match,
                "trigger_type": automation_run.get("trigger_type"),
                "candidate_symbol_count": result_payload.get("candidate_symbol_count"),
                "opportunity_count": result_payload.get("opportunity_count"),
                "collector_candidate_count": len(collector_candidates),
                "scan_run_count": len(run_rows),
                "runs": run_rows,
            }
        )

    summary = {
        "cycle_count": len(cycle_rows),
        "cycle_with_candidates_count": len(
            [row for row in cycle_rows if int(row.get("collector_candidate_count") or 0) > 0]
        ),
        "scan_run_count": sum(int(row.get("scan_run_count") or 0) for row in cycle_rows),
        "exact_match_cycle_count": int(cycle_status_counts.get("exact_match", 0)),
        "mismatch_cycle_count": int(cycle_status_counts.get("mismatch", 0)),
        "unsupported_cycle_count": int(cycle_status_counts.get("unsupported", 0)),
        "no_scan_run_cycle_count": int(cycle_status_counts.get("no_scan_runs", 0)),
        "exact_match_run_count": exact_match_run_count,
        "mismatch_run_count": mismatch_run_count,
        "unsupported_run_count": unsupported_run_count,
        "cycle_status_counts": dict(sorted(cycle_status_counts.items())),
        "run_status_counts": dict(sorted(run_status_counts.items())),
        "run_fidelity_counts": dict(sorted(run_fidelity_counts.items())),
    }
    status = "completed" if cycle_rows else "no_cycles"
    return {
        "status": status,
        "source": "stored",
        "target": {
            "bot_id": bot_id,
            "automation_id": automation_id,
            "start_date": start_date,
            "end_date": end_date,
            "cycle_limit": cycle_limit,
        },
        "summary": summary,
        "cycles": cycle_rows,
    }


__all__ = ["build_replay_payload", "build_replay_range_payload"]
