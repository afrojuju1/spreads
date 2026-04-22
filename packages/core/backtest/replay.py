from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
import os
from typing import Any, Mapping

from core.common import env_or_die, load_local_env
from core.db.decorators import with_storage
from core.integrations.calendar_events import build_calendar_event_resolver
from core.integrations.greeks import build_local_greeks_provider
from core.services.alpaca import create_alpaca_client_from_env
from core.services.automations import cadence_minutes
from core.services.automation_runtime import resolve_entry_runtime, resolve_entry_runtimes
from core.services.bot_analytics import evaluate_entry_controls
from core.services.entry_planner import plan_entry_selection
from core.services.live_selection import select_live_opportunities
from core.services.replay_filters import candidate_matches_filter
from core.services.scanners.config import (
    apply_scan_evaluation_context,
    parse_args as parse_scanner_args,
)
from core.services.scanners.historical import (
    ALPACA_OPTIONS_HISTORY_START,
    build_historical_symbol_market_slice_from_session_data,
    build_historical_symbol_session_data_from_alpaca,
)
from core.services.option_structures import (
    candidate_legs,
    legs_identity_key,
    normalize_strategy_family,
    payload_display_fields,
)
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
from core.services.runtime_candidate_filters import build_runtime_candidate_filter
from core.services.strategy_builders import (
    build_entry_runtime_symbol_candidates_from_market_slice,
    build_market_slice_args,
)
from core.services.market_dates import NEW_YORK


_COMPARABLE_FIELDS = (
    "width",
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


def _candidate_strategy(candidate: Mapping[str, Any]) -> str:
    return str(
        candidate.get("strategy") or candidate.get("strategy_family") or ""
    ).strip()


def _rounded_float(value: Any, *, places: int = 4) -> float | None:
    if value in (None, ""):
        return None
    try:
        return round(float(value), places)
    except (TypeError, ValueError):
        return None


def _is_legacy_iron_condor_candidate(candidate: Mapping[str, Any]) -> bool:
    return _candidate_strategy(candidate) == "iron_condor" and 0 < len(
        candidate_legs(candidate)
    ) < 4


def _legacy_iron_condor_match_key(
    candidate: Mapping[str, Any],
) -> tuple[Any, ...] | None:
    if _candidate_strategy(candidate) != "iron_condor":
        return None
    display = payload_display_fields(candidate)
    expiration_date = str(candidate.get("expiration_date") or "").strip()
    short_symbol = str(
        display.get("short_symbol") or candidate.get("short_symbol") or ""
    ).strip()
    long_symbol = str(
        display.get("long_symbol") or candidate.get("long_symbol") or ""
    ).strip()
    if not expiration_date or not short_symbol or not long_symbol:
        return None
    return (
        "legacy_iron_condor",
        expiration_date,
        short_symbol,
        long_symbol,
        _rounded_float(candidate.get("width")),
        _rounded_float(candidate.get("midpoint_credit")),
        _rounded_float(candidate.get("natural_credit")),
        _rounded_float(candidate.get("breakeven")),
        _rounded_float(candidate.get("max_profit"), places=2),
        _rounded_float(candidate.get("max_loss"), places=2),
        _rounded_float(candidate.get("return_on_risk")),
    )


def _replay_comparison_mode(
    *, run: Mapping[str, Any], stored_candidates: list[dict[str, Any]]
) -> str:
    if str(run.get("strategy") or "").strip() != "iron_condor":
        return "full_identity"
    if any(_is_legacy_iron_condor_candidate(row) for row in stored_candidates):
        return "legacy_iron_condor_compat"
    return "full_identity"


def _candidate_match_key(candidate: Mapping[str, Any], *, mode: str) -> Any:
    if mode == "legacy_iron_condor_compat":
        compat_key = _legacy_iron_condor_match_key(candidate)
        if compat_key is not None:
            return compat_key
    return _candidate_identity(candidate)


def _candidate_match_label(
    *,
    stored_row: Mapping[str, Any] | None,
    replayed_row: Mapping[str, Any] | None,
    fallback_key: Any,
) -> str:
    if replayed_row is not None:
        return _candidate_identity(replayed_row)
    if stored_row is not None:
        return _candidate_identity(stored_row)
    return str(fallback_key)


def _candidate_summary(candidate: Mapping[str, Any], *, rank: int | None = None) -> dict[str, Any]:
    payload = dict(candidate)
    return {
        "rank": rank,
        "identity": _candidate_identity(payload),
        "underlying_symbol": payload.get("underlying_symbol"),
        "strategy": payload.get("strategy"),
        "expiration_date": payload.get("expiration_date"),
        **payload_display_fields(payload),
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


@lru_cache(maxsize=1)
def _cached_entry_runtimes() -> tuple[Any, ...]:
    return tuple(resolve_entry_runtimes())


def _upgrade_legacy_runtime_candidate_filter(
    *,
    run: Mapping[str, Any],
    candidate_filter: Mapping[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(candidate_filter or {})
    if not payload:
        return payload
    if any(
        key in payload
        for key in (
            "strategy_id",
            "symbols",
            "dte_min",
            "dte_max",
            "short_delta_min",
            "short_delta_max",
            "min_open_interest",
            "max_leg_spread_pct_mid",
            "min_return_on_risk",
            "ranking_policy",
            "entry_recipe_refs",
        )
    ):
        return payload

    allowed_widths = {
        round(float(value), 4)
        for value in list(payload.get("allowed_widths") or [])
        if value not in (None, "")
    }
    run_symbol = str(run.get("symbol") or "").upper()
    run_profile = str(run.get("profile") or "").strip()
    run_strategy_family = normalize_strategy_family(run.get("strategy"))

    matching_runtimes: list[Any] = []
    for runtime in _cached_entry_runtimes():
        if normalize_strategy_family(runtime.strategy_id) != run_strategy_family:
            continue
        if run_symbol and run_symbol not in {str(symbol).upper() for symbol in runtime.symbols}:
            continue
        if (
            run_profile
            and str(runtime.build_settings.scanner_profile or "").strip() != run_profile
        ):
            continue
        runtime_widths = {
            round(float(value), 4) for value in runtime.build_settings.width_points
        }
        if allowed_widths and runtime_widths and allowed_widths != runtime_widths:
            continue
        matching_runtimes.append(runtime)

    if len(matching_runtimes) != 1:
        return payload
    upgraded = build_runtime_candidate_filter(matching_runtimes[0])
    if run_symbol:
        upgraded["symbols"] = [run_symbol]
    return upgraded


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
    candidate_filter = _upgrade_legacy_runtime_candidate_filter(
        run=resolved_run,
        candidate_filter=artifact.get("candidate_filter"),
    )

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
        dict(candidate.to_payload())
        for candidate in replayed_candidates
        if candidate_matches_filter(dict(candidate.to_payload()), candidate_filter)
    ]

    comparison_mode = _replay_comparison_mode(
        run=resolved_run,
        stored_candidates=stored_candidates,
    )

    stored_rank_by_identity: dict[Any, int] = {}
    stored_by_identity: dict[Any, dict[str, Any]] = {}
    for index, row in enumerate(stored_candidates, start=1):
        identity = _candidate_match_key(row, mode=comparison_mode)
        stored_rank_by_identity[identity] = index
        stored_by_identity[identity] = dict(row)

    replayed_rank_by_identity: dict[Any, int] = {}
    replayed_by_identity: dict[Any, dict[str, Any]] = {}
    for index, row in enumerate(replayed_rows, start=1):
        identity = _candidate_match_key(row, mode=comparison_mode)
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
        stored_row = stored_by_identity[identity]
        replayed_row = replayed_by_identity[identity]
        identity_label = _candidate_match_label(
            stored_row=stored_row,
            replayed_row=replayed_row,
            fallback_key=identity,
        )
        if stored_rank != replayed_rank:
            rank_changes.append(
                {
                    "identity": identity_label,
                    "stored_rank": stored_rank,
                    "replayed_rank": replayed_rank,
                }
            )
        for field in _COMPARABLE_FIELDS:
            if _values_differ(stored_row.get(field), replayed_row.get(field)):
                field_drifts.append(
                    {
                        "identity": identity_label,
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
        "comparison_mode": comparison_mode,
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


def _normalize_alpaca_sample_mode(sample_mode: str | None) -> str:
    normalized = str(sample_mode or "intraday").strip().lower()
    if normalized not in {"intraday", "eod"}:
        raise ValueError(f"Unsupported Alpaca replay sample_mode: {sample_mode}")
    return normalized


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour_text, _, minute_text = str(value).partition(":")
    if not _:
        raise ValueError(f"Invalid HH:MM time: {value}")
    return int(hour_text), int(minute_text)


def _schedule_cycle_timestamps(
    *,
    session_day: date,
    schedule: Mapping[str, Any],
) -> list[datetime]:
    cadence = max(int(cadence_minutes(dict(schedule))), 1)
    start_text = str(schedule.get("start_time_et") or "09:30")
    end_text = str(schedule.get("end_time_et") or "16:00")
    start_hour, start_minute = _parse_hhmm(start_text)
    end_hour, end_minute = _parse_hhmm(end_text)
    current = datetime.combine(
        session_day,
        time(start_hour, start_minute),
        tzinfo=NEW_YORK,
    )
    end_at = datetime.combine(
        session_day,
        time(end_hour, end_minute),
        tzinfo=NEW_YORK,
    )
    timestamps: list[datetime] = []
    while current <= end_at:
        timestamps.append(current.astimezone(UTC))
        current += timedelta(minutes=cadence)
    return timestamps


def _timestamp_within_schedule_window(
    *,
    schedule: Mapping[str, Any],
    timestamp: datetime,
) -> bool:
    current = timestamp.astimezone(NEW_YORK)
    if bool(schedule.get("market_hours_only", False)) and not (
        (9, 30) <= (current.hour, current.minute) <= (16, 0)
    ):
        return False
    start_time = schedule.get("start_time_et")
    if start_time:
        start_hour, start_minute = _parse_hhmm(str(start_time))
        if (current.hour, current.minute) < (start_hour, start_minute):
            return False
    end_time = schedule.get("end_time_et")
    if end_time:
        end_hour, end_minute = _parse_hhmm(str(end_time))
        if (current.hour, current.minute) > (end_hour, end_minute):
            return False
    return True


def _recorded_cycle_specs(
    *,
    signal_store: Any,
    bot_id: str,
    automation_id: str,
    session_date: str,
    limit: int,
    schedule: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    rows = [
        dict(row)
        for row in signal_store.list_automation_runs(
            bot_id=bot_id,
            automation_id=automation_id,
            session_date=session_date,
            limit=max(int(limit), 1),
        )
    ]
    cycle_specs: list[dict[str, Any]] = []
    clipped_count = 0
    for row in rows:
        started_at = _parse_datetime(row.get("started_at"))
        if started_at is None:
            continue
        if not _timestamp_within_schedule_window(
            schedule=schedule,
            timestamp=started_at,
        ):
            clipped_count += 1
            continue
        cycle_specs.append(
            {
                "session_date": session_date,
                "sample_source": "recorded",
                "as_of": started_at.astimezone(UTC),
                "generated_at": _render_utc(started_at.astimezone(UTC)),
                "automation_run_id": row.get("automation_run_id"),
                "cycle_id": row.get("cycle_id")
                or (
                    f"alpaca:recorded:{bot_id}:{automation_id}:{session_date}:"
                    f"{started_at.astimezone(NEW_YORK).strftime('%H%M')}"
                ),
                "label": row.get("label")
                or f"alpaca_replay:{bot_id}:{automation_id}",
                "trigger_type": row.get("trigger_type") or "alpaca_historical_recorded",
            }
        )
    return sorted(
        cycle_specs,
        key=lambda item: (
            str(item.get("generated_at") or ""),
            str(item.get("cycle_id") or ""),
        ),
    ), clipped_count


def _eod_cycle_specs(
    *,
    bot_id: str,
    automation_id: str,
    session_date: str,
) -> list[dict[str, Any]]:
    session_day = date.fromisoformat(session_date)
    timestamp = _market_close_timestamp(session_day)
    return [
        {
            "session_date": session_date,
            "sample_source": "eod",
            "as_of": timestamp,
            "generated_at": _render_utc(timestamp),
            "automation_run_id": None,
            "cycle_id": (
                f"alpaca:eod:{bot_id}:{automation_id}:{session_date}:"
                f"{timestamp.astimezone(NEW_YORK).strftime('%H%M')}"
            ),
            "label": f"alpaca_replay:{bot_id}:{automation_id}",
            "trigger_type": "alpaca_historical_eod",
        }
    ]


def _session_cycle_specs(
    *,
    signal_store: Any,
    runtime: Any,
    bot_id: str,
    automation_id: str,
    session_date: str,
    limit: int,
    sample_mode: str,
) -> tuple[list[dict[str, Any]], int]:
    normalized_sample_mode = _normalize_alpaca_sample_mode(sample_mode)
    if normalized_sample_mode == "eod":
        return _eod_cycle_specs(
            bot_id=bot_id,
            automation_id=automation_id,
            session_date=session_date,
        )[: max(int(limit), 1)], 0

    recorded, clipped_count = _recorded_cycle_specs(
        signal_store=signal_store,
        bot_id=bot_id,
        automation_id=automation_id,
        session_date=session_date,
        limit=limit,
        schedule=runtime.automation.automation.schedule,
    )
    if recorded:
        return recorded[: max(int(limit), 1)], clipped_count

    session_day = date.fromisoformat(session_date)
    cycle_specs: list[dict[str, Any]] = []
    for timestamp in _schedule_cycle_timestamps(
        session_day=session_day,
        schedule=runtime.automation.automation.schedule,
    )[: max(int(limit), 1)]:
        cycle_specs.append(
            {
                "session_date": session_date,
                "sample_source": "scheduled",
                "as_of": timestamp,
                "generated_at": _render_utc(timestamp),
                "automation_run_id": None,
                "cycle_id": (
                    f"alpaca:scheduled:{bot_id}:{automation_id}:{session_date}:"
                    f"{timestamp.astimezone(NEW_YORK).strftime('%H%M')}"
                ),
                "label": f"alpaca_replay:{bot_id}:{automation_id}",
                "trigger_type": "alpaca_historical_schedule",
            }
        )
    return cycle_specs, clipped_count


def _merge_count_map(target: Counter[str], source: Mapping[str, Any] | None) -> None:
    for key, value in dict(source or {}).items():
        try:
            amount = int(value)
        except (TypeError, ValueError):
            continue
        target[str(key)] += amount


def _count_row_field(rows: list[dict[str, Any]], *, field: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        value = row.get(field)
        normalized = "unknown" if value in (None, "") else str(value)
        counts[normalized] += 1
    return dict(sorted(counts.items()))


def _count_row_list_field(rows: list[dict[str, Any]], *, field: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        values = row.get(field)
        if not isinstance(values, (list, tuple)):
            continue
        for value in values:
            normalized = str(value or "").strip()
            if normalized:
                counts[normalized] += 1
    return dict(sorted(counts.items()))


def _build_cycle_diagnostics(
    *,
    symbol_bundles: list[dict[str, Any]],
    selection: Mapping[str, Any],
    opportunities: list[dict[str, Any]],
    entry_eligible_opportunities: list[dict[str, Any]],
) -> dict[str, Any]:
    builder_setup_status_counts: Counter[str] = Counter()
    builder_calendar_status_counts: Counter[str] = Counter()
    builder_calendar_reason_counts: Counter[str] = Counter()
    builder_data_status_counts: Counter[str] = Counter()
    builder_data_reason_counts: Counter[str] = Counter()
    builder_runtime_filter_reason_counts: Counter[str] = Counter()
    symbol_raw_candidate_counts: dict[str, int] = {}
    symbol_postprocess_candidate_counts: dict[str, int] = {}
    symbol_candidate_counts: dict[str, int] = {}
    symbol_selection_input_candidate_counts: dict[str, int] = {}
    symbol_filter_rejected_counts: dict[str, int] = {}
    builder_raw_candidate_count = 0
    builder_postprocess_candidate_count = 0
    candidate_count = 0
    selection_input_candidate_count = 0

    for bundle in symbol_bundles:
        symbol = str(bundle.get("symbol") or "")
        replay_details = dict(bundle.get("replay_details") or {})
        all_rows = [dict(row) for row in list(bundle.get("all_rows") or [])]
        input_rows = [dict(row) for row in list(bundle.get("rows") or [])]
        raw_candidate_count = int(replay_details.get("raw_candidate_count") or 0)
        postprocess_candidate_count = int(
            replay_details.get("postprocess_candidate_count") or 0
        )

        builder_raw_candidate_count += raw_candidate_count
        builder_postprocess_candidate_count += postprocess_candidate_count
        candidate_count += len(all_rows)
        selection_input_candidate_count += len(input_rows)

        if symbol:
            symbol_raw_candidate_counts[symbol] = raw_candidate_count
            symbol_postprocess_candidate_counts[symbol] = postprocess_candidate_count
            symbol_candidate_counts[symbol] = len(all_rows)
            symbol_selection_input_candidate_counts[symbol] = len(input_rows)
            symbol_filter_rejected_counts[symbol] = max(
                postprocess_candidate_count - len(all_rows),
                0,
            )

        _merge_count_map(
            builder_setup_status_counts,
            replay_details.get("setup_status_counts"),
        )
        _merge_count_map(
            builder_calendar_status_counts,
            replay_details.get("calendar_status_counts"),
        )
        _merge_count_map(
            builder_calendar_reason_counts,
            replay_details.get("calendar_reason_counts"),
        )
        _merge_count_map(
            builder_data_status_counts,
            replay_details.get("data_status_counts"),
        )
        _merge_count_map(
            builder_data_reason_counts,
            replay_details.get("data_reason_counts"),
        )
        _merge_count_map(
            builder_runtime_filter_reason_counts,
            bundle.get("runtime_filter_reason_counts"),
        )

    scored_symbol_candidates = dict(selection.get("symbol_candidates") or {})
    scored_candidates = [
        dict(candidate)
        for rows in scored_symbol_candidates.values()
        for candidate in list(rows or [])
    ]

    return {
        "builder_raw_candidate_count": builder_raw_candidate_count,
        "builder_postprocess_candidate_count": builder_postprocess_candidate_count,
        "candidate_count": candidate_count,
        "selection_input_candidate_count": selection_input_candidate_count,
        "entry_eligible_opportunity_count": len(entry_eligible_opportunities),
        "symbol_raw_candidate_counts": dict(sorted(symbol_raw_candidate_counts.items())),
        "symbol_postprocess_candidate_counts": dict(
            sorted(symbol_postprocess_candidate_counts.items())
        ),
        "symbol_candidate_counts": dict(sorted(symbol_candidate_counts.items())),
        "symbol_selection_input_candidate_counts": dict(
            sorted(symbol_selection_input_candidate_counts.items())
        ),
        "symbol_filter_rejected_counts": dict(
            sorted(symbol_filter_rejected_counts.items())
        ),
        "setup_status_counts": dict(sorted(builder_setup_status_counts.items())),
        "calendar_status_counts": dict(sorted(builder_calendar_status_counts.items())),
        "calendar_reason_counts": dict(sorted(builder_calendar_reason_counts.items())),
        "data_status_counts": dict(sorted(builder_data_status_counts.items())),
        "data_reason_counts": dict(sorted(builder_data_reason_counts.items())),
        "runtime_filter_reason_counts": dict(
            sorted(builder_runtime_filter_reason_counts.items())
        ),
        "selection_state_counts": _count_row_field(
            opportunities,
            field="selection_state",
        ),
        "entry_eligible_selection_state_counts": _count_row_field(
            entry_eligible_opportunities,
            field="selection_state",
        ),
        "eligibility_counts": _count_row_field(opportunities, field="eligibility"),
        "scoring_state_counts": _count_row_field(
            scored_candidates,
            field="scoring_state",
        ),
        "scoring_state_reason_counts": _count_row_field(
            scored_candidates,
            field="scoring_state_reason",
        ),
        "scoring_blocker_counts": _count_row_list_field(
            scored_candidates,
            field="scoring_blockers",
        ),
        "execution_blocker_counts": _count_row_list_field(
            scored_candidates,
            field="execution_blockers",
        ),
    }


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
    return session_dates


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


def _option_bar_timeframe(schedule: Mapping[str, Any]) -> str:
    return _option_bar_timeframe_for_sample_mode(
        schedule=schedule,
        sample_mode="intraday",
    )


def _option_bar_timeframe_for_sample_mode(
    *,
    schedule: Mapping[str, Any],
    sample_mode: str,
) -> str:
    normalized_sample_mode = _normalize_alpaca_sample_mode(sample_mode)
    if normalized_sample_mode == "eod":
        return "1Day"
    cadence = max(int(cadence_minutes(dict(schedule))), 1)
    if cadence < 60:
        return f"{cadence}Min"
    if cadence % 60 == 0 and cadence // 60 <= 23:
        return f"{cadence // 60}Hour"
    return "1Hour"


def _build_alpaca_replay_range_payload(
    *,
    db_target: str,
    bot_id: str,
    automation_id: str,
    start_date: str,
    end_date: str,
    limit: int,
    storage: Any,
    sample_mode: str = "intraday",
) -> dict[str, Any]:
    signal_store = storage.signals
    runtime = resolve_entry_runtime(bot_id=bot_id, automation_id=automation_id)
    cycle_limit = max(int(limit), 1)
    normalized_sample_mode = _normalize_alpaca_sample_mode(sample_mode)
    if not runtime.symbols:
        return {
            "status": "no_cycles",
            "source": "alpaca",
            "target": {
                "bot_id": bot_id,
                "automation_id": automation_id,
                "start_date": start_date,
                "end_date": end_date,
                "cycle_limit": cycle_limit,
                "sample_mode": normalized_sample_mode,
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
    )
    replay_label = f"alpaca_replay:{bot_id}:{automation_id}"
    option_bar_timeframe = _option_bar_timeframe_for_sample_mode(
        schedule=runtime.automation.automation.schedule,
        sample_mode=normalized_sample_mode,
    )
    include_intraday_stock_bars = normalized_sample_mode == "intraday"
    fidelity_reason = (
        "alpaca_intraday_option_bars_with_synthetic_quotes_and_local_greeks"
        if normalized_sample_mode == "intraday"
        else "alpaca_daily_option_bars_with_synthetic_quotes_and_local_greeks"
    )

    cycle_rows: list[dict[str, Any]] = []
    cycle_status_counts: Counter[str] = Counter()
    sample_source_counts: Counter[str] = Counter()
    clipped_recorded_cycle_count = 0
    exact_match_run_count = 0
    mismatch_run_count = 0
    unsupported_run_count = 0
    total_raw_candidate_count = 0
    total_postprocess_candidate_count = 0
    total_candidate_count = 0
    total_selection_input_candidate_count = 0
    total_opportunity_count = 0
    total_entry_eligible_opportunity_count = 0
    selected_cycle_count = 0
    cycles_with_raw_candidates_count = 0
    cycles_with_candidates_count = 0
    cycles_with_opportunities_count = 0
    cycles_with_entry_eligible_opportunities_count = 0

    for session_date in session_dates:
        if len(cycle_rows) >= cycle_limit:
            break
        session_day = date.fromisoformat(session_date)
        cycle_specs, clipped_count = _session_cycle_specs(
            signal_store=signal_store,
            runtime=runtime,
            bot_id=bot_id,
            automation_id=automation_id,
            session_date=session_date,
            limit=cycle_limit - len(cycle_rows),
            sample_mode=normalized_sample_mode,
        )
        clipped_recorded_cycle_count += clipped_count
        if not cycle_specs:
            continue

        session_market_slice_args_by_symbol: dict[str, Any] = {}
        session_data_by_symbol: dict[str, Any] = {}
        session_symbol_failures: dict[str, dict[str, Any]] = {}
        for symbol in runtime.symbols:
            normalized_symbol = str(symbol).upper()
            try:
                market_slice_args = build_market_slice_args(
                    symbol=normalized_symbol,
                    base_scanner_args=parse_scanner_args([]),
                    runtimes=[runtime],
                )
                apply_scan_evaluation_context(
                    market_slice_args,
                    evaluation_timestamp=_market_close_timestamp(session_day),
                    evaluation_date=session_date,
                )
                session_market_slice_args_by_symbol[normalized_symbol] = market_slice_args
                session_data_by_symbol[normalized_symbol] = (
                    build_historical_symbol_session_data_from_alpaca(
                        symbol=normalized_symbol,
                        symbol_args=market_slice_args,
                        client=client,
                        session_date=session_day,
                        option_bar_timeframe=option_bar_timeframe,
                        include_intraday_stock_bars=include_intraday_stock_bars,
                    )
                )
            except Exception as exc:
                session_symbol_failures[normalized_symbol] = {
                    "symbol": normalized_symbol,
                    "error": str(exc).splitlines()[0],
                }

        previous_promotable: dict[str, dict[str, Any]] = {}
        previous_selection_memory: dict[str, dict[str, Any]] = {}
        for cycle_spec in cycle_specs:
            as_of = cycle_spec["as_of"]
            generated_at = str(cycle_spec["generated_at"])
            session_scanner_args = apply_scan_evaluation_context(
                parse_scanner_args([]),
                evaluation_timestamp=as_of,
                evaluation_date=session_date,
            )
            symbol_failures = [
                dict(row) for row in sorted(session_symbol_failures.values(), key=lambda item: str(item.get("symbol") or ""))
            ]
            symbol_bundles: list[dict[str, Any]] = []
            owner_candidates: dict[str, list[dict[str, Any]]] = {
                str(symbol).upper(): [] for symbol in runtime.symbols
            }

            for symbol in runtime.symbols:
                normalized_symbol = str(symbol).upper()
                session_data = session_data_by_symbol.get(normalized_symbol)
                market_slice_args = session_market_slice_args_by_symbol.get(
                    normalized_symbol
                )
                if session_data is None or market_slice_args is None:
                    continue
                try:
                    market_slice = build_historical_symbol_market_slice_from_session_data(
                        session_data=session_data,
                        symbol_args=market_slice_args,
                        greeks_provider=greeks_provider,
                        as_of=as_of,
                        use_latest_option_bar_snapshot=(
                            normalized_sample_mode == "eod"
                        ),
                    )
                    bundle = build_entry_runtime_symbol_candidates_from_market_slice(
                        runtime=runtime,
                        symbol=normalized_symbol,
                        base_scanner_args=session_scanner_args,
                        calendar_resolver=calendar_resolver,
                        market_slice=market_slice,
                        per_runtime_limit=25,
                        history_store=None,
                        session_label=replay_label,
                    )
                    symbol_bundles.append(bundle)
                    owner_candidates[normalized_symbol] = [
                        dict(candidate) for candidate in list(bundle.get("rows") or [])
                    ]
                except Exception as exc:
                    symbol_failures.append(
                        {
                            "symbol": normalized_symbol,
                            "error": str(exc).splitlines()[0],
                        }
                    )

            selection = select_live_opportunities(
                label=replay_label,
                cycle_id=str(cycle_spec["cycle_id"]),
                generated_at=generated_at,
                symbol_candidates=owner_candidates,
                previous_promotable=previous_promotable,
                previous_selection_memory=previous_selection_memory,
                top_promotable=max(
                    int(getattr(session_scanner_args, "top", 10) or 10),
                    1,
                ),
                top_monitor=max(
                    int(getattr(session_scanner_args, "top", 10) or 10),
                    1,
                ),
                profile=runtime.build_settings.scanner_profile,
            )
            opportunities = _with_opportunity_ids(
                [dict(row) for row in list(selection.get("opportunities") or [])],
                bot_id=bot_id,
                automation_id=automation_id,
                session_date=session_date,
            )
            entry_eligible_opportunities = [
                dict(row)
                for row in opportunities
                if str(row.get("selection_state") or "").strip().lower()
                == "promotable"
            ]
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
                min_score=float(
                    runtime.trigger_policy.get("min_opportunity_score") or 0.0
                ),
                eligible_selection_states=("promotable",),
            )
            selected = (
                dict(plan["selected"])
                if isinstance(plan.get("selected"), Mapping)
                else None
            )
            entry_eligible_opportunity_count = int(
                plan.get("eligible_opportunity_count") or 0
            )

            flattened_candidates = [
                dict(candidate)
                for bundle in symbol_bundles
                for candidate in list(bundle.get("all_rows") or [])
            ]
            flattened_candidates.sort(key=_candidate_sort_key, reverse=True)
            diagnostics = _build_cycle_diagnostics(
                symbol_bundles=symbol_bundles,
                selection=selection,
                opportunities=opportunities,
                entry_eligible_opportunities=entry_eligible_opportunities,
            )
            raw_candidate_count = int(
                diagnostics.get("builder_raw_candidate_count") or 0
            )
            postprocess_candidate_count = int(
                diagnostics.get("builder_postprocess_candidate_count") or 0
            )
            candidate_count = int(diagnostics.get("candidate_count") or 0)
            selection_input_candidate_count = int(
                diagnostics.get("selection_input_candidate_count") or 0
            )
            opportunity_count = len(opportunities)
            cycle_status = _alpaca_cycle_status(
                selected=selected,
                opportunities=opportunities,
                candidate_count=candidate_count,
                controls_allowed=controls_allowed,
                symbol_failures=symbol_failures,
            )
            cycle_status_counts[cycle_status] += 1
            sample_source_counts[str(cycle_spec["sample_source"])] += 1
            total_raw_candidate_count += raw_candidate_count
            total_postprocess_candidate_count += postprocess_candidate_count
            total_candidate_count += candidate_count
            total_selection_input_candidate_count += selection_input_candidate_count
            total_opportunity_count += opportunity_count
            total_entry_eligible_opportunity_count += entry_eligible_opportunity_count
            if raw_candidate_count > 0:
                cycles_with_raw_candidates_count += 1
            if candidate_count > 0:
                cycles_with_candidates_count += 1
            if opportunity_count > 0:
                cycles_with_opportunities_count += 1
            if entry_eligible_opportunity_count > 0:
                cycles_with_entry_eligible_opportunities_count += 1
            if selected is not None:
                selected_cycle_count += 1

            cycle_rows.append(
                {
                    "session_date": session_date,
                    "started_at": generated_at,
                    "completed_at": generated_at,
                    "automation_run_id": cycle_spec.get("automation_run_id"),
                    "cycle_id": cycle_spec.get("cycle_id"),
                    "label": cycle_spec.get("label"),
                    "status": cycle_status,
                    "sample_source": cycle_spec.get("sample_source"),
                    "exact_match": None,
                    "fidelity": "reduced",
                    "fidelity_reason": fidelity_reason,
                    "trigger_type": cycle_spec.get("trigger_type"),
                    "candidate_symbol_count": sum(
                        1
                        for rows in owner_candidates.values()
                        if len(list(rows or [])) > 0
                    ),
                    "raw_candidate_count": raw_candidate_count,
                    "postprocess_candidate_count": postprocess_candidate_count,
                    "candidate_count": candidate_count,
                    "selection_input_candidate_count": selection_input_candidate_count,
                    "opportunity_count": opportunity_count,
                    "entry_eligible_opportunity_count": entry_eligible_opportunity_count,
                    "discovery_run_candidate_count": candidate_count,
                    "scan_run_count": 0,
                    "controls_allowed": controls_allowed,
                    "controls_reason": controls_reason,
                    "selected": (
                        None if selected is None else _opportunity_summary(selected)
                    ),
                    "top_opportunities": [
                        _opportunity_summary(row) for row in opportunities[:10]
                    ],
                    "top_candidates": [
                        _candidate_summary(row, rank=index)
                        for index, row in enumerate(
                            flattened_candidates[:10],
                            start=1,
                        )
                    ],
                    "symbol_failures": symbol_failures,
                    "diagnostics": diagnostics,
                    "symbol_summaries": [
                        {
                            "symbol": str(bundle.get("symbol") or ""),
                            "raw_candidate_count": int(
                                dict(bundle.get("replay_details") or {}).get(
                                    "raw_candidate_count"
                                )
                                or 0
                            ),
                            "postprocess_candidate_count": int(
                                dict(bundle.get("replay_details") or {}).get(
                                    "postprocess_candidate_count"
                                )
                                or 0
                            ),
                            "candidate_count": len(list(bundle.get("all_rows") or [])),
                            "selection_input_candidate_count": len(
                                list(bundle.get("rows") or [])
                            ),
                            "top_candidate": (
                                None
                                if not list(bundle.get("all_rows") or [])
                                else _candidate_summary(
                                    list(bundle.get("all_rows") or [])[0],
                                    rank=1,
                                )
                            ),
                        }
                        for bundle in sorted(
                            symbol_bundles,
                            key=lambda item: str(item.get("symbol") or ""),
                        )
                    ],
                    "runs": [],
                }
            )

            previous_promotable = {
                str(candidate.get("underlying_symbol") or "").upper(): dict(candidate)
                for candidate in list(selection.get("promotable_candidates") or [])
                if str(candidate.get("underlying_symbol") or "").strip()
            }
            previous_selection_memory = {
                str(symbol).upper(): dict(state)
                for symbol, state in dict(selection.get("selection_memory") or {}).items()
                if isinstance(symbol, str) and isinstance(state, Mapping)
            }

    return {
        "status": "completed" if cycle_rows else "no_cycles",
        "source": "alpaca",
        "target": {
            "bot_id": bot_id,
            "automation_id": automation_id,
            "start_date": start_date,
            "end_date": end_date,
            "cycle_limit": cycle_limit,
            "sample_mode": normalized_sample_mode,
            "fidelity": "reduced",
        },
        "summary": {
            "cycle_count": len(cycle_rows),
            "cycle_with_raw_candidates_count": cycles_with_raw_candidates_count,
            "cycle_with_candidates_count": cycles_with_candidates_count,
            "cycle_with_opportunities_count": cycles_with_opportunities_count,
            "cycle_with_entry_eligible_opportunities_count": (
                cycles_with_entry_eligible_opportunities_count
            ),
            "scan_run_count": 0,
            "raw_candidate_count": total_raw_candidate_count,
            "postprocess_candidate_count": total_postprocess_candidate_count,
            "candidate_count": total_candidate_count,
            "selection_input_candidate_count": total_selection_input_candidate_count,
            "opportunity_count": total_opportunity_count,
            "entry_eligible_opportunity_count": total_entry_eligible_opportunity_count,
            "selected_cycle_count": selected_cycle_count,
            "exact_match_cycle_count": 0,
            "mismatch_cycle_count": 0,
            "unsupported_cycle_count": int(cycle_status_counts.get("unsupported", 0)),
            "no_scan_run_cycle_count": 0,
            "exact_match_run_count": exact_match_run_count,
            "mismatch_run_count": mismatch_run_count,
            "unsupported_run_count": unsupported_run_count,
            "clipped_recorded_cycle_count": clipped_recorded_cycle_count,
            "sample_source_counts": dict(sorted(sample_source_counts.items())),
            "cycle_status_counts": dict(sorted(cycle_status_counts.items())),
            "run_status_counts": {},
            "run_fidelity_counts": {"reduced": len(cycle_rows)} if cycle_rows else {},
        },
        "cycles": cycle_rows,
        "coverage": {
            "priority_ladder": (
                [
                    "alpaca_historical_option_bars",
                    "automation_schedule_or_recorded_run_timestamps",
                    "inactive_option_contract_metadata",
                    "local_bsm_greeks",
                ]
                if normalized_sample_mode == "intraday"
                else [
                    "alpaca_historical_daily_option_bars",
                    "daily_market_close_sampling",
                    "inactive_option_contract_metadata",
                    "local_bsm_greeks",
                ]
            ),
            "alpaca_options_history_supported_from": ALPACA_OPTIONS_HISTORY_START.isoformat(),
            "quote_reconstruction": "synthetic_from_option_bar_range",
            "option_bar_timeframe": option_bar_timeframe,
            "recorded_cycle_schedule_clipping": normalized_sample_mode == "intraday",
            "stock_intraday_context": (
                "enabled" if include_intraday_stock_bars else "disabled"
            ),
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
    sample_mode: str = "intraday",
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
            sample_mode=sample_mode,
        )
    if normalized_source != "stored":
        raise ValueError(f"Unsupported replay source: {source}")
    if _normalize_alpaca_sample_mode(sample_mode) != "intraday":
        raise ValueError("--sample-mode is only supported with --source alpaca")

    signal_store = storage.signals
    discovery_store = storage.discovery
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
        discovery_run_candidates = (
            []
            if not cycle_id
            else [
                dict(row)
                for row in discovery_store.list_cycle_candidates(cycle_id)
            ]
        )
        unique_run_ids: list[str] = []
        seen_run_ids: set[str] = set()
        for candidate in discovery_run_candidates:
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
                "discovery_run_candidate_count": len(discovery_run_candidates),
                "scan_run_count": len(run_rows),
                "runs": run_rows,
            }
        )

    summary = {
        "cycle_count": len(cycle_rows),
        "cycle_with_candidates_count": len(
            [row for row in cycle_rows if int(row.get("discovery_run_candidate_count") or 0) > 0]
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
