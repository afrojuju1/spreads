from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, date, datetime
from statistics import mean
from typing import Any

from spreads.db.decorators import with_storage
from spreads.domain.opportunity_models import (
    AllocationDecision,
    DecisionReplay,
    ExecutionIntent,
    HorizonIntent,
    Opportunity,
    OpportunityLeg,
    RegimeSnapshot,
    StrategyIntent,
)
from spreads.services.analysis_helpers import (
    candidate_identity,
    resolved_estimated_pnl,
)
from spreads.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    candidate_requires_favorable_setup,
)
from spreads.services.candidate_history_recovery import (
    recover_session_candidates_from_history,
)
from spreads.services.execution import list_session_execution_attempts
from spreads.services.live_pipelines import parse_live_run_scope_id
from spreads.services.opportunity_execution_plan import (
    build_allocation_decisions,
    build_execution_intents,
    execution_complexity,
    rank_opportunities,
)
from spreads.services.positions import enrich_position_row
from spreads.services.opportunity_scoring import (
    build_candidate_opportunity_score,
    candidate_earnings_phase,
    candidate_event_state,
    candidate_event_timing_rule,
    earnings_phase_policy_blockers,
    earnings_phase_policy_preference,
    evaluate_earnings_signal_gate,
)

TOP_TIER_ETF_SYMBOLS = {"SPY", "QQQ", "IWM", "DIA", "GLD", "TLT"}
BROAD_ETF_SYMBOLS = {"XLF", "XLE", "XLI", "XLV"}
CASH_SETTLED_INDEX_SYMBOLS = {"SPX", "XSP", "NDX", "RUT", "VIX"}

PROFILE_TO_STYLE = {
    "0dte": "reactive",
    "weekly": "tactical",
    "core": "carry",
}

HORIZON_BANDS = (
    ("same_day", 0, 0, "daily"),
    ("next_daily", 1, 2, "daily"),
    ("near_term", 3, 12, "weekly"),
    ("post_event", 13, 20, "post_event"),
    ("swing", 21, 45, "weekly"),
    ("carry", 46, 120, "monthly"),
)

RECOVERY_TOP = 12
RECOVERY_PER_STRATEGY = 3

UNKNOWN_BUCKET_ORDER = 99


class OpportunityReplayLookupError(LookupError):
    pass


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ratio_or_none(numerator: Any, denominator: Any) -> float | None:
    resolved_numerator = _as_float(numerator)
    resolved_denominator = _as_float(denominator)
    if (
        resolved_numerator is None
        or resolved_denominator is None
        or resolved_denominator <= 0.0
    ):
        return None
    return round(resolved_numerator / resolved_denominator, 4)


def _entry_return_on_risk_bucket(value: Any) -> tuple[str, int]:
    resolved = _as_float(value)
    if resolved is None:
        return "unknown", UNKNOWN_BUCKET_ORDER
    if resolved < 0.08:
        return "<0.08", 0
    if resolved < 0.10:
        return "0.08-0.10", 1
    if resolved < 0.12:
        return "0.10-0.12", 2
    if resolved < 0.14:
        return "0.12-0.14", 3
    if resolved < 0.16:
        return "0.14-0.16", 4
    if resolved < 0.20:
        return "0.16-0.20", 5
    return "0.20+", 6


def _midpoint_credit_bucket(value: Any) -> tuple[str, int]:
    resolved = _as_float(value)
    if resolved is None:
        return "unknown", UNKNOWN_BUCKET_ORDER
    if resolved < 0.10:
        return "<0.10", 0
    if resolved < 0.15:
        return "0.10-0.14", 1
    if resolved < 0.20:
        return "0.15-0.19", 2
    if resolved < 0.25:
        return "0.20-0.24", 3
    if resolved < 0.35:
        return "0.25-0.34", 4
    if resolved < 0.50:
        return "0.35-0.49", 5
    return "0.50+", 6


def _width_bucket(value: Any) -> tuple[str, int]:
    resolved = _as_float(value)
    if resolved is None:
        return "unknown", UNKNOWN_BUCKET_ORDER
    if resolved <= 1.0:
        return "<=1.00", 0
    if resolved <= 2.0:
        return "1.01-2.00", 1
    if resolved <= 3.0:
        return "2.01-3.00", 2
    if resolved <= 5.0:
        return "3.01-5.00", 3
    return ">5.00", 4


def _dte_bucket(value: Any) -> tuple[str, int]:
    resolved = _as_float(value)
    if resolved is None:
        return "unknown", UNKNOWN_BUCKET_ORDER
    dte = int(resolved)
    if dte <= 0:
        return "0", 0
    if dte <= 2:
        return "1-2", 1
    if dte <= 5:
        return "3-5", 2
    if dte <= 10:
        return "6-10", 3
    if dte <= 20:
        return "11-20", 4
    if dte <= 45:
        return "21-45", 5
    return "46+", 6


def _normalize_legacy_selection_state(value: Any) -> str | None:
    normalized = _as_text(value)
    if normalized == "board":
        return "promotable"
    if normalized == "watchlist":
        return "monitor"
    return normalized


def _legacy_selection_state_from_row(row: Mapping[str, Any]) -> str | None:
    return _normalize_legacy_selection_state(
        row.get("legacy_selection_state", row.get("bucket"))
    )


def _group_value_from_row(
    *,
    dimension: str,
    row: Mapping[str, Any],
) -> str | None:
    group_value = _as_text(row.get("group_value")) or _as_text(row.get("bucket"))
    if dimension == "classification":
        return _normalize_legacy_selection_state(group_value)
    return group_value


def _normalize_score(value: Any, *, default: float = 0.0) -> float:
    parsed = _as_float(value)
    if parsed is None:
        return default
    return _clamp(parsed / 100.0, 0.0, 1.0)


def _style_profile(
    legacy_profile: str | None, *, days_to_expiration: int | None
) -> str:
    normalized = str(legacy_profile or "").strip().lower()
    if normalized in PROFILE_TO_STYLE:
        return PROFILE_TO_STYLE[normalized]
    if days_to_expiration == 0:
        return "reactive"
    if days_to_expiration is not None and days_to_expiration <= 12:
        return "tactical"
    return "carry"


def _product_class(symbol: str) -> str:
    if symbol in CASH_SETTLED_INDEX_SYMBOLS:
        return "cash_settled_index"
    if symbol in TOP_TIER_ETF_SYMBOLS:
        return "top_tier_etf"
    if symbol in BROAD_ETF_SYMBOLS:
        return "broad_etf"
    return "single_name_equity"


def _strategy_family(strategy: str | None) -> str:
    normalized = str(strategy or "").strip().lower()
    return {
        "call_credit": "call_credit_spread",
        "put_credit": "put_credit_spread",
        "call_debit": "call_debit_spread",
        "put_debit": "put_debit_spread",
        "long_straddle": "long_straddle",
        "long_strangle": "long_strangle",
        "long_call": "long_call",
        "long_put": "long_put",
        "iron_condor": "iron_condor",
    }.get(normalized, normalized or "unknown")


def _thesis_direction(family: str) -> str:
    if family in {"put_credit_spread", "call_debit_spread", "long_call"}:
        return "bullish"
    if family in {"call_credit_spread", "put_debit_spread", "long_put"}:
        return "bearish"
    if family == "iron_condor":
        return "neutral"
    return "unknown"


def _horizon_band(days_to_expiration: int | None) -> tuple[str, int, int, str]:
    if days_to_expiration is None:
        return ("near_term", 3, 12, "weekly")
    for band, lower, upper, expiration_type in HORIZON_BANDS:
        if lower <= days_to_expiration <= upper:
            return band, lower, upper, expiration_type
    if days_to_expiration < 0:
        return ("same_day", 0, 0, "daily")
    return ("carry", 46, max(days_to_expiration, 46), "monthly")


def _liquidity_state(candidate: Mapping[str, Any]) -> str:
    fill_ratio = _as_float(candidate.get("fill_ratio")) or 0.0
    min_quote_size = _as_float(candidate.get("min_quote_size")) or 0.0
    if fill_ratio >= 0.85 and min_quote_size >= 50:
        return "healthy"
    if fill_ratio >= 0.7:
        return "thin"
    return "degraded"


def _direction_from_candidates(candidates: list[Mapping[str, Any]]) -> str:
    if not candidates:
        return "unknown"
    best = max(candidates, key=lambda item: _as_float(item.get("quality_score")) or 0.0)
    return _thesis_direction(_strategy_family(_as_text(best.get("strategy"))))


def _intraday_structure(candidate: Mapping[str, Any]) -> str:
    setup_status = str(candidate.get("setup_status") or "").strip().lower()
    if setup_status == "favorable":
        return "trend"
    if setup_status == "neutral":
        return "range"
    if setup_status == "unfavorable":
        return "unstable"
    return "unknown"


def _vol_level(candidate: Mapping[str, Any]) -> str:
    expected_move_pct = _as_float(candidate.get("expected_move_pct"))
    if expected_move_pct is None:
        return "unknown"
    if expected_move_pct < 0.005:
        return "low"
    if expected_move_pct < 0.015:
        return "normal"
    return "high"


def _event_state(candidate: Mapping[str, Any]) -> str:
    return candidate_event_state(candidate)


def _parse_datetime(value: Any) -> datetime | None:
    text = _as_text(value)
    if text is None:
        return None
    normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _minutes_between(start: Any, end: Any) -> float | None:
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    if start_dt is None or end_dt is None:
        return None
    return round((end_dt - start_dt).total_seconds() / 60.0, 1)


def _timestamp_is_after(left: Any, right: Any) -> bool | None:
    left_dt = _parse_datetime(left)
    right_dt = _parse_datetime(right)
    if left_dt is None or right_dt is None:
        return None
    return left_dt > right_dt


def _profile_specific_blockers(
    *,
    candidate: Mapping[str, Any],
    style_profile: str,
) -> list[str]:
    blockers: list[str] = []
    if style_profile == "reactive" and candidate_requires_favorable_setup(candidate):
        if str(candidate.get("setup_status") or "").lower() != "favorable":
            blockers.append("reactive_setup_not_favorable")
        if not candidate_has_intraday_setup_context(candidate):
            blockers.append("missing_intraday_context")
    return blockers


def _profile_specific_score_components(
    *,
    candidate: Mapping[str, Any],
    style_profile: str,
    cycle: Mapping[str, Any],
) -> tuple[dict[str, float], dict[str, Any]]:
    components: dict[str, float] = {}
    evidence: dict[str, Any] = {}

    buffer_ratio = _carry_buffer_ratio(candidate)
    if style_profile == "carry" and buffer_ratio is not None:
        buffer_delta = _clamp((buffer_ratio - 0.15) * 30.0, 0.0, 6.0)
        components["carry_buffer_delta"] = round(buffer_delta, 3)
        evidence["buffer_ratio"] = round(buffer_ratio, 4)

    if style_profile == "tactical":
        setup_status = str(candidate.get("setup_status") or "").strip().lower()
        if setup_status == "favorable":
            components["tactical_setup_delta"] = 2.5
        elif setup_status == "neutral":
            components["tactical_setup_penalty"] = 3.0
        elif setup_status not in {"", "unknown"}:
            components["tactical_setup_penalty"] = 8.0
        evidence["setup_status"] = setup_status or "unknown"

        short_delta = abs(_as_float(candidate.get("short_delta")) or 0.0)
        if short_delta > 0.0:
            delta_fit = _clamp(1.5 - abs(short_delta - 0.13) * 60.0, 0.0, 1.5)
            if delta_fit > 0.0:
                components["tactical_delta_fit_delta"] = round(delta_fit, 3)
            evidence["short_delta"] = round(short_delta, 4)

        expected_move = _as_float(candidate.get("expected_move"))
        short_vs_expected_move = _as_float(candidate.get("short_vs_expected_move"))
        if expected_move not in (None, 0.0) and short_vs_expected_move is not None:
            tactical_buffer_ratio = _clamp(
                short_vs_expected_move / expected_move,
                0.0,
                1.5,
            )
            buffer_delta = _clamp((tactical_buffer_ratio - 0.6) * 12.0, 0.0, 2.0)
            if buffer_delta > 0.0:
                components["tactical_buffer_delta"] = round(buffer_delta, 3)
            evidence["buffer_ratio"] = round(tactical_buffer_ratio, 4)

        if str(candidate.get("calendar_status") or "").strip().lower() == "penalized":
            days_to_event = int(
                _as_float(candidate.get("calendar_days_to_nearest_event")) or 0
            )
            if days_to_event <= 1:
                components["tactical_event_proximity_penalty"] = 4.0
            elif days_to_event == 2:
                components["tactical_event_proximity_penalty"] = 2.0
            else:
                components["tactical_event_proximity_penalty"] = 1.0
            evidence["days_to_nearest_event"] = days_to_event

    if style_profile == "reactive":
        stale_minutes = _minutes_between(
            candidate.get("recovered_from_run_generated_at"),
            cycle.get("generated_at"),
        )
        if stale_minutes is not None:
            evidence["stale_minutes"] = stale_minutes
            if stale_minutes > 20.0:
                components["reactive_staleness_penalty"] = round(
                    _clamp((stale_minutes - 20.0) * 0.25, 0.0, 25.0),
                    3,
                )
        intraday_score = _as_float(candidate.get("setup_intraday_score"))
        if intraday_score is not None:
            intraday_delta = _clamp((intraday_score - 55.0) * 0.12, -8.0, 6.0)
            components["reactive_intraday_delta"] = round(intraday_delta, 3)
            evidence["intraday_score"] = round(intraday_score, 3)
        if candidate.get("selection_source") == "session_history_recovery":
            components["reactive_recovery_penalty"] = 8.0
            evidence["selection_source"] = str(candidate.get("selection_source"))
    return components, evidence


def _calendar_blocks_strategy(
    *,
    calendar_status: str,
    style_profile: str,
) -> bool:
    normalized = calendar_status.strip().lower()
    if normalized in {"", "clean"}:
        return False
    if normalized in {"blocked", "unknown"}:
        return True
    if normalized == "penalized":
        return style_profile == "reactive"
    return True


def _calendar_penalty(
    *,
    calendar_status: str,
    style_profile: str,
) -> float:
    normalized = calendar_status.strip().lower()
    if normalized in {"", "clean"}:
        return 0.0
    if normalized == "penalized":
        if style_profile == "reactive":
            return 6.0
        if style_profile == "tactical":
            return 2.0
        return 3.0
    if normalized == "unknown":
        return 8.0
    return 12.0


def _calibration_dimensions(
    style_profile: str,
) -> tuple[tuple[str, str | None, float], ...]:
    weights = {
        "classification": 1.0,
        "strategy": 0.8,
        "symbol": 0.5,
        "setup_status": 0.7,
    }
    if style_profile == "tactical":
        weights["classification"] = 0.0
        weights["strategy"] = 0.9
        weights["symbol"] = 0.6
        weights["setup_status"] = 0.8
    return (
        ("classification", None, weights["classification"]),
        ("strategy", None, weights["strategy"]),
        ("symbol", None, weights["symbol"]),
        ("setup_status", None, weights["setup_status"]),
    )


def _product_policy_blockers(
    *,
    family: str,
    style_profile: str,
    product_class: str,
    horizon_band: str,
) -> list[str]:
    blockers: list[str] = []
    if family == "iron_condor" and product_class not in {
        "cash_settled_index",
        "top_tier_etf",
    }:
        blockers.append("product_policy_condor_blocked")
    if (
        style_profile == "reactive"
        and family in {"put_credit_spread", "call_credit_spread", "iron_condor"}
        and product_class not in {"cash_settled_index", "top_tier_etf"}
    ):
        blockers.append("reactive_short_premium_product_blocked")
    if family == "iron_condor" and horizon_band == "same_day":
        blockers.append("same_day_iron_condor_blocked")
    return blockers


def _build_historical_dimension_lookup(
    *,
    storage: Any,
    label: str,
    session_date: str | None,
    lookback_sessions: int = 20,
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, Any]]:
    if session_date is None:
        return {}, {"source_session_count": 0}
    try:
        current_session_date = date.fromisoformat(session_date)
    except ValueError:
        return {}, {"source_session_count": 0}

    fetched_runs = storage.post_market.list_runs(
        label=label,
        status="succeeded",
        limit=max(lookback_sessions * 6, lookback_sessions),
    )
    session_summaries: list[Mapping[str, Any]] = []
    seen_dates: set[str] = set()
    used_session_dates: list[str] = []
    for run in fetched_runs:
        run_session_date = _as_text(run.get("session_date"))
        if run_session_date is None or run_session_date in seen_dates:
            continue
        try:
            parsed_run_date = date.fromisoformat(run_session_date)
        except ValueError:
            continue
        if parsed_run_date >= current_session_date:
            continue
        summary = run.get("summary")
        if not isinstance(summary, Mapping):
            continue
        session_summaries.append(summary)
        seen_dates.add(run_session_date)
        used_session_dates.append(run_session_date)
        if len(session_summaries) >= lookback_sessions:
            break

    totals: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for summary in session_summaries:
        tuning = summary.get("tuning")
        dimensions = tuning.get("dimensions") if isinstance(tuning, Mapping) else None
        if not isinstance(dimensions, Mapping):
            continue
        for dimension, rows in dimensions.items():
            if not isinstance(rows, list):
                continue
            dimension_key = str(dimension)
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                group_value = _group_value_from_row(
                    dimension=dimension_key,
                    row=row,
                )
                if group_value is None:
                    continue
                count = max(int(_as_float(row.get("count")) or 0), 0)
                average_estimated_pnl = (
                    _as_float(row.get("average_estimated_pnl")) or 0.0
                )
                legacy_promotable_baseline_count = max(
                    int(
                        _as_float(
                            row.get(
                                "legacy_promotable_baseline_count",
                                row.get("board_count"),
                            )
                        )
                        or 0
                    ),
                    0,
                )
                legacy_monitor_count = max(
                    int(
                        _as_float(
                            row.get("legacy_monitor_count", row.get("watchlist_count"))
                        )
                        or 0
                    ),
                    0,
                )
                bucket_totals = totals[dimension_key].setdefault(
                    group_value,
                    {
                        "count": 0.0,
                        "legacy_promotable_baseline_count": 0.0,
                        "legacy_monitor_count": 0.0,
                        "estimated_pnl_total": 0.0,
                    },
                )
                bucket_totals["count"] += float(count)
                bucket_totals["legacy_promotable_baseline_count"] += float(
                    legacy_promotable_baseline_count
                )
                bucket_totals["legacy_monitor_count"] += float(legacy_monitor_count)
                bucket_totals["estimated_pnl_total"] += average_estimated_pnl * float(
                    count
                )

    lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for dimension, bucket_totals in totals.items():
        dimension_lookup: dict[str, dict[str, Any]] = {}
        for group_value, totals_row in bucket_totals.items():
            count = int(totals_row["count"])
            if count <= 0:
                continue
            dimension_lookup[group_value] = {
                "group_value": group_value,
                "count": count,
                "legacy_promotable_baseline_count": int(
                    totals_row["legacy_promotable_baseline_count"]
                ),
                "legacy_monitor_count": int(totals_row["legacy_monitor_count"]),
                "average_estimated_pnl": round(
                    totals_row["estimated_pnl_total"] / float(count),
                    4,
                ),
            }
        if dimension_lookup:
            lookup[dimension] = dimension_lookup
    metadata = {
        "source_session_count": len(session_summaries),
        "source_session_dates": used_session_dates,
        "lookback_sessions": lookback_sessions,
    }
    return lookup, metadata


def _dimension_adjustment(
    *,
    dimension_lookup: dict[str, dict[str, dict[str, Any]]],
    dimension: str,
    group_value: str | None,
    weight: float,
) -> tuple[float, dict[str, Any] | None]:
    if group_value is None:
        return 0.0, None
    row = dimension_lookup.get(dimension, {}).get(group_value)
    if row is None:
        return 0.0, None
    average_estimated_pnl = _as_float(row.get("average_estimated_pnl")) or 0.0
    return _clamp(average_estimated_pnl, -5.0, 5.0) * weight, {
        "dimension": dimension,
        "group_value": group_value,
        "average_estimated_pnl": average_estimated_pnl,
        "count": row.get("count"),
        "legacy_promotable_baseline_count": row.get("legacy_promotable_baseline_count"),
        "legacy_monitor_count": row.get("legacy_monitor_count"),
    }


def _regime_confidence(candidate: Mapping[str, Any]) -> float:
    quality = _normalize_score(candidate.get("quality_score"), default=0.4)
    setup = _normalize_score(candidate.get("setup_score"), default=0.4)
    fill_ratio = _clamp(_as_float(candidate.get("fill_ratio")) or 0.0, 0.0, 1.0)
    data_bonus = 1.0 if str(candidate.get("data_status") or "") == "clean" else 0.65
    calendar_bonus = (
        1.0 if str(candidate.get("calendar_status") or "") == "clean" else 0.7
    )
    return round(
        _clamp(
            0.45 * quality
            + 0.25 * setup
            + 0.15 * fill_ratio
            + 0.10 * data_bonus
            + 0.05 * calendar_bonus,
            0.0,
            1.0,
        ),
        3,
    )


def _carry_buffer_ratio(candidate: Mapping[str, Any] | None) -> float | None:
    if not isinstance(candidate, Mapping):
        return None
    short_vs_expected_move = _as_float(candidate.get("short_vs_expected_move"))
    expected_move = _as_float(candidate.get("expected_move"))
    if short_vs_expected_move is None or expected_move in (None, 0.0):
        return None
    return _clamp(short_vs_expected_move / expected_move, 0.0, 1.5)


def _build_legs(candidate: Mapping[str, Any]) -> list[OpportunityLeg]:
    order_payload = candidate.get("order_payload")
    if not isinstance(order_payload, Mapping):
        return []
    legs = order_payload.get("legs")
    if not isinstance(legs, list):
        return []
    built: list[OpportunityLeg] = []
    for index, leg in enumerate(legs, start=1):
        if not isinstance(leg, Mapping):
            continue
        symbol = _as_text(leg.get("symbol"))
        side = _as_text(leg.get("side"))
        if symbol is None or side is None:
            continue
        built.append(
            OpportunityLeg(
                leg_index=index,
                symbol=symbol,
                side=side,
                position_intent=_as_text(leg.get("position_intent")),
                ratio_qty=_as_text(leg.get("ratio_qty")),
            )
        )
    return built


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


def _build_regime_snapshots(
    *,
    cycle: Mapping[str, Any],
    candidates: list[Mapping[str, Any]],
) -> list[RegimeSnapshot]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in candidates:
        candidate = row.get("candidate")
        if not isinstance(candidate, Mapping):
            continue
        symbol = _as_text(row.get("underlying_symbol")) or _as_text(
            candidate.get("underlying_symbol")
        )
        if symbol is None:
            continue
        grouped[symbol].append(candidate)

    snapshots: list[RegimeSnapshot] = []
    for symbol, symbol_candidates in sorted(grouped.items()):
        best_candidate = max(
            symbol_candidates,
            key=lambda item: _as_float(item.get("quality_score")) or 0.0,
        )
        dte = int(_as_float(best_candidate.get("days_to_expiration")) or 0)
        style_profile = _style_profile(cycle.get("profile"), days_to_expiration=dte)
        confidence = _regime_confidence(best_candidate)
        snapshots.append(
            RegimeSnapshot(
                regime_snapshot_id=f"regime:{cycle['cycle_id']}:{symbol}:{style_profile}",
                cycle_id=str(cycle["cycle_id"]),
                session_id=_as_text(cycle.get("session_id"))
                or f"historical:{cycle['label']}:{cycle['session_date']}",
                symbol=symbol,
                style_profile=style_profile,
                direction_bias=_direction_from_candidates(symbol_candidates),
                trend_strength=round(
                    _normalize_score(best_candidate.get("setup_score"), default=0.4), 3
                ),
                intraday_structure=_intraday_structure(best_candidate),
                vol_level=_vol_level(best_candidate),
                vol_trend="stable",
                iv_vs_rv="fair",
                event_state=_event_state(best_candidate),
                liquidity_state=_liquidity_state(best_candidate),
                confidence=confidence,
                evidence={
                    "candidate_count": len(symbol_candidates),
                    "quality_scores": [
                        _as_float(item.get("quality_score"))
                        for item in symbol_candidates
                    ],
                    "setup_statuses": [
                        _as_text(item.get("setup_status")) for item in symbol_candidates
                    ],
                    "calendar_statuses": [
                        _as_text(item.get("calendar_status"))
                        for item in symbol_candidates
                    ],
                    "earnings_phases": [
                        candidate_earnings_phase(item) for item in symbol_candidates
                    ],
                    "legacy_profile": cycle.get("profile"),
                    "source": "collector_cycle_candidates",
                },
            )
        )
    return snapshots


def _build_strategy_intents(
    *,
    cycle: Mapping[str, Any],
    candidates: list[Mapping[str, Any]],
    regime_snapshots: list[RegimeSnapshot],
) -> list[StrategyIntent]:
    snapshot_by_symbol = {snapshot.symbol: snapshot for snapshot in regime_snapshots}
    grouped: dict[tuple[str, str], Mapping[str, Any]] = {}

    for row in candidates:
        candidate = row.get("candidate")
        if not isinstance(candidate, Mapping):
            continue
        symbol = _as_text(row.get("underlying_symbol")) or _as_text(
            candidate.get("underlying_symbol")
        )
        if symbol is None:
            continue
        family = _strategy_family(
            _as_text(candidate.get("strategy")) or _as_text(row.get("strategy"))
        )
        current = grouped.get((symbol, family))
        current_quality = (
            -1.0
            if current is None
            else (_as_float(current.get("quality_score")) or -1.0)
        )
        quality = _as_float(candidate.get("quality_score")) or -1.0
        if quality > current_quality:
            grouped[(symbol, family)] = candidate

    intents: list[StrategyIntent] = []
    for (symbol, family), candidate in sorted(grouped.items()):
        snapshot = snapshot_by_symbol.get(symbol)
        if snapshot is None:
            continue
        dte = int(_as_float(candidate.get("days_to_expiration")) or 0)
        horizon_band, _, _, _ = _horizon_band(dte)
        product_class = _product_class(symbol)
        earnings_phase = candidate_earnings_phase(candidate)
        phase_policy_preference = earnings_phase_policy_preference(
            family=family,
            earnings_phase=earnings_phase,
        )
        signal_gate = evaluate_earnings_signal_gate(
            candidate=candidate,
            family=family,
            earnings_phase=earnings_phase,
            days_to_expiration=dte,
            cycle=cycle,
        )
        blockers = _product_policy_blockers(
            family=family,
            style_profile=snapshot.style_profile,
            product_class=product_class,
            horizon_band=horizon_band,
        )
        blockers.extend(
            earnings_phase_policy_blockers(
                family=family,
                earnings_phase=earnings_phase,
                product_class_value=product_class,
                horizon_band_value=horizon_band,
                earnings_timing_confidence=str(
                    candidate.get("earnings_timing_confidence") or "unknown"
                ).strip().lower(),
            )
        )
        blockers.extend(list(signal_gate["blockers"]))
        blockers.extend(
            _profile_specific_blockers(
                candidate=candidate,
                style_profile=snapshot.style_profile,
            )
        )
        if str(candidate.get("data_status") or "") != "clean":
            blockers.append("data_quality_not_clean")
        calendar_status = str(candidate.get("calendar_status") or "")
        if _calendar_blocks_strategy(
            calendar_status=calendar_status,
            style_profile=snapshot.style_profile,
        ):
            blockers.append("calendar_risk_present")

        quality = _normalize_score(candidate.get("quality_score"), default=0.4)
        setup = _normalize_score(candidate.get("setup_score"), default=0.4)
        fill_ratio = _clamp(_as_float(candidate.get("fill_ratio")) or 0.0, 0.0, 1.0)
        blocker_penalty = 0.15 * min(len(blockers), 2)
        desirability = round(
            _clamp(
                0.55 * quality
                + 0.20 * setup
                + 0.15 * fill_ratio
                + 0.10 * snapshot.confidence
                - blocker_penalty,
                0.0,
                1.0,
            ),
            3,
        )
        policy_state = (
            "blocked"
            if blockers
            else (
                "preferred"
                if phase_policy_preference == "preferred"
                else "allowed"
            )
        )
        intents.append(
            StrategyIntent(
                strategy_intent_id=f"strategy_intent:{cycle['cycle_id']}:{symbol}:{family}",
                regime_snapshot_id=snapshot.regime_snapshot_id,
                cycle_id=str(cycle["cycle_id"]),
                session_id=snapshot.session_id,
                symbol=symbol,
                style_profile=snapshot.style_profile,
                strategy_family=family,
                thesis_direction=_thesis_direction(family),
                policy_state=policy_state,
                desirability_score=desirability,
                confidence=round(
                    _clamp((snapshot.confidence + desirability) / 2.0, 0.0, 1.0), 3
                ),
                blockers=blockers,
                evidence={
                    "quality_score": _as_float(candidate.get("quality_score")),
                    "setup_score": _as_float(candidate.get("setup_score")),
                    "setup_status": _as_text(candidate.get("setup_status")),
                    "fill_ratio": _as_float(candidate.get("fill_ratio")),
                    "product_class": product_class,
                    "legacy_strategy": _as_text(candidate.get("strategy")),
                    "earnings_phase": earnings_phase,
                    "phase_policy_preference": phase_policy_preference,
                    "event_timing_rule": candidate_event_timing_rule(candidate),
                    "signal_bundle": signal_gate["bundle"],
                    "signal_thresholds": signal_gate["thresholds"],
                    "signal_gate": {
                        "active": signal_gate["active"],
                        "eligible": signal_gate["eligible"],
                        "coverage_count": signal_gate["coverage_count"],
                        "blockers": list(signal_gate["blockers"]),
                    },
                },
            )
        )
    return intents


def _build_horizon_intents(
    *,
    cycle: Mapping[str, Any],
    strategy_intents: list[StrategyIntent],
    candidates: list[Mapping[str, Any]],
) -> list[HorizonIntent]:
    candidate_by_symbol_family: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in candidates:
        candidate = row.get("candidate")
        if not isinstance(candidate, Mapping):
            continue
        symbol = _as_text(row.get("underlying_symbol")) or _as_text(
            candidate.get("underlying_symbol")
        )
        family = _strategy_family(
            _as_text(candidate.get("strategy")) or _as_text(row.get("strategy"))
        )
        if symbol is None:
            continue
        candidate_by_symbol_family[(symbol, family)] = candidate

    intents: list[HorizonIntent] = []
    for strategy_intent in strategy_intents:
        candidate = candidate_by_symbol_family.get(
            (strategy_intent.symbol, strategy_intent.strategy_family)
        )
        dte = int(
            _as_float(
                None if candidate is None else candidate.get("days_to_expiration")
            )
            or 0
        )
        band, dte_min, dte_max, expiration_type = _horizon_band(dte)
        urgency = (
            "high"
            if band in {"same_day", "next_daily"}
            else ("low" if band == "carry" else "normal")
        )
        event_timing_rule = (
            "none" if candidate is None else candidate_event_timing_rule(candidate)
        )
        intents.append(
            HorizonIntent(
                horizon_intent_id=f"horizon_intent:{cycle['cycle_id']}:{strategy_intent.symbol}:{strategy_intent.strategy_family}:{band}",
                strategy_intent_id=strategy_intent.strategy_intent_id,
                cycle_id=str(cycle["cycle_id"]),
                session_id=strategy_intent.session_id,
                symbol=strategy_intent.symbol,
                style_profile=strategy_intent.style_profile,
                strategy_family=strategy_intent.strategy_family,
                horizon_band=band,
                target_dte_min=dte_min,
                target_dte_max=dte_max,
                preferred_expiration_type=expiration_type,
                event_timing_rule=event_timing_rule,
                urgency=urgency,
                confidence=round(
                    _clamp(
                        strategy_intent.confidence
                        - (
                            0.05
                            if event_timing_rule not in {"none", "normal_policy"}
                            else 0.0
                        ),
                        0.0,
                        1.0,
                    ),
                    3,
                ),
                blockers=list(strategy_intent.blockers),
                evidence={
                    "days_to_expiration": dte,
                    "legacy_profile": cycle.get("profile"),
                    "candidate_expiration_date": None
                    if candidate is None
                    else candidate.get("expiration_date"),
                    "earnings_phase": None
                    if candidate is None
                    else candidate_earnings_phase(candidate),
                },
            )
        )
    return intents


def _build_opportunities(
    *,
    cycle: Mapping[str, Any],
    candidates: list[Mapping[str, Any]],
    strategy_intents: list[StrategyIntent],
    horizon_intents: list[HorizonIntent],
    dimension_lookup: dict[str, dict[str, dict[str, Any]]],
) -> list[Opportunity]:
    strategy_by_key = {
        (item.symbol, item.strategy_family): item for item in strategy_intents
    }
    horizon_by_key = {
        (item.symbol, item.strategy_family): item for item in horizon_intents
    }
    built: list[Opportunity] = []
    for row in candidates:
        candidate = row.get("candidate")
        if not isinstance(candidate, Mapping):
            continue
        symbol = _as_text(row.get("underlying_symbol")) or _as_text(
            candidate.get("underlying_symbol")
        )
        family = _strategy_family(
            _as_text(candidate.get("strategy")) or _as_text(row.get("strategy"))
        )
        if symbol is None:
            continue
        strategy_intent = strategy_by_key.get((symbol, family))
        horizon_intent = horizon_by_key.get((symbol, family))
        if strategy_intent is None or horizon_intent is None:
            continue

        scorecard = build_candidate_opportunity_score(
            candidate,
            cycle=cycle,
            style_profile=strategy_intent.style_profile,
            policy_state=strategy_intent.policy_state,
            blockers=strategy_intent.blockers,
            legacy_selection_state=_legacy_selection_state_from_row(row),
            dimension_lookup=dimension_lookup,
        )
        discovery_score = scorecard["discovery_score"]
        promotion_score = scorecard["promotion_score"]
        state = str(scorecard["state"])
        state_reason = str(scorecard["state_reason"])

        opportunity = Opportunity(
            opportunity_id=f"opportunity:{cycle['cycle_id']}:{row['candidate_id']}",
            cycle_id=str(cycle["cycle_id"]),
            session_id=_as_text(cycle.get("session_id"))
            or f"historical:{cycle['label']}:{cycle['session_date']}",
            candidate_id=int(row["candidate_id"]),
            symbol=symbol,
            legacy_strategy=_as_text(candidate.get("strategy"))
            or _as_text(row.get("strategy"))
            or "unknown",
            expiration_date=str(row["expiration_date"]),
            short_symbol=str(row["short_symbol"]),
            long_symbol=str(row["long_symbol"]),
            style_profile=strategy_intent.style_profile,
            strategy_family=family,
            regime_snapshot_id=strategy_intent.regime_snapshot_id,
            strategy_intent_id=strategy_intent.strategy_intent_id,
            horizon_intent_id=horizon_intent.horizon_intent_id,
            discovery_score=discovery_score,
            promotion_score=promotion_score,
            rank=0,
            state=state,
            state_reason=state_reason,
            expected_edge_value=_as_float(candidate.get("return_on_risk")),
            max_loss=_as_float(candidate.get("max_loss")),
            capital_usage=_as_float(candidate.get("max_loss")),
            execution_complexity=execution_complexity(family),
            product_class=_product_class(symbol),
            legacy_selection_state=_legacy_selection_state_from_row(row),
            evidence={
                "legacy_selection_state": _legacy_selection_state_from_row(row),
                "legacy_position": row.get("position"),
                "quality_score": _as_float(candidate.get("quality_score")),
                "setup_score_delta": scorecard["setup_score_delta"],
                "fill_ratio_delta": scorecard["fill_ratio_delta"],
                "calibration_delta": scorecard["calibration_delta"],
                "calibration_breakdown": scorecard["calibration_breakdown"],
                "profile_score_components": scorecard["profile_score_components"],
                "profile_score_evidence": scorecard["profile_score_evidence"],
                "penalty": scorecard["penalty"],
                "setup_status": _as_text(candidate.get("setup_status")),
                "data_status": _as_text(candidate.get("data_status")),
                "calendar_status": _as_text(candidate.get("calendar_status")),
                "earnings_phase": candidate_earnings_phase(candidate),
                "event_timing_rule": candidate_event_timing_rule(candidate),
                "signal_bundle": scorecard["signal_bundle"],
                "signal_thresholds": scorecard["signal_thresholds"],
                "signal_gate": scorecard["signal_gate"],
                "days_to_expiration": candidate.get("days_to_expiration"),
                "width": _as_float(candidate.get("width")),
                "midpoint_credit": _as_float(candidate.get("midpoint_credit")),
                "natural_credit": _as_float(candidate.get("natural_credit")),
                "selection_source": _as_text(candidate.get("selection_source")),
                "run_id": row.get("run_id"),
            },
            legs=_build_legs(candidate),
        )
        built.append(opportunity)

    return rank_opportunities(built)


def _opportunity_identity(opportunity: Opportunity) -> tuple[str, str, str, str, str]:
    return (
        opportunity.symbol,
        opportunity.legacy_strategy,
        opportunity.expiration_date,
        opportunity.short_symbol,
        opportunity.long_symbol,
    )


def _position_identity(position: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(position.get("underlying_symbol") or ""),
        str(position.get("strategy") or ""),
        str(position.get("expiration_date") or ""),
        str(position.get("short_symbol") or ""),
        str(position.get("long_symbol") or ""),
    )


def _attempt_trade_intent(attempt: Mapping[str, Any]) -> str:
    request = attempt.get("request")
    if isinstance(request, Mapping):
        requested = _as_text(request.get("trade_intent"))
        if requested is not None:
            return requested.lower()
    return (_as_text(attempt.get("trade_intent")) or "open").lower()


def _attempt_force_close_at(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    if not isinstance(request, Mapping):
        return None
    exit_policy = request.get("exit_policy")
    if not isinstance(exit_policy, Mapping):
        return None
    return _as_text(exit_policy.get("force_close_at"))


def _attempt_source_reason(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    if not isinstance(request, Mapping):
        return None
    source = request.get("source")
    if not isinstance(source, Mapping):
        return None
    return _as_text(source.get("reason"))


def _attempt_fill_timestamp(attempt: Mapping[str, Any]) -> str | None:
    fill_times = [
        _as_text(fill.get("filled_at"))
        for fill in attempt.get("fills") or []
        if isinstance(fill, Mapping)
    ]
    filtered = [value for value in fill_times if value]
    if filtered:
        return max(filtered)
    return _as_text(attempt.get("completed_at")) or _as_text(
        attempt.get("submitted_at")
    )


def _empty_execution_match() -> dict[str, Any]:
    return {
        "execution_attempted": False,
        "execution_attempt_ids": [],
        "close_execution_attempt_ids": [],
        "open_attempt_count": 0,
        "open_filled_attempt_count": 0,
        "open_expired_attempt_count": 0,
        "open_failed_attempt_count": 0,
        "open_status_counts": {},
        "open_fill_minutes_total": 0.0,
        "open_fill_minutes_count": 0,
        "average_open_fill_minutes": None,
        "requested_after_force_close_count": 0,
        "opened_after_force_close_count": 0,
        "request_to_force_close_total": 0.0,
        "request_to_force_close_count": 0,
        "average_minutes_to_force_close_at_request": None,
        "fill_to_force_close_total": 0.0,
        "fill_to_force_close_count": 0,
        "average_minutes_to_force_close_at_fill": None,
        "entry_credit_capture_total": 0.0,
        "entry_credit_capture_count": 0,
        "average_entry_credit_capture_pct": None,
        "entry_limit_retention_total": 0.0,
        "entry_limit_retention_count": 0,
        "average_entry_limit_retention_pct": None,
        "close_attempt_count": 0,
        "close_filled_attempt_count": 0,
        "close_status_counts": {},
        "close_fill_minutes_total": 0.0,
        "close_fill_minutes_count": 0,
        "average_close_fill_minutes": None,
        "force_close_exit_count": 0,
    }


def _build_position_matches(
    *,
    opportunities: list[Opportunity],
    storage: Any,
    session_id: str | None,
    positions: list[Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    if session_id is None or not storage.execution.portfolio_schema_ready():
        return {}

    resolved_scope = parse_live_run_scope_id(session_id)
    if resolved_scope is None:
        return {}
    resolved_positions = (
        list(positions)
        if positions is not None
        else [
            enrich_position_row(dict(row))
            for row in storage.execution.list_positions(
                pipeline_id=f"pipeline:{resolved_scope['label']}",
                market_date=resolved_scope["market_date"],
            )
        ]
    )
    positions_by_identity: dict[
        tuple[str, str, str, str, str], list[Mapping[str, Any]]
    ] = defaultdict(list)
    for position in resolved_positions:
        positions_by_identity[_position_identity(position)].append(position)

    matches: dict[str, dict[str, Any]] = {}
    for opportunity in opportunities:
        matched_positions = positions_by_identity.get(
            _opportunity_identity(opportunity), []
        )
        if not matched_positions:
            matches[opportunity.opportunity_id] = {
                "actual_position_matched": False,
                "actual_position_count": 0,
                "actual_position_ids": [],
                "actual_position_status_counts": {},
                "actual_closed_rate": None,
                "actual_realized_pnl": None,
                "actual_unrealized_pnl": None,
                "actual_net_pnl": None,
                "actual_positive_outcome": None,
            }
            continue

        status_counts: dict[str, int] = defaultdict(int)
        for row in matched_positions:
            status_counts[str(row.get("status") or "unknown")] += 1
        position_count = len(matched_positions)
        realized_total = round(
            sum(_as_float(row.get("realized_pnl")) or 0.0 for row in matched_positions),
            4,
        )
        unrealized_values = [
            _as_float(row.get("unrealized_pnl")) for row in matched_positions
        ]
        unrealized_total = (
            None
            if not any(value is not None for value in unrealized_values)
            else round(sum(value or 0.0 for value in unrealized_values), 4)
        )
        net_total = round(realized_total + (unrealized_total or 0.0), 4)
        closed_count = sum(
            count
            for status, count in status_counts.items()
            if status in {"closed", "expired"}
        )
        matches[opportunity.opportunity_id] = {
            "actual_position_matched": True,
            "actual_position_count": position_count,
            "actual_position_ids": [
                str(row.get("position_id"))
                for row in matched_positions
                if row.get("position_id") is not None
            ],
            "actual_position_status_counts": dict(sorted(status_counts.items())),
            "actual_closed_rate": round(closed_count / position_count, 4),
            "actual_realized_pnl": realized_total,
            "actual_unrealized_pnl": unrealized_total,
            "actual_net_pnl": net_total,
            "actual_positive_outcome": net_total > 0.0,
        }
    return matches


def _build_execution_matches(
    *,
    opportunities: list[Opportunity],
    storage: Any,
    session_id: str | None,
    positions: list[Mapping[str, Any]] | None = None,
    attempts: list[Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    if session_id is None or not storage.execution.schema_ready():
        return {}

    resolved_scope = parse_live_run_scope_id(session_id)
    if resolved_scope is None:
        return {}
    resolved_positions = (
        list(positions)
        if positions is not None
        else [
            enrich_position_row(dict(row))
            for row in storage.execution.list_positions(
                pipeline_id=f"pipeline:{resolved_scope['label']}",
                market_date=resolved_scope["market_date"],
            )
        ]
    )
    resolved_attempts = (
        [dict(item) for item in attempts]
        if attempts is not None
        else list_session_execution_attempts(
            db_target="",
            session_id=session_id,
            limit=500,
            storage=storage,
        )
    )

    opportunity_by_candidate_id = {
        int(item.candidate_id): item
        for item in opportunities
        if int(item.candidate_id) > 0
    }
    opportunity_by_id = {item.opportunity_id: item for item in opportunities}
    opportunity_by_identity = {
        _opportunity_identity(item): item for item in opportunities
    }
    position_by_id = {
        str(row.get("position_id")): row
        for row in resolved_positions
        if row.get("position_id") is not None
    }
    position_to_opportunity_id: dict[str, str] = {}
    for row in resolved_positions:
        opportunity: Opportunity | None = None
        candidate_id = row.get("candidate_id")
        if candidate_id is not None:
            try:
                opportunity = opportunity_by_candidate_id.get(int(candidate_id))
            except (TypeError, ValueError):
                opportunity = None
        if opportunity is None:
            opportunity = opportunity_by_identity.get(_position_identity(row))
        if opportunity is None or row.get("position_id") is None:
            continue
        position_to_opportunity_id[str(row["position_id"])] = opportunity.opportunity_id

    matches = {item.opportunity_id: _empty_execution_match() for item in opportunities}

    for raw_attempt in resolved_attempts:
        attempt = dict(raw_attempt)
        trade_intent = _attempt_trade_intent(attempt)
        opportunity_id: str | None = None
        if trade_intent == "open":
            candidate_id = attempt.get("candidate_id")
            if candidate_id is not None:
                try:
                    matched_opportunity = opportunity_by_candidate_id.get(
                        int(candidate_id)
                    )
                except (TypeError, ValueError):
                    matched_opportunity = None
            else:
                matched_opportunity = None
            if matched_opportunity is None:
                matched_opportunity = opportunity_by_identity.get(
                    _position_identity(attempt)
                )
            if matched_opportunity is not None:
                opportunity_id = matched_opportunity.opportunity_id
        elif trade_intent == "close":
            request = attempt.get("request")
            request_position_id = (
                _as_text(request.get("position_id"))
                if isinstance(request, Mapping)
                else None
            )
            position_id = _as_text(attempt.get("position_id")) or request_position_id
            if position_id is not None:
                opportunity_id = position_to_opportunity_id.get(position_id)

        if opportunity_id is None:
            continue

        match = matches[opportunity_id]
        match["execution_attempted"] = True
        status = (_as_text(attempt.get("status")) or "unknown").lower()

        if trade_intent == "open":
            match["execution_attempt_ids"].append(str(attempt["execution_attempt_id"]))
            match["open_attempt_count"] += 1
            open_status_counts = match["open_status_counts"]
            open_status_counts[status] = int(open_status_counts.get(status) or 0) + 1
            if status == "filled":
                match["open_filled_attempt_count"] += 1
            elif status == "expired":
                match["open_expired_attempt_count"] += 1
            elif status in {"canceled", "failed", "rejected"}:
                match["open_failed_attempt_count"] += 1

            fill_minutes = _minutes_between(
                attempt.get("requested_at"),
                _attempt_fill_timestamp(attempt),
            )
            if fill_minutes is not None and status == "filled":
                match["open_fill_minutes_total"] += float(fill_minutes)
                match["open_fill_minutes_count"] += 1

            force_close_at = _attempt_force_close_at(attempt)
            requested_after_force_close = _timestamp_is_after(
                attempt.get("requested_at"),
                force_close_at,
            )
            if requested_after_force_close:
                match["requested_after_force_close_count"] += 1
            request_to_force_close_minutes = _minutes_between(
                attempt.get("requested_at"),
                force_close_at,
            )
            if request_to_force_close_minutes is not None:
                match["request_to_force_close_total"] += float(
                    request_to_force_close_minutes
                )
                match["request_to_force_close_count"] += 1

            fill_time = _attempt_fill_timestamp(attempt)
            opened_after_force_close = _timestamp_is_after(fill_time, force_close_at)
            if opened_after_force_close and status == "filled":
                match["opened_after_force_close_count"] += 1
            fill_to_force_close_minutes = _minutes_between(fill_time, force_close_at)
            if fill_to_force_close_minutes is not None and status == "filled":
                match["fill_to_force_close_total"] += float(fill_to_force_close_minutes)
                match["fill_to_force_close_count"] += 1

            session_position_id = _as_text(attempt.get("position_id"))
            position = (
                None
                if session_position_id is None
                else position_by_id.get(session_position_id)
            )
            entry_credit = None
            if isinstance(position, Mapping):
                entry_credit = _as_float(position.get("entry_credit"))
            if entry_credit is None:
                entry_credit = _as_float(attempt.get("limit_price"))
            opportunity = opportunity_by_id.get(opportunity_id)
            midpoint_credit = None
            if opportunity is not None:
                midpoint_credit = _as_float(opportunity.evidence.get("midpoint_credit"))
            if (
                entry_credit is not None
                and midpoint_credit is not None
                and midpoint_credit > 0.0
            ):
                match["entry_credit_capture_total"] += entry_credit / midpoint_credit
                match["entry_credit_capture_count"] += 1
            limit_price = _as_float(attempt.get("limit_price"))
            if entry_credit is not None and limit_price not in (None, 0.0):
                match["entry_limit_retention_total"] += entry_credit / limit_price
                match["entry_limit_retention_count"] += 1

        elif trade_intent == "close":
            match["close_execution_attempt_ids"].append(
                str(attempt["execution_attempt_id"])
            )
            match["close_attempt_count"] += 1
            close_status_counts = match["close_status_counts"]
            close_status_counts[status] = int(close_status_counts.get(status) or 0) + 1
            if status == "filled":
                match["close_filled_attempt_count"] += 1
            close_fill_minutes = _minutes_between(
                attempt.get("requested_at"),
                _attempt_fill_timestamp(attempt),
            )
            if close_fill_minutes is not None and status == "filled":
                match["close_fill_minutes_total"] += float(close_fill_minutes)
                match["close_fill_minutes_count"] += 1
            if _attempt_source_reason(attempt) == "force_close":
                match["force_close_exit_count"] += 1

    for match in matches.values():
        open_fill_count = int(match["open_fill_minutes_count"] or 0)
        if open_fill_count > 0:
            match["average_open_fill_minutes"] = round(
                float(match["open_fill_minutes_total"]) / open_fill_count,
                4,
            )
        request_force_close_count = int(match["request_to_force_close_count"] or 0)
        if request_force_close_count > 0:
            match["average_minutes_to_force_close_at_request"] = round(
                float(match["request_to_force_close_total"])
                / request_force_close_count,
                4,
            )
        fill_force_close_count = int(match["fill_to_force_close_count"] or 0)
        if fill_force_close_count > 0:
            match["average_minutes_to_force_close_at_fill"] = round(
                float(match["fill_to_force_close_total"]) / fill_force_close_count,
                4,
            )
        capture_count = int(match["entry_credit_capture_count"] or 0)
        if capture_count > 0:
            match["average_entry_credit_capture_pct"] = round(
                float(match["entry_credit_capture_total"]) / capture_count,
                4,
            )
        retention_count = int(match["entry_limit_retention_count"] or 0)
        if retention_count > 0:
            match["average_entry_limit_retention_pct"] = round(
                float(match["entry_limit_retention_total"]) / retention_count,
                4,
            )
        close_fill_count = int(match["close_fill_minutes_count"] or 0)
        if close_fill_count > 0:
            match["average_close_fill_minutes"] = round(
                float(match["close_fill_minutes_total"]) / close_fill_count,
                4,
            )
        match["open_status_counts"] = dict(sorted(match["open_status_counts"].items()))
        match["close_status_counts"] = dict(
            sorted(match["close_status_counts"].items())
        )
    return matches


def _build_outcome_matches(
    *,
    opportunities: list[Opportunity],
    analysis_run: Mapping[str, Any] | None,
    storage: Any,
    session_id: str | None,
) -> dict[str, dict[str, Any]]:
    summary = analysis_run.get("summary") if isinstance(analysis_run, Mapping) else None
    outcomes = summary.get("outcomes") if isinstance(summary, Mapping) else None
    ideas = list(outcomes.get("ideas") or []) if isinstance(outcomes, Mapping) else []
    positions: list[Mapping[str, Any]] | None = None
    attempts: list[Mapping[str, Any]] | None = None
    if session_id is not None and storage.execution.schema_ready():
        resolved_scope = parse_live_run_scope_id(session_id)
        if resolved_scope is not None:
            positions = [
                enrich_position_row(dict(row))
                for row in storage.execution.list_positions(
                    pipeline_id=f"pipeline:{resolved_scope['label']}",
                    market_date=resolved_scope["market_date"],
                )
            ]
        attempts = list_session_execution_attempts(
            db_target="",
            session_id=session_id,
            limit=500,
            storage=storage,
        )
    position_matches = _build_position_matches(
        opportunities=opportunities,
        storage=storage,
        session_id=session_id,
        positions=positions,
    )
    execution_matches = _build_execution_matches(
        opportunities=opportunities,
        storage=storage,
        session_id=session_id,
        positions=positions,
        attempts=attempts,
    )

    lookup: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for idea in ideas:
        if not isinstance(idea, Mapping):
            continue
        try:
            identity = candidate_identity(idea)
        except KeyError:
            continue
        lookup[identity] = dict(idea)

    matches: dict[str, dict[str, Any]] = {}
    for opportunity in opportunities:
        idea = lookup.get(_opportunity_identity(opportunity))
        position_match = position_matches.get(opportunity.opportunity_id) or {}
        execution_match = execution_matches.get(opportunity.opportunity_id) or {}
        estimated_close_pnl = (
            None if idea is None else _as_float(idea.get("estimated_close_pnl"))
        )
        estimated_expiry_pnl = (
            None if idea is None else _as_float(idea.get("estimated_expiry_pnl"))
        )
        estimated_pnl = None if idea is None else resolved_estimated_pnl(idea)
        actual_net_pnl = _as_float(position_match.get("actual_net_pnl"))
        matches[opportunity.opportunity_id] = {
            "matched": idea is not None,
            "estimated_close_pnl": None
            if estimated_close_pnl is None
            else round(float(estimated_close_pnl), 4),
            "estimated_close_positive": None
            if estimated_close_pnl is None
            else float(estimated_close_pnl) > 0.0,
            "estimated_expiry_pnl": None
            if estimated_expiry_pnl is None
            else round(float(estimated_expiry_pnl), 4),
            "estimated_expiry_positive": None
            if estimated_expiry_pnl is None
            else float(estimated_expiry_pnl) > 0.0,
            "estimated_pnl": None
            if estimated_pnl is None
            else round(float(estimated_pnl), 4),
            "positive_outcome": None
            if estimated_pnl is None
            else float(estimated_pnl) > 0.0,
            "outcome_bucket": None if idea is None else idea.get("outcome_bucket"),
            "replay_verdict": None if idea is None else idea.get("replay_verdict"),
            "setup_status": None if idea is None else idea.get("setup_status"),
            "vwap_regime": None if idea is None else idea.get("vwap_regime"),
            "trend_regime": None if idea is None else idea.get("trend_regime"),
            "opening_range_regime": None
            if idea is None
            else idea.get("opening_range_regime"),
            "classification": None if idea is None else idea.get("classification"),
            "actual_minus_estimated_close_pnl": None
            if actual_net_pnl is None or estimated_close_pnl is None
            else round(actual_net_pnl - float(estimated_close_pnl), 4),
            **position_match,
            **execution_match,
        }
    return matches


def _summarize_outcome_rows(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    modeled_final_rows = [row for row in rows if row.get("estimated_pnl") is not None]
    pnl_values = [float(row["estimated_pnl"]) for row in modeled_final_rows]
    signed_rows = [row for row in rows if row.get("positive_outcome") is not None]
    outcome_bucket_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        bucket = _as_text(row.get("outcome_bucket"))
        if bucket is not None:
            outcome_bucket_counts[bucket] += 1
    still_open_count = int(outcome_bucket_counts.get("still_open") or 0)
    modeled_close_rows = [
        row for row in rows if row.get("estimated_close_pnl") is not None
    ]
    modeled_expiry_rows = [
        row for row in rows if row.get("estimated_expiry_pnl") is not None
    ]
    actual_rows = [row for row in rows if row.get("actual_net_pnl") is not None]
    actual_realized_rows = [
        row for row in rows if row.get("actual_realized_pnl") is not None
    ]
    actual_status_counts: dict[str, int] = defaultdict(int)
    for row in actual_rows:
        for status, count in dict(
            row.get("actual_position_status_counts") or {}
        ).items():
            actual_status_counts[str(status)] += int(count or 0)
    actual_closed_rates = [
        float(row["actual_closed_rate"])
        for row in actual_rows
        if row.get("actual_closed_rate") is not None
    ]
    execution_rows = [
        row
        for row in rows
        if bool(row.get("execution_attempted"))
        or int(row.get("open_attempt_count") or 0) > 0
        or int(row.get("close_attempt_count") or 0) > 0
    ]
    open_attempt_total = sum(int(row.get("open_attempt_count") or 0) for row in rows)
    open_filled_total = sum(
        int(row.get("open_filled_attempt_count") or 0) for row in rows
    )
    open_expired_total = sum(
        int(row.get("open_expired_attempt_count") or 0) for row in rows
    )
    open_failed_total = sum(
        int(row.get("open_failed_attempt_count") or 0) for row in rows
    )
    late_open_request_total = sum(
        int(row.get("requested_after_force_close_count") or 0) for row in rows
    )
    late_open_fill_total = sum(
        int(row.get("opened_after_force_close_count") or 0) for row in rows
    )
    close_attempt_total = sum(int(row.get("close_attempt_count") or 0) for row in rows)
    close_filled_total = sum(
        int(row.get("close_filled_attempt_count") or 0) for row in rows
    )
    force_close_exit_total = sum(
        int(row.get("force_close_exit_count") or 0) for row in rows
    )
    actual_minus_close_rows = [
        row for row in rows if row.get("actual_minus_estimated_close_pnl") is not None
    ]

    def _weighted_average(total_field: str, count_field: str) -> float | None:
        weighted_count = sum(int(row.get(count_field) or 0) for row in rows)
        if weighted_count <= 0:
            return None
        weighted_total = sum(float(row.get(total_field) or 0.0) for row in rows)
        return round(weighted_total / weighted_count, 4)

    def _average_ratio(field: str) -> float | None:
        values = [float(row[field]) for row in rows if row.get(field) is not None]
        if not values:
            return None
        return round(mean(values), 4)

    def _pooled_return_on_risk(pnl_field: str) -> tuple[float | None, float]:
        numerator_total = 0.0
        denominator_total = 0.0
        for row in rows:
            pnl_value = _as_float(row.get(pnl_field))
            max_loss = _as_float(row.get("max_loss"))
            if pnl_value is None or max_loss is None or max_loss <= 0.0:
                continue
            numerator_total += pnl_value
            denominator_total += max_loss
        if denominator_total <= 0.0:
            return None, 0.0
        return round(numerator_total / denominator_total, 4), round(
            denominator_total, 2
        )

    pooled_estimated_final_ror, estimated_final_max_loss_total = _pooled_return_on_risk(
        "estimated_pnl"
    )
    pooled_estimated_close_ror, estimated_close_max_loss_total = _pooled_return_on_risk(
        "estimated_close_pnl"
    )
    pooled_actual_net_ror, actual_max_loss_total = _pooled_return_on_risk(
        "actual_net_pnl"
    )
    (
        pooled_actual_minus_estimated_close_ror,
        actual_minus_close_max_loss_total,
    ) = _pooled_return_on_risk("actual_minus_estimated_close_pnl")
    overall_max_loss_total = round(
        sum(
            max(_as_float(row.get("max_loss")) or 0.0, 0.0)
            for row in rows
            if _as_float(row.get("max_loss")) is not None
        ),
        2,
    )

    return {
        "average_estimated_pnl": None if not pnl_values else round(mean(pnl_values), 4),
        "estimated_pnl_count": len(modeled_final_rows),
        "estimated_pnl_coverage_rate": None
        if not rows
        else round(len(modeled_final_rows) / len(rows), 4),
        "positive_rate": None
        if not signed_rows
        else round(
            sum(1 for row in signed_rows if bool(row.get("positive_outcome")))
            / len(signed_rows),
            4,
        ),
        "positive_count": sum(
            1 for row in signed_rows if bool(row.get("positive_outcome"))
        ),
        "negative_or_flat_count": sum(
            1 for row in signed_rows if not bool(row.get("positive_outcome"))
        ),
        "outcome_bucket_counts": dict(sorted(outcome_bucket_counts.items())),
        "still_open_count": still_open_count,
        "still_open_rate": None if not rows else round(still_open_count / len(rows), 4),
        "average_estimated_close_pnl": None
        if not modeled_close_rows
        else round(
            mean(float(row["estimated_close_pnl"]) for row in modeled_close_rows), 4
        ),
        "estimated_close_count": len(modeled_close_rows),
        "estimated_close_coverage_rate": None
        if not rows
        else round(len(modeled_close_rows) / len(rows), 4),
        "estimated_close_positive_rate": None
        if not modeled_close_rows
        else round(
            sum(
                1
                for row in modeled_close_rows
                if bool(row.get("estimated_close_positive"))
            )
            / len(modeled_close_rows),
            4,
        ),
        "average_estimated_expiry_pnl": None
        if not modeled_expiry_rows
        else round(
            mean(float(row["estimated_expiry_pnl"]) for row in modeled_expiry_rows), 4
        ),
        "estimated_expiry_count": len(modeled_expiry_rows),
        "estimated_expiry_coverage_rate": None
        if not rows
        else round(len(modeled_expiry_rows) / len(rows), 4),
        "estimated_expiry_positive_rate": None
        if not modeled_expiry_rows
        else round(
            sum(
                1
                for row in modeled_expiry_rows
                if bool(row.get("estimated_expiry_positive"))
            )
            / len(modeled_expiry_rows),
            4,
        ),
        "average_actual_net_pnl": None
        if not actual_rows
        else round(mean(float(row["actual_net_pnl"]) for row in actual_rows), 4),
        "average_actual_realized_pnl": None
        if not actual_realized_rows
        else round(
            mean(float(row["actual_realized_pnl"]) for row in actual_realized_rows), 4
        ),
        "actual_count": len(actual_rows),
        "actual_coverage_rate": None
        if not rows
        else round(len(actual_rows) / len(rows), 4),
        "actual_positive_rate": None
        if not actual_rows
        else round(
            sum(1 for row in actual_rows if bool(row.get("actual_positive_outcome")))
            / len(actual_rows),
            4,
        ),
        "actual_position_status_counts": dict(sorted(actual_status_counts.items())),
        "actual_closed_rate": None
        if not actual_closed_rates
        else round(mean(actual_closed_rates), 4),
        "execution_attempted_count": len(execution_rows),
        "execution_attempted_rate": None
        if not rows
        else round(len(execution_rows) / len(rows), 4),
        "open_attempt_count": open_attempt_total,
        "open_filled_attempt_count": open_filled_total,
        "open_expired_attempt_count": open_expired_total,
        "open_failed_attempt_count": open_failed_total,
        "open_fill_rate": None
        if open_attempt_total <= 0
        else round(open_filled_total / open_attempt_total, 4),
        "open_expired_rate": None
        if open_attempt_total <= 0
        else round(open_expired_total / open_attempt_total, 4),
        "open_failed_rate": None
        if open_attempt_total <= 0
        else round(open_failed_total / open_attempt_total, 4),
        "late_open_request_count": late_open_request_total,
        "late_open_request_rate": None
        if open_attempt_total <= 0
        else round(late_open_request_total / open_attempt_total, 4),
        "late_open_fill_count": late_open_fill_total,
        "late_open_fill_rate": None
        if open_filled_total <= 0
        else round(late_open_fill_total / open_filled_total, 4),
        "close_attempt_count": close_attempt_total,
        "close_filled_attempt_count": close_filled_total,
        "close_fill_rate": None
        if close_attempt_total <= 0
        else round(close_filled_total / close_attempt_total, 4),
        "force_close_exit_count": force_close_exit_total,
        "force_close_exit_rate": None
        if close_attempt_total <= 0
        else round(force_close_exit_total / close_attempt_total, 4),
        "average_open_fill_minutes": _weighted_average(
            "open_fill_minutes_total",
            "open_fill_minutes_count",
        ),
        "average_close_fill_minutes": _weighted_average(
            "close_fill_minutes_total",
            "close_fill_minutes_count",
        ),
        "average_minutes_to_force_close_at_request": _weighted_average(
            "request_to_force_close_total",
            "request_to_force_close_count",
        ),
        "average_minutes_to_force_close_at_fill": _weighted_average(
            "fill_to_force_close_total",
            "fill_to_force_close_count",
        ),
        "average_entry_credit_capture_pct": _weighted_average(
            "entry_credit_capture_total",
            "entry_credit_capture_count",
        ),
        "average_entry_limit_retention_pct": _weighted_average(
            "entry_limit_retention_total",
            "entry_limit_retention_count",
        ),
        "average_actual_minus_estimated_close_pnl": None
        if not actual_minus_close_rows
        else round(
            mean(
                float(row["actual_minus_estimated_close_pnl"])
                for row in actual_minus_close_rows
            ),
            4,
        ),
        "actual_minus_estimated_close_count": len(actual_minus_close_rows),
        "max_loss_total": overall_max_loss_total,
        "average_estimated_final_return_on_risk": _average_ratio(
            "estimated_final_return_on_risk"
        ),
        "pooled_estimated_final_return_on_risk": pooled_estimated_final_ror,
        "estimated_final_return_on_risk_max_loss_total": estimated_final_max_loss_total,
        "average_estimated_close_return_on_risk": _average_ratio(
            "estimated_close_return_on_risk"
        ),
        "pooled_estimated_close_return_on_risk": pooled_estimated_close_ror,
        "estimated_close_return_on_risk_max_loss_total": estimated_close_max_loss_total,
        "average_actual_net_return_on_risk": _average_ratio(
            "actual_net_return_on_risk"
        ),
        "pooled_actual_net_return_on_risk": pooled_actual_net_ror,
        "actual_net_return_on_risk_max_loss_total": actual_max_loss_total,
        "average_actual_minus_estimated_close_return_on_risk": _average_ratio(
            "actual_minus_estimated_close_return_on_risk"
        ),
        "pooled_actual_minus_estimated_close_return_on_risk": pooled_actual_minus_estimated_close_ror,
        "actual_minus_estimated_close_return_on_risk_max_loss_total": actual_minus_close_max_loss_total,
    }


def _slice_metrics(
    *,
    items: list[Opportunity],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    rows = [
        outcome_matches[item.opportunity_id]
        for item in items
        if outcome_matches.get(item.opportunity_id) is not None
    ]
    matched = [row for row in rows if row.get("matched")]
    metrics = _summarize_outcome_rows(rows)
    return {
        "count": len(items),
        "matched_count": len(matched),
        "coverage_rate": None if not items else round(len(matched) / len(items), 4),
        **metrics,
    }


def _flatten_opportunity_rows(
    *,
    session: Mapping[str, Any],
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
    comparison: Mapping[str, Any],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    allocation_by_id = {item.opportunity_id: item for item in allocation_decisions}
    rank_only_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("rank_only_top", {}) or {}).get("items", [])
        if isinstance(item, Mapping)
    }
    allocator_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("provisional_allocator", {}) or {}).get("items", [])
        if isinstance(item, Mapping)
    }
    promoted_legacy_monitor_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("promoted_from_legacy_monitor") or [])
        if isinstance(item, Mapping)
    }
    rejected_legacy_promotable_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("rejected_legacy_promotable") or [])
        if isinstance(item, Mapping)
    }

    rows: list[dict[str, Any]] = []
    for opportunity in opportunities:
        allocation = allocation_by_id.get(opportunity.opportunity_id)
        outcome = outcome_matches.get(opportunity.opportunity_id, {})
        max_loss = _as_float(opportunity.max_loss)
        entry_return_on_risk = _as_float(opportunity.expected_edge_value)
        midpoint_credit = _as_float(opportunity.evidence.get("midpoint_credit"))
        width = _as_float(opportunity.evidence.get("width"))
        days_to_expiration = _as_float(opportunity.evidence.get("days_to_expiration"))
        entry_ror_bucket, entry_ror_bucket_order = _entry_return_on_risk_bucket(
            entry_return_on_risk
        )
        midpoint_credit_bucket, midpoint_credit_bucket_order = _midpoint_credit_bucket(
            midpoint_credit
        )
        width_bucket, width_bucket_order = _width_bucket(width)
        dte_bucket, dte_bucket_order = _dte_bucket(days_to_expiration)
        rows.append(
            {
                "label": session.get("label"),
                "session_date": session.get("session_date"),
                "cycle_id": session.get("cycle_id"),
                "candidate_id": opportunity.candidate_id,
                "opportunity_id": opportunity.opportunity_id,
                "symbol": opportunity.symbol,
                "style_profile": opportunity.style_profile,
                "strategy_family": opportunity.strategy_family,
                "legacy_strategy": opportunity.legacy_strategy,
                "event_state": opportunity.evidence.get("earnings_phase"),
                "event_timing_rule": opportunity.evidence.get("event_timing_rule"),
                "direction_signal": _as_float(
                    ((opportunity.evidence.get("signal_bundle") or {}).get("signals") or {})
                    .get("direction_signal", {})
                    .get("score")
                ),
                "jump_risk_signal": _as_float(
                    ((opportunity.evidence.get("signal_bundle") or {}).get("signals") or {})
                    .get("jump_risk_signal", {})
                    .get("score")
                ),
                "pricing_signal": _as_float(
                    ((opportunity.evidence.get("signal_bundle") or {}).get("signals") or {})
                    .get("pricing_signal", {})
                    .get("score")
                ),
                "post_event_confirmation_signal": _as_float(
                    ((opportunity.evidence.get("signal_bundle") or {}).get("signals") or {})
                    .get("post_event_confirmation_signal", {})
                    .get("score")
                ),
                "options_bias_alignment": (opportunity.evidence.get("signal_bundle") or {}).get(
                    "options_bias_alignment"
                ),
                "neutral_regime_signal": _as_float(
                    (opportunity.evidence.get("signal_bundle") or {}).get(
                        "neutral_regime_signal"
                    )
                ),
                "residual_iv_richness": _as_float(
                    (opportunity.evidence.get("signal_bundle") or {}).get(
                        "residual_iv_richness"
                    )
                ),
                "signal_gate_active": bool(
                    (opportunity.evidence.get("signal_gate") or {}).get("active")
                ),
                "signal_gate_eligible": bool(
                    (opportunity.evidence.get("signal_gate") or {}).get("eligible")
                ),
                "signal_gate_blockers": (opportunity.evidence.get("signal_gate") or {}).get(
                    "blockers"
                ),
                "expiration_date": opportunity.expiration_date,
                "short_symbol": opportunity.short_symbol,
                "long_symbol": opportunity.long_symbol,
                "max_loss": max_loss,
                "entry_return_on_risk": entry_return_on_risk,
                "entry_return_on_risk_bucket": entry_ror_bucket,
                "entry_return_on_risk_bucket_order": entry_ror_bucket_order,
                "midpoint_credit": midpoint_credit,
                "midpoint_credit_bucket": midpoint_credit_bucket,
                "midpoint_credit_bucket_order": midpoint_credit_bucket_order,
                "width": width,
                "width_bucket": width_bucket,
                "width_bucket_order": width_bucket_order,
                "days_to_expiration": None
                if days_to_expiration is None
                else int(days_to_expiration),
                "dte_bucket": dte_bucket,
                "dte_bucket_order": dte_bucket_order,
                "legacy_selection_state": opportunity.legacy_selection_state,
                "rank": opportunity.rank,
                "state": opportunity.state,
                "promotion_score": opportunity.promotion_score,
                "allocation_state": None
                if allocation is None
                else allocation.allocation_state,
                "allocation_score": None
                if allocation is None
                else allocation.allocation_score,
                "allocation_reason": None
                if allocation is None
                else allocation.allocation_reason,
                "matched_outcome": outcome.get("matched"),
                "estimated_close_pnl": outcome.get("estimated_close_pnl"),
                "estimated_expiry_pnl": outcome.get("estimated_expiry_pnl"),
                "estimated_pnl": outcome.get("estimated_pnl"),
                "estimated_close_return_on_risk": _ratio_or_none(
                    outcome.get("estimated_close_pnl"),
                    max_loss,
                ),
                "estimated_final_return_on_risk": _ratio_or_none(
                    outcome.get("estimated_pnl"),
                    max_loss,
                ),
                "positive_outcome": outcome.get("positive_outcome"),
                "outcome_bucket": outcome.get("outcome_bucket"),
                "replay_verdict": outcome.get("replay_verdict"),
                "setup_status": outcome.get("setup_status"),
                "vwap_regime": outcome.get("vwap_regime"),
                "trend_regime": outcome.get("trend_regime"),
                "opening_range_regime": outcome.get("opening_range_regime"),
                "actual_position_matched": outcome.get("actual_position_matched"),
                "actual_position_count": outcome.get("actual_position_count"),
                "actual_position_status_counts": outcome.get(
                    "actual_position_status_counts"
                ),
                "actual_closed_rate": outcome.get("actual_closed_rate"),
                "actual_realized_pnl": outcome.get("actual_realized_pnl"),
                "actual_unrealized_pnl": outcome.get("actual_unrealized_pnl"),
                "actual_net_pnl": outcome.get("actual_net_pnl"),
                "actual_net_return_on_risk": _ratio_or_none(
                    outcome.get("actual_net_pnl"),
                    max_loss,
                ),
                "actual_positive_outcome": outcome.get("actual_positive_outcome"),
                "actual_minus_estimated_close_pnl": outcome.get(
                    "actual_minus_estimated_close_pnl"
                ),
                "actual_minus_estimated_close_return_on_risk": _ratio_or_none(
                    outcome.get("actual_minus_estimated_close_pnl"),
                    max_loss,
                ),
                "execution_attempted": outcome.get("execution_attempted"),
                "execution_attempt_ids": outcome.get("execution_attempt_ids"),
                "close_execution_attempt_ids": outcome.get(
                    "close_execution_attempt_ids"
                ),
                "open_attempt_count": outcome.get("open_attempt_count"),
                "open_filled_attempt_count": outcome.get("open_filled_attempt_count"),
                "open_expired_attempt_count": outcome.get("open_expired_attempt_count"),
                "open_failed_attempt_count": outcome.get("open_failed_attempt_count"),
                "open_status_counts": outcome.get("open_status_counts"),
                "average_open_fill_minutes": outcome.get("average_open_fill_minutes"),
                "requested_after_force_close_count": outcome.get(
                    "requested_after_force_close_count"
                ),
                "opened_after_force_close_count": outcome.get(
                    "opened_after_force_close_count"
                ),
                "average_minutes_to_force_close_at_request": outcome.get(
                    "average_minutes_to_force_close_at_request"
                ),
                "average_minutes_to_force_close_at_fill": outcome.get(
                    "average_minutes_to_force_close_at_fill"
                ),
                "entry_credit_capture_total": outcome.get("entry_credit_capture_total"),
                "entry_credit_capture_count": outcome.get("entry_credit_capture_count"),
                "average_entry_credit_capture_pct": outcome.get(
                    "average_entry_credit_capture_pct"
                ),
                "entry_limit_retention_total": outcome.get(
                    "entry_limit_retention_total"
                ),
                "entry_limit_retention_count": outcome.get(
                    "entry_limit_retention_count"
                ),
                "average_entry_limit_retention_pct": outcome.get(
                    "average_entry_limit_retention_pct"
                ),
                "close_attempt_count": outcome.get("close_attempt_count"),
                "close_filled_attempt_count": outcome.get("close_filled_attempt_count"),
                "close_status_counts": outcome.get("close_status_counts"),
                "average_close_fill_minutes": outcome.get("average_close_fill_minutes"),
                "force_close_exit_count": outcome.get("force_close_exit_count"),
                "open_fill_minutes_total": outcome.get("open_fill_minutes_total"),
                "open_fill_minutes_count": outcome.get("open_fill_minutes_count"),
                "request_to_force_close_total": outcome.get(
                    "request_to_force_close_total"
                ),
                "request_to_force_close_count": outcome.get(
                    "request_to_force_close_count"
                ),
                "fill_to_force_close_total": outcome.get("fill_to_force_close_total"),
                "fill_to_force_close_count": outcome.get("fill_to_force_close_count"),
                "close_fill_minutes_total": outcome.get("close_fill_minutes_total"),
                "close_fill_minutes_count": outcome.get("close_fill_minutes_count"),
                "is_legacy_promotable_baseline": opportunity.legacy_selection_state
                == "promotable",
                "is_legacy_monitor_baseline": opportunity.legacy_selection_state
                == "monitor",
                "is_rank_only_top": opportunity.opportunity_id in rank_only_ids,
                "is_allocator_selected": opportunity.opportunity_id in allocator_ids,
                "is_promoted_from_legacy_monitor": opportunity.opportunity_id
                in promoted_legacy_monitor_ids,
                "is_rejected_legacy_promotable": opportunity.opportunity_id
                in rejected_legacy_promotable_ids,
            }
        )
    return rows


def _aggregate_dimension_rows(
    rows: list[dict[str, Any]],
    *,
    field: str,
    order_field: str | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(field) or "unknown")].append(row)

    result: list[dict[str, Any]] = []
    for group_value, group_rows in grouped.items():
        matched = [row for row in group_rows if row.get("matched_outcome")]
        metrics = _summarize_outcome_rows(group_rows)
        result.append(
            {
                "group_value": group_value,
                "count": len(group_rows),
                "matched_count": len(matched),
                "coverage_rate": None
                if not group_rows
                else round(len(matched) / len(group_rows), 4),
                "allocator_selected_count": sum(
                    1 for row in group_rows if row.get("is_allocator_selected")
                ),
                "legacy_promotable_baseline_count": sum(
                    1 for row in group_rows if row.get("is_legacy_promotable_baseline")
                ),
                "rank_only_top_count": sum(
                    1 for row in group_rows if row.get("is_rank_only_top")
                ),
                "promoted_from_legacy_monitor_count": sum(
                    1
                    for row in group_rows
                    if row.get("is_promoted_from_legacy_monitor")
                ),
                "rejected_legacy_promotable_count": sum(
                    1 for row in group_rows if row.get("is_rejected_legacy_promotable")
                ),
                **metrics,
            }
        )
    if order_field is not None:
        result.sort(
            key=lambda row: (
                min(
                    int(group_row.get(order_field) or UNKNOWN_BUCKET_ORDER)
                    for group_row in grouped[row["group_value"]]
                ),
                row["group_value"],
            )
        )
    else:
        result.sort(key=lambda row: (-int(row["count"]), row["group_value"]))
    return result


def _build_deployment_quality_views(
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    matched_rows = [row for row in rows if row.get("matched_outcome")]
    return {
        "count": len(rows),
        "matched_count": len(matched_rows),
        "coverage_rate": None if not rows else round(len(matched_rows) / len(rows), 4),
        **_summarize_outcome_rows(rows),
        "by_profile": _aggregate_dimension_rows(rows, field="style_profile"),
        "by_strategy_family": _aggregate_dimension_rows(rows, field="strategy_family"),
        "by_entry_return_on_risk_bucket": _aggregate_dimension_rows(
            rows,
            field="entry_return_on_risk_bucket",
            order_field="entry_return_on_risk_bucket_order",
        ),
        "by_midpoint_credit_bucket": _aggregate_dimension_rows(
            rows,
            field="midpoint_credit_bucket",
            order_field="midpoint_credit_bucket_order",
        ),
        "by_width_bucket": _aggregate_dimension_rows(
            rows,
            field="width_bucket",
            order_field="width_bucket_order",
        ),
        "by_dte_bucket": _aggregate_dimension_rows(
            rows,
            field="dte_bucket",
            order_field="dte_bucket_order",
        ),
    }


def _build_scorecard(
    *,
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
    comparison: Mapping[str, Any],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    allocation_by_id = {item.opportunity_id: item for item in allocation_decisions}
    rank_only_ids = {
        str(row.get("opportunity_id"))
        for row in (comparison.get("rank_only_top", {}) or {}).get("items", [])
        if isinstance(row, Mapping)
    }
    rank_only_top = [
        item for item in opportunities if item.opportunity_id in rank_only_ids
    ]
    allocator_selected = [
        item
        for item in opportunities
        if allocation_by_id.get(item.opportunity_id) is not None
        and allocation_by_id[item.opportunity_id].allocation_state == "allocated"
    ]
    legacy_promotable_baseline = [
        item for item in opportunities if item.legacy_selection_state == "promotable"
    ]
    promoted_from_legacy_monitor = [
        item for item in allocator_selected if item.legacy_selection_state == "monitor"
    ]
    rejected_legacy_promotable = [
        item
        for item in legacy_promotable_baseline
        if allocation_by_id.get(item.opportunity_id) is None
        or allocation_by_id[item.opportunity_id].allocation_state != "allocated"
    ]

    scorecard = {
        "legacy_promotable_baseline": _slice_metrics(
            items=legacy_promotable_baseline,
            outcome_matches=outcome_matches,
        ),
        "rank_only_top": _slice_metrics(
            items=rank_only_top,
            outcome_matches=outcome_matches,
        ),
        "allocator_selected": _slice_metrics(
            items=allocator_selected,
            outcome_matches=outcome_matches,
        ),
        "promoted_from_legacy_monitor": _slice_metrics(
            items=promoted_from_legacy_monitor,
            outcome_matches=outcome_matches,
        ),
        "rejected_legacy_promotable": _slice_metrics(
            items=rejected_legacy_promotable,
            outcome_matches=outcome_matches,
        ),
    }

    def metric_delta(
        *,
        allocator_field: str,
        baseline: dict[str, Any],
    ) -> float | None:
        allocator_value = _as_float(
            scorecard["allocator_selected"].get(allocator_field)
        )
        baseline_value = _as_float(baseline.get(allocator_field))
        if allocator_value is None or baseline_value is None:
            return None
        return round(allocator_value - baseline_value, 4)

    scorecard["deltas"] = {
        "allocator_minus_legacy_promotable_baseline_avg_estimated_pnl": metric_delta(
            allocator_field="average_estimated_pnl",
            baseline=scorecard["legacy_promotable_baseline"],
        ),
        "allocator_minus_rank_only_avg_estimated_pnl": metric_delta(
            allocator_field="average_estimated_pnl",
            baseline=scorecard["rank_only_top"],
        ),
        "allocator_minus_legacy_promotable_baseline_avg_estimated_close_pnl": metric_delta(
            allocator_field="average_estimated_close_pnl",
            baseline=scorecard["legacy_promotable_baseline"],
        ),
        "allocator_minus_rank_only_avg_estimated_close_pnl": metric_delta(
            allocator_field="average_estimated_close_pnl",
            baseline=scorecard["rank_only_top"],
        ),
        "allocator_minus_legacy_promotable_baseline_avg_actual_net_pnl": metric_delta(
            allocator_field="average_actual_net_pnl",
            baseline=scorecard["legacy_promotable_baseline"],
        ),
        "allocator_minus_rank_only_avg_actual_net_pnl": metric_delta(
            allocator_field="average_actual_net_pnl",
            baseline=scorecard["rank_only_top"],
        ),
        "allocator_minus_legacy_promotable_baseline_avg_actual_minus_estimated_close_pnl": metric_delta(
            allocator_field="average_actual_minus_estimated_close_pnl",
            baseline=scorecard["legacy_promotable_baseline"],
        ),
        "allocator_minus_rank_only_avg_actual_minus_estimated_close_pnl": metric_delta(
            allocator_field="average_actual_minus_estimated_close_pnl",
            baseline=scorecard["rank_only_top"],
        ),
        "allocator_minus_legacy_promotable_baseline_late_open_fill_rate": metric_delta(
            allocator_field="late_open_fill_rate",
            baseline=scorecard["legacy_promotable_baseline"],
        ),
        "allocator_minus_rank_only_late_open_fill_rate": metric_delta(
            allocator_field="late_open_fill_rate",
            baseline=scorecard["rank_only_top"],
        ),
        "allocator_minus_legacy_promotable_baseline_force_close_exit_rate": metric_delta(
            allocator_field="force_close_exit_rate",
            baseline=scorecard["legacy_promotable_baseline"],
        ),
        "allocator_minus_rank_only_force_close_exit_rate": metric_delta(
            allocator_field="force_close_exit_rate",
            baseline=scorecard["rank_only_top"],
        ),
        "legacy_monitor_promotion_hit_rate": scorecard["promoted_from_legacy_monitor"][
            "positive_rate"
        ],
        "rejected_legacy_promotable_miss_rate": scorecard["rejected_legacy_promotable"][
            "positive_rate"
        ],
    }
    return scorecard


def _comparison_item(
    opportunity: Opportunity,
    allocation: AllocationDecision | None,
    outcome: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "opportunity_id": opportunity.opportunity_id,
        "candidate_id": opportunity.candidate_id,
        "symbol": opportunity.symbol,
        "strategy_family": opportunity.strategy_family,
        "rank": opportunity.rank,
        "promotion_score": opportunity.promotion_score,
        "legacy_selection_state": opportunity.legacy_selection_state,
        "allocation_state": None if allocation is None else allocation.allocation_state,
        "allocation_reason": None
        if allocation is None
        else allocation.allocation_reason,
        "allocation_score": None if allocation is None else allocation.allocation_score,
        "estimated_pnl": None if outcome is None else outcome.get("estimated_pnl"),
        "outcome_bucket": None if outcome is None else outcome.get("outcome_bucket"),
        "positive_outcome": None
        if outcome is None
        else outcome.get("positive_outcome"),
    }


def _build_comparison(
    *,
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    allocation_by_opportunity_id = {
        item.opportunity_id: item for item in allocation_decisions
    }
    legacy_promotable_baseline = [
        item for item in opportunities if item.legacy_selection_state == "promotable"
    ]
    legacy_monitor_baseline = [
        item for item in opportunities if item.legacy_selection_state == "monitor"
    ]
    allocated = [
        item
        for item in opportunities
        if allocation_by_opportunity_id.get(item.opportunity_id) is not None
        and allocation_by_opportunity_id[item.opportunity_id].allocation_state
        == "allocated"
    ]
    comparison_size = max(len(legacy_promotable_baseline), len(allocated))
    promotable_rank_only = [
        item for item in opportunities if item.state == "promotable"
    ]
    rank_only_top = (
        []
        if comparison_size <= 0
        else (promotable_rank_only[:comparison_size] or opportunities[:comparison_size])
    )

    allocated_ids = {item.opportunity_id for item in allocated}
    legacy_promotable_baseline_ids = {
        item.opportunity_id for item in legacy_promotable_baseline
    }
    rank_only_ids = {item.opportunity_id for item in rank_only_top}

    return {
        "comparison_size": comparison_size,
        "legacy_promotable_baseline": {
            "count": len(legacy_promotable_baseline),
            "symbols": sorted({item.symbol for item in legacy_promotable_baseline}),
            "candidate_ids": [item.candidate_id for item in legacy_promotable_baseline],
            "items": [
                _comparison_item(
                    item,
                    allocation_by_opportunity_id.get(item.opportunity_id),
                    outcome_matches.get(item.opportunity_id),
                )
                for item in legacy_promotable_baseline
            ],
        },
        "legacy_monitor_baseline": {
            "count": len(legacy_monitor_baseline),
            "symbols": sorted({item.symbol for item in legacy_monitor_baseline}),
            "candidate_ids": [item.candidate_id for item in legacy_monitor_baseline],
        },
        "rank_only_top": {
            "count": len(rank_only_top),
            "symbols": sorted({item.symbol for item in rank_only_top}),
            "candidate_ids": [item.candidate_id for item in rank_only_top],
            "items": [
                _comparison_item(
                    item,
                    allocation_by_opportunity_id.get(item.opportunity_id),
                    outcome_matches.get(item.opportunity_id),
                )
                for item in rank_only_top
            ],
        },
        "provisional_allocator": {
            "count": len(allocated),
            "symbols": sorted({item.symbol for item in allocated}),
            "candidate_ids": [item.candidate_id for item in allocated],
            "items": [
                _comparison_item(
                    item,
                    allocation_by_opportunity_id.get(item.opportunity_id),
                    outcome_matches.get(item.opportunity_id),
                )
                for item in allocated
            ],
        },
        "promoted_from_legacy_monitor": [
            _comparison_item(
                item,
                allocation_by_opportunity_id.get(item.opportunity_id),
                outcome_matches.get(item.opportunity_id),
            )
            for item in allocated
            if item.legacy_selection_state == "monitor"
        ],
        "rejected_legacy_promotable": [
            _comparison_item(
                item,
                allocation_by_opportunity_id.get(item.opportunity_id),
                outcome_matches.get(item.opportunity_id),
            )
            for item in legacy_promotable_baseline
            if item.opportunity_id not in allocated_ids
        ],
        "rank_only_rejected_by_allocator": [
            _comparison_item(
                item,
                allocation_by_opportunity_id.get(item.opportunity_id),
                outcome_matches.get(item.opportunity_id),
            )
            for item in rank_only_top
            if item.opportunity_id not in allocated_ids
        ],
        "allocator_added_outside_rank_only": [
            _comparison_item(
                item,
                allocation_by_opportunity_id.get(item.opportunity_id),
                outcome_matches.get(item.opportunity_id),
            )
            for item in allocated
            if item.opportunity_id not in rank_only_ids
        ],
        "overlap": {
            "allocator_vs_legacy_promotable_baseline_count": len(
                allocated_ids & legacy_promotable_baseline_ids
            ),
            "allocator_vs_rank_only_count": len(allocated_ids & rank_only_ids),
        },
    }


def _build_summary(
    *,
    cycle: Mapping[str, Any],
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
    calibration_lookup: dict[str, dict[str, dict[str, Any]]],
    calibration_meta: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    allocation_by_opportunity_id = {
        item.opportunity_id: item for item in allocation_decisions
    }
    legacy_promotable_baseline_symbols = sorted(
        {
            item.symbol
            for item in opportunities
            if item.legacy_selection_state == "promotable"
        }
    )
    allocated_symbols = sorted(
        {
            item.symbol
            for item in opportunities
            if allocation_by_opportunity_id.get(item.opportunity_id) is not None
            and allocation_by_opportunity_id[item.opportunity_id].allocation_state
            == "allocated"
        }
    )
    allocated_opportunity_ids = [
        decision.opportunity_id
        for decision in allocation_decisions
        if decision.allocation_state == "allocated"
    ]
    newly_promoted_legacy_monitor = sorted(
        {
            item.symbol
            for item in opportunities
            if item.legacy_selection_state == "monitor" and item.state == "promotable"
        }
    )
    summary = {
        "label": cycle.get("label"),
        "session_date": cycle.get("session_date"),
        "cycle_id": cycle.get("cycle_id"),
        "legacy_profile": cycle.get("profile"),
        "style_profile": opportunities[0].style_profile if opportunities else None,
        "candidate_count": len(opportunities),
        "promotable_count": sum(
            1 for item in opportunities if item.state == "promotable"
        ),
        "allocated_count": len(allocated_opportunity_ids),
        "allocated_opportunity_ids": allocated_opportunity_ids,
        "allocated_symbols": allocated_symbols,
        "legacy_promotable_baseline_symbols": legacy_promotable_baseline_symbols,
        "newly_promoted_legacy_monitor_symbols": newly_promoted_legacy_monitor,
        "analysis_verdict": None,
        "historical_calibration_session_count": int(
            calibration_meta.get("source_session_count") or 0
        ),
    }

    warnings = [
        "Regime fields are inferred from persisted candidate payloads because collector cycles do not store full regime snapshots yet.",
        "The allocator is provisional and uses deterministic heuristic budgets; it is for offline comparison only.",
    ]
    if int(calibration_meta.get("source_session_count") or 0) <= 0:
        warnings.append(
            "No prior succeeded post-market sessions were available for calibration, so promotion scores rely on raw candidate features only."
        )
    else:
        classification_lookup = calibration_lookup.get("classification", {})
        promotable_row = classification_lookup.get("promotable")
        monitor_row = classification_lookup.get("monitor")
        promotable_pnl = (
            None
            if promotable_row is None
            else _as_float(promotable_row.get("average_estimated_pnl"))
        )
        monitor_pnl = (
            None
            if monitor_row is None
            else _as_float(monitor_row.get("average_estimated_pnl"))
        )
        if (
            promotable_pnl is not None
            and monitor_pnl is not None
            and monitor_pnl > promotable_pnl
        ):
            warnings.append(
                "Prior-session calibration for this label favors legacy monitor ideas over the legacy promotable baseline, so monitor candidates may surface as promotable."
            )
    return summary, warnings


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


@with_storage()
def build_opportunity_replay(
    *,
    db_target: str | None = None,
    session_id: str | None = None,
    label: str | None = None,
    session_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    cycle, analysis_run = _resolve_target(
        storage=storage,
        session_id=session_id,
        label=label,
        session_date=session_date,
    )
    candidate_rows, recovery_warnings = _load_cycle_candidates(
        storage=storage,
        cycle=cycle,
    )
    if not candidate_rows:
        raise OpportunityReplayLookupError(
            f"Collector cycle {cycle['cycle_id']} has no stored candidates."
        )
    calibration_lookup, calibration_meta = _build_historical_dimension_lookup(
        storage=storage,
        label=str(cycle["label"]),
        session_date=_as_text(cycle.get("session_date")),
    )

    regime_snapshots = _build_regime_snapshots(cycle=cycle, candidates=candidate_rows)
    strategy_intents = _build_strategy_intents(
        cycle=cycle,
        candidates=candidate_rows,
        regime_snapshots=regime_snapshots,
    )
    horizon_intents = _build_horizon_intents(
        cycle=cycle,
        strategy_intents=strategy_intents,
        candidates=candidate_rows,
    )
    opportunities = _build_opportunities(
        cycle=cycle,
        candidates=candidate_rows,
        strategy_intents=strategy_intents,
        horizon_intents=horizon_intents,
        dimension_lookup=calibration_lookup,
    )
    allocation_decisions = build_allocation_decisions(opportunities)
    outcome_matches = _build_outcome_matches(
        opportunities=opportunities,
        analysis_run=analysis_run,
        storage=storage,
        session_id=_as_text(cycle.get("session_id")),
    )
    execution_intents = build_execution_intents(
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
    )
    comparison = _build_comparison(
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        outcome_matches=outcome_matches,
    )
    scorecard = _build_scorecard(
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        comparison=comparison,
        outcome_matches=outcome_matches,
    )
    summary, warnings = _build_summary(
        cycle=cycle,
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        calibration_lookup=calibration_lookup,
        calibration_meta=calibration_meta,
    )
    summary["analysis_verdict"] = None
    if analysis_run is not None:
        summary["analysis_verdict"] = (analysis_run.get("diagnostics") or {}).get(
            "overall_verdict"
        )
    rows = _flatten_opportunity_rows(
        session={
            "label": cycle.get("label"),
            "session_date": cycle.get("session_date"),
            "cycle_id": cycle.get("cycle_id"),
        },
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        comparison=comparison,
        outcome_matches=outcome_matches,
    )
    scorecard["deployment_quality"] = {
        "allocator_selected": _build_deployment_quality_views(
            [row for row in rows if row.get("is_allocator_selected")]
        ),
        "actual_deployed": _build_deployment_quality_views(
            [row for row in rows if row.get("actual_position_matched")]
        ),
    }
    if analysis_run is not None:
        matched_count = sum(
            1 for match in outcome_matches.values() if bool(match.get("matched"))
        )
        if matched_count < len(opportunities):
            warnings.append(
                f"Only {matched_count} of {len(opportunities)} latest-cycle opportunities matched persisted post-market ideas."
            )
    allocator_metrics = scorecard.get("allocator_selected") or {}
    if int(allocator_metrics.get("count") or 0) == 0:
        warnings.append(
            "No opportunities cleared the provisional allocator for this session, so allocator-vs-legacy-promotable-baseline comparisons are based on zero selected opportunities."
        )
    allocator_still_open_rate = allocator_metrics.get("still_open_rate")
    if (
        allocator_still_open_rate is not None
        and float(allocator_still_open_rate) >= 0.5
    ):
        warnings.append(
            "Allocator scorecard outcomes are dominated by still_open post-market ideas, so average estimated PnL reflects modeled close-state rather than realized lifecycle results."
        )
    allocator_actual_coverage = _as_float(allocator_metrics.get("actual_coverage_rate"))
    if allocator_actual_coverage is not None and allocator_actual_coverage < 0.5:
        warnings.append(
            "Actual traded-position coverage for allocator-selected opportunities is sparse, so realized PnL comparisons are lower-confidence than modeled replay comparisons."
        )
    allocator_late_open_fill_rate = _as_float(
        allocator_metrics.get("late_open_fill_rate")
    )
    if (
        allocator_late_open_fill_rate is not None
        and allocator_late_open_fill_rate > 0.0
    ):
        warnings.append(
            "Allocator-selected opportunities include filled opens after the configured force-close deadline, which points to execution-path drift rather than pure selection quality."
        )
    allocator_force_close_exit_rate = _as_float(
        allocator_metrics.get("force_close_exit_rate")
    )
    if (
        allocator_force_close_exit_rate is not None
        and allocator_force_close_exit_rate >= 0.5
    ):
        warnings.append(
            "Allocator-selected opportunities are being closed mostly by force-close exits, so actual PnL is sensitive to late-day execution quality."
        )
    allocator_actual_minus_close = _as_float(
        allocator_metrics.get("average_actual_minus_estimated_close_pnl")
    )
    if allocator_actual_minus_close is not None and allocator_actual_minus_close < 0.0:
        warnings.append(
            "Allocator-selected actual PnL is trailing modeled close-state PnL, which suggests execution drag or exit handling slippage."
        )
    warnings.extend(recovery_warnings)

    replay = DecisionReplay(
        target={
            "requested_session_id": session_id,
            "requested_label": label,
            "requested_session_date": session_date,
        },
        session={
            "label": cycle.get("label"),
            "session_date": cycle.get("session_date"),
            "session_id": cycle.get("session_id")
            or f"historical:{cycle['label']}:{cycle['session_date']}",
            "cycle_id": cycle.get("cycle_id"),
            "legacy_profile": cycle.get("profile"),
            "strategy": cycle.get("strategy"),
            "generated_at": cycle.get("generated_at"),
            "analysis_run_id": None
            if analysis_run is None
            else analysis_run.get("analysis_run_id"),
        },
        regime_snapshots=regime_snapshots,
        strategy_intents=strategy_intents,
        horizon_intents=horizon_intents,
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        execution_intents=execution_intents,
        summary=summary,
        comparison=comparison,
        scorecard=scorecard,
        rows=rows,
        warnings=warnings,
    )
    return replay.to_payload()


@with_storage()
def build_recent_opportunity_replay_batch(
    *,
    db_target: str | None = None,
    recent: int = 5,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    targets = _resolve_recent_analysis_targets(
        storage=storage,
        recent=recent,
        label=label,
    )
    if not targets:
        scope = "latest succeeded sessions" if label is None else f"label {label}"
        raise OpportunityReplayLookupError(
            f"No succeeded post-market sessions are available for {scope}."
        )

    sessions: list[dict[str, Any]] = []
    verdict_counts: dict[str, int] = defaultdict(int)
    promoted_from_legacy_monitor_total = 0
    rejected_legacy_promotable_total = 0
    allocator_vs_legacy_promotable_baseline_total = 0
    allocator_vs_rank_only_total = 0
    skipped_sessions: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []

    for target in targets:
        try:
            payload = build_opportunity_replay(
                db_target=db_target,
                label=target["label"],
                session_date=target["session_date"],
                storage=storage,
            )
        except OpportunityReplayLookupError as exc:
            skipped_sessions.append(
                {
                    "label": target["label"],
                    "session_date": target["session_date"],
                    "reason": str(exc),
                }
            )
            continue
        summary = dict(payload.get("summary") or {})
        comparison = dict(payload.get("comparison") or {})
        scorecard = dict(payload.get("scorecard") or {})
        verdict = _as_text(summary.get("analysis_verdict")) or "unknown"
        verdict_counts[verdict] += 1
        promoted_from_legacy_monitor = list(
            comparison.get("promoted_from_legacy_monitor") or []
        )
        rejected_legacy_promotable = list(
            comparison.get("rejected_legacy_promotable") or []
        )
        overlap = dict(comparison.get("overlap") or {})
        promoted_from_legacy_monitor_total += len(promoted_from_legacy_monitor)
        rejected_legacy_promotable_total += len(rejected_legacy_promotable)
        allocator_vs_legacy_promotable_baseline_total += int(
            overlap.get("allocator_vs_legacy_promotable_baseline_count") or 0
        )
        allocator_vs_rank_only_total += int(
            overlap.get("allocator_vs_rank_only_count") or 0
        )
        for row in payload.get("rows") or []:
            if isinstance(row, Mapping):
                all_rows.append(dict(row))
        sessions.append(
            {
                "session": payload.get("session"),
                "summary": summary,
                "scorecard": scorecard,
                "comparison": {
                    "comparison_size": comparison.get("comparison_size"),
                    "legacy_promotable_baseline_candidate_ids": (
                        comparison.get("legacy_promotable_baseline") or {}
                    ).get("candidate_ids", []),
                    "rank_only_candidate_ids": (
                        comparison.get("rank_only_top") or {}
                    ).get("candidate_ids", []),
                    "allocator_candidate_ids": (
                        comparison.get("provisional_allocator") or {}
                    ).get("candidate_ids", []),
                    "promoted_from_legacy_monitor": promoted_from_legacy_monitor,
                    "rejected_legacy_promotable": rejected_legacy_promotable,
                    "overlap": overlap,
                },
                "warnings": list(payload.get("warnings") or []),
            }
        )
        if len(sessions) >= recent:
            break

    session_count = len(sessions)
    if session_count == 0:
        scope = "latest succeeded sessions" if label is None else f"label {label}"
        raise OpportunityReplayLookupError(
            f"No replayable sessions are available for {scope}."
        )

    def pooled_metrics(flag_field: str) -> dict[str, Any]:
        scoped_rows = [row for row in all_rows if row.get(flag_field)]
        matched_rows = [row for row in scoped_rows if row.get("matched_outcome")]
        metrics = _summarize_outcome_rows(scoped_rows)
        return {
            "count": len(scoped_rows),
            "matched_count": len(matched_rows),
            "coverage_rate": None
            if not scoped_rows
            else round(len(matched_rows) / len(scoped_rows), 4),
            **metrics,
        }

    legacy_promotable_baseline_metrics = pooled_metrics("is_legacy_promotable_baseline")
    rank_only_top_metrics = pooled_metrics("is_rank_only_top")
    allocator_selected_metrics = pooled_metrics("is_allocator_selected")
    promoted_from_legacy_monitor_metrics = pooled_metrics(
        "is_promoted_from_legacy_monitor"
    )
    rejected_legacy_promotable_metrics = pooled_metrics("is_rejected_legacy_promotable")
    sessions_with_allocator_selections = sum(
        1
        for item in sessions
        if int((item.get("summary") or {}).get("allocated_count") or 0) > 0
    )
    aggregate = {
        "session_count": session_count,
        "requested_recent": recent,
        "label_filter": label,
        "verdict_counts": dict(sorted(verdict_counts.items())),
        "skipped_session_count": len(skipped_sessions),
        "sessions_with_allocator_selections": sessions_with_allocator_selections,
        "sessions_with_legacy_monitor_promotions": sum(
            1 for item in sessions if item["comparison"]["promoted_from_legacy_monitor"]
        ),
        "sessions_with_rejected_legacy_promotable": sum(
            1 for item in sessions if item["comparison"]["rejected_legacy_promotable"]
        ),
        "promoted_from_legacy_monitor_total": promoted_from_legacy_monitor_total,
        "rejected_legacy_promotable_total": rejected_legacy_promotable_total,
        "average_allocator_vs_legacy_promotable_baseline_overlap": round(
            allocator_vs_legacy_promotable_baseline_total / session_count,
            3,
        ),
        "average_allocator_vs_rank_only_overlap": round(
            allocator_vs_rank_only_total / session_count,
            3,
        ),
        "legacy_promotable_baseline_metrics": legacy_promotable_baseline_metrics,
        "rank_only_top_metrics": rank_only_top_metrics,
        "allocator_selected_metrics": allocator_selected_metrics,
        "promoted_from_legacy_monitor_metrics": promoted_from_legacy_monitor_metrics,
        "rejected_legacy_promotable_metrics": rejected_legacy_promotable_metrics,
        "allocator_minus_legacy_promotable_baseline_avg_estimated_pnl": None
        if allocator_selected_metrics["average_estimated_pnl"] is None
        or legacy_promotable_baseline_metrics["average_estimated_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_pnl"])
            - float(legacy_promotable_baseline_metrics["average_estimated_pnl"]),
            4,
        ),
        "allocator_minus_rank_only_avg_estimated_pnl": None
        if allocator_selected_metrics["average_estimated_pnl"] is None
        or rank_only_top_metrics["average_estimated_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_pnl"])
            - float(rank_only_top_metrics["average_estimated_pnl"]),
            4,
        ),
        "allocator_minus_legacy_promotable_baseline_avg_estimated_close_pnl": None
        if allocator_selected_metrics["average_estimated_close_pnl"] is None
        or legacy_promotable_baseline_metrics["average_estimated_close_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_close_pnl"])
            - float(legacy_promotable_baseline_metrics["average_estimated_close_pnl"]),
            4,
        ),
        "allocator_minus_rank_only_avg_estimated_close_pnl": None
        if allocator_selected_metrics["average_estimated_close_pnl"] is None
        or rank_only_top_metrics["average_estimated_close_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_close_pnl"])
            - float(rank_only_top_metrics["average_estimated_close_pnl"]),
            4,
        ),
        "allocator_minus_legacy_promotable_baseline_avg_actual_net_pnl": None
        if allocator_selected_metrics["average_actual_net_pnl"] is None
        or legacy_promotable_baseline_metrics["average_actual_net_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_actual_net_pnl"])
            - float(legacy_promotable_baseline_metrics["average_actual_net_pnl"]),
            4,
        ),
        "allocator_minus_rank_only_avg_actual_net_pnl": None
        if allocator_selected_metrics["average_actual_net_pnl"] is None
        or rank_only_top_metrics["average_actual_net_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_actual_net_pnl"])
            - float(rank_only_top_metrics["average_actual_net_pnl"]),
            4,
        ),
        "allocator_minus_legacy_promotable_baseline_avg_actual_minus_estimated_close_pnl": None
        if allocator_selected_metrics["average_actual_minus_estimated_close_pnl"]
        is None
        or legacy_promotable_baseline_metrics[
            "average_actual_minus_estimated_close_pnl"
        ]
        is None
        else round(
            float(
                allocator_selected_metrics["average_actual_minus_estimated_close_pnl"]
            )
            - float(
                legacy_promotable_baseline_metrics[
                    "average_actual_minus_estimated_close_pnl"
                ]
            ),
            4,
        ),
        "allocator_minus_rank_only_avg_actual_minus_estimated_close_pnl": None
        if allocator_selected_metrics["average_actual_minus_estimated_close_pnl"]
        is None
        or rank_only_top_metrics["average_actual_minus_estimated_close_pnl"] is None
        else round(
            float(
                allocator_selected_metrics["average_actual_minus_estimated_close_pnl"]
            )
            - float(rank_only_top_metrics["average_actual_minus_estimated_close_pnl"]),
            4,
        ),
        "legacy_monitor_promotion_hit_rate": promoted_from_legacy_monitor_metrics[
            "positive_rate"
        ],
        "rejected_legacy_promotable_miss_rate": rejected_legacy_promotable_metrics[
            "positive_rate"
        ],
        "by_label": _aggregate_dimension_rows(all_rows, field="label"),
        "by_family": _aggregate_dimension_rows(all_rows, field="strategy_family"),
        "by_symbol": _aggregate_dimension_rows(all_rows, field="symbol"),
        "deployment_quality": {
            "allocator_selected": _build_deployment_quality_views(
                [row for row in all_rows if row.get("is_allocator_selected")]
            ),
            "actual_deployed": _build_deployment_quality_views(
                [row for row in all_rows if row.get("actual_position_matched")]
            ),
        },
    }
    warnings: list[str] = []
    if session_count < recent:
        warnings.append(
            f"Only {session_count} replayable sessions were available out of the requested {recent}."
        )
    if skipped_sessions:
        warnings.append(
            f"Skipped {len(skipped_sessions)} sessions because stored collector candidates were unavailable."
        )
    if sessions_with_allocator_selections < session_count:
        warnings.append(
            f"Allocator selections only appeared in {sessions_with_allocator_selections} of {session_count} replayed sessions, so pooled allocator metrics are sparse."
        )
    allocator_still_open_rate = allocator_selected_metrics.get("still_open_rate")
    if (
        allocator_still_open_rate is not None
        and float(allocator_still_open_rate) >= 0.5
    ):
        warnings.append(
            "Allocator scorecard outcomes are dominated by still_open post-market ideas, so average estimated PnL reflects modeled close-state rather than realized lifecycle results."
        )
    allocator_actual_coverage = allocator_selected_metrics.get("actual_coverage_rate")
    if allocator_actual_coverage is not None and float(allocator_actual_coverage) < 0.5:
        warnings.append(
            "Actual traded-position coverage for allocator-selected opportunities is sparse, so realized PnL comparisons are lower-confidence than modeled replay comparisons."
        )
    allocator_late_open_fill_rate = allocator_selected_metrics.get(
        "late_open_fill_rate"
    )
    if (
        allocator_late_open_fill_rate is not None
        and float(allocator_late_open_fill_rate) > 0.0
    ):
        warnings.append(
            "Some allocator-selected opportunities were opened after their configured force-close deadline, which points to execution-path drift in the live system."
        )
    allocator_force_close_exit_rate = allocator_selected_metrics.get(
        "force_close_exit_rate"
    )
    if (
        allocator_force_close_exit_rate is not None
        and float(allocator_force_close_exit_rate) >= 0.5
    ):
        warnings.append(
            "Allocator-selected actual trades are dominated by force-close exits, so live execution timing remains a primary risk."
        )
    allocator_actual_minus_close = allocator_selected_metrics.get(
        "average_actual_minus_estimated_close_pnl"
    )
    if (
        allocator_actual_minus_close is not None
        and float(allocator_actual_minus_close) < 0.0
    ):
        warnings.append(
            "Allocator-selected actual PnL is trailing modeled close-state PnL on the replay sample, which suggests execution drag or exit slippage."
        )
    return {
        "target": {
            "recent": recent,
            "label": label,
        },
        "aggregate": aggregate,
        "sessions": sessions,
        "skipped_sessions": skipped_sessions,
        "rows": all_rows,
        "warnings": warnings,
    }


__all__ = [
    "OpportunityReplayLookupError",
    "build_opportunity_replay",
    "build_recent_opportunity_replay_batch",
]
