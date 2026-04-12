from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
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
from spreads.runtime.config import default_database_url
from spreads.services.analysis import candidate_identity, resolved_estimated_pnl
from spreads.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    candidate_requires_favorable_setup,
)
from spreads.services.candidate_history_recovery import (
    recover_session_candidates_from_history,
)
from spreads.services.live_pipelines import parse_live_session_id

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

SLOT_LIMITS = {
    "reactive": {"slot_limit": 2, "risk_budget": 500.0},
    "tactical": {"slot_limit": 3, "risk_budget": 1000.0},
    "carry": {"slot_limit": 3, "risk_budget": 2000.0},
}
RECOVERY_TOP = 12
RECOVERY_PER_STRATEGY = 3


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
    calendar_status = str(candidate.get("calendar_status") or "").strip().lower()
    if calendar_status in {"clean", ""}:
        return "clean"
    return "event_risk"


def _minutes_between(start: Any, end: Any) -> float | None:
    start_text = _as_text(start)
    end_text = _as_text(end)
    if start_text is None or end_text is None:
        return None
    normalized_start = (
        start_text.replace("Z", "+00:00") if start_text.endswith("Z") else start_text
    )
    normalized_end = (
        end_text.replace("Z", "+00:00") if end_text.endswith("Z") else end_text
    )
    try:
        start_dt = datetime.fromisoformat(normalized_start)
        end_dt = datetime.fromisoformat(normalized_end)
    except ValueError:
        return None
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=UTC)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=UTC)
    return round((end_dt - start_dt).total_seconds() / 60.0, 1)


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
        return style_profile != "carry"
    return True


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
                bucket = _as_text(row.get("bucket"))
                if bucket is None:
                    continue
                count = max(int(_as_float(row.get("count")) or 0), 0)
                average_estimated_pnl = (
                    _as_float(row.get("average_estimated_pnl")) or 0.0
                )
                board_count = max(int(_as_float(row.get("board_count")) or 0), 0)
                watchlist_count = max(
                    int(_as_float(row.get("watchlist_count")) or 0), 0
                )
                bucket_totals = totals[dimension_key].setdefault(
                    bucket,
                    {
                        "count": 0.0,
                        "board_count": 0.0,
                        "watchlist_count": 0.0,
                        "estimated_pnl_total": 0.0,
                    },
                )
                bucket_totals["count"] += float(count)
                bucket_totals["board_count"] += float(board_count)
                bucket_totals["watchlist_count"] += float(watchlist_count)
                bucket_totals["estimated_pnl_total"] += average_estimated_pnl * float(
                    count
                )

    lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for dimension, bucket_totals in totals.items():
        dimension_lookup: dict[str, dict[str, Any]] = {}
        for bucket, totals_row in bucket_totals.items():
            count = int(totals_row["count"])
            if count <= 0:
                continue
            dimension_lookup[bucket] = {
                "bucket": bucket,
                "count": count,
                "board_count": int(totals_row["board_count"]),
                "watchlist_count": int(totals_row["watchlist_count"]),
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
    bucket: str | None,
    weight: float,
) -> tuple[float, dict[str, Any] | None]:
    if bucket is None:
        return 0.0, None
    row = dimension_lookup.get(dimension, {}).get(bucket)
    if row is None:
        return 0.0, None
    average_estimated_pnl = _as_float(row.get("average_estimated_pnl")) or 0.0
    return _clamp(average_estimated_pnl, -5.0, 5.0) * weight, {
        "dimension": dimension,
        "bucket": bucket,
        "average_estimated_pnl": average_estimated_pnl,
        "count": row.get("count"),
        "board_count": row.get("board_count"),
        "watchlist_count": row.get("watchlist_count"),
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


def _execution_complexity(family: str) -> float:
    if family in {"long_call", "long_put"}:
        return 0.2
    if family in {
        "call_credit_spread",
        "put_credit_spread",
        "call_debit_spread",
        "put_debit_spread",
    }:
        return 0.4
    if family == "iron_condor":
        return 0.8
    return 0.5


def _carry_buffer_ratio(candidate: Mapping[str, Any] | None) -> float | None:
    if not isinstance(candidate, Mapping):
        return None
    short_vs_expected_move = _as_float(candidate.get("short_vs_expected_move"))
    expected_move = _as_float(candidate.get("expected_move"))
    if short_vs_expected_move is None or expected_move in (None, 0.0):
        return None
    return _clamp(short_vs_expected_move / expected_move, 0.0, 1.5)


def _opportunity_buffer_ratio(opportunity: Opportunity) -> float | None:
    evidence = opportunity.evidence
    if not isinstance(evidence, Mapping):
        return None
    profile_evidence = evidence.get("profile_score_evidence")
    if not isinstance(profile_evidence, Mapping):
        return None
    value = _as_float(profile_evidence.get("buffer_ratio"))
    if value is None:
        return None
    return _clamp(value, 0.0, 1.5)


def _opportunity_rank_score(opportunity: Opportunity) -> float:
    if opportunity.style_profile != "carry":
        return opportunity.promotion_score
    buffer_ratio = _opportunity_buffer_ratio(opportunity)
    if buffer_ratio is None:
        return opportunity.promotion_score
    return round(opportunity.promotion_score + min(buffer_ratio * 2.0, 2.5), 4)


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
            parsed = parse_live_session_id(session_id)
            if parsed is not None:
                label = parsed["label"]
                session_date = parsed["session_date"]
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
        blockers = _product_policy_blockers(
            family=family,
            style_profile=snapshot.style_profile,
            product_class=product_class,
            horizon_band=horizon_band,
        )
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
            else ("preferred" if desirability >= 0.7 else "allowed")
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
            "avoid_event"
            if candidate
            and str(candidate.get("calendar_status") or "") not in {"", "clean"}
            else "none"
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
                        - (0.05 if event_timing_rule != "none" else 0.0),
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

        discovery_score = round(
            _as_float(row.get("quality_score"))
            or _as_float(candidate.get("quality_score"))
            or 0.0,
            1,
        )
        calibration_breakdown: list[dict[str, Any]] = []
        calibration_delta = 0.0
        for dimension, bucket, weight in (
            ("classification", _as_text(row.get("bucket")), 1.0),
            (
                "strategy",
                _as_text(candidate.get("strategy")) or _as_text(row.get("strategy")),
                0.8,
            ),
            ("symbol", symbol, 0.5),
            ("setup_status", _as_text(candidate.get("setup_status")), 0.7),
        ):
            delta, evidence = _dimension_adjustment(
                dimension_lookup=dimension_lookup,
                dimension=dimension,
                bucket=bucket,
                weight=weight,
            )
            calibration_delta += delta
            if evidence is not None:
                evidence["score_delta"] = round(delta, 3)
                calibration_breakdown.append(evidence)

        setup_delta = ((_as_float(candidate.get("setup_score")) or 50.0) - 50.0) * 0.15
        fill_ratio_delta = (
            (_as_float(candidate.get("fill_ratio")) or 0.8) - 0.8
        ) * 25.0
        profile_components, profile_evidence = _profile_specific_score_components(
            candidate=candidate,
            style_profile=strategy_intent.style_profile,
            cycle=cycle,
        )
        component_boost = sum(
            value
            for key, value in profile_components.items()
            if not key.endswith("_penalty")
        )
        component_penalty = sum(
            value
            for key, value in profile_components.items()
            if key.endswith("_penalty")
        )
        penalty = 0.0
        if str(candidate.get("data_status") or "") != "clean":
            penalty += 8.0
        if str(candidate.get("calendar_status") or "") not in {"", "clean"}:
            penalty += 6.0
        if strategy_intent.policy_state == "blocked":
            penalty += 20.0

        raw_promotion_score = (
            discovery_score
            + setup_delta
            + fill_ratio_delta
            + calibration_delta
            + component_boost
            - penalty
            - component_penalty
        )
        promotion_score = round(_clamp(raw_promotion_score, 0.0, 100.0), 1)

        promotion_floor = 70.0
        monitor_floor = 55.0
        if strategy_intent.style_profile == "reactive":
            promotion_floor = 78.0
            monitor_floor = 62.0
        elif strategy_intent.style_profile == "carry":
            promotion_floor = 68.0
            monitor_floor = 58.0

        if strategy_intent.policy_state == "blocked":
            state = "blocked"
            state_reason = "Blocked by product or event policy."
        elif promotion_score >= promotion_floor:
            state = "promotable"
            state_reason = "Meets provisional promotion floor."
        elif promotion_score >= monitor_floor:
            state = "monitor"
            state_reason = "Retained but below promotion floor."
        else:
            state = "discarded"
            state_reason = "Below provisional retention floor."

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
            execution_complexity=_execution_complexity(family),
            product_class=_product_class(symbol),
            legacy_bucket=_as_text(row.get("bucket")),
            evidence={
                "legacy_bucket": row.get("bucket"),
                "legacy_position": row.get("position"),
                "quality_score": _as_float(candidate.get("quality_score")),
                "setup_score_delta": round(setup_delta, 3),
                "fill_ratio_delta": round(fill_ratio_delta, 3),
                "calibration_delta": round(calibration_delta, 3),
                "calibration_breakdown": calibration_breakdown,
                "profile_score_components": profile_components,
                "profile_score_evidence": profile_evidence,
                "penalty": round(penalty, 3),
                "setup_status": _as_text(candidate.get("setup_status")),
                "data_status": _as_text(candidate.get("data_status")),
                "calendar_status": _as_text(candidate.get("calendar_status")),
                "days_to_expiration": candidate.get("days_to_expiration"),
                "run_id": row.get("run_id"),
            },
            legs=_build_legs(candidate),
        )
        built.append(opportunity)

    ranked = sorted(
        built,
        key=lambda item: (
            _opportunity_rank_score(item),
            item.promotion_score,
            item.discovery_score,
            -(item.execution_complexity or 0.0),
        ),
        reverse=True,
    )
    return [
        Opportunity(
            **{
                **item.to_payload(),
                "legs": item.legs,
                "rank": index,
            }
        )
        for index, item in enumerate(ranked, start=1)
    ]


def _allocation_score(
    *,
    opportunity: Opportunity,
    style_profile: str,
) -> float:
    policy = SLOT_LIMITS[style_profile]
    desirability = opportunity.promotion_score / 100.0
    edge_value = _clamp(opportunity.expected_edge_value or 0.0, 0.0, 0.25) / 0.25
    readiness = 1.0 if opportunity.state == "promotable" else 0.5
    max_loss = opportunity.max_loss or policy["risk_budget"]
    capital_efficiency = 1.0 - _clamp(max_loss / policy["risk_budget"], 0.0, 1.0)
    if style_profile == "carry":
        buffer_ratio = _opportunity_buffer_ratio(opportunity) or 0.0
        structure_quality = _clamp((buffer_ratio - 0.15) / 0.20, 0.0, 1.0)
        return round(
            100.0
            * (
                0.40 * desirability
                + 0.25 * edge_value
                + 0.10 * readiness
                + 0.15 * capital_efficiency
                + 0.10 * structure_quality
            ),
            1,
        )
    return round(
        100.0
        * (
            0.45 * desirability
            + 0.25 * edge_value
            + 0.15 * readiness
            + 0.15 * capital_efficiency
        ),
        1,
    )


def _build_allocation_decisions(
    opportunities: list[Opportunity],
) -> list[AllocationDecision]:
    if not opportunities:
        return []
    style_profile = opportunities[0].style_profile
    policy = SLOT_LIMITS.get(style_profile, SLOT_LIMITS["tactical"])
    remaining_budget = float(policy["risk_budget"])
    remaining_slots = int(policy["slot_limit"])
    taken_symbols: set[str] = set()
    ranked = sorted(
        opportunities,
        key=lambda item: (
            _allocation_score(opportunity=item, style_profile=style_profile),
            -item.rank,
        ),
        reverse=True,
    )

    decisions: list[AllocationDecision] = []
    for opportunity in ranked:
        allocation_score = _allocation_score(
            opportunity=opportunity, style_profile=style_profile
        )
        rejection_codes: list[str] = []
        allocation_state = "not_allocated"
        allocation_reason = "Not selected."
        max_loss = opportunity.max_loss or 0.0
        budget_before = remaining_budget
        slots_before = remaining_slots

        if opportunity.state != "promotable":
            rejection_codes.append("not_promotable")
            allocation_reason = "Opportunity did not clear the promotion floor."
        elif opportunity.symbol in taken_symbols:
            rejection_codes.append("same_symbol_conflict")
            allocation_reason = (
                "A higher-ranked opportunity already consumed the symbol slot."
            )
        elif remaining_slots <= 0:
            rejection_codes.append("slot_full")
            allocation_reason = "The style slot budget is already full."
        elif max_loss > remaining_budget:
            rejection_codes.append("budget_exhausted")
            allocation_reason = "Remaining downside budget is too small."
        elif allocation_score < 55.0:
            rejection_codes.append("allocation_score_too_low")
            allocation_reason = (
                "Portfolio-adjusted score is below the allocation floor."
            )
        else:
            allocation_state = "allocated"
            allocation_reason = "Selected by the provisional portfolio allocator."
            taken_symbols.add(opportunity.symbol)
            remaining_slots -= 1
            remaining_budget -= max_loss

        decisions.append(
            AllocationDecision(
                allocation_id=f"allocation:{opportunity.opportunity_id}",
                opportunity_id=opportunity.opportunity_id,
                cycle_id=opportunity.cycle_id,
                session_id=opportunity.session_id,
                allocation_state=allocation_state,
                allocation_score=allocation_score,
                slot_class=style_profile,
                allocation_reason=allocation_reason,
                rejection_codes=rejection_codes,
                budget_impact={
                    "max_loss": max_loss,
                    "risk_budget_before": round(budget_before, 2),
                    "risk_budget_after": round(remaining_budget, 2),
                    "slots_before": slots_before,
                    "slots_after": remaining_slots,
                },
                evidence={
                    "rank": opportunity.rank,
                    "promotion_score": opportunity.promotion_score,
                    "legacy_bucket": opportunity.legacy_bucket,
                    "product_class": opportunity.product_class,
                },
            )
        )
    return decisions


def _execution_template(opportunity: Opportunity) -> dict[str, str]:
    family = opportunity.strategy_family
    style = opportunity.style_profile
    if family in {"long_call", "long_put"}:
        return {
            "order_structure": "single_leg",
            "entry_policy": "passive_then_small_escalation"
            if style == "reactive"
            else "patient_single_leg_entry",
            "price_policy": "tight_debit_cap",
            "timeout_policy": "short"
            if style == "reactive"
            else ("medium" if style == "tactical" else "long"),
            "replace_policy": "1_step" if style == "reactive" else "2_step",
            "exit_policy": "defined_risk_directional_exit",
        }
    if family in {"call_debit_spread", "put_debit_spread"}:
        return {
            "order_structure": "vertical",
            "entry_policy": "passive_then_midpoint",
            "price_policy": "tight_debit_cap",
            "timeout_policy": "short" if style == "reactive" else "medium",
            "replace_policy": "1_step" if style == "reactive" else "2_step",
            "exit_policy": "defined_risk_debit_spread_exit",
        }
    if family in {"call_credit_spread", "put_credit_spread"}:
        return {
            "order_structure": "vertical",
            "entry_policy": "passive_credit_entry",
            "price_policy": "credit_floor_from_scanned_midpoint",
            "timeout_policy": "short"
            if style == "reactive"
            else ("medium" if style == "tactical" else "long"),
            "replace_policy": "1_step" if style == "reactive" else "2_step",
            "exit_policy": "defined_risk_credit_spread_exit",
        }
    return {
        "order_structure": "condor",
        "entry_policy": "passive_complex_entry",
        "price_policy": "complex_credit_floor",
        "timeout_policy": "medium",
        "replace_policy": "2_step",
        "exit_policy": "defined_risk_condor_exit",
    }


def _build_execution_intents(
    *,
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
) -> list[ExecutionIntent]:
    opportunity_by_id = {item.opportunity_id: item for item in opportunities}
    intents: list[ExecutionIntent] = []
    for decision in allocation_decisions:
        if decision.allocation_state != "allocated":
            continue
        opportunity = opportunity_by_id[decision.opportunity_id]
        template = _execution_template(opportunity)
        intents.append(
            ExecutionIntent(
                execution_intent_id=f"execution_intent:{opportunity.opportunity_id}",
                opportunity_id=opportunity.opportunity_id,
                cycle_id=opportunity.cycle_id,
                session_id=opportunity.session_id,
                symbol=opportunity.symbol,
                strategy_family=opportunity.strategy_family,
                order_structure=template["order_structure"],
                entry_policy=template["entry_policy"],
                price_policy=template["price_policy"],
                timeout_policy=template["timeout_policy"],
                replace_policy=template["replace_policy"],
                exit_policy=template["exit_policy"],
                validation_state="provisional_offline",
                evidence={
                    "allocation_score": decision.allocation_score,
                    "legacy_bucket": opportunity.legacy_bucket,
                    "rank": opportunity.rank,
                    "legs": [leg.to_payload() for leg in opportunity.legs],
                },
            )
        )
    return intents


def _opportunity_identity(opportunity: Opportunity) -> tuple[str, str, str, str, str]:
    return (
        opportunity.symbol,
        opportunity.legacy_strategy,
        opportunity.expiration_date,
        opportunity.short_symbol,
        opportunity.long_symbol,
    )


def _build_outcome_matches(
    *,
    opportunities: list[Opportunity],
    analysis_run: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    summary = analysis_run.get("summary") if isinstance(analysis_run, Mapping) else None
    outcomes = summary.get("outcomes") if isinstance(summary, Mapping) else None
    ideas = list(outcomes.get("ideas") or []) if isinstance(outcomes, Mapping) else []

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
        estimated_pnl = None if idea is None else resolved_estimated_pnl(idea)
        matches[opportunity.opportunity_id] = {
            "matched": idea is not None,
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
        }
    return matches


def _summarize_outcome_rows(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    pnl_values = [
        float(row["estimated_pnl"])
        for row in rows
        if row.get("estimated_pnl") is not None
    ]
    signed_rows = [row for row in rows if row.get("positive_outcome") is not None]
    outcome_bucket_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        bucket = _as_text(row.get("outcome_bucket"))
        if bucket is not None:
            outcome_bucket_counts[bucket] += 1
    still_open_count = int(outcome_bucket_counts.get("still_open") or 0)
    return {
        "average_estimated_pnl": None if not pnl_values else round(mean(pnl_values), 4),
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
    }


def _slice_metrics(
    *,
    items: list[Opportunity],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    matched = [
        outcome_matches[item.opportunity_id]
        for item in items
        if outcome_matches.get(item.opportunity_id, {}).get("matched")
    ]
    metrics = _summarize_outcome_rows(matched)
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
    promoted_watchlist_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("promoted_from_watchlist") or [])
        if isinstance(item, Mapping)
    }
    rejected_board_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("rejected_legacy_board") or [])
        if isinstance(item, Mapping)
    }

    rows: list[dict[str, Any]] = []
    for opportunity in opportunities:
        allocation = allocation_by_id.get(opportunity.opportunity_id)
        outcome = outcome_matches.get(opportunity.opportunity_id, {})
        rows.append(
            {
                "label": session.get("label"),
                "session_date": session.get("session_date"),
                "cycle_id": session.get("cycle_id"),
                "candidate_id": opportunity.candidate_id,
                "opportunity_id": opportunity.opportunity_id,
                "symbol": opportunity.symbol,
                "strategy_family": opportunity.strategy_family,
                "legacy_strategy": opportunity.legacy_strategy,
                "expiration_date": opportunity.expiration_date,
                "short_symbol": opportunity.short_symbol,
                "long_symbol": opportunity.long_symbol,
                "legacy_bucket": opportunity.legacy_bucket,
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
                "estimated_pnl": outcome.get("estimated_pnl"),
                "positive_outcome": outcome.get("positive_outcome"),
                "outcome_bucket": outcome.get("outcome_bucket"),
                "replay_verdict": outcome.get("replay_verdict"),
                "setup_status": outcome.get("setup_status"),
                "vwap_regime": outcome.get("vwap_regime"),
                "trend_regime": outcome.get("trend_regime"),
                "opening_range_regime": outcome.get("opening_range_regime"),
                "is_legacy_board": opportunity.legacy_bucket == "board",
                "is_legacy_watchlist": opportunity.legacy_bucket == "watchlist",
                "is_rank_only_top": opportunity.opportunity_id in rank_only_ids,
                "is_allocator_selected": opportunity.opportunity_id in allocator_ids,
                "is_promoted_from_watchlist": opportunity.opportunity_id
                in promoted_watchlist_ids,
                "is_rejected_legacy_board": opportunity.opportunity_id
                in rejected_board_ids,
            }
        )
    return rows


def _aggregate_dimension_rows(
    rows: list[dict[str, Any]],
    *,
    field: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(field) or "unknown")].append(row)

    result: list[dict[str, Any]] = []
    for bucket, bucket_rows in grouped.items():
        matched = [row for row in bucket_rows if row.get("matched_outcome")]
        metrics = _summarize_outcome_rows(matched)
        result.append(
            {
                "bucket": bucket,
                "count": len(bucket_rows),
                "matched_count": len(matched),
                "coverage_rate": None
                if not bucket_rows
                else round(len(matched) / len(bucket_rows), 4),
                "allocator_selected_count": sum(
                    1 for row in bucket_rows if row.get("is_allocator_selected")
                ),
                "legacy_board_count": sum(
                    1 for row in bucket_rows if row.get("is_legacy_board")
                ),
                "rank_only_top_count": sum(
                    1 for row in bucket_rows if row.get("is_rank_only_top")
                ),
                "promoted_from_watchlist_count": sum(
                    1 for row in bucket_rows if row.get("is_promoted_from_watchlist")
                ),
                "rejected_legacy_board_count": sum(
                    1 for row in bucket_rows if row.get("is_rejected_legacy_board")
                ),
                **metrics,
            }
        )
    result.sort(key=lambda row: (-int(row["count"]), row["bucket"]))
    return result


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
    legacy_board = [item for item in opportunities if item.legacy_bucket == "board"]
    promoted_from_watchlist = [
        item for item in allocator_selected if item.legacy_bucket == "watchlist"
    ]
    rejected_legacy_board = [
        item
        for item in legacy_board
        if allocation_by_id.get(item.opportunity_id) is None
        or allocation_by_id[item.opportunity_id].allocation_state != "allocated"
    ]

    scorecard = {
        "legacy_board": _slice_metrics(
            items=legacy_board,
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
        "promoted_from_watchlist": _slice_metrics(
            items=promoted_from_watchlist,
            outcome_matches=outcome_matches,
        ),
        "rejected_legacy_board": _slice_metrics(
            items=rejected_legacy_board,
            outcome_matches=outcome_matches,
        ),
    }
    legacy_avg = scorecard["legacy_board"]["average_estimated_pnl"]
    rank_only_avg = scorecard["rank_only_top"]["average_estimated_pnl"]
    allocator_avg = scorecard["allocator_selected"]["average_estimated_pnl"]
    scorecard["deltas"] = {
        "allocator_minus_legacy_board_avg_estimated_pnl": None
        if allocator_avg is None or legacy_avg is None
        else round(float(allocator_avg) - float(legacy_avg), 4),
        "allocator_minus_rank_only_avg_estimated_pnl": None
        if allocator_avg is None or rank_only_avg is None
        else round(float(allocator_avg) - float(rank_only_avg), 4),
        "watchlist_promotion_hit_rate": scorecard["promoted_from_watchlist"][
            "positive_rate"
        ],
        "rejected_board_miss_rate": scorecard["rejected_legacy_board"]["positive_rate"],
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
        "legacy_bucket": opportunity.legacy_bucket,
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
    legacy_board = [item for item in opportunities if item.legacy_bucket == "board"]
    legacy_watchlist = [
        item for item in opportunities if item.legacy_bucket == "watchlist"
    ]
    allocated = [
        item
        for item in opportunities
        if allocation_by_opportunity_id.get(item.opportunity_id) is not None
        and allocation_by_opportunity_id[item.opportunity_id].allocation_state
        == "allocated"
    ]
    comparison_size = max(len(legacy_board), len(allocated))
    promotable_rank_only = [
        item for item in opportunities if item.state == "promotable"
    ]
    rank_only_top = (
        []
        if comparison_size <= 0
        else (promotable_rank_only[:comparison_size] or opportunities[:comparison_size])
    )

    allocated_ids = {item.opportunity_id for item in allocated}
    legacy_board_ids = {item.opportunity_id for item in legacy_board}
    rank_only_ids = {item.opportunity_id for item in rank_only_top}

    return {
        "comparison_size": comparison_size,
        "legacy_board": {
            "count": len(legacy_board),
            "symbols": sorted({item.symbol for item in legacy_board}),
            "candidate_ids": [item.candidate_id for item in legacy_board],
            "items": [
                _comparison_item(
                    item,
                    allocation_by_opportunity_id.get(item.opportunity_id),
                    outcome_matches.get(item.opportunity_id),
                )
                for item in legacy_board
            ],
        },
        "legacy_watchlist": {
            "count": len(legacy_watchlist),
            "symbols": sorted({item.symbol for item in legacy_watchlist}),
            "candidate_ids": [item.candidate_id for item in legacy_watchlist],
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
        "promoted_from_watchlist": [
            _comparison_item(
                item,
                allocation_by_opportunity_id.get(item.opportunity_id),
                outcome_matches.get(item.opportunity_id),
            )
            for item in allocated
            if item.legacy_bucket == "watchlist"
        ],
        "rejected_legacy_board": [
            _comparison_item(
                item,
                allocation_by_opportunity_id.get(item.opportunity_id),
                outcome_matches.get(item.opportunity_id),
            )
            for item in legacy_board
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
            "allocator_vs_legacy_board_count": len(allocated_ids & legacy_board_ids),
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
    legacy_board_symbols = sorted(
        {item.symbol for item in opportunities if item.legacy_bucket == "board"}
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
    newly_promoted_watchlist = sorted(
        {
            item.symbol
            for item in opportunities
            if item.legacy_bucket == "watchlist" and item.state == "promotable"
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
        "legacy_board_symbols": legacy_board_symbols,
        "newly_promoted_watchlist_symbols": newly_promoted_watchlist,
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
        board_row = classification_lookup.get("board")
        watchlist_row = classification_lookup.get("watchlist")
        board_pnl = (
            None
            if board_row is None
            else _as_float(board_row.get("average_estimated_pnl"))
        )
        watchlist_pnl = (
            None
            if watchlist_row is None
            else _as_float(watchlist_row.get("average_estimated_pnl"))
        )
        if (
            board_pnl is not None
            and watchlist_pnl is not None
            and watchlist_pnl > board_pnl
        ):
            warnings.append(
                "Prior-session calibration for this label favors watchlist ideas over legacy board ideas, so watchlist candidates may surface as promotable."
            )
    return summary, warnings


def _write_json_export(path: str, payload: Mapping[str, Any]) -> None:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, default=str) + "\n")


def _write_csv_export(path: str, rows: list[dict[str, Any]]) -> None:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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
                "bucket": "recovered",
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
    allocation_decisions = _build_allocation_decisions(opportunities)
    outcome_matches = _build_outcome_matches(
        opportunities=opportunities,
        analysis_run=analysis_run,
    )
    execution_intents = _build_execution_intents(
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
            "No opportunities cleared the provisional allocator for this session, so allocator-vs-board comparisons are based on zero selected opportunities."
        )
    allocator_still_open_rate = allocator_metrics.get("still_open_rate")
    if (
        allocator_still_open_rate is not None
        and float(allocator_still_open_rate) >= 0.5
    ):
        warnings.append(
            "Allocator scorecard outcomes are dominated by still_open post-market ideas, so average estimated PnL reflects modeled close-state rather than realized lifecycle results."
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
    promoted_from_watchlist_total = 0
    rejected_legacy_board_total = 0
    allocator_vs_legacy_board_total = 0
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
        promoted_from_watchlist = list(comparison.get("promoted_from_watchlist") or [])
        rejected_legacy_board = list(comparison.get("rejected_legacy_board") or [])
        overlap = dict(comparison.get("overlap") or {})
        promoted_from_watchlist_total += len(promoted_from_watchlist)
        rejected_legacy_board_total += len(rejected_legacy_board)
        allocator_vs_legacy_board_total += int(
            overlap.get("allocator_vs_legacy_board_count") or 0
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
                    "legacy_board_candidate_ids": (
                        comparison.get("legacy_board") or {}
                    ).get("candidate_ids", []),
                    "rank_only_candidate_ids": (
                        comparison.get("rank_only_top") or {}
                    ).get("candidate_ids", []),
                    "allocator_candidate_ids": (
                        comparison.get("provisional_allocator") or {}
                    ).get("candidate_ids", []),
                    "promoted_from_watchlist": promoted_from_watchlist,
                    "rejected_legacy_board": rejected_legacy_board,
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
        metrics = _summarize_outcome_rows(matched_rows)
        return {
            "count": len(scoped_rows),
            "matched_count": len(matched_rows),
            "coverage_rate": None
            if not scoped_rows
            else round(len(matched_rows) / len(scoped_rows), 4),
            **metrics,
        }

    legacy_board_metrics = pooled_metrics("is_legacy_board")
    rank_only_top_metrics = pooled_metrics("is_rank_only_top")
    allocator_selected_metrics = pooled_metrics("is_allocator_selected")
    promoted_from_watchlist_metrics = pooled_metrics("is_promoted_from_watchlist")
    rejected_legacy_board_metrics = pooled_metrics("is_rejected_legacy_board")
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
        "sessions_with_watchlist_promotions": sum(
            1 for item in sessions if item["comparison"]["promoted_from_watchlist"]
        ),
        "sessions_with_rejected_legacy_board": sum(
            1 for item in sessions if item["comparison"]["rejected_legacy_board"]
        ),
        "promoted_from_watchlist_total": promoted_from_watchlist_total,
        "rejected_legacy_board_total": rejected_legacy_board_total,
        "average_allocator_vs_legacy_board_overlap": round(
            allocator_vs_legacy_board_total / session_count,
            3,
        ),
        "average_allocator_vs_rank_only_overlap": round(
            allocator_vs_rank_only_total / session_count,
            3,
        ),
        "legacy_board_metrics": legacy_board_metrics,
        "rank_only_top_metrics": rank_only_top_metrics,
        "allocator_selected_metrics": allocator_selected_metrics,
        "promoted_from_watchlist_metrics": promoted_from_watchlist_metrics,
        "rejected_legacy_board_metrics": rejected_legacy_board_metrics,
        "allocator_minus_legacy_board_avg_estimated_pnl": None
        if allocator_selected_metrics["average_estimated_pnl"] is None
        or legacy_board_metrics["average_estimated_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_pnl"])
            - float(legacy_board_metrics["average_estimated_pnl"]),
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
        "watchlist_promotion_hit_rate": promoted_from_watchlist_metrics[
            "positive_rate"
        ],
        "rejected_board_miss_rate": rejected_legacy_board_metrics["positive_rate"],
        "by_label": _aggregate_dimension_rows(all_rows, field="label"),
        "by_family": _aggregate_dimension_rows(all_rows, field="strategy_family"),
        "by_symbol": _aggregate_dimension_rows(all_rows, field="symbol"),
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


def _render_text(payload: Mapping[str, Any]) -> str:
    session = payload.get("session") or {}
    summary = payload.get("summary") or {}
    comparison = payload.get("comparison") or {}
    scorecard = payload.get("scorecard") or {}
    opportunities = payload.get("opportunities") or []
    allocations = {
        row["opportunity_id"]: row
        for row in payload.get("allocation_decisions") or []
        if isinstance(row, Mapping)
    }

    lines = [
        f"Session: {session.get('label')} | {session.get('session_date')} | cycle {session.get('cycle_id')}",
        f"Style: {summary.get('style_profile')} | candidates {summary.get('candidate_count')} | promotable {summary.get('promotable_count')} | allocated {summary.get('allocated_count')}",
        f"Analysis verdict: {summary.get('analysis_verdict') or 'n/a'}",
        "",
        "Top opportunities:",
    ]
    for row in opportunities[:6]:
        if not isinstance(row, Mapping):
            continue
        allocation = allocations.get(str(row.get("opportunity_id"))) or {}
        lines.append(
            "- "
            f"{row.get('symbol')} {row.get('strategy_family')} "
            f"| candidate {row.get('candidate_id')} "
            f"| rank {row.get('rank')} "
            f"| state {row.get('state')} "
            f"| promo {row.get('promotion_score')} "
            f"| alloc {allocation.get('allocation_state', 'n/a')} "
            f"| alloc_score {allocation.get('allocation_score', 'n/a')} "
            f"| legacy {row.get('legacy_bucket')} "
            f"| reason {allocation.get('allocation_reason', row.get('state_reason'))}"
        )
    if comparison:
        lines.append("")
        lines.append("Comparison:")
        lines.append(
            "- "
            f"legacy board ids {comparison.get('legacy_board', {}).get('candidate_ids', [])} "
            f"| symbols {comparison.get('legacy_board', {}).get('symbols', [])}"
        )
        lines.append(
            "- "
            f"rank-only top ids {comparison.get('rank_only_top', {}).get('candidate_ids', [])} "
            f"| symbols {comparison.get('rank_only_top', {}).get('symbols', [])}"
        )
        lines.append(
            "- "
            f"allocator ids {comparison.get('provisional_allocator', {}).get('candidate_ids', [])} "
            f"| symbols {comparison.get('provisional_allocator', {}).get('symbols', [])}"
        )
        promoted_from_watchlist = comparison.get("promoted_from_watchlist") or []
        if promoted_from_watchlist:
            lines.append(
                "- "
                f"promoted from watchlist {[item.get('candidate_id') for item in promoted_from_watchlist]}"
            )
        rejected_legacy_board = comparison.get("rejected_legacy_board") or []
        if rejected_legacy_board:
            lines.append("- rejected legacy board:")
            for item in rejected_legacy_board[:4]:
                lines.append(
                    "  "
                    f"{item.get('candidate_id')} {item.get('symbol')} {item.get('strategy_family')} "
                    f"| reason {item.get('allocation_reason')}"
                )
    if scorecard:
        allocator_metrics = scorecard.get("allocator_selected") or {}
        legacy_metrics = scorecard.get("legacy_board") or {}
        rank_only_metrics = scorecard.get("rank_only_top") or {}
        deltas = scorecard.get("deltas") or {}
        lines.append("")
        lines.append("Scorecard:")
        lines.append(
            "- "
            f"legacy board avg pnl {legacy_metrics.get('average_estimated_pnl')} "
            f"| positive_rate {legacy_metrics.get('positive_rate')} "
            f"| still_open_rate {legacy_metrics.get('still_open_rate')}"
        )
        lines.append(
            "- "
            f"rank-only avg pnl {rank_only_metrics.get('average_estimated_pnl')} "
            f"| positive_rate {rank_only_metrics.get('positive_rate')} "
            f"| still_open_rate {rank_only_metrics.get('still_open_rate')}"
        )
        lines.append(
            "- "
            f"allocator avg pnl {allocator_metrics.get('average_estimated_pnl')} "
            f"| positive_rate {allocator_metrics.get('positive_rate')} "
            f"| still_open_rate {allocator_metrics.get('still_open_rate')}"
        )
        lines.append(
            "- "
            f"allocator minus legacy board {deltas.get('allocator_minus_legacy_board_avg_estimated_pnl')} "
            f"| watchlist hit rate {deltas.get('watchlist_promotion_hit_rate')} "
            f"| rejected board miss rate {deltas.get('rejected_board_miss_rate')}"
        )
    warnings = payload.get("warnings") or []
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        for warning in warnings:
            lines.append(f"- {warning}")
    return "\n".join(lines)


def _render_batch_text(payload: Mapping[str, Any]) -> str:
    target = payload.get("target") or {}
    aggregate = payload.get("aggregate") or {}
    sessions = payload.get("sessions") or []
    skipped_sessions = payload.get("skipped_sessions") or []
    warnings = payload.get("warnings") or []
    lines = [
        f"Recent sessions: {aggregate.get('session_count')} of requested {aggregate.get('requested_recent')} | label filter {target.get('label') or 'all'}",
        f"Skipped sessions: {aggregate.get('skipped_session_count')}",
        f"Allocator selections: {(aggregate.get('allocator_selected_metrics') or {}).get('count')} opportunities across {aggregate.get('sessions_with_allocator_selections')} sessions",
        f"Watchlist promotions: {aggregate.get('promoted_from_watchlist_total')} across {aggregate.get('sessions_with_watchlist_promotions')} sessions",
        f"Rejected legacy board candidates: {aggregate.get('rejected_legacy_board_total')} across {aggregate.get('sessions_with_rejected_legacy_board')} sessions",
        f"Average overlap | allocator vs legacy board {aggregate.get('average_allocator_vs_legacy_board_overlap')} | allocator vs rank-only {aggregate.get('average_allocator_vs_rank_only_overlap')}",
        f"Pooled avg pnl | legacy board {(aggregate.get('legacy_board_metrics') or {}).get('average_estimated_pnl')} | rank-only {(aggregate.get('rank_only_top_metrics') or {}).get('average_estimated_pnl')} | allocator {(aggregate.get('allocator_selected_metrics') or {}).get('average_estimated_pnl')}",
        f"Still-open rate | legacy board {(aggregate.get('legacy_board_metrics') or {}).get('still_open_rate')} | rank-only {(aggregate.get('rank_only_top_metrics') or {}).get('still_open_rate')} | allocator {(aggregate.get('allocator_selected_metrics') or {}).get('still_open_rate')}",
        f"Pooled deltas | allocator minus legacy board {aggregate.get('allocator_minus_legacy_board_avg_estimated_pnl')} | allocator minus rank-only {aggregate.get('allocator_minus_rank_only_avg_estimated_pnl')}",
        f"Hit rates | watchlist promotions {aggregate.get('watchlist_promotion_hit_rate')} | rejected board miss rate {aggregate.get('rejected_board_miss_rate')}",
        f"Verdicts: {aggregate.get('verdict_counts')}",
        "",
        "Sessions:",
    ]
    for item in sessions:
        if not isinstance(item, Mapping):
            continue
        session = item.get("session") or {}
        summary = item.get("summary") or {}
        comparison = item.get("comparison") or {}
        promoted = comparison.get("promoted_from_watchlist") or []
        rejected = comparison.get("rejected_legacy_board") or []
        lines.append(
            "- "
            f"{session.get('label')} {session.get('session_date')} "
            f"| verdict {summary.get('analysis_verdict') or 'n/a'} "
            f"| allocated {summary.get('allocated_count')} "
            f"| promoted_watchlist {[row.get('candidate_id') for row in promoted]} "
            f"| rejected_board {[row.get('candidate_id') for row in rejected]}"
        )
    if skipped_sessions:
        lines.append("")
        lines.append("Skipped:")
        for item in skipped_sessions:
            if not isinstance(item, Mapping):
                continue
            lines.append(
                "- "
                f"{item.get('label')} {item.get('session_date')} | reason {item.get('reason')}"
            )
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        for warning in warnings:
            lines.append(f"- {warning}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an offline opportunity-design replay from stored collector and post-market data.",
    )
    parser.add_argument(
        "--db", default=default_database_url(), help="Postgres database URL."
    )
    parser.add_argument("--session-id", help="Session id to replay.")
    parser.add_argument("--label", help="Collector label to replay.")
    parser.add_argument("--date", help="Session date in YYYY-MM-DD.")
    parser.add_argument(
        "--recent",
        type=int,
        help="Build a batch report across the most recent succeeded post-market sessions.",
    )
    parser.add_argument(
        "--export-json",
        help="Write the full replay payload to a JSON file.",
    )
    parser.add_argument(
        "--export-csv",
        help="Write flattened opportunity rows to a CSV file.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.recent is not None:
            if args.session_id is not None or args.date is not None:
                raise SystemExit(
                    "--recent cannot be combined with --session-id or --date"
                )
            payload = build_recent_opportunity_replay_batch(
                db_target=args.db,
                recent=args.recent,
                label=args.label,
            )
        else:
            payload = build_opportunity_replay(
                db_target=args.db,
                session_id=args.session_id,
                label=args.label,
                session_date=args.date,
            )
    except OpportunityReplayLookupError as exc:
        raise SystemExit(str(exc)) from None

    if args.export_json:
        _write_json_export(args.export_json, payload)
    if args.export_csv:
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            rows = []
        _write_csv_export(
            args.export_csv, [dict(row) for row in rows if isinstance(row, Mapping)]
        )

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        if args.recent is not None:
            print(_render_batch_text(payload))
        else:
            print(_render_text(payload))
    return 0


__all__ = [
    "OpportunityReplayLookupError",
    "build_opportunity_replay",
    "build_recent_opportunity_replay_batch",
    "main",
    "parse_args",
]


if __name__ == "__main__":
    raise SystemExit(main())
