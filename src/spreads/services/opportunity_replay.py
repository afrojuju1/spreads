from __future__ import annotations

import argparse
import json
from collections import defaultdict
from collections.abc import Mapping
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


def _build_dimension_lookup(
    summary: Mapping[str, Any] | None,
) -> dict[str, dict[str, dict[str, Any]]]:
    tuning = summary.get("tuning") if isinstance(summary, Mapping) else None
    dimensions = tuning.get("dimensions") if isinstance(tuning, Mapping) else None
    if not isinstance(dimensions, Mapping):
        return {}
    lookup: dict[str, dict[str, dict[str, Any]]] = {}
    for dimension, rows in dimensions.items():
        if not isinstance(rows, list):
            continue
        bucket_lookup: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            bucket = _as_text(row.get("bucket"))
            if bucket is None:
                continue
            bucket_lookup[bucket] = dict(row)
        if bucket_lookup:
            lookup[str(dimension)] = bucket_lookup
    return lookup


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
    if family == "iron_condor":
        return 0.8
    if family in {
        "call_credit_spread",
        "put_credit_spread",
        "call_debit_spread",
        "put_debit_spread",
    }:
        return 0.4
    return 0.2


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
        if str(candidate.get("data_status") or "") != "clean":
            blockers.append("data_quality_not_clean")
        if str(candidate.get("calendar_status") or "") not in {"", "clean"}:
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
    analysis_run: Mapping[str, Any] | None,
) -> list[Opportunity]:
    strategy_by_key = {
        (item.symbol, item.strategy_family): item for item in strategy_intents
    }
    horizon_by_key = {
        (item.symbol, item.strategy_family): item for item in horizon_intents
    }
    dimension_lookup = _build_dimension_lookup(
        analysis_run.get("summary") if isinstance(analysis_run, Mapping) else None
    )

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
        penalty = 0.0
        if str(candidate.get("data_status") or "") != "clean":
            penalty += 8.0
        if str(candidate.get("calendar_status") or "") not in {"", "clean"}:
            penalty += 6.0
        if strategy_intent.policy_state == "blocked":
            penalty += 20.0

        promotion_score = round(
            _clamp(
                discovery_score
                + setup_delta
                + fill_ratio_delta
                + calibration_delta
                - penalty,
                0.0,
                100.0,
            ),
            1,
        )

        if strategy_intent.policy_state == "blocked":
            state = "blocked"
            state_reason = "Blocked by product or event policy."
        elif promotion_score >= 70.0:
            state = "promotable"
            state_reason = "Meets provisional promotion floor."
        elif promotion_score >= 55.0:
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


def _comparison_item(
    opportunity: Opportunity,
    allocation: AllocationDecision | None,
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
    }


def _build_comparison(
    *,
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
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
    comparison_size = max(len(legacy_board), len(allocated), 1)
    promotable_rank_only = [
        item for item in opportunities if item.state == "promotable"
    ]
    rank_only_top = (
        promotable_rank_only[:comparison_size] or opportunities[:comparison_size]
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
                    item, allocation_by_opportunity_id.get(item.opportunity_id)
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
                    item, allocation_by_opportunity_id.get(item.opportunity_id)
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
                    item, allocation_by_opportunity_id.get(item.opportunity_id)
                )
                for item in allocated
            ],
        },
        "promoted_from_watchlist": [
            _comparison_item(
                item, allocation_by_opportunity_id.get(item.opportunity_id)
            )
            for item in allocated
            if item.legacy_bucket == "watchlist"
        ],
        "rejected_legacy_board": [
            _comparison_item(
                item, allocation_by_opportunity_id.get(item.opportunity_id)
            )
            for item in legacy_board
            if item.opportunity_id not in allocated_ids
        ],
        "rank_only_rejected_by_allocator": [
            _comparison_item(
                item, allocation_by_opportunity_id.get(item.opportunity_id)
            )
            for item in rank_only_top
            if item.opportunity_id not in allocated_ids
        ],
        "allocator_added_outside_rank_only": [
            _comparison_item(
                item, allocation_by_opportunity_id.get(item.opportunity_id)
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
    analysis_run: Mapping[str, Any] | None,
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
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
        "analysis_verdict": None
        if analysis_run is None
        else ((analysis_run.get("diagnostics") or {}).get("overall_verdict")),
    }

    warnings = [
        "Regime fields are inferred from persisted candidate payloads because collector cycles do not store full regime snapshots yet.",
        "The allocator is provisional and uses deterministic heuristic budgets; it is for offline comparison only.",
    ]
    if analysis_run is not None:
        dimensions = ((analysis_run.get("summary") or {}).get("tuning") or {}).get(
            "dimensions"
        ) or {}
        classification_rows = (
            dimensions.get("classification")
            if isinstance(dimensions, Mapping)
            else None
        )
        if isinstance(classification_rows, list):
            board_row = next(
                (
                    row
                    for row in classification_rows
                    if str(row.get("bucket")) == "board"
                ),
                None,
            )
            watchlist_row = next(
                (
                    row
                    for row in classification_rows
                    if str(row.get("bucket")) == "watchlist"
                ),
                None,
            )
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
                    "Historical tuning for this label favors watchlist ideas over legacy board ideas, so watchlist candidates may surface as promotable."
                )
    return summary, warnings


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
    candidate_rows = [
        dict(row)
        for row in storage.collector.list_cycle_candidates(str(cycle["cycle_id"]))
    ]
    if not candidate_rows:
        raise OpportunityReplayLookupError(
            f"Collector cycle {cycle['cycle_id']} has no stored candidates."
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
        analysis_run=analysis_run,
    )
    allocation_decisions = _build_allocation_decisions(opportunities)
    execution_intents = _build_execution_intents(
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
    )
    comparison = _build_comparison(
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
    )
    summary, warnings = _build_summary(
        cycle=cycle,
        analysis_run=analysis_run,
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
    )

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
        warnings=warnings,
    )
    return replay.to_payload()


def _render_text(payload: Mapping[str, Any]) -> str:
    session = payload.get("session") or {}
    summary = payload.get("summary") or {}
    comparison = payload.get("comparison") or {}
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
    warnings = payload.get("warnings") or []
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
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = build_opportunity_replay(
            db_target=args.db,
            session_id=args.session_id,
            label=args.label,
            session_date=args.date,
        )
    except OpportunityReplayLookupError as exc:
        raise SystemExit(str(exc)) from None

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(_render_text(payload))
    return 0


__all__ = [
    "OpportunityReplayLookupError",
    "build_opportunity_replay",
    "main",
    "parse_args",
]


if __name__ == "__main__":
    raise SystemExit(main())
