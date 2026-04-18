from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from typing import Any

from core.domain.opportunity_models import (
    HorizonIntent,
    Opportunity,
    OpportunityLeg,
    RegimeSnapshot,
    StrategyIntent,
)
from core.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    candidate_requires_favorable_setup,
)
from core.services.opportunity_execution_plan import (
    execution_complexity,
    rank_opportunities,
)
from core.services.opportunity_scoring import (
    build_candidate_opportunity_score,
    candidate_earnings_phase,
    candidate_event_timing_rule,
    earnings_phase_policy_blockers,
    earnings_phase_policy_preference,
    evaluate_earnings_signal_gate,
)

from .shared import (
    _as_float,
    _as_text,
    _baseline_selection_state_from_row,
    _clamp,
    _direction_from_candidates,
    _event_state,
    _group_value_from_row,
    _horizon_band,
    _intraday_structure,
    _liquidity_state,
    _normalize_score,
    _product_class,
    _strategy_family,
    _style_profile,
    _thesis_direction,
    _vol_level,
)


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
        from .shared import _minutes_between

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
                promotable_count = max(
                    int(_as_float(row.get("promotable_count")) or 0),
                    0,
                )
                monitor_count = max(
                    int(_as_float(row.get("monitor_count")) or 0),
                    0,
                )
                bucket_totals = totals[dimension_key].setdefault(
                    group_value,
                    {
                        "count": 0.0,
                        "promotable_count": 0.0,
                        "monitor_count": 0.0,
                        "estimated_pnl_total": 0.0,
                    },
                )
                bucket_totals["count"] += float(count)
                bucket_totals["promotable_count"] += float(promotable_count)
                bucket_totals["monitor_count"] += float(monitor_count)
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
                "promotable_count": int(totals_row["promotable_count"]),
                "monitor_count": int(totals_row["monitor_count"]),
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
        "promotable_count": row.get("promotable_count"),
        "monitor_count": row.get("monitor_count"),
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
            baseline_selection_state=_baseline_selection_state_from_row(row),
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
            baseline_selection_state=_baseline_selection_state_from_row(row),
            evidence={
                "baseline_selection_state": _baseline_selection_state_from_row(row),
                "baseline_position": row.get("position"),
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
