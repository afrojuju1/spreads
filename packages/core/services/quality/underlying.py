from __future__ import annotations

from collections.abc import Mapping

from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, unique_text_list

from .models import (
    EntryFilterRef,
    EntryQualityContext,
    FeatureSnapshot,
    FilterResult,
    FilterResultStatus,
)

from core.services.quality.shared import (
    _benchmark_rows,
    _benchmark_values,
    _context_policy_status,
    _market_context_payload,
    _resolved_thresholds,
    _result,
    _setup_metrics,
    _threshold_float,
    _threshold_int,
)

def _setup_context_usable(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    underlying = as_mapping(snapshot.underlying)
    setup = as_mapping(underlying.get("setup"))
    setup_status = str(underlying.get("setup_status") or setup.get("status") or "").strip().lower()
    metrics = {
        "setup_status": setup_status or None,
        "setup_score": underlying.get("setup_score"),
        "daily_bar_count": underlying.get("daily_bar_count"),
        "intraday_bar_count": underlying.get("intraday_bar_count"),
        **as_mapping(underlying.get("setup_metrics")),
    }
    if not setup:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("setup_context_missing",),
            metrics=metrics,
            message="Setup context was not present in candidate diagnostics.",
        )
    if setup_status in {"favorable", "neutral"}:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS,
            reason_codes=(f"setup_{setup_status}",),
            metrics=metrics,
            message=f"Underlying setup was {setup_status}.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.WATCH,
        reason_codes=(f"setup_{setup_status or 'unknown'}",),
        metrics=metrics,
        message="Underlying setup did not block existing behavior but should remain visible.",
    )


def _relative_strength_supportive(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    thresholds = _resolved_thresholds(context, filter_ref, {})
    minimum_benchmark_count = _threshold_int(thresholds, filter_ref, "min_benchmark_count")
    minimum_supportive_count = _threshold_int(thresholds, filter_ref, "min_supportive_benchmark_count")
    minimum_relative_5d = _threshold_float(thresholds, filter_ref, "min_relative_strength_5d_pct")
    minimum_relative_intraday = _threshold_float(thresholds, filter_ref, "min_relative_strength_intraday_pct")
    relative_strength = as_mapping(_setup_metrics(snapshot).get("relative_strength"))
    rows = _benchmark_rows(relative_strength)
    relative_5d = _benchmark_values(rows, "relative_return_5d_pct")
    relative_intraday = _benchmark_values(rows, "relative_intraday_return_pct")
    available_count = len(relative_5d)
    supportive: list[str] = []
    below_5d: list[str] = []
    below_intraday: list[str] = []
    for symbol, value_5d in relative_5d.items():
        intraday_value = relative_intraday.get(symbol)
        passes_5d = minimum_relative_5d is None or value_5d >= minimum_relative_5d
        passes_intraday = minimum_relative_intraday is None or intraday_value is None or intraday_value >= minimum_relative_intraday
        if passes_5d and passes_intraday:
            supportive.append(symbol)
        if not passes_5d:
            below_5d.append(symbol)
        if intraday_value is not None and not passes_intraday:
            below_intraday.append(symbol)
    metrics = {
        "benchmark_symbols": list(relative_strength.get("benchmark_symbols") or rows.keys()),
        "available_benchmark_count": available_count,
        "supportive_benchmark_count": len(supportive),
        "supportive_benchmarks": supportive,
        "relative_return_5d_pct_by_benchmark": relative_5d,
        "relative_intraday_return_pct_by_benchmark": relative_intraday,
        "symbol_return_5d_pct": relative_strength.get("symbol_return_5d_pct"),
        "symbol_intraday_return_pct": relative_strength.get("symbol_intraday_return_pct"),
    }
    resolved_thresholds = {
        "min_benchmark_count": minimum_benchmark_count,
        "min_supportive_benchmark_count": minimum_supportive_count,
        "min_relative_strength_5d_pct": minimum_relative_5d,
        "min_relative_strength_intraday_pct": minimum_relative_intraday,
    }
    if not rows or available_count < (minimum_benchmark_count or 0):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("relative_strength_benchmark_data_missing",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Relative strength versus SPY/QQQ could not be fully proven.",
        )
    if len(supportive) >= (minimum_supportive_count or 1):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS,
            reason_codes=("relative_strength_supportive",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Underlying showed supportive relative strength versus SPY/QQQ.",
        )
    reasons = ["relative_strength_below_benchmarks"]
    if len(below_5d) == available_count:
        reasons.append("relative_strength_5d_below_min")
    if relative_intraday and len(below_intraday) == len(relative_intraday):
        reasons.append("relative_strength_intraday_below_min")
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.BLOCK,
        reason_codes=tuple(reasons),
        metrics=metrics,
        thresholds=resolved_thresholds,
        message="Underlying lagged SPY/QQQ beyond the profile threshold.",
    )


def _market_context_regime_fit(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    thresholds = _resolved_thresholds(context, filter_ref, {})
    minimum_benchmark_count = _threshold_int(thresholds, filter_ref, "min_benchmark_count")
    minimum_supportive_count = _threshold_int(thresholds, filter_ref, "min_supportive_benchmark_count")
    minimum_blocking_count = _threshold_int(thresholds, filter_ref, "min_blocking_benchmark_count")
    minimum_confidence = _threshold_float(thresholds, filter_ref, "min_confidence")
    missing_context_status = _context_policy_status(thresholds.get("missing_context_status"))
    unsupported_context_status = _context_policy_status(thresholds.get("unsupported_context_status"))
    low_confidence_status = _context_policy_status(thresholds.get("low_confidence_status"))
    allowed_regime_labels = set(unique_text_list(thresholds.get("allowed_regime_labels")))
    preferred_regime_labels = set(unique_text_list(thresholds.get("preferred_regime_labels")))
    blocked_regime_labels = set(unique_text_list(thresholds.get("blocked_regime_labels")))
    allowed_risk_postures = set(unique_text_list(thresholds.get("allowed_risk_postures")))
    preferred_risk_postures = set(unique_text_list(thresholds.get("preferred_risk_postures")))
    blocked_risk_postures = set(unique_text_list(thresholds.get("blocked_risk_postures")))
    allowed_trend_strengths = set(unique_text_list(thresholds.get("allowed_trend_strengths")))
    blocked_trend_strengths = set(unique_text_list(thresholds.get("blocked_trend_strengths")))
    allowed_volatility_states = set(unique_text_list(thresholds.get("allowed_volatility_states")))
    blocked_volatility_states = set(unique_text_list(thresholds.get("blocked_volatility_states")))
    market_context = _market_context_payload(context, snapshot)
    regime = as_mapping(market_context.get("regime"))
    regime_metrics = as_mapping(regime.get("metrics"))
    data_quality = as_mapping(market_context.get("data_quality"))
    benchmark_evidence = [as_mapping(row) for row in list(market_context.get("benchmark_evidence") or []) if isinstance(row, Mapping)]
    benchmark_symbols = list(market_context.get("benchmark_symbols") or [str(row.get("symbol") or "").upper() for row in benchmark_evidence])
    observed_count = coerce_int(regime_metrics.get("observed_benchmark_count"))
    if observed_count is None:
        observed_count = len([symbol for symbol in benchmark_symbols if symbol])
    expected_count = coerce_int(regime_metrics.get("expected_benchmark_count"))
    if expected_count is None:
        expected_count = len(benchmark_symbols)
    supportive_symbols = unique_text_list(regime_metrics.get("supportive_benchmarks"))
    blocking_symbols = unique_text_list(regime_metrics.get("blocking_benchmarks"))
    supportive_count = coerce_int(regime_metrics.get("supportive_benchmark_count"))
    if supportive_count is None:
        supportive_count = len(supportive_symbols)
    blocking_count = coerce_int(regime_metrics.get("blocking_benchmark_count"))
    if blocking_count is None:
        blocking_count = len(blocking_symbols)
    regime_reason_codes = unique_text_list(regime.get("reason_codes"))
    regime_label = as_text(regime.get("regime_label"))
    risk_posture = as_text(regime.get("risk_posture"))
    trend_strength = as_text(regime.get("trend_strength"))
    volatility_state = as_text(regime.get("volatility_state"))
    confidence = coerce_float(regime.get("confidence"))
    blocked_policy_reasons: list[str] = []
    if regime_label in blocked_regime_labels:
        blocked_policy_reasons.append("market_context_regime_blocked")
    if risk_posture in blocked_risk_postures:
        blocked_policy_reasons.append("market_context_risk_posture_blocked")
    if trend_strength in blocked_trend_strengths:
        blocked_policy_reasons.append("market_context_trend_strength_blocked")
    if volatility_state in blocked_volatility_states:
        blocked_policy_reasons.append("market_context_volatility_state_blocked")
    unsupported_policy_reasons: list[str] = []
    if allowed_regime_labels and regime_label not in allowed_regime_labels:
        unsupported_policy_reasons.append("market_context_regime_not_allowed")
    if allowed_risk_postures and risk_posture not in allowed_risk_postures:
        unsupported_policy_reasons.append("market_context_risk_posture_not_allowed")
    if allowed_trend_strengths and trend_strength not in allowed_trend_strengths:
        unsupported_policy_reasons.append("market_context_trend_strength_not_allowed")
    if allowed_volatility_states and volatility_state not in allowed_volatility_states:
        unsupported_policy_reasons.append("market_context_volatility_state_not_allowed")
    preferred_policy_reasons: list[str] = []
    if regime_label in preferred_regime_labels:
        preferred_policy_reasons.append("market_context_regime_preferred")
    if risk_posture in preferred_risk_postures:
        preferred_policy_reasons.append("market_context_risk_posture_preferred")
    metrics = {
        "market_context_snapshot_id": market_context.get("snapshot_id"),
        "market_context_scope": market_context.get("scope"),
        "market_context_observed_at": market_context.get("observed_at"),
        "market_context_expires_at": market_context.get("expires_at"),
        "regime_label": regime_label,
        "risk_posture": risk_posture,
        "trend_strength": trend_strength,
        "volatility_state": volatility_state,
        "confidence": confidence,
        "freshness": data_quality.get("freshness"),
        "data_quality_state": data_quality.get("state"),
        "fidelity": list(market_context.get("fidelity") or []),
        "benchmark_symbols": benchmark_symbols,
        "expected_benchmark_count": expected_count,
        "available_benchmark_count": observed_count,
        "supportive_benchmark_count": supportive_count,
        "supportive_benchmarks": supportive_symbols,
        "blocking_benchmark_count": blocking_count,
        "blocking_benchmarks": blocking_symbols,
        "blocked_policy_reasons": blocked_policy_reasons,
        "unsupported_policy_reasons": unsupported_policy_reasons,
        "preferred_policy_reasons": preferred_policy_reasons,
        "regime_reason_codes": regime_reason_codes,
    }
    resolved_thresholds = {
        "min_benchmark_count": minimum_benchmark_count,
        "min_supportive_benchmark_count": minimum_supportive_count,
        "min_blocking_benchmark_count": minimum_blocking_count,
        "min_confidence": minimum_confidence,
        "missing_context_status": missing_context_status.value,
        "unsupported_context_status": unsupported_context_status.value,
        "low_confidence_status": low_confidence_status.value,
        "allowed_regime_labels": sorted(allowed_regime_labels),
        "preferred_regime_labels": sorted(preferred_regime_labels),
        "blocked_regime_labels": sorted(blocked_regime_labels),
        "allowed_risk_postures": sorted(allowed_risk_postures),
        "preferred_risk_postures": sorted(preferred_risk_postures),
        "blocked_risk_postures": sorted(blocked_risk_postures),
        "allowed_trend_strengths": sorted(allowed_trend_strengths),
        "blocked_trend_strengths": sorted(blocked_trend_strengths),
        "allowed_volatility_states": sorted(allowed_volatility_states),
        "blocked_volatility_states": sorted(blocked_volatility_states),
    }
    if not market_context:
        return _result(
            filter_ref=filter_ref,
            status=missing_context_status,
            reason_codes=("market_context_missing",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Shared market context was not available for entry-quality evaluation.",
        )
    if minimum_confidence is not None and confidence is not None and confidence < minimum_confidence:
        return _result(
            filter_ref=filter_ref,
            status=low_confidence_status,
            reason_codes=("market_context_confidence_below_min",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Shared market context confidence was below the strategy policy threshold.",
        )
    if observed_count < (minimum_benchmark_count or 0):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("market_context_benchmark_data_missing",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Shared market context did not have enough benchmark evidence.",
        )
    if blocked_policy_reasons:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=tuple(blocked_policy_reasons),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Shared market context matched a blocked strategy policy state.",
        )
    if unsupported_policy_reasons:
        return _result(
            filter_ref=filter_ref,
            status=unsupported_context_status,
            reason_codes=tuple(unsupported_policy_reasons),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Shared market context was outside the strategy policy's allowed states.",
        )
    if blocking_count >= (minimum_blocking_count or observed_count + 1):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=tuple(regime_reason_codes or ("market_context_broad_drawdown",)),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Shared market context showed broad-market drawdown risk for new long-call entries.",
        )
    if supportive_count >= (minimum_supportive_count or 1):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS,
            reason_codes=tuple(preferred_policy_reasons or ("market_context_regime_supportive",)),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Shared market context was supportive enough for long-call entries.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.WATCH,
        reason_codes=("market_context_regime_not_supportive",),
        metrics=metrics,
        thresholds=resolved_thresholds,
        message="Shared market context was not supportive, but did not hit drawdown blockers.",
    )
