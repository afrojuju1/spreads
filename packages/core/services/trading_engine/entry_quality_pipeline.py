from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from core.services.option_structures import normalize_strategy_family
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, unique_text_list

from .entry_quality import (
    EntryFilterRef,
    EntryQualityContext,
    EntryQualityProfile,
    EntryQualityStageName,
    EntryQualityWaterfall,
    FeatureSnapshot,
    FilterResult,
    FilterResultStatus,
    MOMENTUM_LONG_CALL_PROFILE_ID,
    resolve_entry_quality_profile,
)

_RAW_REJECTION_STAGE = {
    "no_expected_move": EntryQualityStageName.CONTRACT_FIT,
    "no_snapshot": EntryQualityStageName.CHAIN_VIABILITY,
    "no_delta": EntryQualityStageName.CHAIN_VIABILITY,
    "itm_call_skipped": EntryQualityStageName.CONTRACT_FIT,
    "itm_put_skipped": EntryQualityStageName.CONTRACT_FIT,
    "open_interest_below_min": EntryQualityStageName.PREMIUM_QUALITY,
    "bid_or_ask_size_zero": EntryQualityStageName.PREMIUM_QUALITY,
    "relative_spread_above_max": EntryQualityStageName.PREMIUM_QUALITY,
    "delta_outside_range": EntryQualityStageName.CONTRACT_FIT,
    "premium_too_low_or_no_natural": EntryQualityStageName.PREMIUM_QUALITY,
    "expected_move_profit_not_positive": EntryQualityStageName.CONTRACT_FIT,
    "return_on_risk_below_min": EntryQualityStageName.PREMIUM_QUALITY,
}

_CHAIN_USABILITY_REASON_MAP = {
    "no_expected_move": "target_dte_expected_move_missing",
    "no_snapshot": "target_dte_snapshots_unusable",
    "no_delta": "target_dte_greeks_unusable",
    "bid_or_ask_size_zero": "target_dte_quote_size_unusable",
    "open_interest_below_min": "target_dte_open_interest_unusable",
    "relative_spread_above_max": "target_dte_relative_spread_unusable",
}

_THRESHOLD_ALIASES = {
    "dte_min": "min_dte",
    "dte_max": "max_dte",
    "short_delta_min": "delta_min",
    "short_delta_max": "delta_max",
    "min_delta": "delta_min",
    "max_delta": "delta_max",
    "max_leg_spread_pct_mid": "max_relative_spread",
}


def _candidate(snapshot: FeatureSnapshot, candidate: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if candidate is not None:
        return dict(candidate)
    if isinstance(snapshot.candidate, Mapping):
        return dict(snapshot.candidate)
    return {}


def _candidate_contract(snapshot: FeatureSnapshot) -> dict[str, Any]:
    return as_mapping(as_mapping(snapshot.chain).get("candidate_contract"))


def _candidate_economics(snapshot: FeatureSnapshot) -> dict[str, Any]:
    return as_mapping(as_mapping(snapshot.premium).get("candidate_economics"))


def _chain_filters(snapshot: FeatureSnapshot) -> dict[str, Any]:
    return as_mapping(as_mapping(snapshot.chain).get("filters"))


def _threshold_key(key: Any) -> str:
    rendered = str(key or "").strip()
    return _THRESHOLD_ALIASES.get(rendered, rendered)


def _usable_threshold(value: Any) -> bool:
    return value not in (None, "", [], {})


def _flat_thresholds(values: Mapping[str, Any]) -> dict[str, Any]:
    return {_threshold_key(key): value for key, value in values.items() if _usable_threshold(value) and not isinstance(value, Mapping)}


def _policy_thresholds(context: EntryQualityContext, filter_ref: EntryFilterRef) -> dict[str, Any]:
    policy = as_mapping(context.policy)
    stage_policy = as_mapping(policy.get(filter_ref.stage.value))
    thresholds: dict[str, Any] = {}
    thresholds.update(_flat_thresholds(as_mapping(filter_ref.thresholds)))
    thresholds.update(_flat_thresholds(as_mapping(policy.get("thresholds"))))
    thresholds.update(_flat_thresholds(stage_policy))
    thresholds.update(_flat_thresholds(as_mapping(stage_policy.get(filter_ref.filter_id))))
    thresholds.update(_flat_thresholds(as_mapping(policy.get(filter_ref.filter_id))))
    return thresholds


def _resolved_thresholds(
    context: EntryQualityContext,
    filter_ref: EntryFilterRef,
    base: Mapping[str, Any],
) -> dict[str, Any]:
    thresholds = {_threshold_key(key): value for key, value in base.items() if _usable_threshold(value)}
    thresholds.update(_policy_thresholds(context, filter_ref))
    return thresholds


def _status(status: FilterResultStatus | str) -> FilterResultStatus:
    return status if isinstance(status, FilterResultStatus) else FilterResultStatus(status)


def _result(
    *,
    filter_ref: EntryFilterRef,
    status: FilterResultStatus | str,
    reason_codes: Sequence[Any] = (),
    metrics: Mapping[str, Any] | None = None,
    thresholds: Mapping[str, Any] | None = None,
    message: str = "",
) -> FilterResult:
    return FilterResult(
        filter_id=filter_ref.filter_id,
        stage=filter_ref.stage,
        status=_status(status),
        reason_codes=tuple(str(code) for code in reason_codes if as_text(code) is not None),
        metrics=dict(metrics or {}),
        thresholds=dict(thresholds or {}),
        message=message,
    )


def _first_reason(*values: Any, default: str) -> tuple[str, ...]:
    reasons: list[str] = []
    for value in values:
        reasons.extend(unique_text_list(value, accept_scalar=True))
    return tuple(reasons or (default,))


def _count_reasons(reasons: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(reason) for reason, count in sorted(reasons.items()) if coerce_int(count) not in (None, 0))


def _rejection_counts(snapshot: FeatureSnapshot) -> dict[str, Any]:
    return as_mapping(as_mapping(snapshot.chain).get("rejection_counts"))


def _raw_rejection_counts(snapshot: FeatureSnapshot) -> dict[str, Any]:
    return as_mapping(_rejection_counts(snapshot).get("raw"))


def _chain_examples(snapshot: FeatureSnapshot) -> dict[str, Any]:
    return as_mapping(as_mapping(snapshot.chain).get("examples"))


def _diagnostic_evidence(snapshot: FeatureSnapshot) -> dict[str, Any]:
    return as_mapping(snapshot.metadata.get("diagnostic_evidence"))


def _stage_rejection_reasons(snapshot: FeatureSnapshot, stage: EntryQualityStageName) -> tuple[str, ...]:
    counts = _rejection_counts(snapshot)
    reasons: list[str] = []
    raw = as_mapping(counts.get("raw"))
    ranking_policy = as_mapping(counts.get("ranking_policy"))
    for reason in _count_reasons(raw):
        if _RAW_REJECTION_STAGE.get(reason) == stage:
            reasons.append(reason)
    if stage == EntryQualityStageName.PREMIUM_QUALITY:
        reasons.extend(_count_reasons(ranking_policy))
    return tuple(dict.fromkeys(reasons))


def _ratio(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return round(max(float(numerator), 0.0) / max(float(denominator), 1.0), 4)


def _candidate_min_quote_size(candidate: Mapping[str, Any]) -> int | None:
    values = [
        coerce_int(candidate.get("min_quote_size")),
        coerce_int(candidate.get("short_bid_size")),
        coerce_int(candidate.get("short_ask_size")),
        coerce_int(candidate.get("long_bid_size")),
        coerce_int(candidate.get("long_ask_size")),
        coerce_int(candidate.get("bid_size")),
        coerce_int(candidate.get("ask_size")),
    ]
    resolved = [value for value in values if value is not None]
    return min(resolved) if resolved else None


def _candidate_bid_ask_spread(candidate: Mapping[str, Any], contract: Mapping[str, Any]) -> float | None:
    spread = coerce_float(
        candidate.get("bid_ask_spread") or candidate.get("spread_width") or contract.get("bid_ask_spread") or contract.get("spread_width")
    )
    if spread is not None:
        return spread
    bid = coerce_float(candidate.get("short_bid") or candidate.get("long_bid") or candidate.get("bid") or contract.get("bid_price"))
    ask = coerce_float(candidate.get("short_ask") or candidate.get("long_ask") or candidate.get("ask") or contract.get("ask_price"))
    if bid is None or ask is None:
        return None
    return round(max(ask - bid, 0.0), 4)


def _candidate_option_volume(candidate: Mapping[str, Any], contract: Mapping[str, Any]) -> int | None:
    values = [
        coerce_int(candidate.get("short_volume")),
        coerce_int(candidate.get("long_volume")),
        coerce_int(candidate.get("option_volume")),
        coerce_int(candidate.get("volume")),
        coerce_int(contract.get("option_volume")),
        coerce_int(contract.get("volume")),
    ]
    resolved = [value for value in values if value is not None]
    return min(resolved) if resolved else None


def _setup_metrics(snapshot: FeatureSnapshot) -> dict[str, Any]:
    return as_mapping(as_mapping(snapshot.underlying).get("setup_metrics"))


def _threshold_int(thresholds: Mapping[str, Any], filter_ref: EntryFilterRef, key: str) -> int | None:
    value = coerce_int(thresholds.get(key))
    if value is not None:
        return value
    return coerce_int(as_mapping(filter_ref.thresholds).get(key))


def _threshold_float(thresholds: Mapping[str, Any], filter_ref: EntryFilterRef, key: str) -> float | None:
    value = coerce_float(thresholds.get(key))
    if value is not None:
        return value
    return coerce_float(as_mapping(filter_ref.thresholds).get(key))


def _benchmark_rows(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(symbol).upper(): as_mapping(row) for symbol, row in as_mapping(payload.get("by_benchmark")).items()}


def _benchmark_values(rows: Mapping[str, Mapping[str, Any]], field: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for symbol, row in rows.items():
        value = coerce_float(row.get(field))
        if value is not None:
            values[symbol] = value
    return values


def _source_is_fresh(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    source = as_mapping(snapshot.source)
    thresholds = _resolved_thresholds(
        context,
        filter_ref,
        {"max_age_seconds": source.get("max_age_seconds")},
    )
    blockers = unique_text_list(source.get("blockers"))
    metrics = {
        "ticker_source_kind": source.get("ticker_source_kind"),
        "ticker_source_id": source.get("ticker_source_id"),
        "ticker_source_run_id": source.get("ticker_source_run_id"),
        "resolved_at": source.get("resolved_at"),
        "max_age_seconds": source.get("max_age_seconds"),
        "status": source.get("status"),
    }
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            thresholds=thresholds,
            message="Ticker source was not usable for entry.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=_first_reason(source.get("reason_codes"), default="ticker_source_usable"),
        metrics=metrics,
        thresholds=thresholds,
        message="Ticker source was usable.",
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


def _market_regime_supportive(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    thresholds = _resolved_thresholds(context, filter_ref, {})
    minimum_benchmark_count = _threshold_int(thresholds, filter_ref, "min_benchmark_count")
    minimum_supportive_count = _threshold_int(thresholds, filter_ref, "min_supportive_benchmark_count")
    minimum_benchmark_5d = _threshold_float(thresholds, filter_ref, "min_benchmark_5d_return_pct")
    minimum_benchmark_intraday = _threshold_float(thresholds, filter_ref, "min_benchmark_intraday_return_pct")
    maximum_5d_drawdown = _threshold_float(thresholds, filter_ref, "max_benchmark_5d_drawdown_pct")
    maximum_intraday_drawdown = _threshold_float(thresholds, filter_ref, "max_benchmark_intraday_drawdown_pct")
    minimum_blocking_count = _threshold_int(thresholds, filter_ref, "min_blocking_benchmark_count")
    market_regime = as_mapping(_setup_metrics(snapshot).get("market_regime"))
    rows = _benchmark_rows(market_regime)
    returns_5d = _benchmark_values(rows, "return_5d_pct")
    intraday_returns = _benchmark_values(rows, "intraday_return_pct")
    available_count = len(returns_5d)
    supportive: list[str] = []
    drawdown_5d: list[str] = []
    drawdown_intraday: list[str] = []
    for symbol, value_5d in returns_5d.items():
        intraday_value = intraday_returns.get(symbol)
        if maximum_5d_drawdown is not None and value_5d <= maximum_5d_drawdown:
            drawdown_5d.append(symbol)
        if maximum_intraday_drawdown is not None and intraday_value is not None and intraday_value <= maximum_intraday_drawdown:
            drawdown_intraday.append(symbol)
        passes_5d = minimum_benchmark_5d is None or value_5d >= minimum_benchmark_5d
        passes_intraday = minimum_benchmark_intraday is None or intraday_value is None or intraday_value >= minimum_benchmark_intraday
        if passes_5d and passes_intraday:
            supportive.append(symbol)
    blocking_symbols = tuple(dict.fromkeys((*drawdown_5d, *drawdown_intraday)))
    metrics = {
        "benchmark_symbols": list(market_regime.get("benchmark_symbols") or rows.keys()),
        "available_benchmark_count": available_count,
        "supportive_benchmark_count": len(supportive),
        "supportive_benchmarks": supportive,
        "blocking_benchmark_count": len(blocking_symbols),
        "blocking_benchmarks": list(blocking_symbols),
        "benchmark_return_5d_pct": returns_5d,
        "benchmark_intraday_return_pct": intraday_returns,
    }
    resolved_thresholds = {
        "min_benchmark_count": minimum_benchmark_count,
        "min_supportive_benchmark_count": minimum_supportive_count,
        "min_benchmark_5d_return_pct": minimum_benchmark_5d,
        "min_benchmark_intraday_return_pct": minimum_benchmark_intraday,
        "max_benchmark_5d_drawdown_pct": maximum_5d_drawdown,
        "max_benchmark_intraday_drawdown_pct": maximum_intraday_drawdown,
        "min_blocking_benchmark_count": minimum_blocking_count,
    }
    if not rows or available_count < (minimum_benchmark_count or 0):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("market_regime_benchmark_data_missing",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Broad-market regime could not be fully proven from SPY/QQQ.",
        )
    if len(blocking_symbols) >= (minimum_blocking_count or available_count + 1):
        reasons = ["market_regime_broad_drawdown"]
        if drawdown_5d:
            reasons.append("market_regime_5d_drawdown")
        if drawdown_intraday:
            reasons.append("market_regime_intraday_drawdown")
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=tuple(reasons),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="SPY/QQQ regime was too weak for new long-call entries.",
        )
    if len(supportive) >= (minimum_supportive_count or 1):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS,
            reason_codes=("market_regime_supportive",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="SPY/QQQ regime was supportive enough for long-call entries.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.WATCH,
        reason_codes=("market_regime_not_supportive",),
        metrics=metrics,
        thresholds=resolved_thresholds,
        message="SPY/QQQ regime was not supportive, but did not hit drawdown blockers.",
    )


def _chain_data_available(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    chain = as_mapping(snapshot.chain)
    metrics = {
        "expiration_count": chain.get("expiration_count"),
        "contract_count": chain.get("contract_count"),
        "expected_move_count": chain.get("expected_move_count"),
    }
    if not bool(chain.get("has_contracts")):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=_first_reason(_stage_rejection_reasons(snapshot, EntryQualityStageName.CHAIN_VIABILITY), default="contract_count_zero"),
            metrics=metrics,
            message="No usable option contracts were available.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("chain_contracts_available",),
        metrics=metrics,
        message="Option chain contracts were available.",
    )


def _option_snapshots_available(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    chain = as_mapping(snapshot.chain)
    metrics = {
        "snapshot_count": chain.get("snapshot_count"),
        "contract_count": chain.get("contract_count"),
    }
    if not bool(chain.get("has_snapshots")):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=_first_reason(_stage_rejection_reasons(snapshot, EntryQualityStageName.CHAIN_VIABILITY), default="no_snapshot"),
            metrics=metrics,
            message="Option snapshots were not available.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("option_snapshots_available",),
        metrics=metrics,
        message="Option snapshots were available.",
    )


def _greeks_available(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    chain = as_mapping(snapshot.chain)
    metrics = {
        "delta_snapshot_count": chain.get("delta_snapshot_count"),
        "greeks_available": bool(chain.get("greeks_available")),
    }
    if not bool(chain.get("greeks_available")):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=_first_reason(_stage_rejection_reasons(snapshot, EntryQualityStageName.CHAIN_VIABILITY), default="no_delta"),
            metrics=metrics,
            message="Required option Greeks were not available.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("greeks_available",),
        metrics=metrics,
        message="Required option Greeks were available.",
    )


def _target_dte_chain_usable(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    chain = as_mapping(snapshot.chain)
    filters = _chain_filters(snapshot)
    diagnostic_evidence = _diagnostic_evidence(snapshot)
    raw_counts = _raw_rejection_counts(snapshot)
    candidate = _candidate(snapshot)
    contract = _candidate_contract(snapshot)
    thresholds = _resolved_thresholds(
        context,
        filter_ref,
        {
            "min_open_interest": filters.get("min_open_interest"),
            "max_relative_spread": filters.get("max_relative_spread") or filters.get("max_leg_spread_pct_mid"),
            "max_quote_age_seconds": filters.get("max_quote_age_seconds"),
            "min_option_volume": filters.get("min_option_volume"),
            "max_bid_ask_spread": filters.get("max_bid_ask_spread") or filters.get("max_absolute_spread"),
        },
    )
    minimum_expirations = coerce_int(thresholds.get("min_target_dte_expirations"))
    minimum_contracts = coerce_int(thresholds.get("min_target_dte_contracts"))
    minimum_snapshots = coerce_int(thresholds.get("min_chain_snapshot_count"))
    minimum_delta_snapshots = coerce_int(thresholds.get("min_delta_snapshot_count"))
    minimum_snapshot_ratio = coerce_float(thresholds.get("min_snapshot_coverage_ratio"))
    minimum_delta_ratio = coerce_float(thresholds.get("min_delta_coverage_ratio"))
    minimum_viable_contracts = coerce_int(thresholds.get("min_viable_contracts"))
    minimum_open_interest = coerce_int(thresholds.get("min_open_interest"))
    minimum_bid_ask_size = coerce_int(thresholds.get("min_bid_ask_size"))
    minimum_option_volume = coerce_int(thresholds.get("min_option_volume"))
    maximum_relative_spread = coerce_float(thresholds.get("max_relative_spread"))
    maximum_bid_ask_spread = coerce_float(thresholds.get("max_bid_ask_spread"))
    maximum_quote_age_seconds = coerce_int(thresholds.get("max_quote_age_seconds"))
    expiration_count = coerce_int(chain.get("expiration_count")) or 0
    contract_count = coerce_int(chain.get("contract_count")) or 0
    snapshot_count = coerce_int(chain.get("snapshot_count")) or 0
    delta_snapshot_count = coerce_int(chain.get("delta_snapshot_count")) or 0
    expected_move_count = coerce_int(chain.get("expected_move_count")) or 0
    examples = _chain_examples(snapshot)
    raw_passes = list(examples.get("raw_passes") or [])
    raw_pass_count = coerce_int(diagnostic_evidence.get("raw_pass_count"))
    if raw_pass_count is None and raw_passes:
        raw_pass_count = len(raw_passes)
    snapshot_coverage_ratio = _ratio(snapshot_count, contract_count)
    delta_coverage_ratio = _ratio(delta_snapshot_count, snapshot_count)
    candidate_relative_spread = coerce_float(
        candidate.get("relative_spread")
        or candidate.get("short_relative_spread")
        or candidate.get("long_relative_spread")
        or contract.get("relative_spread")
    )
    candidate_open_interest = coerce_int(
        candidate.get("open_interest") or candidate.get("short_open_interest") or candidate.get("long_open_interest") or contract.get("open_interest")
    )
    candidate_bid_ask_spread = _candidate_bid_ask_spread(candidate, contract)
    candidate_option_volume = _candidate_option_volume(candidate, contract)
    candidate_min_quote_size = _candidate_min_quote_size(candidate)
    chain_rejection_counts = {
        reason: count
        for reason, value in raw_counts.items()
        if reason in _CHAIN_USABILITY_REASON_MAP and (count := coerce_int(value)) not in (None, 0)
    }
    metrics = {
        "expiration_count": expiration_count,
        "contract_count": contract_count,
        "snapshot_count": snapshot_count,
        "delta_snapshot_count": delta_snapshot_count,
        "expected_move_count": expected_move_count,
        "snapshot_coverage_ratio": snapshot_coverage_ratio,
        "delta_coverage_ratio": delta_coverage_ratio,
        "raw_pass_count": raw_pass_count,
        "candidate_count_for_symbol": coerce_int(snapshot.metadata.get("candidate_count_for_symbol")),
        "candidate_open_interest": candidate_open_interest,
        "candidate_relative_spread": candidate_relative_spread,
        "candidate_bid_ask_spread": candidate_bid_ask_spread,
        "candidate_option_volume": candidate_option_volume,
        "candidate_min_quote_size": candidate_min_quote_size,
        "raw_chain_rejection_counts": chain_rejection_counts,
    }
    resolved_thresholds = {
        "min_target_dte_expirations": minimum_expirations,
        "min_target_dte_contracts": minimum_contracts,
        "min_chain_snapshot_count": minimum_snapshots,
        "min_delta_snapshot_count": minimum_delta_snapshots,
        "min_snapshot_coverage_ratio": minimum_snapshot_ratio,
        "min_delta_coverage_ratio": minimum_delta_ratio,
        "min_viable_contracts": minimum_viable_contracts,
        "min_open_interest": minimum_open_interest,
        "min_bid_ask_size": minimum_bid_ask_size,
        "min_option_volume": minimum_option_volume,
        "max_relative_spread": maximum_relative_spread,
        "max_bid_ask_spread": maximum_bid_ask_spread,
        "max_quote_age_seconds": maximum_quote_age_seconds,
    }
    blockers: list[str] = []
    if minimum_expirations is not None and expiration_count < minimum_expirations:
        blockers.append("target_dte_expiration_count_below_min")
    if minimum_contracts is not None and contract_count < minimum_contracts:
        blockers.append("target_dte_contract_count_below_min")
    if minimum_snapshots is not None and snapshot_count < minimum_snapshots:
        blockers.append("target_dte_snapshot_count_below_min")
    if minimum_delta_snapshots is not None and delta_snapshot_count < minimum_delta_snapshots:
        blockers.append("target_dte_delta_snapshot_count_below_min")
    if expected_move_count <= 0:
        blockers.append("target_dte_expected_move_missing")
    if minimum_snapshot_ratio is not None and snapshot_coverage_ratio is not None and snapshot_coverage_ratio < minimum_snapshot_ratio:
        blockers.append("target_dte_snapshot_coverage_below_min")
    if minimum_delta_ratio is not None and delta_coverage_ratio is not None and delta_coverage_ratio < minimum_delta_ratio:
        blockers.append("target_dte_delta_coverage_below_min")
    if minimum_viable_contracts is not None and raw_pass_count is not None and raw_pass_count < minimum_viable_contracts:
        blockers.append("target_dte_chain_no_viable_contracts")
        for reason, _count in sorted(chain_rejection_counts.items(), key=lambda item: (-int(item[1]), item[0])):
            mapped = _CHAIN_USABILITY_REASON_MAP[reason]
            if mapped not in blockers:
                blockers.append(mapped)
    if candidate:
        if minimum_open_interest is not None and (candidate_open_interest is None or candidate_open_interest < minimum_open_interest):
            blockers.append("target_dte_candidate_open_interest_below_min")
        if minimum_bid_ask_size is not None and (candidate_min_quote_size is None or candidate_min_quote_size < minimum_bid_ask_size):
            blockers.append("target_dte_candidate_quote_size_below_min")
        if minimum_option_volume is not None and (candidate_option_volume is None or candidate_option_volume < minimum_option_volume):
            blockers.append("target_dte_candidate_option_volume_below_min")
        if maximum_relative_spread is not None and candidate_relative_spread is not None and candidate_relative_spread > maximum_relative_spread:
            blockers.append("target_dte_candidate_relative_spread_above_max")
        if maximum_bid_ask_spread is not None and candidate_bid_ask_spread is not None and candidate_bid_ask_spread > maximum_bid_ask_spread:
            blockers.append("target_dte_candidate_bid_ask_spread_above_max")
    blockers = list(dict.fromkeys(blockers))
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Target-DTE option chain was not mechanically usable for momentum long calls.",
        )
    if raw_pass_count is None:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("target_dte_chain_viability_unknown",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Target-DTE chain viability could not be proven from diagnostics.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("target_dte_chain_usable",),
        metrics=metrics,
        thresholds=resolved_thresholds,
        message="Target-DTE option chain had usable coverage and at least one viable contract.",
    )


def _strategy_family_matches(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    strategy = as_text(candidate.get("strategy") or context.trade_structure)
    metrics = {
        "candidate_strategy": strategy,
        "trade_structure": context.trade_structure,
    }
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            message="No candidate was attached for contract-fit evaluation.",
        )
    if normalize_strategy_family(strategy) != normalize_strategy_family(context.trade_structure):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("strategy_family_mismatch",),
            metrics=metrics,
            message="Candidate strategy family did not match the entry routine.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("strategy_family_matched",),
        metrics=metrics,
        message="Candidate strategy family matched the entry routine.",
    )


def _dte_in_range(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    contract = _candidate_contract(snapshot)
    filters = _chain_filters(snapshot)
    dte = coerce_int(candidate.get("days_to_expiration") or candidate.get("dte") or contract.get("days_to_expiration") or contract.get("dte"))
    thresholds = _resolved_thresholds(
        context,
        filter_ref,
        {
            "min_dte": filters.get("min_dte") or filters.get("dte_min"),
            "max_dte": filters.get("max_dte") or filters.get("dte_max"),
        },
    )
    minimum = coerce_int(thresholds.get("min_dte"))
    maximum = coerce_int(thresholds.get("max_dte"))
    metrics = {"days_to_expiration": dte}
    thresholds = {"min_dte": minimum, "max_dte": maximum}
    if not candidate:
        reasons = _stage_rejection_reasons(snapshot, EntryQualityStageName.CONTRACT_FIT)
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK if reasons else FilterResultStatus.WATCH,
            reason_codes=reasons or ("candidate_missing",),
            metrics=metrics,
            thresholds=thresholds,
            message="No candidate was attached for DTE evaluation.",
        )
    if dte is None:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("dte_missing",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate did not expose DTE.",
        )
    if minimum is not None and dte < minimum:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("dte_below_min",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate DTE was below the configured range.",
        )
    if maximum is not None and dte > maximum:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("dte_above_max",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate DTE was above the configured range.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("dte_in_range",),
        metrics=metrics,
        thresholds=thresholds,
        message="Candidate DTE was in range.",
    )


def _delta_in_range(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    contract = _candidate_contract(snapshot)
    filters = _chain_filters(snapshot)
    delta = coerce_float(candidate.get("short_delta") or candidate.get("delta") or contract.get("delta"))
    if delta is not None:
        delta = abs(delta)
    thresholds = _resolved_thresholds(
        context,
        filter_ref,
        {
            "delta_min": filters.get("delta_min") or filters.get("short_delta_min"),
            "delta_max": filters.get("delta_max") or filters.get("short_delta_max"),
        },
    )
    minimum = coerce_float(thresholds.get("delta_min"))
    maximum = coerce_float(thresholds.get("delta_max"))
    metrics = {"delta": delta}
    thresholds = {"delta_min": minimum, "delta_max": maximum}
    if not candidate:
        reasons = _stage_rejection_reasons(snapshot, EntryQualityStageName.CONTRACT_FIT)
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK if reasons else FilterResultStatus.WATCH,
            reason_codes=reasons or ("candidate_missing",),
            metrics=metrics,
            thresholds=thresholds,
            message="No candidate was attached for delta evaluation.",
        )
    if delta is None:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("delta_missing",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate did not expose delta.",
        )
    if minimum is not None and delta < minimum:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("short_delta_below_min",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate delta was below the configured range.",
        )
    if maximum is not None and delta > maximum:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("short_delta_above_max",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate delta was above the configured range.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("delta_in_range",),
        metrics=metrics,
        thresholds=thresholds,
        message="Candidate delta was in range.",
    )


def _entry_recipe_passed(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    candidate = _candidate(snapshot)
    reasons = tuple(
        reason
        for reason in _stage_rejection_reasons(snapshot, EntryQualityStageName.CONTRACT_FIT)
        if reason.startswith("entry_recipe") or "recipe" in reason
    )
    if reasons:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=reasons,
            message="Entry recipe rejected the candidate.",
        )
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            message="No candidate was attached for entry recipe evaluation.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("entry_recipe_passed",),
        metrics={"runtime_recipe_refs": list(candidate.get("runtime_recipe_refs") or [])},
        message="Entry recipe checks passed or were not configured.",
    )


def _open_interest_ok(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    contract = _candidate_contract(snapshot)
    filters = _chain_filters(snapshot)
    thresholds = _resolved_thresholds(
        context,
        filter_ref,
        {"min_open_interest": filters.get("min_open_interest")},
    )
    minimum = coerce_int(thresholds.get("min_open_interest"))
    open_interest = coerce_int(
        candidate.get("open_interest") or candidate.get("short_open_interest") or candidate.get("long_open_interest") or contract.get("open_interest")
    )
    metrics = {"open_interest": open_interest}
    thresholds = {"min_open_interest": minimum}
    reasons = tuple(
        reason
        for reason in _stage_rejection_reasons(snapshot, EntryQualityStageName.PREMIUM_QUALITY)
        if reason in {"open_interest_below_min", "open_interest_below_floor"}
    )
    if not candidate and reasons:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=reasons,
            metrics=metrics,
            thresholds=thresholds,
            message="Open interest blocked all candidates.",
        )
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            thresholds=thresholds,
            message="No candidate was attached for open-interest evaluation.",
        )
    if minimum is not None and (open_interest is None or open_interest < minimum):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("open_interest_below_min",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate open interest was below the configured floor.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS if open_interest is not None or minimum is None else FilterResultStatus.WATCH,
        reason_codes=("open_interest_ok",) if open_interest is not None or minimum is None else ("open_interest_missing",),
        metrics=metrics,
        thresholds=thresholds,
        message="Candidate open interest was usable.",
    )


def _relative_spread_ok(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    contract = _candidate_contract(snapshot)
    filters = _chain_filters(snapshot)
    thresholds = _resolved_thresholds(
        context,
        filter_ref,
        {"max_relative_spread": filters.get("max_relative_spread") or filters.get("max_leg_spread_pct_mid")},
    )
    maximum = coerce_float(thresholds.get("max_relative_spread"))
    relative_spread = coerce_float(
        candidate.get("relative_spread")
        or candidate.get("short_relative_spread")
        or candidate.get("long_relative_spread")
        or contract.get("relative_spread")
    )
    metrics = {"relative_spread": relative_spread}
    thresholds = {"max_relative_spread": maximum}
    reasons = tuple(
        reason
        for reason in _stage_rejection_reasons(snapshot, EntryQualityStageName.PREMIUM_QUALITY)
        if reason in {"relative_spread_above_max", "relative_spread_above_ceiling"}
    )
    if not candidate and reasons:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=reasons,
            metrics=metrics,
            thresholds=thresholds,
            message="Relative spread blocked all candidates.",
        )
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            thresholds=thresholds,
            message="No candidate was attached for relative-spread evaluation.",
        )
    if maximum is not None and relative_spread is not None and relative_spread > maximum:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("relative_spread_above_max",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate relative spread was above the configured ceiling.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS if relative_spread is not None or maximum is None else FilterResultStatus.WATCH,
        reason_codes=("relative_spread_ok",) if relative_spread is not None or maximum is None else ("relative_spread_missing",),
        metrics=metrics,
        thresholds=thresholds,
        message="Candidate relative spread was usable.",
    )


def _return_on_risk_ok(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    economics = _candidate_economics(snapshot)
    filters = _chain_filters(snapshot)
    thresholds = _resolved_thresholds(
        context,
        filter_ref,
        {"min_return_on_risk": filters.get("min_return_on_risk")},
    )
    minimum = coerce_float(thresholds.get("min_return_on_risk"))
    return_on_risk = coerce_float(candidate.get("return_on_risk") or economics.get("return_on_risk"))
    metrics = {"return_on_risk": return_on_risk}
    thresholds = {"min_return_on_risk": minimum}
    reasons = tuple(
        reason
        for reason in _stage_rejection_reasons(snapshot, EntryQualityStageName.PREMIUM_QUALITY)
        if reason in {"return_on_risk_below_min", "return_on_risk_below_floor"}
    )
    if not candidate and reasons:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=reasons,
            metrics=metrics,
            thresholds=thresholds,
            message="Return on risk blocked all candidates.",
        )
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            thresholds=thresholds,
            message="No candidate was attached for return-on-risk evaluation.",
        )
    if minimum is not None and (return_on_risk is None or return_on_risk < minimum):
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("return_on_risk_below_min",),
            metrics=metrics,
            thresholds=thresholds,
            message="Candidate return on risk was below the configured floor.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS if return_on_risk is not None or minimum is None else FilterResultStatus.WATCH,
        reason_codes=("return_on_risk_ok",) if return_on_risk is not None or minimum is None else ("return_on_risk_missing",),
        metrics=metrics,
        thresholds=thresholds,
        message="Candidate return on risk was usable.",
    )


def _ranking_policy_passed(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    premium = as_mapping(snapshot.premium)
    ranking_gate = as_mapping(premium.get("ranking_gate"))
    blocker_counts = as_mapping(ranking_gate.get("blocker_counts"))
    candidate_blockers = unique_text_list(premium.get("ranking_policy_blockers"))
    status = str(premium.get("ranking_policy_status") or "").strip().lower()
    if candidate_blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=candidate_blockers,
            metrics={"ranking_policy_status": status},
            message="Ranking policy rejected the candidate.",
        )
    if blocker_counts and snapshot.candidate is None:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=_count_reasons(blocker_counts),
            metrics={"blocker_counts": dict(blocker_counts)},
            message="Ranking policy blocked all candidates for this symbol.",
        )
    if snapshot.candidate is None:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics={"ranking_policy_status": status or None},
            message="No candidate was attached for ranking policy evaluation.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("ranking_policy_passed",),
        metrics={"ranking_policy_status": status or None},
        message="Ranking policy passed.",
    )


def _selection_score_ok(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    candidate = _candidate(snapshot)
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            message="No candidate was attached for selection evaluation.",
        )
    scoring_state = str(candidate.get("scoring_state") or "").strip().lower()
    selection_state = str(candidate.get("selection_state") or "").strip().lower()
    metrics = {
        "selection_state": selection_state or None,
        "scoring_state": scoring_state or None,
        "promotion_score": coerce_float(candidate.get("promotion_score")),
        "execution_score": coerce_float(candidate.get("execution_score")),
        "confidence": coerce_float(candidate.get("confidence")),
    }
    if scoring_state == "blocked":
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=_first_reason(candidate.get("scoring_blockers"), default="scoring_blocked"),
            metrics=metrics,
            message="Candidate scoring blocked selection.",
        )
    if selection_state in {"promotable", "monitor"}:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS if selection_state == "promotable" else FilterResultStatus.WATCH,
            reason_codes=(f"selected_{selection_state}",),
            metrics=metrics,
            message="Candidate was selected for live or monitor output.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.WATCH,
        reason_codes=("selection_not_evaluated",),
        metrics=metrics,
        message="Selection scoring has not been evaluated on this snapshot yet.",
    )


def _selection_live_ready(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    candidate = _candidate(snapshot)
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            message="No candidate was attached for live-readiness evaluation.",
        )
    eligibility = str(candidate.get("eligibility") or candidate.get("eligibility_state") or "live").strip().lower()
    selection_state = str(candidate.get("selection_state") or "").strip().lower()
    blockers = []
    for field in ("blockers", "execution_blockers", "scoring_blockers"):
        blockers.extend(unique_text_list(candidate.get(field)))
    blockers = list(dict.fromkeys(blockers))
    metrics = {
        "eligibility": eligibility,
        "selection_state": selection_state or None,
        "blocker_count": len(blockers),
    }
    if eligibility != "live":
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=(eligibility or "analysis_only",),
            metrics=metrics,
            message="Candidate was not eligible for live entry.",
        )
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            message="Candidate had selection or execution blockers.",
        )
    if selection_state == "monitor":
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("selected_monitor",),
            metrics=metrics,
            message="Candidate was retained for monitoring, not live entry.",
        )
    if selection_state == "promotable":
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS,
            reason_codes=("selection_live_ready",),
            metrics=metrics,
            message="Candidate was live-ready after selection.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.WATCH,
        reason_codes=("selection_not_evaluated",),
        metrics=metrics,
        message="Live-readiness has not been evaluated on this snapshot yet.",
    )


_FILTERS = {
    "source_is_fresh": _source_is_fresh,
    "setup_context_usable": _setup_context_usable,
    "relative_strength_supportive": _relative_strength_supportive,
    "market_regime_supportive": _market_regime_supportive,
    "chain_data_available": _chain_data_available,
    "option_snapshots_available": _option_snapshots_available,
    "greeks_available": _greeks_available,
    "target_dte_chain_usable": _target_dte_chain_usable,
    "strategy_family_matches": _strategy_family_matches,
    "dte_in_range": _dte_in_range,
    "delta_in_range": _delta_in_range,
    "entry_recipe_passed": _entry_recipe_passed,
    "open_interest_ok": _open_interest_ok,
    "relative_spread_ok": _relative_spread_ok,
    "return_on_risk_ok": _return_on_risk_ok,
    "ranking_policy_passed": _ranking_policy_passed,
    "selection_score_ok": _selection_score_ok,
    "selection_live_ready": _selection_live_ready,
}

PRE_SELECTION_ENTRY_QUALITY_STAGES = (
    EntryQualityStageName.SOURCE_PREFLIGHT,
    EntryQualityStageName.UNDERLYING_SETUP,
    EntryQualityStageName.CHAIN_VIABILITY,
    EntryQualityStageName.CONTRACT_FIT,
    EntryQualityStageName.PREMIUM_QUALITY,
)

POST_SELECTION_ENTRY_QUALITY_STAGES = (EntryQualityStageName.SELECTION,)


def _stage_names(
    values: Sequence[EntryQualityStageName | str] | None,
    *,
    profile: EntryQualityProfile,
) -> tuple[EntryQualityStageName, ...]:
    if values is None:
        return tuple(stage.stage for stage in profile.stages)
    return tuple(value if isinstance(value, EntryQualityStageName) else EntryQualityStageName(str(value)) for value in values)


def evaluate_entry_quality_snapshot(
    *,
    context: EntryQualityContext,
    snapshot: FeatureSnapshot,
    profile: EntryQualityProfile | None = None,
    candidate: Mapping[str, Any] | None = None,
    stage_names: Sequence[EntryQualityStageName | str] | None = None,
    evaluation_phase: str = "full",
) -> EntryQualityWaterfall:
    resolved_profile = profile or resolve_entry_quality_profile(context.quality_profile_id)
    included_stages = _stage_names(stage_names, profile=resolved_profile)
    included_stage_set = set(included_stages)
    waterfall = EntryQualityWaterfall(
        profile_id=resolved_profile.profile_id,
        metadata={
            "symbol": snapshot.symbol,
            "trade_structure": context.trade_structure,
            "candidate_attached": snapshot.candidate is not None or candidate is not None,
            "evaluation_phase": evaluation_phase,
            "included_stages": [stage.value for stage in included_stages],
        },
    )
    active_snapshot = snapshot if candidate is None else snapshot.with_candidate(candidate)
    for stage in resolved_profile.stages:
        if stage.stage not in included_stage_set:
            continue
        for filter_ref in stage.filters:
            evaluator = _FILTERS.get(filter_ref.filter_id)
            if evaluator is None:
                waterfall = waterfall.add_result(
                    _result(
                        filter_ref=filter_ref,
                        status=FilterResultStatus.WATCH,
                        reason_codes=("filter_not_implemented",),
                        message="Entry quality filter is declared but not implemented.",
                    )
                )
                continue
            waterfall = waterfall.add_result(evaluator(context, active_snapshot, filter_ref))
    return waterfall


def evaluate_momentum_long_call_snapshot(
    *,
    context: EntryQualityContext,
    snapshot: FeatureSnapshot,
    candidate: Mapping[str, Any] | None = None,
    stage_names: Sequence[EntryQualityStageName | str] | None = None,
    evaluation_phase: str = "full",
) -> EntryQualityWaterfall:
    profile = resolve_entry_quality_profile(MOMENTUM_LONG_CALL_PROFILE_ID)
    return evaluate_entry_quality_snapshot(
        context=context,
        snapshot=snapshot,
        profile=profile,
        candidate=candidate,
        stage_names=stage_names,
        evaluation_phase=evaluation_phase,
    )


__all__ = [
    "POST_SELECTION_ENTRY_QUALITY_STAGES",
    "PRE_SELECTION_ENTRY_QUALITY_STAGES",
    "evaluate_entry_quality_snapshot",
    "evaluate_momentum_long_call_snapshot",
]
