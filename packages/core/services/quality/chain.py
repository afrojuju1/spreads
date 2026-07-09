from __future__ import annotations


from core.value_coercion import as_mapping, coerce_float, coerce_int

from .models import (
    EntryFilterRef,
    EntryQualityContext,
    EntryQualityStageName,
    FeatureSnapshot,
    FilterResult,
    FilterResultStatus,
)

from core.services.quality.shared import (
    _CHAIN_USABILITY_REASON_MAP,
    _candidate,
    _candidate_bid_ask_spread,
    _candidate_contract,
    _candidate_min_quote_size,
    _candidate_option_volume,
    _chain_examples,
    _chain_filters,
    _diagnostic_evidence,
    _first_reason,
    _ratio,
    _raw_rejection_counts,
    _resolved_thresholds,
    _result,
    _stage_rejection_reasons,
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
