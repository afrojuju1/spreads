from __future__ import annotations


from core.services.option_structures import normalize_strategy_family
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, unique_text_list

from .models import (
    EntryFilterRef,
    EntryQualityContext,
    EntryQualityStageName,
    FeatureSnapshot,
    FilterResult,
    FilterResultStatus,
)

from core.services.quality.shared import (
    _CRITICAL_STRUCTURE_MISSING_REASONS,
    _candidate,
    _candidate_contract,
    _chain_filters,
    _count_mismatches,
    _leg_option_type_counts,
    _leg_strikes,
    _resolved_thresholds,
    _result,
    _stage_rejection_reasons,
    _structure,
    _structure_count_map,
    _structure_legs,
    _structure_missing_reasons,
    _structure_rules,
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


def _structure_family_matches(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    structure = _structure(snapshot)
    expected = normalize_strategy_family(context.trade_structure)
    candidate_strategy = as_text(candidate.get("strategy") or candidate.get("strategy_family"))
    structure_family = as_text(structure.get("trade_structure"))
    candidate_family = normalize_strategy_family(candidate_strategy) if candidate_strategy is not None else None
    snapshot_family = normalize_strategy_family(structure_family) if structure_family is not None else None
    metrics = {
        "trade_structure": expected,
        "candidate_strategy": candidate_strategy,
        "structure_trade_structure": structure_family,
    }
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            message="No candidate was attached for structure-family evaluation.",
        )
    blockers: list[str] = []
    if candidate_family is not None and candidate_family != expected:
        blockers.append("candidate_strategy_family_mismatch")
    if snapshot_family is not None and snapshot_family != expected:
        blockers.append("structure_family_mismatch")
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            message="Candidate structure family did not match the entry routine.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("structure_family_matched",),
        metrics=metrics,
        message="Candidate structure family matched the entry routine.",
    )


def _canonical_structure_available(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    structure = _structure(snapshot)
    rules = _structure_rules(context)
    leg_count = coerce_int(structure.get("leg_count") or as_mapping(snapshot.chain).get("leg_count")) or 0
    expected_leg_count = coerce_int(rules.get("leg_count"))
    missing_reasons = _structure_missing_reasons(snapshot)
    critical_missing = tuple(reason for reason in missing_reasons if reason in _CRITICAL_STRUCTURE_MISSING_REASONS)
    metrics = {
        "trade_structure": normalize_strategy_family(context.trade_structure),
        "structure_trade_structure": structure.get("trade_structure"),
        "leg_count": leg_count,
        "expected_leg_count": expected_leg_count,
        "role_counts": _structure_count_map(structure.get("role_counts")),
        "side_counts": _structure_count_map(structure.get("side_counts")),
        "premium_kind": structure.get("premium_kind"),
        "order_class": structure.get("order_class"),
        "missing_structure_reasons": list(missing_reasons),
    }
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            message="No candidate was attached for canonical-structure evaluation.",
        )
    blockers = list(critical_missing)
    if expected_leg_count is not None and leg_count != expected_leg_count:
        blockers.append("structure_leg_count_mismatch")
    blockers = list(dict.fromkeys(blockers))
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            message="Candidate did not expose a complete canonical option structure.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("canonical_structure_available",),
        metrics=metrics,
        message="Candidate exposed a complete canonical option structure.",
    )


def _structure_leg_mix_matches(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    structure = _structure(snapshot)
    rules = _structure_rules(context)
    legs = _structure_legs(snapshot)
    role_counts = _structure_count_map(structure.get("role_counts"))
    side_counts = _structure_count_map(structure.get("side_counts"))
    option_type_counts = _leg_option_type_counts(legs)
    strikes = _leg_strikes(legs)
    strike_relationship = as_text(rules.get("strike_relationship"))
    metrics = {
        "leg_count": len(legs),
        "expected_leg_count": rules.get("leg_count"),
        "role_counts": role_counts,
        "expected_role_counts": dict(as_mapping(rules.get("role_counts"))),
        "side_counts": side_counts,
        "expected_side_counts": dict(as_mapping(rules.get("side_counts"))),
        "option_type_counts": option_type_counts,
        "expected_option_type_counts": dict(as_mapping(rules.get("option_type_counts"))),
        "strike_count": len(strikes),
        "strike_relationship": strike_relationship,
    }
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            message="No candidate was attached for leg-mix evaluation.",
        )
    if not legs:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("canonical_legs_missing",),
            metrics=metrics,
            message="Candidate did not expose canonical legs for leg-mix evaluation.",
        )
    blockers: list[str] = []
    expected_roles = {str(key): int(value) for key, value in as_mapping(rules.get("role_counts")).items()}
    expected_sides = {str(key): int(value) for key, value in as_mapping(rules.get("side_counts")).items()}
    expected_option_types = {str(key): int(value) for key, value in as_mapping(rules.get("option_type_counts")).items()}
    blockers.extend(_count_mismatches(actual=role_counts, expected=expected_roles, reason_prefix="structure_role_count"))
    blockers.extend(_count_mismatches(actual=side_counts, expected=expected_sides, reason_prefix="structure_side_count"))
    blockers.extend(_count_mismatches(actual=option_type_counts, expected=expected_option_types, reason_prefix="structure_option_type_count"))
    if strike_relationship == "same" and len(strikes) != 1:
        blockers.append("structure_strikes_not_equal")
    elif strike_relationship == "different" and len(strikes) < 2:
        blockers.append("structure_strikes_not_distinct")
    blockers = list(dict.fromkeys(blockers))
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            message="Candidate leg mix did not match the strategy family.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("structure_leg_mix_matched",),
        metrics=metrics,
        message="Candidate leg mix matched the strategy family.",
    )


def _structure_expiration_consistent(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    candidate = _candidate(snapshot)
    structure = _structure(snapshot)
    leg_count = coerce_int(structure.get("leg_count") or as_mapping(snapshot.chain).get("leg_count")) or 0
    expiration_date = as_text(structure.get("expiration_date"))
    expiration_dates = tuple(unique_text_list(structure.get("expiration_dates")))
    same_expiration = bool(structure.get("same_expiration"))
    metrics = {
        "leg_count": leg_count,
        "expiration_date": expiration_date,
        "expiration_dates": list(expiration_dates),
        "same_expiration": same_expiration,
    }
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            message="No candidate was attached for expiration evaluation.",
        )
    if leg_count <= 0:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("canonical_legs_missing",),
            metrics=metrics,
            message="Candidate did not expose canonical legs for expiration evaluation.",
        )
    if expiration_date is None:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("structure_expiration_missing",),
            metrics=metrics,
            message="Candidate structure did not expose a common expiration.",
        )
    if leg_count > 1 and not same_expiration:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("structure_expiration_mismatch",),
            metrics=metrics,
            message="Candidate structure legs did not share one expiration.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("structure_expiration_consistent",),
        metrics=metrics,
        message="Candidate structure expiration was consistent.",
    )


def _structure_width_ok(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    structure = _structure(snapshot)
    rules = _structure_rules(context)
    threshold_defaults = {"min_width": rules.get("min_width")}
    thresholds = _resolved_thresholds(context, filter_ref, threshold_defaults)
    minimum_width = coerce_float(thresholds.get("min_width"))
    width = coerce_float(structure.get("width"))
    metrics = {
        "width": width,
        "trade_structure": normalize_strategy_family(context.trade_structure),
    }
    resolved_thresholds = {"min_width": minimum_width}
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="No candidate was attached for width evaluation.",
        )
    if minimum_width is None:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS if width is not None else FilterResultStatus.WATCH,
            reason_codes=("structure_width_available",) if width is not None else ("structure_width_not_required",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Structure width was not required by this profile.",
        )
    if width is None:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("structure_width_missing",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Candidate structure width was required but missing.",
        )
    if width < minimum_width:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=("structure_width_below_min",),
            metrics=metrics,
            thresholds=resolved_thresholds,
            message="Candidate structure width was below the profile minimum.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("structure_width_ok",),
        metrics=metrics,
        thresholds=resolved_thresholds,
        message="Candidate structure width was usable.",
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
