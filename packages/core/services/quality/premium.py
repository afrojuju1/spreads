from __future__ import annotations


from core.services.option_structures import net_premium_kind
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
    _candidate,
    _candidate_contract,
    _candidate_economics,
    _chain_filters,
    _count_reasons,
    _resolved_thresholds,
    _result,
    _stage_rejection_reasons,
    _structure,
    _structure_limit_price,
    _structure_max_risk,
    _structure_missing_reasons,
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


def _structure_economics_available(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    candidate = _candidate(snapshot)
    economics = _candidate_economics(snapshot)
    structure = _structure(snapshot)
    expected_premium_kind = net_premium_kind(context.trade_structure)
    premium_kind = as_text(structure.get("premium_kind"))
    limit_price = _structure_limit_price(candidate, economics)
    max_risk = _structure_max_risk(candidate, economics)
    max_profit = coerce_float(candidate.get("max_profit") if "max_profit" in candidate else economics.get("max_profit"))
    missing_reasons = _structure_missing_reasons(snapshot)
    metrics = {
        "premium_kind": premium_kind,
        "expected_premium_kind": expected_premium_kind,
        "limit_price": limit_price,
        "max_risk": max_risk,
        "max_profit": max_profit,
        "missing_structure_reasons": list(missing_reasons),
    }
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            metrics=metrics,
            message="No candidate was attached for structure economics evaluation.",
        )
    blockers: list[str] = []
    if expected_premium_kind is not None and premium_kind != expected_premium_kind:
        blockers.append("structure_premium_kind_mismatch")
    if limit_price is None:
        blockers.append("structure_limit_price_missing")
    if max_risk is None:
        blockers.append("max_risk_missing")
    blockers = list(dict.fromkeys(blockers))
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            message="Candidate did not expose complete structure economics.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=("structure_economics_available",),
        metrics=metrics,
        message="Candidate exposed complete structure economics.",
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
