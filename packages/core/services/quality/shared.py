from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

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

_STRUCTURE_RULES: dict[str, dict[str, Any]] = {
    "call_credit_spread": {
        "leg_count": 2,
        "role_counts": {"short": 1, "long": 1},
        "side_counts": {"sell": 1, "buy": 1},
        "option_type_counts": {"call": 2},
        "min_width": 0.0001,
    },
    "put_credit_spread": {
        "leg_count": 2,
        "role_counts": {"short": 1, "long": 1},
        "side_counts": {"sell": 1, "buy": 1},
        "option_type_counts": {"put": 2},
        "min_width": 0.0001,
    },
    "call_debit_spread": {
        "leg_count": 2,
        "role_counts": {"short": 1, "long": 1},
        "side_counts": {"sell": 1, "buy": 1},
        "option_type_counts": {"call": 2},
        "min_width": 0.0001,
    },
    "put_debit_spread": {
        "leg_count": 2,
        "role_counts": {"short": 1, "long": 1},
        "side_counts": {"sell": 1, "buy": 1},
        "option_type_counts": {"put": 2},
        "min_width": 0.0001,
    },
    "iron_condor": {
        "leg_count": 4,
        "role_counts": {"short": 2, "long": 2},
        "side_counts": {"sell": 2, "buy": 2},
        "option_type_counts": {"call": 2, "put": 2},
        "min_width": 0.0001,
    },
    "short_put": {
        "leg_count": 1,
        "role_counts": {"short": 1},
        "side_counts": {"sell": 1},
        "option_type_counts": {"put": 1},
    },
    "long_straddle": {
        "leg_count": 2,
        "role_counts": {"long": 2},
        "side_counts": {"buy": 2},
        "option_type_counts": {"call": 1, "put": 1},
        "strike_relationship": "same",
    },
    "long_strangle": {
        "leg_count": 2,
        "role_counts": {"long": 2},
        "side_counts": {"buy": 2},
        "option_type_counts": {"call": 1, "put": 1},
        "strike_relationship": "different",
    },
}

_CRITICAL_STRUCTURE_MISSING_REASONS = {
    "canonical_legs_missing",
    "leg_role_missing",
    "leg_side_missing",
    "leg_position_intent_missing",
    "trade_structure_missing",
    "premium_kind_unknown",
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


def _structure(snapshot: FeatureSnapshot) -> dict[str, Any]:
    chain_structure = as_mapping(as_mapping(snapshot.chain).get("structure"))
    if chain_structure:
        return chain_structure
    premium_structure = as_mapping(as_mapping(snapshot.premium).get("structure"))
    if premium_structure:
        return premium_structure
    return as_mapping(snapshot.metadata.get("structure"))


def _structure_rules(context: EntryQualityContext) -> dict[str, Any]:
    return dict(_STRUCTURE_RULES.get(normalize_strategy_family(context.trade_structure), {}))


def _structure_missing_reasons(snapshot: FeatureSnapshot) -> tuple[str, ...]:
    reasons: list[str] = []
    reasons.extend(unique_text_list(as_mapping(snapshot.chain).get("missing_structure_reasons")))
    reasons.extend(unique_text_list(as_mapping(snapshot.premium).get("missing_structure_reasons")))
    reasons.extend(unique_text_list(snapshot.metadata.get("missing_structure_reasons")))
    reasons.extend(unique_text_list(_structure(snapshot).get("missing_reasons")))
    return tuple(dict.fromkeys(reasons))


def _structure_legs(snapshot: FeatureSnapshot) -> tuple[dict[str, Any], ...]:
    structure = _structure(snapshot)
    legs = structure.get("legs")
    if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes)):
        legs = as_mapping(snapshot.chain).get("legs")
    if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes)):
        return ()
    return tuple(dict(leg) for leg in legs if isinstance(leg, Mapping))


def _structure_count_map(value: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key, count in as_mapping(value).items():
        rendered = as_text(key)
        if rendered is None:
            continue
        counts[rendered] = coerce_int(count) or 0
    return counts


def _leg_option_type_counts(legs: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for leg in legs:
        option_type = as_text(leg.get("option_type"))
        if option_type is None:
            symbol = as_text(leg.get("symbol"))
            if symbol is not None:
                marker = symbol.upper().rsplit("C", 1)
                if len(marker) == 2 and marker[1].isdigit():
                    option_type = "call"
                else:
                    marker = symbol.upper().rsplit("P", 1)
                    if len(marker) == 2 and marker[1].isdigit():
                        option_type = "put"
        normalized = str(option_type or "unknown").strip().lower()
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _leg_strikes(legs: Sequence[Mapping[str, Any]]) -> tuple[float, ...]:
    strikes: list[float] = []
    for leg in legs:
        strike = coerce_float(leg.get("strike") or leg.get("strike_price"))
        if strike is None:
            continue
        strikes.append(round(strike, 4))
    return tuple(strike for strike in dict.fromkeys(strikes))


def _count_mismatches(
    *,
    actual: Mapping[str, int],
    expected: Mapping[str, int],
    reason_prefix: str,
) -> tuple[str, ...]:
    mismatches: list[str] = []
    for key, expected_count in expected.items():
        if actual.get(key, 0) != expected_count:
            mismatches.append(f"{reason_prefix}_{key}_mismatch")
    extra = sorted(key for key, count in actual.items() if count and key not in expected and key != "unknown")
    if extra:
        mismatches.append(f"{reason_prefix}_unexpected")
    return tuple(mismatches)


def _structure_limit_price(candidate: Mapping[str, Any], economics: Mapping[str, Any]) -> float | None:
    for field in ("limit_price", "midpoint_credit", "midpoint_value", "net_credit", "net_debit", "credit", "debit"):
        value = coerce_float(candidate.get(field))
        if value is None:
            value = coerce_float(economics.get(field))
        if value is not None:
            return value
    return None


def _structure_max_risk(candidate: Mapping[str, Any], economics: Mapping[str, Any]) -> float | None:
    for field in ("max_loss", "max_risk"):
        value = coerce_float(candidate.get(field))
        if value is None:
            value = coerce_float(economics.get(field))
        if value is not None:
            return value
    return None


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


def _context_policy_status(value: Any, *, default: FilterResultStatus = FilterResultStatus.WATCH) -> FilterResultStatus:
    rendered = as_text(value)
    if rendered is None:
        return default
    normalized = rendered.strip().lower()
    if normalized == FilterResultStatus.BLOCK.value:
        return FilterResultStatus.BLOCK
    return FilterResultStatus.WATCH


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


def _payload_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_payload = getattr(value, "to_payload", None)
    if callable(to_payload):
        return as_mapping(to_payload())
    return {}


def _market_context_payload(context: EntryQualityContext, snapshot: FeatureSnapshot) -> dict[str, Any]:
    for value in (
        as_mapping(context.metadata).get("market_context"),
        as_mapping(snapshot.metadata).get("market_context"),
        as_mapping(as_mapping(snapshot.metadata).get("candidate_result_summary")).get("market_context"),
        as_mapping(_diagnostic_evidence(snapshot)).get("market_context"),
    ):
        payload = _payload_mapping(value)
        if payload:
            return payload
    return {}
