from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from typing import Any, Protocol

from core.services.option_structures import (
    candidate_legs as option_candidate_legs,
    common_expiration_date,
    net_premium_kind,
    normalize_strategy_family,
    structure_barrier_strike,
    structure_strike_path,
    structure_symbol_path,
    structure_width,
    unique_leg_symbols,
)
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, coerce_utc_datetime, unique_text_list

from .data import CandidateBuildResult, ResolvedTickerSet
from core.services.quality.models import FeatureSnapshot

_CHAIN_COUNT_FIELDS = (
    "expiration_count",
    "contract_count",
    "snapshot_count",
    "delta_snapshot_count",
    "expected_move_count",
)

_CANDIDATE_CONTRACT_FIELDS = (
    "option_symbol",
    "contract_symbol",
    "root_symbol",
    "option_type",
    "expiration_date",
    "strike_price",
    "dte",
    "days_to_expiration",
    "delta",
    "gamma",
    "theta",
    "vega",
    "iv",
    "implied_volatility",
    "moneyness",
    "otm_pct",
    "distance_otm_pct",
    "open_interest",
    "option_volume",
    "volume",
    "bid_price",
    "ask_price",
    "mid_price",
    "mark_price",
    "last_price",
    "bid_size",
    "ask_size",
    "relative_spread",
    "bid_ask_spread",
    "spread_width",
)

_CANDIDATE_PREMIUM_FIELDS = (
    "debit",
    "credit",
    "net_debit",
    "net_credit",
    "limit_price",
    "max_loss",
    "max_profit",
    "breakeven",
    "return_on_risk",
    "expected_return",
    "probability_of_profit",
    "profit_probability",
    "slippage_estimate",
    "relative_spread",
    "bid_ask_spread",
    "spread_width",
    "ranking_policy_status",
    "ranking_policy_blockers",
    "scoring_state",
    "scoring_blockers",
    "execution_blockers",
    "quality_score",
    "promotion_score",
    "execution_score",
    "score",
    "confidence",
)

_GENERIC_STRUCTURE_PROFILE_IDS = {
    "call_credit_spread": "call_credit_spread_v1",
    "put_credit_spread": "put_credit_spread_v1",
    "call_debit_spread": "call_debit_spread_v1",
    "put_debit_spread": "put_debit_spread_v1",
    "iron_condor": "iron_condor_v1",
    "short_put": "short_put_v1",
    "long_straddle": "long_straddle_v1",
    "long_strangle": "long_strangle_v1",
}


class FeatureSnapshotBuilder(Protocol):
    def __call__(
        self,
        *,
        ticker_set: ResolvedTickerSet,
        candidate_result: CandidateBuildResult,
    ) -> tuple[FeatureSnapshot, ...]: ...


FeatureSnapshotBuilderKey = tuple[str, str]
FEATURE_SNAPSHOT_BUILDER_REGISTRY: dict[FeatureSnapshotBuilderKey, FeatureSnapshotBuilder] = {}


def _builder_key(*, trade_structure: str, quality_profile_id: str) -> FeatureSnapshotBuilderKey:
    normalized_structure = normalize_strategy_family(trade_structure)
    normalized_profile = str(quality_profile_id or "").strip()
    if not normalized_structure or normalized_structure == "unknown":
        raise ValueError("feature snapshot builder trade_structure is required")
    if not normalized_profile:
        raise ValueError("feature snapshot builder quality_profile_id is required")
    return normalized_structure, normalized_profile


def register_feature_snapshot_builder(
    *,
    trade_structure: str,
    quality_profile_id: str,
    builder: FeatureSnapshotBuilder,
) -> None:
    FEATURE_SNAPSHOT_BUILDER_REGISTRY[_builder_key(trade_structure=trade_structure, quality_profile_id=quality_profile_id)] = builder


def resolve_feature_snapshot_builder(
    *,
    trade_structure: str,
    quality_profile_id: str,
) -> FeatureSnapshotBuilder:
    key = _builder_key(trade_structure=trade_structure, quality_profile_id=quality_profile_id)
    builder = FEATURE_SNAPSHOT_BUILDER_REGISTRY.get(key)
    if builder is None:
        raise ValueError("No feature snapshot builder registered for " f"trade_structure={key[0]!r}, quality_profile_id={key[1]!r}")
    return builder


def build_feature_snapshots_for_strategy(
    *,
    trade_structure: str,
    quality_profile_id: str,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult,
) -> tuple[FeatureSnapshot, ...]:
    builder = resolve_feature_snapshot_builder(
        trade_structure=trade_structure,
        quality_profile_id=quality_profile_id,
    )
    return builder(
        ticker_set=ticker_set,
        candidate_result=candidate_result,
    )


def _symbol_from_row(row: Mapping[str, Any]) -> str | None:
    return as_text(row.get("underlying_symbol") or row.get("symbol") or row.get("root_symbol"))


def _observed_at(*values: Any) -> datetime | None:
    for value in values:
        if isinstance(value, datetime):
            return value
        rendered = as_text(value)
        if rendered is None:
            continue
        parsed = coerce_utc_datetime(rendered)
        if parsed is not None:
            return parsed
    return None


def _candidate_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return dict(row)


def _subset(row: Mapping[str, Any], fields: Sequence[str]) -> dict[str, Any]:
    return {field: row.get(field) for field in fields if row.get(field) not in (None, "", [], {})}


def _role_counts(legs: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for leg in legs:
        role = as_text(leg.get("role")) or "unknown"
        counts[role] = counts.get(role, 0) + 1
    return counts


def _side_counts(legs: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for leg in legs:
        side = as_text(leg.get("side")) or "unknown"
        counts[side] = counts.get(side, 0) + 1
    return counts


def _coalesced_float(row: Mapping[str, Any], *fields: str) -> float | None:
    for field in fields:
        value = coerce_float(row.get(field))
        if value is not None:
            return value
    return None


def _structure_missing_reasons(
    *,
    candidate_payload: Mapping[str, Any],
    legs: Sequence[Mapping[str, Any]],
    trade_structure: str | None,
    premium_kind: str | None,
) -> list[str]:
    reasons: list[str] = []
    if not legs:
        reasons.append("canonical_legs_missing")
    if any(as_text(leg.get("role")) is None for leg in legs):
        reasons.append("leg_role_missing")
    if any(as_text(leg.get("side")) is None for leg in legs):
        reasons.append("leg_side_missing")
    if any(as_text(leg.get("position_intent")) is None for leg in legs):
        reasons.append("leg_position_intent_missing")
    if trade_structure is None:
        reasons.append("trade_structure_missing")
    if premium_kind is None:
        reasons.append("premium_kind_unknown")
    if _coalesced_float(candidate_payload, "limit_price", "midpoint_credit", "midpoint_value", "net_credit", "net_debit", "credit", "debit") is None:
        reasons.append("structure_limit_price_missing")
    if _coalesced_float(candidate_payload, "max_loss", "max_risk") is None:
        reasons.append("max_risk_missing")
    return reasons


def _structure_snapshot(
    *,
    candidate_payload: Mapping[str, Any],
    trade_structure: str | None,
) -> dict[str, Any]:
    normalized_structure = None if trade_structure is None else normalize_strategy_family(trade_structure)
    legs = option_candidate_legs(candidate_payload)
    premium_kind = None if normalized_structure is None else net_premium_kind(normalized_structure)
    expiration = common_expiration_date(list(legs))
    unique_expirations = sorted(
        {expiration_value for expiration_value in (as_text(leg.get("expiration_date")) for leg in legs) if expiration_value is not None}
    )
    return {
        "trade_structure": normalized_structure,
        "premium_kind": premium_kind,
        "legs": [dict(leg) for leg in legs],
        "leg_count": len(legs),
        "leg_symbols": unique_leg_symbols(list(legs)),
        "role_counts": _role_counts(legs),
        "side_counts": _side_counts(legs),
        "expiration_date": expiration,
        "expiration_dates": unique_expirations,
        "same_expiration": bool(expiration) if legs else False,
        "width": structure_width(list(legs), strategy=normalized_structure),
        "barrier_strike": structure_barrier_strike(list(legs), strategy=normalized_structure),
        "symbol_path": structure_symbol_path(list(legs)),
        "strike_path": structure_strike_path(list(legs)),
        "structure_identity": candidate_payload.get("structure_identity") or candidate_payload.get("candidate_identity"),
        "order_class": as_mapping(candidate_payload.get("order_payload")).get("order_class"),
        "missing_reasons": _structure_missing_reasons(
            candidate_payload=candidate_payload,
            legs=legs,
            trade_structure=normalized_structure,
            premium_kind=premium_kind,
        ),
    }


def _source_snapshot(
    *,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult,
) -> dict[str, Any]:
    evidence = dict(ticker_set.evidence or {})
    return {
        "ticker_source_kind": ticker_set.source.source_type,
        "ticker_source_id": ticker_set.source.ref,
        "ticker_source_run_id": ticker_set.ticker_source_run_id,
        "resolved_at": ticker_set.resolved_at.isoformat().replace("+00:00", "Z"),
        "max_age_seconds": ticker_set.source.max_age_seconds,
        "symbol_count": len(ticker_set.symbols),
        "reason_codes": list(ticker_set.reason_codes),
        "blockers": list(ticker_set.blockers),
        "status": evidence.get("status"),
        "degradation": as_mapping(evidence.get("degradation")),
        "summary": as_mapping(evidence.get("summary")),
        "candidate_run_id": candidate_result.candidate_run_id,
        "run_ref": asdict(candidate_result.run_ref),
    }


def _underlying_snapshot(diagnostic: Mapping[str, Any], candidate: Mapping[str, Any] | None) -> dict[str, Any]:
    market_data = as_mapping(diagnostic.get("market_data"))
    setup = as_mapping(diagnostic.get("setup"))
    candidate_payload = _candidate_payload(candidate) if candidate is not None else {}
    return {
        "diagnostic_status": diagnostic.get("diagnostic_status") or diagnostic.get("status"),
        "spot_price": coerce_float(diagnostic.get("spot_price") or candidate_payload.get("underlying_price")),
        "underlying_type": market_data.get("underlying_type") or candidate_payload.get("underlying_type"),
        "daily_bar_count": coerce_int(market_data.get("daily_bar_count")),
        "intraday_bar_count": coerce_int(market_data.get("intraday_bar_count")),
        "quote": as_mapping(market_data.get("quote")),
        "quote_freshness": as_mapping(market_data.get("quote_freshness")),
        "setup": setup,
        "setup_status": setup.get("status") or candidate_payload.get("setup_status"),
        "setup_score": coerce_float(setup.get("score") or candidate_payload.get("setup_score")),
        "setup_reasons": list(setup.get("reasons") or candidate_payload.get("setup_reasons") or []),
        "setup_metrics": {
            key: value for key, value in setup.items() if key not in {"status", "score", "reasons"} and value not in (None, "", [], {})
        },
    }


def _chain_snapshot(
    diagnostic: Mapping[str, Any],
    candidate: Mapping[str, Any] | None,
    *,
    trade_structure: str | None,
) -> dict[str, Any]:
    market_data = as_mapping(diagnostic.get("market_data"))
    candidate_payload = _candidate_payload(candidate) if candidate is not None else {}
    structure = _structure_snapshot(
        candidate_payload=candidate_payload,
        trade_structure=trade_structure or as_text(candidate_payload.get("strategy") or candidate_payload.get("strategy_family")),
    )
    chain = {
        "expirations": list(market_data.get("expirations") or []),
        "filters": as_mapping(market_data.get("filters")),
        "rejection_counts": as_mapping(diagnostic.get("rejection_counts")),
        "examples": as_mapping(diagnostic.get("examples")),
        "candidate_contract": _subset(candidate_payload, _CANDIDATE_CONTRACT_FIELDS),
        "structure": structure,
        "legs": list(structure["legs"]),
        "leg_count": structure["leg_count"],
        "missing_structure_reasons": list(structure["missing_reasons"]),
    }
    for field in _CHAIN_COUNT_FIELDS:
        chain[field] = coerce_int(market_data.get(field) or diagnostic.get(field))
    chain["has_contracts"] = (chain.get("contract_count") or 0) > 0
    chain["has_snapshots"] = (chain.get("snapshot_count") or 0) > 0
    chain["greeks_available"] = (chain.get("delta_snapshot_count") or 0) > 0 or candidate_payload.get("delta") not in (None, "")
    return chain


def _premium_snapshot(
    diagnostic: Mapping[str, Any],
    candidate: Mapping[str, Any] | None,
    *,
    trade_structure: str | None,
) -> dict[str, Any]:
    candidate_payload = _candidate_payload(candidate) if candidate is not None else {}
    structure = _structure_snapshot(
        candidate_payload=candidate_payload,
        trade_structure=trade_structure or as_text(candidate_payload.get("strategy") or candidate_payload.get("strategy_family")),
    )
    return {
        "ranking_gate": as_mapping(diagnostic.get("ranking_gate")),
        "ranking_policy_status": candidate_payload.get("ranking_policy_status"),
        "ranking_policy_blockers": unique_text_list(candidate_payload.get("ranking_policy_blockers")),
        "scoring_state": candidate_payload.get("scoring_state"),
        "scoring_blockers": unique_text_list(candidate_payload.get("scoring_blockers")),
        "execution_blockers": unique_text_list(candidate_payload.get("execution_blockers")),
        "candidate_economics": _subset(candidate_payload, _CANDIDATE_PREMIUM_FIELDS),
        "structure": structure,
        "missing_structure_reasons": list(structure["missing_reasons"]),
    }


def _metadata_snapshot(
    *,
    candidate_result: CandidateBuildResult,
    diagnostic: Mapping[str, Any],
    candidate: Mapping[str, Any] | None,
    candidate_count_for_symbol: int,
    trade_structure: str | None,
) -> dict[str, Any]:
    candidate_payload = _candidate_payload(candidate) if candidate is not None else {}
    structure = _structure_snapshot(
        candidate_payload=candidate_payload,
        trade_structure=trade_structure or as_text(candidate_payload.get("strategy") or candidate_payload.get("strategy_family")),
    )
    diagnostic_evidence = as_mapping(diagnostic.get("evidence"))
    market_context = as_mapping(diagnostic_evidence.get("market_context"))
    if not market_context:
        market_context = as_mapping(as_mapping(candidate_result.summary or {}).get("market_context"))
    return {
        "candidate_run_id": candidate_result.candidate_run_id,
        "candidate_result_summary": dict(candidate_result.summary or {}),
        "diagnostic": dict(diagnostic),
        "diagnostic_evidence": diagnostic_evidence,
        "market_context": market_context,
        "candidate_attached": candidate is not None,
        "candidate_count_for_symbol": candidate_count_for_symbol,
        "candidate_identity": candidate_payload.get("candidate_identity") or candidate_payload.get("structure_identity"),
        "structure": structure,
        "missing_structure_reasons": list(structure["missing_reasons"]),
    }


def _snapshot_from_parts(
    *,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult,
    diagnostic: Mapping[str, Any],
    candidate: Mapping[str, Any] | None,
    candidate_count_for_symbol: int,
    trade_structure: str | None = None,
) -> FeatureSnapshot | None:
    candidate_payload = _candidate_payload(candidate) if candidate is not None else {}
    symbol = _symbol_from_row(diagnostic) or _symbol_from_row(candidate_payload)
    if symbol is None:
        return None
    observed_at = _observed_at(
        candidate_payload.get("observed_at"),
        diagnostic.get("observed_at"),
        ticker_set.resolved_at,
    )
    return FeatureSnapshot(
        symbol=symbol,
        observed_at=observed_at,
        source=_source_snapshot(ticker_set=ticker_set, candidate_result=candidate_result),
        underlying=_underlying_snapshot(diagnostic, candidate),
        chain=_chain_snapshot(diagnostic, candidate, trade_structure=trade_structure),
        premium=_premium_snapshot(diagnostic, candidate, trade_structure=trade_structure),
        candidate=None if candidate is None else dict(candidate_payload),
        metadata=_metadata_snapshot(
            candidate_result=candidate_result,
            diagnostic=diagnostic,
            candidate=candidate,
            candidate_count_for_symbol=candidate_count_for_symbol,
            trade_structure=trade_structure,
        ),
    )


def build_generic_structure_feature_snapshots(
    *,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult,
    trade_structure: str | None = None,
) -> tuple[FeatureSnapshot, ...]:
    """Build filter-ready facts from canonical option-structure candidate diagnostics."""

    diagnostics_by_symbol: dict[str, dict[str, Any]] = {}
    for diagnostic in candidate_result.diagnostics:
        if not isinstance(diagnostic, Mapping):
            continue
        symbol = _symbol_from_row(diagnostic)
        if symbol is None:
            continue
        diagnostics_by_symbol[symbol.upper()] = dict(diagnostic)

    candidates_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in candidate_result.candidates:
        if not isinstance(row, Mapping):
            continue
        candidate = _candidate_payload(row)
        symbol = _symbol_from_row(candidate)
        if symbol is None:
            continue
        candidates_by_symbol.setdefault(symbol.upper(), []).append(dict(row))

    snapshots: list[FeatureSnapshot] = []
    symbols = tuple(dict.fromkeys((*ticker_set.symbols, *diagnostics_by_symbol.keys(), *candidates_by_symbol.keys())))
    for symbol in symbols:
        diagnostic = diagnostics_by_symbol.get(symbol, {"underlying_symbol": symbol, "diagnostic_status": "diagnostic_missing"})
        candidates = candidates_by_symbol.get(symbol, [])
        if candidates:
            for candidate in candidates:
                snapshot = _snapshot_from_parts(
                    ticker_set=ticker_set,
                    candidate_result=candidate_result,
                    diagnostic=diagnostic,
                    candidate=candidate,
                    candidate_count_for_symbol=len(candidates),
                    trade_structure=trade_structure,
                )
                if snapshot is not None:
                    snapshots.append(snapshot)
            continue
        snapshot = _snapshot_from_parts(
            ticker_set=ticker_set,
            candidate_result=candidate_result,
            diagnostic=diagnostic,
            candidate=None,
            candidate_count_for_symbol=0,
            trade_structure=trade_structure,
        )
        if snapshot is not None:
            snapshots.append(snapshot)
    return tuple(snapshots)


def build_momentum_long_call_feature_snapshots(
    *,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult,
) -> tuple[FeatureSnapshot, ...]:
    """Build filter-ready facts from existing momentum_long_calls candidate diagnostics."""

    return build_generic_structure_feature_snapshots(
        ticker_set=ticker_set,
        candidate_result=candidate_result,
        trade_structure="long_call",
    )


def generic_structure_feature_snapshot_builder(trade_structure: str) -> FeatureSnapshotBuilder:
    normalized_structure = normalize_strategy_family(trade_structure)

    def _build(
        *,
        ticker_set: ResolvedTickerSet,
        candidate_result: CandidateBuildResult,
    ) -> tuple[FeatureSnapshot, ...]:
        return build_generic_structure_feature_snapshots(
            ticker_set=ticker_set,
            candidate_result=candidate_result,
            trade_structure=normalized_structure,
        )

    return _build


register_feature_snapshot_builder(
    trade_structure="long_call",
    quality_profile_id="momentum_long_call_v1",
    builder=build_momentum_long_call_feature_snapshots,
)

for _trade_structure, _profile_id in _GENERIC_STRUCTURE_PROFILE_IDS.items():
    register_feature_snapshot_builder(
        trade_structure=_trade_structure,
        quality_profile_id=_profile_id,
        builder=generic_structure_feature_snapshot_builder(_trade_structure),
    )


__all__ = [
    "FEATURE_SNAPSHOT_BUILDER_REGISTRY",
    "FeatureSnapshotBuilder",
    "FeatureSnapshotBuilderKey",
    "build_feature_snapshots_for_strategy",
    "build_generic_structure_feature_snapshots",
    "build_momentum_long_call_feature_snapshots",
    "generic_structure_feature_snapshot_builder",
    "register_feature_snapshot_builder",
    "resolve_feature_snapshot_builder",
]
