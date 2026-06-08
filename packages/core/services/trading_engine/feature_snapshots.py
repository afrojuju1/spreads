from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from typing import Any, Protocol

from core.services.option_structures import normalize_strategy_family
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, unique_text_list

from .data import CandidateBuildResult, ResolvedTickerSet
from .entry_quality import FeatureSnapshot

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
        try:
            return datetime.fromisoformat(rendered.replace("Z", "+00:00"))
        except ValueError:
            continue
    return None


def _candidate_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return dict(row)


def _subset(row: Mapping[str, Any], fields: Sequence[str]) -> dict[str, Any]:
    return {field: row.get(field) for field in fields if row.get(field) not in (None, "", [], {})}


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


def _chain_snapshot(diagnostic: Mapping[str, Any], candidate: Mapping[str, Any] | None) -> dict[str, Any]:
    market_data = as_mapping(diagnostic.get("market_data"))
    candidate_payload = _candidate_payload(candidate) if candidate is not None else {}
    chain = {
        "expirations": list(market_data.get("expirations") or []),
        "filters": as_mapping(market_data.get("filters")),
        "rejection_counts": as_mapping(diagnostic.get("rejection_counts")),
        "examples": as_mapping(diagnostic.get("examples")),
        "candidate_contract": _subset(candidate_payload, _CANDIDATE_CONTRACT_FIELDS),
    }
    for field in _CHAIN_COUNT_FIELDS:
        chain[field] = coerce_int(market_data.get(field) or diagnostic.get(field))
    chain["has_contracts"] = (chain.get("contract_count") or 0) > 0
    chain["has_snapshots"] = (chain.get("snapshot_count") or 0) > 0
    chain["greeks_available"] = (chain.get("delta_snapshot_count") or 0) > 0 or candidate_payload.get("delta") not in (None, "")
    return chain


def _premium_snapshot(diagnostic: Mapping[str, Any], candidate: Mapping[str, Any] | None) -> dict[str, Any]:
    candidate_payload = _candidate_payload(candidate) if candidate is not None else {}
    return {
        "ranking_gate": as_mapping(diagnostic.get("ranking_gate")),
        "ranking_policy_status": candidate_payload.get("ranking_policy_status"),
        "ranking_policy_blockers": unique_text_list(candidate_payload.get("ranking_policy_blockers")),
        "scoring_state": candidate_payload.get("scoring_state"),
        "scoring_blockers": unique_text_list(candidate_payload.get("scoring_blockers")),
        "execution_blockers": unique_text_list(candidate_payload.get("execution_blockers")),
        "candidate_economics": _subset(candidate_payload, _CANDIDATE_PREMIUM_FIELDS),
    }


def _metadata_snapshot(
    *,
    candidate_result: CandidateBuildResult,
    diagnostic: Mapping[str, Any],
    candidate: Mapping[str, Any] | None,
    candidate_count_for_symbol: int,
) -> dict[str, Any]:
    candidate_payload = _candidate_payload(candidate) if candidate is not None else {}
    return {
        "candidate_run_id": candidate_result.candidate_run_id,
        "candidate_result_summary": dict(candidate_result.summary or {}),
        "diagnostic": dict(diagnostic),
        "diagnostic_evidence": as_mapping(diagnostic.get("evidence")),
        "candidate_attached": candidate is not None,
        "candidate_count_for_symbol": candidate_count_for_symbol,
        "candidate_identity": candidate_payload.get("candidate_identity") or candidate_payload.get("structure_identity"),
    }


def _snapshot_from_parts(
    *,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult,
    diagnostic: Mapping[str, Any],
    candidate: Mapping[str, Any] | None,
    candidate_count_for_symbol: int,
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
        chain=_chain_snapshot(diagnostic, candidate),
        premium=_premium_snapshot(diagnostic, candidate),
        candidate=None if candidate is None else dict(candidate_payload),
        metadata=_metadata_snapshot(
            candidate_result=candidate_result,
            diagnostic=diagnostic,
            candidate=candidate,
            candidate_count_for_symbol=candidate_count_for_symbol,
        ),
    )


def build_momentum_long_call_feature_snapshots(
    *,
    ticker_set: ResolvedTickerSet,
    candidate_result: CandidateBuildResult,
) -> tuple[FeatureSnapshot, ...]:
    """Build filter-ready facts from existing momentum_long_calls candidate diagnostics."""

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
        )
        if snapshot is not None:
            snapshots.append(snapshot)
    return tuple(snapshots)


register_feature_snapshot_builder(
    trade_structure="long_call",
    quality_profile_id="momentum_long_call_v1",
    builder=build_momentum_long_call_feature_snapshots,
)


__all__ = [
    "FEATURE_SNAPSHOT_BUILDER_REGISTRY",
    "FeatureSnapshotBuilder",
    "FeatureSnapshotBuilderKey",
    "build_feature_snapshots_for_strategy",
    "build_momentum_long_call_feature_snapshots",
    "register_feature_snapshot_builder",
    "resolve_feature_snapshot_builder",
]
