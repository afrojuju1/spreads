from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, date, datetime
import hashlib
import json
from typing import Any

from pydantic import Field, field_validator

from core.model_contracts import DomainModel
from core.services.candidate_identity import resolve_candidate_identity
from core.services.quality.models import EntryQualityWaterfall, FeatureSnapshot
from core.services.quality.evidence import EntryQualityAnalysis, quality_key_for_snapshot
from core.storage.serializers import parse_date, parse_datetime
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int, normalize_symbol

TRADING_FEATURE_VERSION = "entry_quality_snapshot_v1"
MARKET_DATA_QUALITY_VERSION = "market_data_quality_sla_v1"

PASS_STATES = {"ok", "pass", "ready", "fresh", "completed", "available"}
BLOCK_STATES = {"block", "blocked", "stale", "missing", "failed", "error", "unavailable", "data_unavailable"}
NEUTRAL_STATES = {"not_applicable", "not_checked"}

SOURCE_CONFIDENCE_KEYS = (
    "source_confidence",
    "event_confidence",
    "earnings_confidence",
    "calendar_confidence",
    "consensus_confidence",
    "confidence",
)


class TradingFeatureSnapshotRecord(DomainModel):
    trading_feature_snapshot_id: str
    feature_version: str = TRADING_FEATURE_VERSION
    candidate_run_id: str
    trade_candidate_id: str | None = None
    ticker_source_run_id: str | None = None
    ticker_source_kind: str
    ticker_source_id: str
    trading_strategy_id: str
    trade_structure: str
    routine: str = "entry"
    config_hash: str
    session_date: date
    observed_at: datetime
    underlying_symbol: str
    candidate_identity: str | None = None
    feature_scope: str
    quality_profile_id: str
    quality_status: str
    market_data_quality_state: str
    market_data_quality_reason: str
    source: dict[str, Any] = Field(default_factory=dict)
    underlying: dict[str, Any] = Field(default_factory=dict)
    chain: dict[str, Any] = Field(default_factory=dict)
    premium: dict[str, Any] = Field(default_factory=dict)
    candidate: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    quality: dict[str, Any] = Field(default_factory=dict)
    market_data_quality: dict[str, Any] = Field(default_factory=dict)

    @field_validator("underlying_symbol", mode="before")
    @classmethod
    def _normalize_symbol(cls, value: Any) -> str:
        symbol = normalize_symbol(value)
        if symbol is None:
            raise ValueError("underlying_symbol is required")
        return symbol

    @field_validator(
        "trading_feature_snapshot_id",
        "candidate_run_id",
        "ticker_source_kind",
        "ticker_source_id",
        "trading_strategy_id",
        "trade_structure",
        "routine",
        "config_hash",
        "feature_scope",
        "quality_profile_id",
        "quality_status",
        "market_data_quality_state",
        "market_data_quality_reason",
        mode="before",
    )
    @classmethod
    def _normalize_required_text(cls, value: Any) -> str:
        rendered = as_text(value)
        if rendered is None:
            raise ValueError("required text field is empty")
        return rendered

    @field_validator("trade_candidate_id", "ticker_source_run_id", "candidate_identity", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return as_text(value)

    @field_validator("session_date", mode="before")
    @classmethod
    def _normalize_session_date(cls, value: Any) -> date:
        return parse_date(value)

    @field_validator("observed_at", mode="before")
    @classmethod
    def _normalize_observed_at(cls, value: Any) -> datetime:
        parsed = parse_datetime(value)
        if parsed is None:
            raise ValueError("observed_at is required")
        return parsed


def _stable_id(prefix: str, *parts: Any) -> str:
    material = json.dumps([str(part) for part in parts], sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"{prefix}:{hashlib.sha1(material).hexdigest()[:24]}"


def _component(state: str, reason: str, **evidence: Any) -> dict[str, Any]:
    payload = {"state": state, "reason": reason}
    payload.update({key: value for key, value in evidence.items() if value not in (None, "", [], {})})
    return payload


def _state_from_status(status: Any) -> str | None:
    rendered = as_text(status)
    if rendered is None:
        return None
    normalized = rendered.lower()
    if normalized in PASS_STATES:
        return "pass"
    if normalized in BLOCK_STATES:
        return "block"
    if normalized in {"watch", "degraded", "partial", "unknown"}:
        return "watch"
    return None


def _source_component(snapshot: FeatureSnapshot) -> dict[str, Any]:
    source = as_mapping(snapshot.source)
    degradation = as_mapping(source.get("degradation"))
    blockers = list(source.get("blockers") or [])
    symbol_count = coerce_int(source.get("symbol_count")) or 0
    degradation_state = _state_from_status(degradation.get("status"))
    if blockers:
        return _component("block", "source_blockers_present", blockers=blockers, symbol_count=symbol_count)
    if degradation_state == "block":
        return _component("block", as_text(degradation.get("reason")) or "source_degraded", symbol_count=symbol_count)
    if symbol_count <= 0:
        return _component("block", "no_source_symbols")
    status_state = _state_from_status(source.get("status"))
    if status_state in {"pass", None}:
        return _component("pass", "source_symbols_available", symbol_count=symbol_count)
    return _component(status_state, as_text(source.get("status")) or "source_status_unknown", symbol_count=symbol_count)


def _quote_freshness_component(snapshot: FeatureSnapshot) -> dict[str, Any]:
    underlying = as_mapping(snapshot.underlying)
    freshness = as_mapping(underlying.get("quote_freshness"))
    quote = as_mapping(underlying.get("quote"))
    status_state = _state_from_status(freshness.get("status"))
    age_seconds = coerce_float(freshness.get("age_seconds"))
    max_age_seconds = coerce_float(freshness.get("max_age_seconds"))
    if status_state is not None:
        return _component(
            status_state,
            as_text(freshness.get("reason")) or as_text(freshness.get("status")) or "quote_freshness_status",
            age_seconds=age_seconds,
            max_age_seconds=max_age_seconds,
        )
    if age_seconds is not None and max_age_seconds is not None:
        state = "pass" if age_seconds <= max_age_seconds else "block"
        reason = "quote_fresh" if state == "pass" else "quote_stale"
        return _component(state, reason, age_seconds=age_seconds, max_age_seconds=max_age_seconds)
    if quote:
        return _component("watch", "quote_present_without_freshness", quote_keys=sorted(quote))
    return _component("watch", "quote_freshness_unknown")


def _chain_component(snapshot: FeatureSnapshot) -> dict[str, Any]:
    chain = as_mapping(snapshot.chain)
    expiration_count = coerce_int(chain.get("expiration_count")) or 0
    contract_count = coerce_int(chain.get("contract_count")) or 0
    snapshot_count = coerce_int(chain.get("snapshot_count")) or 0
    if expiration_count <= 0:
        return _component("block", "no_expirations", expiration_count=expiration_count)
    if contract_count <= 0:
        return _component("block", "no_contracts", expiration_count=expiration_count, contract_count=contract_count)
    if snapshot_count <= 0:
        return _component(
            "block",
            "no_option_snapshots",
            expiration_count=expiration_count,
            contract_count=contract_count,
            snapshot_count=snapshot_count,
        )
    return _component(
        "pass",
        "chain_snapshots_available",
        expiration_count=expiration_count,
        contract_count=contract_count,
        snapshot_count=snapshot_count,
    )


def _greeks_component(snapshot: FeatureSnapshot) -> dict[str, Any]:
    chain = as_mapping(snapshot.chain)
    delta_snapshot_count = coerce_int(chain.get("delta_snapshot_count")) or 0
    if bool(chain.get("greeks_available")) or delta_snapshot_count > 0:
        return _component("pass", "greeks_available", delta_snapshot_count=delta_snapshot_count)
    if (coerce_int(chain.get("snapshot_count")) or 0) <= 0:
        return _component("not_applicable", "option_snapshots_missing")
    return _component("block", "greeks_missing", delta_snapshot_count=delta_snapshot_count)


def _expected_move_component(snapshot: FeatureSnapshot) -> dict[str, Any]:
    chain = as_mapping(snapshot.chain)
    expected_move_count = coerce_int(chain.get("expected_move_count")) or 0
    if expected_move_count > 0:
        return _component("pass", "expected_move_available", expected_move_count=expected_move_count)
    if (coerce_int(chain.get("contract_count")) or 0) <= 0:
        return _component("not_applicable", "contracts_missing")
    return _component("block", "expected_move_missing", expected_move_count=expected_move_count)


def _confidence_values(value: Any) -> list[float]:
    values: list[float] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in SOURCE_CONFIDENCE_KEYS and (coerced := coerce_float(item)) is not None:
                values.append(coerced)
            values.extend(_confidence_values(item))
    elif isinstance(value, list | tuple):
        for item in value:
            values.extend(_confidence_values(item))
    return values


def _source_confidence_component(snapshot: FeatureSnapshot) -> dict[str, Any]:
    source = as_mapping(snapshot.source)
    metadata = as_mapping(snapshot.metadata)
    candidate = as_mapping(snapshot.candidate)
    confidence_values = _confidence_values(
        {
            "source": source,
            "diagnostic": metadata.get("diagnostic"),
            "diagnostic_evidence": metadata.get("diagnostic_evidence"),
            "candidate": candidate,
        }
    )
    source_id = as_text(source.get("ticker_source_id")) or ""
    if not confidence_values:
        state = "watch" if "earnings" in source_id else "not_applicable"
        return _component(state, "source_confidence_not_reported")
    confidence = min(confidence_values)
    if confidence >= 0.70:
        return _component("pass", "source_confidence_clear", confidence=confidence)
    if confidence >= 0.40:
        return _component("watch", "source_confidence_marginal", confidence=confidence)
    return _component("block", "source_confidence_low", confidence=confidence)


def _clickhouse_component(snapshot: FeatureSnapshot) -> dict[str, Any]:
    chain = as_mapping(snapshot.chain)
    structure = as_mapping(chain.get("structure"))
    leg_symbols = list(structure.get("leg_symbols") or [])
    return _component(
        "not_checked",
        "live_feature_persistence_does_not_probe_clickhouse",
        leg_symbol_count=len(leg_symbols),
    )


def build_market_data_quality(snapshot: FeatureSnapshot) -> dict[str, Any]:
    components = {
        "source": _source_component(snapshot),
        "quote_freshness": _quote_freshness_component(snapshot),
        "chain_completeness": _chain_component(snapshot),
        "greeks": _greeks_component(snapshot),
        "expected_move": _expected_move_component(snapshot),
        "clickhouse_coverage": _clickhouse_component(snapshot),
        "source_confidence": _source_confidence_component(snapshot),
    }
    state_counts = Counter(str(component.get("state") or "unknown") for component in components.values())
    blocking = [name for name, component in components.items() if component.get("state") == "block"]
    watched = [name for name, component in components.items() if component.get("state") in {"watch", "unknown"}]
    if blocking:
        state = "block"
        reason = blocking[0]
    elif watched:
        state = "watch"
        reason = watched[0]
    else:
        state = "pass"
        reason = "market_data_sla_clear"
    return {
        "version": MARKET_DATA_QUALITY_VERSION,
        "state": state,
        "reason": reason,
        "state_counts": dict(sorted(state_counts.items())),
        "components": components,
    }


def _quality_status(waterfall: EntryQualityWaterfall | None) -> str:
    if waterfall is None:
        return "unknown"
    if waterfall.blocked:
        return "block"
    if any(result.status.value == "watch" for result in waterfall.results):
        return "watch"
    return "pass"


def _quality_payload(waterfall: EntryQualityWaterfall | None) -> dict[str, Any]:
    if waterfall is None:
        return {"quality_waterfall": None}
    return {"quality_waterfall": waterfall.as_dict()}


def _observed_at(snapshot: FeatureSnapshot, fallback: datetime) -> datetime:
    if snapshot.observed_at is not None:
        return snapshot.observed_at.astimezone(UTC) if snapshot.observed_at.tzinfo is not None else snapshot.observed_at.replace(tzinfo=UTC)
    return fallback


def build_trading_feature_snapshot_records(
    *,
    runtime: Any,
    market_date: str | date,
    generated_at: str | datetime,
    ticker_set: Any,
    candidate_run_id: str,
    trade_candidate_ids_by_identity: Mapping[str, str],
    quality_analysis: EntryQualityAnalysis,
) -> tuple[TradingFeatureSnapshotRecord, ...]:
    generated_at_dt = parse_datetime(generated_at)
    if generated_at_dt is None:
        raise ValueError("generated_at is required")
    records: list[TradingFeatureSnapshotRecord] = []
    for snapshot in quality_analysis.snapshots:
        key = quality_key_for_snapshot(snapshot)
        candidate_identity = key[1] or None
        candidate = as_mapping(snapshot.candidate)
        if candidate_identity is None and candidate:
            candidate_identity = resolve_candidate_identity(candidate)
        waterfall = quality_analysis.by_candidate.get(key) if candidate_identity else quality_analysis.by_symbol.get(snapshot.symbol)
        quality = _quality_payload(waterfall)
        market_data_quality = build_market_data_quality(snapshot)
        source = as_mapping(snapshot.source)
        records.append(
            TradingFeatureSnapshotRecord(
                trading_feature_snapshot_id=_stable_id(
                    "trading_feature_snapshot",
                    candidate_run_id,
                    runtime.trading_strategy_id,
                    snapshot.symbol,
                    candidate_identity or "symbol",
                ),
                candidate_run_id=candidate_run_id,
                trade_candidate_id=None if candidate_identity is None else trade_candidate_ids_by_identity.get(candidate_identity),
                ticker_source_run_id=source.get("ticker_source_run_id") or ticker_set.ticker_source_run_id,
                ticker_source_kind=source.get("ticker_source_kind") or ticker_set.source.source_type,
                ticker_source_id=source.get("ticker_source_id") or ticker_set.source.ref,
                trading_strategy_id=runtime.trading_strategy_id,
                trade_structure=runtime.trade_structure,
                routine="entry",
                config_hash=runtime.config_hash,
                session_date=market_date,
                observed_at=_observed_at(snapshot, generated_at_dt),
                underlying_symbol=snapshot.symbol,
                candidate_identity=candidate_identity,
                feature_scope="candidate" if candidate_identity else "symbol",
                quality_profile_id=quality_analysis.profile_id,
                quality_status=_quality_status(waterfall),
                market_data_quality_state=str(market_data_quality["state"]),
                market_data_quality_reason=str(market_data_quality["reason"]),
                source=dict(snapshot.source),
                underlying=dict(snapshot.underlying),
                chain=dict(snapshot.chain),
                premium=dict(snapshot.premium),
                candidate=dict(candidate),
                metadata=dict(snapshot.metadata),
                quality=quality,
                market_data_quality=market_data_quality,
            )
        )
    return tuple(records)


__all__ = [
    "MARKET_DATA_QUALITY_VERSION",
    "TRADING_FEATURE_VERSION",
    "TradingFeatureSnapshotRecord",
    "build_market_data_quality",
    "build_trading_feature_snapshot_records",
]
