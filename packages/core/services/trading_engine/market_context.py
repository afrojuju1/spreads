from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator, model_validator

from core.model_contracts import DomainModel


class MarketContextFreshnessState(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    MISSING = "missing"
    UNKNOWN = "unknown"


class MarketContextDataQualityState(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    DEGRADED = "degraded"
    MISSING = "missing"
    UNKNOWN = "unknown"


class MarketContextFidelityLabel(StrEnum):
    LIVE_MARKET_SLICE = "live_market_slice"
    HISTORICAL_MARKET_SLICE = "historical_market_slice"
    STORED_MARKET_CONTEXT = "stored_market_context"
    STORED_CANDIDATE_CONTEXT = "stored_candidate_context"
    PARTIAL_BENCHMARK_COVERAGE = "partial_benchmark_coverage"
    INCOMPLETE_VOLATILITY_EVIDENCE = "incomplete_volatility_evidence"


class MarketBenchmarkRole(StrEnum):
    BROAD_MARKET = "broad_market"
    GROWTH_MARKET = "growth_market"
    SMALL_CAP = "small_cap"
    SECTOR = "sector"
    RATES = "rates"
    VOLATILITY = "volatility"


class MarketRegimeLabel(StrEnum):
    BULLISH_TREND = "bullish_trend"
    BEARISH_TREND = "bearish_trend"
    RANGE_BOUND = "range_bound"
    VOLATILITY_EXPANSION = "volatility_expansion"
    TRANSITION = "transition"
    UNKNOWN = "unknown"


class MarketRiskPosture(StrEnum):
    RISK_ON = "risk_on"
    NEUTRAL = "neutral"
    RISK_OFF = "risk_off"
    DEFENSIVE = "defensive"
    UNKNOWN = "unknown"


class MarketTrendStrength(StrEnum):
    STRONG_UP = "strong_up"
    MODEST_UP = "modest_up"
    FLAT = "flat"
    MODEST_DOWN = "modest_down"
    STRONG_DOWN = "strong_down"
    UNKNOWN = "unknown"


class MarketVolatilityState(StrEnum):
    COMPRESSED = "compressed"
    NORMAL = "normal"
    ELEVATED = "elevated"
    STRESSED = "stressed"
    UNKNOWN = "unknown"


def _normalize_optional_text(value: Any) -> str | None:
    rendered = str(value or "").strip()
    return rendered or None


def _normalize_text_tuple(value: Any) -> tuple[str, ...]:
    if value in (None, "", (), []):
        return ()
    values = (value,) if isinstance(value, str) else tuple(value)
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        rendered = str(item or "").strip()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return tuple(normalized)


def _normalize_symbol_tuple(value: Any) -> tuple[str, ...]:
    return tuple(symbol.upper() for symbol in _normalize_text_tuple(value))


def _normalize_enum_text(value: Any) -> str:
    return str(value or "").strip().lower()


class MarketContextDataQuality(DomainModel):
    state: MarketContextDataQualityState = MarketContextDataQualityState.UNKNOWN
    freshness: MarketContextFreshnessState = MarketContextFreshnessState.UNKNOWN
    observed_at: datetime | None = None
    stale_after: datetime | None = None
    reason_codes: tuple[str, ...] = ()
    missing_components: tuple[str, ...] = ()
    component_states: dict[str, MarketContextDataQualityState] = Field(default_factory=dict)

    @field_validator("state", "freshness", mode="before")
    @classmethod
    def _normalize_enum(cls, value: Any) -> str:
        return _normalize_enum_text(value)

    @field_validator("reason_codes", "missing_components", mode="before")
    @classmethod
    def _normalize_tuples(cls, value: Any) -> tuple[str, ...]:
        return _normalize_text_tuple(value)

    @field_validator("component_states", mode="before")
    @classmethod
    def _normalize_component_states(cls, value: Any) -> dict[str, Any]:
        if value in (None, ""):
            return {}
        if not isinstance(value, dict):
            raise ValueError("market context component_states must be a mapping")
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            rendered = str(key or "").strip()
            if rendered:
                normalized[rendered] = item
        return normalized

    @model_validator(mode="after")
    def _validate_freshness_window(self) -> MarketContextDataQuality:
        if self.observed_at is not None and self.stale_after is not None and self.stale_after <= self.observed_at:
            raise ValueError("market context stale_after must be after observed_at")
        return self


class MarketBenchmarkEvidence(DomainModel):
    symbol: str
    role: MarketBenchmarkRole = MarketBenchmarkRole.BROAD_MARKET
    observed_at: datetime | None = None
    freshness: MarketContextFreshnessState = MarketContextFreshnessState.UNKNOWN
    data_quality: MarketContextDataQualityState = MarketContextDataQualityState.UNKNOWN
    return_1d_pct: float | None = None
    return_5d_pct: float | None = None
    intraday_return_pct: float | None = None
    drawdown_5d_pct: float | None = None
    realized_volatility_5d_pct: float | None = None
    implied_volatility_pct: float | None = None
    trend_score: float | None = Field(default=None, ge=-1.0, le=1.0)
    momentum_score: float | None = Field(default=None, ge=-1.0, le=1.0)
    reason_codes: tuple[str, ...] = ()
    evidence: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", mode="before")
    @classmethod
    def _normalize_symbol(cls, value: Any) -> str:
        rendered = str(value or "").strip().upper()
        if not rendered:
            raise ValueError("market benchmark symbol is required")
        return rendered

    @field_validator("role", "freshness", "data_quality", mode="before")
    @classmethod
    def _normalize_enum(cls, value: Any) -> str:
        return _normalize_enum_text(value)

    @field_validator("reason_codes", mode="before")
    @classmethod
    def _normalize_reasons(cls, value: Any) -> tuple[str, ...]:
        return _normalize_text_tuple(value)


class RegimeSnapshot(DomainModel):
    regime_label: MarketRegimeLabel = MarketRegimeLabel.UNKNOWN
    risk_posture: MarketRiskPosture = MarketRiskPosture.UNKNOWN
    trend_strength: MarketTrendStrength = MarketTrendStrength.UNKNOWN
    volatility_state: MarketVolatilityState = MarketVolatilityState.UNKNOWN
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    observed_at: datetime
    expires_at: datetime
    benchmark_symbols: tuple[str, ...] = ()
    data_quality: MarketContextDataQuality = Field(default_factory=MarketContextDataQuality)
    fidelity: tuple[MarketContextFidelityLabel, ...] = ()
    reason_codes: tuple[str, ...] = ()
    metrics: dict[str, Any] = Field(default_factory=dict)
    source_evidence: dict[str, Any] = Field(default_factory=dict)

    @field_validator("regime_label", "risk_posture", "trend_strength", "volatility_state", mode="before")
    @classmethod
    def _normalize_enum(cls, value: Any) -> str:
        return _normalize_enum_text(value)

    @field_validator("benchmark_symbols", mode="before")
    @classmethod
    def _normalize_benchmark_symbols(cls, value: Any) -> tuple[str, ...]:
        return _normalize_symbol_tuple(value)

    @field_validator("fidelity", mode="before")
    @classmethod
    def _normalize_fidelity(cls, value: Any) -> tuple[Any, ...]:
        return tuple(_normalize_text_tuple(value))

    @field_validator("reason_codes", mode="before")
    @classmethod
    def _normalize_reasons(cls, value: Any) -> tuple[str, ...]:
        return _normalize_text_tuple(value)

    @model_validator(mode="after")
    def _validate_window(self) -> RegimeSnapshot:
        if self.expires_at <= self.observed_at:
            raise ValueError("regime snapshot expires_at must be after observed_at")
        return self


class MarketContextSnapshot(DomainModel):
    snapshot_id: str | None = None
    scope: str = "global_market"
    observed_at: datetime
    expires_at: datetime
    generated_at: datetime | None = None
    context_version: int = Field(default=1, ge=1)
    config_hash: str | None = None
    regime: RegimeSnapshot
    benchmark_evidence: tuple[MarketBenchmarkEvidence, ...] = ()
    data_quality: MarketContextDataQuality = Field(default_factory=MarketContextDataQuality)
    fidelity: tuple[MarketContextFidelityLabel, ...] = ()
    source_evidence: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("snapshot_id", "config_hash", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return _normalize_optional_text(value)

    @field_validator("scope", mode="before")
    @classmethod
    def _normalize_scope(cls, value: Any) -> str:
        rendered = str(value or "").strip()
        if not rendered:
            raise ValueError("market context scope is required")
        return rendered

    @field_validator("fidelity", mode="before")
    @classmethod
    def _normalize_fidelity(cls, value: Any) -> tuple[Any, ...]:
        return tuple(_normalize_text_tuple(value))

    @model_validator(mode="after")
    def _validate_snapshot(self) -> MarketContextSnapshot:
        if self.expires_at <= self.observed_at:
            raise ValueError("market context snapshot expires_at must be after observed_at")
        symbols = [item.symbol for item in self.benchmark_evidence]
        if len(symbols) != len(set(symbols)):
            raise ValueError("market context benchmark_evidence contains duplicate symbols")
        return self

    @property
    def benchmark_symbols(self) -> tuple[str, ...]:
        return tuple(item.symbol for item in self.benchmark_evidence)


__all__ = [
    "MarketBenchmarkEvidence",
    "MarketBenchmarkRole",
    "MarketContextDataQuality",
    "MarketContextDataQualityState",
    "MarketContextFidelityLabel",
    "MarketContextFreshnessState",
    "MarketContextSnapshot",
    "MarketRegimeLabel",
    "MarketRiskPosture",
    "MarketTrendStrength",
    "MarketVolatilityState",
    "RegimeSnapshot",
]
