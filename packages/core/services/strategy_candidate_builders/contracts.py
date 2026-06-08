from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from core.services.trading_engine.kernel import EngineEvidence, EnginePayload, EnginePolicy, EngineSummary

if TYPE_CHECKING:
    from core.domain.models import SymbolMarketSlice


class CandidateDiagnosticStatus(StrEnum):
    CANDIDATE_AVAILABLE = "candidate_available"
    RUNTIME_REJECTED = "runtime_rejected"
    RANKING_REJECTED = "ranking_rejected"
    POSTPROCESS_REJECTED = "postprocess_rejected"
    DATA_UNAVAILABLE = "data_unavailable"
    NO_RAW_CANDIDATES = "no_raw_candidates"
    FAILED = "failed"


class CandidateMarketSide(StrEnum):
    CALL = "call"
    PUT = "put"
    BOTH = "both"


@dataclass(frozen=True)
class CandidateBuildLimits:
    total: int = 10
    per_symbol: int = 1

    def __post_init__(self) -> None:
        if self.total < 1:
            raise ValueError("CandidateBuildLimits.total must be >= 1")
        if self.per_symbol < 1:
            raise ValueError("CandidateBuildLimits.per_symbol must be >= 1")


@dataclass(frozen=True)
class CandidateBuildRun:
    run_id: str
    label: str
    observed_at: datetime
    history_enabled: bool = True
    metadata: EnginePayload = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyCandidateBuildSettings:
    trading_strategy_id: str
    trade_structure: str
    builder_key: str
    build_profile: str
    option_side: CandidateMarketSide
    dte_min: int | None = None
    dte_max: int | None = None
    short_delta_min: float | None = None
    short_delta_max: float | None = None
    short_delta_target: float | None = None
    width_points: tuple[float, ...] = ()
    min_open_interest: int | None = None
    max_leg_spread_pct_mid: float | None = None
    min_return_on_risk: float | None = None
    min_fill_ratio: float | None = None
    min_short_vs_expected_move_ratio: float | None = None
    min_breakeven_vs_expected_move_ratio: float | None = None
    max_quote_age_seconds: int | None = None
    ranking_policy: EnginePolicy = field(default_factory=dict)
    builder_params: EnginePolicy = field(default_factory=dict)
    liquidity_rules: EnginePolicy = field(default_factory=dict)
    risk_defaults: EnginePolicy = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.trading_strategy_id:
            raise ValueError("StrategyCandidateBuildSettings.trading_strategy_id is required")
        if not self.trade_structure:
            raise ValueError("StrategyCandidateBuildSettings.trade_structure is required")
        if not self.builder_key:
            raise ValueError("StrategyCandidateBuildSettings.builder_key is required")
        if not self.build_profile:
            raise ValueError("StrategyCandidateBuildSettings.build_profile is required")
        if self.dte_min is not None and self.dte_min < 0:
            raise ValueError("StrategyCandidateBuildSettings.dte_min must be >= 0")
        if self.dte_max is not None and self.dte_max < 0:
            raise ValueError("StrategyCandidateBuildSettings.dte_max must be >= 0")
        if self.dte_min is not None and self.dte_max is not None and self.dte_max < self.dte_min:
            raise ValueError("StrategyCandidateBuildSettings requires dte_min <= dte_max")


@dataclass(frozen=True)
class CandidateMarketDataSettings:
    option_feed: str = "opra"
    stock_feed: str = "sip"
    greeks_source: str = "auto"
    data_base_url: str | None = None


@dataclass(frozen=True)
class CandidateMarketDataRequest:
    symbol: str
    settings: StrategyCandidateBuildSettings
    market_data: CandidateMarketDataSettings

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("CandidateMarketDataRequest.symbol is required")


@dataclass(frozen=True)
class CandidateMarketDataSnapshot:
    symbol: str
    market_slice: SymbolMarketSlice
    quoted_contract_count: int
    alpaca_delta_contract_count: int
    delta_contract_count: int
    local_delta_contract_count: int
    metadata: EnginePayload = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyCandidateBuildRequest:
    symbols: tuple[str, ...]
    settings: StrategyCandidateBuildSettings
    limits: CandidateBuildLimits
    run: CandidateBuildRun
    market_data: CandidateMarketDataSettings = field(default_factory=CandidateMarketDataSettings)
    source_evidence: EngineEvidence = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized = tuple(dict.fromkeys(str(symbol).upper().strip() for symbol in self.symbols if str(symbol or "").strip()))
        object.__setattr__(self, "symbols", normalized)


@dataclass(frozen=True)
class CandidateDiagnostic:
    underlying_symbol: str
    status: CandidateDiagnosticStatus
    observed_at: datetime
    spot_price: float | None = None
    expiration_count: int = 0
    contract_count: int = 0
    snapshot_count: int = 0
    raw_candidate_count: int = 0
    postprocess_candidate_count: int = 0
    runtime_candidate_count: int = 0
    returned_candidate_count: int = 0
    setup: EnginePayload = field(default_factory=dict)
    market_data: EnginePayload = field(default_factory=dict)
    rejection_counts: Mapping[str, EnginePayload] = field(default_factory=dict)
    ranking_gate: EnginePayload = field(default_factory=dict)
    examples: EnginePayload = field(default_factory=dict)
    evidence: EngineEvidence = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateBuildFailure:
    symbol: str
    stage: str
    error: str
    metadata: EnginePayload = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyCandidateBuildOutcome:
    candidates_by_symbol: Mapping[str, tuple[EnginePayload, ...]]
    diagnostics: tuple[CandidateDiagnostic, ...] = ()
    failures: tuple[CandidateBuildFailure, ...] = ()
    summary: EngineSummary = field(default_factory=dict)

    @property
    def candidates(self) -> tuple[EnginePayload, ...]:
        return tuple(candidate for rows in self.candidates_by_symbol.values() for candidate in rows)


class StrategyCandidateMarketDataProvider(Protocol):
    def build_market_snapshot(
        self,
        request: CandidateMarketDataRequest,
    ) -> CandidateMarketDataSnapshot: ...


class StrategyCandidateBuilder(Protocol):
    def build_candidates(
        self,
        request: StrategyCandidateBuildRequest,
        *,
        market_snapshots: Mapping[str, CandidateMarketDataSnapshot],
        calendar_resolver: object,
    ) -> StrategyCandidateBuildOutcome: ...


__all__ = [
    "CandidateBuildFailure",
    "CandidateBuildLimits",
    "CandidateBuildRun",
    "CandidateDiagnostic",
    "CandidateDiagnosticStatus",
    "CandidateMarketDataRequest",
    "CandidateMarketDataSettings",
    "CandidateMarketDataSnapshot",
    "CandidateMarketSide",
    "StrategyCandidateBuildOutcome",
    "StrategyCandidateBuildRequest",
    "StrategyCandidateBuildSettings",
    "StrategyCandidateBuilder",
    "StrategyCandidateMarketDataProvider",
]
