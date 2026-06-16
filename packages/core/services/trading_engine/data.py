from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from .kernel import EngineEvidence, EnginePayload, EngineSummary, EngineRunRef

if TYPE_CHECKING:
    from core.services.trading_strategy_runtime_models import EntryRuntime


@dataclass(frozen=True)
class TickerSourceFallback:
    universe_ref: str | None = None


@dataclass(frozen=True)
class TickerSourceSpec:
    source_type: str
    ref: str
    max_age_seconds: int | None = None
    max_symbols: int | None = None
    stale_behavior: str = "skip"
    fallback: TickerSourceFallback = field(default_factory=TickerSourceFallback)


@dataclass(frozen=True)
class ResolvedTickerSet:
    symbols: tuple[str, ...]
    source: TickerSourceSpec
    resolved_at: datetime
    ticker_source_run_id: str | None = None
    reason_codes: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    evidence: EngineEvidence = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateBuildRequest:
    run_ref: EngineRunRef
    trading_strategy_id: str
    trade_structure: str
    symbols: tuple[str, ...]
    entry_runtime: EntryRuntime | None = None
    candidate_limit: int | None = None
    per_symbol_top: int = 1
    greeks_source: str = "auto"
    source_evidence: EngineEvidence = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateBuildResult:
    run_ref: EngineRunRef
    candidate_run_id: str
    candidates: tuple[EnginePayload, ...]
    diagnostics: tuple[EnginePayload, ...] = ()
    failures: tuple[EnginePayload, ...] = ()
    summary: EngineSummary = field(default_factory=dict)


@dataclass(frozen=True)
class CaptureTargetRequest:
    owner_type: str
    owner_id: str
    symbols: tuple[str, ...]
    priority: int
    ttl_seconds: int
    reason: str
    metadata: EnginePayload = field(default_factory=dict)


@dataclass(frozen=True)
class CaptureTargetDeclaration:
    status: str
    request_count: int
    target_counts: Mapping[str, int] = field(default_factory=dict)
    reason: str | None = None
