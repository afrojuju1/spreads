from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from .kernel import EngineRunRef


@dataclass(frozen=True)
class TickerSourceSpec:
    source_type: str
    ref: str
    max_age_seconds: int | None = None
    max_symbols: int | None = None
    stale_behavior: str = "skip"
    fallback: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedTickerSet:
    symbols: tuple[str, ...]
    source: TickerSourceSpec
    resolved_at: datetime
    source_run_id: str | None = None
    reason_codes: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateBuildRequest:
    run_ref: EngineRunRef
    trading_strategy_id: str
    trade_structure: str
    symbols: tuple[str, ...]
    build_policy: Mapping[str, Any] = field(default_factory=dict)
    source_evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateBuildResult:
    run_ref: EngineRunRef
    candidate_run_id: str
    candidates: tuple[Mapping[str, Any], ...]
    failures: tuple[Mapping[str, Any], ...] = ()
    summary: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CaptureTargetRequest:
    owner_type: str
    owner_id: str
    symbols: tuple[str, ...]
    priority: int
    ttl_seconds: int
    reason: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


class DataEngine(Protocol):
    def resolve_tickers(
        self,
        *,
        source: TickerSourceSpec,
        as_of: datetime,
    ) -> ResolvedTickerSet:
        ...

    def build_trade_candidates(
        self,
        request: CandidateBuildRequest,
    ) -> CandidateBuildResult:
        ...

    def declare_capture_targets(
        self,
        requests: Sequence[CaptureTargetRequest],
    ) -> Mapping[str, Any]:
        ...
