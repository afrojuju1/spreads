from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal, TypeAlias


MarketIntelDepth: TypeAlias = Literal["quick", "standard", "deep"]
MarketIntelRunStatus: TypeAlias = Literal[
    "created",
    "fetching_sources",
    "extracting_evidence",
    "drafting_thesis",
    "reviewing",
    "completed",
    "completed_with_warnings",
    "failed",
]
SourceType: TypeAlias = Literal[
    "sec",
    "ir",
    "market",
    "news",
    "calendar",
    "valuation_context",
]
ClaimType: TypeAlias = Literal[
    "fact",
    "derived_metric",
    "inference",
    "forecast",
    "soft_signal",
]
SupportsOrRefutes: TypeAlias = Literal["supports", "refutes", "mixed", "neutral"]
ModelProfile: TypeAlias = Literal[
    "fast_structured",
    "standard_reasoning",
    "deep_reasoning",
    "long_context",
    "embedding",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_as_of(value: str | None) -> date:
    if value is None or not str(value).strip():
        return utc_now().date()
    return date.fromisoformat(str(value).strip())


def _payload_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {key: _payload_value(row) for key, row in asdict(value).items()}
    if isinstance(value, tuple):
        return [_payload_value(row) for row in value]
    if isinstance(value, list):
        return [_payload_value(row) for row in value]
    if isinstance(value, dict):
        return {str(key): _payload_value(row) for key, row in value.items()}
    return value


@dataclass(frozen=True)
class MarketIntelRequest:
    ticker: str
    as_of: date
    output_root: Path
    sources: tuple[SourceType, ...] = ("sec", "market")
    depth: MarketIntelDepth = "standard"
    no_llm: bool = False
    refresh: bool = False

    def to_payload(self) -> dict[str, Any]:
        return _payload_value(asdict(self))


@dataclass(frozen=True)
class MarketIntelRun:
    run_id: str
    ticker: str
    as_of: date
    status: MarketIntelRunStatus
    config_hash: str
    started_at: datetime
    output_root: Path
    run_dir: Path
    completed_at: datetime | None = None
    warnings: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return _payload_value(asdict(self))


@dataclass(frozen=True)
class SourceArtifact:
    artifact_id: str
    run_id: str
    ticker: str
    source_type: SourceType
    source_name: str
    source_url: str | None
    fetched_at: datetime | None
    observed_at: datetime | None
    available_at: datetime | None
    raw_path: Path | None
    normalized_path: Path | None
    content_hash: str | None
    trust_tier: int
    notes: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return _payload_value(asdict(self))


@dataclass(frozen=True)
class EvidenceItem:
    evidence_id: str
    run_id: str
    ticker: str
    artifact_id: str | None
    claim_type: ClaimType
    claim_text: str
    normalized_value: Any | None
    observed_at: datetime | None
    available_at: datetime | None
    supports_or_refutes: SupportsOrRefutes
    source_rank: int | None
    extraction_method: str
    extraction_confidence: float
    final_confidence: float
    conflicts: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return _payload_value(asdict(self))


@dataclass(frozen=True)
class ReviewFinding:
    finding_id: str
    run_id: str
    severity: Literal["blocker", "major", "minor", "note"]
    finding_type: str
    claim_ref: str | None
    evidence_refs: tuple[str, ...] = ()
    note: str = ""
    required_action: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return _payload_value(asdict(self))


@dataclass(frozen=True)
class ThesisArtifact:
    run_id: str
    ticker: str
    as_of: date
    setup: str | None = None
    why_now: str | None = None
    variant_view: str | None = None
    core_evidence: tuple[str, ...] = ()
    catalysts: tuple[str, ...] = ()
    base_case: str | None = None
    bull_case: str | None = None
    bear_case: str | None = None
    expected_window: str | None = None
    expected_return: str | None = None
    invalidation: str | None = None
    portfolio_fit: str | None = None
    thesis_quality: float | None = None
    evidence_quality: float | None = None
    catalyst_quality: float | None = None
    market_confirmation: float | None = None
    portfolio_fit_score: float | None = None
    confidence: float | None = None
    skeptic_notes: tuple[str, ...] = ()
    source_pack: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return _payload_value(asdict(self))


@dataclass(frozen=True)
class ModelCallRecord:
    call_id: str
    run_id: str | None
    agent_id: str
    backend: Literal["ollama"]
    profile: ModelProfile
    model: str | None
    started_at: datetime
    completed_at: datetime | None
    elapsed_seconds: float | None
    status: Literal["completed", "failed", "skipped"]
    retry_count: int = 0
    token_estimate: int | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return _payload_value(asdict(self))
