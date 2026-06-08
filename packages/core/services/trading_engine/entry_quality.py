from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from typing import Literal, Protocol, TypeAlias

from .kernel import EngineEvidence, EngineMetadata, EnginePayload, EnginePolicy, EngineRunRef, EngineSummary

EntryQualityStatus: TypeAlias = Literal["pass", "watch", "block"]


class EntryQualityStageName(StrEnum):
    SOURCE_PREFLIGHT = "source_preflight"
    UNDERLYING_SETUP = "underlying_setup"
    CHAIN_VIABILITY = "chain_viability"
    CONTRACT_FIT = "contract_fit"
    PREMIUM_QUALITY = "premium_quality"
    SELECTION = "selection"
    ADMISSION = "admission"


class FilterResultStatus(StrEnum):
    PASS = "pass"
    WATCH = "watch"
    BLOCK = "block"


def _normalized_text(value: object) -> str:
    return str(value or "").strip()


def _normalized_stage_name(value: EntryQualityStageName | str) -> EntryQualityStageName:
    if isinstance(value, EntryQualityStageName):
        return value
    rendered = _normalized_text(value)
    if not rendered:
        raise ValueError("entry quality stage is required")
    try:
        return EntryQualityStageName(rendered)
    except ValueError as exc:
        raise ValueError(f"Unsupported entry quality stage: {rendered}") from exc


def _normalized_status(value: FilterResultStatus | str) -> FilterResultStatus:
    if isinstance(value, FilterResultStatus):
        return value
    rendered = _normalized_text(value)
    if not rendered:
        raise ValueError("filter result status is required")
    try:
        return FilterResultStatus(rendered)
    except ValueError as exc:
        raise ValueError(f"Unsupported filter result status: {rendered}") from exc


def _normalized_text_tuple(values: Sequence[object] | None) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values or ():
        rendered = _normalized_text(value)
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        normalized.append(rendered)
    return tuple(normalized)


@dataclass(frozen=True)
class FeatureSnapshot:
    symbol: str
    observed_at: datetime | None = None
    source: EngineEvidence = field(default_factory=dict)
    underlying: EngineEvidence = field(default_factory=dict)
    chain: EngineEvidence = field(default_factory=dict)
    premium: EngineEvidence = field(default_factory=dict)
    candidate: EnginePayload | None = None
    metadata: EngineMetadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        symbol = _normalized_text(self.symbol).upper()
        if not symbol:
            raise ValueError("FeatureSnapshot.symbol is required")
        object.__setattr__(self, "symbol", symbol)

    def with_candidate(self, candidate: Mapping[str, object]) -> FeatureSnapshot:
        return replace(self, candidate=dict(candidate))

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "observed_at": None if self.observed_at is None else self.observed_at.isoformat().replace("+00:00", "Z"),
            "source": dict(self.source),
            "underlying": dict(self.underlying),
            "chain": dict(self.chain),
            "premium": dict(self.premium),
            "candidate": None if self.candidate is None else dict(self.candidate),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class EntryQualityContext:
    trading_strategy_id: str
    trade_structure: str
    quality_profile_id: str
    run_ref: EngineRunRef | None = None
    routine: str = "entry"
    policy: EnginePolicy = field(default_factory=dict)
    metadata: EngineMetadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _normalized_text(self.trading_strategy_id):
            raise ValueError("EntryQualityContext.trading_strategy_id is required")
        if not _normalized_text(self.trade_structure):
            raise ValueError("EntryQualityContext.trade_structure is required")
        if not _normalized_text(self.quality_profile_id):
            raise ValueError("EntryQualityContext.quality_profile_id is required")
        if not _normalized_text(self.routine):
            raise ValueError("EntryQualityContext.routine is required")


@dataclass(frozen=True)
class FilterResult:
    filter_id: str
    stage: EntryQualityStageName | str
    status: FilterResultStatus | str
    reason_codes: tuple[str, ...] = ()
    metrics: EngineSummary = field(default_factory=dict)
    thresholds: EnginePolicy = field(default_factory=dict)
    message: str = ""

    def __post_init__(self) -> None:
        filter_id = _normalized_text(self.filter_id)
        if not filter_id:
            raise ValueError("FilterResult.filter_id is required")
        object.__setattr__(self, "filter_id", filter_id)
        object.__setattr__(self, "stage", _normalized_stage_name(self.stage))
        object.__setattr__(self, "status", _normalized_status(self.status))
        object.__setattr__(self, "reason_codes", _normalized_text_tuple(self.reason_codes))
        object.__setattr__(self, "message", _normalized_text(self.message))

    @property
    def passed(self) -> bool:
        return self.status == FilterResultStatus.PASS

    @property
    def blocked(self) -> bool:
        return self.status == FilterResultStatus.BLOCK

    def as_dict(self) -> dict[str, object]:
        return {
            "filter_id": self.filter_id,
            "stage": self.stage.value,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "metrics": dict(self.metrics),
            "thresholds": dict(self.thresholds),
            "message": self.message,
        }


class EntryFilter(Protocol):
    filter_id: str
    stage: EntryQualityStageName

    def evaluate(
        self,
        context: EntryQualityContext,
        snapshot: FeatureSnapshot,
        candidate: Mapping[str, object] | None = None,
    ) -> FilterResult: ...


@dataclass(frozen=True)
class EntryFilterRef:
    filter_id: str
    stage: EntryQualityStageName | str
    thresholds: EnginePolicy = field(default_factory=dict)
    metadata: EngineMetadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        filter_id = _normalized_text(self.filter_id)
        if not filter_id:
            raise ValueError("EntryFilterRef.filter_id is required")
        object.__setattr__(self, "filter_id", filter_id)
        object.__setattr__(self, "stage", _normalized_stage_name(self.stage))

    def as_dict(self) -> dict[str, object]:
        return {
            "filter_id": self.filter_id,
            "stage": self.stage.value,
            "thresholds": dict(self.thresholds),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class EntryQualityStage:
    stage: EntryQualityStageName | str
    filters: tuple[EntryFilterRef, ...]
    required: bool = True
    metadata: EngineMetadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        stage = _normalized_stage_name(self.stage)
        if not self.filters:
            raise ValueError(f"EntryQualityStage {stage.value} requires at least one filter")
        normalized_filters: list[EntryFilterRef] = []
        seen: set[str] = set()
        for item in self.filters:
            filter_ref = item if isinstance(item, EntryFilterRef) else EntryFilterRef(filter_id=str(item), stage=stage)
            if filter_ref.stage != stage:
                raise ValueError(f"EntryQualityStage {stage.value} contains filter for stage {filter_ref.stage.value}")
            if filter_ref.filter_id in seen:
                raise ValueError(f"Duplicate entry quality filter in stage {stage.value}: {filter_ref.filter_id}")
            seen.add(filter_ref.filter_id)
            normalized_filters.append(filter_ref)
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "filters", tuple(normalized_filters))

    @property
    def filter_ids(self) -> tuple[str, ...]:
        return tuple(filter_ref.filter_id for filter_ref in self.filters)

    def as_dict(self) -> dict[str, object]:
        return {
            "stage": self.stage.value,
            "required": self.required,
            "filters": [filter_ref.as_dict() for filter_ref in self.filters],
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class EntryQualityProfile:
    profile_id: str
    trade_structure: str
    stages: tuple[EntryQualityStage, ...]
    version: int = 1
    description: str = ""
    metadata: EngineMetadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        profile_id = _normalized_text(self.profile_id)
        trade_structure = _normalized_text(self.trade_structure)
        if not profile_id:
            raise ValueError("EntryQualityProfile.profile_id is required")
        if not trade_structure:
            raise ValueError("EntryQualityProfile.trade_structure is required")
        if not self.stages:
            raise ValueError("EntryQualityProfile requires at least one stage")
        seen: set[EntryQualityStageName] = set()
        for stage in self.stages:
            if stage.stage in seen:
                raise ValueError(f"Duplicate entry quality stage in profile {profile_id}: {stage.stage.value}")
            seen.add(stage.stage)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "trade_structure", trade_structure)
        object.__setattr__(self, "description", _normalized_text(self.description))

    @property
    def stage_names(self) -> tuple[str, ...]:
        return tuple(stage.stage.value for stage in self.stages)

    @property
    def filter_ids(self) -> tuple[str, ...]:
        return tuple(filter_id for stage in self.stages for filter_id in stage.filter_ids)

    def stage(self, stage: EntryQualityStageName | str) -> EntryQualityStage | None:
        stage_name = _normalized_stage_name(stage)
        return next((item for item in self.stages if item.stage == stage_name), None)

    def as_dict(self) -> dict[str, object]:
        return {
            "profile_id": self.profile_id,
            "trade_structure": self.trade_structure,
            "version": self.version,
            "description": self.description,
            "stages": [stage.as_dict() for stage in self.stages],
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class EntryQualityWaterfall:
    profile_id: str
    results: tuple[FilterResult, ...] = ()
    metadata: EngineMetadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        profile_id = _normalized_text(self.profile_id)
        if not profile_id:
            raise ValueError("EntryQualityWaterfall.profile_id is required")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "results", tuple(self.results))

    @property
    def blocked(self) -> bool:
        return any(result.blocked for result in self.results)

    def add_result(self, result: FilterResult) -> EntryQualityWaterfall:
        return replace(self, results=(*self.results, result))

    def stage_counts(self) -> dict[str, dict[str, int]]:
        counts: dict[str, dict[str, int]] = {}
        for result in self.results:
            stage_counts = counts.setdefault(result.stage.value, {})
            status = result.status.value
            stage_counts[status] = stage_counts.get(status, 0) + 1
        return counts

    def as_dict(self) -> dict[str, object]:
        return {
            "profile_id": self.profile_id,
            "blocked": self.blocked,
            "stage_counts": self.stage_counts(),
            "results": [result.as_dict() for result in self.results],
            "metadata": dict(self.metadata),
        }


def _filter_ref(filter_id: str, stage: EntryQualityStageName) -> EntryFilterRef:
    return EntryFilterRef(filter_id=filter_id, stage=stage)


def _stage(stage: EntryQualityStageName, filter_ids: Sequence[str]) -> EntryQualityStage:
    return EntryQualityStage(
        stage=stage,
        filters=tuple(_filter_ref(filter_id, stage) for filter_id in filter_ids),
    )


MOMENTUM_LONG_CALL_PROFILE_ID = "momentum_long_call_v1"

MOMENTUM_LONG_CALL_V1 = EntryQualityProfile(
    profile_id=MOMENTUM_LONG_CALL_PROFILE_ID,
    trade_structure="long_call",
    description="Momentum long-call entry quality profile.",
    stages=(
        _stage(
            EntryQualityStageName.SOURCE_PREFLIGHT,
            ("source_is_fresh",),
        ),
        _stage(
            EntryQualityStageName.UNDERLYING_SETUP,
            ("setup_context_usable",),
        ),
        _stage(
            EntryQualityStageName.CHAIN_VIABILITY,
            (
                "chain_data_available",
                "option_snapshots_available",
                "greeks_available",
            ),
        ),
        _stage(
            EntryQualityStageName.CONTRACT_FIT,
            (
                "strategy_family_matches",
                "dte_in_range",
                "delta_in_range",
                "entry_recipe_passed",
            ),
        ),
        _stage(
            EntryQualityStageName.PREMIUM_QUALITY,
            (
                "open_interest_ok",
                "relative_spread_ok",
                "return_on_risk_ok",
                "ranking_policy_passed",
            ),
        ),
        _stage(
            EntryQualityStageName.SELECTION,
            (
                "selection_score_ok",
                "selection_live_ready",
            ),
        ),
    ),
)

ENTRY_QUALITY_PROFILE_REGISTRY: dict[str, EntryQualityProfile] = {
    MOMENTUM_LONG_CALL_V1.profile_id: MOMENTUM_LONG_CALL_V1,
}


def resolve_entry_quality_profile(profile_id: str) -> EntryQualityProfile:
    normalized = _normalized_text(profile_id)
    if not normalized:
        raise ValueError("entry quality profile id is required")
    profile = ENTRY_QUALITY_PROFILE_REGISTRY.get(normalized)
    if profile is None:
        raise KeyError(f"Unknown entry quality profile: {normalized}")
    return profile


def list_entry_quality_profiles() -> tuple[EntryQualityProfile, ...]:
    return tuple(ENTRY_QUALITY_PROFILE_REGISTRY[key] for key in sorted(ENTRY_QUALITY_PROFILE_REGISTRY))


__all__ = [
    "ENTRY_QUALITY_PROFILE_REGISTRY",
    "EntryFilter",
    "EntryFilterRef",
    "EntryQualityContext",
    "EntryQualityProfile",
    "EntryQualityStage",
    "EntryQualityStageName",
    "EntryQualityStatus",
    "EntryQualityWaterfall",
    "FeatureSnapshot",
    "FilterResult",
    "FilterResultStatus",
    "MOMENTUM_LONG_CALL_PROFILE_ID",
    "MOMENTUM_LONG_CALL_V1",
    "list_entry_quality_profiles",
    "resolve_entry_quality_profile",
]
