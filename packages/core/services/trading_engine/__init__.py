"""Nautilus-shaped trading engine boundaries for Spreads.

This package names the engine roles first; runtime wiring moves here bead by bead.
"""

from .data import DataEngine
from .capture_targets import refresh_engine_capture_targets
from .data_runtime import PostgresDataEngine
from .entry_quality import (
    ENTRY_QUALITY_PROFILE_REGISTRY,
    MOMENTUM_LONG_CALL_PROFILE_ID,
    MOMENTUM_LONG_CALL_V1,
    EntryFilter,
    EntryFilterRef,
    EntryQualityContext,
    EntryQualityProfile,
    EntryQualityStage,
    EntryQualityStageName,
    EntryQualityStatus,
    EntryQualityWaterfall,
    FeatureSnapshot,
    FilterResult,
    FilterResultStatus,
    list_entry_quality_profiles,
    resolve_entry_quality_profile,
)
from .entry_quality_evidence import EntryQualityAnalysis, build_entry_quality_analysis
from .entry_quality_pipeline import (
    POST_SELECTION_ENTRY_QUALITY_STAGES,
    PRE_SELECTION_ENTRY_QUALITY_STAGES,
    evaluate_entry_quality_snapshot,
    evaluate_momentum_long_call_snapshot,
)
from .entry_selection import EntrySelectionEngine, EntrySelectionResult
from .feature_snapshots import (
    FEATURE_SNAPSHOT_BUILDER_REGISTRY,
    FeatureSnapshotBuilder,
    FeatureSnapshotBuilderKey,
    build_feature_snapshots_for_strategy,
    build_momentum_long_call_feature_snapshots,
    register_feature_snapshot_builder,
    resolve_feature_snapshot_builder,
)
from .execution import ExecutionEngine
from .kernel import EngineComponentRole, EngineContext, EngineRunRef
from .portfolio import PortfolioEngine
from .portfolio_runtime import PostgresPortfolioEngine
from .risk import RiskEngine
from .strategy import StrategyEngine

__all__ = [
    "DataEngine",
    "ENTRY_QUALITY_PROFILE_REGISTRY",
    "EngineComponentRole",
    "EngineContext",
    "EngineRunRef",
    "EntryQualityAnalysis",
    "EntryFilter",
    "EntryFilterRef",
    "EntryQualityContext",
    "EntryQualityProfile",
    "EntryQualityStage",
    "EntryQualityStageName",
    "EntryQualityStatus",
    "EntryQualityWaterfall",
    "EntrySelectionEngine",
    "EntrySelectionResult",
    "ExecutionEngine",
    "FEATURE_SNAPSHOT_BUILDER_REGISTRY",
    "FeatureSnapshot",
    "FeatureSnapshotBuilder",
    "FeatureSnapshotBuilderKey",
    "FilterResult",
    "FilterResultStatus",
    "MOMENTUM_LONG_CALL_PROFILE_ID",
    "MOMENTUM_LONG_CALL_V1",
    "POST_SELECTION_ENTRY_QUALITY_STAGES",
    "PRE_SELECTION_ENTRY_QUALITY_STAGES",
    "PortfolioEngine",
    "PostgresDataEngine",
    "PostgresPortfolioEngine",
    "RiskEngine",
    "StrategyEngine",
    "build_entry_quality_analysis",
    "build_feature_snapshots_for_strategy",
    "build_momentum_long_call_feature_snapshots",
    "evaluate_entry_quality_snapshot",
    "evaluate_momentum_long_call_snapshot",
    "list_entry_quality_profiles",
    "register_feature_snapshot_builder",
    "refresh_engine_capture_targets",
    "resolve_feature_snapshot_builder",
    "resolve_entry_quality_profile",
]
