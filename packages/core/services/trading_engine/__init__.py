"""Spreads trading engine boundaries."""

from .capture_targets import refresh_engine_capture_targets
from .data_runtime import DataEngine
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
from .exit_runtime import ExitEngine
from .kernel import EngineComponentRole, EngineContext, EngineRunRef
from .market_context import (
    MarketBenchmarkEvidence,
    MarketBenchmarkRole,
    MarketContextDataQuality,
    MarketContextDataQualityState,
    MarketContextFidelityLabel,
    MarketContextFreshnessState,
    MarketContextSnapshot,
    MarketRegimeLabel,
    MarketRiskPosture,
    MarketTrendStrength,
    MarketVolatilityState,
    RegimeSnapshot,
)
from .market_context_runtime import (
    DEFAULT_MARKET_CONTEXT_BENCHMARK_SYMBOLS,
    DEFAULT_MARKET_CONTEXT_TTL_SECONDS,
    MARKET_CONTEXT_VERSION,
    MarketContextBenchmarkFact,
    MarketContextEngine,
    MarketContextRequest,
)
from .portfolio_runtime import PortfolioEngine

__all__ = [
    "DataEngine",
    "EngineComponentRole",
    "EngineContext",
    "EngineRunRef",
    "EntrySelectionEngine",
    "EntrySelectionResult",
    "FEATURE_SNAPSHOT_BUILDER_REGISTRY",
    "FeatureSnapshotBuilder",
    "FeatureSnapshotBuilderKey",
    "DEFAULT_MARKET_CONTEXT_BENCHMARK_SYMBOLS",
    "DEFAULT_MARKET_CONTEXT_TTL_SECONDS",
    "MARKET_CONTEXT_VERSION",
    "MarketBenchmarkEvidence",
    "MarketBenchmarkRole",
    "MarketContextBenchmarkFact",
    "MarketContextDataQuality",
    "MarketContextDataQualityState",
    "MarketContextEngine",
    "MarketContextFidelityLabel",
    "MarketContextFreshnessState",
    "MarketContextRequest",
    "MarketContextSnapshot",
    "MarketRegimeLabel",
    "MarketRiskPosture",
    "MarketTrendStrength",
    "MarketVolatilityState",
    "PortfolioEngine",
    "RegimeSnapshot",
    "ExitEngine",
    "build_feature_snapshots_for_strategy",
    "build_momentum_long_call_feature_snapshots",
    "register_feature_snapshot_builder",
    "refresh_engine_capture_targets",
    "resolve_feature_snapshot_builder",
]
