from __future__ import annotations

from core.storage.base import RepositoryBase
from core.storage.engine_facts.candidates import EngineFactCandidateMixin
from core.storage.engine_facts.feature_snapshots import EngineFactFeatureSnapshotMixin
from core.storage.engine_facts.lifecycle import EngineFactLifecycleMixin
from core.storage.engine_facts.market_context import EngineFactMarketContextMixin
from core.storage.engine_facts.signals import EngineFactSignalMixin
from core.storage.engine_facts.sources import EngineFactSourceMixin


class EngineFactRepository(
    EngineFactMarketContextMixin,
    EngineFactSourceMixin,
    EngineFactCandidateMixin,
    EngineFactFeatureSnapshotMixin,
    EngineFactSignalMixin,
    EngineFactLifecycleMixin,
    RepositoryBase,
):
    def schema_ready(self) -> bool:
        return self.schema_has_tables(
            "ticker_source_runs",
            "ticker_source_observations",
            "ticker_source_state",
            "candidate_runs",
            "candidate_symbol_diagnostics",
            "trade_candidates",
            "trade_signals",
            "trade_decisions",
            "trade_execution_intents",
            "trade_admissions",
        )

    def close_lifecycle_schema_ready(self) -> bool:
        return self.schema_has_tables(
            "trade_positions",
            "trade_close_decisions",
            "trade_execution_intents",
            "trade_admissions",
        )

    def feature_store_schema_ready(self) -> bool:
        return self.schema_has_tables("trading_feature_snapshots")

    def market_context_schema_ready(self) -> bool:
        return self.schema_has_tables("market_context_snapshots")


__all__ = ["EngineFactRepository"]
