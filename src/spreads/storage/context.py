from __future__ import annotations

from spreads.runtime.config import default_database_url
from spreads.storage.alert_repository import AlertRepository
from spreads.storage.broker_repository import BrokerRepository
from spreads.storage.capabilities import StorageCapabilities
from spreads.storage.collector_repository import CollectorRepository
from spreads.storage.control_repository import ControlRepository
from spreads.storage.event_repository import EventRepository
from spreads.storage.db import build_session_factory
from spreads.storage.execution_repository import ExecutionRepository
from spreads.storage.job_repository import JobRepository
from spreads.storage.ops_store import OpsStore
from spreads.storage.post_market_repository import PostMarketAnalysisRepository
from spreads.storage.recovery_repository import RecoveryRepository
from spreads.storage.risk_repository import RiskDecisionRepository
from spreads.storage.run_history_repository import RunHistoryRepository
from spreads.storage.signal_repository import SignalRepository
from spreads.storage.trading_store import TradingStore


class StorageContext:
    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = str(database_url or default_database_url())
        self.engine, self.session_factory = build_session_factory(self.database_url)
        self.capabilities = StorageCapabilities(self.engine)
        self._repositories: dict[str, object] = {}

    def _build_repository(self, key: str, repository_type: type[object]) -> object:
        repository = self._repositories.get(key)
        if repository is None:
            repository = repository_type(
                self.database_url,
                engine=self.engine,
                session_factory=self.session_factory,
                capabilities=self.capabilities,
            )
            self._repositories[key] = repository
        return repository

    @property
    def alerts(self) -> AlertRepository:
        return self._build_repository("alerts", AlertRepository)  # type: ignore[return-value]

    @property
    def broker(self) -> BrokerRepository:
        return self._build_repository("broker", BrokerRepository)  # type: ignore[return-value]

    @property
    def collector(self) -> CollectorRepository:
        return self._build_repository("collector", CollectorRepository)  # type: ignore[return-value]

    @property
    def trading(self) -> TradingStore:
        return self._build_repository("trading", TradingStore)  # type: ignore[return-value]

    @property
    def execution(self) -> ExecutionRepository:
        return self.trading

    @property
    def events(self) -> EventRepository:
        return self._build_repository("events", EventRepository)  # type: ignore[return-value]

    @property
    def control(self) -> ControlRepository:
        return self._build_repository("control", ControlRepository)  # type: ignore[return-value]

    @property
    def signals(self) -> SignalRepository:
        return self._build_repository("signals", SignalRepository)  # type: ignore[return-value]

    @property
    def history(self) -> RunHistoryRepository:
        return self._build_repository("history", RunHistoryRepository)  # type: ignore[return-value]

    @property
    def jobs(self) -> JobRepository:
        return self._build_repository("jobs", JobRepository)  # type: ignore[return-value]

    @property
    def risk(self) -> RiskDecisionRepository:
        return self._build_repository("risk", RiskDecisionRepository)  # type: ignore[return-value]

    @property
    def post_market(self) -> PostMarketAnalysisRepository:
        return self._build_repository("post_market", PostMarketAnalysisRepository)  # type: ignore[return-value]

    @property
    def recovery(self) -> RecoveryRepository:
        return self._build_repository("recovery", RecoveryRepository)  # type: ignore[return-value]

    @property
    def ops(self) -> OpsStore:
        store = self._repositories.get("ops")
        if store is None:
            store = OpsStore(
                alerts=self.alerts,
                jobs=self.jobs,
                post_market=self.post_market,
            )
            self._repositories["ops"] = store
        return store  # type: ignore[return-value]

    def close(self) -> None:
        return None

    def __enter__(self) -> StorageContext:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False
