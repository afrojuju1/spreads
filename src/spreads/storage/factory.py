from __future__ import annotations

from spreads.runtime.config import DEFAULT_POSTGRES_URL, default_database_url
from spreads.storage.alert_repository import AlertRepository
from spreads.storage.broker_repository import BrokerRepository
from spreads.storage.collector_repository import CollectorRepository
from spreads.storage.control_repository import ControlRepository
from spreads.storage.context import StorageContext
from spreads.storage.event_repository import EventRepository
from spreads.storage.execution_repository import ExecutionRepository
from spreads.storage.job_repository import JobRepository
from spreads.storage.ops_store import OpsStore
from spreads.storage.post_market_repository import PostMarketAnalysisRepository
from spreads.storage.recovery_repository import RecoveryRepository
from spreads.storage.risk_repository import RiskDecisionRepository
from spreads.storage.run_history_repository import RunHistoryRepository
from spreads.storage.signal_repository import SignalRepository
from spreads.storage.trading_store import TradingStore


def _resolve_postgres_url(path_or_url: str | None = None) -> str:
    if path_or_url is None:
        path_or_url = default_database_url()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return value
    raise RuntimeError(
        f"Storage is Postgres-only. Use a PostgreSQL URL, for example {DEFAULT_POSTGRES_URL}."
    )


def build_storage_context(path_or_url: str | None = None) -> StorageContext:
    return StorageContext(_resolve_postgres_url(path_or_url))


def build_history_store(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.history
    value = _resolve_postgres_url(path_or_url)
    return RunHistoryRepository(value)


def build_collector_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.collector
    value = _resolve_postgres_url(path_or_url)
    return CollectorRepository(value)


def build_alert_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.alerts
    value = _resolve_postgres_url(path_or_url)
    return AlertRepository(value)


def build_broker_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.broker
    value = _resolve_postgres_url(path_or_url)
    return BrokerRepository(value)


def build_job_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.jobs
    value = _resolve_postgres_url(path_or_url)
    return JobRepository(value)


def build_control_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.control
    value = _resolve_postgres_url(path_or_url)
    return ControlRepository(value)


def build_risk_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.risk
    value = _resolve_postgres_url(path_or_url)
    return RiskDecisionRepository(value)


def build_execution_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.execution
    value = _resolve_postgres_url(path_or_url)
    return ExecutionRepository(value)


def build_event_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.events
    value = _resolve_postgres_url(path_or_url)
    return EventRepository(value)


def build_signal_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.signals
    value = _resolve_postgres_url(path_or_url)
    return SignalRepository(value)


def build_trading_store(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.trading
    value = _resolve_postgres_url(path_or_url)
    return TradingStore(value)


def build_post_market_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.post_market
    value = _resolve_postgres_url(path_or_url)
    return PostMarketAnalysisRepository(value)


def build_recovery_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.recovery
    value = _resolve_postgres_url(path_or_url)
    return RecoveryRepository(value)


def build_ops_store(path_or_url: str | None = None, *, context: StorageContext | None = None) -> OpsStore:
    if context is not None:
        return context.ops
    return build_storage_context(path_or_url).ops
