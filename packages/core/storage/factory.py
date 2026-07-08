from __future__ import annotations

from core.runtime.config import DEFAULT_POSTGRES_URL, default_clickhouse_url, default_database_url
from core.storage.alert_repository import AlertRepository
from core.storage.backtest_repository import BacktestRepository
from core.storage.broker_repository import BrokerRepository
from core.storage.capture_repository import CaptureRepository
from core.storage.control_repository import ControlRepository
from core.storage.context import StorageContext
from core.storage.engine_event_repository import EngineEventRepository
from core.storage.engine_fact_repository import EngineFactRepository
from core.storage.execution_repository import ExecutionRepository
from core.storage.job_repository import JobRepository
from core.storage.market_data_store import ClickHouseMarketDataStore
from core.storage.ops_store import OpsStore
from core.storage.signal_repository import SignalRepository
from core.storage.trading_store import TradingStore


def _resolve_postgres_url(path_or_url: str | None = None) -> str:
    if path_or_url is None:
        path_or_url = default_database_url()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return value
    raise RuntimeError(f"Storage is Postgres-only. Use a PostgreSQL URL, for example {DEFAULT_POSTGRES_URL}.")


def build_storage_context(path_or_url: str | None = None) -> StorageContext:
    return StorageContext(_resolve_postgres_url(path_or_url))


def build_market_data_store(clickhouse_url: str | None = None, *, context: StorageContext | None = None) -> ClickHouseMarketDataStore:
    if context is not None:
        return context.market_data
    return ClickHouseMarketDataStore(clickhouse_url or default_clickhouse_url())


def build_alert_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.alerts
    value = _resolve_postgres_url(path_or_url)
    return AlertRepository(value)


def build_backtest_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.backtests
    value = _resolve_postgres_url(path_or_url)
    return BacktestRepository(value)


def build_broker_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.broker
    value = _resolve_postgres_url(path_or_url)
    return BrokerRepository(value)


def build_capture_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.capture
    value = _resolve_postgres_url(path_or_url)
    return CaptureRepository(value)


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


def build_execution_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.execution
    value = _resolve_postgres_url(path_or_url)
    return ExecutionRepository(value)


def build_engine_fact_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.engine_facts
    value = _resolve_postgres_url(path_or_url)
    return EngineFactRepository(value)


def build_engine_event_repository(path_or_url: str | None = None, *, context: StorageContext | None = None):
    if context is not None:
        return context.engine_events
    value = _resolve_postgres_url(path_or_url)
    return EngineEventRepository(value)


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


def build_ops_store(path_or_url: str | None = None, *, context: StorageContext | None = None) -> OpsStore:
    if context is not None:
        return context.ops
    return build_storage_context(path_or_url).ops
