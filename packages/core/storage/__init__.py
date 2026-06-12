from __future__ import annotations

from typing import Any

from .capabilities import StorageCapabilities
from .db import DEFAULT_POSTGRES_URL, default_database_url

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "AlertRepository": (".alert_repository", "AlertRepository"),
    "BrokerRepository": (".broker_repository", "BrokerRepository"),
    "CaptureRepository": (".capture_repository", "CaptureRepository"),
    "ControlRepository": (".control_repository", "ControlRepository"),
    "EngineFactRepository": (".engine_fact_repository", "EngineFactRepository"),
    "ExecutionRepository": (".execution_repository", "ExecutionRepository"),
    "JobRepository": (".job_repository", "JobRepository"),
    "ClickHouseMarketDataStore": (".market_data_store", "ClickHouseMarketDataStore"),
    "OpsStore": (".ops_store", "OpsStore"),
    "SignalRepository": (".signal_repository", "SignalRepository"),
    "StorageContext": (".context", "StorageContext"),
    "TradingStore": (".trading_store", "TradingStore"),
    "build_alert_repository": (".factory", "build_alert_repository"),
    "build_broker_repository": (".factory", "build_broker_repository"),
    "build_capture_repository": (".factory", "build_capture_repository"),
    "build_control_repository": (".factory", "build_control_repository"),
    "build_engine_fact_repository": (".factory", "build_engine_fact_repository"),
    "build_execution_repository": (".factory", "build_execution_repository"),
    "build_market_data_store": (".factory", "build_market_data_store"),
    "build_job_repository": (".factory", "build_job_repository"),
    "build_ops_store": (".factory", "build_ops_store"),
    "build_signal_repository": (".factory", "build_signal_repository"),
    "build_storage_context": (".factory", "build_storage_context"),
    "build_trading_store": (".factory", "build_trading_store"),
}


def __getattr__(name: str) -> Any:
    export = _LAZY_EXPORTS.get(name)
    if export is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute_name = export
    from importlib import import_module

    value = getattr(import_module(module_name, __name__), attribute_name)
    globals()[name] = value
    return value


__all__ = [
    "DEFAULT_POSTGRES_URL",
    "StorageCapabilities",
    "default_database_url",
    *_LAZY_EXPORTS,
]
