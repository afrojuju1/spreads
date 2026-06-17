from __future__ import annotations

import os


DEFAULT_POSTGRES_URL = "postgresql://spreads:spreads@localhost:55432/spreads"
DEFAULT_REDIS_URL = "redis://localhost:56379/0"
DEFAULT_CLICKHOUSE_URL = "http://spreads:spreads@localhost:58123/spreads"
DEFAULT_BACKTEST_ARTIFACT_ROOT = "outputs/backtest_runs"


def normalize_database_url(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    return url


def default_database_url() -> str:
    return os.environ.get("SPREADS_DATABASE_URL") or os.environ.get("DATABASE_URL") or DEFAULT_POSTGRES_URL


def default_redis_url() -> str:
    return os.environ.get("REDIS_URL") or DEFAULT_REDIS_URL


def default_clickhouse_url() -> str:
    return os.environ.get("SPREADS_CLICKHOUSE_URL") or os.environ.get("CLICKHOUSE_URL") or DEFAULT_CLICKHOUSE_URL


def default_backtest_artifact_root() -> str:
    return os.environ.get("SPREADS_BACKTEST_ARTIFACT_ROOT") or DEFAULT_BACKTEST_ARTIFACT_ROOT


def default_alpha_vantage_api_key() -> str | None:
    return os.environ.get("ALPHAVANTAGE_API_KEY") or os.environ.get("ALPHA_VANTAGE_API_KEY")


def default_sec_user_agent() -> str:
    return (
        os.environ.get("SEC_USER_AGENT")
        or os.environ.get("SEC_EDGAR_USER_AGENT")
        or "Spreads Company Valuation Engine/1.0 company-valuation@spreads.local"
    )


def default_sec_request_interval_seconds() -> float:
    raw = os.environ.get("SEC_REQUEST_INTERVAL_SECONDS")
    if raw in (None, ""):
        return 1.0
    try:
        return max(float(raw), 0.1)
    except ValueError:
        return 1.0


def default_openfigi_api_key() -> str | None:
    return os.environ.get("OPENFIGI_API_KEY")
