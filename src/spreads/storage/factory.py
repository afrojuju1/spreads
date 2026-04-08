from __future__ import annotations

from spreads.storage.alert_repository import AlertRepository
from spreads.storage.collector_repository import CollectorRepository
from spreads.storage.db import DEFAULT_POSTGRES_URL, default_database_url
from spreads.storage.generator_job_repository import GeneratorJobRepository
from spreads.storage.job_repository import JobRepository
from spreads.storage.post_market_repository import PostMarketAnalysisRepository
from spreads.storage.run_history_repository import RunHistoryRepository


def build_history_store(path_or_url: str | None = None):
    if path_or_url is None:
        path_or_url = default_database_url()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return RunHistoryRepository(value)
    raise RuntimeError(
        f"History storage is Postgres-only. Use a PostgreSQL URL, for example {DEFAULT_POSTGRES_URL}."
    )


def build_collector_repository(path_or_url: str | None = None):
    if path_or_url is None:
        path_or_url = default_database_url()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return CollectorRepository(value)
    raise RuntimeError(
        f"Collector storage is Postgres-only. Use a PostgreSQL URL, for example {DEFAULT_POSTGRES_URL}."
    )


def build_alert_repository(path_or_url: str | None = None):
    if path_or_url is None:
        path_or_url = default_database_url()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return AlertRepository(value)
    raise RuntimeError(
        f"Alert storage is Postgres-only. Use a PostgreSQL URL, for example {DEFAULT_POSTGRES_URL}."
    )


def build_job_repository(path_or_url: str | None = None):
    if path_or_url is None:
        path_or_url = default_database_url()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return JobRepository(value)
    raise RuntimeError(
        f"Job storage is Postgres-only. Use a PostgreSQL URL, for example {DEFAULT_POSTGRES_URL}."
    )


def build_generator_job_repository(path_or_url: str | None = None):
    if path_or_url is None:
        path_or_url = default_database_url()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return GeneratorJobRepository(value)
    raise RuntimeError(
        f"Generator job storage is Postgres-only. Use a PostgreSQL URL, for example {DEFAULT_POSTGRES_URL}."
    )


def build_post_market_repository(path_or_url: str | None = None):
    if path_or_url is None:
        path_or_url = default_database_url()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return PostMarketAnalysisRepository(value)
    raise RuntimeError(
        f"Post-market analysis storage is Postgres-only. Use a PostgreSQL URL, for example {DEFAULT_POSTGRES_URL}."
    )
