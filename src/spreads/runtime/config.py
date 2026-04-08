from __future__ import annotations

import os


DEFAULT_POSTGRES_URL = "postgresql://spreads:spreads@localhost:55432/spreads"
DEFAULT_REDIS_URL = "redis://localhost:56379/0"


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
