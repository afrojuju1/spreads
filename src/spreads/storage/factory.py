from __future__ import annotations

import os

from spreads.storage.postgres import PostgresRunHistoryStore

DEFAULT_POSTGRES_URL = "postgresql://spreads:spreads@localhost:55432/spreads"


def default_history_target() -> str:
    return os.environ.get("SPREADS_DATABASE_URL") or os.environ.get("DATABASE_URL") or DEFAULT_POSTGRES_URL


def build_history_store(path_or_url: str | None = None):
    if path_or_url is None:
        path_or_url = default_history_target()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://") or value.startswith("postgresql+psycopg://"):
        return PostgresRunHistoryStore(value)
    raise RuntimeError(
        "SQLite history storage is deprecated. Configure SPREADS_DATABASE_URL for the Postgres backend."
    )
