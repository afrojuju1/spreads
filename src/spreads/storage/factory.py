from __future__ import annotations

import os

from spreads.storage.db import DEFAULT_POSTGRES_URL, default_database_url
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
