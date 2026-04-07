from __future__ import annotations

import os
from pathlib import Path

from spreads.storage.history import DEFAULT_HISTORY_DB_PATH, RunHistoryStore
from spreads.storage.postgres import PostgresRunHistoryStore


def default_history_target() -> str:
    return (
        os.environ.get("SPREADS_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or str(DEFAULT_HISTORY_DB_PATH)
    )


def build_history_store(path_or_url: str | Path | None = None):
    if path_or_url is None:
        path_or_url = default_history_target()
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://"):
        return PostgresRunHistoryStore(value)
    return RunHistoryStore(value)
