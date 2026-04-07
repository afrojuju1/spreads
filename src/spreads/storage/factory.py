from __future__ import annotations

from pathlib import Path

from spreads.storage.history import DEFAULT_HISTORY_DB_PATH, RunHistoryStore
from spreads.storage.postgres import PostgresRunHistoryStore


def build_history_store(path_or_url: str | Path | None = None):
    if path_or_url is None:
        return RunHistoryStore(DEFAULT_HISTORY_DB_PATH)
    value = str(path_or_url)
    if value.startswith("postgres://") or value.startswith("postgresql://"):
        return PostgresRunHistoryStore(value)
    return RunHistoryStore(value)
