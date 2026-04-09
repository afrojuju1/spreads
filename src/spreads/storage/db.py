from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from spreads.runtime.config import (
    DEFAULT_POSTGRES_URL,
    default_database_url,
    normalize_database_url,
)


class Base(DeclarativeBase):
    pass


@lru_cache(maxsize=8)
def _cached_engine(database_url: str):
    return create_engine(
        database_url,
        future=True,
        pool_pre_ping=True,
    )


@lru_cache(maxsize=8)
def _cached_session_factory(database_url: str):
    engine = _cached_engine(database_url)
    return engine, sessionmaker(bind=engine, expire_on_commit=False, future=True)


def build_engine(database_url: str | None = None):
    normalized = normalize_database_url(database_url or default_database_url())
    return _cached_engine(normalized)


def build_session_factory(database_url: str | None = None):
    normalized = normalize_database_url(database_url or default_database_url())
    return _cached_session_factory(normalized)
