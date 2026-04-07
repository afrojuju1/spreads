from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


DEFAULT_POSTGRES_URL = "postgresql://spreads:spreads@localhost:55432/spreads"


class Base(DeclarativeBase):
    pass


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


def build_engine(database_url: str | None = None):
    return create_engine(
        normalize_database_url(database_url or default_database_url()),
        future=True,
        pool_pre_ping=True,
    )


def build_session_factory(database_url: str | None = None):
    engine = build_engine(database_url)
    return engine, sessionmaker(bind=engine, expire_on_commit=False, future=True)
