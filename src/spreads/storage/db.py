from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from spreads.runtime.config import (
    DEFAULT_POSTGRES_URL,
    default_database_url,
    normalize_database_url,
)


class Base(DeclarativeBase):
    pass
def build_engine(database_url: str | None = None):
    return create_engine(
        normalize_database_url(database_url or default_database_url()),
        future=True,
        pool_pre_ping=True,
    )


def build_session_factory(database_url: str | None = None):
    engine = build_engine(database_url)
    return engine, sessionmaker(bind=engine, expire_on_commit=False, future=True)
