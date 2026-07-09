from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, TypeVar

from sqlalchemy.orm import Session

from core.runtime.config import default_database_url
from core.storage.context import StorageContext
from core.storage.db import build_session_factory
from core.storage.factory import build_storage_context
from core.storage.records import StorageRow
from core.storage.row_serialization import to_storage_row, to_storage_rows

ModelT = TypeVar("ModelT")


def resolve_database_url(database_url: str | None = None) -> str:
    return str(database_url or default_database_url())


def open_storage(database_url: str | None = None) -> StorageContext:
    return build_storage_context(resolve_database_url(database_url))


@contextmanager
def session_scope(database_url: str | None = None) -> Iterator[Session]:
    _, session_factory = build_session_factory(resolve_database_url(database_url))
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_model_row(
    session: Session,
    model_type: type[ModelT],
    identity: Any,
    *,
    aliases: dict[str, str] | None = None,
    exclude: set[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> StorageRow | None:
    model = session.get(model_type, identity)
    if model is None:
        return None
    return to_storage_row(model, aliases=aliases, exclude=exclude, extra=extra)


def first_model_row(
    session: Session,
    statement: Any,
    *,
    aliases: dict[str, str] | None = None,
    exclude: set[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> StorageRow | None:
    model = session.scalar(statement)
    if model is None:
        return None
    return to_storage_row(model, aliases=aliases, exclude=exclude, extra=extra)


def list_model_rows(
    session: Session,
    statement: Any,
    *,
    aliases: dict[str, str] | None = None,
    exclude: set[str] | None = None,
) -> list[StorageRow]:
    return to_storage_rows(list(session.scalars(statement).all()), aliases=aliases, exclude=exclude)
