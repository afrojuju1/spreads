from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, TypeVar

from sqlalchemy.inspection import inspect as sqlalchemy_inspect
from sqlalchemy.orm import Session

from spreads.runtime.config import default_database_url
from spreads.storage.context import StorageContext
from spreads.storage.db import build_session_factory
from spreads.storage.factory import build_storage_context
from spreads.storage.records import StorageRow
from spreads.storage.serializers import render_value

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


def _copy_value(value: Any) -> Any:
    rendered = render_value(value)
    if isinstance(rendered, dict):
        return dict(rendered)
    if isinstance(rendered, list):
        return list(rendered)
    return rendered


def to_storage_row(
    model: Any,
    *,
    aliases: dict[str, str] | None = None,
    exclude: set[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> StorageRow:
    mapper = sqlalchemy_inspect(model.__class__)
    payload: dict[str, Any] = {}
    for column in mapper.columns:
        source_key = str(column.key)
        if exclude and source_key in exclude:
            continue
        target_key = None if aliases is None else aliases.get(source_key)
        if target_key is None:
            target_key = source_key.removesuffix("_json") if source_key.endswith("_json") else source_key
        payload[target_key] = _copy_value(getattr(model, source_key))
    if extra:
        for key, value in extra.items():
            payload[key] = _copy_value(value)
    return StorageRow(payload)


def to_storage_rows(
    models: list[Any],
    *,
    aliases: dict[str, str] | None = None,
    exclude: set[str] | None = None,
) -> list[StorageRow]:
    return [
        to_storage_row(model, aliases=aliases, exclude=exclude)
        for model in models
    ]


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
