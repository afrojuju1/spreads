from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy.inspection import inspect as sqlalchemy_inspect
from sqlalchemy.orm import Session

from spreads.storage.capabilities import StorageCapabilities
from spreads.storage.db import build_session_factory
from spreads.storage.records import StorageRow, make_storage_row
from spreads.storage.serializers import render_value


def _copy_value(value: Any) -> Any:
    rendered = render_value(value)
    if isinstance(rendered, dict):
        return dict(rendered)
    if isinstance(rendered, list):
        return list(rendered)
    return rendered


class RepositoryBase:
    def __init__(
        self,
        database_url: str | None = None,
        *,
        engine: Any | None = None,
        session_factory: Any | None = None,
        capabilities: StorageCapabilities | None = None,
    ) -> None:
        resolved_engine = engine
        resolved_session_factory = session_factory
        if resolved_engine is None or resolved_session_factory is None:
            resolved_engine, resolved_session_factory = build_session_factory(database_url)
        self.path = database_url
        self.engine = resolved_engine
        self.session_factory = resolved_session_factory
        self.capabilities = capabilities or StorageCapabilities(self.engine)

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def schema_has_tables(self, *table_names: str) -> bool:
        return self.capabilities.has_tables(*table_names)

    def row(
        self,
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
        return make_storage_row(payload)

    def rows(
        self,
        models: list[Any],
        *,
        aliases: dict[str, str] | None = None,
        exclude: set[str] | None = None,
    ) -> list[StorageRow]:
        return [self.row(model, aliases=aliases, exclude=exclude) for model in models]

    def close(self) -> None:
        # Engines/sessionmakers are shared and cached in storage.db.
        return None
