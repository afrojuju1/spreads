from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy.orm import Session

from spreads.storage.capabilities import StorageCapabilities
from spreads.storage.db import build_session_factory


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

    def close(self) -> None:
        # Engines/sessionmakers are shared and cached in storage.db.
        return None
