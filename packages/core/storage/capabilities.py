from __future__ import annotations

from functools import cached_property

from sqlalchemy import inspect


class StorageCapabilities:
    def __init__(self, engine: object) -> None:
        self.engine = engine

    @cached_property
    def table_names(self) -> frozenset[str]:
        return frozenset(inspect(self.engine).get_table_names(schema="public"))

    def has_tables(self, *table_names: str) -> bool:
        return set(table_names).issubset(self.table_names)

    def refresh(self) -> frozenset[str]:
        self.__dict__.pop("table_names", None)
        return self.table_names
