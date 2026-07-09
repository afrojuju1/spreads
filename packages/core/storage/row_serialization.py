from __future__ import annotations

from typing import Any

from sqlalchemy.inspection import inspect as sqlalchemy_inspect

from core.storage.records import StorageRow, make_storage_row
from core.storage.serializers import render_value


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
    return make_storage_row(payload)


def to_storage_rows(
    models: list[Any],
    *,
    aliases: dict[str, str] | None = None,
    exclude: set[str] | None = None,
) -> list[StorageRow]:
    return [to_storage_row(model, aliases=aliases, exclude=exclude) for model in models]
