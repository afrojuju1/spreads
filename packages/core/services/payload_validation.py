from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

PayloadModelT = TypeVar("PayloadModelT", bound=BaseModel)


def normalize_required_text(value: Any, *, field_name: str | None = None) -> str:
    rendered = str(value or "").strip()
    if not rendered:
        if field_name is not None:
            raise ValueError(f"{field_name} is required")
        raise ValueError("must not be empty")
    return rendered


def normalize_optional_text(value: Any) -> str | None:
    rendered = str(value or "").strip()
    return rendered or None


def normalize_mapping(value: Any, *, field_name: str | None = None) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        if field_name is not None:
            raise ValueError(f"{field_name} must be a mapping")
        raise ValueError("must be a mapping")
    return dict(value)


def normalize_text_tuple(
    value: Any,
    *,
    field_name: str | None = None,
    uppercase: bool = False,
    require_non_empty: bool = False,
) -> tuple[str, ...]:
    if value is None:
        items: tuple[str, ...] = ()
    elif isinstance(value, list | tuple):
        items = tuple(str(item).strip() for item in value if str(item or "").strip())
    else:
        if field_name is not None:
            raise ValueError(f"{field_name} must be a list")
        raise ValueError("must be a list")
    if uppercase:
        items = tuple(item.upper() for item in items)
    if require_non_empty and not items:
        if field_name is not None:
            raise ValueError(f"{field_name} must not be empty")
        raise ValueError("must not be empty")
    return items


def format_validation_error(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors():
        location = ".".join(str(item) for item in error["loc"]) or "payload"
        parts.append(f"{location}: {error['msg']}")
    return "; ".join(parts)


def validate_payload_model(model_type: type[PayloadModelT], payload: Mapping[str, Any], *, path: Path, label: str) -> PayloadModelT:
    try:
        return model_type.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid {label} config in {path}: {format_validation_error(exc)}") from exc
