from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import re
from typing import Any

_SAFE_COMPONENT_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def coerce_bool(value: Any, *, default: bool | None = None) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def unique_text_list(value: Any, *, accept_scalar: bool = False) -> list[str]:
    if isinstance(value, (list, tuple)):
        values = value
    elif accept_scalar:
        values = (value,)
    else:
        values = ()
    normalized: list[str] = []
    for item in values:
        rendered = as_text(item)
        if rendered is not None and rendered not in normalized:
            normalized.append(rendered)
    return normalized


def safe_component(value: Any, *, default: str = "unknown") -> str:
    rendered = str(value or "").strip()
    return _SAFE_COMPONENT_RE.sub("_", rendered) or default


__all__ = [
    "as_list",
    "as_mapping",
    "as_text",
    "coerce_bool",
    "coerce_float",
    "coerce_int",
    "safe_component",
    "unique_text_list",
    "utc_now",
    "utc_now_iso",
]
