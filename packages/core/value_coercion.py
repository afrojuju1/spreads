from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import re
from typing import Any

from whenever import Instant

_SAFE_COMPONENT_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_instant(value: Any | None = None) -> Instant | None:
    if value is None:
        return Instant.now()
    if isinstance(value, Instant):
        return value
    if isinstance(value, datetime):
        resolved = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return Instant(resolved.astimezone(UTC))
    if isinstance(value, (int, float)):
        return Instant.from_timestamp(float(value))
    rendered = str(value).strip()
    if not rendered:
        return None
    if rendered.isdigit():
        return Instant.from_timestamp(float(rendered))
    try:
        return Instant.parse_iso(rendered)
    except ValueError:
        normalized = rendered.replace("Z", "+00:00") if rendered.endswith("Z") else rendered
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return Instant(parsed.astimezone(UTC))


def utc_iso(value: Any | None = None) -> str | None:
    instant = _utc_instant(value)
    if instant is None:
        return None
    return instant.format_iso(unit="second")


def utc_now_iso() -> str:
    rendered = utc_iso()
    if rendered is None:
        raise RuntimeError("Unable to render current UTC timestamp")
    return rendered


def utc_expiry_iso(
    *,
    seconds: int | float | None = None,
    minutes: int | float | None = None,
    from_time: Any | None = None,
    minimum_seconds: int = 1,
) -> str:
    base = _utc_instant(from_time) or Instant.now()
    total_seconds = int(seconds or 0) + int((minutes or 0) * 60)
    total_seconds = max(total_seconds, int(minimum_seconds))
    return base.add(seconds=total_seconds).format_iso(unit="second")


def coerce_utc_iso(value: Any) -> str | None:
    return utc_iso(value)


def coerce_utc_datetime(value: Any) -> datetime | None:
    instant = _utc_instant(value)
    if instant is None:
        return None
    return instant.to_stdlib()


def as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def normalize_symbol(value: Any) -> str | None:
    rendered = as_text(value)
    return None if rendered is None else rendered.upper()


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
    "coerce_utc_datetime",
    "coerce_utc_iso",
    "normalize_symbol",
    "safe_component",
    "unique_text_list",
    "utc_expiry_iso",
    "utc_iso",
    "utc_now",
    "utc_now_iso",
]
