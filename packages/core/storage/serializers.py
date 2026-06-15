from __future__ import annotations

from datetime import date, datetime
from typing import Any

from core.value_coercion import coerce_utc_datetime


def parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    parsed = coerce_utc_datetime(value)
    if parsed is None:
        raise ValueError(f"Invalid datetime value: {value!r}")
    return parsed


def parse_date(value: str | date) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value))


def render_value(value: Any) -> Any:
    if isinstance(value, datetime):
        rendered = value.isoformat()
        return rendered.replace("+00:00", "Z") if rendered.endswith("+00:00") else rendered
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: render_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [render_value(item) for item in value]
    if isinstance(value, tuple):
        return [render_value(item) for item in value]
    return value


__all__ = ["parse_datetime", "parse_date", "render_value"]
