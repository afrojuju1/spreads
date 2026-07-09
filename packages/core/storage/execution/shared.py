from __future__ import annotations

from typing import Any

from core.storage.serializers import parse_date


def _optional_date(value: str | None) -> Any:
    if value in (None, ""):
        return None
    return parse_date(value)
