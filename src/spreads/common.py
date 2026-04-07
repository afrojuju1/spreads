from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import msgpack


def load_local_env(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def env_or_die(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    expected = " or ".join(names)
    raise SystemExit(f"Missing required environment variable: {expected}")


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def pick(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def format_stream_timestamp(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, msgpack.Timestamp):
        dt = datetime.fromtimestamp(value.to_unix(), tz=UTC)
        return dt.isoformat(timespec="seconds").replace("+00:00", "Z")
    return str(value)
