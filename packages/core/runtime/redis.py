from __future__ import annotations

from urllib.parse import urlparse

from arq.connections import RedisSettings

from core.runtime.config import default_redis_url


def build_redis_settings(redis_url: str | None = None) -> RedisSettings:
    parsed = urlparse(redis_url or default_redis_url())
    database = 0
    if parsed.path and parsed.path != "/":
        database = int(parsed.path.lstrip("/"))
    return RedisSettings(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        database=database,
        password=parsed.password,
        ssl=parsed.scheme == "rediss",
    )
