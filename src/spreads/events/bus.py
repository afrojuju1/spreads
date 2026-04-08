from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import redis

from spreads.jobs.orchestration import default_redis_url

GLOBAL_EVENTS_CHANNEL = "spreads:events"


def _render_timestamp(value: str | datetime | None = None) -> str:
    if value is None:
        value = datetime.now(UTC)
    if isinstance(value, datetime):
        rendered = value.isoformat()
    else:
        rendered = str(value)
    return rendered.replace("+00:00", "Z") if rendered.endswith("+00:00") else rendered


def build_global_event(
    *,
    topic: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any] | None = None,
    event_type: str = "event",
    timestamp: str | datetime | None = None,
) -> dict[str, Any]:
    return {
        "type": event_type,
        "topic": topic,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "timestamp": _render_timestamp(timestamp),
        "payload": payload or {},
    }


async def publish_global_event_async(
    event_bus: Any,
    *,
    topic: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any] | None = None,
    event_type: str = "event",
    timestamp: str | datetime | None = None,
) -> dict[str, Any]:
    envelope = build_global_event(
        topic=topic,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload,
        event_type=event_type,
        timestamp=timestamp,
    )
    await event_bus.publish(GLOBAL_EVENTS_CHANNEL, json.dumps(envelope))
    return envelope


def publish_global_event_sync(
    *,
    topic: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any] | None = None,
    event_type: str = "event",
    timestamp: str | datetime | None = None,
    redis_url: str | None = None,
) -> dict[str, Any]:
    envelope = build_global_event(
        topic=topic,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload,
        event_type=event_type,
        timestamp=timestamp,
    )
    client = redis.Redis.from_url(redis_url or default_redis_url(), decode_responses=True)
    try:
        client.publish(GLOBAL_EVENTS_CHANNEL, json.dumps(envelope))
    finally:
        client.close()
    return envelope
