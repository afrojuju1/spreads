from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import redis

from core.runtime.config import default_database_url, default_redis_url
from core.storage.factory import build_event_repository

GLOBAL_EVENTS_CHANNEL = "spreads:events"
EVENT_SCHEMA_VERSION = "v1"
EVENT_PRODUCER_VERSION = "spreads"


def _render_timestamp(value: str | datetime | None = None) -> str:
    if value is None:
        value = datetime.now(UTC)
    if isinstance(value, datetime):
        rendered = value.isoformat()
    else:
        rendered = str(value)
    return rendered.replace("+00:00", "Z") if rendered.endswith("+00:00") else rendered


def _default_event_class(topic: str) -> str:
    if topic.startswith(("broker.", "execution.")):
        return "broker_event"
    if topic.startswith(("post_market.",)):
        return "analytics_event"
    return "control_event"


def _persist_event(envelope: dict[str, Any], *, database_url: str | None = None) -> dict[str, Any]:
    repository = build_event_repository(database_url or default_database_url())
    if not repository.schema_ready():
        return envelope
    repository.create_event(
        event_id=str(envelope["event_id"]),
        event_class=str(envelope["event_class"]),
        event_type=str(envelope["event_type"]),
        topic=str(envelope["topic"]),
        occurred_at=str(envelope["occurred_at"]),
        ingested_at=str(envelope["ingested_at"]),
        source=str(envelope["source"]),
        entity_type=str(envelope["entity_type"]),
        entity_key=str(envelope["entity_key"]),
        payload=dict(envelope.get("payload") or {}),
        schema_version=str(envelope["schema_version"]),
        producer_version=str(envelope["producer_version"]),
        session_date=envelope.get("session_date"),
        market_session=envelope.get("market_session"),
        correlation_id=envelope.get("correlation_id"),
        causation_id=envelope.get("causation_id"),
    )
    return envelope


def build_global_event(
    *,
    topic: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any] | None = None,
    event_class: str | None = None,
    event_type: str = "event",
    timestamp: str | datetime | None = None,
    source: str | None = None,
    session_date: str | None = None,
    market_session: str | None = None,
    schema_version: str = EVENT_SCHEMA_VERSION,
    producer_version: str = EVENT_PRODUCER_VERSION,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    occurred_at = _render_timestamp(timestamp)
    ingested_at = _render_timestamp()
    entity_key = str(entity_id)
    return {
        "type": event_type,
        "topic": topic,
        "entity_type": entity_type,
        "entity_id": entity_key,
        "timestamp": occurred_at,
        "payload": payload or {},
        "event_id": event_id or str(uuid4()),
        "event_class": event_class or _default_event_class(topic),
        "event_type": event_type,
        "occurred_at": occurred_at,
        "ingested_at": ingested_at,
        "source": source or topic.split(".", 1)[0],
        "entity_key": entity_key,
        "session_date": session_date,
        "market_session": market_session,
        "schema_version": schema_version,
        "producer_version": producer_version,
        "correlation_id": correlation_id,
        "causation_id": causation_id,
    }


async def publish_global_event_async(
    event_bus: Any,
    *,
    topic: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any] | None = None,
    event_class: str | None = None,
    event_type: str = "event",
    timestamp: str | datetime | None = None,
    source: str | None = None,
    session_date: str | None = None,
    market_session: str | None = None,
    schema_version: str = EVENT_SCHEMA_VERSION,
    producer_version: str = EVENT_PRODUCER_VERSION,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    envelope = build_global_event(
        topic=topic,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload,
        event_class=event_class,
        event_type=event_type,
        timestamp=timestamp,
        source=source,
        session_date=session_date,
        market_session=market_session,
        schema_version=schema_version,
        producer_version=producer_version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
    await asyncio.to_thread(_persist_event, envelope, database_url=database_url)
    await event_bus.publish(GLOBAL_EVENTS_CHANNEL, json.dumps(envelope))
    return envelope


async def record_global_event_async(
    *,
    topic: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any] | None = None,
    event_class: str | None = None,
    event_type: str = "event",
    timestamp: str | datetime | None = None,
    source: str | None = None,
    session_date: str | None = None,
    market_session: str | None = None,
    schema_version: str = EVENT_SCHEMA_VERSION,
    producer_version: str = EVENT_PRODUCER_VERSION,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    envelope = build_global_event(
        topic=topic,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload,
        event_class=event_class,
        event_type=event_type,
        timestamp=timestamp,
        source=source,
        session_date=session_date,
        market_session=market_session,
        schema_version=schema_version,
        producer_version=producer_version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
    await asyncio.to_thread(_persist_event, envelope, database_url=database_url)
    return envelope


def publish_global_event_sync(
    *,
    topic: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any] | None = None,
    event_class: str | None = None,
    event_type: str = "event",
    timestamp: str | datetime | None = None,
    redis_url: str | None = None,
    source: str | None = None,
    session_date: str | None = None,
    market_session: str | None = None,
    schema_version: str = EVENT_SCHEMA_VERSION,
    producer_version: str = EVENT_PRODUCER_VERSION,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    envelope = build_global_event(
        topic=topic,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload,
        event_class=event_class,
        event_type=event_type,
        timestamp=timestamp,
        source=source,
        session_date=session_date,
        market_session=market_session,
        schema_version=schema_version,
        producer_version=producer_version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
    _persist_event(envelope, database_url=database_url)
    client = redis.Redis.from_url(redis_url or default_redis_url(), decode_responses=True)
    try:
        client.publish(GLOBAL_EVENTS_CHANNEL, json.dumps(envelope))
    finally:
        client.close()
    return envelope


def record_global_event_sync(
    *,
    topic: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any] | None = None,
    event_class: str | None = None,
    event_type: str = "event",
    timestamp: str | datetime | None = None,
    source: str | None = None,
    session_date: str | None = None,
    market_session: str | None = None,
    schema_version: str = EVENT_SCHEMA_VERSION,
    producer_version: str = EVENT_PRODUCER_VERSION,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    database_url: str | None = None,
) -> dict[str, Any]:
    envelope = build_global_event(
        topic=topic,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload,
        event_class=event_class,
        event_type=event_type,
        timestamp=timestamp,
        source=source,
        session_date=session_date,
        market_session=market_session,
        schema_version=schema_version,
        producer_version=producer_version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
    _persist_event(envelope, database_url=database_url)
    return envelope
