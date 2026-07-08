from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from core.storage.serializers import render_value


def stable_hash(value: Any, *, length: int = 24) -> str:
    rendered = render_value(value)
    encoded = json.dumps(rendered, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:length]


def idempotency_key(namespace: str, *parts: Any) -> str:
    return f"{namespace}:{stable_hash(parts, length=32)}"


def engine_event_id(key: str) -> str:
    return f"engine_event:{stable_hash(key, length=32)}"


def engine_outbox_id(event_id: str, *, stream: str, subject: str) -> str:
    return f"engine_outbox:{stable_hash((event_id, stream, subject), length=32)}"


def trade_lifecycle_workflow_id(execution_intent_id: str) -> str:
    return f"trade_lifecycle:{execution_intent_id}"


def close_lifecycle_workflow_id(position_id: str, execution_intent_id: str | None = None) -> str:
    if execution_intent_id:
        return f"close_lifecycle:{position_id}:{execution_intent_id}"
    return f"close_lifecycle:{position_id}"


def client_order_id(prefix: str, *parts: Any) -> str:
    safe_prefix = prefix.strip().lower().replace("_", "-")[:12] or "spreads"
    return f"{safe_prefix}-{stable_hash(parts, length=28)}"


def aggregate_version_key(aggregate_type: str, aggregate_id: str, events: Sequence[Mapping[str, Any]] | None = None) -> str:
    return idempotency_key("aggregate_version", aggregate_type, aggregate_id, list(events or ()))


__all__ = [
    "aggregate_version_key",
    "client_order_id",
    "close_lifecycle_workflow_id",
    "engine_event_id",
    "engine_outbox_id",
    "idempotency_key",
    "stable_hash",
    "trade_lifecycle_workflow_id",
]
