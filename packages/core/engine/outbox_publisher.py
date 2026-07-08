from __future__ import annotations

import json
from typing import Any

import nats
from nats.js.api import RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from core.engine.events import ENGINE_EVENT_SUBJECT_PREFIX
from core.runtime.config import default_nats_url
from core.storage.engine_event_repository import EngineEventRepository
from core.storage.factory import build_engine_event_repository


async def _ensure_engine_stream(jetstream: Any, *, stream: str) -> None:
    try:
        await jetstream.stream_info(stream)
    except NotFoundError:
        await jetstream.add_stream(
            StreamConfig(
                name=stream,
                subjects=[f"{ENGINE_EVENT_SUBJECT_PREFIX}.>"],
                retention=RetentionPolicy.LIMITS,
                storage=StorageType.FILE,
                duplicate_window=120,
            )
        )


async def publish_pending_engine_outbox(
    *,
    repository: EngineEventRepository | None = None,
    nats_url: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    store = repository or build_engine_event_repository()
    pending = store.list_pending_outbox(limit=limit)
    summary: dict[str, Any] = {"pending": len(pending), "published": 0, "failed": 0, "errors": []}
    if not pending:
        return summary

    client = await nats.connect(nats_url or default_nats_url())
    try:
        jetstream = client.jetstream()
        ensured_streams: set[str] = set()
        for row in pending:
            outbox_id = str(row["engine_outbox_id"])
            stream = str(row.get("stream") or "")
            try:
                if stream and stream not in ensured_streams:
                    await _ensure_engine_stream(jetstream, stream=stream)
                    ensured_streams.add(stream)
                payload = json.dumps(row["payload"], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
                await jetstream.publish(str(row["subject"]), payload, stream=stream or None, headers=dict(row.get("headers") or {}))
                store.mark_outbox_published(outbox_id)
                summary["published"] = int(summary["published"]) + 1
            except Exception as exc:
                store.mark_outbox_failed(outbox_id, error_text=str(exc))
                summary["failed"] = int(summary["failed"]) + 1
                summary["errors"].append({"engine_outbox_id": outbox_id, "error": str(exc)})
    finally:
        await client.close()
    return summary


__all__ = ["publish_pending_engine_outbox"]
