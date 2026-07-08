from __future__ import annotations

import json
from typing import Any

import nats

from core.runtime.config import default_nats_url
from core.storage.engine_event_repository import EngineEventRepository
from core.storage.factory import build_engine_event_repository


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
        for row in pending:
            outbox_id = str(row["engine_outbox_id"])
            try:
                payload = json.dumps(row["payload"], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
                await jetstream.publish(str(row["subject"]), payload, headers=dict(row.get("headers") or {}))
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
