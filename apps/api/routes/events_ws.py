from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import redis.asyncio as redis_async

from spreads.events.bus import GLOBAL_EVENTS_CHANNEL
from spreads.runtime.config import default_redis_url

router = APIRouter()


@router.websocket("/ws/events")
async def global_events_ws(websocket: WebSocket) -> None:
    redis = redis_async.from_url(default_redis_url(), decode_responses=True)
    pubsub = redis.pubsub()
    try:
        await websocket.accept()
        await pubsub.subscribe(GLOBAL_EVENTS_CHANNEL)
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("type") == "message":
                payload = message["data"]
                if isinstance(payload, str):
                    await websocket.send_json(json.loads(payload))
                else:
                    await websocket.send_json(payload)
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        return
    except WebSocketDisconnect:
        return
    finally:
        await pubsub.unsubscribe(GLOBAL_EVENTS_CHANNEL)
        await pubsub.aclose()
        await redis.aclose()
