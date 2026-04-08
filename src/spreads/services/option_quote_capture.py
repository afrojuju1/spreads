from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import msgpack
import websockets

from spreads.common import env_or_die
from spreads.services.option_quote_records import build_quote_records, build_quote_symbol_metadata
from spreads.services.scanner import (
    DEFAULT_DATA_BASE_URL,
    LiveOptionQuote,
    format_stream_timestamp,
    parse_float,
    parse_int,
)


def default_internal_api_base_url() -> str:
    configured = os.environ.get("SPREADS_INTERNAL_API_BASE_URL") or os.environ.get("SPREADS_API_BASE_URL")
    if configured:
        return configured.rstrip("/")
    if os.path.exists("/.dockerenv"):
        return "http://api:8000"
    return "http://localhost:58080"


def resolve_option_stream_url(*, data_base_url: str, feed: str) -> str:
    parsed = urllib.parse.urlparse(data_base_url)
    hostname = parsed.netloc.lower()
    if "sandbox" in hostname:
        return f"wss://stream.data.sandbox.alpaca.markets/v1beta1/{feed}"
    return f"wss://stream.data.alpaca.markets/v1beta1/{feed}"


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    for item in symbols:
        symbol = str(item).strip()
        if symbol and symbol not in normalized:
            normalized.append(symbol)
    return normalized


def _render_captured_at() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def request_option_quote_capture(
    *,
    candidates: list[dict[str, Any]],
    feed: str,
    duration_seconds: float,
    data_base_url: str | None = None,
    api_base_url: str | None = None,
) -> list[dict[str, Any]]:
    if not candidates or duration_seconds <= 0:
        return []

    request_payload = {
        "candidates": candidates,
        "feed": str(feed),
        "duration_seconds": float(duration_seconds),
        "data_base_url": data_base_url or DEFAULT_DATA_BASE_URL,
    }
    request_data = json.dumps(request_payload).encode("utf-8")
    request_url = f"{(api_base_url or default_internal_api_base_url()).rstrip('/')}/internal/market-data/option-quotes/capture"
    request = urllib.request.Request(
        request_url,
        data=request_data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    timeout_seconds = max(float(duration_seconds) + 15.0, 20.0)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Option quote capture request failed: {exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Option quote capture request failed: {exc.reason}") from exc
    quotes = payload.get("quotes")
    if not isinstance(quotes, list):
        raise RuntimeError("Option quote capture response did not include a quotes list")
    return [dict(item) for item in quotes if isinstance(item, dict)]


@dataclass
class _ActiveCapture:
    capture_id: str
    symbols: set[str]
    future: asyncio.Future[list[LiveOptionQuote]]
    quotes: list[LiveOptionQuote] = field(default_factory=list)


class _FeedCaptureBroker:
    def __init__(self, *, key_id: str, secret_key: str, data_base_url: str, feed: str) -> None:
        self.key_id = key_id
        self.secret_key = secret_key
        self.data_base_url = data_base_url
        self.feed = feed
        self.url = resolve_option_stream_url(data_base_url=data_base_url, feed=feed)
        self._captures: dict[str, _ActiveCapture] = {}
        self._capture_counter = 0
        self._lock = asyncio.Lock()
        self._runner_task: asyncio.Task[None] | None = None

    async def capture_quotes(self, *, symbols: list[str], duration_seconds: float) -> list[LiveOptionQuote]:
        normalized_symbols = set(_normalize_symbols(symbols))
        if not normalized_symbols:
            return []
        loop = asyncio.get_running_loop()
        future: asyncio.Future[list[LiveOptionQuote]] = loop.create_future()
        async with self._lock:
            self._capture_counter += 1
            capture_id = f"{self.feed}:{self._capture_counter}"
            self._captures[capture_id] = _ActiveCapture(
                capture_id=capture_id,
                symbols=normalized_symbols,
                future=future,
            )
            if self._runner_task is None or self._runner_task.done():
                self._runner_task = asyncio.create_task(self._run())
        timer_task = asyncio.create_task(self._complete_capture_after(capture_id, duration_seconds))
        try:
            return await future
        finally:
            timer_task.cancel()
            await self._discard_capture(capture_id)

    async def _complete_capture_after(self, capture_id: str, duration_seconds: float) -> None:
        try:
            await asyncio.sleep(max(float(duration_seconds), 0.5))
        except asyncio.CancelledError:
            return
        async with self._lock:
            capture = self._captures.pop(capture_id, None)
            if capture is not None and not capture.future.done():
                capture.future.set_result(list(capture.quotes))

    async def _discard_capture(self, capture_id: str) -> None:
        async with self._lock:
            capture = self._captures.pop(capture_id, None)
            if capture is not None and not capture.future.done():
                capture.future.cancel()

    async def _fail_captures(self, exc: Exception) -> None:
        async with self._lock:
            captures = list(self._captures.values())
            self._captures.clear()
        for capture in captures:
            if not capture.future.done():
                capture.future.set_exception(exc)

    async def _record_quote(self, quote: LiveOptionQuote) -> None:
        async with self._lock:
            captures = list(self._captures.values())
        for capture in captures:
            if quote.symbol in capture.symbols and not capture.future.done():
                capture.quotes.append(quote)

    async def _desired_symbols(self) -> set[str]:
        async with self._lock:
            desired: set[str] = set()
            for capture in self._captures.values():
                desired.update(capture.symbols)
            return desired

    async def _run(self) -> None:
        try:
            async with websockets.connect(
                self.url,
                additional_headers=[("Content-Type", "application/msgpack")],
                open_timeout=5,
                ping_interval=None,
                max_size=None,
            ) as ws:
                await self._await_connection(ws)
                await self._send(ws, {"action": "auth", "key": self.key_id, "secret": self.secret_key})
                await self._await_authentication(ws)
                subscribed_symbols: set[str] = set()
                while True:
                    desired_symbols = await self._desired_symbols()
                    if not desired_symbols:
                        return
                    added_symbols = sorted(desired_symbols - subscribed_symbols)
                    removed_symbols = sorted(subscribed_symbols - desired_symbols)
                    if added_symbols:
                        await self._send(ws, {"action": "subscribe", "quotes": added_symbols})
                    if removed_symbols:
                        await self._send(ws, {"action": "unsubscribe", "quotes": removed_symbols})
                    subscribed_symbols = desired_symbols
                    try:
                        payload = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    for message in self._decode_messages(payload):
                        message_type = message.get("T")
                        if message_type == "error":
                            code = message.get("code", "unknown")
                            detail = message.get("msg", "unknown websocket error")
                            raise RuntimeError(f"Option stream error {code}: {detail}")
                        if message_type != "q":
                            continue
                        quote = self._build_live_option_quote(message)
                        if quote is None:
                            continue
                        await self._record_quote(quote)
        except Exception as exc:
            await self._fail_captures(exc)
        finally:
            async with self._lock:
                self._runner_task = None

    async def _await_connection(self, ws: Any) -> None:
        payload = await ws.recv()
        for message in self._decode_messages(payload):
            if message.get("T") == "success" and message.get("msg") == "connected":
                return
            if message.get("T") == "error":
                code = message.get("code", "unknown")
                detail = message.get("msg", "unknown websocket error")
                raise RuntimeError(f"Option stream error {code}: {detail}")
        raise RuntimeError("Option stream did not acknowledge the connection")

    async def _await_authentication(self, ws: Any) -> None:
        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            payload = await ws.recv()
            for message in self._decode_messages(payload):
                message_type = message.get("T")
                if message_type == "success" and message.get("msg") == "authenticated":
                    return
                if message_type == "error":
                    code = message.get("code", "unknown")
                    detail = message.get("msg", "unknown websocket error")
                    raise RuntimeError(f"Option stream auth error {code}: {detail}")
        raise RuntimeError("Option stream authentication timed out")

    @staticmethod
    async def _send(ws: Any, payload: dict[str, Any]) -> None:
        await ws.send(msgpack.packb(payload, use_bin_type=True))

    @staticmethod
    def _decode_messages(payload: bytes | str) -> list[dict[str, Any]]:
        if isinstance(payload, bytes):
            decoded = msgpack.unpackb(payload, raw=False)
        else:
            decoded = json.loads(payload)
        if isinstance(decoded, dict):
            return [decoded]
        if isinstance(decoded, list):
            return [item for item in decoded if isinstance(item, dict)]
        return []

    @staticmethod
    def _build_live_option_quote(message: dict[str, Any]) -> LiveOptionQuote | None:
        bid = parse_float(message.get("bp"))
        ask = parse_float(message.get("ap"))
        if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
            return None
        return LiveOptionQuote(
            symbol=str(message.get("S")),
            bid=bid,
            ask=ask,
            bid_size=parse_int(message.get("bs")) or 0,
            ask_size=parse_int(message.get("as")) or 0,
            timestamp=format_stream_timestamp(message.get("t")),
        )


class AlpacaOptionQuoteCaptureBroker:
    def __init__(self) -> None:
        self.key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
        self.secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
        self._lock = asyncio.Lock()
        self._brokers: dict[tuple[str, str], _FeedCaptureBroker] = {}

    async def capture_quote_records(
        self,
        *,
        candidates: list[dict[str, Any]],
        feed: str,
        duration_seconds: float,
        data_base_url: str | None = None,
    ) -> list[dict[str, Any]]:
        symbol_metadata = build_quote_symbol_metadata(candidates)
        symbols = list(symbol_metadata.keys())
        if not symbols or duration_seconds <= 0:
            return []
        broker = await self._get_feed_broker(
            feed=str(feed),
            data_base_url=(data_base_url or DEFAULT_DATA_BASE_URL).rstrip("/"),
        )
        quotes = await broker.capture_quotes(
            symbols=symbols,
            duration_seconds=float(duration_seconds),
        )
        return build_quote_records(
            captured_at=_render_captured_at(),
            symbol_metadata=symbol_metadata,
            quotes=quotes,
            source="alpaca_websocket",
        )

    async def aclose(self) -> None:
        async with self._lock:
            brokers = list(self._brokers.values())
            self._brokers.clear()
        for broker in brokers:
            runner_task = broker._runner_task
            if runner_task is None:
                continue
            runner_task.cancel()
            try:
                await runner_task
            except asyncio.CancelledError:
                pass

    async def _get_feed_broker(self, *, feed: str, data_base_url: str) -> _FeedCaptureBroker:
        key = (feed, data_base_url)
        async with self._lock:
            broker = self._brokers.get(key)
            if broker is None:
                broker = _FeedCaptureBroker(
                    key_id=self.key_id,
                    secret_key=self.secret_key,
                    data_base_url=data_base_url,
                    feed=feed,
                )
                self._brokers[key] = broker
            return broker
