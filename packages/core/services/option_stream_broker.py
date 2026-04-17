from __future__ import annotations

import asyncio
import json
import os
import urllib.parse
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import msgpack
import websockets

from core.common import env_or_die, format_stream_timestamp, parse_float, parse_int
from core.domain.models import LiveOptionQuote
from core.integrations.alpaca.client import DEFAULT_DATA_BASE_URL
from core.services.option_trade_records import normalize_trade_conditions

DEFAULT_OPTION_STREAM_IDLE_TIMEOUT_SECONDS = 2.0
OPTION_STREAM_SHUTDOWN_MESSAGE = "Option stream capture cancelled during API shutdown"


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


def normalize_option_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    for item in symbols:
        symbol = str(item).strip()
        if symbol and symbol not in normalized:
            normalized.append(symbol)
    return normalized


def render_option_capture_timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


class OptionStreamCaptureError(RuntimeError):
    def __init__(self, message: str, *, code: str | int | None = None, detail: str | None = None) -> None:
        super().__init__(message)
        self.code = None if code in (None, "") else str(code)
        self.detail = detail


class OptionStreamShutdownError(OptionStreamCaptureError):
    pass


@dataclass(frozen=True)
class LiveOptionTrade:
    symbol: str
    price: float
    size: int
    exchange_code: str | None
    conditions: list[str]
    timestamp: str | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class OptionStreamCaptureResult:
    quotes: list[LiveOptionQuote] = field(default_factory=list)
    trades: list[LiveOptionTrade] = field(default_factory=list)


@dataclass
class _ActiveOptionCapture:
    capture_id: str
    symbols: set[str]
    want_quotes: bool
    want_trades: bool
    future: asyncio.Future[OptionStreamCaptureResult]
    quotes: list[LiveOptionQuote] = field(default_factory=list)
    trades: list[LiveOptionTrade] = field(default_factory=list)


class _FeedOptionStreamBroker:
    def __init__(
        self,
        *,
        key_id: str,
        secret_key: str,
        data_base_url: str,
        feed: str,
        idle_timeout_seconds: float,
    ) -> None:
        self.key_id = key_id
        self.secret_key = secret_key
        self.data_base_url = data_base_url
        self.feed = feed
        self.url = resolve_option_stream_url(data_base_url=data_base_url, feed=feed)
        self.idle_timeout_seconds = max(float(idle_timeout_seconds), 0.0)
        self._captures: dict[str, _ActiveOptionCapture] = {}
        self._capture_counter = 0
        self._lock = asyncio.Lock()
        self._runner_task: asyncio.Task[None] | None = None
        self._connect_count = 0
        self._reconnect_count = 0
        self._auth_406_count = 0
        self._shutdown_cancel_count = 0
        self._last_error: str | None = None
        self._last_error_at: str | None = None

    async def capture(
        self,
        *,
        symbols: list[str],
        duration_seconds: float,
        want_quotes: bool,
        want_trades: bool,
    ) -> OptionStreamCaptureResult:
        normalized_symbols = set(normalize_option_symbols(symbols))
        if not normalized_symbols or duration_seconds <= 0 or (not want_quotes and not want_trades):
            return OptionStreamCaptureResult()

        loop = asyncio.get_running_loop()
        future: asyncio.Future[OptionStreamCaptureResult] = loop.create_future()
        async with self._lock:
            self._capture_counter += 1
            capture_id = f"{self.feed}:{self._capture_counter}"
            self._captures[capture_id] = _ActiveOptionCapture(
                capture_id=capture_id,
                symbols=normalized_symbols,
                want_quotes=want_quotes,
                want_trades=want_trades,
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

    async def aclose(self) -> None:
        runner_task: asyncio.Task[None] | None
        async with self._lock:
            runner_task = self._runner_task
        if runner_task is None:
            await self._fail_captures(OptionStreamShutdownError(OPTION_STREAM_SHUTDOWN_MESSAGE))
            return
        runner_task.cancel()
        try:
            await runner_task
        except asyncio.CancelledError:
            pass

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            desired_quotes: set[str] = set()
            desired_trades: set[str] = set()
            for capture in self._captures.values():
                if capture.want_quotes:
                    desired_quotes.update(capture.symbols)
                if capture.want_trades:
                    desired_trades.update(capture.symbols)
            return {
                "feed": self.feed,
                "data_base_url": self.data_base_url,
                "stream_url": self.url,
                "connection_active": self._runner_task is not None and not self._runner_task.done(),
                "active_capture_count": len(self._captures),
                "desired_quote_symbol_count": len(desired_quotes),
                "desired_trade_symbol_count": len(desired_trades),
                "metrics": {
                    "connect_count": self._connect_count,
                    "reconnect_count": self._reconnect_count,
                    "auth_406_count": self._auth_406_count,
                    "shutdown_cancel_count": self._shutdown_cancel_count,
                    "last_error": self._last_error,
                    "last_error_at": self._last_error_at,
                },
            }

    async def _complete_capture_after(self, capture_id: str, duration_seconds: float) -> None:
        try:
            await asyncio.sleep(max(float(duration_seconds), 0.5))
        except asyncio.CancelledError:
            return
        async with self._lock:
            capture = self._captures.pop(capture_id, None)
            if capture is not None and not capture.future.done():
                capture.future.set_result(
                    OptionStreamCaptureResult(
                        quotes=list(capture.quotes),
                        trades=list(capture.trades),
                    )
                )

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
            if capture.want_quotes and quote.symbol in capture.symbols and not capture.future.done():
                capture.quotes.append(quote)

    async def _record_trade(self, trade: LiveOptionTrade) -> None:
        async with self._lock:
            captures = list(self._captures.values())
        for capture in captures:
            if capture.want_trades and trade.symbol in capture.symbols and not capture.future.done():
                capture.trades.append(trade)

    async def _desired_symbols(self) -> tuple[set[str], set[str]]:
        async with self._lock:
            desired_quotes: set[str] = set()
            desired_trades: set[str] = set()
            for capture in self._captures.values():
                if capture.want_quotes:
                    desired_quotes.update(capture.symbols)
                if capture.want_trades:
                    desired_trades.update(capture.symbols)
            return desired_quotes, desired_trades

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
                await self._mark_connected()
                subscribed_quotes: set[str] = set()
                subscribed_trades: set[str] = set()
                idle_deadline: float | None = None
                loop = asyncio.get_running_loop()
                while True:
                    desired_quotes, desired_trades = await self._desired_symbols()

                    added_quotes = sorted(desired_quotes - subscribed_quotes)
                    removed_quotes = sorted(subscribed_quotes - desired_quotes)
                    added_trades = sorted(desired_trades - subscribed_trades)
                    removed_trades = sorted(subscribed_trades - desired_trades)

                    if added_quotes or added_trades:
                        payload: dict[str, Any] = {"action": "subscribe"}
                        if added_quotes:
                            payload["quotes"] = added_quotes
                        if added_trades:
                            payload["trades"] = added_trades
                        await self._send(ws, payload)
                    if removed_quotes or removed_trades:
                        payload = {"action": "unsubscribe"}
                        if removed_quotes:
                            payload["quotes"] = removed_quotes
                        if removed_trades:
                            payload["trades"] = removed_trades
                        await self._send(ws, payload)

                    subscribed_quotes = set(desired_quotes)
                    subscribed_trades = set(desired_trades)

                    if not desired_quotes and not desired_trades:
                        if idle_deadline is None:
                            idle_deadline = loop.time() + self.idle_timeout_seconds
                        remaining = idle_deadline - loop.time()
                        if remaining <= 0:
                            return
                        timeout_seconds = min(1.0, remaining)
                    else:
                        idle_deadline = None
                        timeout_seconds = 1.0

                    try:
                        payload = await asyncio.wait_for(ws.recv(), timeout=timeout_seconds)
                    except asyncio.TimeoutError:
                        continue

                    for message in self._decode_messages(payload):
                        message_type = message.get("T")
                        if message_type == "error":
                            code = message.get("code", "unknown")
                            detail = message.get("msg", "unknown websocket error")
                            raise OptionStreamCaptureError(
                                f"Option stream error {code}: {detail}",
                                code=code,
                                detail=detail,
                            )
                        if message_type == "q":
                            quote = self._build_live_option_quote(message)
                            if quote is not None:
                                await self._record_quote(quote)
                        elif message_type == "t":
                            trade = self._build_live_option_trade(message)
                            if trade is not None:
                                await self._record_trade(trade)
        except asyncio.CancelledError:
            await self._record_shutdown_cancel()
            await self._fail_captures(OptionStreamShutdownError(OPTION_STREAM_SHUTDOWN_MESSAGE))
            raise
        except Exception as exc:
            await self._record_error(exc)
            await self._fail_captures(exc if isinstance(exc, OptionStreamCaptureError) else OptionStreamCaptureError(str(exc)))
        finally:
            async with self._lock:
                self._runner_task = None

    async def _mark_connected(self) -> None:
        async with self._lock:
            self._connect_count += 1
            if self._connect_count > 1:
                self._reconnect_count += 1

    async def _record_shutdown_cancel(self) -> None:
        async with self._lock:
            self._shutdown_cancel_count += 1

    async def _record_error(self, exc: Exception) -> None:
        rendered = str(exc)
        async with self._lock:
            if isinstance(exc, OptionStreamCaptureError) and exc.code == "406":
                self._auth_406_count += 1
            self._last_error = rendered
            self._last_error_at = render_option_capture_timestamp()

    async def _await_connection(self, ws: Any) -> None:
        payload = await ws.recv()
        for message in self._decode_messages(payload):
            if message.get("T") == "success" and message.get("msg") == "connected":
                return
            if message.get("T") == "error":
                code = message.get("code", "unknown")
                detail = message.get("msg", "unknown websocket error")
                raise OptionStreamCaptureError(f"Option stream error {code}: {detail}", code=code, detail=detail)
        raise OptionStreamCaptureError("Option stream did not acknowledge the connection")

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
                    raise OptionStreamCaptureError(
                        f"Option stream auth error {code}: {detail}",
                        code=code,
                        detail=detail,
                    )
        raise OptionStreamCaptureError("Option stream authentication timed out")

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

    @staticmethod
    def _build_live_option_trade(message: dict[str, Any]) -> LiveOptionTrade | None:
        symbol = str(message.get("S") or "").strip()
        price = parse_float(message.get("p"))
        size = parse_int(message.get("s"))
        if not symbol or price is None or price <= 0 or size is None or size <= 0:
            return None
        exchange_code = None if message.get("x") in (None, "") else str(message.get("x"))
        conditions = normalize_trade_conditions(message.get("c"))
        raw_payload = dict(message)
        raw_payload["S"] = symbol
        raw_payload["p"] = price
        raw_payload["s"] = size
        raw_payload["x"] = exchange_code
        raw_payload["c"] = list(conditions)
        raw_payload["t"] = format_stream_timestamp(message.get("t"))
        return LiveOptionTrade(
            symbol=symbol,
            price=price,
            size=size,
            exchange_code=exchange_code,
            conditions=conditions,
            timestamp=format_stream_timestamp(message.get("t")),
            raw_payload=raw_payload,
        )


class AlpacaOptionStreamBroker:
    def __init__(
        self,
        *,
        key_id: str | None = None,
        secret_key: str | None = None,
        idle_timeout_seconds: float = DEFAULT_OPTION_STREAM_IDLE_TIMEOUT_SECONDS,
    ) -> None:
        self.key_id = key_id or env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
        self.secret_key = secret_key or env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
        self.idle_timeout_seconds = idle_timeout_seconds
        self._lock = asyncio.Lock()
        self._brokers: dict[tuple[str, str], _FeedOptionStreamBroker] = {}

    async def capture(
        self,
        *,
        symbols: list[str],
        feed: str,
        duration_seconds: float,
        want_quotes: bool,
        want_trades: bool,
        data_base_url: str | None = None,
    ) -> OptionStreamCaptureResult:
        normalized_data_base_url = (data_base_url or DEFAULT_DATA_BASE_URL).rstrip("/")
        broker = await self._get_feed_broker(feed=str(feed), data_base_url=normalized_data_base_url)
        return await broker.capture(
            symbols=symbols,
            duration_seconds=float(duration_seconds),
            want_quotes=want_quotes,
            want_trades=want_trades,
        )

    async def aclose(self) -> None:
        async with self._lock:
            brokers = list(self._brokers.values())
            self._brokers.clear()
        for broker in brokers:
            await broker.aclose()

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            brokers = list(self._brokers.values())
        broker_snapshots = [await broker.snapshot() for broker in brokers]
        return {
            "generated_at": render_option_capture_timestamp(),
            "broker_count": len(broker_snapshots),
            "active_capture_count": sum(int(snapshot.get("active_capture_count") or 0) for snapshot in broker_snapshots),
            "desired_quote_symbol_count": sum(
                int(snapshot.get("desired_quote_symbol_count") or 0) for snapshot in broker_snapshots
            ),
            "desired_trade_symbol_count": sum(
                int(snapshot.get("desired_trade_symbol_count") or 0) for snapshot in broker_snapshots
            ),
            "metrics": {
                "connect_count": sum(
                    int((snapshot.get("metrics") or {}).get("connect_count") or 0) for snapshot in broker_snapshots
                ),
                "reconnect_count": sum(
                    int((snapshot.get("metrics") or {}).get("reconnect_count") or 0) for snapshot in broker_snapshots
                ),
                "auth_406_count": sum(
                    int((snapshot.get("metrics") or {}).get("auth_406_count") or 0) for snapshot in broker_snapshots
                ),
                "shutdown_cancel_count": sum(
                    int((snapshot.get("metrics") or {}).get("shutdown_cancel_count") or 0)
                    for snapshot in broker_snapshots
                ),
            },
            "brokers": broker_snapshots,
        }

    async def _get_feed_broker(self, *, feed: str, data_base_url: str) -> _FeedOptionStreamBroker:
        key = (feed, data_base_url)
        async with self._lock:
            broker = self._brokers.get(key)
            if broker is None:
                broker = _FeedOptionStreamBroker(
                    key_id=self.key_id,
                    secret_key=self.secret_key,
                    data_base_url=data_base_url,
                    feed=feed,
                    idle_timeout_seconds=self.idle_timeout_seconds,
                )
                self._brokers[key] = broker
            return broker
