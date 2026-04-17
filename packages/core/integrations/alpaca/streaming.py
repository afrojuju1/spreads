from __future__ import annotations

import json
import time as time_module
import urllib.parse
from typing import Any

import msgpack
import websocket

from core.common import format_stream_timestamp, parse_float, parse_int
from core.domain.models import LiveOptionQuote


class AlpacaOptionQuoteStreamer:
    def __init__(
        self, *, key_id: str, secret_key: str, data_base_url: str, feed: str
    ) -> None:
        self.key_id = key_id
        self.secret_key = secret_key
        self.feed = feed
        parsed = urllib.parse.urlparse(data_base_url)
        hostname = parsed.netloc.lower()
        if "sandbox" in hostname:
            self.url = f"wss://stream.data.sandbox.alpaca.markets/v1beta1/{feed}"
        else:
            self.url = f"wss://stream.data.alpaca.markets/v1beta1/{feed}"

    def stream_quotes(
        self,
        symbols: list[str],
        *,
        duration_seconds: float,
    ) -> dict[str, LiveOptionQuote]:
        latest_quotes: dict[str, LiveOptionQuote] = {}
        for quote in self.collect_quote_events(
            symbols, duration_seconds=duration_seconds
        ):
            latest_quotes[quote.symbol] = quote
        return latest_quotes

    def collect_quote_events(
        self,
        symbols: list[str],
        *,
        duration_seconds: float,
    ) -> list[LiveOptionQuote]:
        if not symbols:
            return []

        quote_events: list[LiveOptionQuote] = []
        ws = websocket.create_connection(
            self.url,
            timeout=5,
            header=["Content-Type: application/msgpack"],
        )
        try:
            self._await_connection(ws)
            self._send(
                ws, {"action": "auth", "key": self.key_id, "secret": self.secret_key}
            )
            self._await_authentication(ws)
            self._send(ws, {"action": "subscribe", "quotes": symbols})
            deadline = time_module.monotonic() + max(duration_seconds, 0.5)
            while time_module.monotonic() < deadline:
                remaining = deadline - time_module.monotonic()
                ws.settimeout(max(min(remaining, 1.0), 0.1))
                try:
                    messages = self._recv_messages(ws)
                except websocket.WebSocketTimeoutException:
                    continue
                for message in messages:
                    message_type = message.get("T")
                    if message_type == "error":
                        code = message.get("code", "unknown")
                        detail = message.get("msg", "unknown websocket error")
                        raise RuntimeError(f"Option stream error {code}: {detail}")
                    if message_type != "q":
                        continue
                    bid = parse_float(message.get("bp"))
                    ask = parse_float(message.get("ap"))
                    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
                        continue
                    quote_events.append(
                        LiveOptionQuote(
                            symbol=str(message.get("S")),
                            bid=bid,
                            ask=ask,
                            bid_size=parse_int(message.get("bs")) or 0,
                            ask_size=parse_int(message.get("as")) or 0,
                            timestamp=format_stream_timestamp(message.get("t")),
                        )
                    )
        finally:
            ws.close()
        return quote_events

    def _await_connection(self, ws: websocket.WebSocket) -> None:
        messages = self._recv_messages(ws)
        for message in messages:
            if message.get("T") == "success" and message.get("msg") == "connected":
                return
            if message.get("T") == "error":
                code = message.get("code", "unknown")
                detail = message.get("msg", "unknown websocket error")
                raise RuntimeError(f"Option stream error {code}: {detail}")
        raise RuntimeError("Option stream did not acknowledge the connection")

    def _await_authentication(self, ws: websocket.WebSocket) -> None:
        deadline = time_module.monotonic() + 5
        while time_module.monotonic() < deadline:
            messages = self._recv_messages(ws)
            for message in messages:
                message_type = message.get("T")
                if message_type == "success" and message.get("msg") == "authenticated":
                    return
                if message_type == "error":
                    code = message.get("code", "unknown")
                    detail = message.get("msg", "unknown websocket error")
                    raise RuntimeError(f"Option stream auth error {code}: {detail}")
        raise RuntimeError("Option stream authentication timed out")

    @staticmethod
    def _send(ws: websocket.WebSocket, payload: dict[str, Any]) -> None:
        ws.send(
            msgpack.packb(payload, use_bin_type=True),
            opcode=websocket.ABNF.OPCODE_BINARY,
        )

    @staticmethod
    def _recv_messages(ws: websocket.WebSocket) -> list[dict[str, Any]]:
        payload = ws.recv()
        if isinstance(payload, bytes):
            decoded = msgpack.unpackb(payload, raw=False)
        else:
            decoded = json.loads(payload)
        if isinstance(decoded, dict):
            return [decoded]
        if isinstance(decoded, list):
            return [item for item in decoded if isinstance(item, dict)]
        return []


__all__ = ["AlpacaOptionQuoteStreamer"]
