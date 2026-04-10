from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from spreads.services.option_stream_broker import (
    AlpacaOptionStreamBroker,
    LiveOptionTrade,
    default_internal_api_base_url,
    render_option_capture_timestamp,
)
from spreads.services.option_trade_records import (
    build_trade_records,
    build_trade_symbol_metadata,
)
from spreads.services.scanner import DEFAULT_DATA_BASE_URL


def request_option_trade_capture(
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
    request_url = f"{(api_base_url or default_internal_api_base_url()).rstrip('/')}/internal/market-data/option-trades/capture"
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
        raise RuntimeError(f"Option trade capture request failed: {exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Option trade capture request failed: {exc.reason}") from exc
    trades = payload.get("trades")
    if not isinstance(trades, list):
        raise RuntimeError("Option trade capture response did not include a trades list")
    return [dict(item) for item in trades if isinstance(item, dict)]


class AlpacaOptionTradeCaptureBroker:
    def __init__(self, *, option_stream_broker: AlpacaOptionStreamBroker | None = None) -> None:
        self.option_stream_broker = option_stream_broker or AlpacaOptionStreamBroker()
        self._owns_broker = option_stream_broker is None

    async def capture_trade_records(
        self,
        *,
        candidates: list[dict[str, Any]],
        feed: str,
        duration_seconds: float,
        data_base_url: str | None = None,
    ) -> list[dict[str, Any]]:
        symbol_metadata = build_trade_symbol_metadata(candidates)
        symbols = list(symbol_metadata.keys())
        if not symbols or duration_seconds <= 0:
            return []
        result = await self.option_stream_broker.capture(
            symbols=symbols,
            feed=str(feed),
            duration_seconds=float(duration_seconds),
            want_quotes=False,
            want_trades=True,
            data_base_url=(data_base_url or DEFAULT_DATA_BASE_URL).rstrip("/"),
        )
        return build_trade_records(
            captured_at=render_option_capture_timestamp(),
            symbol_metadata=symbol_metadata,
            trades=result.trades,
            source="alpaca_websocket",
        )

    async def aclose(self) -> None:
        if self._owns_broker:
            await self.option_stream_broker.aclose()
