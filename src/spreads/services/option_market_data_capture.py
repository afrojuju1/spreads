from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any

from spreads.services.option_quote_records import build_quote_records, build_quote_symbol_metadata
from spreads.services.option_stream_broker import (
    AlpacaOptionStreamBroker,
    default_internal_api_base_url,
    render_option_capture_timestamp,
)
from spreads.services.option_trade_records import build_trade_records, build_trade_symbol_metadata
from spreads.services.scanner import DEFAULT_DATA_BASE_URL


def request_option_market_data_capture(
    *,
    candidates: list[dict[str, Any]],
    feed: str,
    quote_duration_seconds: float,
    trade_duration_seconds: float,
    data_base_url: str | None = None,
    api_base_url: str | None = None,
) -> dict[str, Any]:
    if not candidates or (quote_duration_seconds <= 0 and trade_duration_seconds <= 0):
        return {
            "quotes": [],
            "trades": [],
            "quote_error": None,
            "trade_error": None,
        }

    request_payload = {
        "candidates": candidates,
        "feed": str(feed),
        "quote_duration_seconds": float(quote_duration_seconds),
        "trade_duration_seconds": float(trade_duration_seconds),
        "data_base_url": data_base_url or DEFAULT_DATA_BASE_URL,
    }
    request_data = json.dumps(request_payload).encode("utf-8")
    request_url = f"{(api_base_url or default_internal_api_base_url()).rstrip('/')}/internal/market-data/options/capture"
    request = urllib.request.Request(
        request_url,
        data=request_data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    timeout_seconds = max(float(max(quote_duration_seconds, trade_duration_seconds)) + 15.0, 20.0)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Option market-data capture request failed: {exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Option market-data capture request failed: {exc.reason}") from exc

    quotes = payload.get("quotes")
    trades = payload.get("trades")
    if not isinstance(quotes, list):
        raise RuntimeError("Option market-data capture response did not include a quotes list")
    if not isinstance(trades, list):
        raise RuntimeError("Option market-data capture response did not include a trades list")
    return {
        "quotes": [dict(item) for item in quotes if isinstance(item, dict)],
        "trades": [dict(item) for item in trades if isinstance(item, dict)],
        "quote_error": None if payload.get("quote_error") in (None, "") else str(payload.get("quote_error")),
        "trade_error": None if payload.get("trade_error") in (None, "") else str(payload.get("trade_error")),
    }


class AlpacaOptionMarketDataCaptureBroker:
    def __init__(self, *, option_stream_broker: AlpacaOptionStreamBroker | None = None) -> None:
        self.option_stream_broker = option_stream_broker or AlpacaOptionStreamBroker()
        self._owns_broker = option_stream_broker is None

    async def capture_market_data_records(
        self,
        *,
        candidates: list[dict[str, Any]],
        feed: str,
        quote_duration_seconds: float,
        trade_duration_seconds: float,
        data_base_url: str | None = None,
    ) -> dict[str, Any]:
        normalized_data_base_url = (data_base_url or DEFAULT_DATA_BASE_URL).rstrip("/")
        quote_symbol_metadata = build_quote_symbol_metadata(candidates) if quote_duration_seconds > 0 else {}
        trade_symbol_metadata = build_trade_symbol_metadata(candidates) if trade_duration_seconds > 0 else {}

        quote_task = None
        trade_task = None
        if quote_symbol_metadata and quote_duration_seconds > 0:
            quote_task = asyncio.create_task(
                self.option_stream_broker.capture(
                    symbols=list(quote_symbol_metadata.keys()),
                    feed=str(feed),
                    duration_seconds=float(quote_duration_seconds),
                    want_quotes=True,
                    want_trades=False,
                    data_base_url=normalized_data_base_url,
                )
            )
        if trade_symbol_metadata and trade_duration_seconds > 0:
            trade_task = asyncio.create_task(
                self.option_stream_broker.capture(
                    symbols=list(trade_symbol_metadata.keys()),
                    feed=str(feed),
                    duration_seconds=float(trade_duration_seconds),
                    want_quotes=False,
                    want_trades=True,
                    data_base_url=normalized_data_base_url,
                )
            )

        captured_at = render_option_capture_timestamp()
        quote_records: list[dict[str, Any]] = []
        trade_records: list[dict[str, Any]] = []
        quote_error: str | None = None
        trade_error: str | None = None

        if quote_task is not None:
            try:
                quote_result = await quote_task
                quote_records = build_quote_records(
                    captured_at=captured_at,
                    symbol_metadata=quote_symbol_metadata,
                    quotes=quote_result.quotes,
                    source="alpaca_websocket",
                )
            except Exception as exc:
                quote_error = str(exc)

        if trade_task is not None:
            try:
                trade_result = await trade_task
                trade_records = build_trade_records(
                    captured_at=captured_at,
                    symbol_metadata=trade_symbol_metadata,
                    trades=trade_result.trades,
                    source="alpaca_websocket",
                )
            except Exception as exc:
                trade_error = str(exc)

        return {
            "quotes": quote_records,
            "trades": trade_records,
            "quote_error": quote_error,
            "trade_error": trade_error,
        }

    async def aclose(self) -> None:
        if self._owns_broker:
            await self.option_stream_broker.aclose()
