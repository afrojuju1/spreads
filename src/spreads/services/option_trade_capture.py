from __future__ import annotations

from typing import Any

from spreads.services.option_market_data_capture import request_option_market_data_capture
from spreads.services.option_stream_broker import (
    AlpacaOptionStreamBroker,
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

    payload = request_option_market_data_capture(
        candidates=candidates,
        feed=feed,
        quote_duration_seconds=0.0,
        trade_duration_seconds=duration_seconds,
        data_base_url=data_base_url or DEFAULT_DATA_BASE_URL,
        api_base_url=api_base_url,
    )
    trade_error = payload.get("trade_error")
    if trade_error not in (None, ""):
        raise RuntimeError(f"Option trade capture request failed: {trade_error}")
    trades = payload.get("trades") or []
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
