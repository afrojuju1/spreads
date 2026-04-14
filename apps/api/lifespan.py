from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from spreads.services.option_market_data_capture import AlpacaOptionMarketDataCaptureBroker
from spreads.services.option_stream_broker import AlpacaOptionStreamBroker


@asynccontextmanager
async def api_lifespan(app: FastAPI):
    app.state.option_stream_broker = AlpacaOptionStreamBroker()
    app.state.option_market_data_capture_broker = AlpacaOptionMarketDataCaptureBroker(
        option_stream_broker=app.state.option_stream_broker
    )
    try:
        yield
    finally:
        await app.state.option_stream_broker.aclose()
