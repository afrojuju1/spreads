from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request

from apps.api.errors import bad_request_error, service_unavailable_error
from apps.api.schemas.internal_market_data import OptionMarketDataCaptureRequest
from spreads.services.option_stream_broker import OPTION_STREAM_SHUTDOWN_MESSAGE

router = APIRouter()


@router.post("/internal/market-data/options/capture")
async def capture_option_market_data_route(
    payload: OptionMarketDataCaptureRequest,
    request: Request,
) -> dict[str, object]:
    if payload.quote_duration_seconds <= 0 and payload.trade_duration_seconds <= 0:
        raise bad_request_error("At least one option capture duration must be greater than zero")

    broker = request.app.state.option_market_data_capture_broker
    try:
        return await broker.capture_market_data_records(
            candidates=list(payload.candidates),
            feed=payload.feed,
            quote_duration_seconds=payload.quote_duration_seconds,
            trade_duration_seconds=payload.trade_duration_seconds,
            data_base_url=payload.data_base_url,
        )
    except asyncio.CancelledError as exc:
        raise service_unavailable_error(OPTION_STREAM_SHUTDOWN_MESSAGE) from exc
    except Exception as exc:
        raise service_unavailable_error(exc) from exc


@router.get("/internal/market-data/options/stream-health")
async def get_option_stream_health_route(request: Request) -> dict[str, object]:
    return await request.app.state.option_stream_broker.snapshot()
