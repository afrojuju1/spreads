from __future__ import annotations

import os

from core.common import env_or_die, load_local_env
from core.integrations.alpaca.client import (
    AlpacaClient,
    DEFAULT_DATA_BASE_URL,
    infer_trading_base_url,
)


def create_alpaca_client_from_env(*, request_timeout_seconds: float | None = None) -> AlpacaClient:
    load_local_env()
    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    trading_base_url = infer_trading_base_url(key_id, os.environ.get("ALPACA_TRADING_BASE_URL"))
    data_base_url = os.environ.get("ALPACA_DATA_BASE_URL", DEFAULT_DATA_BASE_URL)
    return AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=trading_base_url,
        data_base_url=data_base_url,
        request_timeout_seconds=30.0 if request_timeout_seconds is None else float(request_timeout_seconds),
    )


def resolve_trading_environment(trading_base_url: str) -> str:
    lowered = trading_base_url.lower()
    if "paper-api.alpaca.markets" in lowered:
        return "paper"
    if "api.alpaca.markets" in lowered:
        return "live"
    return "custom"
