from __future__ import annotations

import time as time_module
from datetime import UTC, datetime, timedelta
from typing import Any

from core.integrations.alpaca.client import AlpacaClient
from core.services.option_quote_records import build_quote_records, build_quote_symbol_metadata
from core.storage.run_history_repository import RunHistoryRepository

MARKET_RECORDER_SOURCE = "market_recorder"
MARKET_RECORDER_POLL_SECONDS = 25.0
MARKET_RECORDER_WAIT_GRACE_SECONDS = 10.0
MARKET_RECORDER_QUERY_POLL_SECONDS = 2.0


def collect_latest_quote_records(
    *,
    client: AlpacaClient,
    candidates: list[dict[str, Any]],
    feed: str,
    attempts: int = 1,
    retry_delay_seconds: float = 0.0,
    source: str = "alpaca_latest_quote",
) -> list[dict[str, Any]]:
    if not candidates:
        return []

    symbol_metadata = build_quote_symbol_metadata(candidates)
    stream_symbols = list(symbol_metadata.keys())
    max_attempts = max(int(attempts), 1)
    for attempt in range(max_attempts):
        latest_quotes = client.get_latest_option_quotes(stream_symbols, feed=feed)
        if latest_quotes:
            latest_captured_at = (
                datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
            )
            return build_quote_records(
                captured_at=latest_captured_at,
                symbol_metadata=symbol_metadata,
                quotes=list(latest_quotes.values()),
                source=source,
            )
        if attempt < max_attempts - 1 and retry_delay_seconds > 0:
            time_module.sleep(retry_delay_seconds)
    return []


def collect_recorded_market_data_records(
    *,
    history_store: RunHistoryRepository,
    label: str,
    profile: str,
    expected_quote_symbols: list[str],
    expected_trade_symbols: list[str],
    captured_from: str,
    wait_timeout_seconds: float,
    poll_interval_seconds: float = MARKET_RECORDER_QUERY_POLL_SECONDS,
) -> dict[str, Any]:
    normalized_quote_symbols = sorted(
        {
            str(symbol or "").strip()
            for symbol in expected_quote_symbols
            if str(symbol or "").strip()
        }
    )
    normalized_trade_symbols = sorted(
        {
            str(symbol or "").strip()
            for symbol in expected_trade_symbols
            if str(symbol or "").strip()
        }
    )
    if not normalized_quote_symbols and not normalized_trade_symbols:
        return {
            "quotes": [],
            "trades": [],
            "quote_error": None,
            "trade_error": None,
            "quote_complete": True,
        }

    deadline = datetime.now(UTC) + timedelta(
        seconds=max(float(wait_timeout_seconds), 0.0)
    )
    quote_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    missing_quote_symbols = list(normalized_quote_symbols)

    while True:
        captured_to = (
            datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        )
        if normalized_quote_symbols:
            quote_records = history_store.list_option_quote_events_window(
                option_symbols=normalized_quote_symbols,
                captured_from=captured_from,
                captured_to=captured_to,
                label=label,
                profile=profile,
                sources=MARKET_RECORDER_SOURCE,
            )
        if normalized_trade_symbols:
            trade_records = history_store.list_option_trade_events_window(
                option_symbols=normalized_trade_symbols,
                captured_from=captured_from,
                captured_to=captured_to,
                label=label,
                profile=profile,
                sources=MARKET_RECORDER_SOURCE,
            )
        covered_quote_symbols = {
            str(row.get("option_symbol") or "").strip()
            for row in quote_records
            if str(row.get("option_symbol") or "").strip()
        }
        missing_quote_symbols = [
            symbol
            for symbol in normalized_quote_symbols
            if symbol not in covered_quote_symbols
        ]
        if not missing_quote_symbols or datetime.now(UTC) >= deadline:
            break
        time_module.sleep(max(float(poll_interval_seconds), 0.2))

    quote_complete = not missing_quote_symbols
    quote_error = None
    if missing_quote_symbols:
        quote_error = (
            "Market recorder did not cover "
            f"{len(missing_quote_symbols)}/{len(normalized_quote_symbols)} expected quote symbols before timeout."
        )
    return {
        "quotes": [dict(row) for row in quote_records],
        "trades": [dict(row) for row in trade_records],
        "quote_error": quote_error,
        "trade_error": None,
        "quote_complete": quote_complete,
    }


__all__ = [
    "MARKET_RECORDER_POLL_SECONDS",
    "MARKET_RECORDER_WAIT_GRACE_SECONDS",
    "collect_latest_quote_records",
    "collect_recorded_market_data_records",
]
