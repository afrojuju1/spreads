from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .shared import (
    _with_legacy_count_aliases,
    normalize_expected_quote_symbols,
    normalize_expected_trade_symbols,
)


def build_quote_capture_summary(
    *,
    expected_quote_symbols: Sequence[str] | None,
    total_quote_events_saved: int,
    baseline_quote_events_saved: int,
    stream_quote_events_saved: int = 0,
    websocket_quote_events_saved: int | None = None,
    recovery_quote_events_saved: int = 0,
) -> dict[str, Any]:
    expected_symbols = normalize_expected_quote_symbols(expected_quote_symbols)
    total = max(int(total_quote_events_saved), 0)
    baseline = max(int(baseline_quote_events_saved), 0)
    stream = max(
        int(
            stream_quote_events_saved
            if stream_quote_events_saved
            else (websocket_quote_events_saved or 0)
        ),
        0,
    )
    recovery = max(int(recovery_quote_events_saved), 0)
    recovery_used = recovery > 0

    if total <= 0 and not expected_symbols:
        capture_status = "idle"
    elif total <= 0:
        capture_status = "empty"
    elif stream > 0:
        capture_status = "healthy"
    elif recovery_used:
        capture_status = "recovery_only"
    else:
        capture_status = "baseline_only"

    return _with_legacy_count_aliases(
        {
            "capture_status": capture_status,
            "expected_quote_symbols": expected_symbols,
            "expected_quote_symbol_count": len(expected_symbols),
            "total_quote_events_saved": total,
            "baseline_quote_events_saved": baseline,
            "stream_quote_events_saved": stream,
            "recovery_quote_events_saved": recovery,
            "recovery_used": recovery_used,
        },
        stream_key="stream_quote_events_saved",
        legacy_key="websocket_quote_events_saved",
    )


def build_trade_capture_summary(
    *,
    expected_trade_symbols: Sequence[str] | None,
    total_trade_events_saved: int,
    stream_trade_events_saved: int = 0,
    websocket_trade_events_saved: int | None = None,
) -> dict[str, Any]:
    expected_symbols = normalize_expected_trade_symbols(expected_trade_symbols)
    total = max(int(total_trade_events_saved), 0)
    stream = max(
        int(
            stream_trade_events_saved
            if stream_trade_events_saved
            else (websocket_trade_events_saved or 0)
        ),
        0,
    )

    if total <= 0 and not expected_symbols:
        capture_status = "idle"
    elif total <= 0:
        capture_status = "empty"
    elif stream > 0:
        capture_status = "healthy"
    else:
        capture_status = "baseline_only"

    return _with_legacy_count_aliases(
        {
            "capture_status": capture_status,
            "expected_trade_symbols": expected_symbols,
            "expected_trade_symbol_count": len(expected_symbols),
            "total_trade_events_saved": total,
            "stream_trade_events_saved": stream,
        },
        stream_key="stream_trade_events_saved",
        legacy_key="websocket_trade_events_saved",
    )


__all__ = ["build_quote_capture_summary", "build_trade_capture_summary"]
