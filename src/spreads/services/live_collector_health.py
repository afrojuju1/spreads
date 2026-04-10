from __future__ import annotations

from typing import Any, Mapping, Sequence


def _read_int(mapping: Mapping[str, Any] | None, key: str) -> int:
    if mapping is None:
        return 0
    value = mapping.get(key)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def normalize_expected_symbols(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        symbol = item.strip()
        if symbol and symbol not in normalized:
            normalized.append(symbol)
    return normalized


def normalize_expected_quote_symbols(value: Any) -> list[str]:
    return normalize_expected_symbols(value)


def normalize_expected_trade_symbols(value: Any) -> list[str]:
    return normalize_expected_symbols(value)


def build_quote_capture_summary(
    *,
    expected_quote_symbols: Sequence[str] | None,
    total_quote_events_saved: int,
    baseline_quote_events_saved: int,
    websocket_quote_events_saved: int,
    recovery_quote_events_saved: int = 0,
) -> dict[str, Any]:
    expected_symbols = normalize_expected_quote_symbols(expected_quote_symbols)
    total = max(int(total_quote_events_saved), 0)
    baseline = max(int(baseline_quote_events_saved), 0)
    websocket = max(int(websocket_quote_events_saved), 0)
    recovery = max(int(recovery_quote_events_saved), 0)
    recovery_used = recovery > 0

    if total <= 0:
        capture_status = "empty"
    elif websocket > 0:
        capture_status = "healthy"
    elif recovery_used:
        capture_status = "recovery_only"
    else:
        capture_status = "baseline_only"

    return {
        "capture_status": capture_status,
        "expected_quote_symbols": expected_symbols,
        "expected_quote_symbol_count": len(expected_symbols),
        "total_quote_events_saved": total,
        "baseline_quote_events_saved": baseline,
        "websocket_quote_events_saved": websocket,
        "recovery_quote_events_saved": recovery,
        "recovery_used": recovery_used,
    }


def build_trade_capture_summary(
    *,
    expected_trade_symbols: Sequence[str] | None,
    total_trade_events_saved: int,
    websocket_trade_events_saved: int,
) -> dict[str, Any]:
    expected_symbols = normalize_expected_trade_symbols(expected_trade_symbols)
    total = max(int(total_trade_events_saved), 0)
    websocket = max(int(websocket_trade_events_saved), 0)

    if total <= 0:
        capture_status = "empty"
    elif websocket > 0:
        capture_status = "healthy"
    else:
        capture_status = "baseline_only"

    return {
        "capture_status": capture_status,
        "expected_trade_symbols": expected_symbols,
        "expected_trade_symbol_count": len(expected_symbols),
        "total_trade_events_saved": total,
        "websocket_trade_events_saved": websocket,
    }


def enrich_live_collector_result(result: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if result is None:
        return None
    enriched = dict(result)
    quote_capture = build_quote_capture_summary(
        expected_quote_symbols=enriched.get("expected_quote_symbols"),
        total_quote_events_saved=_read_int(enriched, "quote_events_saved"),
        baseline_quote_events_saved=_read_int(enriched, "baseline_quote_events_saved"),
        websocket_quote_events_saved=_read_int(enriched, "websocket_quote_events_saved"),
        recovery_quote_events_saved=_read_int(enriched, "recovery_quote_events_saved"),
    )
    trade_capture = build_trade_capture_summary(
        expected_trade_symbols=enriched.get("expected_trade_symbols"),
        total_trade_events_saved=_read_int(enriched, "trade_events_saved"),
        websocket_trade_events_saved=_read_int(enriched, "websocket_trade_events_saved"),
    )
    enriched["quote_capture"] = quote_capture
    enriched["trade_capture"] = trade_capture
    return enriched


def enrich_live_collector_job_run_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    enriched = dict(payload)
    if enriched.get("job_type") != "live_collector":
        return enriched
    result = enrich_live_collector_result(
        enriched.get("result") if isinstance(enriched.get("result"), Mapping) else None
    )
    if result is None:
        return enriched
    enriched["result"] = result
    enriched["quote_capture"] = result["quote_capture"]
    enriched["trade_capture"] = result["trade_capture"]
    enriched["uoa_summary"] = result.get("uoa_summary") or {}
    enriched["uoa_decisions"] = result.get("uoa_decisions") or {}
    enriched["capture_status"] = result["quote_capture"]["capture_status"]
    return enriched
