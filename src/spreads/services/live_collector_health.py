from __future__ import annotations

from typing import Any, Mapping, Sequence

from spreads.services.selection_terms import normalize_uoa_decision_state


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


def _resolve_profile(profile: Any, *, label: Any = None) -> str | None:
    normalized = str(profile or "").strip().lower()
    if normalized in {"0dte", "weekly", "core", "micro", "swing"}:
        return normalized
    label_text = str(label or "").strip().lower()
    for candidate in ("0dte", "weekly", "core", "micro", "swing"):
        if candidate in label_text:
            return candidate
    return normalized or None


def _with_legacy_count_aliases(
    payload: dict[str, Any],
    *,
    stream_key: str,
    legacy_key: str,
) -> dict[str, Any]:
    count = _read_int(payload, stream_key) or _read_int(payload, legacy_key)
    payload[stream_key] = count
    payload[legacy_key] = count
    return payload


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

    if total <= 0:
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

    if total <= 0:
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


def build_live_action_gate(
    *,
    profile: str | None,
    label: str | None = None,
    quote_capture: Mapping[str, Any] | None,
) -> dict[str, Any]:
    normalized_profile = _resolve_profile(profile, label=label) or ""
    capture = quote_capture if isinstance(quote_capture, Mapping) else {}
    capture_status = str(capture.get("capture_status") or "").strip().lower()

    if normalized_profile == "0dte" and capture_status in {
        "empty",
        "baseline_only",
        "recovery_only",
    }:
        reason_code = {
            "empty": "quote_capture_empty",
            "baseline_only": "quote_capture_baseline_only",
            "recovery_only": "quote_capture_recovery_only",
        }[capture_status]
        return {
            "status": "blocked",
            "reason_code": reason_code,
            "message": (
                "0DTE live actions are blocked because quote capture did not finish healthy "
                f"({capture_status})."
            ),
            "allow_alerts": False,
            "allow_auto_execution": False,
        }

    return {
        "status": "pass",
        "reason_code": None,
        "message": "Live actions are allowed.",
        "allow_alerts": True,
        "allow_auto_execution": True,
    }


def _normalize_uoa_root(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    decision_state = normalize_uoa_decision_state(payload.get("decision_state"))
    if decision_state is not None:
        payload["decision_state"] = decision_state
    return payload


def normalize_uoa_decisions_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    source = {} if not isinstance(payload, Mapping) else dict(payload)
    overview = (
        {}
        if not isinstance(source.get("overview"), Mapping)
        else dict(source.get("overview"))
    )
    normalized_overview = {
        key: value
        for key, value in overview.items()
        if key not in {"watchlist_count", "board_count"}
    }
    normalized_overview.setdefault(
        "monitor_count", overview.get("watchlist_count")
    )
    normalized_overview.setdefault(
        "promotable_count", overview.get("board_count")
    )
    normalized_top_decision_state = normalize_uoa_decision_state(
        normalized_overview.get("top_decision_state")
    )
    if normalized_top_decision_state is not None:
        normalized_overview["top_decision_state"] = normalized_top_decision_state
    roots = [
        _normalize_uoa_root(item)
        for item in list(source.get("roots") or [])
        if isinstance(item, Mapping)
    ]
    top_monitor_roots = [
        _normalize_uoa_root(item)
        for item in list(
            source.get("top_monitor_roots", source.get("top_watchlist_roots")) or []
        )
        if isinstance(item, Mapping)
    ]
    top_promotable_roots = [
        _normalize_uoa_root(item)
        for item in list(
            source.get("top_promotable_roots", source.get("top_board_roots")) or []
        )
        if isinstance(item, Mapping)
    ]
    top_high_roots = [
        _normalize_uoa_root(item)
        for item in list(source.get("top_high_roots") or [])
        if isinstance(item, Mapping)
    ]
    return {
        **{
            key: value
            for key, value in source.items()
            if key not in {"overview", "top_watchlist_roots", "top_board_roots"}
        },
        "overview": normalized_overview,
        "roots": roots,
        "top_monitor_roots": top_monitor_roots,
        "top_promotable_roots": top_promotable_roots,
        "top_high_roots": top_high_roots,
    }


def enrich_live_collector_result(result: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if result is None:
        return None
    enriched = dict(result)
    quote_capture = build_quote_capture_summary(
        expected_quote_symbols=enriched.get("expected_quote_symbols"),
        total_quote_events_saved=_read_int(enriched, "quote_events_saved"),
        baseline_quote_events_saved=_read_int(enriched, "baseline_quote_events_saved"),
        stream_quote_events_saved=_read_int(enriched, "stream_quote_events_saved"),
        websocket_quote_events_saved=_read_int(enriched, "websocket_quote_events_saved"),
        recovery_quote_events_saved=_read_int(enriched, "recovery_quote_events_saved"),
    )
    trade_capture = build_trade_capture_summary(
        expected_trade_symbols=enriched.get("expected_trade_symbols"),
        total_trade_events_saved=_read_int(enriched, "trade_events_saved"),
        stream_trade_events_saved=_read_int(enriched, "stream_trade_events_saved"),
        websocket_trade_events_saved=_read_int(enriched, "websocket_trade_events_saved"),
    )
    enriched["stream_quote_events_saved"] = quote_capture["stream_quote_events_saved"]
    enriched["websocket_quote_events_saved"] = quote_capture["websocket_quote_events_saved"]
    enriched["stream_trade_events_saved"] = trade_capture["stream_trade_events_saved"]
    enriched["websocket_trade_events_saved"] = trade_capture["websocket_trade_events_saved"]
    enriched["quote_capture"] = quote_capture
    enriched["trade_capture"] = trade_capture
    enriched["uoa_decisions"] = normalize_uoa_decisions_payload(
        enriched.get("uoa_decisions")
        if isinstance(enriched.get("uoa_decisions"), Mapping)
        else None
    )
    enriched["live_action_gate"] = dict(
        enriched.get("live_action_gate")
        or build_live_action_gate(
            profile=str(enriched.get("profile") or ""),
            label=str(enriched.get("label") or ""),
            quote_capture=quote_capture,
        )
    )
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
    enriched["uoa_quote_summary"] = result.get("uoa_quote_summary") or {}
    enriched["uoa_decisions"] = result.get("uoa_decisions") or {}
    enriched["capture_status"] = result["quote_capture"]["capture_status"]
    run_payload = enriched.get("payload") if isinstance(enriched.get("payload"), Mapping) else {}
    enriched["live_action_gate"] = dict(
        result.get("live_action_gate")
        or build_live_action_gate(
            profile=str(run_payload.get("profile") or result.get("profile") or ""),
            label=str(
                run_payload.get("label")
                or result.get("label")
                or enriched.get("label")
                or ""
            ),
            quote_capture=result.get("quote_capture"),
        )
    )
    return enriched
