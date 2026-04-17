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


def _read_float(mapping: Mapping[str, Any] | None, key: str) -> float | None:
    if mapping is None:
        return None
    value = mapping.get(key)
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _read_text(value: Any) -> str | None:
    rendered = str(value or "").strip()
    return rendered or None


def _normalize_text_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item for item in (_read_text(entry) for entry in value) if item is not None]


def _symbol_from_opportunity_id(value: Any) -> str | None:
    text = _read_text(value)
    if text is None:
        return None
    parts = text.split(":")
    if len(parts) < 5 or parts[0] != "opportunity":
        return None
    return _read_text(parts[3])


def _strategy_from_opportunity_id(value: Any) -> str | None:
    text = _read_text(value)
    if text is None:
        return None
    parts = text.split(":")
    if len(parts) < 6 or parts[0] != "opportunity":
        return None
    return _read_text(parts[4])


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


def normalize_capture_status(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized or None


def normalize_tradeability_state(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized or None
