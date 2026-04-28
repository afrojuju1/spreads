from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from core.storage.serializers import parse_datetime

MAX_QUOTE_LOOKBACK_MS = 2_000.0
MAX_QUOTE_LOOKAHEAD_MS = 250.0


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_quote_indexes(
    quotes: Sequence[Mapping[str, Any]] | None,
) -> dict[str, list[tuple[float, dict[str, Any]]]]:
    indexed: dict[str, list[tuple[float, dict[str, Any]]]] = defaultdict(list)
    for row in quotes or []:
        if not isinstance(row, Mapping):
            continue
        option_symbol = str(row.get("option_symbol") or "").strip()
        if not option_symbol:
            continue
        quote_ts = parse_datetime(row.get("quote_timestamp")) or parse_datetime(
            row.get("captured_at")
        )
        if quote_ts is None:
            continue
        indexed[option_symbol].append((quote_ts.timestamp(), dict(row)))
    for option_symbol in indexed:
        indexed[option_symbol].sort(key=lambda item: item[0])
    return indexed


def _quote_metrics(row: Mapping[str, Any]) -> tuple[float | None, float | None, float | None]:
    bid = _as_float(row.get("bid"))
    ask = _as_float(row.get("ask"))
    midpoint = _as_float(row.get("midpoint"))
    if midpoint is None and bid is not None and ask is not None:
        midpoint = (bid + ask) / 2.0
    if bid is None or ask is None:
        spread = None
    else:
        spread = max(ask - bid, 0.0)
    return bid, ask, midpoint if midpoint is None else round(midpoint, 6), spread


def _classify_aggressor(
    *,
    price: float | None,
    bid: float | None,
    ask: float | None,
    midpoint: float | None,
    spread: float | None,
    fallback_after_quote: bool,
) -> tuple[str, str]:
    if (
        price is None
        or bid is None
        or ask is None
        or midpoint is None
        or spread is None
        or ask <= 0
        or bid <= 0
        or ask <= bid
    ):
        return "unknown", "unknown"

    touch_epsilon = max(0.01, spread * 0.10)
    midpoint_epsilon = max(0.005, spread * 0.15)
    if price >= ask - touch_epsilon:
        return "buy", "low" if fallback_after_quote else "high"
    if price <= bid + touch_epsilon:
        return "sell", "low" if fallback_after_quote else "high"
    if price > midpoint + midpoint_epsilon:
        return "buy", "low" if fallback_after_quote else "medium"
    if price < midpoint - midpoint_epsilon:
        return "sell", "low" if fallback_after_quote else "medium"
    return "unknown", "unknown"


def _match_quote(
    *,
    trade_timestamp_seconds: float | None,
    quote_rows: list[tuple[float, dict[str, Any]]] | None,
) -> tuple[dict[str, Any] | None, str | None, float | None]:
    if trade_timestamp_seconds is None or not quote_rows:
        return None, None, None
    timestamps = [item[0] for item in quote_rows]
    position = bisect_right(timestamps, trade_timestamp_seconds)

    before = quote_rows[position - 1] if position > 0 else None
    if before is not None:
        age_ms = (trade_timestamp_seconds - before[0]) * 1000.0
        if 0.0 <= age_ms <= MAX_QUOTE_LOOKBACK_MS:
            return dict(before[1]), "at_or_before_trade", round(age_ms, 2)

    after = quote_rows[position] if position < len(quote_rows) else None
    if after is not None:
        age_ms = (after[0] - trade_timestamp_seconds) * 1000.0
        if 0.0 <= age_ms <= MAX_QUOTE_LOOKAHEAD_MS:
            return dict(after[1]), "after_trade_fallback", round(age_ms, 2)
    return None, None, None


def _side_sign(value: str) -> float:
    if value == "buy":
        return 1.0
    if value == "sell":
        return -1.0
    return 0.0


def _signed_greek_notional(
    *,
    sign: float,
    greek_value: float | None,
    underlying_price: float | None,
    size: int,
    greek_type: str,
) -> float | None:
    if sign == 0.0 or greek_value is None:
        return 0.0 if sign == 0.0 else None
    if greek_type == "delta":
        if underlying_price is None:
            return None
        return round(sign * greek_value * underlying_price * 100.0 * size, 4)
    if greek_type == "vega":
        return round(sign * greek_value * 100.0 * size, 4)
    if greek_type == "gamma":
        if underlying_price is None:
            return None
        return round(
            sign * greek_value * underlying_price * underlying_price * 100.0 * size,
            4,
        )
    return None


def enrich_uoa_trade_records(
    *,
    trades: Sequence[Mapping[str, Any]] | None,
    quotes: Sequence[Mapping[str, Any]] | None,
    contract_metadata_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    metadata_map = (
        {}
        if contract_metadata_by_symbol is None
        else {str(symbol): dict(payload) for symbol, payload in contract_metadata_by_symbol.items()}
    )
    quote_indexes = _build_quote_indexes(quotes)
    enriched_rows: list[dict[str, Any]] = []

    for trade in trades or []:
        if not isinstance(trade, Mapping):
            continue
        row = dict(trade)
        option_symbol = str(row.get("option_symbol") or "").strip()
        trade_ts = parse_datetime(row.get("trade_timestamp")) or parse_datetime(
            row.get("captured_at")
        )
        trade_ts_seconds = None if trade_ts is None else trade_ts.timestamp()
        matched_quote, match_source, quote_match_age_ms = _match_quote(
            trade_timestamp_seconds=trade_ts_seconds,
            quote_rows=quote_indexes.get(option_symbol),
        )
        bid, ask, matched_midpoint, matched_spread = _quote_metrics(
            {} if matched_quote is None else matched_quote
        )
        aggressor_side, aggressor_confidence = _classify_aggressor(
            price=_as_float(row.get("price")),
            bid=bid,
            ask=ask,
            midpoint=matched_midpoint,
            spread=matched_spread,
            fallback_after_quote=match_source == "after_trade_fallback",
        )
        sign = _side_sign(aggressor_side)
        metadata = metadata_map.get(option_symbol) or {}
        size = int(row.get("size") or 0)
        underlying_price = _as_float(metadata.get("underlying_price"))
        delta = _as_float(metadata.get("delta"))
        gamma = _as_float(metadata.get("gamma"))
        vega = _as_float(metadata.get("vega"))
        signed_delta_notional = _signed_greek_notional(
            sign=sign,
            greek_value=delta,
            underlying_price=underlying_price,
            size=size,
            greek_type="delta",
        )
        signed_vega_notional = _signed_greek_notional(
            sign=sign,
            greek_value=vega,
            underlying_price=underlying_price,
            size=size,
            greek_type="vega",
        )
        signed_gamma_dollar_exposure = _signed_greek_notional(
            sign=sign,
            greek_value=gamma,
            underlying_price=underlying_price,
            size=size,
            greek_type="gamma",
        )
        gross_delta_notional = (
            None
            if delta is None or underlying_price is None
            else round(abs(delta) * underlying_price * 100.0 * size, 4)
        )
        gross_vega_notional = (
            None if vega is None else round(abs(vega) * 100.0 * size, 4)
        )
        gross_gamma_dollar_exposure = (
            None
            if gamma is None or underlying_price is None
            else round(abs(gamma) * underlying_price * underlying_price * 100.0 * size, 4)
        )
        row.update(
            {
                "aggressor_side": aggressor_side,
                "aggressor_confidence": aggressor_confidence,
                "quote_match_source": match_source,
                "quote_match_age_ms": quote_match_age_ms,
                "matched_bid": bid,
                "matched_ask": ask,
                "matched_midpoint": matched_midpoint,
                "matched_spread": None
                if matched_spread is None
                else round(matched_spread, 6),
                "signed_trade_count": int(sign),
                "signed_size": int(sign * size),
                "signed_premium": round(sign * float(row.get("premium") or 0.0), 4),
                "signed_delta_notional": signed_delta_notional,
                "signed_vega_notional": signed_vega_notional,
                "signed_gamma_dollar_exposure": signed_gamma_dollar_exposure,
                "gross_delta_notional": gross_delta_notional,
                "gross_vega_notional": gross_vega_notional,
                "gross_gamma_dollar_exposure": gross_gamma_dollar_exposure,
            }
        )
        enriched_rows.append(row)

    return enriched_rows


__all__ = ["enrich_uoa_trade_records"]
