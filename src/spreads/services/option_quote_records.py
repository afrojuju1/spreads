from __future__ import annotations

from typing import Any


def _strategy_option_type(strategy: Any) -> str | None:
    rendered = str(strategy or "").strip().lower()
    if rendered.startswith("call"):
        return "call"
    if rendered.startswith("put"):
        return "put"
    return None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _leg_value(candidate: dict[str, Any], *, leg_role: str, key: str) -> Any:
    if candidate.get("option_symbol"):
        return candidate.get(key)
    return candidate.get(f"{leg_role}_{key}")


def _days_to_expiration(candidate: dict[str, Any]) -> int | None:
    value = candidate.get("days_to_expiration")
    if value in (None, ""):
        value = candidate.get("dte")
    return _coerce_int(value)


def _percent_otm(*, option_type: str | None, strike_price: float | None, underlying_price: float | None) -> float | None:
    if option_type not in {"call", "put"} or strike_price is None or underlying_price is None or underlying_price <= 0:
        return None
    if option_type == "call":
        return round((strike_price - underlying_price) / underlying_price, 4)
    return round((underlying_price - strike_price) / underlying_price, 4)


def build_option_symbol_metadata(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        contract_symbol = str(candidate.get("option_symbol") or "").strip()
        if contract_symbol:
            option_type = str(candidate.get("option_type") or "").strip().lower() or _strategy_option_type(candidate.get("strategy"))
            strike_price = _coerce_float(candidate.get("strike_price"))
            underlying_price = _coerce_float(candidate.get("underlying_price"))
            metadata[contract_symbol] = {
                "underlying_symbol": candidate.get("underlying_symbol"),
                "strategy": candidate.get("strategy"),
                "leg_role": candidate.get("leg_role") or candidate.get("contract_role") or "contract",
                "option_type": option_type,
                "expiration_date": candidate.get("expiration_date"),
                "days_to_expiration": _days_to_expiration(candidate),
                "strike_price": strike_price,
                "underlying_price": underlying_price,
                "open_interest": _coerce_int(candidate.get("open_interest")),
                "volume": _coerce_int(candidate.get("volume")),
                "implied_volatility": _coerce_float(candidate.get("implied_volatility")),
                "delta": _coerce_float(candidate.get("delta")),
                "bid": _coerce_float(candidate.get("bid")),
                "ask": _coerce_float(candidate.get("ask")),
                "midpoint": _coerce_float(candidate.get("midpoint")),
                "bid_size": _coerce_int(candidate.get("bid_size")),
                "ask_size": _coerce_int(candidate.get("ask_size")),
                "last_trade_price": _coerce_float(candidate.get("last_trade_price")),
                "relative_spread": _coerce_float(candidate.get("relative_spread")),
                "percent_otm": _percent_otm(
                    option_type=option_type,
                    strike_price=strike_price,
                    underlying_price=underlying_price,
                ),
            }
            continue
        for leg_role, option_symbol in (
            ("short", candidate.get("short_symbol")),
            ("long", candidate.get("long_symbol")),
        ):
            option_symbol = str(option_symbol or "").strip()
            if not option_symbol:
                continue
            option_type = _strategy_option_type(candidate.get("strategy"))
            strike_price = _coerce_float(_leg_value(candidate, leg_role=leg_role, key="strike"))
            underlying_price = _coerce_float(candidate.get("underlying_price"))
            metadata[option_symbol] = {
                "underlying_symbol": candidate.get("underlying_symbol"),
                "strategy": candidate.get("strategy"),
                "leg_role": leg_role,
                "option_type": option_type,
                "expiration_date": candidate.get("expiration_date"),
                "days_to_expiration": _days_to_expiration(candidate),
                "strike_price": strike_price,
                "underlying_price": underlying_price,
                "open_interest": _coerce_int(_leg_value(candidate, leg_role=leg_role, key="open_interest")),
                "volume": _coerce_int(_leg_value(candidate, leg_role=leg_role, key="volume")),
                "implied_volatility": _coerce_float(_leg_value(candidate, leg_role=leg_role, key="implied_volatility")),
                "delta": _coerce_float(_leg_value(candidate, leg_role=leg_role, key="delta")),
                "bid": _coerce_float(_leg_value(candidate, leg_role=leg_role, key="bid")),
                "ask": _coerce_float(_leg_value(candidate, leg_role=leg_role, key="ask")),
                "midpoint": _coerce_float(_leg_value(candidate, leg_role=leg_role, key="midpoint")),
                "bid_size": _coerce_int(_leg_value(candidate, leg_role=leg_role, key="bid_size")),
                "ask_size": _coerce_int(_leg_value(candidate, leg_role=leg_role, key="ask_size")),
                "last_trade_price": _coerce_float(_leg_value(candidate, leg_role=leg_role, key="last_trade_price")),
                "relative_spread": _coerce_float(_leg_value(candidate, leg_role=leg_role, key="relative_spread")),
                "percent_otm": _percent_otm(
                    option_type=option_type,
                    strike_price=strike_price,
                    underlying_price=underlying_price,
                ),
            }
    return metadata


def build_quote_symbol_metadata(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return build_option_symbol_metadata(candidates)


def build_quote_records(
    *,
    captured_at: str,
    symbol_metadata: dict[str, dict[str, Any]],
    quotes: list[Any],
    source: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for quote in quotes:
        metadata = symbol_metadata.get(quote.symbol, {})
        records.append(
            {
                "captured_at": captured_at,
                "underlying_symbol": metadata.get("underlying_symbol"),
                "strategy": metadata.get("strategy"),
                "option_symbol": quote.symbol,
                "leg_role": metadata.get("leg_role", "unknown"),
                "bid": quote.bid,
                "ask": quote.ask,
                "midpoint": quote.midpoint,
                "bid_size": quote.bid_size,
                "ask_size": quote.ask_size,
                "quote_timestamp": quote.timestamp,
                "source": source,
            }
        )
    return records
