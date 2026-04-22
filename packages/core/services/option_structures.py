from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

NET_CREDIT_FAMILIES = {
    "call_credit_spread",
    "put_credit_spread",
    "iron_condor",
    "short_call",
    "short_put",
}
NET_DEBIT_FAMILIES = {
    "call_debit_spread",
    "put_debit_spread",
    "long_call",
    "long_put",
    "long_straddle",
    "long_strangle",
}
OPEN_INTENT_BY_ROLE = {
    "short": "sell_to_open",
    "long": "buy_to_open",
}
CLOSE_INTENT_BY_ROLE = {
    "short": "buy_to_close",
    "long": "sell_to_close",
}
SIDE_BY_INTENT = {
    "sell_to_open": "sell",
    "buy_to_open": "buy",
    "buy_to_close": "buy",
    "sell_to_close": "sell",
}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_strategy_family(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return {
        "call_credit": "call_credit_spread",
        "put_credit": "put_credit_spread",
        "call_debit": "call_debit_spread",
        "put_debit": "put_debit_spread",
    }.get(normalized, normalized or "unknown")


def _strike_from_symbol(value: Any) -> float | None:
    rendered = _as_text(value)
    if rendered is None:
        return None
    match = re.search(r"([cp])(\d+(?:\.\d+)?)$", rendered, re.IGNORECASE)
    if match is None:
        return None
    raw_strike = match.group(2)
    try:
        if "." in raw_strike:
            return float(raw_strike)
        if len(raw_strike) >= 8:
            return round(int(raw_strike) / 1000.0, 4)
        return float(raw_strike)
    except (TypeError, ValueError):
        return None


def net_premium_kind(strategy_family: Any) -> str | None:
    family = normalize_strategy_family(strategy_family)
    if family in NET_CREDIT_FAMILIES:
        return "credit"
    if family in NET_DEBIT_FAMILIES:
        return "debit"
    return None


def signed_net_limit_price(
    *,
    limit_price: float,
    strategy_family: Any,
    trade_intent: str,
) -> float:
    normalized_limit = abs(float(limit_price))
    family = normalize_strategy_family(strategy_family)
    intent = str(trade_intent or "open").strip().lower()
    premium_kind = net_premium_kind(family)
    if premium_kind is None:
        return normalized_limit
    if intent == "open":
        return -normalized_limit if premium_kind == "credit" else normalized_limit
    if intent == "close":
        return normalized_limit if premium_kind == "credit" else -normalized_limit
    return normalized_limit


def leg_role(*, side: Any, position_intent: Any) -> str | None:
    intent = str(position_intent or "").strip().lower()
    if intent in {"sell_to_open", "buy_to_close"}:
        return "short"
    if intent in {"buy_to_open", "sell_to_close"}:
        return "long"
    normalized_side = str(side or "").strip().lower()
    if normalized_side == "sell":
        return "short"
    if normalized_side == "buy":
        return "long"
    return None


def normalize_position_intent(
    position_intent: Any,
    *,
    role: str | None,
    trade_intent: str = "open",
) -> str | None:
    normalized = str(position_intent or "").strip().lower()
    if normalized in {"buy_to_open", "sell_to_open", "buy_to_close", "sell_to_close"}:
        return normalized
    if role is None:
        return None
    intent = str(trade_intent or "open").strip().lower()
    if normalized == "close" or intent == "close":
        return CLOSE_INTENT_BY_ROLE.get(role)
    if normalized in {"", "open"}:
        return OPEN_INTENT_BY_ROLE.get(role)
    return None


def normalize_legs(
    legs_payload: Any,
    *,
    expiration_date: str | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(legs_payload, (list, tuple)):
        return []
    built: list[dict[str, Any]] = []
    for leg in legs_payload:
        if not isinstance(leg, Mapping):
            continue
        symbol = _as_text(leg.get("symbol"))
        if symbol is None:
            continue
        side = _as_text(leg.get("side"))
        role = _as_text(leg.get("role")) or leg_role(
            side=side,
            position_intent=leg.get("position_intent"),
        )
        position_intent = normalize_position_intent(
            leg.get("position_intent"),
            role=role,
            trade_intent="close"
            if str(leg.get("position_intent") or "").strip().lower() == "close"
            else "open",
        )
        option_type = _option_type_from_value(leg.get("option_type")) or (
            _option_type_from_value(symbol)
        )
        strike = _as_float(leg.get("strike"))
        if strike is None:
            strike = _strike_from_symbol(symbol)
        built.append(
            {
                "symbol": symbol,
                "side": side,
                "position_intent": position_intent,
                "ratio_qty": _as_text(leg.get("ratio_qty")) or "1",
                "role": role or leg_role(side=side, position_intent=position_intent),
                "expiration_date": _as_text(leg.get("expiration_date"))
                or expiration_date,
                "strike": strike,
                "option_type": option_type,
            }
        )
    return built


def fallback_vertical_legs(
    *,
    short_symbol: Any,
    long_symbol: Any,
    expiration_date: Any = None,
) -> list[dict[str, Any]]:
    built: list[dict[str, Any]] = []
    rendered_expiration = _as_text(expiration_date)
    if _as_text(short_symbol) is not None:
        built.append(
            {
                "symbol": str(short_symbol),
                "role": "short",
                "expiration_date": rendered_expiration,
                "ratio_qty": "1",
            }
        )
    if _as_text(long_symbol) is not None:
        built.append(
            {
                "symbol": str(long_symbol),
                "role": "long",
                "expiration_date": rendered_expiration,
                "ratio_qty": "1",
            }
        )
    return built


def candidate_legs(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    resolved = normalize_legs(
        candidate.get("legs"),
        expiration_date=_as_text(candidate.get("expiration_date")),
    )
    if resolved:
        return resolved
    order_payload = candidate.get("order_payload")
    if isinstance(order_payload, Mapping):
        resolved = normalize_legs(
            order_payload.get("legs"),
            expiration_date=_as_text(candidate.get("expiration_date")),
        )
        if resolved:
            return resolved
    return fallback_vertical_legs(
        short_symbol=candidate.get("short_symbol"),
        long_symbol=candidate.get("long_symbol"),
        expiration_date=candidate.get("expiration_date"),
    )


def position_legs(position: Mapping[str, Any]) -> list[dict[str, Any]]:
    resolved = normalize_legs(
        position.get("legs"),
        expiration_date=_as_text(position.get("expiration_date")),
    )
    if resolved:
        return resolved
    return fallback_vertical_legs(
        short_symbol=position.get("short_symbol"),
        long_symbol=position.get("long_symbol"),
        expiration_date=position.get("expiration_date"),
    )


def payload_structure_identity(
    payload: Mapping[str, Any],
    *,
    strategy: Any = None,
) -> str | None:
    existing = _as_text(payload.get("structure_identity")) or _as_text(
        payload.get("candidate_identity")
    )
    if existing is not None:
        return existing
    legs = candidate_legs(payload)
    if not legs:
        return None
    return legs_identity_key(
        strategy=(
            strategy
            if strategy is not None
            else payload.get("strategy_family") or payload.get("strategy")
        ),
        legs=legs,
    )


def structure_symbol_path(legs: list[Mapping[str, Any]]) -> str:
    symbols = unique_leg_symbols(legs)
    if not symbols:
        return "n/a"
    if len(symbols) == 1:
        return symbols[0]
    return " / ".join(symbols)


def _option_type_from_value(value: Any) -> str | None:
    rendered = _as_text(value)
    if rendered is None:
        return None
    normalized = rendered.lower()
    if normalized in {"call", "put"}:
        return normalized
    match = re.search(r"([cp])(?=\d+(?:\.\d+)?$)", rendered, re.IGNORECASE)
    if match:
        return "call" if match.group(1).lower() == "c" else "put"
    return None


def structure_strike_path(
    legs: list[Mapping[str, Any]],
    *,
    strategy: Any = None,
) -> str:
    normalized_legs = normalize_legs(legs)
    normalized_strategy = normalize_strategy_family(strategy)
    if normalized_strategy == "iron_condor":
        grouped: dict[str, dict[str, float | None]] = {
            "put": {"short": None, "long": None},
            "call": {"short": None, "long": None},
        }
        for leg in normalized_legs:
            option_type = _option_type_from_value(
                leg.get("option_type")
            ) or _option_type_from_value(leg.get("symbol"))
            role = _as_text(leg.get("role")) or leg_role(
                side=leg.get("side"),
                position_intent=leg.get("position_intent"),
            )
            strike = _as_float(leg.get("strike"))
            if (
                option_type in {"call", "put"}
                and role in {"short", "long"}
                and strike is not None
                and grouped[option_type][role] is None
            ):
                grouped[option_type][role] = strike
        put_long = grouped["put"]["long"]
        put_short = grouped["put"]["short"]
        call_short = grouped["call"]["short"]
        call_long = grouped["call"]["long"]
        if None not in (put_long, put_short, call_short, call_long):
            return (
                f"{put_long:.2f}-{put_short:.2f}P"
                f" / {call_short:.2f}-{call_long:.2f}C"
            )

    rendered_parts: list[str] = []
    for leg in normalized_legs:
        strike = _as_float(leg.get("strike"))
        if strike is None:
            continue
        option_type = _option_type_from_value(leg.get("option_type")) or (
            _option_type_from_value(leg.get("symbol"))
        )
        suffix = {"call": "C", "put": "P"}.get(option_type, "")
        rendered_parts.append(f"{strike:.2f}{suffix}")
    if not rendered_parts:
        return "n/a"
    if len(rendered_parts) == 1:
        return rendered_parts[0]
    return " / ".join(rendered_parts)


def structure_width(
    legs: list[Mapping[str, Any]],
    *,
    strategy: Any = None,
) -> float | None:
    normalized_legs = normalize_legs(legs)
    if not normalized_legs:
        return None
    normalized_strategy = normalize_strategy_family(strategy)
    if normalized_strategy in {
        "long_call",
        "long_put",
        "long_straddle",
        "short_call",
        "short_put",
    }:
        return 0.0
    widths: list[float] = []
    grouped: dict[str | None, list[float]] = {}
    for leg in normalized_legs:
        strike = _as_float(leg.get("strike"))
        if strike is None:
            continue
        option_type = _option_type_from_value(leg.get("option_type")) or (
            _option_type_from_value(leg.get("symbol"))
        )
        grouped.setdefault(option_type, []).append(strike)
    for strikes in grouped.values():
        unique_strikes = sorted({round(strike, 4) for strike in strikes})
        if len(unique_strikes) < 2:
            continue
        widths.append(round(unique_strikes[-1] - unique_strikes[0], 4))
    if widths:
        return max(widths)
    strikes = sorted(
        {
            round(strike, 4)
            for strike in (
                _as_float(leg.get("strike")) for leg in normalized_legs
            )
            if strike is not None
        }
    )
    if len(strikes) <= 1:
        return 0.0 if strikes else None
    return round(strikes[-1] - strikes[0], 4)


def structure_barrier_strike(
    legs: list[Mapping[str, Any]],
    *,
    strategy: Any = None,
) -> float | None:
    normalized_legs = normalize_legs(legs)
    if not normalized_legs:
        return None
    normalized_strategy = normalize_strategy_family(strategy)
    target_option_type = None
    if normalized_strategy in {
        "call_credit_spread",
        "call_debit_spread",
        "long_call",
        "short_call",
    }:
        target_option_type = "call"
    elif normalized_strategy in {
        "put_credit_spread",
        "put_debit_spread",
        "long_put",
        "short_put",
    }:
        target_option_type = "put"

    ordered_legs = normalized_legs
    if target_option_type is not None:
        matching_legs = [
            leg
            for leg in normalized_legs
            if (
                _option_type_from_value(leg.get("option_type"))
                or _option_type_from_value(leg.get("symbol"))
            )
            == target_option_type
        ]
        if matching_legs:
            ordered_legs = matching_legs

    for leg in ordered_legs:
        role = _as_text(leg.get("role")) or leg_role(
            side=leg.get("side"),
            position_intent=leg.get("position_intent"),
        )
        strike = _as_float(leg.get("strike"))
        if role == "short" and strike is not None:
            return strike
    for leg in ordered_legs:
        strike = _as_float(leg.get("strike"))
        if strike is not None:
            return strike
    return None


def structure_display_fields(legs: list[Mapping[str, Any]]) -> dict[str, str | None]:
    short_symbol, long_symbol = primary_short_long_symbols(legs)
    return {
        "short_symbol": short_symbol,
        "long_symbol": long_symbol,
        "symbol_path": structure_symbol_path(legs),
        "strike_path": structure_strike_path(legs),
    }


def payload_display_fields(payload: Mapping[str, Any]) -> dict[str, str | None]:
    return structure_display_fields(candidate_legs(payload))


def primary_short_long_symbols(
    legs: list[Mapping[str, Any]],
) -> tuple[str | None, str | None]:
    short_symbol = None
    long_symbol = None
    for leg in legs:
        symbol = _as_text(leg.get("symbol"))
        role = _as_text(leg.get("role")) or leg_role(
            side=leg.get("side"),
            position_intent=leg.get("position_intent"),
        )
        if symbol is None or role is None:
            continue
        if role == "short" and short_symbol is None:
            short_symbol = symbol
        elif role == "long" and long_symbol is None:
            long_symbol = symbol
    return short_symbol, long_symbol


def unique_leg_symbols(legs: list[Mapping[str, Any]]) -> list[str]:
    symbols: list[str] = []
    for leg in legs:
        symbol = _as_text(leg.get("symbol"))
        if symbol is None or symbol in symbols:
            continue
        symbols.append(symbol)
    return symbols


def common_expiration_date(legs: list[Mapping[str, Any]]) -> str | None:
    expirations = {
        expiration
        for expiration in (_as_text(leg.get("expiration_date")) for leg in legs)
        if expiration is not None
    }
    if len(expirations) != 1:
        return None
    return next(iter(expirations))


def reverse_position_intent(position_intent: Any) -> str | None:
    intent = str(position_intent or "").strip().lower()
    return {
        "buy_to_open": "sell_to_close",
        "sell_to_open": "buy_to_close",
        "buy_to_close": "sell_to_open",
        "sell_to_close": "buy_to_open",
    }.get(intent)


def closing_legs(legs: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    built: list[dict[str, Any]] = []
    for leg in legs:
        symbol = _as_text(leg.get("symbol"))
        if symbol is None:
            continue
        position_intent = _as_text(leg.get("position_intent"))
        reversed_intent = reverse_position_intent(position_intent)
        side = _as_text(leg.get("side"))
        if side == "buy":
            reversed_side = "sell"
        elif side == "sell":
            reversed_side = "buy"
        else:
            reversed_side = None
        built.append(
            {
                "symbol": symbol,
                "side": reversed_side,
                "position_intent": reversed_intent,
                "ratio_qty": _as_text(leg.get("ratio_qty")) or "1",
                "role": _as_text(leg.get("role")),
                "expiration_date": _as_text(leg.get("expiration_date")),
                "strike": leg.get("strike"),
                "option_type": leg.get("option_type"),
            }
        )
    return built


def vertical_opening_legs(
    *,
    short_symbol: Any,
    long_symbol: Any,
    expiration_date: Any = None,
) -> list[dict[str, Any]]:
    built: list[dict[str, Any]] = []
    rendered_expiration = _as_text(expiration_date)
    if _as_text(short_symbol) is not None:
        built.append(
            {
                "symbol": str(short_symbol),
                "side": "sell",
                "position_intent": "sell_to_open",
                "ratio_qty": "1",
                "role": "short",
                "expiration_date": rendered_expiration,
            }
        )
    if _as_text(long_symbol) is not None:
        built.append(
            {
                "symbol": str(long_symbol),
                "side": "buy",
                "position_intent": "buy_to_open",
                "ratio_qty": "1",
                "role": "long",
                "expiration_date": rendered_expiration,
            }
        )
    return built


def long_only_opening_legs(
    *symbols: Any,
    expiration_date: Any = None,
) -> list[dict[str, Any]]:
    built: list[dict[str, Any]] = []
    rendered_expiration = _as_text(expiration_date)
    for symbol in symbols:
        resolved_symbol = _as_text(symbol)
        if resolved_symbol is None:
            continue
        built.append(
            {
                "symbol": resolved_symbol,
                "side": "buy",
                "position_intent": "buy_to_open",
                "ratio_qty": "1",
                "role": "long",
                "expiration_date": rendered_expiration,
            }
        )
    return built


def iron_condor_opening_legs(
    *,
    short_put_symbol: Any,
    long_put_symbol: Any,
    short_call_symbol: Any,
    long_call_symbol: Any,
    expiration_date: Any = None,
) -> list[dict[str, Any]]:
    rendered_expiration = _as_text(expiration_date)
    built: list[dict[str, Any]] = []
    for symbol, role, side in (
        (short_put_symbol, "short", "sell"),
        (long_put_symbol, "long", "buy"),
        (short_call_symbol, "short", "sell"),
        (long_call_symbol, "long", "buy"),
    ):
        resolved_symbol = _as_text(symbol)
        if resolved_symbol is None:
            continue
        built.append(
            {
                "symbol": resolved_symbol,
                "side": side,
                "position_intent": normalize_position_intent(
                    "open",
                    role=role,
                    trade_intent="open",
                ),
                "ratio_qty": "1",
                "role": role,
                "expiration_date": rendered_expiration,
            }
        )
    return built


def build_multileg_order_payload(
    *,
    legs: list[Mapping[str, Any]],
    limit_price: float,
    strategy_family: Any,
    trade_intent: str,
    quantity: int = 1,
) -> dict[str, Any]:
    intent = str(trade_intent or "open").strip().lower()
    normalized_legs = normalize_legs(legs)
    if not normalized_legs:
        raise ValueError("Order payload requires at least one normalized leg")

    rendered_legs: list[dict[str, Any]] = []
    for leg in normalized_legs:
        role = _as_text(leg.get("role")) or leg_role(
            side=leg.get("side"),
            position_intent=leg.get("position_intent"),
        )
        if role is None:
            raise ValueError("Order payload requires each leg to resolve a role")
        position_intent = normalize_position_intent(
            leg.get("position_intent"),
            role=role,
            trade_intent=intent,
        )
        if position_intent is None:
            raise ValueError("Order payload requires each leg to resolve a position intent")
        side = _as_text(leg.get("side")) or SIDE_BY_INTENT.get(position_intent)
        if side is None:
            raise ValueError("Order payload requires each leg to resolve a side")
        rendered_legs.append(
            {
                "symbol": str(leg["symbol"]),
                "ratio_qty": _as_text(leg.get("ratio_qty")) or "1",
                "side": side,
                "position_intent": position_intent,
            }
        )

    signed_limit = signed_net_limit_price(
        limit_price=limit_price,
        strategy_family=strategy_family,
        trade_intent=intent,
    )
    return {
        "order_class": "mleg",
        "qty": str(max(int(quantity), 1)),
        "type": "limit",
        "limit_price": f"{signed_limit:.2f}",
        "time_in_force": "day",
        "legs": rendered_legs,
    }


def _quote_number(quote: Any, key: str) -> float | None:
    if isinstance(quote, Mapping):
        value = quote.get(key)
    else:
        value = getattr(quote, key, None)
    if key == "midpoint" and value is None:
        bid = _quote_number(quote, "bid")
        ask = _quote_number(quote, "ask")
        if bid is not None and ask is not None:
            return round((bid + ask) / 2.0, 4)
    return _as_float(value)


def _quote_text(quote: Any, key: str) -> str | None:
    if isinstance(quote, Mapping):
        return _as_text(quote.get(key))
    return _as_text(getattr(quote, key, None))


def structure_quote_snapshot(
    *,
    legs: list[Mapping[str, Any]],
    strategy_family: Any,
    quotes_by_symbol: Mapping[str, Any],
    sources_by_symbol: Mapping[str, str] | None = None,
    normalized_legs: bool = False,
) -> dict[str, Any] | None:
    normalized_leg_rows = list(legs) if normalized_legs else normalize_legs(legs)
    if not normalized_leg_rows:
        return None
    family = normalize_strategy_family(strategy_family)
    premium_kind = net_premium_kind(family)
    if premium_kind is None:
        return None

    midpoint_short = 0.0
    midpoint_long = 0.0
    natural_short = 0.0
    natural_long = 0.0
    close_short = 0.0
    close_long = 0.0
    leg_quotes: list[dict[str, Any]] = []
    timestamps: list[str] = []
    quote_sources: list[str] = []

    for leg in normalized_leg_rows:
        symbol = _as_text(leg.get("symbol"))
        role = _as_text(leg.get("role")) or leg_role(
            side=leg.get("side"),
            position_intent=leg.get("position_intent"),
        )
        if symbol is None or role is None:
            return None
        quote = quotes_by_symbol.get(symbol)
        if quote is None:
            return None
        bid = _quote_number(quote, "bid")
        ask = _quote_number(quote, "ask")
        midpoint = _quote_number(quote, "midpoint")
        if bid is None or ask is None or midpoint is None:
            return None
        timestamp = (
            _quote_text(quote, "timestamp")
            or _quote_text(quote, "quote_timestamp")
            or _quote_text(quote, "captured_at")
        )
        if timestamp:
            timestamps.append(timestamp)
        source = None if sources_by_symbol is None else _as_text(sources_by_symbol.get(symbol))
        if source:
            quote_sources.append(source)
        leg_quotes.append(
            {
                "symbol": symbol,
                "role": role,
                "bid": bid,
                "ask": ask,
                "midpoint": midpoint,
                "timestamp": timestamp,
                "source": source,
            }
        )
        if role == "short":
            midpoint_short += midpoint
            natural_short += bid
            close_short += ask
        else:
            midpoint_long += midpoint
            natural_long += ask
            close_long += bid

    if premium_kind == "debit":
        midpoint_value = round(midpoint_long - midpoint_short, 4)
        natural_value = round(natural_long - natural_short, 4)
        close_mark = round(max(close_long - close_short, 0.0), 4)
    else:
        midpoint_value = round(midpoint_short - midpoint_long, 4)
        natural_value = round(natural_short - natural_long, 4)
        close_mark = round(max(close_short - close_long, 0.0), 4)

    quote_source = None
    unique_sources = {value for value in quote_sources if value}
    if unique_sources:
        quote_source = next(iter(unique_sources)) if len(unique_sources) == 1 else "mixed"

    short_symbol, long_symbol = primary_short_long_symbols(normalized_leg_rows)
    return {
        "short_symbol": short_symbol,
        "long_symbol": long_symbol,
        "strategy_family": family,
        "premium_kind": premium_kind,
        "midpoint_value": midpoint_value,
        "natural_value": natural_value,
        "close_mark": close_mark,
        "midpoint_credit": midpoint_value,
        "natural_credit": natural_value,
        "captured_at": max(timestamps) if timestamps else None,
        "quote_source": quote_source,
        "legs": leg_quotes,
    }


def legs_identity_key(
    *,
    strategy: Any,
    legs: list[Mapping[str, Any]],
) -> str:
    rendered_parts: list[str] = []
    for leg in sorted(
        legs,
        key=lambda item: (
            str(item.get("role") or ""),
            str(item.get("symbol") or ""),
            str(item.get("position_intent") or ""),
            str(item.get("ratio_qty") or ""),
            str(item.get("expiration_date") or ""),
        ),
    ):
        symbol = _as_text(leg.get("symbol"))
        if symbol is None:
            continue
        rendered_parts.append(
            ":".join(
                [
                    _as_text(leg.get("role")) or "",
                    symbol,
                    _as_text(leg.get("position_intent")) or "",
                    _as_text(leg.get("ratio_qty")) or "1",
                    _as_text(leg.get("expiration_date")) or "",
                ]
            )
        )
    return "|".join([normalize_strategy_family(strategy), *rendered_parts])


__all__ = [
    "build_multileg_order_payload",
    "candidate_legs",
    "closing_legs",
    "common_expiration_date",
    "fallback_vertical_legs",
    "iron_condor_opening_legs",
    "legs_identity_key",
    "leg_role",
    "long_only_opening_legs",
    "net_premium_kind",
    "normalize_legs",
    "normalize_position_intent",
    "normalize_strategy_family",
    "position_legs",
    "primary_short_long_symbols",
    "payload_structure_identity",
    "payload_display_fields",
    "structure_quote_snapshot",
    "structure_display_fields",
    "structure_barrier_strike",
    "structure_strike_path",
    "structure_width",
    "structure_symbol_path",
    "signed_net_limit_price",
    "unique_leg_symbols",
    "vertical_opening_legs",
]
