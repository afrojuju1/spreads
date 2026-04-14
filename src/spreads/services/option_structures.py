from __future__ import annotations

from collections.abc import Mapping
from typing import Any

NET_CREDIT_FAMILIES = {
    "call_credit_spread",
    "put_credit_spread",
    "iron_condor",
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


def normalize_strategy_family(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return {
        "call_credit": "call_credit_spread",
        "put_credit": "put_credit_spread",
        "call_debit": "call_debit_spread",
        "put_debit": "put_debit_spread",
    }.get(normalized, normalized or "unknown")


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


def normalize_legs(
    legs_payload: Any,
    *,
    expiration_date: str | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(legs_payload, list):
        return []
    built: list[dict[str, Any]] = []
    for leg in legs_payload:
        if not isinstance(leg, Mapping):
            continue
        symbol = _as_text(leg.get("symbol"))
        if symbol is None:
            continue
        side = _as_text(leg.get("side"))
        position_intent = _as_text(leg.get("position_intent"))
        built.append(
            {
                "symbol": symbol,
                "side": side,
                "position_intent": position_intent,
                "ratio_qty": _as_text(leg.get("ratio_qty")) or "1",
                "role": _as_text(leg.get("role"))
                or leg_role(side=side, position_intent=position_intent),
                "expiration_date": _as_text(leg.get("expiration_date"))
                or expiration_date,
                "strike": leg.get("strike"),
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
        default_intent = (
            OPEN_INTENT_BY_ROLE.get(role)
            if intent == "open"
            else CLOSE_INTENT_BY_ROLE.get(role)
        )
        position_intent = _as_text(leg.get("position_intent")) or default_intent
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
    "fallback_vertical_legs",
    "legs_identity_key",
    "leg_role",
    "net_premium_kind",
    "normalize_legs",
    "normalize_strategy_family",
    "position_legs",
    "primary_short_long_symbols",
    "signed_net_limit_price",
    "unique_leg_symbols",
    "vertical_opening_legs",
]
