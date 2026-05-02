from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.option_structures import order_payload_legs
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
)

ALPACA_DIRECT_RUNTIME = "alpaca_direct"
NAUTILUS_RUNTIME = "nautilus"
SUPPORTED_EXECUTION_RUNTIMES = {ALPACA_DIRECT_RUNTIME, NAUTILUS_RUNTIME}
ALPACA_VENUE = "ALPACA"


def normalize_execution_runtime(value: Any) -> str:
    runtime = str(value or ALPACA_DIRECT_RUNTIME).strip().lower()
    if runtime in {"alpaca", "direct", "alpaca_direct"}:
        return ALPACA_DIRECT_RUNTIME
    if runtime == NAUTILUS_RUNTIME:
        return NAUTILUS_RUNTIME
    raise ValueError(
        "execution runtime must be one of "
        f"{', '.join(sorted(SUPPORTED_EXECUTION_RUNTIMES))}"
    )


def execution_runtime_from_request(request: Mapping[str, Any]) -> str:
    return normalize_execution_runtime(request.get("execution_runtime"))


def _quote_legs(live_quote: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(live_quote, Mapping):
        return {}
    return {
        str(leg["symbol"]): dict(leg)
        for leg in live_quote.get("legs") or []
        if isinstance(leg, Mapping) and _as_text(leg.get("symbol")) is not None
    }


def _side(leg: Mapping[str, Any]) -> str | None:
    side = _as_text(leg.get("side"))
    if side is not None:
        return side.lower()
    intent = (_as_text(leg.get("position_intent")) or "").lower()
    if intent in {"sell_to_open", "sell_to_close"}:
        return "sell"
    if intent in {"buy_to_open", "buy_to_close"}:
        return "buy"
    return None


def _leg_quote_price(
    *,
    side: str,
    quote: Mapping[str, Any],
) -> float | None:
    if side == "sell":
        return _coerce_float(quote.get("bid"))
    if side == "buy":
        return _coerce_float(quote.get("ask"))
    return None


def _derive_vertical_leg_prices(
    *,
    legs: list[dict[str, Any]],
    order_request: Mapping[str, Any],
    live_quote: Mapping[str, Any] | None,
) -> tuple[dict[str, float], str | None]:
    if len(legs) != 2:
        return {}, "nautilus_handoff_requires_two_leg_vertical_pricing"
    quote_legs = _quote_legs(live_quote)
    if not quote_legs:
        return {}, "nautilus_handoff_requires_live_leg_quotes"

    priced: dict[int, float] = {}
    sell_index: int | None = None
    buy_index: int | None = None
    for index, leg in enumerate(legs):
        symbol = _as_text(leg.get("symbol"))
        side = _side(leg)
        if symbol is None or side not in {"buy", "sell"}:
            return {}, "nautilus_handoff_requires_leg_symbol_and_side"
        quote = quote_legs.get(symbol)
        if quote is None:
            return {}, f"nautilus_handoff_missing_live_quote:{symbol}"
        natural_price = _leg_quote_price(side=side, quote=quote)
        if natural_price is None or natural_price <= 0:
            return {}, f"nautilus_handoff_unpriced_leg:{symbol}"
        priced[index] = natural_price
        if side == "sell":
            sell_index = index
        else:
            buy_index = index

    if sell_index is None or buy_index is None:
        return {}, "nautilus_handoff_requires_one_buy_and_one_sell_leg"

    target_net = _coerce_float(order_request.get("limit_price"))
    if target_net is None or target_net == 0:
        return {}, "nautilus_handoff_requires_nonzero_net_limit"

    sell_natural = priced[sell_index]
    buy_natural = priced[buy_index]
    if target_net < 0:
        target_credit = abs(target_net)
        natural_credit = sell_natural - buy_natural
        if natural_credit <= 0:
            return {}, "nautilus_handoff_live_quotes_not_credit_executable"
        if target_credit > natural_credit + 0.005:
            return {}, "nautilus_handoff_target_credit_above_natural"
        priced[sell_index] = round(buy_natural + target_credit, 2)
    else:
        target_debit = target_net
        priced[buy_index] = round(sell_natural + target_debit, 2)

    return {str(index): round(price, 2) for index, price in priced.items()}, None


def build_nautilus_submit_order_list_handoff(
    *,
    attempt: Mapping[str, Any],
    order_request: Mapping[str, Any],
    live_quote: Mapping[str, Any] | None,
) -> dict[str, Any]:
    client_order_id = _as_text(order_request.get("client_order_id")) or _as_text(
        attempt.get("client_order_id")
    )
    if client_order_id is None:
        raise ValueError("Nautilus handoff requires a client order id")
    legs = order_payload_legs(
        order_request,
        expiration_date=_as_text(attempt.get("expiration_date")),
    )
    if len(legs) < 2:
        raise ValueError("Nautilus handoff requires a multi-leg order payload")

    leg_prices, not_ready_reason = _derive_vertical_leg_prices(
        legs=legs,
        order_request=order_request,
        live_quote=live_quote,
    )
    quantity = max(_coerce_int(order_request.get("qty")) or _coerce_int(attempt.get("quantity")) or 1, 1)
    handoff_legs: list[dict[str, Any]] = []
    for index, leg in enumerate(legs):
        symbol = str(leg["symbol"])
        ratio_qty = max(_coerce_int(leg.get("ratio_qty")) or 1, 1)
        handoff_legs.append(
            {
                "client_order_id": f"{client_order_id}-L{index + 1}",
                "symbol": symbol,
                "instrument_id": f"{symbol}.{ALPACA_VENUE}",
                "side": _side(leg),
                "position_intent": _as_text(leg.get("position_intent")),
                "role": _as_text(leg.get("role")),
                "ratio_qty": str(ratio_qty),
                "quantity": quantity * ratio_qty,
                "order_type": "limit",
                "time_in_force": "day",
                "limit_price": leg_prices.get(str(index)),
            }
        )

    return {
        "runtime": NAUTILUS_RUNTIME,
        "command": "SubmitOrderList",
        "ready": not_ready_reason is None,
        "not_ready_reason": not_ready_reason,
        "execution_attempt_id": _as_text(attempt.get("execution_attempt_id")),
        "order_list_id": client_order_id,
        "parent_client_order_id": client_order_id,
        "venue": ALPACA_VENUE,
        "trade_intent": _as_text(attempt.get("trade_intent")),
        "strategy_family": _as_text(attempt.get("strategy_family"))
        or _as_text(attempt.get("strategy")),
        "underlying_symbol": _as_text(attempt.get("underlying_symbol")),
        "net_limit_price": _as_text(order_request.get("limit_price")),
        "quantity": quantity,
        "legs": handoff_legs,
        "price_source": "live_quote" if live_quote is not None else None,
    }


__all__ = [
    "ALPACA_DIRECT_RUNTIME",
    "NAUTILUS_RUNTIME",
    "SUPPORTED_EXECUTION_RUNTIMES",
    "build_nautilus_submit_order_list_handoff",
    "execution_runtime_from_request",
    "normalize_execution_runtime",
]
