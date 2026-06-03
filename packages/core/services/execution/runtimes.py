from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any

from core.services.bots import load_active_bots
from core.services.option_structures import normalize_strategy_family, order_payload_legs
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
)

from .nautilus_bridge import describe_nautilus_bridge

ALPACA_DIRECT_RUNTIME = "alpaca_direct"
NAUTILUS_RUNTIME = "nautilus"
SUPPORTED_EXECUTION_RUNTIMES = {ALPACA_DIRECT_RUNTIME, NAUTILUS_RUNTIME}
ALPACA_VENUE = "ALPACA"
NAUTILUS_HANDOFF_SCHEMA_VERSION = "spreads.nautilus.submit_order_list.v1"
NAUTILUS_EQUITY_HANDOFF_SCHEMA_VERSION = "spreads.nautilus.submit_order.v1"
EXECUTION_RUNTIME_CAPABILITIES_SCHEMA_VERSION = (
    "spreads.execution_runtime_capabilities.v1"
)
NAUTILUS_TWO_LEG_VERTICAL_FAMILIES = frozenset(
    {
        "call_credit_spread",
        "put_credit_spread",
        "call_debit_spread",
        "put_debit_spread",
    }
)
NAUTILUS_FOUR_LEG_FAMILIES = frozenset({"iron_condor"})
NAUTILUS_OPTION_SPREAD_FAMILIES = (
    NAUTILUS_TWO_LEG_VERTICAL_FAMILIES | NAUTILUS_FOUR_LEG_FAMILIES
)
NAUTILUS_OPTION_SPREAD_ACTIONS = frozenset({"open", "close"})
NAUTILUS_TWO_LEG_VERTICAL_OPEN_CAPABILITY = "option_two_leg_vertical_open"
NAUTILUS_OPTION_SPREAD_ORDER_LIST_CAPABILITY = "option_spread_order_list"
NAUTILUS_EQUITY_BUY_SELL_CAPABILITY = "equity_buy_sell"


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


def resolve_nautilus_handoff_capability(
    *,
    strategy_family: Any,
    trade_intent: Any,
    legs: list[Mapping[str, Any]],
) -> dict[str, Any]:
    family = normalize_strategy_family(strategy_family)
    action = str(trade_intent or "open").strip().lower()
    sides = [
        side
        for side in (_side(leg) for leg in legs)
        if side in {"buy", "sell"}
    ]
    side_counts = Counter(sides)
    ratio_quantities = [
        max(_coerce_int(leg.get("ratio_qty")) or 1, 1)
        for leg in legs
    ]
    structure = "unsupported"
    if family in NAUTILUS_TWO_LEG_VERTICAL_FAMILIES and len(legs) == 2:
        structure = "two_leg_vertical"
    elif family in NAUTILUS_FOUR_LEG_FAMILIES and len(legs) == 4:
        structure = "four_leg"
    not_ready_reason: str | None = None
    if action not in NAUTILUS_OPTION_SPREAD_ACTIONS:
        not_ready_reason = f"nautilus_unsupported_action:{action}"
    elif family not in NAUTILUS_OPTION_SPREAD_FAMILIES:
        not_ready_reason = f"nautilus_unsupported_strategy_family:{family}"
    elif family in NAUTILUS_TWO_LEG_VERTICAL_FAMILIES and len(legs) != 2:
        not_ready_reason = "nautilus_handoff_requires_two_leg_vertical"
    elif family in NAUTILUS_FOUR_LEG_FAMILIES and len(legs) != 4:
        not_ready_reason = "nautilus_handoff_requires_four_leg_structure"
    elif family in NAUTILUS_TWO_LEG_VERTICAL_FAMILIES and side_counts != Counter(
        {"buy": 1, "sell": 1}
    ):
        not_ready_reason = "nautilus_handoff_requires_one_buy_and_one_sell_leg"
    elif family in NAUTILUS_FOUR_LEG_FAMILIES and side_counts != Counter(
        {"buy": 2, "sell": 2}
    ):
        not_ready_reason = "nautilus_handoff_requires_two_buy_and_two_sell_legs"
    elif any(quantity != 1 for quantity in ratio_quantities):
        not_ready_reason = "nautilus_handoff_requires_one_to_one_ratios"

    return {
        "name": NAUTILUS_OPTION_SPREAD_ORDER_LIST_CAPABILITY,
        "asset_class": "option",
        "action": action,
        "structure": structure,
        "strategy_family": family,
        "leg_count": len(legs),
        "ready": not_ready_reason is None,
        "not_ready_reason": not_ready_reason,
    }


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


def _derive_spread_leg_prices(
    *,
    legs: list[dict[str, Any]],
    order_request: Mapping[str, Any],
    live_quote: Mapping[str, Any] | None,
) -> tuple[dict[str, float], str | None]:
    if len(legs) not in {2, 4}:
        return {}, "nautilus_handoff_requires_two_or_four_leg_pricing"
    quote_legs = _quote_legs(live_quote)
    if not quote_legs:
        return {}, "nautilus_handoff_requires_live_leg_quotes"

    priced: dict[int, float] = {}
    sell_indexes: list[int] = []
    buy_indexes: list[int] = []
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
            sell_indexes.append(index)
        else:
            buy_indexes.append(index)

    if not sell_indexes or not buy_indexes:
        return {}, "nautilus_handoff_requires_buy_and_sell_legs"

    target_net = _coerce_float(order_request.get("limit_price"))
    if target_net is None or target_net == 0:
        return {}, "nautilus_handoff_requires_nonzero_net_limit"

    sell_natural = sum(priced[index] for index in sell_indexes)
    buy_natural = sum(priced[index] for index in buy_indexes)
    if target_net < 0:
        target_credit = abs(target_net)
        natural_credit = sell_natural - buy_natural
        if natural_credit <= 0:
            return {}, "nautilus_handoff_live_quotes_not_credit_executable"
        if target_credit > natural_credit + 0.005:
            return {}, "nautilus_handoff_target_credit_above_natural"
        adjustment = target_credit - natural_credit
        adjusted_index = sell_indexes[0]
        adjusted_price = priced[adjusted_index] + adjustment
        if adjusted_price <= 0:
            return {}, "nautilus_handoff_adjusted_leg_price_not_positive"
        priced[adjusted_index] = round(adjusted_price, 2)
    else:
        target_debit = target_net
        natural_debit = buy_natural - sell_natural
        adjustment = target_debit - natural_debit
        adjusted_index = buy_indexes[0]
        adjusted_price = priced[adjusted_index] + adjustment
        if adjusted_price <= 0:
            return {}, "nautilus_handoff_adjusted_leg_price_not_positive"
        priced[adjusted_index] = round(adjusted_price, 2)

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
    if not legs:
        raise ValueError("Nautilus handoff requires at least one order leg")

    trade_intent = _as_text(attempt.get("trade_intent"))
    strategy_family = _as_text(attempt.get("strategy_family")) or _as_text(
        attempt.get("strategy")
    )
    capability = resolve_nautilus_handoff_capability(
        strategy_family=strategy_family,
        trade_intent=trade_intent,
        legs=legs,
    )
    capability_not_ready = _as_text(capability.get("not_ready_reason"))
    pricing_not_ready: str | None = None
    if capability_not_ready is None:
        leg_prices, pricing_not_ready = _derive_spread_leg_prices(
            legs=legs,
            order_request=order_request,
            live_quote=live_quote,
        )
    else:
        leg_prices = {}
    not_ready_reason = capability_not_ready or pricing_not_ready
    quantity = max(
        _coerce_int(order_request.get("qty"))
        or _coerce_int(attempt.get("quantity"))
        or 1,
        1,
    )
    handoff_legs: list[dict[str, Any]] = []
    for index, leg in enumerate(legs):
        symbol = str(leg["symbol"])
        ratio_qty = max(_coerce_int(leg.get("ratio_qty")) or 1, 1)
        handoff_legs.append(
            {
                "client_order_id": f"{client_order_id}-L{index + 1}",
                "symbol": symbol,
                "instrument_id": f"{symbol}.{ALPACA_VENUE}",
                "asset_class": "option",
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
        "handoff_schema_version": NAUTILUS_HANDOFF_SCHEMA_VERSION,
        "runtime": NAUTILUS_RUNTIME,
        "command": "SubmitOrderList",
        "ready": not_ready_reason is None,
        "not_ready_reason": not_ready_reason,
        "capability": {
            **capability,
            "ready": not_ready_reason is None,
            "not_ready_reason": not_ready_reason,
        },
        "execution_attempt_id": _as_text(attempt.get("execution_attempt_id")),
        "order_list_id": client_order_id,
        "parent_client_order_id": client_order_id,
        "venue": ALPACA_VENUE,
        "trade_intent": trade_intent,
        "strategy_family": strategy_family,
        "underlying_symbol": _as_text(attempt.get("underlying_symbol")),
        "net_limit_price": _as_text(order_request.get("limit_price")),
        "quantity": quantity,
        "legs": handoff_legs,
        "price_source": "live_quote" if live_quote is not None else None,
    }


def build_nautilus_equity_order_handoff(
    *,
    attempt: Mapping[str, Any],
    order_request: Mapping[str, Any],
) -> dict[str, Any]:
    client_order_id = _as_text(order_request.get("client_order_id")) or _as_text(
        attempt.get("client_order_id")
    )
    symbol = _as_text(order_request.get("symbol")) or _as_text(
        attempt.get("underlying_symbol")
    )
    side = (_as_text(order_request.get("side")) or _as_text(attempt.get("trade_intent")))
    quantity = _coerce_int(order_request.get("qty")) or _coerce_int(
        attempt.get("quantity")
    )
    limit_price = _coerce_float(order_request.get("limit_price")) or _coerce_float(
        attempt.get("limit_price")
    )
    time_in_force = (_as_text(order_request.get("time_in_force")) or "day").lower()
    order_type = (_as_text(order_request.get("type")) or "limit").lower()

    not_ready_reason: str | None = None
    if client_order_id is None:
        not_ready_reason = "nautilus_equity_handoff_requires_client_order_id"
    elif symbol is None:
        not_ready_reason = "nautilus_equity_handoff_requires_symbol"
    elif str(side or "").lower() not in {"buy", "sell"}:
        not_ready_reason = "nautilus_equity_handoff_requires_buy_or_sell"
    elif quantity is None or quantity <= 0:
        not_ready_reason = "nautilus_equity_handoff_requires_positive_quantity"
    elif limit_price is None or limit_price <= 0:
        not_ready_reason = "nautilus_equity_handoff_requires_positive_limit_price"
    elif order_type != "limit":
        not_ready_reason = "nautilus_equity_handoff_supports_limit_orders_only"
    elif time_in_force != "day":
        not_ready_reason = "nautilus_equity_handoff_supports_day_orders_only"

    normalized_symbol = "" if symbol is None else symbol.upper()
    normalized_side = "" if side is None else str(side).lower()
    return {
        "handoff_schema_version": NAUTILUS_EQUITY_HANDOFF_SCHEMA_VERSION,
        "runtime": NAUTILUS_RUNTIME,
        "command": "SubmitOrder",
        "ready": not_ready_reason is None,
        "not_ready_reason": not_ready_reason,
        "capability": {
            "name": NAUTILUS_EQUITY_BUY_SELL_CAPABILITY,
            "asset_class": "equity",
            "actions": ["buy", "sell"],
            "ready": not_ready_reason is None,
            "not_ready_reason": not_ready_reason,
        },
        "execution_attempt_id": _as_text(attempt.get("execution_attempt_id")),
        "client_order_id": client_order_id,
        "venue": ALPACA_VENUE,
        "asset_class": "equity",
        "symbol": normalized_symbol,
        "instrument_id": f"{normalized_symbol}.{ALPACA_VENUE}",
        "side": normalized_side,
        "quantity": quantity,
        "order_type": order_type,
        "time_in_force": time_in_force,
        "limit_price": None if limit_price is None else round(limit_price, 2),
        "strategy_family": _as_text(attempt.get("strategy_family"))
        or _as_text(attempt.get("strategy")),
    }


def _runtime_usage_summary(config_root: Any = None) -> dict[str, dict[str, Any]]:
    counts: dict[str, Counter[str]] = {
        ALPACA_DIRECT_RUNTIME: Counter(),
        NAUTILUS_RUNTIME: Counter(),
    }
    automation_counts: Counter[str] = Counter()
    for bot in load_active_bots(config_root).values():
        for runtime in bot.automations:
            if not runtime.automation.is_entry:
                continue
            execution_runtime = normalize_execution_runtime(
                runtime.automation.execution_runtime
            )
            automation_counts[execution_runtime] += 1
            counts[execution_runtime][runtime.strategy_config.strategy_family] += 1

    return {
        runtime: {
            "entry_automation_count": int(automation_counts.get(runtime, 0)),
            "strategy_families": dict(sorted(counts[runtime].items())),
        }
        for runtime in sorted(SUPPORTED_EXECUTION_RUNTIMES)
    }


def resolve_execution_runtime_capabilities(config_root: Any = None) -> dict[str, Any]:
    usage = _runtime_usage_summary(config_root)
    bridge = describe_nautilus_bridge()
    nautilus_ready = bool(bridge.get("ready"))
    return {
        "schema_version": EXECUTION_RUNTIME_CAPABILITIES_SCHEMA_VERSION,
        "default_runtime": ALPACA_DIRECT_RUNTIME,
        "runtimes": [
            {
                "runtime": ALPACA_DIRECT_RUNTIME,
                "status": "ready",
                "ready": True,
                **usage[ALPACA_DIRECT_RUNTIME],
                "capabilities": [
                    {
                        "name": "alpaca_broker_order_submit",
                        "adapter": "python_native_alpaca_order_adapter",
                        "asset_classes": ["equity", "option"],
                        "actions": ["buy", "sell", "open", "close"],
                        "structures": [
                            "single_name_equity",
                            "single_leg_option",
                            "alpaca_order_payload",
                        ],
                        "status": "ready",
                    },
                    {
                        "name": "alpaca_broker_order_manage",
                        "adapter": "python_native_alpaca_order_adapter",
                        "asset_classes": ["equity", "option"],
                        "actions": ["refresh", "cancel"],
                        "status": "ready",
                    },
                ],
            },
            {
                "runtime": NAUTILUS_RUNTIME,
                "status": "ready" if nautilus_ready else "blocked",
                "ready": nautilus_ready,
                "reason": bridge.get("reason"),
                **usage[NAUTILUS_RUNTIME],
                "bridge": bridge,
                "capabilities": [
                    {
                        "name": NAUTILUS_OPTION_SPREAD_ORDER_LIST_CAPABILITY,
                        "asset_classes": ["option"],
                        "actions": sorted(NAUTILUS_OPTION_SPREAD_ACTIONS),
                        "structures": ["two_leg_vertical", "four_leg"],
                        "strategy_families": sorted(
                            NAUTILUS_OPTION_SPREAD_FAMILIES
                        ),
                        "status": "ready" if nautilus_ready else "blocked",
                    },
                    {
                        "name": "option_close_cancel_refresh",
                        "asset_classes": ["option"],
                        "actions": ["close", "cancel", "refresh"],
                        "status": "ready",
                    },
                    {
                        "name": NAUTILUS_EQUITY_BUY_SELL_CAPABILITY,
                        "asset_classes": ["equity"],
                        "actions": ["buy", "sell"],
                        "status": "ready" if nautilus_ready else "blocked",
                    },
                ],
            },
        ],
    }


__all__ = [
    "ALPACA_DIRECT_RUNTIME",
    "EXECUTION_RUNTIME_CAPABILITIES_SCHEMA_VERSION",
    "NAUTILUS_RUNTIME",
    "NAUTILUS_HANDOFF_SCHEMA_VERSION",
    "NAUTILUS_EQUITY_BUY_SELL_CAPABILITY",
    "NAUTILUS_EQUITY_HANDOFF_SCHEMA_VERSION",
    "NAUTILUS_FOUR_LEG_FAMILIES",
    "NAUTILUS_OPTION_SPREAD_FAMILIES",
    "NAUTILUS_OPTION_SPREAD_ORDER_LIST_CAPABILITY",
    "NAUTILUS_TWO_LEG_VERTICAL_FAMILIES",
    "SUPPORTED_EXECUTION_RUNTIMES",
    "build_nautilus_equity_order_handoff",
    "build_nautilus_submit_order_list_handoff",
    "execution_runtime_from_request",
    "normalize_execution_runtime",
    "resolve_execution_runtime_capabilities",
    "resolve_nautilus_handoff_capability",
]
