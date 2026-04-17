from __future__ import annotations

from typing import Any

from core.services.option_structures import (
    build_multileg_order_payload,
    iron_condor_opening_legs,
    long_only_opening_legs,
    vertical_opening_legs,
)


def make_open_order_payload(
    short_symbol: str,
    long_symbol: str,
    limit_price: float,
    *,
    strategy: str = "call_credit",
) -> dict[str, Any]:
    return build_multileg_order_payload(
        legs=vertical_opening_legs(
            short_symbol=short_symbol,
            long_symbol=long_symbol,
        ),
        limit_price=limit_price,
        strategy_family=strategy,
        trade_intent="open",
    )


def make_close_order_payload(
    short_symbol: str,
    long_symbol: str,
    limit_price: float,
    *,
    strategy: str = "call_credit",
) -> dict[str, Any]:
    return build_multileg_order_payload(
        legs=[
            {
                "symbol": short_symbol,
                "ratio_qty": "1",
                "side": "buy",
                "position_intent": "buy_to_close",
                "role": "short",
            },
            {
                "symbol": long_symbol,
                "ratio_qty": "1",
                "side": "sell",
                "position_intent": "sell_to_close",
                "role": "long",
            },
        ],
        limit_price=limit_price,
        strategy_family=strategy,
        trade_intent="close",
    )


def make_order_payload(
    short_symbol: str,
    long_symbol: str,
    limit_price: float,
    *,
    strategy: str = "call_credit",
) -> dict[str, Any]:
    return make_open_order_payload(
        short_symbol=short_symbol,
        long_symbol=long_symbol,
        limit_price=limit_price,
        strategy=strategy,
    )


def make_iron_condor_order_payload(
    *,
    short_put_symbol: str,
    long_put_symbol: str,
    short_call_symbol: str,
    long_call_symbol: str,
    limit_price: float,
) -> dict[str, Any]:
    return build_multileg_order_payload(
        legs=iron_condor_opening_legs(
            short_put_symbol=short_put_symbol,
            long_put_symbol=long_put_symbol,
            short_call_symbol=short_call_symbol,
            long_call_symbol=long_call_symbol,
        ),
        limit_price=limit_price,
        strategy_family="iron_condor",
        trade_intent="open",
    )


def make_long_vol_order_payload(
    *,
    symbols: tuple[str, ...],
    limit_price: float,
    strategy: str,
) -> dict[str, Any]:
    return build_multileg_order_payload(
        legs=long_only_opening_legs(*symbols),
        limit_price=limit_price,
        strategy_family=strategy,
        trade_intent="open",
    )
