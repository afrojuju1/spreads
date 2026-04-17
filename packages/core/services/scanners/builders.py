from __future__ import annotations

import argparse
import math
from dataclasses import replace
from typing import Any

from core.common import clamp
from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
    SpreadCandidate,
)
from core.domain.profiles import (
    LONG_VOL_STRATEGIES,
    zero_dte_delta_target,
    zero_dte_session_bucket,
)
from core.services.option_structures import (
    build_multileg_order_payload,
    iron_condor_opening_legs,
    long_only_opening_legs,
    net_premium_kind,
    vertical_opening_legs,
)
from core.services.scanners.config import strategy_option_type


def effective_min_credit(width: float, args: argparse.Namespace) -> float:
    threshold = args.min_credit
    if args.profile != "0dte":
        return threshold
    session_bucket = zero_dte_session_bucket()
    if session_bucket != "late":
        return threshold
    if width <= 1.0:
        return max(threshold, 0.10)
    return max(threshold, 0.15)


def days_from_today(expiration_date: str) -> int:
    from datetime import date

    return (date.fromisoformat(expiration_date) - date.today()).days


def relative_spread(snapshot: OptionSnapshot) -> float:
    return (snapshot.ask - snapshot.bid) / snapshot.midpoint


def relative_spread_exceeds(
    snapshot: OptionSnapshot,
    maximum: float,
    *,
    tolerance: float = 1e-9,
) -> bool:
    return relative_spread(snapshot) > float(maximum) + tolerance


def log_scaled_score(value: int, floor: int, ceiling: int) -> float:
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 1.0
    numerator = math.log10(value) - math.log10(max(floor, 1))
    denominator = math.log10(max(ceiling, 1)) - math.log10(max(floor, 1))
    if denominator <= 0:
        return 0.0
    return clamp(numerator / denominator)


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


def build_vertical_spreads(
    *,
    symbol: str,
    strategy: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []
    option_type = strategy_option_type(strategy)
    premium_kind = net_premium_kind(strategy)

    for expiration_date, contracts in sorted(contracts_by_expiration.items()):
        snapshot_map = snapshots_by_expiration.get(expiration_date, {})
        sorted_contracts = sorted(contracts, key=lambda contract: contract.strike_price)
        expected_move = expected_moves_by_expiration.get(expiration_date)
        days_to_expiration = days_from_today(expiration_date)

        short_contracts = (
            sorted_contracts
            if option_type == "call"
            else list(reversed(sorted_contracts))
        )

        for short_contract in short_contracts:
            short_snapshot = snapshot_map.get(short_contract.symbol)
            if not short_snapshot:
                continue
            if option_type == "call":
                if short_contract.strike_price <= spot_price:
                    continue
            else:
                if short_contract.strike_price >= spot_price:
                    continue
            if short_contract.open_interest < args.min_open_interest:
                continue
            short_leg_relative_spread = relative_spread(short_snapshot)
            if relative_spread_exceeds(short_snapshot, args.max_relative_spread):
                continue
            if short_snapshot.bid_size <= 0:
                continue
            short_delta = short_snapshot.delta
            if short_delta is None:
                continue
            short_delta_magnitude = abs(short_delta)
            if not (
                args.short_delta_min <= short_delta_magnitude <= args.short_delta_max
            ):
                continue

            short_index = sorted_contracts.index(short_contract)
            if option_type == "call" and premium_kind == "credit":
                long_contract_iterable = sorted_contracts[short_index + 1 :]
            elif option_type == "call":
                long_contract_iterable = reversed(sorted_contracts[:short_index])
            elif premium_kind == "credit":
                long_contract_iterable = reversed(sorted_contracts[:short_index])
            else:
                long_contract_iterable = sorted_contracts[short_index + 1 :]

            for long_contract in long_contract_iterable:
                if option_type == "call":
                    if premium_kind == "credit":
                        if long_contract.strike_price <= short_contract.strike_price:
                            continue
                        width = long_contract.strike_price - short_contract.strike_price
                    else:
                        if long_contract.strike_price >= short_contract.strike_price:
                            continue
                        width = short_contract.strike_price - long_contract.strike_price
                else:
                    if premium_kind == "credit":
                        if long_contract.strike_price >= short_contract.strike_price:
                            continue
                        width = short_contract.strike_price - long_contract.strike_price
                    else:
                        if long_contract.strike_price <= short_contract.strike_price:
                            continue
                        width = long_contract.strike_price - short_contract.strike_price
                if width < args.min_width:
                    continue
                if width > args.max_width:
                    continue

                long_snapshot = snapshot_map.get(long_contract.symbol)
                if not long_snapshot:
                    continue
                if long_contract.open_interest < args.min_open_interest:
                    continue
                long_leg_relative_spread = relative_spread(long_snapshot)
                if relative_spread_exceeds(long_snapshot, args.max_relative_spread):
                    continue
                if long_snapshot.ask_size <= 0:
                    continue
                long_delta = long_snapshot.delta

                if premium_kind == "debit":
                    midpoint_credit = long_snapshot.midpoint - short_snapshot.midpoint
                    natural_credit = long_snapshot.ask - short_snapshot.bid
                else:
                    midpoint_credit = short_snapshot.midpoint - long_snapshot.midpoint
                    natural_credit = short_snapshot.bid - long_snapshot.ask
                if midpoint_credit < effective_min_credit(width, args):
                    continue
                if natural_credit <= 0:
                    continue
                if midpoint_credit >= width:
                    continue

                if premium_kind == "debit":
                    max_profit = (width - midpoint_credit) * 100.0
                    max_loss = midpoint_credit * 100.0
                else:
                    max_profit = midpoint_credit * 100.0
                    max_loss = (width - midpoint_credit) * 100.0
                if max_loss <= 0 or max_profit <= 0:
                    continue

                if premium_kind == "debit":
                    return_on_risk = (width - midpoint_credit) / midpoint_credit
                else:
                    return_on_risk = midpoint_credit / (width - midpoint_credit)
                if return_on_risk < args.min_return_on_risk:
                    continue

                if option_type == "call":
                    breakeven = (
                        long_contract.strike_price + midpoint_credit
                        if premium_kind == "debit"
                        else short_contract.strike_price + midpoint_credit
                    )
                    short_otm_pct = (
                        short_contract.strike_price - spot_price
                    ) / spot_price
                    breakeven_cushion_pct = (breakeven - spot_price) / spot_price
                else:
                    breakeven = (
                        long_contract.strike_price - midpoint_credit
                        if premium_kind == "debit"
                        else short_contract.strike_price - midpoint_credit
                    )
                    short_otm_pct = (
                        spot_price - short_contract.strike_price
                    ) / spot_price
                    breakeven_cushion_pct = (spot_price - breakeven) / spot_price
                fill_ratio = clamp(
                    midpoint_credit / natural_credit
                    if premium_kind == "debit"
                    else natural_credit / midpoint_credit,
                    0.0,
                    1.25,
                )
                short_vs_expected_move = None
                breakeven_vs_expected_move = None
                expected_move_amount = None
                expected_move_pct = None
                expected_move_source_strike = None
                if expected_move:
                    expected_move_amount = expected_move.amount
                    expected_move_pct = expected_move.percent_of_spot
                    expected_move_source_strike = expected_move.reference_strike
                    if option_type == "call":
                        expected_move_boundary = spot_price + expected_move.amount
                        short_vs_expected_move = (
                            short_contract.strike_price - expected_move_boundary
                        )
                        breakeven_vs_expected_move = (
                            expected_move_boundary - breakeven
                            if premium_kind == "debit"
                            else breakeven - expected_move_boundary
                        )
                    else:
                        expected_move_boundary = spot_price - expected_move.amount
                        short_vs_expected_move = (
                            expected_move_boundary - short_contract.strike_price
                        )
                        breakeven_vs_expected_move = (
                            breakeven - expected_move_boundary
                            if premium_kind == "debit"
                            else expected_move_boundary - breakeven
                        )

                candidates.append(
                    SpreadCandidate(
                        underlying_symbol=symbol,
                        strategy=strategy,
                        profile=args.profile,
                        expiration_date=expiration_date,
                        days_to_expiration=days_to_expiration,
                        underlying_price=spot_price,
                        short_symbol=short_contract.symbol,
                        long_symbol=long_contract.symbol,
                        short_strike=short_contract.strike_price,
                        long_strike=long_contract.strike_price,
                        width=width,
                        short_delta=short_delta,
                        long_delta=long_delta,
                        greeks_source=short_snapshot.greeks_source
                        if short_snapshot.greeks_source == long_snapshot.greeks_source
                        else "mixed",
                        short_midpoint=short_snapshot.midpoint,
                        long_midpoint=long_snapshot.midpoint,
                        short_bid=short_snapshot.bid,
                        short_ask=short_snapshot.ask,
                        long_bid=long_snapshot.bid,
                        long_ask=long_snapshot.ask,
                        midpoint_credit=midpoint_credit,
                        natural_credit=natural_credit,
                        max_profit=max_profit,
                        max_loss=max_loss,
                        return_on_risk=return_on_risk,
                        breakeven=breakeven,
                        breakeven_cushion_pct=breakeven_cushion_pct,
                        short_otm_pct=short_otm_pct,
                        short_open_interest=short_contract.open_interest,
                        long_open_interest=long_contract.open_interest,
                        short_relative_spread=short_leg_relative_spread,
                        long_relative_spread=long_leg_relative_spread,
                        fill_ratio=fill_ratio,
                        min_quote_size=min(
                            short_snapshot.bid_size,
                            short_snapshot.ask_size,
                            long_snapshot.bid_size,
                            long_snapshot.ask_size,
                        ),
                        expected_move=expected_move_amount,
                        expected_move_pct=expected_move_pct,
                        expected_move_source_strike=expected_move_source_strike,
                        short_vs_expected_move=short_vs_expected_move,
                        breakeven_vs_expected_move=breakeven_vs_expected_move,
                        order_payload=make_order_payload(
                            short_contract.symbol,
                            long_contract.symbol,
                            midpoint_credit,
                            strategy=strategy,
                        ),
                        short_bid_size=short_snapshot.bid_size,
                        short_ask_size=short_snapshot.ask_size,
                        long_bid_size=long_snapshot.bid_size,
                        long_ask_size=long_snapshot.ask_size,
                        short_implied_volatility=short_snapshot.implied_volatility,
                        long_implied_volatility=long_snapshot.implied_volatility,
                        short_volume=short_snapshot.daily_volume,
                        long_volume=long_snapshot.daily_volume,
                    )
                )

    return candidates


def build_iron_condors(
    *,
    symbol: str,
    spot_price: float,
    call_contracts_by_expiration: dict[str, list[OptionContract]],
    put_contracts_by_expiration: dict[str, list[OptionContract]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []
    common_expirations = sorted(
        set(call_contracts_by_expiration).intersection(put_contracts_by_expiration)
    )
    delta_window = max(args.short_delta_max - args.short_delta_min, 0.08)

    for expiration_date in common_expirations:
        days_to_expiration = days_from_today(expiration_date)
        if days_to_expiration <= 0:
            continue

        call_contracts = sorted(
            call_contracts_by_expiration.get(expiration_date, []),
            key=lambda contract: contract.strike_price,
        )
        put_contracts = sorted(
            put_contracts_by_expiration.get(expiration_date, []),
            key=lambda contract: contract.strike_price,
        )
        call_snapshots = call_snapshots_by_expiration.get(expiration_date, {})
        put_snapshots = put_snapshots_by_expiration.get(expiration_date, {})
        expected_move = expected_moves_by_expiration.get(expiration_date)

        call_contract_by_strike = {
            round(contract.strike_price, 4): contract for contract in call_contracts
        }

        short_calls = [
            contract
            for contract in call_contracts
            if contract.strike_price > spot_price
        ]
        short_puts = list(
            reversed(
                [
                    contract
                    for contract in put_contracts
                    if contract.strike_price < spot_price
                ]
            )
        )

        for short_put in short_puts:
            short_put_snapshot = put_snapshots.get(short_put.symbol)
            if short_put_snapshot is None:
                continue
            short_put_delta = abs(short_put_snapshot.delta or 0.0)
            if (
                short_put.open_interest < args.min_open_interest
                or short_put_snapshot.bid_size <= 0
                or short_put_snapshot.ask_size <= 0
                or short_put_delta <= 0
                or relative_spread_exceeds(short_put_snapshot, args.max_relative_spread)
                or not (args.short_delta_min <= short_put_delta <= args.short_delta_max)
            ):
                continue
            short_put_index = put_contracts.index(short_put)

            for long_put in reversed(put_contracts[:short_put_index]):
                width = short_put.strike_price - long_put.strike_price
                if width < args.min_width or width > args.max_width:
                    continue
                long_put_snapshot = put_snapshots.get(long_put.symbol)
                if long_put_snapshot is None:
                    continue
                if (
                    long_put.open_interest < args.min_open_interest
                    or long_put_snapshot.bid_size <= 0
                    or long_put_snapshot.ask_size <= 0
                    or relative_spread_exceeds(
                        long_put_snapshot, args.max_relative_spread
                    )
                ):
                    continue

                for short_call in short_calls:
                    short_call_snapshot = call_snapshots.get(short_call.symbol)
                    if short_call_snapshot is None:
                        continue
                    short_call_delta = abs(short_call_snapshot.delta or 0.0)
                    if (
                        short_call.open_interest < args.min_open_interest
                        or short_call_snapshot.bid_size <= 0
                        or short_call_snapshot.ask_size <= 0
                        or short_call_delta <= 0
                        or relative_spread_exceeds(
                            short_call_snapshot,
                            args.max_relative_spread,
                        )
                        or not (
                            args.short_delta_min
                            <= short_call_delta
                            <= args.short_delta_max
                        )
                    ):
                        continue
                    if abs(short_put_delta - short_call_delta) > delta_window:
                        continue

                    long_call = call_contract_by_strike.get(
                        round(short_call.strike_price + width, 4)
                    )
                    if long_call is None:
                        continue
                    long_call_snapshot = call_snapshots.get(long_call.symbol)
                    if long_call_snapshot is None:
                        continue
                    if (
                        long_call.open_interest < args.min_open_interest
                        or long_call_snapshot.bid_size <= 0
                        or long_call_snapshot.ask_size <= 0
                        or relative_spread_exceeds(
                            long_call_snapshot,
                            args.max_relative_spread,
                        )
                    ):
                        continue

                    midpoint_credit = round(
                        short_put_snapshot.midpoint
                        + short_call_snapshot.midpoint
                        - long_put_snapshot.midpoint
                        - long_call_snapshot.midpoint,
                        4,
                    )
                    natural_credit = round(
                        short_put_snapshot.bid
                        + short_call_snapshot.bid
                        - long_put_snapshot.ask
                        - long_call_snapshot.ask,
                        4,
                    )
                    if midpoint_credit < effective_min_credit(width, args):
                        continue
                    if midpoint_credit >= width or natural_credit <= 0:
                        continue

                    max_profit = midpoint_credit * 100.0
                    max_loss = (width - midpoint_credit) * 100.0
                    if max_profit <= 0 or max_loss <= 0:
                        continue
                    return_on_risk = midpoint_credit / (width - midpoint_credit)
                    if return_on_risk < args.min_return_on_risk:
                        continue

                    lower_breakeven = short_put.strike_price - midpoint_credit
                    upper_breakeven = short_call.strike_price + midpoint_credit
                    lower_cushion_pct = (spot_price - lower_breakeven) / spot_price
                    upper_cushion_pct = (upper_breakeven - spot_price) / spot_price
                    short_otm_pct = min(
                        (spot_price - short_put.strike_price) / spot_price,
                        (short_call.strike_price - spot_price) / spot_price,
                    )
                    breakeven_cushion_pct = min(
                        lower_cushion_pct,
                        upper_cushion_pct,
                    )
                    fill_ratio = clamp(natural_credit / midpoint_credit, 0.0, 1.25)
                    short_vs_expected_move = None
                    breakeven_vs_expected_move = None
                    expected_move_amount = None
                    expected_move_pct = None
                    expected_move_source_strike = None
                    if expected_move:
                        expected_move_amount = expected_move.amount
                        expected_move_pct = expected_move.percent_of_spot
                        expected_move_source_strike = expected_move.reference_strike
                        lower_boundary = spot_price - expected_move.amount
                        upper_boundary = spot_price + expected_move.amount
                        short_vs_expected_move = min(
                            short_put.strike_price - lower_boundary,
                            upper_boundary - short_call.strike_price,
                        )
                        breakeven_vs_expected_move = min(
                            lower_breakeven - lower_boundary,
                            upper_boundary - upper_breakeven,
                        )
                    side_balance_score = round(
                        clamp(1.0 - abs(short_put_delta - short_call_delta) / delta_window),
                        4,
                    )

                    candidates.append(
                        SpreadCandidate(
                            underlying_symbol=symbol,
                            strategy="iron_condor",
                            profile=args.profile,
                            expiration_date=expiration_date,
                            days_to_expiration=days_to_expiration,
                            underlying_price=spot_price,
                            short_symbol=short_put.symbol,
                            long_symbol=long_put.symbol,
                            short_strike=short_put.strike_price,
                            long_strike=long_put.strike_price,
                            width=width,
                            short_delta=round((short_put_delta + short_call_delta) / 2.0, 4),
                            long_delta=round(
                                (
                                    abs(long_put_snapshot.delta or 0.0)
                                    + abs(long_call_snapshot.delta or 0.0)
                                )
                                / 2.0,
                                4,
                            ),
                            greeks_source=short_put_snapshot.greeks_source
                            if (
                                short_put_snapshot.greeks_source
                                == long_put_snapshot.greeks_source
                                == short_call_snapshot.greeks_source
                                == long_call_snapshot.greeks_source
                            )
                            else "mixed",
                            short_midpoint=round(
                                (
                                    short_put_snapshot.midpoint
                                    + short_call_snapshot.midpoint
                                )
                                / 2.0,
                                4,
                            ),
                            long_midpoint=round(
                                (
                                    long_put_snapshot.midpoint
                                    + long_call_snapshot.midpoint
                                )
                                / 2.0,
                                4,
                            ),
                            short_bid=round(
                                (short_put_snapshot.bid + short_call_snapshot.bid) / 2.0,
                                4,
                            ),
                            short_ask=round(
                                (short_put_snapshot.ask + short_call_snapshot.ask) / 2.0,
                                4,
                            ),
                            long_bid=round(
                                (long_put_snapshot.bid + long_call_snapshot.bid) / 2.0,
                                4,
                            ),
                            long_ask=round(
                                (long_put_snapshot.ask + long_call_snapshot.ask) / 2.0,
                                4,
                            ),
                            midpoint_credit=midpoint_credit,
                            natural_credit=natural_credit,
                            max_profit=max_profit,
                            max_loss=max_loss,
                            return_on_risk=return_on_risk,
                            breakeven=spot_price,
                            breakeven_cushion_pct=breakeven_cushion_pct,
                            short_otm_pct=short_otm_pct,
                            short_open_interest=min(
                                short_put.open_interest,
                                short_call.open_interest,
                            ),
                            long_open_interest=min(
                                long_put.open_interest,
                                long_call.open_interest,
                            ),
                            short_relative_spread=max(
                                relative_spread(short_put_snapshot),
                                relative_spread(short_call_snapshot),
                            ),
                            long_relative_spread=max(
                                relative_spread(long_put_snapshot),
                                relative_spread(long_call_snapshot),
                            ),
                            fill_ratio=fill_ratio,
                            min_quote_size=min(
                                short_put_snapshot.bid_size,
                                short_put_snapshot.ask_size,
                                short_call_snapshot.bid_size,
                                short_call_snapshot.ask_size,
                                long_put_snapshot.bid_size,
                                long_put_snapshot.ask_size,
                                long_call_snapshot.bid_size,
                                long_call_snapshot.ask_size,
                            ),
                            order_payload=make_iron_condor_order_payload(
                                short_put_symbol=short_put.symbol,
                                long_put_symbol=long_put.symbol,
                                short_call_symbol=short_call.symbol,
                                long_call_symbol=long_call.symbol,
                                limit_price=midpoint_credit,
                            ),
                            expected_move=expected_move_amount,
                            expected_move_pct=expected_move_pct,
                            expected_move_source_strike=expected_move_source_strike,
                            short_vs_expected_move=short_vs_expected_move,
                            breakeven_vs_expected_move=breakeven_vs_expected_move,
                            short_bid_size=min(
                                short_put_snapshot.bid_size,
                                short_call_snapshot.bid_size,
                            ),
                            short_ask_size=min(
                                short_put_snapshot.ask_size,
                                short_call_snapshot.ask_size,
                            ),
                            long_bid_size=min(
                                long_put_snapshot.bid_size,
                                long_call_snapshot.bid_size,
                            ),
                            long_ask_size=min(
                                long_put_snapshot.ask_size,
                                long_call_snapshot.ask_size,
                            ),
                            short_implied_volatility=round(
                                (
                                    (short_put_snapshot.implied_volatility or 0.0)
                                    + (short_call_snapshot.implied_volatility or 0.0)
                                )
                                / 2.0,
                                4,
                            ),
                            long_implied_volatility=round(
                                (
                                    (long_put_snapshot.implied_volatility or 0.0)
                                    + (long_call_snapshot.implied_volatility or 0.0)
                                )
                                / 2.0,
                                4,
                            ),
                            short_volume=(short_put_snapshot.daily_volume or 0)
                            + (short_call_snapshot.daily_volume or 0),
                            long_volume=(long_put_snapshot.daily_volume or 0)
                            + (long_call_snapshot.daily_volume or 0),
                            secondary_short_symbol=short_call.symbol,
                            secondary_long_symbol=long_call.symbol,
                            secondary_short_strike=short_call.strike_price,
                            secondary_long_strike=long_call.strike_price,
                            lower_breakeven=lower_breakeven,
                            upper_breakeven=upper_breakeven,
                            side_balance_score=side_balance_score,
                            wing_symmetry_ratio=1.0,
                        )
                    )

    return candidates


def build_credit_spreads(
    *,
    symbol: str,
    strategy: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    return build_vertical_spreads(
        symbol=symbol,
        strategy=strategy,
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=args,
    )


def _ratio_or_none(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0.0):
        return None
    return round(numerator / denominator, 4)


def build_long_straddles(
    *,
    symbol: str,
    spot_price: float,
    call_contracts_by_expiration: dict[str, list[OptionContract]],
    put_contracts_by_expiration: dict[str, list[OptionContract]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []

    for expiration_date, call_contracts in sorted(call_contracts_by_expiration.items()):
        put_contracts = put_contracts_by_expiration.get(expiration_date, [])
        call_snapshot_map = call_snapshots_by_expiration.get(expiration_date, {})
        put_snapshot_map = put_snapshots_by_expiration.get(expiration_date, {})
        put_by_strike = {contract.strike_price: contract for contract in put_contracts}
        expected_move = expected_moves_by_expiration.get(expiration_date)
        days_to_expiration = days_from_today(expiration_date)

        shared_contracts = sorted(
            (
                contract
                for contract in call_contracts
                if contract.strike_price in put_by_strike
            ),
            key=lambda contract: (
                abs(contract.strike_price - spot_price),
                contract.strike_price,
            ),
        )
        for call_contract in shared_contracts:
            put_contract = put_by_strike.get(call_contract.strike_price)
            if put_contract is None:
                continue
            call_snapshot = call_snapshot_map.get(call_contract.symbol)
            put_snapshot = put_snapshot_map.get(put_contract.symbol)
            if call_snapshot is None or put_snapshot is None:
                continue
            if (
                call_contract.open_interest < args.min_open_interest
                or put_contract.open_interest < args.min_open_interest
                or call_snapshot.bid_size <= 0
                or call_snapshot.ask_size <= 0
                or put_snapshot.bid_size <= 0
                or put_snapshot.ask_size <= 0
                or relative_spread_exceeds(call_snapshot, args.max_relative_spread)
                or relative_spread_exceeds(put_snapshot, args.max_relative_spread)
            ):
                continue

            call_delta = abs(call_snapshot.delta or 0.0)
            put_delta = abs(put_snapshot.delta or 0.0)
            if call_snapshot.delta is None or put_snapshot.delta is None:
                continue
            if abs(call_delta - 0.50) > 0.20 or abs(put_delta - 0.50) > 0.20:
                continue

            midpoint_credit = round(call_snapshot.midpoint + put_snapshot.midpoint, 4)
            natural_credit = round(call_snapshot.ask + put_snapshot.ask, 4)
            if midpoint_credit < args.min_credit or natural_credit <= 0:
                continue

            break_even_move = midpoint_credit
            modeled_move_vs_implied_move = _ratio_or_none(
                expected_move.amount if expected_move is not None else None,
                midpoint_credit,
            )
            modeled_move_vs_break_even_move = modeled_move_vs_implied_move
            return_on_risk = (
                modeled_move_vs_break_even_move
                or modeled_move_vs_implied_move
                or round(max(call_delta, put_delta), 4)
            )
            fill_ratio = clamp(midpoint_credit / natural_credit, 0.0, 1.25)
            strike = call_contract.strike_price
            short_otm_pct = (
                abs(strike - spot_price) / spot_price if spot_price > 0 else 0.0
            )
            breakeven_cushion_pct = (
                break_even_move / spot_price if spot_price > 0 else 0.0
            )
            average_delta = round((call_delta + put_delta) / 2.0, 4)
            side_balance_score = round(
                clamp(1.0 - abs(call_delta - put_delta) / 0.35),
                4,
            )
            expected_move_amount = expected_move_pct = expected_move_source_strike = (
                None
            )
            short_vs_expected_move = breakeven_vs_expected_move = None
            if expected_move is not None:
                expected_move_amount = expected_move.amount
                expected_move_pct = expected_move.percent_of_spot
                expected_move_source_strike = expected_move.reference_strike
                short_vs_expected_move = expected_move.amount - midpoint_credit
                breakeven_vs_expected_move = expected_move.amount - break_even_move

            candidates.append(
                SpreadCandidate(
                    underlying_symbol=symbol,
                    strategy="long_straddle",
                    profile=args.profile,
                    expiration_date=expiration_date,
                    days_to_expiration=days_to_expiration,
                    underlying_price=spot_price,
                    short_symbol=put_contract.symbol,
                    long_symbol=call_contract.symbol,
                    short_strike=put_contract.strike_price,
                    long_strike=call_contract.strike_price,
                    width=0.0,
                    short_delta=average_delta,
                    long_delta=average_delta,
                    greeks_source=call_snapshot.greeks_source
                    if call_snapshot.greeks_source == put_snapshot.greeks_source
                    else "mixed",
                    short_midpoint=put_snapshot.midpoint,
                    long_midpoint=call_snapshot.midpoint,
                    short_bid=put_snapshot.bid,
                    short_ask=put_snapshot.ask,
                    long_bid=call_snapshot.bid,
                    long_ask=call_snapshot.ask,
                    midpoint_credit=midpoint_credit,
                    natural_credit=natural_credit,
                    max_profit=round(
                        max(
                            (expected_move_amount or midpoint_credit) - midpoint_credit,
                            0.01,
                        )
                        * 100.0,
                        2,
                    ),
                    max_loss=round(midpoint_credit * 100.0, 2),
                    return_on_risk=return_on_risk,
                    breakeven=strike,
                    breakeven_cushion_pct=breakeven_cushion_pct,
                    short_otm_pct=short_otm_pct,
                    short_open_interest=put_contract.open_interest,
                    long_open_interest=call_contract.open_interest,
                    short_relative_spread=relative_spread(put_snapshot),
                    long_relative_spread=relative_spread(call_snapshot),
                    fill_ratio=fill_ratio,
                    min_quote_size=min(
                        put_snapshot.bid_size,
                        put_snapshot.ask_size,
                        call_snapshot.bid_size,
                        call_snapshot.ask_size,
                    ),
                    order_payload=make_long_vol_order_payload(
                        symbols=(put_contract.symbol, call_contract.symbol),
                        limit_price=midpoint_credit,
                        strategy="long_straddle",
                    ),
                    expected_move=expected_move_amount,
                    expected_move_pct=expected_move_pct,
                    expected_move_source_strike=expected_move_source_strike,
                    modeled_move_vs_implied_move=modeled_move_vs_implied_move,
                    modeled_move_vs_break_even_move=modeled_move_vs_break_even_move,
                    short_vs_expected_move=short_vs_expected_move,
                    breakeven_vs_expected_move=breakeven_vs_expected_move,
                    short_bid_size=put_snapshot.bid_size,
                    short_ask_size=put_snapshot.ask_size,
                    long_bid_size=call_snapshot.bid_size,
                    long_ask_size=call_snapshot.ask_size,
                    short_implied_volatility=put_snapshot.implied_volatility,
                    long_implied_volatility=call_snapshot.implied_volatility,
                    short_volume=put_snapshot.daily_volume,
                    long_volume=call_snapshot.daily_volume,
                    lower_breakeven=round(strike - midpoint_credit, 4),
                    upper_breakeven=round(strike + midpoint_credit, 4),
                    side_balance_score=side_balance_score,
                    wing_symmetry_ratio=1.0,
                )
            )

    return candidates


def build_long_strangles(
    *,
    symbol: str,
    spot_price: float,
    call_contracts_by_expiration: dict[str, list[OptionContract]],
    put_contracts_by_expiration: dict[str, list[OptionContract]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []
    delta_window = max(args.short_delta_max - args.short_delta_min, 0.05)

    for expiration_date, call_contracts in sorted(call_contracts_by_expiration.items()):
        put_contracts = put_contracts_by_expiration.get(expiration_date, [])
        call_snapshot_map = call_snapshots_by_expiration.get(expiration_date, {})
        put_snapshot_map = put_snapshots_by_expiration.get(expiration_date, {})
        expected_move = expected_moves_by_expiration.get(expiration_date)
        days_to_expiration = days_from_today(expiration_date)

        eligible_calls = [
            contract
            for contract in call_contracts
            if contract.strike_price > spot_price
            and contract.open_interest >= args.min_open_interest
            and (call_snapshot_map.get(contract.symbol) is not None)
        ]
        eligible_puts = [
            contract
            for contract in put_contracts
            if contract.strike_price < spot_price
            and contract.open_interest >= args.min_open_interest
            and (put_snapshot_map.get(contract.symbol) is not None)
        ]
        eligible_calls.sort(
            key=lambda contract: abs(
                abs(call_snapshot_map[contract.symbol].delta or 0.0)
                - args.short_delta_target
            )
        )
        eligible_puts.sort(
            key=lambda contract: abs(
                abs(put_snapshot_map[contract.symbol].delta or 0.0)
                - args.short_delta_target
            )
        )

        for put_contract in eligible_puts[:6]:
            put_snapshot = put_snapshot_map.get(put_contract.symbol)
            if put_snapshot is None or put_snapshot.delta is None:
                continue
            put_delta = abs(put_snapshot.delta)
            if not (args.short_delta_min <= put_delta <= args.short_delta_max):
                continue
            if (
                put_snapshot.bid_size <= 0
                or put_snapshot.ask_size <= 0
                or relative_spread_exceeds(put_snapshot, args.max_relative_spread)
            ):
                continue

            for call_contract in eligible_calls[:6]:
                call_snapshot = call_snapshot_map.get(call_contract.symbol)
                if call_snapshot is None or call_snapshot.delta is None:
                    continue
                call_delta = abs(call_snapshot.delta)
                if not (args.short_delta_min <= call_delta <= args.short_delta_max):
                    continue
                if (
                    call_snapshot.bid_size <= 0
                    or call_snapshot.ask_size <= 0
                    or relative_spread_exceeds(call_snapshot, args.max_relative_spread)
                ):
                    continue

                lower_distance = spot_price - put_contract.strike_price
                upper_distance = call_contract.strike_price - spot_price
                if lower_distance <= 0 or upper_distance <= 0:
                    continue
                wing_symmetry_ratio = round(
                    min(lower_distance, upper_distance)
                    / max(lower_distance, upper_distance),
                    4,
                )
                if wing_symmetry_ratio < 0.55:
                    continue

                midpoint_credit = round(
                    put_snapshot.midpoint + call_snapshot.midpoint, 4
                )
                natural_credit = round(put_snapshot.ask + call_snapshot.ask, 4)
                if midpoint_credit < args.min_credit or natural_credit <= 0:
                    continue

                break_even_move = min(
                    lower_distance + midpoint_credit, upper_distance + midpoint_credit
                )
                modeled_move_vs_implied_move = _ratio_or_none(
                    expected_move.amount if expected_move is not None else None,
                    midpoint_credit,
                )
                modeled_move_vs_break_even_move = _ratio_or_none(
                    expected_move.amount if expected_move is not None else None,
                    break_even_move,
                )
                return_on_risk = (
                    modeled_move_vs_break_even_move
                    or modeled_move_vs_implied_move
                    or round((put_delta + call_delta) / 2.0, 4)
                )
                fill_ratio = clamp(midpoint_credit / natural_credit, 0.0, 1.25)
                short_otm_pct = (
                    min(lower_distance, upper_distance) / spot_price
                    if spot_price > 0
                    else 0.0
                )
                breakeven_cushion_pct = (
                    break_even_move / spot_price if spot_price > 0 else 0.0
                )
                average_delta = round((put_delta + call_delta) / 2.0, 4)
                side_balance_score = round(
                    clamp(1.0 - abs(put_delta - call_delta) / delta_window),
                    4,
                )
                expected_move_amount = expected_move_pct = (
                    expected_move_source_strike
                ) = None
                short_vs_expected_move = breakeven_vs_expected_move = None
                if expected_move is not None:
                    expected_move_amount = expected_move.amount
                    expected_move_pct = expected_move.percent_of_spot
                    expected_move_source_strike = expected_move.reference_strike
                    short_vs_expected_move = expected_move.amount - min(
                        lower_distance, upper_distance
                    )
                    breakeven_vs_expected_move = expected_move.amount - break_even_move

                candidates.append(
                    SpreadCandidate(
                        underlying_symbol=symbol,
                        strategy="long_strangle",
                        profile=args.profile,
                        expiration_date=expiration_date,
                        days_to_expiration=days_to_expiration,
                        underlying_price=spot_price,
                        short_symbol=put_contract.symbol,
                        long_symbol=call_contract.symbol,
                        short_strike=put_contract.strike_price,
                        long_strike=call_contract.strike_price,
                        width=round(
                            call_contract.strike_price - put_contract.strike_price, 4
                        ),
                        short_delta=average_delta,
                        long_delta=average_delta,
                        greeks_source=call_snapshot.greeks_source
                        if call_snapshot.greeks_source == put_snapshot.greeks_source
                        else "mixed",
                        short_midpoint=put_snapshot.midpoint,
                        long_midpoint=call_snapshot.midpoint,
                        short_bid=put_snapshot.bid,
                        short_ask=put_snapshot.ask,
                        long_bid=call_snapshot.bid,
                        long_ask=call_snapshot.ask,
                        midpoint_credit=midpoint_credit,
                        natural_credit=natural_credit,
                        max_profit=round(
                            max(
                                (expected_move_amount or break_even_move)
                                - break_even_move,
                                0.01,
                            )
                            * 100.0,
                            2,
                        ),
                        max_loss=round(midpoint_credit * 100.0, 2),
                        return_on_risk=return_on_risk,
                        breakeven=spot_price,
                        breakeven_cushion_pct=breakeven_cushion_pct,
                        short_otm_pct=short_otm_pct,
                        short_open_interest=put_contract.open_interest,
                        long_open_interest=call_contract.open_interest,
                        short_relative_spread=relative_spread(put_snapshot),
                        long_relative_spread=relative_spread(call_snapshot),
                        fill_ratio=fill_ratio,
                        min_quote_size=min(
                            put_snapshot.bid_size,
                            put_snapshot.ask_size,
                            call_snapshot.bid_size,
                            call_snapshot.ask_size,
                        ),
                        order_payload=make_long_vol_order_payload(
                            symbols=(put_contract.symbol, call_contract.symbol),
                            limit_price=midpoint_credit,
                            strategy="long_strangle",
                        ),
                        expected_move=expected_move_amount,
                        expected_move_pct=expected_move_pct,
                        expected_move_source_strike=expected_move_source_strike,
                        modeled_move_vs_implied_move=modeled_move_vs_implied_move,
                        modeled_move_vs_break_even_move=modeled_move_vs_break_even_move,
                        short_vs_expected_move=short_vs_expected_move,
                        breakeven_vs_expected_move=breakeven_vs_expected_move,
                        short_bid_size=put_snapshot.bid_size,
                        short_ask_size=put_snapshot.ask_size,
                        long_bid_size=call_snapshot.bid_size,
                        long_ask_size=call_snapshot.ask_size,
                        short_implied_volatility=put_snapshot.implied_volatility,
                        long_implied_volatility=call_snapshot.implied_volatility,
                        short_volume=put_snapshot.daily_volume,
                        long_volume=call_snapshot.daily_volume,
                        lower_breakeven=round(
                            put_contract.strike_price - midpoint_credit, 4
                        ),
                        upper_breakeven=round(
                            call_contract.strike_price + midpoint_credit, 4
                        ),
                        side_balance_score=side_balance_score,
                        wing_symmetry_ratio=wing_symmetry_ratio,
                    )
                )

    return candidates


def score_candidate(candidate: SpreadCandidate, args: argparse.Namespace) -> float:
    premium_kind = net_premium_kind(candidate.strategy)
    long_vol = candidate.strategy in LONG_VOL_STRATEGIES
    session_bucket = zero_dte_session_bucket() if args.profile == "0dte" else None
    if args.profile == "0dte":
        delta_target = zero_dte_delta_target(session_bucket or "off_hours")
    elif candidate.strategy == "long_straddle":
        delta_target = 0.50
    else:
        delta_target = args.short_delta_target
    delta_half_band = max((args.short_delta_max - args.short_delta_min) / 2.0, 0.01)
    if candidate.strategy == "long_straddle":
        delta_half_band = 0.20
    delta_score = 1.0
    if candidate.short_delta is not None:
        delta_score = 1.0 - min(
            abs(abs(candidate.short_delta) - delta_target) / delta_half_band, 1.0
        )

    dte_target = (args.min_dte + args.max_dte) / 2.0
    dte_half_band = max((args.max_dte - args.min_dte) / 2.0, 1.0)
    dte_score = 1.0 - min(
        abs(candidate.days_to_expiration - dte_target) / dte_half_band, 1.0
    )

    fill_score = clamp(candidate.fill_ratio)
    liquidity_score = 0.75 * log_scaled_score(
        min(candidate.short_open_interest, candidate.long_open_interest),
        floor=max(args.min_open_interest, 1),
        ceiling=max(args.min_open_interest * 8, 10),
    ) + 0.25 * clamp(candidate.min_quote_size / 100.0)

    if candidate.strategy == "long_straddle":
        width_target = 0.0
    elif candidate.strategy == "long_strangle" and candidate.expected_move is not None:
        width_target = max(candidate.expected_move * 2.0, args.min_width)
    elif args.profile == "0dte":
        width_target = 2.0 if session_bucket == "late" else 1.0
    else:
        width_target = max(
            args.min_width, 2.0 if args.profile == "core" else args.min_width
        )
    width_window = max(args.max_width - args.min_width, 1.0)
    if candidate.strategy == "long_straddle":
        width_window = 1.0
    elif candidate.strategy == "long_strangle" and candidate.expected_move is not None:
        width_window = max(candidate.expected_move, 1.0)
    width_score = 1.0 - min(abs(candidate.width - width_target) / width_window, 1.0)

    if long_vol:
        return_on_risk_score = clamp(candidate.return_on_risk / 1.10)
    else:
        return_on_risk_score = clamp(candidate.return_on_risk / 0.60)
    if long_vol:
        breakeven_cushion_score = clamp(1.0 - (candidate.breakeven_cushion_pct / 0.06))
    elif premium_kind == "debit":
        breakeven_cushion_score = clamp(1.0 - (candidate.breakeven_cushion_pct / 0.08))
    else:
        breakeven_cushion_score = clamp(candidate.breakeven_cushion_pct / 0.035)

    if long_vol:
        short_expected_move_score = clamp(
            ((candidate.modeled_move_vs_implied_move or 0.85) - 0.80) / 0.35
        )
        breakeven_expected_move_score = clamp(
            ((candidate.modeled_move_vs_break_even_move or 0.85) - 0.80) / 0.30
        )
    elif candidate.expected_move and candidate.expected_move > 0:
        short_expected_move_score = clamp(
            0.50 + (candidate.short_vs_expected_move or 0.0) / candidate.expected_move
        )
        breakeven_expected_move_score = clamp(
            0.45
            + (candidate.breakeven_vs_expected_move or 0.0) / candidate.expected_move
        )
    else:
        short_expected_move_score = clamp(candidate.short_otm_pct / 0.03)
        breakeven_expected_move_score = breakeven_cushion_score

    base_score = (
        0.24 * delta_score
        + 0.18 * short_expected_move_score
        + 0.16 * breakeven_expected_move_score
        + 0.14 * fill_score
        + 0.12 * liquidity_score
        + 0.08 * width_score
        + 0.05 * dte_score
        + 0.03 * return_on_risk_score
    )

    calendar_multiplier = {
        "clean": 1.0,
        "penalized": 0.92,
        "unknown": 0.82,
        "blocked": 0.0,
    }.get(candidate.calendar_status, 1.0)
    setup_multiplier = {
        "favorable": 1.0,
        "neutral": 0.93,
        "unfavorable": 0.78,
        "unknown": 0.88,
    }.get(candidate.setup_status, 0.88)
    data_multiplier = {
        "clean": 1.0,
        "penalized": 0.90,
        "blocked": 0.0,
    }.get(candidate.data_status, 1.0)
    return round(
        base_score * calendar_multiplier * setup_multiplier * data_multiplier * 100.0, 1
    )


def rank_candidates(
    candidates: list[SpreadCandidate], args: argparse.Namespace
) -> list[SpreadCandidate]:
    ranked = [
        replace(candidate, quality_score=score_candidate(candidate, args))
        for candidate in candidates
    ]
    return sort_candidates_for_display(ranked)


def sort_candidates_for_display(
    candidates: list[SpreadCandidate],
) -> list[SpreadCandidate]:
    ranked = list(candidates)
    ranked.sort(
        key=lambda candidate: (
            candidate.quality_score,
            candidate.return_on_risk,
            candidate.midpoint_credit,
            min(candidate.short_open_interest, candidate.long_open_interest),
        ),
        reverse=True,
    )
    return ranked


__all__ = [
    "build_iron_condors",
    "build_long_straddles",
    "build_long_strangles",
    "build_vertical_spreads",
    "rank_candidates",
    "sort_candidates_for_display",
]
