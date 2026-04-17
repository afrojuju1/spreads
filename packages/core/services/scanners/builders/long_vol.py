from __future__ import annotations

import argparse

from core.common import clamp
from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
    SpreadCandidate,
)

from .orders import make_long_vol_order_payload
from .shared import days_from_today, relative_spread, relative_spread_exceeds


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
