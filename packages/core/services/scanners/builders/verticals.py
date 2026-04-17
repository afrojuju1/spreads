from __future__ import annotations

import argparse

from core.common import clamp
from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
    SpreadCandidate,
)
from core.services.option_structures import net_premium_kind
from core.services.scanners.config import strategy_option_type

from .orders import make_order_payload
from .shared import (
    days_from_today,
    effective_min_credit,
    relative_spread,
    relative_spread_exceeds,
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
