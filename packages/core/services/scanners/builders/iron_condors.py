from __future__ import annotations

import argparse

from core.common import clamp
from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
    SpreadCandidate,
)

from .orders import make_iron_condor_order_payload
from .shared import (
    days_from_today,
    effective_min_credit,
    relative_spread,
    relative_spread_exceeds,
)


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
                        clamp(
                            1.0 - abs(short_put_delta - short_call_delta) / delta_window
                        ),
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
                            short_delta=round(
                                (short_put_delta + short_call_delta) / 2.0,
                                4,
                            ),
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
