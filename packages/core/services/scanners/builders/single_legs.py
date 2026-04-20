from __future__ import annotations

import argparse

from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
    SpreadCandidate,
)
from core.services.scanners.config import strategy_option_type

from .shared import days_from_reference, relative_spread, relative_spread_exceeds
from .structures import build_long_candidate_structure


def _modeled_boundary(
    *,
    option_type: str,
    spot_price: float,
    expected_move: ExpectedMoveEstimate,
) -> float:
    return (
        spot_price + expected_move.amount
        if option_type == "call"
        else spot_price - expected_move.amount
    )


def build_long_single_legs(
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

    for expiration_date, contracts in sorted(contracts_by_expiration.items()):
        snapshot_map = snapshots_by_expiration.get(expiration_date, {})
        expected_move = expected_moves_by_expiration.get(expiration_date)
        if expected_move is None or expected_move.amount <= 0:
            continue
        days_to_expiration = days_from_reference(expiration_date, args)

        eligible_contracts = sorted(
            contracts,
            key=lambda contract: abs(contract.strike_price - spot_price),
        )
        for contract in eligible_contracts:
            snapshot = snapshot_map.get(contract.symbol)
            if snapshot is None or snapshot.delta is None:
                continue
            if option_type == "call" and contract.strike_price < spot_price:
                continue
            if option_type == "put" and contract.strike_price > spot_price:
                continue
            if contract.open_interest < args.min_open_interest:
                continue
            if snapshot.bid_size <= 0 or snapshot.ask_size <= 0:
                continue
            if relative_spread_exceeds(snapshot, args.max_relative_spread):
                continue

            delta_magnitude = abs(snapshot.delta)
            if not (args.short_delta_min <= delta_magnitude <= args.short_delta_max):
                continue

            midpoint_credit = round(snapshot.midpoint, 4)
            natural_credit = round(snapshot.ask, 4)
            if midpoint_credit < args.min_credit or natural_credit <= 0:
                continue

            breakeven = (
                round(contract.strike_price + midpoint_credit, 4)
                if option_type == "call"
                else round(contract.strike_price - midpoint_credit, 4)
            )
            short_otm_pct = (
                abs(contract.strike_price - spot_price) / spot_price
                if spot_price > 0
                else 0.0
            )
            breakeven_cushion_pct = (
                abs(breakeven - spot_price) / spot_price if spot_price > 0 else 0.0
            )
            modeled_boundary = _modeled_boundary(
                option_type=option_type,
                spot_price=spot_price,
                expected_move=expected_move,
            )
            modeled_intrinsic = (
                max(modeled_boundary - contract.strike_price, 0.0)
                if option_type == "call"
                else max(contract.strike_price - modeled_boundary, 0.0)
            )
            modeled_profit = modeled_intrinsic - midpoint_credit
            if modeled_profit <= 0:
                continue
            return_on_risk = round(modeled_profit / midpoint_credit, 4)
            if return_on_risk < args.min_return_on_risk:
                continue

            short_vs_expected_move = (
                modeled_boundary - contract.strike_price
                if option_type == "call"
                else contract.strike_price - modeled_boundary
            )
            breakeven_vs_expected_move = (
                modeled_boundary - breakeven
                if option_type == "call"
                else breakeven - modeled_boundary
            )
            fill_ratio = min(max(midpoint_credit / natural_credit, 0.0), 1.25)
            structure = build_long_candidate_structure(
                strategy=strategy,
                contracts=[contract],
                limit_price=midpoint_credit,
            )
            candidates.append(
                SpreadCandidate(
                    underlying_symbol=symbol,
                    strategy=strategy,
                    profile=args.profile,
                    expiration_date=expiration_date,
                    days_to_expiration=days_to_expiration,
                    underlying_price=spot_price,
                    legs=tuple(structure["legs"]),
                    structure_identity=str(structure["structure_identity"]),
                    short_strike=contract.strike_price,
                    long_strike=contract.strike_price,
                    width=0.0,
                    short_delta=delta_magnitude,
                    long_delta=delta_magnitude,
                    greeks_source=snapshot.greeks_source or "unknown",
                    short_midpoint=snapshot.midpoint,
                    long_midpoint=snapshot.midpoint,
                    short_bid=snapshot.bid,
                    short_ask=snapshot.ask,
                    long_bid=snapshot.bid,
                    long_ask=snapshot.ask,
                    midpoint_credit=midpoint_credit,
                    natural_credit=natural_credit,
                    max_profit=round(modeled_profit * 100.0, 2),
                    max_loss=round(midpoint_credit * 100.0, 2),
                    return_on_risk=return_on_risk,
                    breakeven=breakeven,
                    breakeven_cushion_pct=breakeven_cushion_pct,
                    short_otm_pct=short_otm_pct,
                    short_open_interest=contract.open_interest,
                    long_open_interest=contract.open_interest,
                    short_relative_spread=relative_spread(snapshot),
                    long_relative_spread=relative_spread(snapshot),
                    fill_ratio=fill_ratio,
                    min_quote_size=min(snapshot.bid_size, snapshot.ask_size),
                    order_payload=dict(structure["order_payload"]),
                    expected_move=expected_move.amount,
                    expected_move_pct=expected_move.percent_of_spot,
                    expected_move_source_strike=expected_move.reference_strike,
                    short_vs_expected_move=round(short_vs_expected_move, 4),
                    breakeven_vs_expected_move=round(breakeven_vs_expected_move, 4),
                    short_bid_size=snapshot.bid_size,
                    short_ask_size=snapshot.ask_size,
                    long_bid_size=snapshot.bid_size,
                    long_ask_size=snapshot.ask_size,
                    short_implied_volatility=snapshot.implied_volatility,
                    long_implied_volatility=snapshot.implied_volatility,
                    short_volume=snapshot.daily_volume,
                    long_volume=snapshot.daily_volume,
                    lower_breakeven=breakeven if option_type == "put" else None,
                    upper_breakeven=breakeven if option_type == "call" else None,
                )
            )
    return candidates


def build_long_calls(
    *,
    symbol: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    return build_long_single_legs(
        symbol=symbol,
        strategy="long_call",
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=args,
    )


def build_long_puts(
    *,
    symbol: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    return build_long_single_legs(
        symbol=symbol,
        strategy="long_put",
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=args,
    )


__all__ = ["build_long_calls", "build_long_puts", "build_long_single_legs"]
