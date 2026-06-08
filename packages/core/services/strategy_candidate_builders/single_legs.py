from __future__ import annotations

from collections import Counter
from typing import Any

from core.domain.models import (
    ExpectedMoveEstimate,
    OptionContract,
    OptionSnapshot,
    SpreadCandidate,
)
from core.services.option_structures import net_premium_kind
from core.services.strategy_specs import strategy_option_type

from .analytics import attach_structure_analytics
from .shared import days_from_reference, relative_spread, relative_spread_exceeds
from .structures import build_long_candidate_structure, build_short_candidate_structure

_SHORT_SINGLE_LEG_STRESS_MOVE_MULTIPLE = 2.0


def _expected_move_boundary(
    *,
    option_type: str,
    spot_price: float,
    expected_move: ExpectedMoveEstimate,
) -> float:
    return spot_price + expected_move.amount if option_type == "call" else spot_price - expected_move.amount


def _modeled_intrinsic(
    *,
    option_type: str,
    strike_price: float,
    terminal_spot: float,
) -> float:
    return max(terminal_spot - strike_price, 0.0) if option_type == "call" else max(strike_price - terminal_spot, 0.0)


def _short_single_leg_stress_loss(
    *,
    option_type: str,
    strike_price: float,
    spot_price: float,
    expected_move: ExpectedMoveEstimate,
    entry_credit: float,
) -> float:
    stress_boundary = (
        spot_price + (expected_move.amount * _SHORT_SINGLE_LEG_STRESS_MOVE_MULTIPLE)
        if option_type == "call"
        else spot_price - (expected_move.amount * _SHORT_SINGLE_LEG_STRESS_MOVE_MULTIPLE)
    )
    intrinsic_loss = (
        _modeled_intrinsic(
            option_type=option_type,
            strike_price=strike_price,
            terminal_spot=stress_boundary,
        )
        - entry_credit
    )
    # Keep naked shorts finite on the scanner path with a conservative
    # expected-move stress proxy rather than implying live undefined-risk support.
    return max(intrinsic_loss, entry_credit)


def _single_leg_reject_example(
    *,
    contract: OptionContract,
    snapshot: OptionSnapshot | None,
    expiration_date: str,
) -> dict[str, object]:
    return {
        "contract": contract.symbol,
        "expiration": expiration_date,
        "strike": contract.strike_price,
        "open_interest": contract.open_interest,
        "bid": None if snapshot is None else snapshot.bid,
        "ask": None if snapshot is None else snapshot.ask,
        "delta": None if snapshot is None else snapshot.delta,
        "relative_spread": None if snapshot is None else relative_spread(snapshot),
    }


def _evaluate_single_leg_contract(
    *,
    option_type: str,
    premium_kind: str,
    spot_price: float,
    contract: OptionContract,
    snapshot: OptionSnapshot | None,
    expected_move: ExpectedMoveEstimate | None,
    args: Any,
) -> tuple[str | None, dict[str, float]]:
    if expected_move is None or expected_move.amount <= 0:
        return "no_expected_move", {}
    if snapshot is None:
        return "no_snapshot", {}
    if snapshot.delta is None:
        return "no_delta", {}
    if option_type == "call" and contract.strike_price < spot_price:
        return "itm_call_skipped", {}
    if option_type == "put" and contract.strike_price > spot_price:
        return "itm_put_skipped", {}
    if contract.open_interest < args.min_open_interest:
        return "open_interest_below_min", {}
    if snapshot.bid_size <= 0 or snapshot.ask_size <= 0:
        return "bid_or_ask_size_zero", {}
    if relative_spread_exceeds(snapshot, args.max_relative_spread):
        return "relative_spread_above_max", {}

    delta_magnitude = abs(snapshot.delta)
    if not (args.short_delta_min <= delta_magnitude <= args.short_delta_max):
        return "delta_outside_range", {}

    midpoint_credit = round(snapshot.midpoint, 4)
    natural_credit = round(snapshot.ask if premium_kind == "debit" else snapshot.bid, 4)
    if midpoint_credit < args.min_credit or natural_credit <= 0:
        return "premium_too_low_or_no_natural", {}

    breakeven = round(contract.strike_price + midpoint_credit, 4) if option_type == "call" else round(contract.strike_price - midpoint_credit, 4)
    short_otm_pct = abs(contract.strike_price - spot_price) / spot_price if spot_price > 0 else 0.0
    breakeven_cushion_pct = abs(breakeven - spot_price) / spot_price if spot_price > 0 else 0.0
    expected_move_boundary = _expected_move_boundary(
        option_type=option_type,
        spot_price=spot_price,
        expected_move=expected_move,
    )
    modeled_intrinsic = _modeled_intrinsic(
        option_type=option_type,
        strike_price=contract.strike_price,
        terminal_spot=expected_move_boundary,
    )
    if premium_kind == "debit":
        modeled_profit = modeled_intrinsic - midpoint_credit
        if modeled_profit <= 0:
            return "expected_move_profit_not_positive", {}
        max_profit = round(modeled_profit * 100.0, 2)
        max_loss = round(midpoint_credit * 100.0, 2)
        return_on_risk = round(modeled_profit / midpoint_credit, 4)
        short_vs_expected_move = (
            expected_move_boundary - contract.strike_price if option_type == "call" else contract.strike_price - expected_move_boundary
        )
        breakeven_vs_expected_move = expected_move_boundary - breakeven if option_type == "call" else breakeven - expected_move_boundary
        fill_ratio = min(max(midpoint_credit / natural_credit, 0.0), 1.25)
    else:
        stress_loss = _short_single_leg_stress_loss(
            option_type=option_type,
            strike_price=contract.strike_price,
            spot_price=spot_price,
            expected_move=expected_move,
            entry_credit=midpoint_credit,
        )
        if stress_loss <= 0:
            return "stress_loss_not_positive", {}
        max_profit = round(midpoint_credit * 100.0, 2)
        max_loss = round(stress_loss * 100.0, 2)
        return_on_risk = round(midpoint_credit / stress_loss, 4)
        short_vs_expected_move = (
            contract.strike_price - expected_move_boundary if option_type == "call" else expected_move_boundary - contract.strike_price
        )
        breakeven_vs_expected_move = breakeven - expected_move_boundary if option_type == "call" else expected_move_boundary - breakeven
        fill_ratio = min(max(natural_credit / midpoint_credit, 0.0), 1.25)

    if return_on_risk < args.min_return_on_risk:
        return "return_on_risk_below_min", {}

    return None, {
        "midpoint_credit": midpoint_credit,
        "natural_credit": natural_credit,
        "breakeven": breakeven,
        "short_otm_pct": short_otm_pct,
        "breakeven_cushion_pct": breakeven_cushion_pct,
        "expected_move_boundary": expected_move_boundary,
        "max_profit": max_profit,
        "max_loss": max_loss,
        "return_on_risk": return_on_risk,
        "short_vs_expected_move": short_vs_expected_move,
        "breakeven_vs_expected_move": breakeven_vs_expected_move,
        "fill_ratio": fill_ratio,
    }


def diagnose_single_leg_rejections(
    *,
    strategy: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: Any,
    example_limit: int = 3,
) -> dict[str, object]:
    option_type = strategy_option_type(strategy)
    premium_kind = net_premium_kind(strategy)
    counts: Counter[str] = Counter()
    examples: dict[str, list[dict[str, object]]] = {}
    pass_examples: list[dict[str, object]] = []
    pass_count = 0

    if premium_kind not in {"credit", "debit"}:
        return {"reject_counts": {}, "examples": {}, "pass_count": 0, "pass_examples": []}

    for expiration_date, contracts in sorted(contracts_by_expiration.items()):
        snapshot_map = snapshots_by_expiration.get(expiration_date, {})
        expected_move = expected_moves_by_expiration.get(expiration_date)
        eligible_contracts = sorted(contracts, key=lambda contract: abs(contract.strike_price - spot_price))
        for contract in eligible_contracts:
            snapshot = snapshot_map.get(contract.symbol)
            reason, metrics = _evaluate_single_leg_contract(
                option_type=option_type,
                premium_kind=premium_kind,
                spot_price=spot_price,
                contract=contract,
                snapshot=snapshot,
                expected_move=expected_move,
                args=args,
            )
            if reason is None:
                pass_count += 1
                if len(pass_examples) < max(int(example_limit), 1):
                    pass_examples.append(
                        {
                            "contract": contract.symbol,
                            "expiration": expiration_date,
                            "strike": contract.strike_price,
                            "open_interest": contract.open_interest,
                            "delta": None if snapshot is None else snapshot.delta,
                            "midpoint": None if snapshot is None else snapshot.midpoint,
                            "return_on_risk": metrics.get("return_on_risk"),
                        }
                    )
                continue
            counts[reason] += 1
            bucket = examples.setdefault(reason, [])
            if len(bucket) < max(int(example_limit), 1):
                bucket.append(_single_leg_reject_example(contract=contract, snapshot=snapshot, expiration_date=expiration_date))

    return {
        "reject_counts": dict(sorted(counts.items())),
        "examples": examples,
        "pass_count": pass_count,
        "pass_examples": pass_examples,
    }


def build_single_legs(
    *,
    symbol: str,
    strategy: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: Any,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []
    option_type = strategy_option_type(strategy)
    premium_kind = net_premium_kind(strategy)
    if premium_kind not in {"credit", "debit"}:
        return candidates

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
            reason, metrics = _evaluate_single_leg_contract(
                option_type=option_type,
                premium_kind=premium_kind,
                spot_price=spot_price,
                contract=contract,
                snapshot=snapshot,
                expected_move=expected_move,
                args=args,
            )
            if reason is not None or snapshot is None or expected_move is None:
                continue

            delta_magnitude = abs(snapshot.delta)
            midpoint_credit = float(metrics["midpoint_credit"])
            natural_credit = float(metrics["natural_credit"])
            breakeven = float(metrics["breakeven"])
            short_otm_pct = float(metrics["short_otm_pct"])
            breakeven_cushion_pct = float(metrics["breakeven_cushion_pct"])
            if premium_kind == "debit":
                structure = build_long_candidate_structure(
                    strategy=strategy,
                    contracts=[contract],
                    limit_price=midpoint_credit,
                )
            else:
                structure = build_short_candidate_structure(
                    strategy=strategy,
                    contracts=[contract],
                    limit_price=midpoint_credit,
                )

            candidate = SpreadCandidate(
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
                max_profit=float(metrics["max_profit"]),
                max_loss=float(metrics["max_loss"]),
                return_on_risk=float(metrics["return_on_risk"]),
                breakeven=breakeven,
                breakeven_cushion_pct=breakeven_cushion_pct,
                short_otm_pct=short_otm_pct,
                short_open_interest=contract.open_interest,
                long_open_interest=contract.open_interest,
                short_relative_spread=relative_spread(snapshot),
                long_relative_spread=relative_spread(snapshot),
                fill_ratio=float(metrics["fill_ratio"]),
                min_quote_size=min(snapshot.bid_size, snapshot.ask_size),
                order_payload=dict(structure["order_payload"]),
                expected_move=expected_move.amount,
                expected_move_pct=expected_move.percent_of_spot,
                expected_move_source_strike=expected_move.reference_strike,
                short_vs_expected_move=round(float(metrics["short_vs_expected_move"]), 4),
                breakeven_vs_expected_move=round(float(metrics["breakeven_vs_expected_move"]), 4),
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
            candidates.append(
                attach_structure_analytics(
                    candidate,
                    snapshots_by_symbol={contract.symbol: snapshot},
                    args=args,
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
    args: Any,
) -> list[SpreadCandidate]:
    return build_single_legs(
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
    args: Any,
) -> list[SpreadCandidate]:
    return build_single_legs(
        symbol=symbol,
        strategy="long_put",
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=args,
    )


def build_short_calls(
    *,
    symbol: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: Any,
) -> list[SpreadCandidate]:
    return build_single_legs(
        symbol=symbol,
        strategy="short_call",
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=args,
    )


def build_short_puts(
    *,
    symbol: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate],
    args: Any,
) -> list[SpreadCandidate]:
    return build_single_legs(
        symbol=symbol,
        strategy="short_put",
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=args,
    )


__all__ = [
    "build_long_calls",
    "build_long_puts",
    "diagnose_single_leg_rejections",
    "build_short_calls",
    "build_short_puts",
    "build_single_legs",
]
