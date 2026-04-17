from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, time
from typing import Any, Iterable

from core.domain.models import ExpectedMoveEstimate, OptionContract, OptionSnapshot
from core.services.market_dates import NEW_YORK


def count_snapshot_delta_coverage(
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
) -> tuple[int, int]:
    quoted_contracts = 0
    contracts_with_delta = 0
    for snapshot_map in snapshots_by_expiration.values():
        for snapshot in snapshot_map.values():
            quoted_contracts += 1
            if snapshot.delta is not None:
                contracts_with_delta += 1
    return quoted_contracts, contracts_with_delta


def count_local_greeks_coverage(
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
) -> int:
    local_contracts = 0
    for snapshot_map in snapshots_by_expiration.values():
        for snapshot in snapshot_map.values():
            if snapshot.greeks_source == "local_bsm":
                local_contracts += 1
    return local_contracts


def count_alpaca_greeks_coverage(
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
) -> int:
    alpaca_contracts = 0
    for snapshot_map in snapshots_by_expiration.values():
        for snapshot in snapshot_map.values():
            if snapshot.greeks_source == "alpaca":
                alpaca_contracts += 1
    return alpaca_contracts


def pick_atm_expected_move(
    *,
    spot_price: float,
    expiration_date: str,
    call_contracts: list[OptionContract],
    put_contracts: list[OptionContract],
    call_snapshots: dict[str, OptionSnapshot],
    put_snapshots: dict[str, OptionSnapshot],
) -> ExpectedMoveEstimate | None:
    puts_by_strike = {contract.strike_price: contract for contract in put_contracts}
    best_estimate: ExpectedMoveEstimate | None = None
    best_distance: float | None = None

    for call_contract in call_contracts:
        put_contract = puts_by_strike.get(call_contract.strike_price)
        if not put_contract:
            continue

        call_snapshot = call_snapshots.get(call_contract.symbol)
        put_snapshot = put_snapshots.get(put_contract.symbol)
        if not call_snapshot or not put_snapshot:
            continue

        expected_move = call_snapshot.midpoint + put_snapshot.midpoint
        if expected_move <= 0:
            continue

        distance = abs(call_contract.strike_price - spot_price)
        if best_distance is not None and distance > best_distance:
            continue

        estimate = ExpectedMoveEstimate(
            expiration_date=expiration_date,
            amount=expected_move,
            percent_of_spot=expected_move / spot_price,
            reference_strike=call_contract.strike_price,
        )
        best_distance = distance
        best_estimate = estimate

    return best_estimate


def build_expected_move_estimates(
    *,
    spot_price: float,
    call_contracts_by_expiration: dict[str, list[OptionContract]],
    put_contracts_by_expiration: dict[str, list[OptionContract]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
) -> dict[str, ExpectedMoveEstimate]:
    estimates: dict[str, ExpectedMoveEstimate] = {}
    for expiration_date, call_contracts in call_contracts_by_expiration.items():
        estimate = pick_atm_expected_move(
            spot_price=spot_price,
            expiration_date=expiration_date,
            call_contracts=call_contracts,
            put_contracts=put_contracts_by_expiration.get(expiration_date, []),
            call_snapshots=call_snapshots_by_expiration.get(expiration_date, {}),
            put_snapshots=put_snapshots_by_expiration.get(expiration_date, {}),
        )
        if estimate:
            estimates[expiration_date] = estimate
    return estimates


def option_expiry_close(expiration_date: str) -> datetime:
    local_close = datetime.combine(
        datetime.fromisoformat(expiration_date).date(),
        time(16, 0),
        tzinfo=NEW_YORK,
    )
    return local_close.astimezone(UTC)


def enrich_missing_greeks(
    *,
    symbol: str,
    option_type: str,
    spot_price: float,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    greeks_provider: Any,
    as_of: datetime,
    source_mode: str,
) -> dict[str, dict[str, OptionSnapshot]]:
    if greeks_provider is None or source_mode == "alpaca":
        return snapshots_by_expiration

    enriched_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    for expiration_date, contracts in contracts_by_expiration.items():
        snapshot_map = snapshots_by_expiration.get(expiration_date, {})
        contract_by_symbol = {contract.symbol: contract for contract in contracts}
        expiry_close = option_expiry_close(expiration_date)
        updated_map: dict[str, OptionSnapshot] = {}

        for contract_symbol, snapshot in snapshot_map.items():
            if source_mode == "auto" and snapshot.delta is not None:
                updated_map[contract_symbol] = snapshot
                continue

            contract = contract_by_symbol.get(contract_symbol)
            if contract is None:
                updated_map[contract_symbol] = snapshot
                continue

            request = greeks_provider.build_request(
                symbol=symbol,
                option_symbol=contract_symbol,
                option_type=option_type,
                spot_price=spot_price,
                strike_price=contract.strike_price,
                bid=snapshot.bid,
                ask=snapshot.ask,
                expiration=expiry_close,
                as_of=as_of,
            )
            result = greeks_provider.compute(request)
            if result.status != "ok":
                if source_mode == "local":
                    updated_map[contract_symbol] = replace(
                        snapshot,
                        delta=None,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=None,
                        greeks_source=None,
                    )
                else:
                    updated_map[contract_symbol] = snapshot
                continue

            updated_map[contract_symbol] = replace(
                snapshot,
                delta=result.delta,
                gamma=result.gamma,
                theta=result.theta,
                vega=result.vega,
                implied_volatility=result.implied_volatility,
                greeks_source=result.source,
            )

        enriched_by_expiration[expiration_date] = updated_map
    return enriched_by_expiration


def group_contracts_by_expiration(
    contracts: Iterable[OptionContract],
) -> dict[str, list[OptionContract]]:
    grouped: dict[str, list[OptionContract]] = {}
    for contract in contracts:
        grouped.setdefault(contract.expiration_date, []).append(contract)
    return grouped


__all__ = [
    "build_expected_move_estimates",
    "count_alpaca_greeks_coverage",
    "count_local_greeks_coverage",
    "count_snapshot_delta_coverage",
    "enrich_missing_greeks",
    "group_contracts_by_expiration",
    "option_expiry_close",
    "pick_atm_expected_move",
]
