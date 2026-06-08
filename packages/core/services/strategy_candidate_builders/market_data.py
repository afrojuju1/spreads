from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from typing import Any, Iterable

from core.domain.models import DailyBar, ExpectedMoveEstimate, IntradayBar, OptionContract, OptionSnapshot, SymbolMarketSlice
from core.integrations.alpaca.client import AlpacaClient
from core.observability.logging import log_event
from core.integrations.calendar_events import classify_underlying_type
from core.services.market_dates import NEW_YORK
from core.services.strategy_candidate_builders.runtime_context import candidate_reference_date, candidate_reference_datetime, option_expiry_close

logger = logging.getLogger(__name__)


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


def build_market_slice_from_loaded_data(
    *,
    symbol: str,
    underlying_type: str,
    spot_price: float,
    daily_bars: list[DailyBar],
    intraday_bars: list[IntradayBar],
    call_contracts_by_expiration: dict[str, list[Any]],
    put_contracts_by_expiration: dict[str, list[Any]],
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]],
    greeks_provider: Any,
    greeks_as_of: datetime,
    greeks_source_mode: str,
) -> SymbolMarketSlice:
    resolved_call_snapshots = enrich_missing_greeks(
        symbol=symbol,
        option_type="call",
        spot_price=spot_price,
        contracts_by_expiration=call_contracts_by_expiration,
        snapshots_by_expiration=call_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        as_of=greeks_as_of,
        source_mode=greeks_source_mode,
    )
    resolved_put_snapshots = enrich_missing_greeks(
        symbol=symbol,
        option_type="put",
        spot_price=spot_price,
        contracts_by_expiration=put_contracts_by_expiration,
        snapshots_by_expiration=put_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        as_of=greeks_as_of,
        source_mode=greeks_source_mode,
    )
    expected_moves_by_expiration = build_expected_move_estimates(
        spot_price=spot_price,
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=resolved_call_snapshots,
        put_snapshots_by_expiration=resolved_put_snapshots,
    )
    return SymbolMarketSlice(
        symbol=symbol,
        underlying_type=underlying_type,
        spot_price=spot_price,
        daily_bars=tuple(daily_bars),
        intraday_bars=tuple(intraday_bars),
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=resolved_call_snapshots,
        put_snapshots_by_expiration=resolved_put_snapshots,
        expected_moves_by_expiration=expected_moves_by_expiration,
    )


def build_symbol_market_slice(
    *,
    symbol: str,
    symbol_args: Any,
    client: AlpacaClient,
    greeks_provider: Any,
) -> SymbolMarketSlice:
    normalized_symbol = symbol.upper()
    underlying_type = classify_underlying_type(normalized_symbol)
    reference_date = candidate_reference_date(symbol_args)
    reference_timestamp = candidate_reference_datetime(symbol_args) or datetime.now(UTC)
    min_expiration = (reference_date + timedelta(days=symbol_args.min_dte)).isoformat()
    max_expiration = (reference_date + timedelta(days=symbol_args.max_dte)).isoformat()

    spot_price = client.get_underlying_price(normalized_symbol, symbol_args.stock_feed)
    daily_bars: list[DailyBar] = []
    intraday_bars: list[IntradayBar] = []
    if symbol_args.setup_filter == "on":
        daily_bars = client.get_daily_bars(
            normalized_symbol,
            start=(reference_date - timedelta(days=120)).isoformat(),
            end=reference_date.isoformat(),
            stock_feed=symbol_args.stock_feed,
        )
        try:
            session_start = datetime.combine(reference_date, time(9, 30), tzinfo=NEW_YORK).astimezone(UTC)
            session_end = reference_timestamp
            intraday_bars = client.get_intraday_bars(
                normalized_symbol,
                start=session_start.isoformat(),
                end=session_end.isoformat(),
                stock_feed=symbol_args.stock_feed,
            )
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "candidate_market_slice_intraday_bars_failed",
                exc_info=True,
                symbol=normalized_symbol,
                start=session_start.isoformat(),
                end=session_end.isoformat(),
                stock_feed=symbol_args.stock_feed,
                error=str(exc),
            )
            intraday_bars = []

    call_contracts = client.list_option_contracts(normalized_symbol, min_expiration, max_expiration, option_type="call")
    put_contracts = client.list_option_contracts(normalized_symbol, min_expiration, max_expiration, option_type="put")
    call_contracts_by_expiration = group_contracts_by_expiration(call_contracts)
    put_contracts_by_expiration = group_contracts_by_expiration(put_contracts)

    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    for expiration_date in sorted(call_contracts_by_expiration):
        call_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            normalized_symbol,
            expiration_date,
            "call",
            symbol_args.feed,
        )
        put_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            normalized_symbol,
            expiration_date,
            "put",
            symbol_args.feed,
        )

    return build_market_slice_from_loaded_data(
        symbol=normalized_symbol,
        underlying_type=underlying_type,
        spot_price=spot_price,
        daily_bars=daily_bars,
        intraday_bars=intraday_bars,
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=call_snapshots_by_expiration,
        put_snapshots_by_expiration=put_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        greeks_as_of=reference_timestamp,
        greeks_source_mode=symbol_args.greeks_source,
    )


__all__ = [
    "build_expected_move_estimates",
    "build_market_slice_from_loaded_data",
    "build_symbol_market_slice",
    "count_alpaca_greeks_coverage",
    "count_local_greeks_coverage",
    "count_snapshot_delta_coverage",
    "enrich_missing_greeks",
    "group_contracts_by_expiration",
    "pick_atm_expected_move",
]
