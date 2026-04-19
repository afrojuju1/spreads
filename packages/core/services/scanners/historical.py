from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from core.domain.models import DailyBar, OptionContract, OptionSnapshot, SymbolMarketSlice
from core.integrations.alpaca.client import AlpacaClient
from core.integrations.calendar_events import classify_underlying_type
from core.services.market_dates import NEW_YORK
from core.services.scanners.market_data import group_contracts_by_expiration
from core.services.scanners.runtime import build_market_slice_from_loaded_data


ALPACA_OPTIONS_HISTORY_START = date(2024, 2, 1)
_MIN_SYNTHETIC_SPREAD_RATIO = 0.01
_MAX_SYNTHETIC_SPREAD_RATIO = 0.20
_RANGE_TO_SPREAD_MULTIPLIER = 0.20


def _latest_bar(bars: list[DailyBar]) -> DailyBar | None:
    if not bars:
        return None
    return max(bars, key=lambda bar: bar.timestamp)


def _dedupe_contracts(contracts: list[OptionContract]) -> list[OptionContract]:
    contract_by_symbol: dict[str, OptionContract] = {}
    for contract in contracts:
        contract_by_symbol[contract.symbol] = contract
    return list(contract_by_symbol.values())


def _historical_contracts(
    *,
    client: AlpacaClient,
    symbol: str,
    min_expiration: str,
    max_expiration: str,
    option_type: str,
) -> list[OptionContract]:
    contracts: list[OptionContract] = []
    for status in ("active", "inactive"):
        contracts.extend(
            client.list_option_contracts(
                symbol,
                min_expiration,
                max_expiration,
                option_type=option_type,
                status=status,
            )
        )
    return _dedupe_contracts(contracts)


def _synthetic_relative_spread(bar: DailyBar) -> float:
    close_price = float(bar.close)
    if close_price <= 0:
        return _MAX_SYNTHETIC_SPREAD_RATIO
    intrabar_range_ratio = abs(float(bar.high) - float(bar.low)) / close_price
    return min(
        max(
            intrabar_range_ratio * _RANGE_TO_SPREAD_MULTIPLIER,
            _MIN_SYNTHETIC_SPREAD_RATIO,
        ),
        _MAX_SYNTHETIC_SPREAD_RATIO,
    )


def _snapshot_from_bar(*, symbol: str, bar: DailyBar) -> OptionSnapshot | None:
    close_price = float(bar.close)
    if close_price <= 0:
        return None
    relative_spread = _synthetic_relative_spread(bar)
    half_spread = close_price * relative_spread / 2.0
    bid = max(round(close_price - half_spread, 4), 0.01)
    ask = round(max(close_price + half_spread, bid), 4)
    midpoint = round((bid + ask) / 2.0, 4)
    quote_size = 0 if int(bar.volume) <= 0 else min(max(int(bar.volume), 1), 1000)
    if midpoint <= 0 or quote_size <= 0:
        return None
    return OptionSnapshot(
        symbol=symbol,
        bid=bid,
        ask=ask,
        bid_size=quote_size,
        ask_size=quote_size,
        midpoint=midpoint,
        delta=None,
        gamma=None,
        theta=None,
        vega=None,
        implied_volatility=None,
        last_trade_price=close_price,
        daily_volume=int(bar.volume),
        greeks_source=None,
    )


def _historical_snapshots_by_symbol(
    *,
    client: AlpacaClient,
    contracts: list[OptionContract],
    session_date: date,
    timeframe: str,
) -> dict[str, OptionSnapshot]:
    if not contracts:
        return {}
    bars_by_symbol = client.get_option_bars(
        [contract.symbol for contract in contracts],
        start=session_date.isoformat(),
        end=session_date.isoformat(),
        timeframe=timeframe,
    )
    snapshots: dict[str, OptionSnapshot] = {}
    for contract in contracts:
        latest_bar = _latest_bar(list(bars_by_symbol.get(contract.symbol, [])))
        if latest_bar is None:
            continue
        snapshot = _snapshot_from_bar(symbol=contract.symbol, bar=latest_bar)
        if snapshot is None:
            continue
        snapshots[contract.symbol] = snapshot
    return snapshots


def _snapshots_by_expiration(
    *,
    contracts_by_expiration: dict[str, list[OptionContract]],
    snapshots_by_symbol: dict[str, OptionSnapshot],
) -> dict[str, dict[str, OptionSnapshot]]:
    payload: dict[str, dict[str, OptionSnapshot]] = {}
    for expiration_date, contracts in contracts_by_expiration.items():
        payload[expiration_date] = {
            contract.symbol: snapshots_by_symbol[contract.symbol]
            for contract in contracts
            if contract.symbol in snapshots_by_symbol
        }
    return payload


def build_historical_symbol_market_slice_from_alpaca(
    *,
    symbol: str,
    symbol_args: Any,
    client: AlpacaClient,
    greeks_provider: Any,
    as_of: datetime,
    option_bar_timeframe: str = "1Day",
) -> SymbolMarketSlice:
    reference_timestamp = as_of.astimezone(UTC)
    reference_date = reference_timestamp.astimezone(NEW_YORK).date()
    if reference_date < ALPACA_OPTIONS_HISTORY_START:
        raise ValueError(
            "Alpaca historical options data is unsupported before 2024-02-01"
        )

    normalized_symbol = str(symbol).upper()
    underlying_type = classify_underlying_type(normalized_symbol)
    min_expiration = (
        reference_date + timedelta(days=int(symbol_args.min_dte))
    ).isoformat()
    max_expiration = (
        reference_date + timedelta(days=int(symbol_args.max_dte))
    ).isoformat()

    daily_bars = client.get_daily_bars(
        normalized_symbol,
        start=(reference_date - timedelta(days=120)).isoformat(),
        end=reference_date.isoformat(),
        stock_feed=symbol_args.stock_feed,
    )
    latest_daily_bar = _latest_bar(daily_bars)
    if latest_daily_bar is None:
        raise ValueError(
            f"No Alpaca daily bars available for {normalized_symbol} on {reference_date.isoformat()}"
        )
    spot_price = float(latest_daily_bar.close)

    call_contracts = _historical_contracts(
        client=client,
        symbol=normalized_symbol,
        min_expiration=min_expiration,
        max_expiration=max_expiration,
        option_type="call",
    )
    put_contracts = _historical_contracts(
        client=client,
        symbol=normalized_symbol,
        min_expiration=min_expiration,
        max_expiration=max_expiration,
        option_type="put",
    )
    call_contracts_by_expiration = group_contracts_by_expiration(call_contracts)
    put_contracts_by_expiration = group_contracts_by_expiration(put_contracts)

    call_snapshots_by_symbol = _historical_snapshots_by_symbol(
        client=client,
        contracts=call_contracts,
        session_date=reference_date,
        timeframe=option_bar_timeframe,
    )
    put_snapshots_by_symbol = _historical_snapshots_by_symbol(
        client=client,
        contracts=put_contracts,
        session_date=reference_date,
        timeframe=option_bar_timeframe,
    )

    return build_market_slice_from_loaded_data(
        symbol=normalized_symbol,
        underlying_type=underlying_type,
        spot_price=spot_price,
        daily_bars=daily_bars,
        intraday_bars=[],
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=_snapshots_by_expiration(
            contracts_by_expiration=call_contracts_by_expiration,
            snapshots_by_symbol=call_snapshots_by_symbol,
        ),
        put_snapshots_by_expiration=_snapshots_by_expiration(
            contracts_by_expiration=put_contracts_by_expiration,
            snapshots_by_symbol=put_snapshots_by_symbol,
        ),
        greeks_provider=greeks_provider,
        greeks_as_of=reference_timestamp,
        greeks_source_mode=(
            "auto"
            if str(getattr(symbol_args, "greeks_source", "auto")).lower() == "alpaca"
            else str(getattr(symbol_args, "greeks_source", "auto") or "auto")
        ),
    )


__all__ = [
    "ALPACA_OPTIONS_HISTORY_START",
    "build_historical_symbol_market_slice_from_alpaca",
]
