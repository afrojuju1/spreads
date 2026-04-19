from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from core.domain.models import (
    DailyBar,
    IntradayBar,
    OptionContract,
    OptionSnapshot,
    SymbolMarketSlice,
)
from core.integrations.alpaca.client import AlpacaClient
from core.integrations.calendar_events import classify_underlying_type
from core.services.market_dates import NEW_YORK
from core.services.scanners.market_data import group_contracts_by_expiration
from core.services.scanners.runtime import build_market_slice_from_loaded_data


ALPACA_OPTIONS_HISTORY_START = date(2024, 2, 1)
_MIN_SYNTHETIC_SPREAD_RATIO = 0.01
_MAX_SYNTHETIC_SPREAD_RATIO = 0.20
_RANGE_TO_SPREAD_MULTIPLIER = 0.20


@dataclass(frozen=True)
class HistoricalSymbolSessionData:
    symbol: str
    underlying_type: str
    session_date: date
    daily_bars: tuple[DailyBar, ...]
    intraday_bars: tuple[IntradayBar, ...]
    call_contracts_by_expiration: dict[str, list[OptionContract]]
    put_contracts_by_expiration: dict[str, list[OptionContract]]
    call_option_bars_by_symbol: dict[str, tuple[DailyBar, ...]]
    put_option_bars_by_symbol: dict[str, tuple[DailyBar, ...]]


def _render_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _latest_bar(bars: list[DailyBar]) -> DailyBar | None:
    if not bars:
        return None
    return max(bars, key=lambda bar: bar.timestamp)


def _latest_timestamped_bar(
    bars: list[DailyBar] | list[IntradayBar] | tuple[DailyBar, ...] | tuple[IntradayBar, ...],
    *,
    as_of_text: str,
) -> DailyBar | IntradayBar | None:
    latest: DailyBar | IntradayBar | None = None
    for bar in bars:
        if str(bar.timestamp) > as_of_text:
            break
        latest = bar
    return latest


def _bars_through_timestamp(
    bars: tuple[IntradayBar, ...],
    *,
    as_of_text: str,
) -> list[IntradayBar]:
    sliced: list[IntradayBar] = []
    for bar in bars:
        if str(bar.timestamp) > as_of_text:
            break
        sliced.append(bar)
    return sliced


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


def _snapshot_from_bar(
    *,
    symbol: str,
    bar: DailyBar,
    daily_volume: int | None = None,
) -> OptionSnapshot | None:
    close_price = float(bar.close)
    if close_price <= 0:
        return None
    relative_spread = _synthetic_relative_spread(bar)
    half_spread = close_price * relative_spread / 2.0
    bid = max(round(close_price - half_spread, 4), 0.01)
    ask = round(max(close_price + half_spread, bid), 4)
    midpoint = round((bid + ask) / 2.0, 4)
    quote_size = 0 if int(bar.volume) <= 0 else min(max(int(bar.volume), 1), 1000)
    resolved_daily_volume = int(bar.volume) if daily_volume is None else int(daily_volume)
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
        daily_volume=resolved_daily_volume,
        greeks_source=None,
    )


def _historical_option_bars_by_symbol(
    *,
    client: AlpacaClient,
    contracts: list[OptionContract],
    start: str,
    end: str,
    timeframe: str,
) -> dict[str, tuple[DailyBar, ...]]:
    if not contracts:
        return {}
    bars_by_symbol = client.get_option_bars(
        [contract.symbol for contract in contracts],
        start=start,
        end=end,
        timeframe=timeframe,
    )
    return {
        symbol: tuple(list(bars))
        for symbol, bars in bars_by_symbol.items()
        if list(bars)
    }


def _snapshot_from_bars_before(
    *,
    symbol: str,
    bars: tuple[DailyBar, ...],
    as_of_text: str,
    use_latest_bar: bool = False,
) -> OptionSnapshot | None:
    if use_latest_bar and bars:
        return _snapshot_from_bar(symbol=symbol, bar=bars[-1], daily_volume=bars[-1].volume)
    latest_bar: DailyBar | None = None
    cumulative_volume = 0
    for bar in bars:
        if str(bar.timestamp) > as_of_text:
            break
        latest_bar = bar
        cumulative_volume += int(bar.volume)
    if latest_bar is None:
        return None
    return _snapshot_from_bar(
        symbol=symbol,
        bar=latest_bar,
        daily_volume=cumulative_volume,
    )


def _snapshots_by_expiration_from_bars(
    *,
    contracts_by_expiration: dict[str, list[OptionContract]],
    option_bars_by_symbol: dict[str, tuple[DailyBar, ...]],
    as_of_text: str,
    use_latest_bar: bool = False,
) -> dict[str, dict[str, OptionSnapshot]]:
    payload: dict[str, dict[str, OptionSnapshot]] = {}
    for expiration_date, contracts in contracts_by_expiration.items():
        snapshots: dict[str, OptionSnapshot] = {}
        for contract in contracts:
            bars = option_bars_by_symbol.get(contract.symbol)
            if not bars:
                continue
            snapshot = _snapshot_from_bars_before(
                symbol=contract.symbol,
                bars=bars,
                as_of_text=as_of_text,
                use_latest_bar=use_latest_bar,
            )
            if snapshot is None:
                continue
            snapshots[contract.symbol] = snapshot
        payload[expiration_date] = snapshots
    return payload


def _session_window(session_date: date) -> tuple[datetime, datetime]:
    session_start = datetime.combine(session_date, time(9, 30), tzinfo=NEW_YORK)
    session_end = datetime.combine(session_date, time(16, 0), tzinfo=NEW_YORK)
    return session_start.astimezone(UTC), session_end.astimezone(UTC)


def build_historical_symbol_session_data_from_alpaca(
    *,
    symbol: str,
    symbol_args: Any,
    client: AlpacaClient,
    session_date: date,
    option_bar_timeframe: str = "1Day",
    stock_intraday_timeframe: str = "1Min",
    include_intraday_stock_bars: bool = True,
) -> HistoricalSymbolSessionData:
    if session_date < ALPACA_OPTIONS_HISTORY_START:
        raise ValueError(
            "Alpaca historical options data is unsupported before 2024-02-01"
        )

    normalized_symbol = str(symbol).upper()
    underlying_type = classify_underlying_type(normalized_symbol)
    min_expiration = (
        session_date + timedelta(days=int(symbol_args.min_dte))
    ).isoformat()
    max_expiration = (
        session_date + timedelta(days=int(symbol_args.max_dte))
    ).isoformat()
    session_start, session_end = _session_window(session_date)
    option_bars_start = (
        session_date.isoformat()
        if option_bar_timeframe == "1Day"
        else _render_utc(session_start)
    )
    option_bars_end = (
        session_date.isoformat()
        if option_bar_timeframe == "1Day"
        else _render_utc(session_end + timedelta(minutes=1))
    )

    daily_bars = client.get_daily_bars(
        normalized_symbol,
        start=(session_date - timedelta(days=120)).isoformat(),
        end=session_date.isoformat(),
        stock_feed=symbol_args.stock_feed,
    )
    if include_intraday_stock_bars:
        try:
            intraday_bars = client.get_intraday_bars(
                normalized_symbol,
                start=_render_utc(session_start),
                end=_render_utc(session_end + timedelta(minutes=1)),
                stock_feed=symbol_args.stock_feed,
                timeframe=stock_intraday_timeframe,
            )
        except Exception:
            intraday_bars = []
    else:
        intraday_bars = []

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

    call_option_bars_by_symbol = _historical_option_bars_by_symbol(
        client=client,
        contracts=call_contracts,
        start=option_bars_start,
        end=option_bars_end,
        timeframe=option_bar_timeframe,
    )
    put_option_bars_by_symbol = _historical_option_bars_by_symbol(
        client=client,
        contracts=put_contracts,
        start=option_bars_start,
        end=option_bars_end,
        timeframe=option_bar_timeframe,
    )

    return HistoricalSymbolSessionData(
        symbol=normalized_symbol,
        underlying_type=underlying_type,
        session_date=session_date,
        daily_bars=tuple(daily_bars),
        intraday_bars=tuple(intraday_bars),
        call_contracts_by_expiration=call_contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_option_bars_by_symbol=call_option_bars_by_symbol,
        put_option_bars_by_symbol=put_option_bars_by_symbol,
    )


def build_historical_symbol_market_slice_from_session_data(
    *,
    session_data: HistoricalSymbolSessionData,
    symbol_args: Any,
    greeks_provider: Any,
    as_of: datetime,
    use_latest_option_bar_snapshot: bool = False,
) -> SymbolMarketSlice:
    reference_timestamp = as_of.astimezone(UTC)
    reference_date = reference_timestamp.astimezone(NEW_YORK).date()
    if reference_date != session_data.session_date:
        raise ValueError(
            "Historical session data date does not match requested as_of session"
        )

    latest_daily_bar = _latest_bar(list(session_data.daily_bars))
    if latest_daily_bar is None:
        raise ValueError(
            f"No Alpaca daily bars available for {session_data.symbol} on {reference_date.isoformat()}"
        )

    as_of_text = _render_utc(reference_timestamp)
    latest_intraday_bar = _latest_timestamped_bar(
        session_data.intraday_bars,
        as_of_text=as_of_text,
    )
    spot_price = float(
        latest_daily_bar.close
        if latest_intraday_bar is None
        else latest_intraday_bar.close
    )

    return build_market_slice_from_loaded_data(
        symbol=session_data.symbol,
        underlying_type=session_data.underlying_type,
        spot_price=spot_price,
        daily_bars=list(session_data.daily_bars),
        intraday_bars=_bars_through_timestamp(
            session_data.intraday_bars,
            as_of_text=as_of_text,
        ),
        call_contracts_by_expiration=session_data.call_contracts_by_expiration,
        put_contracts_by_expiration=session_data.put_contracts_by_expiration,
        call_snapshots_by_expiration=_snapshots_by_expiration_from_bars(
            contracts_by_expiration=session_data.call_contracts_by_expiration,
            option_bars_by_symbol=session_data.call_option_bars_by_symbol,
            as_of_text=as_of_text,
            use_latest_bar=use_latest_option_bar_snapshot,
        ),
        put_snapshots_by_expiration=_snapshots_by_expiration_from_bars(
            contracts_by_expiration=session_data.put_contracts_by_expiration,
            option_bars_by_symbol=session_data.put_option_bars_by_symbol,
            as_of_text=as_of_text,
            use_latest_bar=use_latest_option_bar_snapshot,
        ),
        greeks_provider=greeks_provider,
        greeks_as_of=reference_timestamp,
        greeks_source_mode=(
            "auto"
            if str(getattr(symbol_args, "greeks_source", "auto")).lower() == "alpaca"
            else str(getattr(symbol_args, "greeks_source", "auto") or "auto")
        ),
    )


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
    session_data = build_historical_symbol_session_data_from_alpaca(
        symbol=symbol,
        symbol_args=symbol_args,
        client=client,
        session_date=reference_timestamp.astimezone(NEW_YORK).date(),
        option_bar_timeframe=option_bar_timeframe,
    )
    return build_historical_symbol_market_slice_from_session_data(
        session_data=session_data,
        symbol_args=symbol_args,
        greeks_provider=greeks_provider,
        as_of=reference_timestamp,
    )


__all__ = [
    "ALPACA_OPTIONS_HISTORY_START",
    "HistoricalSymbolSessionData",
    "build_historical_symbol_market_slice_from_alpaca",
    "build_historical_symbol_market_slice_from_session_data",
    "build_historical_symbol_session_data_from_alpaca",
]
