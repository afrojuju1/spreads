from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OptionContract:
    symbol: str
    expiration_date: str
    strike_price: float
    open_interest: int
    close_price: float | None


@dataclass(frozen=True)
class OptionSnapshot:
    symbol: str
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    midpoint: float
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    implied_volatility: float | None
    last_trade_price: float | None
    daily_volume: int | None = None
    greeks_source: str | None = None


@dataclass(frozen=True)
class DailyBar:
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class IntradayBar:
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class LiveOptionQuote:
    symbol: str
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    timestamp: str | None

    @property
    def midpoint(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(frozen=True)
class ExpectedMoveEstimate:
    expiration_date: str
    amount: float
    percent_of_spot: float
    reference_strike: float
    method: str = "atm_straddle_midpoint"


@dataclass(frozen=True)
class UnderlyingSetupContext:
    strategy: str
    status: str
    score: float
    reasons: tuple[str, ...]
    daily_score: float | None
    intraday_score: float | None
    spot_vs_sma20_pct: float | None
    sma20_vs_sma50_pct: float | None
    return_5d_pct: float | None
    distance_to_20d_extreme_pct: float | None
    latest_close: float | None
    sma20: float | None
    sma50: float | None
    source_window_days: int
    spot_vs_vwap_pct: float | None = None
    intraday_return_pct: float | None = None
    distance_to_session_extreme_pct: float | None = None
    opening_range_break_pct: float | None = None
    vwap: float | None = None
    opening_range_high: float | None = None
    opening_range_low: float | None = None
    source_window_minutes: int | None = None


@dataclass(frozen=True)
class SpreadCandidate:
    underlying_symbol: str
    strategy: str
    profile: str
    expiration_date: str
    days_to_expiration: int
    underlying_price: float
    short_symbol: str
    long_symbol: str
    short_strike: float
    long_strike: float
    width: float
    short_delta: float | None
    long_delta: float | None
    greeks_source: str
    short_midpoint: float
    long_midpoint: float
    short_bid: float
    short_ask: float
    long_bid: float
    long_ask: float
    midpoint_credit: float
    natural_credit: float
    max_profit: float
    max_loss: float
    return_on_risk: float
    breakeven: float
    breakeven_cushion_pct: float
    short_otm_pct: float
    short_open_interest: int
    long_open_interest: int
    short_relative_spread: float
    long_relative_spread: float
    fill_ratio: float
    min_quote_size: int
    order_payload: dict[str, Any]
    expected_move: float | None = None
    expected_move_pct: float | None = None
    expected_move_source_strike: float | None = None
    debit_width_ratio: float | None = None
    modeled_move_vs_implied_move: float | None = None
    modeled_move_vs_break_even_move: float | None = None
    short_vs_expected_move: float | None = None
    breakeven_vs_expected_move: float | None = None
    quality_score: float = 0.0
    calendar_status: str = "clean"
    calendar_reasons: tuple[str, ...] = ()
    calendar_confidence: str = "unknown"
    calendar_sources: tuple[str, ...] = ()
    calendar_last_updated: str | None = None
    calendar_days_to_nearest_event: int | None = None
    macro_regime: str | None = None
    earnings_phase: str = "clean"
    earnings_event_date: str | None = None
    earnings_session_timing: str = "unknown"
    earnings_cohort_key: str | None = None
    earnings_days_to_event: int | None = None
    earnings_days_since_event: int | None = None
    earnings_timing_confidence: str = "unknown"
    earnings_horizon_crosses_report: bool = False
    earnings_primary_source: str | None = None
    earnings_supporting_sources: tuple[str, ...] = ()
    earnings_consensus_status: str = "missing"
    setup_status: str = "unknown"
    setup_score: float | None = None
    setup_reasons: tuple[str, ...] = ()
    setup_daily_score: float | None = None
    setup_intraday_score: float | None = None
    setup_intraday_minutes: int | None = None
    setup_has_intraday_context: bool = False
    setup_spot_vs_vwap_pct: float | None = None
    setup_intraday_return_pct: float | None = None
    setup_distance_to_session_extreme_pct: float | None = None
    setup_opening_range_break_pct: float | None = None
    setup_latest_close: float | None = None
    setup_vwap: float | None = None
    setup_opening_range_high: float | None = None
    setup_opening_range_low: float | None = None
    data_status: str = "clean"
    data_reasons: tuple[str, ...] = ()
    selection_notes: tuple[str, ...] = ()
    short_bid_size: int = 0
    short_ask_size: int = 0
    long_bid_size: int = 0
    long_ask_size: int = 0
    short_implied_volatility: float | None = None
    long_implied_volatility: float | None = None
    short_volume: int | None = None
    long_volume: int | None = None
    secondary_short_symbol: str | None = None
    secondary_long_symbol: str | None = None
    secondary_short_strike: float | None = None
    secondary_long_strike: float | None = None
    lower_breakeven: float | None = None
    upper_breakeven: float | None = None
    side_balance_score: float | None = None
    wing_symmetry_ratio: float | None = None


@dataclass(frozen=True)
class SymbolScanResult:
    symbol: str
    underlying_type: str
    spot_price: float
    args: argparse.Namespace
    setup: UnderlyingSetupContext | None
    candidates: list[SpreadCandidate]
    run_id: str
    quoted_contract_count: int = 0
    alpaca_delta_contract_count: int = 0
    delta_contract_count: int = 0
    local_delta_contract_count: int = 0


@dataclass(frozen=True)
class SymbolMarketSlice:
    symbol: str
    underlying_type: str
    spot_price: float
    daily_bars: tuple[DailyBar, ...]
    intraday_bars: tuple[IntradayBar, ...]
    call_contracts_by_expiration: dict[str, list[OptionContract]]
    put_contracts_by_expiration: dict[str, list[OptionContract]]
    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]]
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]]
    expected_moves_by_expiration: dict[str, ExpectedMoveEstimate]


@dataclass(frozen=True)
class UniverseScanFailure:
    symbol: str
    error: str


__all__ = [
    "DailyBar",
    "ExpectedMoveEstimate",
    "IntradayBar",
    "LiveOptionQuote",
    "OptionContract",
    "OptionSnapshot",
    "SpreadCandidate",
    "SymbolMarketSlice",
    "SymbolScanResult",
    "UnderlyingSetupContext",
    "UniverseScanFailure",
]
