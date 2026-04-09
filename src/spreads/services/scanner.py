#!/usr/bin/env python3
"""Scan Alpaca option chains for credit spread candidates.

Usage:
    uv run spreads-scan --symbol SPY

Required environment variables:
    APCA_API_KEY_ID
    APCA_API_SECRET_KEY

Notes:
    - Uses Alpaca's Trading API for option contract metadata.
    - Uses Alpaca's Market Data API for underlying price and option chain snapshots.
    - Supports call credit and put credit spreads with shared ranking/replay logic.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import time as time_module
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import msgpack
import websocket

from spreads.integrations.calendar_events import build_calendar_event_resolver, classify_underlying_type
from spreads.integrations.calendar_events.models import CalendarPolicyDecision
from spreads.integrations.calendar_events.policy import apply_credit_spread_policy
from spreads.integrations.greeks import build_local_greeks_provider
from spreads.runtime.config import default_database_url
from spreads.storage.factory import build_history_store
from spreads.storage.run_history_repository import RunHistoryRepository


DEFAULT_DATA_BASE_URL = "https://data.alpaca.markets"
DEFAULT_TRADING_BASE_URL = "https://api.alpaca.markets"
NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_BOARD_UNIVERSE = "etf_core"
ZERO_DTE_CORE_SYMBOLS = ("SPY", "QQQ", "IWM")
ZERO_DTE_ALLOWED_SYMBOLS = ("SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLI", "XLV", "GLD", "TLT")
UNIVERSE_PRESETS: dict[str, tuple[str, ...]] = {
    "0dte_core": ZERO_DTE_CORE_SYMBOLS,
    "explore_10": ZERO_DTE_ALLOWED_SYMBOLS,
    "etf_core": ("SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLE", "SMH"),
    "liquid_stocks": ("AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "AMD", "TSLA"),
    "liquid_mixed": (
        "SPY",
        "QQQ",
        "IWM",
        "SMH",
        "XLK",
        "XLF",
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "AMD",
    ),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find credit spread candidates for one symbol or a ranked universe board using Alpaca."
    )
    parser.add_argument("--symbol", help="Scan a single underlying.")
    parser.add_argument(
        "--symbols",
        help="Comma-separated list of underlyings to scan as a universe board.",
    )
    parser.add_argument(
        "--symbols-file",
        help="Optional file containing one symbol per line for universe scanning.",
    )
    parser.add_argument(
        "--universe",
        choices=tuple(sorted(UNIVERSE_PRESETS)),
        help="Use a curated symbol preset for multi-symbol scanning.",
    )
    parser.add_argument(
        "--strategy",
        default="call_credit",
        choices=("call_credit", "put_credit", "combined"),
        help="Credit spread strategy. Use combined to evaluate both call and put credit spreads. Default: call_credit",
    )
    parser.add_argument(
        "--profile",
        default="core",
        choices=("0dte", "micro", "weekly", "swing", "core"),
        help="Scanner profile preset. Default: core",
    )
    parser.add_argument(
        "--min-dte",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--short-delta-min",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--short-delta-max",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--short-delta-target",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-width",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--max-width",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-credit",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-open-interest",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--max-relative-spread",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-return-on-risk",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--feed",
        default="opra",
        choices=("opra", "indicative"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--stock-feed",
        default="sip",
        choices=("sip", "iex", "delayed_sip", "boats", "overnight"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of candidates to print. Default: 10",
    )
    parser.add_argument(
        "--per-symbol-top",
        type=int,
        default=1,
        help="Maximum number of ranked spreads to keep per symbol in universe mode. Default: 1",
    )
    parser.add_argument(
        "--trading-base-url",
        default=os.environ.get("ALPACA_TRADING_BASE_URL"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--data-base-url",
        default=os.environ.get("ALPACA_DATA_BASE_URL", DEFAULT_DATA_BASE_URL),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--output",
        help="Output file path. Default: strategy-specific outputs directory",
    )
    parser.add_argument(
        "--output-format",
        default="csv",
        choices=("csv", "json"),
        help="Output file format. Default: csv",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of a human-readable summary.",
    )
    parser.add_argument(
        "--show-order-json",
        action="store_true",
        help="Print a sample Alpaca multi-leg order payload for each result.",
    )
    parser.add_argument(
        "--greeks-source",
        default="auto",
        choices=("alpaca", "local", "auto"),
        help="Greeks source mode. Default: auto",
    )
    parser.add_argument(
        "--stream-live-quotes",
        action="store_true",
        help="After printing results, stream fresh Alpaca option quotes for the displayed legs.",
    )
    parser.add_argument(
        "--calendar-policy",
        default="strict",
        choices=("strict", "warn", "off"),
        help="Calendar event mode. Default: strict",
    )
    parser.add_argument(
        "--refresh-calendar-events",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--expand-duplicates",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--setup-filter",
        default="on",
        choices=("on", "off"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--data-policy",
        default="strict",
        choices=("strict", "warn", "off"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-fill-ratio",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-short-vs-expected-move-ratio",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-breakeven-vs-expected-move-ratio",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--history-db",
        default=default_database_url(),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--session-label",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--replay-latest",
        action="store_true",
        help="Replay the most recent stored run for the selected symbol instead of scanning live.",
    )
    parser.add_argument(
        "--replay-run-id",
        help="Replay a specific stored run id instead of scanning live.",
    )
    parser.add_argument(
        "--replay-profit-target",
        type=float,
        default=0.50,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--replay-stop-multiple",
        type=float,
        default=2.0,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--stream-seconds",
        type=float,
        default=8.0,
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def load_local_env(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def load_symbols_file(path: str) -> list[str]:
    symbols: list[str] = []
    for raw_line in Path(path).read_text().splitlines():
        line = raw_line.strip().upper()
        if not line or line.startswith("#"):
            continue
        symbols.append(line)
    return symbols


def resolve_symbols(args: argparse.Namespace) -> tuple[list[str], str]:
    symbols: list[str] = []
    default_label = "0dte_core" if args.profile == "0dte" else DEFAULT_BOARD_UNIVERSE
    label = args.symbol.upper() if args.symbol else default_label

    if args.universe:
        symbols.extend(UNIVERSE_PRESETS[args.universe])
        label = args.universe
    if args.symbols:
        symbols.extend([token.strip().upper() for token in args.symbols.split(",") if token.strip()])
        if args.symbols.strip():
            label = "custom_symbols"
    if args.symbols_file:
        symbols.extend(load_symbols_file(args.symbols_file))
        label = Path(args.symbols_file).stem.lower()

    if args.symbol:
        symbols.append(args.symbol.upper())
        label = args.symbol.upper()

    if not symbols:
        return list(UNIVERSE_PRESETS[default_label]), default_label

    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(symbol)
    return deduped, label


def concrete_strategies(strategy: str) -> tuple[str, ...]:
    if strategy == "combined":
        return ("call_credit", "put_credit")
    return (strategy,)


def strategy_display_label(strategy: str) -> str:
    return {
        "call_credit": "Call",
        "put_credit": "Put",
        "combined": "Combined",
    }.get(strategy, strategy)


def zero_dte_session_bucket(now: datetime | None = None) -> str:
    current = datetime.now(NEW_YORK) if now is None else now.astimezone(NEW_YORK)
    current_time = current.time()
    if current_time < time(9, 30) or current_time >= time(16, 0):
        return "off_hours"
    if current_time < time(10, 30):
        return "open"
    if current_time < time(13, 30):
        return "midday"
    return "late"


def format_session_bucket(bucket: str) -> str:
    return bucket.replace("_", "-")


def zero_dte_delta_target(session_bucket: str) -> float:
    return {
        "open": 0.08,
        "midday": 0.10,
        "late": 0.12,
        "off_hours": 0.10,
    }[session_bucket]


def env_or_die(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    expected = " or ".join(names)
    raise SystemExit(f"Missing required environment variable: {expected}")


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def pick(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def format_stream_timestamp(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, msgpack.Timestamp):
        dt = datetime.fromtimestamp(value.to_unix(), tz=UTC)
        return dt.isoformat(timespec="seconds").replace("+00:00", "Z")
    return str(value)


def infer_underlying_key(underlying_type: str) -> str:
    return "etf_index_proxy" if underlying_type == "etf_index_proxy" else "single_name_equity"


def resolve_profile_value(override: Any, preset: Any) -> Any:
    return preset if override is None else override


def apply_profile_defaults(args: argparse.Namespace, underlying_type: str) -> None:
    profile = PROFILE_CONFIGS[args.profile]
    underlying_key = infer_underlying_key(underlying_type)

    args.min_dte = resolve_profile_value(args.min_dte, profile.min_dte)
    args.max_dte = resolve_profile_value(args.max_dte, profile.max_dte)
    args.short_delta_min = resolve_profile_value(args.short_delta_min, profile.short_delta_min)
    args.short_delta_max = resolve_profile_value(args.short_delta_max, profile.short_delta_max)
    args.short_delta_target = resolve_profile_value(args.short_delta_target, profile.short_delta_target)
    args.min_width = resolve_profile_value(args.min_width, profile.min_width)
    args.max_width = resolve_profile_value(args.max_width, profile.max_width_by_underlying[underlying_key])
    args.min_credit = resolve_profile_value(args.min_credit, profile.min_credit)
    args.min_open_interest = resolve_profile_value(
        args.min_open_interest,
        profile.min_open_interest_by_underlying[underlying_key],
    )
    args.max_relative_spread = resolve_profile_value(
        args.max_relative_spread,
        profile.max_relative_spread_by_underlying[underlying_key],
    )
    args.min_return_on_risk = resolve_profile_value(args.min_return_on_risk, profile.min_return_on_risk)
    args.min_fill_ratio = resolve_profile_value(args.min_fill_ratio, profile.min_fill_ratio)
    args.min_short_vs_expected_move_ratio = resolve_profile_value(
        args.min_short_vs_expected_move_ratio,
        profile.min_short_vs_expected_move_ratio,
    )
    args.min_breakeven_vs_expected_move_ratio = resolve_profile_value(
        args.min_breakeven_vs_expected_move_ratio,
        profile.min_breakeven_vs_expected_move_ratio,
    )


def validate_profile_scope(symbol: str, args: argparse.Namespace, underlying_type: str) -> None:
    if args.profile != "0dte":
        return
    if underlying_type != "etf_index_proxy":
        raise SystemExit("0dte profile is currently limited to ETF/index proxies")
    if symbol.upper() not in ZERO_DTE_ALLOWED_SYMBOLS:
        allowed = ", ".join(ZERO_DTE_ALLOWED_SYMBOLS)
        raise SystemExit(f"0dte profile is currently limited to: {allowed}")


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
class ProfileConfig:
    name: str
    min_dte: int
    max_dte: int
    short_delta_min: float
    short_delta_max: float
    short_delta_target: float
    min_width: float
    max_width_by_underlying: dict[str, float]
    min_credit: float
    min_open_interest_by_underlying: dict[str, int]
    max_relative_spread_by_underlying: dict[str, float]
    min_return_on_risk: float
    min_fill_ratio: float
    min_short_vs_expected_move_ratio: float
    min_breakeven_vs_expected_move_ratio: float


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
    setup_status: str = "unknown"
    setup_score: float | None = None
    setup_reasons: tuple[str, ...] = ()
    data_status: str = "clean"
    data_reasons: tuple[str, ...] = ()
    board_notes: tuple[str, ...] = ()


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
class UniverseScanFailure:
    symbol: str
    error: str


def build_setup_summaries(results: list[SymbolScanResult]) -> tuple[str, ...]:
    summaries: list[str] = []
    for result in results:
        if result.setup is None:
            continue
        summaries.append(
            f"{result.args.strategy} {result.setup.status} ({result.setup.score:.1f})"
        )
    return tuple(summaries)


def count_snapshot_delta_coverage(snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]]) -> tuple[int, int]:
    quoted_contracts = 0
    contracts_with_delta = 0
    for snapshot_map in snapshots_by_expiration.values():
        for snapshot in snapshot_map.values():
            quoted_contracts += 1
            if snapshot.delta is not None:
                contracts_with_delta += 1
    return quoted_contracts, contracts_with_delta


def count_local_greeks_coverage(snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]]) -> int:
    local_contracts = 0
    for snapshot_map in snapshots_by_expiration.values():
        for snapshot in snapshot_map.values():
            if snapshot.greeks_source == "local_bsm":
                local_contracts += 1
    return local_contracts


PROFILE_CONFIGS: dict[str, ProfileConfig] = {
    "0dte": ProfileConfig(
        name="0dte",
        min_dte=0,
        max_dte=0,
        short_delta_min=0.03,
        short_delta_max=0.18,
        short_delta_target=0.10,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 2.0, "single_name_equity": 2.0},
        min_credit=0.05,
        min_open_interest_by_underlying={"etf_index_proxy": 750, "single_name_equity": 750},
        max_relative_spread_by_underlying={"etf_index_proxy": 0.12, "single_name_equity": 0.12},
        min_return_on_risk=0.05,
        min_fill_ratio=0.72,
        min_short_vs_expected_move_ratio=0.08,
        min_breakeven_vs_expected_move_ratio=0.03,
    ),
    "micro": ProfileConfig(
        name="micro",
        min_dte=1,
        max_dte=3,
        short_delta_min=0.05,
        short_delta_max=0.12,
        short_delta_target=0.08,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 2.0, "single_name_equity": 2.0},
        min_credit=0.10,
        min_open_interest_by_underlying={"etf_index_proxy": 1500, "single_name_equity": 1500},
        max_relative_spread_by_underlying={"etf_index_proxy": 0.10, "single_name_equity": 0.10},
        min_return_on_risk=0.08,
        min_fill_ratio=0.75,
        min_short_vs_expected_move_ratio=0.05,
        min_breakeven_vs_expected_move_ratio=0.00,
    ),
    "weekly": ProfileConfig(
        name="weekly",
        min_dte=4,
        max_dte=10,
        short_delta_min=0.08,
        short_delta_max=0.16,
        short_delta_target=0.12,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 3.0, "single_name_equity": 5.0},
        min_credit=0.18,
        min_open_interest_by_underlying={"etf_index_proxy": 500, "single_name_equity": 400},
        max_relative_spread_by_underlying={"etf_index_proxy": 0.12, "single_name_equity": 0.15},
        min_return_on_risk=0.10,
        min_fill_ratio=0.72,
        min_short_vs_expected_move_ratio=-0.05,
        min_breakeven_vs_expected_move_ratio=-0.02,
    ),
    "swing": ProfileConfig(
        name="swing",
        min_dte=11,
        max_dte=21,
        short_delta_min=0.12,
        short_delta_max=0.20,
        short_delta_target=0.16,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 5.0, "single_name_equity": 10.0},
        min_credit=0.25,
        min_open_interest_by_underlying={"etf_index_proxy": 500, "single_name_equity": 250},
        max_relative_spread_by_underlying={"etf_index_proxy": 0.18, "single_name_equity": 0.18},
        min_return_on_risk=0.10,
        min_fill_ratio=0.70,
        min_short_vs_expected_move_ratio=-0.08,
        min_breakeven_vs_expected_move_ratio=-0.04,
    ),
    "core": ProfileConfig(
        name="core",
        min_dte=22,
        max_dte=35,
        short_delta_min=0.15,
        short_delta_max=0.22,
        short_delta_target=0.18,
        min_width=2.0,
        max_width_by_underlying={"etf_index_proxy": 10.0, "single_name_equity": 10.0},
        min_credit=0.35,
        min_open_interest_by_underlying={"etf_index_proxy": 300, "single_name_equity": 200},
        max_relative_spread_by_underlying={"etf_index_proxy": 0.20, "single_name_equity": 0.20},
        min_return_on_risk=0.12,
        min_fill_ratio=0.68,
        min_short_vs_expected_move_ratio=-0.10,
        min_breakeven_vs_expected_move_ratio=-0.05,
    ),
}


def effective_min_credit(width: float, args: argparse.Namespace) -> float:
    threshold = args.min_credit
    if args.profile != "0dte":
        return threshold
    session_bucket = zero_dte_session_bucket()
    if session_bucket != "late":
        return threshold
    if width <= 1.0:
        return max(threshold, 0.08)
    return max(threshold, 0.15)


class AlpacaClient:
    def __init__(
        self,
        *,
        key_id: str,
        secret_key: str,
        trading_base_url: str,
        data_base_url: str,
        request_timeout_seconds: float = 30.0,
    ) -> None:
        self.trading_base_url = trading_base_url.rstrip("/")
        self.data_base_url = data_base_url.rstrip("/")
        self.request_timeout_seconds = float(request_timeout_seconds)
        self.headers = {
            "APCA-API-KEY-ID": key_id,
            "APCA-API-SECRET-KEY": secret_key,
            "Accept": "application/json",
            "User-Agent": "call-credit-spread-scanner/1.0",
        }

    def request_json(
        self,
        method: str,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: Any | None = None,
    ) -> Any:
        query = ""
        if params:
            filtered = {k: v for k, v in params.items() if v not in (None, "")}
            query = "?" + urllib.parse.urlencode(filtered)
        url = f"{base_url}{path}{query}"
        request_headers = dict(self.headers)
        request_data = None
        if body is not None:
            request_headers["Content-Type"] = "application/json"
            request_data = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            url,
            headers=request_headers,
            data=request_data,
            method=method.upper(),
        )
        try:
            with urllib.request.urlopen(request, timeout=self.request_timeout_seconds) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Alpaca request failed: {exc.code} {exc.reason} for {url}\n{body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Failed to reach Alpaca for {url}: {exc.reason}") from exc

    def get_json(self, base_url: str, path: str, params: dict[str, Any] | None = None) -> Any:
        return self.request_json("GET", base_url, path, params=params)

    def post_json(
        self,
        base_url: str,
        path: str,
        body: Any,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return self.request_json("POST", base_url, path, params=params, body=body)

    def submit_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.post_json(self.trading_base_url, "/v2/orders", payload)
        if not isinstance(response, dict):
            raise RuntimeError("Unexpected Alpaca order submission response shape")
        return response

    def get_order(self, order_id: str, *, nested: bool = False) -> dict[str, Any]:
        response = self.get_json(
            self.trading_base_url,
            f"/v2/orders/{order_id}",
            {"nested": "true" if nested else None},
        )
        if not isinstance(response, dict):
            raise RuntimeError("Unexpected Alpaca order response shape")
        return response

    def get_account(self) -> dict[str, Any]:
        response = self.get_json(self.trading_base_url, "/v2/account")
        if not isinstance(response, dict):
            raise RuntimeError("Unexpected Alpaca account response shape")
        return response

    def list_positions(self) -> list[dict[str, Any]]:
        response = self.get_json(self.trading_base_url, "/v2/positions")
        if not isinstance(response, list):
            raise RuntimeError("Unexpected Alpaca positions response shape")
        return [dict(item) for item in response if isinstance(item, dict)]

    def get_account_portfolio_history(
        self,
        *,
        period: str | None = None,
        timeframe: str | None = None,
        intraday_reporting: str | None = None,
    ) -> dict[str, Any]:
        response = self.get_json(
            self.trading_base_url,
            "/v2/account/portfolio/history",
            {
                "period": period,
                "timeframe": timeframe,
                "intraday_reporting": intraday_reporting,
            },
        )
        if not isinstance(response, dict):
            raise RuntimeError("Unexpected Alpaca portfolio history response shape")
        return response

    def list_account_activities(
        self,
        *,
        activity_type: str = "FILL",
        date: str | None = None,
        page_size: int | None = None,
        direction: str | None = None,
    ) -> list[dict[str, Any]]:
        response = self.get_json(
            self.trading_base_url,
            f"/v2/account/activities/{activity_type}",
            {
                "date": date,
                "page_size": page_size,
                "direction": direction,
            },
        )
        if not isinstance(response, list):
            raise RuntimeError("Unexpected Alpaca account activity response shape")
        return [dict(item) for item in response if isinstance(item, dict)]

    def list_optionable_underlyings(self) -> list[dict[str, Any]]:
        payload = self.get_json(
            self.trading_base_url,
            "/v2/assets",
            {
                "status": "active",
                "asset_class": "us_equity",
                "attributes": "has_options",
            },
        )
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected Alpaca assets response shape")

        assets: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").upper()
            status = str(item.get("status") or "").lower()
            if not symbol or status != "active":
                continue
            if item.get("tradable") is False:
                continue
            assets.append(item)
        return assets

    def get_underlying_price(self, symbol: str, stock_feed: str) -> float:
        quote_payload = self.get_json(
            self.data_base_url,
            "/v2/stocks/quotes/latest",
            {"symbols": symbol, "feed": stock_feed},
        )
        quote = self._extract_symbol_payload(quote_payload, symbol, plural_key="quotes", singular_key="quote")
        bid = parse_float(pick(quote, "bp", "bid_price"))
        ask = parse_float(pick(quote, "ap", "ask_price"))
        if bid and ask and bid > 0 and ask > 0:
            return (bid + ask) / 2.0

        trade_payload = self.get_json(
            self.data_base_url,
            "/v2/stocks/trades/latest",
            {"symbols": symbol, "feed": stock_feed},
        )
        trade = self._extract_symbol_payload(trade_payload, symbol, plural_key="trades", singular_key="trade")
        price = parse_float(pick(trade, "p", "price"))
        if price and price > 0:
            return price
        raise RuntimeError(f"Could not determine current price for {symbol}")

    def get_latest_option_quotes(
        self,
        symbols: list[str],
        *,
        feed: str,
    ) -> dict[str, LiveOptionQuote]:
        if not symbols:
            return {}

        payload = self.get_json(
            self.data_base_url,
            "/v1beta1/options/quotes/latest",
            {
                "symbols": ",".join(symbols),
                "feed": feed,
            },
        )
        raw_quotes = payload.get("quotes", {})
        if not isinstance(raw_quotes, dict):
            return {}

        latest_quotes: dict[str, LiveOptionQuote] = {}
        for symbol, quote in raw_quotes.items():
            if not isinstance(quote, dict):
                continue
            bid = parse_float(pick(quote, "bp", "bid_price"))
            ask = parse_float(pick(quote, "ap", "ask_price"))
            if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
                continue
            latest_quotes[str(symbol)] = LiveOptionQuote(
                symbol=str(symbol),
                bid=bid,
                ask=ask,
                bid_size=parse_int(pick(quote, "bs", "bid_size")) or 0,
                ask_size=parse_int(pick(quote, "as", "ask_size")) or 0,
                timestamp=format_stream_timestamp(pick(quote, "t", "timestamp")),
            )
        return latest_quotes

    def get_daily_bars(
        self,
        symbol: str,
        *,
        start: str,
        end: str,
        stock_feed: str,
    ) -> list[DailyBar]:
        payload = self.get_json(
            self.data_base_url,
            "/v2/stocks/bars",
            {
                "symbols": symbol,
                "timeframe": "1Day",
                "start": start,
                "end": end,
                "adjustment": "raw",
                "feed": stock_feed,
                "limit": 1000,
            },
        )
        bars_payload = payload.get("bars", {})
        bars: list[DailyBar] = []
        for item in bars_payload.get(symbol, []):
            open_price = parse_float(pick(item, "o", "open"))
            high_price = parse_float(pick(item, "h", "high"))
            low_price = parse_float(pick(item, "l", "low"))
            close_price = parse_float(pick(item, "c", "close"))
            volume = parse_int(pick(item, "v", "volume"))
            timestamp = pick(item, "t", "timestamp")
            if None in (open_price, high_price, low_price, close_price, volume) or not timestamp:
                continue
            bars.append(
                DailyBar(
                    timestamp=str(timestamp),
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                )
            )
        return bars

    def get_intraday_bars(
        self,
        symbol: str,
        *,
        start: str,
        end: str,
        stock_feed: str,
        timeframe: str = "1Min",
    ) -> list[IntradayBar]:
        payload = self.get_json(
            self.data_base_url,
            "/v2/stocks/bars",
            {
                "symbols": symbol,
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "adjustment": "raw",
                "feed": stock_feed,
                "limit": 1000,
            },
        )
        bars_payload = payload.get("bars", {})
        bars: list[IntradayBar] = []
        for item in bars_payload.get(symbol, []):
            open_price = parse_float(pick(item, "o", "open"))
            high_price = parse_float(pick(item, "h", "high"))
            low_price = parse_float(pick(item, "l", "low"))
            close_price = parse_float(pick(item, "c", "close"))
            volume = parse_int(pick(item, "v", "volume"))
            timestamp = pick(item, "t", "timestamp")
            if None in (open_price, high_price, low_price, close_price, volume) or not timestamp:
                continue
            bars.append(
                IntradayBar(
                    timestamp=str(timestamp),
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                )
            )
        return bars

    def list_option_contracts(
        self,
        symbol: str,
        min_expiration: str,
        max_expiration: str,
        *,
        option_type: str = "call",
    ) -> list[OptionContract]:
        contracts: list[OptionContract] = []
        page_token: str | None = None
        while True:
            payload = self.get_json(
                self.trading_base_url,
                "/v2/options/contracts",
                {
                    "underlying_symbols": symbol,
                    "type": option_type,
                    "status": "active",
                    "expiration_date_gte": min_expiration,
                    "expiration_date_lte": max_expiration,
                    "limit": 1000,
                    "page_token": page_token,
                },
            )
            for item in payload.get("option_contracts", []):
                strike_price = parse_float(item.get("strike_price"))
                open_interest = parse_int(item.get("open_interest"))
                if not strike_price or open_interest is None:
                    continue
                contracts.append(
                    OptionContract(
                        symbol=item["symbol"],
                        expiration_date=item["expiration_date"],
                        strike_price=strike_price,
                        open_interest=open_interest,
                        close_price=parse_float(item.get("close_price")),
                    )
                )
            page_token = payload.get("next_page_token") or payload.get("page_token")
            if not page_token:
                break
        return contracts

    def get_option_chain_snapshots(
        self,
        symbol: str,
        expiration_date: str,
        option_type: str,
        feed: str,
    ) -> dict[str, OptionSnapshot]:
        snapshots: dict[str, OptionSnapshot] = {}
        page_token: str | None = None
        while True:
            payload = self.get_json(
                self.data_base_url,
                f"/v1beta1/options/snapshots/{symbol}",
                {
                    "feed": feed,
                    "type": option_type,
                    "expiration_date": expiration_date,
                    "limit": 1000,
                    "page_token": page_token,
                },
            )
            raw_snapshots = payload.get("snapshots", {})
            if isinstance(raw_snapshots, dict):
                for contract_symbol, snapshot in raw_snapshots.items():
                    parsed = self._parse_option_snapshot(contract_symbol, snapshot)
                    if parsed:
                        snapshots[contract_symbol] = parsed
            page_token = payload.get("next_page_token") or payload.get("page_token")
            if not page_token:
                break
        return snapshots

    def get_option_bars(
        self,
        symbols: list[str],
        *,
        start: str,
        end: str,
    ) -> dict[str, list[DailyBar]]:
        if not symbols:
            return {}

        bars_by_symbol: dict[str, list[DailyBar]] = {symbol: [] for symbol in symbols}
        page_token: str | None = None
        while True:
            payload = self.get_json(
                self.data_base_url,
                "/v1beta1/options/bars",
                {
                    "symbols": ",".join(symbols),
                    "timeframe": "1Day",
                    "start": start,
                    "end": end,
                    "limit": 1000,
                    "page_token": page_token,
                },
            )
            raw_bars = payload.get("bars", {})
            if isinstance(raw_bars, dict):
                for symbol, bars in raw_bars.items():
                    for item in bars:
                        open_price = parse_float(pick(item, "o", "open"))
                        high_price = parse_float(pick(item, "h", "high"))
                        low_price = parse_float(pick(item, "l", "low"))
                        close_price = parse_float(pick(item, "c", "close"))
                        volume = parse_int(pick(item, "v", "volume")) or 0
                        timestamp = pick(item, "t", "timestamp")
                        if None in (open_price, high_price, low_price, close_price) or not timestamp:
                            continue
                        bars_by_symbol.setdefault(symbol, []).append(
                            DailyBar(
                                timestamp=str(timestamp),
                                open=open_price,
                                high=high_price,
                                low=low_price,
                                close=close_price,
                                volume=volume,
                            )
                        )
            page_token = payload.get("next_page_token") or payload.get("page_token")
            if not page_token:
                break
        return bars_by_symbol

    @staticmethod
    def _extract_symbol_payload(
        payload: dict[str, Any],
        symbol: str,
        *,
        plural_key: str,
        singular_key: str,
    ) -> dict[str, Any]:
        if plural_key in payload and isinstance(payload[plural_key], dict):
            if symbol in payload[plural_key]:
                return payload[plural_key][symbol]
        if singular_key in payload and isinstance(payload[singular_key], dict):
            return payload[singular_key]
        raise RuntimeError(f"Unexpected Alpaca response shape while looking up {symbol}")

    @staticmethod
    def _parse_option_snapshot(symbol: str, snapshot: dict[str, Any]) -> OptionSnapshot | None:
        latest_quote = snapshot.get("latestQuote") or snapshot.get("latest_quote") or {}
        greeks = snapshot.get("greeks") or {}
        latest_trade = snapshot.get("latestTrade") or snapshot.get("latest_trade") or {}

        bid = parse_float(pick(latest_quote, "bp", "bid_price"))
        ask = parse_float(pick(latest_quote, "ap", "ask_price"))
        bid_size = parse_int(pick(latest_quote, "bs", "bid_size")) or 0
        ask_size = parse_int(pick(latest_quote, "as", "ask_size")) or 0

        if not bid or not ask or bid <= 0 or ask <= 0 or ask < bid:
            return None

        midpoint = (bid + ask) / 2.0
        if midpoint <= 0:
            return None

        delta_value = parse_float(pick(greeks, "delta", "d"))

        return OptionSnapshot(
            symbol=symbol,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            midpoint=midpoint,
            delta=delta_value,
            gamma=parse_float(pick(greeks, "gamma", "g")),
            theta=parse_float(pick(greeks, "theta", "t")),
            vega=parse_float(pick(greeks, "vega", "v")),
            implied_volatility=parse_float(
                pick(snapshot, "impliedVolatility", "implied_volatility", "iv")
            ),
            last_trade_price=parse_float(pick(latest_trade, "p", "price")),
            greeks_source="alpaca" if delta_value is not None else None,
        )


class AlpacaOptionQuoteStreamer:
    def __init__(self, *, key_id: str, secret_key: str, data_base_url: str, feed: str) -> None:
        self.key_id = key_id
        self.secret_key = secret_key
        self.feed = feed
        parsed = urllib.parse.urlparse(data_base_url)
        hostname = parsed.netloc.lower()
        if "sandbox" in hostname:
            self.url = f"wss://stream.data.sandbox.alpaca.markets/v1beta1/{feed}"
        else:
            self.url = f"wss://stream.data.alpaca.markets/v1beta1/{feed}"

    def stream_quotes(
        self,
        symbols: list[str],
        *,
        duration_seconds: float,
    ) -> dict[str, LiveOptionQuote]:
        latest_quotes: dict[str, LiveOptionQuote] = {}
        for quote in self.collect_quote_events(symbols, duration_seconds=duration_seconds):
            latest_quotes[quote.symbol] = quote
        return latest_quotes

    def collect_quote_events(
        self,
        symbols: list[str],
        *,
        duration_seconds: float,
    ) -> list[LiveOptionQuote]:
        if not symbols:
            return []

        quote_events: list[LiveOptionQuote] = []
        ws = websocket.create_connection(
            self.url,
            timeout=5,
            header=["Content-Type: application/msgpack"],
        )
        try:
            self._await_connection(ws)
            self._send(ws, {"action": "auth", "key": self.key_id, "secret": self.secret_key})
            self._await_authentication(ws)
            self._send(ws, {"action": "subscribe", "quotes": symbols})
            deadline = time_module.monotonic() + max(duration_seconds, 0.5)
            while time_module.monotonic() < deadline:
                remaining = deadline - time_module.monotonic()
                ws.settimeout(max(min(remaining, 1.0), 0.1))
                try:
                    messages = self._recv_messages(ws)
                except websocket.WebSocketTimeoutException:
                    continue
                for message in messages:
                    message_type = message.get("T")
                    if message_type == "error":
                        code = message.get("code", "unknown")
                        detail = message.get("msg", "unknown websocket error")
                        raise RuntimeError(f"Option stream error {code}: {detail}")
                    if message_type != "q":
                        continue
                    bid = parse_float(message.get("bp"))
                    ask = parse_float(message.get("ap"))
                    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
                        continue
                    quote_events.append(
                        LiveOptionQuote(
                        symbol=str(message.get("S")),
                        bid=bid,
                        ask=ask,
                        bid_size=parse_int(message.get("bs")) or 0,
                        ask_size=parse_int(message.get("as")) or 0,
                        timestamp=format_stream_timestamp(message.get("t")),
                    )
                    )
        finally:
            ws.close()
        return quote_events

    def _await_connection(self, ws: websocket.WebSocket) -> None:
        messages = self._recv_messages(ws)
        for message in messages:
            if message.get("T") == "success" and message.get("msg") == "connected":
                return
            if message.get("T") == "error":
                code = message.get("code", "unknown")
                detail = message.get("msg", "unknown websocket error")
                raise RuntimeError(f"Option stream error {code}: {detail}")
        raise RuntimeError("Option stream did not acknowledge the connection")

    def _await_authentication(self, ws: websocket.WebSocket) -> None:
        deadline = time_module.monotonic() + 5
        while time_module.monotonic() < deadline:
            messages = self._recv_messages(ws)
            for message in messages:
                message_type = message.get("T")
                if message_type == "success" and message.get("msg") == "authenticated":
                    return
                if message_type == "error":
                    code = message.get("code", "unknown")
                    detail = message.get("msg", "unknown websocket error")
                    raise RuntimeError(f"Option stream auth error {code}: {detail}")
        raise RuntimeError("Option stream authentication timed out")

    @staticmethod
    def _send(ws: websocket.WebSocket, payload: dict[str, Any]) -> None:
        ws.send(msgpack.packb(payload, use_bin_type=True), opcode=websocket.ABNF.OPCODE_BINARY)

    @staticmethod
    def _recv_messages(ws: websocket.WebSocket) -> list[dict[str, Any]]:
        payload = ws.recv()
        if isinstance(payload, bytes):
            decoded = msgpack.unpackb(payload, raw=False)
        else:
            decoded = json.loads(payload)
        if isinstance(decoded, dict):
            return [decoded]
        if isinstance(decoded, list):
            return [item for item in decoded if isinstance(item, dict)]
        return []


def days_from_today(expiration_date: str) -> int:
    return (date.fromisoformat(expiration_date) - date.today()).days


def relative_spread(snapshot: OptionSnapshot) -> float:
    return (snapshot.ask - snapshot.bid) / snapshot.midpoint


def log_scaled_score(value: int, floor: int, ceiling: int) -> float:
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 1.0
    numerator = math.log10(value) - math.log10(max(floor, 1))
    denominator = math.log10(max(ceiling, 1)) - math.log10(max(floor, 1))
    if denominator <= 0:
        return 0.0
    return clamp(numerator / denominator)


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


def average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def supportive_note(message: str) -> str:
    return f"Supportive: {message}"


def caution_note(message: str) -> str:
    return f"Caution: {message}"


def setup_status_from_score(score: float) -> str:
    if score >= 60:
        return "favorable"
    if score >= 40:
        return "neutral"
    return "unfavorable"


def dedupe_reasons(reasons: list[str]) -> tuple[str, ...]:
    deduped: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        if reason in seen:
            continue
        seen.add(reason)
        deduped.append(reason)
    return tuple(deduped)


def analyze_daily_setup(
    symbol: str,
    spot_price: float,
    bars: list[DailyBar],
    *,
    strategy: str,
) -> UnderlyingSetupContext:
    if len(bars) < 20:
        return UnderlyingSetupContext(
            strategy=strategy,
            status="unknown",
            score=0.0,
            reasons=("Not enough daily-bar history for setup analysis",),
            daily_score=0.0,
            intraday_score=None,
            spot_vs_sma20_pct=None,
            sma20_vs_sma50_pct=None,
            return_5d_pct=None,
            distance_to_20d_extreme_pct=None,
            latest_close=bars[-1].close if bars else None,
            sma20=None,
            sma50=None,
            source_window_days=len(bars),
        )

    closes = [bar.close for bar in bars]
    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    sma20 = average(closes[-20:])
    sma50 = average(closes[-50:]) if len(closes) >= 50 else None
    latest_close = closes[-1]
    return_5d_pct = None
    if len(closes) >= 6 and closes[-6] > 0:
        return_5d_pct = latest_close / closes[-6] - 1.0
    high_20 = max(highs[-20:])
    low_20 = min(lows[-20:])
    spot_vs_sma20_pct = ((spot_price - sma20) / sma20) if sma20 else None
    sma20_vs_sma50_pct = ((sma20 - sma50) / sma50) if sma20 and sma50 else None

    if strategy == "put_credit":
        distance_to_20d_extreme_pct = (spot_price - low_20) / spot_price if spot_price > 0 else None
        price_vs_sma20_score = 0.5 if spot_vs_sma20_pct is None else clamp(0.5 + (spot_vs_sma20_pct / 0.08))
        trend_score = 0.5 if sma20_vs_sma50_pct is None else clamp(0.5 + (sma20_vs_sma50_pct / 0.06))
        momentum_score = 0.5 if return_5d_pct is None else clamp(0.45 + (return_5d_pct / 0.08))
        extreme_distance_score = (
            0.5 if distance_to_20d_extreme_pct is None else clamp(distance_to_20d_extreme_pct / 0.04)
        )
    else:
        distance_to_20d_extreme_pct = (high_20 - spot_price) / spot_price if spot_price > 0 else None
        price_vs_sma20_score = 0.5 if spot_vs_sma20_pct is None else clamp(0.5 - (spot_vs_sma20_pct / 0.08))
        trend_score = 0.5 if sma20_vs_sma50_pct is None else clamp(0.5 - (sma20_vs_sma50_pct / 0.06))
        momentum_score = 0.5 if return_5d_pct is None else clamp(0.55 - (return_5d_pct / 0.08))
        extreme_distance_score = (
            0.5 if distance_to_20d_extreme_pct is None else clamp(distance_to_20d_extreme_pct / 0.04)
        )

    score = round(
        100.0
        * (
            0.35 * price_vs_sma20_score
            + 0.25 * trend_score
            + 0.20 * momentum_score
            + 0.20 * extreme_distance_score
        ),
        1,
    )

    reasons: list[str] = []
    if strategy == "put_credit":
        if spot_vs_sma20_pct is not None:
            if spot_vs_sma20_pct > 0.02:
                reasons.append(supportive_note("spot is extended above the 20-day average"))
            elif spot_vs_sma20_pct < -0.01:
                reasons.append(caution_note("spot is trading below the 20-day average"))
        if sma20_vs_sma50_pct is not None:
            if sma20_vs_sma50_pct > 0.015:
                reasons.append(supportive_note("20-day average is above the 50-day average"))
            elif sma20_vs_sma50_pct < -0.01:
                reasons.append(caution_note("20-day average is below the 50-day average"))
        if return_5d_pct is not None:
            if return_5d_pct > 0.03:
                reasons.append(supportive_note("recent 5-day momentum is strongly positive"))
            elif return_5d_pct < -0.02:
                reasons.append(caution_note("recent 5-day momentum is weak to negative"))
        if distance_to_20d_extreme_pct is not None:
            if distance_to_20d_extreme_pct < 0.01:
                reasons.append(caution_note("spot is trading near the 20-day low"))
            elif distance_to_20d_extreme_pct > 0.03:
                reasons.append(supportive_note("spot has room above the recent 20-day low"))
    else:
        if spot_vs_sma20_pct is not None:
            if spot_vs_sma20_pct > 0.02:
                reasons.append(supportive_note("spot is extended above the 20-day average"))
            elif spot_vs_sma20_pct < -0.01:
                reasons.append(caution_note("spot is trading below the 20-day average"))
        if sma20_vs_sma50_pct is not None:
            if sma20_vs_sma50_pct > 0.015:
                reasons.append(caution_note("20-day average is leading the 50-day average higher"))
            elif sma20_vs_sma50_pct < -0.01:
                reasons.append(supportive_note("20-day average is below the 50-day average"))
        if return_5d_pct is not None:
            if return_5d_pct > 0.03:
                reasons.append(caution_note("recent 5-day momentum is strongly positive"))
            elif return_5d_pct < -0.02:
                reasons.append(supportive_note("recent 5-day momentum is weak to negative"))
        if distance_to_20d_extreme_pct is not None:
            if distance_to_20d_extreme_pct < 0.01:
                reasons.append(caution_note("spot is trading near the 20-day high"))
            elif distance_to_20d_extreme_pct > 0.03:
                reasons.append(supportive_note("spot has room below the recent 20-day high"))

    status = setup_status_from_score(score)
    if not reasons:
        if strategy == "put_credit":
            reasons.append(f"{symbol} daily setup is {status} for bullish or neutral premium selling")
        else:
            reasons.append(f"{symbol} daily setup is {status} for bearish or neutral premium selling")

    return UnderlyingSetupContext(
        strategy=strategy,
        status=status,
        score=score,
        reasons=tuple(reasons),
        daily_score=score,
        intraday_score=None,
        spot_vs_sma20_pct=spot_vs_sma20_pct,
        sma20_vs_sma50_pct=sma20_vs_sma50_pct,
        return_5d_pct=return_5d_pct,
        distance_to_20d_extreme_pct=distance_to_20d_extreme_pct,
        latest_close=latest_close,
        sma20=sma20,
        sma50=sma50,
        source_window_days=len(bars),
    )


def analyze_intraday_setup(
    symbol: str,
    spot_price: float,
    bars: list[IntradayBar],
    *,
    strategy: str,
) -> UnderlyingSetupContext | None:
    if len(bars) < 5:
        return None

    open_price = bars[0].open
    if open_price <= 0:
        return None

    session_high = max(bar.high for bar in bars)
    session_low = min(bar.low for bar in bars)
    weighted_prices = [
        ((bar.high + bar.low + bar.close) / 3.0) * max(bar.volume, 1)
        for bar in bars
    ]
    total_volume = sum(max(bar.volume, 1) for bar in bars)
    vwap = None if total_volume <= 0 else sum(weighted_prices) / total_volume
    spot_vs_vwap_pct = None if vwap in (None, 0) else (spot_price - vwap) / vwap
    intraday_return_pct = (spot_price / open_price - 1.0) if open_price > 0 else None
    opening_range_window = bars[: min(30, len(bars))]
    opening_range_high = max(bar.high for bar in opening_range_window)
    opening_range_low = min(bar.low for bar in opening_range_window)
    if strategy == "put_credit":
        distance_to_session_extreme_pct = (spot_price - session_low) / spot_price if spot_price > 0 else None
        opening_range_break_pct = (spot_price - opening_range_high) / spot_price if spot_price > 0 else None
        vwap_score = 0.5 if spot_vs_vwap_pct is None else clamp(0.5 + (spot_vs_vwap_pct / 0.01))
        opening_range_score = 0.5 if opening_range_break_pct is None else clamp(0.55 + (opening_range_break_pct / 0.01))
        momentum_score = 0.5 if intraday_return_pct is None else clamp(0.5 + (intraday_return_pct / 0.015))
        extreme_score = (
            0.5 if distance_to_session_extreme_pct is None else clamp(distance_to_session_extreme_pct / 0.012)
        )
    else:
        distance_to_session_extreme_pct = (session_high - spot_price) / spot_price if spot_price > 0 else None
        opening_range_break_pct = (opening_range_low - spot_price) / spot_price if spot_price > 0 else None
        vwap_score = 0.5 if spot_vs_vwap_pct is None else clamp(0.5 - (spot_vs_vwap_pct / 0.01))
        opening_range_score = 0.5 if opening_range_break_pct is None else clamp(0.55 + (opening_range_break_pct / 0.01))
        momentum_score = 0.5 if intraday_return_pct is None else clamp(0.5 - (intraday_return_pct / 0.015))
        extreme_score = (
            0.5 if distance_to_session_extreme_pct is None else clamp(distance_to_session_extreme_pct / 0.012)
        )

    score = round(
        100.0
        * (
            0.35 * vwap_score
            + 0.25 * opening_range_score
            + 0.20 * momentum_score
            + 0.20 * extreme_score
        ),
        1,
    )
    status = setup_status_from_score(score)

    reasons: list[str] = []
    if strategy == "put_credit":
        if spot_vs_vwap_pct is not None:
            if spot_vs_vwap_pct > 0.0015:
                reasons.append(supportive_note("spot is holding above VWAP"))
            elif spot_vs_vwap_pct < -0.0015:
                reasons.append(caution_note("spot is trading below VWAP"))
        if opening_range_break_pct is not None:
            if opening_range_break_pct > 0.001:
                reasons.append(supportive_note("spot is above the opening range high"))
            elif spot_price < opening_range_low:
                reasons.append(caution_note("spot has broken below the opening range low"))
        if intraday_return_pct is not None:
            if intraday_return_pct > 0.004:
                reasons.append(supportive_note("intraday trend is positive"))
            elif intraday_return_pct < -0.004:
                reasons.append(caution_note("intraday trend is negative"))
        if distance_to_session_extreme_pct is not None:
            if distance_to_session_extreme_pct < 0.003:
                reasons.append(caution_note("spot is trading near the session low"))
            elif distance_to_session_extreme_pct > 0.008:
                reasons.append(supportive_note("spot has room above the session low"))
    else:
        if spot_vs_vwap_pct is not None:
            if spot_vs_vwap_pct < -0.0015:
                reasons.append(supportive_note("spot is holding below VWAP"))
            elif spot_vs_vwap_pct > 0.0015:
                reasons.append(caution_note("spot is trading above VWAP"))
        if opening_range_break_pct is not None:
            if opening_range_break_pct > 0.001:
                reasons.append(supportive_note("spot is below the opening range low"))
            elif spot_price > opening_range_high:
                reasons.append(caution_note("spot has broken above the opening range high"))
        if intraday_return_pct is not None:
            if intraday_return_pct < -0.004:
                reasons.append(supportive_note("intraday trend is negative"))
            elif intraday_return_pct > 0.004:
                reasons.append(caution_note("intraday trend is positive"))
        if distance_to_session_extreme_pct is not None:
            if distance_to_session_extreme_pct < 0.003:
                reasons.append(caution_note("spot is trading near the session high"))
            elif distance_to_session_extreme_pct > 0.008:
                reasons.append(supportive_note("spot has room below the session high"))

    if not reasons:
        if strategy == "put_credit":
            reasons.append(f"{symbol} intraday setup is {status} for bullish or neutral premium selling")
        else:
            reasons.append(f"{symbol} intraday setup is {status} for bearish or neutral premium selling")

    return UnderlyingSetupContext(
        strategy=strategy,
        status=status,
        score=score,
        reasons=tuple(reasons),
        daily_score=None,
        intraday_score=score,
        spot_vs_sma20_pct=None,
        sma20_vs_sma50_pct=None,
        return_5d_pct=None,
        distance_to_20d_extreme_pct=None,
        latest_close=bars[-1].close,
        sma20=None,
        sma50=None,
        source_window_days=0,
        spot_vs_vwap_pct=spot_vs_vwap_pct,
        intraday_return_pct=intraday_return_pct,
        distance_to_session_extreme_pct=distance_to_session_extreme_pct,
        opening_range_break_pct=opening_range_break_pct,
        vwap=vwap,
        opening_range_high=opening_range_high,
        opening_range_low=opening_range_low,
        source_window_minutes=len(bars),
    )


def combine_setup_contexts(
    daily_setup: UnderlyingSetupContext,
    intraday_setup: UnderlyingSetupContext | None,
    *,
    profile: str,
    strategy: str,
) -> UnderlyingSetupContext:
    if intraday_setup is None:
        return daily_setup

    intraday_weight = {
        "0dte": 0.65,
        "micro": 0.50,
        "weekly": 0.35,
        "swing": 0.20,
        "core": 0.10,
    }.get(profile, 0.10)
    if daily_setup.status == "unknown":
        intraday_weight = 1.0
    daily_weight = 1.0 - intraday_weight
    blended_score = round((daily_setup.score * daily_weight) + (intraday_setup.score * intraday_weight), 1)
    blended_status = setup_status_from_score(blended_score)

    ordered_reasons = list(intraday_setup.reasons[:4]) + list(daily_setup.reasons[:3])
    if not ordered_reasons:
        ordered_reasons.append(f"Combined setup is {blended_status} for {strategy}")

    return UnderlyingSetupContext(
        strategy=strategy,
        status=blended_status,
        score=blended_score,
        reasons=dedupe_reasons(ordered_reasons),
        daily_score=daily_setup.score,
        intraday_score=intraday_setup.score,
        spot_vs_sma20_pct=daily_setup.spot_vs_sma20_pct,
        sma20_vs_sma50_pct=daily_setup.sma20_vs_sma50_pct,
        return_5d_pct=daily_setup.return_5d_pct,
        distance_to_20d_extreme_pct=daily_setup.distance_to_20d_extreme_pct,
        latest_close=intraday_setup.latest_close or daily_setup.latest_close,
        sma20=daily_setup.sma20,
        sma50=daily_setup.sma50,
        source_window_days=daily_setup.source_window_days,
        spot_vs_vwap_pct=intraday_setup.spot_vs_vwap_pct,
        intraday_return_pct=intraday_setup.intraday_return_pct,
        distance_to_session_extreme_pct=intraday_setup.distance_to_session_extreme_pct,
        opening_range_break_pct=intraday_setup.opening_range_break_pct,
        vwap=intraday_setup.vwap,
        opening_range_high=intraday_setup.opening_range_high,
        opening_range_low=intraday_setup.opening_range_low,
        source_window_minutes=intraday_setup.source_window_minutes,
    )


def analyze_underlying_setup(
    symbol: str,
    spot_price: float,
    daily_bars: list[DailyBar],
    *,
    strategy: str,
    profile: str,
    intraday_bars: list[IntradayBar] | None = None,
) -> UnderlyingSetupContext:
    daily_setup = analyze_daily_setup(symbol, spot_price, daily_bars, strategy=strategy)
    intraday_setup = analyze_intraday_setup(symbol, spot_price, intraday_bars or [], strategy=strategy)
    return combine_setup_contexts(daily_setup, intraday_setup, profile=profile, strategy=strategy)


def attach_underlying_setup(
    candidates: list[SpreadCandidate],
    setup: UnderlyingSetupContext | None,
) -> list[SpreadCandidate]:
    if setup is None:
        return candidates
    return [
        replace(
            candidate,
            setup_status=setup.status,
            setup_score=setup.score,
            setup_reasons=setup.reasons,
        )
        for candidate in candidates
    ]


def assess_data_quality(
    candidate: SpreadCandidate,
    *,
    underlying_type: str,
    args: argparse.Namespace,
) -> tuple[str, tuple[str, ...]]:
    if args.data_policy == "off":
        return "clean", ()

    reasons: list[str] = []
    blocked = False
    penalized = False

    if candidate.expected_move is None or candidate.expected_move <= 0:
        reason = "Missing expected-move estimate"
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)
    else:
        short_ratio = (candidate.short_vs_expected_move or 0.0) / candidate.expected_move
        breakeven_ratio = (candidate.breakeven_vs_expected_move or 0.0) / candidate.expected_move
        if short_ratio < args.min_short_vs_expected_move_ratio:
            reason = (
                f"Short strike sits too far inside expected move "
                f"({short_ratio:.2f} < {args.min_short_vs_expected_move_ratio:.2f})"
            )
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)
        if breakeven_ratio < args.min_breakeven_vs_expected_move_ratio:
            reason = (
                f"Breakeven sits too far inside expected move "
                f"({breakeven_ratio:.2f} < {args.min_breakeven_vs_expected_move_ratio:.2f})"
            )
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)

    if candidate.fill_ratio < args.min_fill_ratio:
        reason = f"Natural-to-mid fill ratio is too weak ({candidate.fill_ratio:.2f} < {args.min_fill_ratio:.2f})"
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)

    if underlying_type == "single_name_equity" and candidate.calendar_confidence == "low":
        reason = "Calendar data confidence is low for this single-name candidate"
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)

    if blocked:
        return "blocked", tuple(reasons)
    if penalized:
        return "penalized", tuple(reasons)
    return "clean", ()


def attach_data_quality(
    *,
    candidates: list[SpreadCandidate],
    underlying_type: str,
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    enriched: list[SpreadCandidate] = []
    for candidate in candidates:
        status, reasons = assess_data_quality(candidate, underlying_type=underlying_type, args=args)
        if args.data_policy == "strict" and status == "blocked":
            continue
        enriched.append(replace(candidate, data_status=status, data_reasons=reasons))
    return enriched


def build_board_notes(candidate: SpreadCandidate, args: argparse.Namespace) -> tuple[str, ...]:
    notes: list[str] = []
    delta_target = args.short_delta_target
    if args.profile == "0dte":
        session_bucket = zero_dte_session_bucket()
        notes.append(f"session-{format_session_bucket(session_bucket)}")
        delta_target = zero_dte_delta_target(session_bucket)
    if candidate.short_delta is not None and abs(abs(candidate.short_delta) - delta_target) <= 0.02:
        notes.append("delta-fit")
    if candidate.expected_move and candidate.short_vs_expected_move is not None:
        if candidate.short_vs_expected_move >= 0:
            notes.append("outside-em")
        else:
            notes.append("inside-em")
    if candidate.fill_ratio >= 0.80:
        notes.append("good-fill")
    elif candidate.fill_ratio >= args.min_fill_ratio:
        notes.append("acceptable-fill")
    if min(candidate.short_open_interest, candidate.long_open_interest) >= max(args.min_open_interest * 3, 500):
        notes.append("liquid")
    if candidate.calendar_status == "clean":
        notes.append("calendar-clean")
    elif candidate.calendar_status == "penalized":
        notes.append("calendar-risk")
    if candidate.setup_status == "favorable":
        notes.append("setup-favorable")
    elif candidate.setup_status == "neutral":
        notes.append("setup-neutral")
    if candidate.data_status == "penalized":
        notes.append("data-caution")
    if candidate.greeks_source != "alpaca":
        notes.append("local-greeks")
    if len(notes) > 4 and candidate.greeks_source != "alpaca" and "local-greeks" not in notes[:4]:
        notes = [*notes[:3], "local-greeks"]
    return tuple(notes[:4])


def attach_board_notes(candidates: list[SpreadCandidate], args: argparse.Namespace) -> list[SpreadCandidate]:
    return [replace(candidate, board_notes=build_board_notes(candidate, args)) for candidate in candidates]


def deduplicate_candidates(candidates: list[SpreadCandidate], expand_duplicates: bool) -> list[SpreadCandidate]:
    if expand_duplicates:
        return candidates

    deduplicated: list[SpreadCandidate] = []
    seen_short_legs: set[str] = set()
    for candidate in candidates:
        if candidate.short_symbol in seen_short_legs:
            continue
        seen_short_legs.add(candidate.short_symbol)
        deduplicated.append(candidate)
    return deduplicated


def build_run_id(symbol: str, strategy: str, profile: str) -> str:
    return f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}_{symbol.lower()}_{strategy}_{profile}"


def build_filter_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "strategy": args.strategy,
        "profile": args.profile,
        "session_label": getattr(args, "session_label", None),
        "greeks_source": args.greeks_source,
        "session_bucket": zero_dte_session_bucket() if args.profile == "0dte" else None,
        "min_dte": args.min_dte,
        "max_dte": args.max_dte,
        "short_delta_min": args.short_delta_min,
        "short_delta_max": args.short_delta_max,
        "short_delta_target": args.short_delta_target,
        "min_width": args.min_width,
        "max_width": args.max_width,
        "min_credit": args.min_credit,
        "min_open_interest": args.min_open_interest,
        "max_relative_spread": args.max_relative_spread,
        "min_return_on_risk": args.min_return_on_risk,
        "feed": args.feed,
        "stock_feed": args.stock_feed,
        "calendar_policy": args.calendar_policy,
        "setup_filter": args.setup_filter,
        "expand_duplicates": args.expand_duplicates,
        "data_policy": args.data_policy,
        "min_fill_ratio": args.min_fill_ratio,
        "min_short_vs_expected_move_ratio": args.min_short_vs_expected_move_ratio,
        "min_breakeven_vs_expected_move_ratio": args.min_breakeven_vs_expected_move_ratio,
    }


def clone_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(**vars(args))


def validate_resolved_args(args: argparse.Namespace) -> None:
    if args.min_dte < 0 or args.max_dte < args.min_dte:
        raise SystemExit("Expected 0 <= min-dte <= max-dte")
    if args.short_delta_min < 0 or args.short_delta_max > 1 or args.short_delta_min > args.short_delta_max:
        raise SystemExit("Expected 0 <= short-delta-min <= short-delta-max <= 1")
    if args.short_delta_target < args.short_delta_min or args.short_delta_target > args.short_delta_max:
        raise SystemExit("Expected short-delta-target to fall inside the selected delta band")
    if args.min_width <= 0:
        raise SystemExit("Expected min-width > 0")
    if args.max_width < args.min_width:
        raise SystemExit("Expected max-width >= min-width")
    if args.min_credit <= 0:
        raise SystemExit("Expected min-credit > 0")
    if args.min_open_interest < 0:
        raise SystemExit("Expected min-open-interest >= 0")
    if args.max_relative_spread <= 0:
        raise SystemExit("Expected max-relative-spread > 0")
    if args.per_symbol_top <= 0:
        raise SystemExit("Expected per-symbol-top > 0")
    if args.min_fill_ratio <= 0 or args.min_fill_ratio > 1.25:
        raise SystemExit("Expected min-fill-ratio to be in (0, 1.25]")
    if args.min_short_vs_expected_move_ratio < -1 or args.min_short_vs_expected_move_ratio > 1:
        raise SystemExit("Expected min-short-vs-expected-move-ratio to be between -1 and 1")
    if args.min_breakeven_vs_expected_move_ratio < -1 or args.min_breakeven_vs_expected_move_ratio > 1:
        raise SystemExit("Expected min-breakeven-vs-expected-move-ratio to be between -1 and 1")


def make_open_order_payload(short_symbol: str, long_symbol: str, limit_price: float) -> dict[str, Any]:
    return {
        "order_class": "mleg",
        "qty": "1",
        "type": "limit",
        "limit_price": f"{limit_price:.2f}",
        "time_in_force": "day",
        "legs": [
            {
                "symbol": short_symbol,
                "ratio_qty": "1",
                "side": "sell",
                "position_intent": "sell_to_open",
            },
            {
                "symbol": long_symbol,
                "ratio_qty": "1",
                "side": "buy",
                "position_intent": "buy_to_open",
            },
        ],
    }


def make_close_order_payload(short_symbol: str, long_symbol: str, limit_price: float) -> dict[str, Any]:
    return {
        "order_class": "mleg",
        "qty": "1",
        "type": "limit",
        "limit_price": f"{limit_price:.2f}",
        "time_in_force": "day",
        "legs": [
            {
                "symbol": short_symbol,
                "ratio_qty": "1",
                "side": "buy",
                "position_intent": "buy_to_close",
            },
            {
                "symbol": long_symbol,
                "ratio_qty": "1",
                "side": "sell",
                "position_intent": "sell_to_close",
            },
        ],
    }


def make_order_payload(short_symbol: str, long_symbol: str, limit_price: float) -> dict[str, Any]:
    return make_open_order_payload(short_symbol=short_symbol, long_symbol=long_symbol, limit_price=limit_price)


def infer_trading_base_url(key_id: str, explicit_base_url: str | None) -> str:
    if explicit_base_url:
        return explicit_base_url.rstrip("/")
    if key_id.startswith("PK"):
        return "https://paper-api.alpaca.markets"
    return DEFAULT_TRADING_BASE_URL


def default_output_path(symbol: str, strategy: str, output_format: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    directory = {
        "call_credit": "call_credit_spreads",
        "put_credit": "put_credit_spreads",
        "combined": "combined_credit_spreads",
    }.get(strategy, "call_credit_spreads")
    return str(Path("outputs") / directory / f"{symbol.lower()}_{timestamp}.{output_format}")


def default_universe_output_path(label: str, strategy: str, output_format: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_label = label.lower().replace(" ", "_")
    return str(Path("outputs") / "universe_boards" / f"{safe_label}_{strategy}_{timestamp}.{output_format}")


def write_latest_copy(output_path: str, latest_name: str) -> str:
    latest_path = str(Path(output_path).with_name(latest_name))
    shutil.copyfile(output_path, latest_path)
    return latest_path


def option_expiry_close(expiration_date: str) -> datetime:
    local_close = datetime.combine(date.fromisoformat(expiration_date), time(16, 0), tzinfo=NEW_YORK)
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
    candidates: list[SpreadCandidate] = []

    for expiration_date, contracts in sorted(contracts_by_expiration.items()):
        snapshot_map = snapshots_by_expiration.get(expiration_date, {})
        sorted_contracts = sorted(contracts, key=lambda contract: contract.strike_price)
        expected_move = expected_moves_by_expiration.get(expiration_date)
        days_to_expiration = days_from_today(expiration_date)

        short_contracts = sorted_contracts if strategy == "call_credit" else list(reversed(sorted_contracts))

        for short_contract in short_contracts:
            short_snapshot = snapshot_map.get(short_contract.symbol)
            if not short_snapshot:
                continue
            if strategy == "call_credit":
                if short_contract.strike_price <= spot_price:
                    continue
            else:
                if short_contract.strike_price >= spot_price:
                    continue
            if short_contract.open_interest < args.min_open_interest:
                continue
            short_leg_relative_spread = relative_spread(short_snapshot)
            if short_leg_relative_spread > args.max_relative_spread:
                continue
            if short_snapshot.bid_size <= 0:
                continue
            short_delta = short_snapshot.delta
            if short_delta is None:
                continue
            short_delta_magnitude = abs(short_delta)
            if not (args.short_delta_min <= short_delta_magnitude <= args.short_delta_max):
                continue

            if strategy == "call_credit":
                long_contract_iterable = sorted_contracts
            else:
                short_index = sorted_contracts.index(short_contract)
                long_contract_iterable = reversed(sorted_contracts[:short_index])

            for long_contract in long_contract_iterable:
                if strategy == "call_credit":
                    if long_contract.strike_price <= short_contract.strike_price:
                        continue
                    width = long_contract.strike_price - short_contract.strike_price
                else:
                    if long_contract.strike_price >= short_contract.strike_price:
                        continue
                    width = short_contract.strike_price - long_contract.strike_price
                if width < args.min_width:
                    continue
                if width > args.max_width:
                    if strategy == "call_credit":
                        break
                    break

                long_snapshot = snapshot_map.get(long_contract.symbol)
                if not long_snapshot:
                    continue
                if long_contract.open_interest < args.min_open_interest:
                    continue
                long_leg_relative_spread = relative_spread(long_snapshot)
                if long_leg_relative_spread > args.max_relative_spread:
                    continue
                if long_snapshot.ask_size <= 0:
                    continue
                long_delta = long_snapshot.delta

                midpoint_credit = short_snapshot.midpoint - long_snapshot.midpoint
                natural_credit = short_snapshot.bid - long_snapshot.ask
                if midpoint_credit < effective_min_credit(width, args):
                    continue
                if natural_credit <= 0:
                    continue
                if midpoint_credit >= width:
                    continue

                max_profit = midpoint_credit * 100.0
                max_loss = (width - midpoint_credit) * 100.0
                if max_loss <= 0:
                    continue

                return_on_risk = midpoint_credit / (width - midpoint_credit)
                if return_on_risk < args.min_return_on_risk:
                    continue

                if strategy == "call_credit":
                    breakeven = short_contract.strike_price + midpoint_credit
                    short_otm_pct = (short_contract.strike_price - spot_price) / spot_price
                else:
                    breakeven = short_contract.strike_price - midpoint_credit
                    short_otm_pct = (spot_price - short_contract.strike_price) / spot_price
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
                    if strategy == "call_credit":
                        expected_move_boundary = spot_price + expected_move.amount
                        short_vs_expected_move = short_contract.strike_price - expected_move_boundary
                        breakeven_vs_expected_move = breakeven - expected_move_boundary
                    else:
                        expected_move_boundary = spot_price - expected_move.amount
                        short_vs_expected_move = expected_move_boundary - short_contract.strike_price
                        breakeven_vs_expected_move = expected_move_boundary - breakeven

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
                        breakeven_cushion_pct=(
                            (breakeven - spot_price) / spot_price
                            if strategy == "call_credit"
                            else (spot_price - breakeven) / spot_price
                        ),
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
                        ),
                    )
                )

    return candidates


def score_candidate(candidate: SpreadCandidate, args: argparse.Namespace) -> float:
    session_bucket = zero_dte_session_bucket() if args.profile == "0dte" else None
    if args.profile == "0dte":
        delta_target = zero_dte_delta_target(session_bucket or "off_hours")
    else:
        delta_target = args.short_delta_target
    delta_half_band = max((args.short_delta_max - args.short_delta_min) / 2.0, 0.01)
    delta_score = 1.0
    if candidate.short_delta is not None:
        delta_score = 1.0 - min(abs(abs(candidate.short_delta) - delta_target) / delta_half_band, 1.0)

    dte_target = (args.min_dte + args.max_dte) / 2.0
    dte_half_band = max((args.max_dte - args.min_dte) / 2.0, 1.0)
    dte_score = 1.0 - min(abs(candidate.days_to_expiration - dte_target) / dte_half_band, 1.0)

    fill_score = clamp(candidate.fill_ratio)
    liquidity_score = (
        0.75
        * log_scaled_score(
            min(candidate.short_open_interest, candidate.long_open_interest),
            floor=max(args.min_open_interest, 1),
            ceiling=max(args.min_open_interest * 8, 10),
        )
        + 0.25 * clamp(candidate.min_quote_size / 100.0)
    )

    if args.profile == "0dte":
        width_target = 2.0 if session_bucket == "late" else 1.0
    else:
        width_target = max(args.min_width, 2.0 if args.profile == "core" else args.min_width)
    width_window = max(args.max_width - args.min_width, 1.0)
    width_score = 1.0 - min(abs(candidate.width - width_target) / width_window, 1.0)

    return_on_risk_score = clamp(candidate.return_on_risk / 0.60)
    breakeven_cushion_score = clamp(candidate.breakeven_cushion_pct / 0.035)

    if candidate.expected_move and candidate.expected_move > 0:
        short_expected_move_score = clamp(0.50 + (candidate.short_vs_expected_move or 0.0) / candidate.expected_move)
        breakeven_expected_move_score = clamp(
            0.45 + (candidate.breakeven_vs_expected_move or 0.0) / candidate.expected_move
        )
    else:
        short_expected_move_score = clamp(candidate.short_otm_pct / 0.03)
        breakeven_expected_move_score = breakeven_cushion_score

    base_score = (
        0.24 * delta_score
        + 0.18 * short_expected_move_score
        + 0.16 * breakeven_expected_move_score
        + 0.14 * fill_score
        + 0.12 * liquidity_score
        + 0.08 * width_score
        + 0.05 * dte_score
        + 0.03 * return_on_risk_score
    )

    calendar_multiplier = {
        "clean": 1.0,
        "penalized": 0.92,
        "unknown": 0.82,
        "blocked": 0.0,
    }.get(candidate.calendar_status, 1.0)
    setup_multiplier = {
        "favorable": 1.0,
        "neutral": 0.93,
        "unfavorable": 0.78,
        "unknown": 0.88,
    }.get(candidate.setup_status, 0.88)
    data_multiplier = {
        "clean": 1.0,
        "penalized": 0.90,
        "blocked": 0.0,
    }.get(candidate.data_status, 1.0)
    return round(base_score * calendar_multiplier * setup_multiplier * data_multiplier * 100.0, 1)


def rank_candidates(candidates: list[SpreadCandidate], args: argparse.Namespace) -> list[SpreadCandidate]:
    ranked = [replace(candidate, quality_score=score_candidate(candidate, args)) for candidate in candidates]
    return sort_candidates_for_display(ranked)


def sort_candidates_for_display(candidates: list[SpreadCandidate]) -> list[SpreadCandidate]:
    ranked = list(candidates)
    ranked.sort(
        key=lambda candidate: (
            candidate.quality_score,
            candidate.return_on_risk,
            candidate.midpoint_credit,
            min(candidate.short_open_interest, candidate.long_open_interest),
        ),
        reverse=True,
    )
    return ranked


def format_dte_label(days_to_expiration: int) -> str:
    return "0D" if days_to_expiration == 0 else str(days_to_expiration)


def build_table_rows(candidates: list[SpreadCandidate], *, include_strategy: bool = False) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        row = []
        if include_strategy:
            row.append(strategy_display_label(candidate.strategy))
        row.extend(
            [
                candidate.expiration_date,
                format_dte_label(candidate.days_to_expiration),
                f"{candidate.short_strike:.2f}",
                f"{candidate.long_strike:.2f}",
                f"{candidate.width:.2f}",
                f"{candidate.midpoint_credit:.2f}",
                f"{candidate.return_on_risk * 100:.1f}",
                f"{candidate.quality_score:.1f}",
                "n/a" if candidate.short_delta is None else f"{candidate.short_delta:.2f}",
                f"{candidate.short_otm_pct * 100:.1f}",
                f"{candidate.breakeven_cushion_pct * 100:.1f}",
                "n/a" if candidate.short_vs_expected_move is None else f"{candidate.short_vs_expected_move:.2f}",
                f"{min(candidate.short_open_interest, candidate.long_open_interest)}",
                candidate.calendar_status,
                candidate.data_status,
                "n/a"
                if candidate.calendar_days_to_nearest_event is None
                else str(candidate.calendar_days_to_nearest_event),
            ]
        )
        rows.append(row)
    return rows


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt_row(row: list[str]) -> str:
        return " | ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(row))

    separator = "-+-".join("-" * width for width in widths)
    rendered = [fmt_row(headers), separator]
    rendered.extend(fmt_row(row) for row in rows)
    return "\n".join(rendered)


def print_human_readable(
    symbol: str,
    spot_price: float,
    candidates: list[SpreadCandidate],
    show_order_json: bool,
    setup: UnderlyingSetupContext | None,
    *,
    strategy: str,
    profile: str,
    greeks_source: str,
    setup_summaries: tuple[str, ...] = (),
) -> None:
    print(f"{symbol.upper()} spot: {spot_price:.2f}")
    print(f"Strategy: {strategy}")
    print(f"Profile: {profile}")
    print(f"Greeks: {greeks_source}")
    if profile == "0dte":
        print(f"0DTE session: {format_session_bucket(zero_dte_session_bucket())}")
    if setup is not None:
        print(f"Setup: {setup.status} ({setup.score:.1f})")
        if setup.reasons:
            print(f"Setup notes: {'; '.join(setup.reasons)}")
    elif setup_summaries:
        print(f"Setups: {'; '.join(setup_summaries)}")
    print(f"Candidates found: {len(candidates)}")
    print()

    if not candidates:
        print("No credit spreads matched the current filters and calendar policy.")
        return

    include_strategy = strategy == "combined" or len({candidate.strategy for candidate in candidates}) > 1
    headers = ["Expiry", "DTE", "Short", "Long", "Width", "MidCr", "ROR%", "Score", "Δ", "OTM%", "BE%", "S-EM", "MinOI", "Cal", "DQ", "EvtD"]
    if include_strategy:
        headers = ["Side", *headers]
    rows = build_table_rows(candidates, include_strategy=include_strategy)
    print(format_table(headers, rows))
    print()

    for index, candidate in enumerate(candidates, start=1):
        print(
            f"{index}. [{strategy_display_label(candidate.strategy)}] {candidate.short_symbol} -> {candidate.long_symbol} | "
            f"score {candidate.quality_score:.1f} | "
            f"breakeven {candidate.breakeven:.2f} | "
            f"calendar {candidate.calendar_status}"
        )
        if candidate.greeks_source != "alpaca":
            print(f"   greeks: {candidate.greeks_source}")
        if candidate.expected_move is not None:
            print(
                "   expected move: "
                f"{candidate.expected_move:.2f} ({candidate.expected_move_pct * 100:.2f}% of spot) "
                f"from {candidate.expected_move_source_strike:.2f} strike"
            )
        if candidate.calendar_reasons:
            print(f"   reasons: {'; '.join(candidate.calendar_reasons)}")
        if candidate.data_reasons:
            print(f"   data: {'; '.join(candidate.data_reasons)}")
        if candidate.calendar_sources:
            source_line = ", ".join(candidate.calendar_sources)
            print(f"   sources: {source_line} | confidence {candidate.calendar_confidence}")
        if candidate.macro_regime:
            print(f"   macro regime: {candidate.macro_regime}")
        if candidate.setup_score is not None:
            print(f"   setup: {candidate.setup_status} ({candidate.setup_score:.1f})")
        if show_order_json:
            print("   order payload:")
            print(json.dumps(candidate.order_payload, indent=2))
        print()


def write_csv(path: str, candidates: list[SpreadCandidate]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "underlying_symbol",
        "strategy",
        "profile",
        "expiration_date",
        "days_to_expiration",
        "underlying_price",
        "short_symbol",
        "long_symbol",
        "short_strike",
        "long_strike",
        "width",
        "short_delta",
        "long_delta",
        "greeks_source",
        "short_midpoint",
        "long_midpoint",
        "short_bid",
        "short_ask",
        "long_bid",
        "long_ask",
        "midpoint_credit",
        "natural_credit",
        "max_profit",
        "max_loss",
        "return_on_risk",
        "breakeven",
        "breakeven_cushion_pct",
        "short_otm_pct",
        "short_open_interest",
        "long_open_interest",
        "short_relative_spread",
        "long_relative_spread",
        "fill_ratio",
        "min_quote_size",
        "expected_move",
        "expected_move_pct",
        "expected_move_source_strike",
        "short_vs_expected_move",
        "breakeven_vs_expected_move",
        "quality_score",
        "calendar_status",
        "calendar_reasons",
        "calendar_confidence",
        "calendar_sources",
        "calendar_last_updated",
        "calendar_days_to_nearest_event",
        "macro_regime",
        "setup_status",
        "setup_score",
        "setup_reasons",
        "data_status",
        "data_reasons",
        "board_notes",
        "order_payload",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for candidate in candidates:
            row = asdict(candidate)
            row["calendar_reasons"] = "; ".join(candidate.calendar_reasons)
            row["calendar_sources"] = ", ".join(candidate.calendar_sources)
            row["setup_reasons"] = "; ".join(candidate.setup_reasons)
            row["data_reasons"] = "; ".join(candidate.data_reasons)
            row["board_notes"] = ", ".join(candidate.board_notes)
            row["order_payload"] = json.dumps(candidate.order_payload, separators=(",", ":"))
            writer.writerow(row)


def serialize_setup_context(setup: UnderlyingSetupContext | None) -> dict[str, Any] | None:
    if setup is None:
        return None
    payload = asdict(setup)
    payload["reasons"] = list(setup.reasons)
    return payload


def write_json(
    path: str,
    symbol: str,
    spot_price: float,
    args: argparse.Namespace,
    candidates: list[SpreadCandidate],
    *,
    run_id: str | None = None,
    setup: UnderlyingSetupContext | None = None,
) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": symbol,
        "spot_price": spot_price,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "run_id": run_id,
        "filters": build_filter_payload(args),
        "setup": serialize_setup_context(setup),
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def build_universe_board_rows(candidates: list[SpreadCandidate], *, include_strategy: bool = False) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        row = [candidate.underlying_symbol]
        if include_strategy:
            row.append(strategy_display_label(candidate.strategy))
        row.extend(
            [
                candidate.expiration_date,
                format_dte_label(candidate.days_to_expiration),
                f"{candidate.underlying_price:.2f}",
                f"{candidate.short_strike:.2f}",
                f"{candidate.long_strike:.2f}",
                f"{candidate.midpoint_credit:.2f}",
                f"{candidate.quality_score:.1f}",
                "n/a" if candidate.short_delta is None else f"{candidate.short_delta:.2f}",
                f"{candidate.breakeven_cushion_pct * 100:.1f}",
                "n/a" if candidate.short_vs_expected_move is None else f"{candidate.short_vs_expected_move:.2f}",
                candidate.calendar_status,
                candidate.data_status,
                candidate.setup_status,
                ",".join(candidate.board_notes),
            ]
        )
        rows.append(row)
    return rows


def print_universe_board(
    *,
    label: str,
    strategy: str,
    profile: str,
    greeks_source: str,
    symbols: list[str],
    board_candidates: list[SpreadCandidate],
    failures: list[UniverseScanFailure],
) -> None:
    print(f"Universe: {label}")
    print(f"Strategy: {strategy}")
    print(f"Greeks: {greeks_source}")
    if profile == "0dte" or (board_candidates and any(candidate.profile == "0dte" for candidate in board_candidates)):
        print(f"0DTE session: {format_session_bucket(zero_dte_session_bucket())}")
    print(f"Symbols requested: {len(symbols)}")
    print(f"Board entries: {len(board_candidates)}")
    if failures:
        print(f"Failures: {len(failures)}")
    print()

    if board_candidates:
        include_strategy = strategy == "combined" or len({candidate.strategy for candidate in board_candidates}) > 1
        headers = ["Symbol", "Expiry", "DTE", "Spot", "Short", "Long", "MidCr", "Score", "Δ", "BE%", "S-EM", "Cal", "DQ", "Setup", "Why"]
        if include_strategy:
            headers = ["Symbol", "Side", *headers[1:]]
        print(format_table(headers, build_universe_board_rows(board_candidates, include_strategy=include_strategy)))
        print()
    else:
        print("No universe candidates matched the current filters.")
        print()

    for index, candidate in enumerate(board_candidates, start=1):
        print(
            f"{index}. {candidate.underlying_symbol} [{strategy_display_label(candidate.strategy)}] "
            f"{candidate.short_symbol} -> {candidate.long_symbol} | "
            f"score {candidate.quality_score:.1f} | breakeven {candidate.breakeven:.2f}"
        )
        if candidate.board_notes:
            print(f"   why: {', '.join(candidate.board_notes)}")
        if candidate.calendar_reasons:
            print(f"   calendar: {'; '.join(candidate.calendar_reasons)}")
        if candidate.data_reasons:
            print(f"   data: {'; '.join(candidate.data_reasons)}")
        if candidate.setup_reasons:
            print(f"   setup: {'; '.join(candidate.setup_reasons)}")
        print()

    if failures:
        print("Failures:")
        for failure in failures:
            print(f"- {failure.symbol}: {failure.error}")


def write_universe_csv(path: str, candidates: list[SpreadCandidate]) -> None:
    write_csv(path, candidates)


def write_universe_json(
    path: str,
    *,
    label: str,
    strategy: str,
    symbols: list[str],
    candidates: list[SpreadCandidate],
    failures: list[UniverseScanFailure],
) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": "universe",
        "label": label,
        "strategy": strategy,
        "symbols": symbols,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "candidate_count": len(candidates),
        "failures": [asdict(failure) for failure in failures],
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def build_stream_symbols(candidates: list[SpreadCandidate], *, max_symbols: int = 16) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        for symbol in (candidate.short_symbol, candidate.long_symbol):
            if symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
            if len(symbols) >= max_symbols:
                return symbols
    return symbols


def build_live_spread_rows(
    candidates: list[SpreadCandidate],
    live_quotes: dict[str, LiveOptionQuote],
) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        short_quote = live_quotes.get(candidate.short_symbol)
        long_quote = live_quotes.get(candidate.long_symbol)
        if short_quote is None or long_quote is None:
            continue
        live_mid_credit = short_quote.midpoint - long_quote.midpoint
        live_natural_credit = short_quote.bid - long_quote.ask
        rows.append(
            [
                strategy_display_label(candidate.strategy),
                candidate.expiration_date,
                f"{candidate.short_strike:.2f}",
                f"{candidate.long_strike:.2f}",
                f"{live_mid_credit:.2f}",
                f"{live_natural_credit:.2f}",
                f"{short_quote.bid:.2f}/{short_quote.ask:.2f}",
                f"{long_quote.bid:.2f}/{long_quote.ask:.2f}",
                "n/a" if short_quote.timestamp is None else str(short_quote.timestamp),
            ]
        )
    return rows


def maybe_stream_live_quotes(
    *,
    args: argparse.Namespace,
    client: AlpacaClient,
    candidates: list[SpreadCandidate],
) -> None:
    if not args.stream_live_quotes or args.json or not candidates:
        return

    stream_symbols = build_stream_symbols(candidates[: args.top])
    if not stream_symbols:
        return

    print()
    print(f"Streaming live option quotes for {len(stream_symbols)} legs via Alpaca websocket...")
    try:
        streamer = AlpacaOptionQuoteStreamer(
            key_id=client.headers["APCA-API-KEY-ID"],
            secret_key=client.headers["APCA-API-SECRET-KEY"],
            data_base_url=client.data_base_url,
            feed=args.feed,
        )
        live_quotes = streamer.stream_quotes(stream_symbols, duration_seconds=args.stream_seconds)
    except Exception as exc:
        print(f"Live quote stream unavailable: {exc}")
        return

    if not live_quotes:
        print("Live quote stream returned no quote updates.")
        return

    rows = build_live_spread_rows(candidates[: args.top], live_quotes)
    if not rows:
        print("Live quote stream did not return both legs for the displayed spreads.")
        return

    headers = ["Side", "Expiry", "Short", "Long", "LiveMid", "LiveNat", "ShortQ", "LongQ", "Time"]
    print(format_table(headers, rows))
    print()


def build_calendar_reason_messages(decision: CalendarPolicyDecision) -> tuple[str, ...]:
    return tuple(reason.message for reason in decision.reasons)


def attach_calendar_decisions(
    *,
    symbol: str,
    strategy: str,
    underlying_type: str,
    candidates: list[SpreadCandidate],
    resolver: Any,
    calendar_policy: str,
    refresh_calendar_events: bool,
) -> list[SpreadCandidate]:
    if calendar_policy == "off" or not candidates:
        return candidates

    window_start = datetime.now(UTC).isoformat()
    decisions_by_expiration: dict[str, CalendarPolicyDecision] = {}
    for expiration_date in sorted({candidate.expiration_date for candidate in candidates}, reverse=True):
        context = resolver.resolve_calendar_context(
            symbol=symbol,
            strategy=strategy,
            window_start=window_start,
            window_end=option_expiry_close(expiration_date).isoformat(),
            underlying_type=underlying_type,
            refresh=refresh_calendar_events,
        )
        decisions_by_expiration[expiration_date] = apply_credit_spread_policy(
            context,
            strategy=strategy,
            underlying_type=underlying_type,
            mode=calendar_policy,
        )

    filtered_candidates: list[SpreadCandidate] = []
    for candidate in candidates:
        decision = decisions_by_expiration[candidate.expiration_date]
        if calendar_policy == "strict" and decision.status == "blocked":
            continue
        filtered_candidates.append(
            replace(
                candidate,
                calendar_status=decision.status,
                calendar_reasons=build_calendar_reason_messages(decision),
                calendar_confidence=decision.source_confidence,
                calendar_sources=decision.sources,
                calendar_last_updated=decision.last_updated,
                calendar_days_to_nearest_event=decision.days_to_nearest_event,
                macro_regime=decision.macro_regime,
            )
        )
    return filtered_candidates


def group_contracts_by_expiration(contracts: Iterable[OptionContract]) -> dict[str, list[OptionContract]]:
    grouped: dict[str, list[OptionContract]] = {}
    for contract in contracts:
        grouped.setdefault(contract.expiration_date, []).append(contract)
    return grouped


def latest_bar_on_or_before(bars: list[DailyBar], target_date: date) -> DailyBar | None:
    eligible = [bar for bar in bars if datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date() <= target_date]
    if not eligible:
        return None
    return eligible[-1]


def bars_through_date(bars: list[DailyBar], target_date: date) -> list[DailyBar]:
    return [bar for bar in bars if datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date() <= target_date]


def latest_option_bar_on_or_before(
    bars_by_symbol: dict[str, list[DailyBar]],
    symbol: str,
    target_date: date,
) -> DailyBar | None:
    return latest_bar_on_or_before(bars_by_symbol.get(symbol, []), target_date)


def estimate_spread_bar(short_bar: DailyBar, long_bar: DailyBar) -> dict[str, float]:
    return {
        "open": max(short_bar.open - long_bar.open, 0.0),
        "high": max(short_bar.high - long_bar.low, 0.0),
        "low": max(short_bar.low - long_bar.high, 0.0),
        "close": max(short_bar.close - long_bar.close, 0.0),
    }


def option_bar_available_for_target(
    bars_by_symbol: dict[str, list[DailyBar]],
    short_symbol: str,
    long_symbol: str,
    target_date: date,
) -> bool:
    short_bar = latest_option_bar_on_or_before(bars_by_symbol, short_symbol, target_date)
    long_bar = latest_option_bar_on_or_before(bars_by_symbol, long_symbol, target_date)
    if short_bar is None or long_bar is None:
        return False
    short_date = datetime.fromisoformat(short_bar.timestamp.replace("Z", "+00:00")).date()
    long_date = datetime.fromisoformat(long_bar.timestamp.replace("Z", "+00:00")).date()
    return short_date == target_date and long_date == target_date


def option_bars_by_date(bars: list[DailyBar]) -> dict[date, DailyBar]:
    return {
        datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date(): bar
        for bar in bars
    }


def simulate_exit_until_date(
    candidate: dict[str, Any],
    *,
    option_bars: dict[str, list[DailyBar]],
    start_date: date,
    target_date: date,
    profit_target: float,
    stop_multiple: float,
) -> dict[str, Any]:
    short_bars = option_bars_by_date(option_bars.get(candidate["short_symbol"], []))
    long_bars = option_bars_by_date(option_bars.get(candidate["long_symbol"], []))
    entry_credit = candidate["midpoint_credit"]
    target_mark = max(entry_credit * (1.0 - profit_target), 0.0)
    stop_mark = entry_credit * stop_multiple

    path_dates = sorted(d for d in short_bars if start_date <= d <= target_date and d in long_bars)
    if not path_dates:
        return {"status": "pending_option_bars"}

    last_mark = None
    for path_date in path_dates:
        spread_bar = estimate_spread_bar(short_bars[path_date], long_bars[path_date])
        last_mark = spread_bar["close"]
        hit_target = spread_bar["low"] <= target_mark
        hit_stop = spread_bar["high"] >= stop_mark

        if hit_target and hit_stop:
            return {
                "status": "conflict",
                "exit_date": path_date.isoformat(),
                "exit_reason": "conflict_stop_first",
                "exit_mark": stop_mark,
                "estimated_pnl": (entry_credit - stop_mark) * 100.0,
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": True,
                "stop_hit": True,
            }
        if hit_target:
            return {
                "status": "exited",
                "exit_date": path_date.isoformat(),
                "exit_reason": "profit_target",
                "exit_mark": target_mark,
                "estimated_pnl": (entry_credit - target_mark) * 100.0,
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": True,
                "stop_hit": False,
            }
        if hit_stop:
            return {
                "status": "exited",
                "exit_date": path_date.isoformat(),
                "exit_reason": "stop",
                "exit_mark": stop_mark,
                "estimated_pnl": (entry_credit - stop_mark) * 100.0,
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": False,
                "stop_hit": True,
            }

    return {
        "status": "open",
        "exit_date": path_dates[-1].isoformat(),
        "exit_reason": "mark",
        "exit_mark": last_mark,
        "estimated_pnl": (entry_credit - last_mark) * 100.0 if last_mark is not None else None,
        "spread_mark_close": last_mark,
        "spread_mark_low": None,
        "spread_mark_high": None,
        "profit_target_hit": False,
        "stop_hit": False,
    }


def mark_spread_on_date(
    candidate: dict[str, Any],
    *,
    option_bars: dict[str, list[DailyBar]],
    target_date: date,
) -> dict[str, Any]:
    short_bar = latest_option_bar_on_or_before(option_bars, candidate["short_symbol"], target_date)
    long_bar = latest_option_bar_on_or_before(option_bars, candidate["long_symbol"], target_date)
    if short_bar is None or long_bar is None:
        return {"status": "pending_option_bars"}

    spread_bar = estimate_spread_bar(short_bar, long_bar)
    entry_credit = candidate["midpoint_credit"]
    close_mark = spread_bar["close"]
    return {
        "status": "mark_only",
        "exit_date": target_date.isoformat(),
        "exit_reason": "entry_mark",
        "exit_mark": close_mark,
        "estimated_pnl": (entry_credit - close_mark) * 100.0,
        "spread_mark_close": close_mark,
        "spread_mark_low": None,
        "spread_mark_high": None,
        "profit_target_hit": False,
        "stop_hit": False,
    }


def summarize_replay(
    *,
    run_payload: dict[str, Any],
    candidates: list[dict[str, Any]],
    bars: list[DailyBar],
    option_bars: dict[str, list[DailyBar]],
    profit_target: float,
    stop_multiple: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    generated_at = datetime.fromisoformat(run_payload["generated_at"].replace("Z", "+00:00"))
    run_date = generated_at.astimezone(NEW_YORK).date()
    strategy = run_payload.get("strategy") or run_payload["filters"].get("strategy", "call_credit")
    latest_available_date = None if not bars else max(
        datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date() for bar in bars
    )
    horizons = [
        ("entry", run_date),
        ("1d", run_date + timedelta(days=1)),
        ("3d", run_date + timedelta(days=3)),
    ]

    summaries: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for label, target_date in horizons:
        horizon_bars = bars_through_date(bars, target_date)
        available_candidates = 0
        touched = 0
        closed_past_short = 0
        closed_past_breakeven = 0
        profit_target_hits = 0
        stop_hits = 0
        conflicts = 0
        total_pnl = 0.0

        for candidate in candidates:
            if latest_available_date is None or latest_available_date < target_date:
                rows.append(
                    {
                        "horizon": label,
                        "short_symbol": candidate["short_symbol"],
                        "long_symbol": candidate["long_symbol"],
                        "expiration_date": candidate["expiration_date"],
                        "status": "pending",
                    }
                )
                continue

            horizon_bar = latest_bar_on_or_before(bars, target_date)
            if horizon_bar is None:
                continue
            if label == "entry":
                replay_path = mark_spread_on_date(
                    candidate,
                    option_bars=option_bars,
                    target_date=target_date,
                )
            else:
                replay_path = simulate_exit_until_date(
                    candidate,
                    option_bars=option_bars,
                    start_date=run_date,
                    target_date=target_date,
                    profit_target=profit_target,
                    stop_multiple=stop_multiple,
                )
            if replay_path["status"] == "pending_option_bars":
                rows.append(
                    {
                        "horizon": label,
                        "short_symbol": candidate["short_symbol"],
                        "long_symbol": candidate["long_symbol"],
                        "expiration_date": candidate["expiration_date"],
                        "status": "pending_option_bars",
                    }
                )
                continue

            available_candidates += 1
            path_bars = horizon_bars
            path_high = max(bar.high for bar in path_bars) if path_bars else horizon_bar.high
            path_low = min(bar.low for bar in path_bars) if path_bars else horizon_bar.low
            if strategy == "put_credit":
                touched_short = path_low <= candidate["short_strike"]
                closed_beyond_short = horizon_bar.close <= candidate["short_strike"]
                closed_beyond_breakeven = horizon_bar.close <= candidate["breakeven"]
            else:
                touched_short = path_high >= candidate["short_strike"]
                closed_beyond_short = horizon_bar.close >= candidate["short_strike"]
                closed_beyond_breakeven = horizon_bar.close >= candidate["breakeven"]
            touched += int(touched_short)
            closed_past_short += int(closed_beyond_short)
            closed_past_breakeven += int(closed_beyond_breakeven)
            profit_target_hits += int(replay_path["profit_target_hit"])
            stop_hits += int(replay_path["stop_hit"])
            conflicts += int(replay_path["status"] == "conflict")
            total_pnl += replay_path["estimated_pnl"] or 0.0
            rows.append(
                {
                    "horizon": label,
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "available",
                    "spot_at_horizon": horizon_bar.close,
                    "path_extreme_to_horizon": path_low if strategy == "put_credit" else path_high,
                    "touched_short_strike": touched_short,
                    "closed_past_short_strike": closed_beyond_short,
                    "closed_past_breakeven": closed_beyond_breakeven,
                    "spread_mark_close": replay_path["spread_mark_close"],
                    "spread_mark_low": replay_path["spread_mark_low"],
                    "spread_mark_high": replay_path["spread_mark_high"],
                    "estimated_pnl": replay_path["estimated_pnl"],
                    "estimated_profit_target_hit": replay_path["profit_target_hit"],
                    "estimated_stop_hit": replay_path["stop_hit"],
                    "exit_reason": replay_path["exit_reason"],
                    "exit_date": replay_path["exit_date"],
                    "replay_status": replay_path["status"],
                }
            )

        total = len(candidates)
        summaries.append(
            {
                "horizon": label,
                "available": available_candidates,
                "pending": total - available_candidates,
                "touch_pct": None if available_candidates == 0 else 100.0 * touched / available_candidates,
                "close_past_short_pct": None
                if available_candidates == 0
                else 100.0 * closed_past_short / available_candidates,
                "close_past_breakeven_pct": None
                if available_candidates == 0
                else 100.0 * closed_past_breakeven / available_candidates,
                "profit_target_hit_pct": None
                if available_candidates == 0
                else 100.0 * profit_target_hits / available_candidates,
                "stop_hit_pct": None if available_candidates == 0 else 100.0 * stop_hits / available_candidates,
                "conflict_pct": None if available_candidates == 0 else 100.0 * conflicts / available_candidates,
                "avg_pnl": None if available_candidates == 0 else total_pnl / available_candidates,
            }
        )

    expiry_available = 0
    expiry_touched = 0
    expiry_closed_past_short = 0
    expiry_closed_past_breakeven = 0
    expiry_profit_targets = 0
    expiry_stop_hits = 0
    expiry_conflicts = 0
    expiry_total_pnl = 0.0
    for candidate in candidates:
        expiry_date = date.fromisoformat(candidate["expiration_date"])
        if latest_available_date is None or latest_available_date < expiry_date:
            rows.append(
                {
                    "horizon": "expiry",
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "pending",
                }
            )
            continue

        horizon_bar = latest_bar_on_or_before(bars, expiry_date)
        if horizon_bar is None:
            continue
        replay_path = simulate_exit_until_date(
            candidate,
            option_bars=option_bars,
            start_date=run_date,
            target_date=expiry_date,
            profit_target=profit_target,
            stop_multiple=stop_multiple,
        )
        if replay_path["status"] == "pending_option_bars":
            rows.append(
                {
                    "horizon": "expiry",
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "pending_option_bars",
                }
            )
            continue
        expiry_available += 1
        path_bars = bars_through_date(bars, expiry_date)
        path_high = max(bar.high for bar in path_bars) if path_bars else horizon_bar.high
        path_low = min(bar.low for bar in path_bars) if path_bars else horizon_bar.low
        if strategy == "put_credit":
            touched_short = path_low <= candidate["short_strike"]
            closed_beyond_short = horizon_bar.close <= candidate["short_strike"]
            closed_beyond_breakeven = horizon_bar.close <= candidate["breakeven"]
        else:
            touched_short = path_high >= candidate["short_strike"]
            closed_beyond_short = horizon_bar.close >= candidate["short_strike"]
            closed_beyond_breakeven = horizon_bar.close >= candidate["breakeven"]
        expiry_touched += int(touched_short)
        expiry_closed_past_short += int(closed_beyond_short)
        expiry_closed_past_breakeven += int(closed_beyond_breakeven)
        expiry_profit_targets += int(replay_path["profit_target_hit"])
        expiry_stop_hits += int(replay_path["stop_hit"])
        expiry_conflicts += int(replay_path["status"] == "conflict")
        expiry_total_pnl += replay_path["estimated_pnl"] or 0.0
        rows.append(
            {
                "horizon": "expiry",
                "short_symbol": candidate["short_symbol"],
                "long_symbol": candidate["long_symbol"],
                "expiration_date": candidate["expiration_date"],
                "status": "available",
                "spot_at_horizon": horizon_bar.close,
                "path_extreme_to_horizon": path_low if strategy == "put_credit" else path_high,
                "touched_short_strike": touched_short,
                "closed_past_short_strike": closed_beyond_short,
                "closed_past_breakeven": closed_beyond_breakeven,
                "spread_mark_close": replay_path["spread_mark_close"],
                "spread_mark_low": replay_path["spread_mark_low"],
                "spread_mark_high": replay_path["spread_mark_high"],
                "estimated_pnl": replay_path["estimated_pnl"],
                "estimated_profit_target_hit": replay_path["profit_target_hit"],
                "estimated_stop_hit": replay_path["stop_hit"],
                "exit_reason": replay_path["exit_reason"],
                "exit_date": replay_path["exit_date"],
                "replay_status": replay_path["status"],
            }
        )

    total = len(candidates)
    summaries.append(
        {
            "horizon": "expiry",
            "available": expiry_available,
            "pending": total - expiry_available,
            "touch_pct": None if expiry_available == 0 else 100.0 * expiry_touched / expiry_available,
            "close_past_short_pct": None
            if expiry_available == 0
            else 100.0 * expiry_closed_past_short / expiry_available,
            "close_past_breakeven_pct": None
            if expiry_available == 0
            else 100.0 * expiry_closed_past_breakeven / expiry_available,
            "profit_target_hit_pct": None
            if expiry_available == 0
            else 100.0 * expiry_profit_targets / expiry_available,
            "stop_hit_pct": None if expiry_available == 0 else 100.0 * expiry_stop_hits / expiry_available,
            "conflict_pct": None if expiry_available == 0 else 100.0 * expiry_conflicts / expiry_available,
            "avg_pnl": None if expiry_available == 0 else expiry_total_pnl / expiry_available,
        }
    )

    return summaries, rows


def print_replay_summary(
    run_payload: dict[str, Any],
    summaries: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    strategy = run_payload.get("strategy") or run_payload["filters"].get("strategy", "call_credit")
    print(
        f"Replay run: {run_payload['run_id']} | {run_payload['symbol']} | "
        f"strategy {strategy} | profile {run_payload['profile']} | generated {run_payload['generated_at']}"
    )
    print(f"Stored candidates: {run_payload['candidate_count']}")
    print()

    table_headers = ["Horizon", "Avail", "Pending", "Touch%", "PastShort%", "PastBE%", "PT%", "Stop%", "Conf%", "AvgPnL$"]
    table_rows = []
    for summary in summaries:
        table_rows.append(
            [
                summary["horizon"],
                str(summary["available"]),
                str(summary["pending"]),
                "n/a" if summary["touch_pct"] is None else f"{summary['touch_pct']:.1f}",
                "n/a" if summary["close_past_short_pct"] is None else f"{summary['close_past_short_pct']:.1f}",
                "n/a"
                if summary["close_past_breakeven_pct"] is None
                else f"{summary['close_past_breakeven_pct']:.1f}",
                "n/a" if summary["profit_target_hit_pct"] is None else f"{summary['profit_target_hit_pct']:.1f}",
                "n/a" if summary["stop_hit_pct"] is None else f"{summary['stop_hit_pct']:.1f}",
                "n/a" if summary["conflict_pct"] is None else f"{summary['conflict_pct']:.1f}",
                "n/a" if summary["avg_pnl"] is None else f"{summary['avg_pnl']:.0f}",
            ]
        )
    print(format_table(table_headers, table_rows))
    print()

    available_rows = [row for row in rows if row["status"] == "available"][:10]
    if available_rows:
        detail_headers = [
            "Horizon",
            "Short",
            "Long",
            "Expiry",
            "Spot",
            "Sprd",
            "PnL$",
            "Touch",
            "PastShort",
            "PastBE",
            "Exit",
            "PT",
            "Stop",
        ]
        detail_rows = [
            [
                row["horizon"],
                row["short_symbol"],
                row["long_symbol"],
                row["expiration_date"],
                f"{row['spot_at_horizon']:.2f}",
                f"{row['spread_mark_close']:.2f}",
                f"{row['estimated_pnl']:.0f}",
                "yes" if row["touched_short_strike"] else "no",
                "yes" if row["closed_past_short_strike"] else "no",
                "yes" if row["closed_past_breakeven"] else "no",
                row["exit_reason"],
                "yes" if row["estimated_profit_target_hit"] else "no",
                "yes" if row["estimated_stop_hit"] else "no",
            ]
            for row in available_rows
        ]
        print(format_table(detail_headers, detail_rows))
    else:
        print("Replay data is not available yet for the stored horizons.")


def run_replay(
    *,
    args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
) -> int:
    if args.replay_latest and args.strategy == "combined":
        raise SystemExit("Replay latest requires --strategy call_credit or --strategy put_credit")
    if args.replay_run_id:
        run_payload = history_store.get_run(args.replay_run_id)
    else:
        if not args.symbol:
            raise SystemExit("Replay latest requires --symbol or use --replay-run-id")
        run_payload = history_store.get_latest_run(args.symbol.upper(), strategy=args.strategy)

    if not run_payload:
        target = args.replay_run_id or args.symbol.upper()
        raise SystemExit(f"No stored run found for replay target: {target}")

    candidates = history_store.list_candidates(run_payload["run_id"])
    generated_at = datetime.fromisoformat(run_payload["generated_at"].replace("Z", "+00:00"))
    run_date = generated_at.astimezone(NEW_YORK).date()
    replay_end = max(
        [
            run_date + timedelta(days=3),
            *[date.fromisoformat(candidate["expiration_date"]) for candidate in candidates],
        ]
    )
    bars = client.get_daily_bars(
        run_payload["symbol"],
        start=(run_date - timedelta(days=2)).isoformat(),
        end=replay_end.isoformat(),
        stock_feed=args.stock_feed,
    )
    option_symbols = sorted(
        {
            *[candidate["short_symbol"] for candidate in candidates],
            *[candidate["long_symbol"] for candidate in candidates],
        }
    )
    option_bars = client.get_option_bars(
        option_symbols,
        start=run_date.isoformat(),
        end=replay_end.isoformat(),
    )
    summaries, rows = summarize_replay(
        run_payload=run_payload,
        candidates=candidates,
        bars=bars,
        option_bars=option_bars,
        profit_target=args.replay_profit_target,
        stop_multiple=args.replay_stop_multiple,
    )
    print_replay_summary(run_payload, summaries, rows)
    return 0


def scan_symbol_live(
    *,
    symbol: str,
    base_args: argparse.Namespace,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    history_store: RunHistoryRepository,
) -> SymbolScanResult:
    symbol = symbol.upper()
    underlying_type = classify_underlying_type(symbol)
    symbol_args = clone_args(base_args)
    symbol_args.symbol = symbol
    apply_profile_defaults(symbol_args, underlying_type)
    validate_resolved_args(symbol_args)
    validate_profile_scope(symbol, symbol_args, underlying_type)

    min_expiration = (date.today() + timedelta(days=symbol_args.min_dte)).isoformat()
    max_expiration = (date.today() + timedelta(days=symbol_args.max_dte)).isoformat()

    spot_price = client.get_underlying_price(symbol, symbol_args.stock_feed)
    setup_context: UnderlyingSetupContext | None = None
    if symbol_args.setup_filter == "on":
        daily_bars = client.get_daily_bars(
            symbol,
            start=(date.today() - timedelta(days=120)).isoformat(),
            end=date.today().isoformat(),
            stock_feed=symbol_args.stock_feed,
        )
        intraday_bars: list[IntradayBar] = []
        try:
            session_start = datetime.combine(date.today(), time(9, 30), tzinfo=NEW_YORK).astimezone(UTC)
            session_end = datetime.now(UTC)
            intraday_bars = client.get_intraday_bars(
                symbol,
                start=session_start.isoformat(),
                end=session_end.isoformat(),
                stock_feed=symbol_args.stock_feed,
            )
        except Exception:
            intraday_bars = []
        setup_context = analyze_underlying_setup(
            symbol,
            spot_price,
            daily_bars,
            strategy=symbol_args.strategy,
            profile=symbol_args.profile,
            intraday_bars=intraday_bars,
        )

    call_contracts = client.list_option_contracts(symbol, min_expiration, max_expiration, option_type="call")
    put_contracts = client.list_option_contracts(symbol, min_expiration, max_expiration, option_type="put")
    contracts_by_expiration = group_contracts_by_expiration(call_contracts)
    put_contracts_by_expiration = group_contracts_by_expiration(put_contracts)

    call_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    put_snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    for expiration_date in sorted(contracts_by_expiration):
        call_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            symbol,
            expiration_date,
            "call",
            symbol_args.feed,
        )
        put_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            symbol,
            expiration_date,
            "put",
            symbol_args.feed,
        )

    raw_option_snapshots_by_expiration = (
        call_snapshots_by_expiration if symbol_args.strategy == "call_credit" else put_snapshots_by_expiration
    )
    quoted_contract_count, alpaca_delta_contract_count = count_snapshot_delta_coverage(raw_option_snapshots_by_expiration)

    snapshot_timestamp = datetime.now(UTC)
    call_snapshots_by_expiration = enrich_missing_greeks(
        symbol=symbol,
        option_type="call",
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=call_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        as_of=snapshot_timestamp,
        source_mode=symbol_args.greeks_source,
    )
    put_snapshots_by_expiration = enrich_missing_greeks(
        symbol=symbol,
        option_type="put",
        spot_price=spot_price,
        contracts_by_expiration=put_contracts_by_expiration,
        snapshots_by_expiration=put_snapshots_by_expiration,
        greeks_provider=greeks_provider,
        as_of=snapshot_timestamp,
        source_mode=symbol_args.greeks_source,
    )

    expected_moves_by_expiration = build_expected_move_estimates(
        spot_price=spot_price,
        call_contracts_by_expiration=contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=call_snapshots_by_expiration,
        put_snapshots_by_expiration=put_snapshots_by_expiration,
    )

    option_contracts_by_expiration = (
        contracts_by_expiration if symbol_args.strategy == "call_credit" else put_contracts_by_expiration
    )
    option_snapshots_by_expiration = (
        call_snapshots_by_expiration if symbol_args.strategy == "call_credit" else put_snapshots_by_expiration
    )
    _, delta_contract_count = count_snapshot_delta_coverage(option_snapshots_by_expiration)
    local_delta_contract_count = count_local_greeks_coverage(option_snapshots_by_expiration)

    all_candidates = build_credit_spreads(
        symbol=symbol,
        strategy=symbol_args.strategy,
        spot_price=spot_price,
        contracts_by_expiration=option_contracts_by_expiration,
        snapshots_by_expiration=option_snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=symbol_args,
    )
    all_candidates = attach_underlying_setup(all_candidates, setup_context)
    all_candidates = attach_calendar_decisions(
        symbol=symbol,
        strategy=symbol_args.strategy,
        underlying_type=underlying_type,
        candidates=all_candidates,
        resolver=calendar_resolver,
        calendar_policy=symbol_args.calendar_policy,
        refresh_calendar_events=symbol_args.refresh_calendar_events,
    )
    all_candidates = attach_data_quality(
        candidates=all_candidates,
        underlying_type=underlying_type,
        args=symbol_args,
    )
    all_candidates = attach_board_notes(all_candidates, symbol_args)
    all_candidates = rank_candidates(all_candidates, symbol_args)
    all_candidates = deduplicate_candidates(all_candidates, symbol_args.expand_duplicates)

    run_id = build_run_id(symbol, symbol_args.strategy, symbol_args.profile)
    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    history_store.save_run(
        run_id=run_id,
        generated_at=generated_at,
        symbol=symbol,
        strategy=symbol_args.strategy,
        session_label=getattr(symbol_args, "session_label", None),
        profile=symbol_args.profile,
        spot_price=spot_price,
        output_path="",
        filters=build_filter_payload(symbol_args),
        setup_status=None if setup_context is None else setup_context.status,
        setup_score=None if setup_context is None else setup_context.score,
        setup_payload=serialize_setup_context(setup_context),
        candidates=all_candidates,
    )

    return SymbolScanResult(
        symbol=symbol,
        underlying_type=underlying_type,
        spot_price=spot_price,
        args=symbol_args,
        setup=setup_context,
        candidates=all_candidates,
        run_id=run_id,
        quoted_contract_count=quoted_contract_count,
        alpaca_delta_contract_count=alpaca_delta_contract_count,
        delta_contract_count=delta_contract_count,
        local_delta_contract_count=local_delta_contract_count,
    )


def scan_symbol_across_strategies(
    *,
    symbol: str,
    base_args: argparse.Namespace,
    client: AlpacaClient,
    calendar_resolver: Any,
    greeks_provider: Any,
    history_store: RunHistoryRepository,
) -> tuple[list[SymbolScanResult], list[UniverseScanFailure]]:
    results: list[SymbolScanResult] = []
    failures: list[UniverseScanFailure] = []
    for strategy in concrete_strategies(base_args.strategy):
        strategy_args = clone_args(base_args)
        strategy_args.strategy = strategy
        try:
            results.append(
                scan_symbol_live(
                    symbol=symbol,
                    base_args=strategy_args,
                    client=client,
                    calendar_resolver=calendar_resolver,
                    greeks_provider=greeks_provider,
                    history_store=history_store,
                )
            )
        except Exception as exc:
            label = f"{symbol}:{strategy}" if base_args.strategy == "combined" else symbol
            failures.append(UniverseScanFailure(symbol=label, error=str(exc).splitlines()[0]))
    return results, failures


def merge_strategy_candidates(
    results: list[SymbolScanResult],
    *,
    per_strategy_top: int | None = None,
) -> list[SpreadCandidate]:
    merged: list[SpreadCandidate] = []
    for result in results:
        candidates = result.candidates if per_strategy_top is None else result.candidates[:per_strategy_top]
        merged.extend(candidates)
    return sort_candidates_for_display(merged)


def main() -> int:
    load_local_env()
    args = parse_args()

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    symbols, universe_label = resolve_symbols(args)

    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, args.trading_base_url),
        data_base_url=args.data_base_url,
    )
    history_store = build_history_store(args.history_db)
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=args.data_base_url,
        database_url=args.history_db,
    )
    greeks_provider = build_local_greeks_provider()

    if args.replay_latest or args.replay_run_id:
        try:
            return run_replay(args=args, client=client, history_store=history_store)
        finally:
            history_store.close()
            calendar_resolver.store.close()

    if len(symbols) == 1:
        strategy_results, failures = scan_symbol_across_strategies(
            symbol=symbols[0],
            base_args=args,
            client=client,
            calendar_resolver=calendar_resolver,
            greeks_provider=greeks_provider,
            history_store=history_store,
        )
        if failures and not strategy_results:
            raise SystemExit(failures[0].error)

        if args.strategy == "combined":
            combined_candidates = merge_strategy_candidates(strategy_results)
            primary_result = strategy_results[0]
            output_path = args.output or default_output_path(primary_result.symbol, args.strategy, args.output_format)
            if args.output_format == "csv":
                write_csv(output_path, combined_candidates)
            else:
                write_json(
                    output_path,
                    primary_result.symbol,
                    primary_result.spot_price,
                    args,
                    combined_candidates,
                )
            latest_copy = write_latest_copy(
                output_path,
                f"latest_{primary_result.symbol.lower()}_{args.strategy}.{args.output_format}",
            )
            candidates = combined_candidates[: args.top]
            setup_summaries = build_setup_summaries(strategy_results)
            if args.json:
                print(
                    json.dumps(
                        {
                            "symbol": primary_result.symbol,
                            "strategy": args.strategy,
                            "spot_price": primary_result.spot_price,
                            "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                            "filters": build_filter_payload(args),
                            "strategy_runs": [
                                {
                                    "strategy": result.args.strategy,
                                    "run_id": result.run_id,
                                    "setup": None
                                    if result.setup is None
                                    else {
                                        "status": result.setup.status,
                                        "score": result.setup.score,
                                        "reasons": list(result.setup.reasons),
                                    },
                                }
                                for result in strategy_results
                            ],
                            "failures": [asdict(failure) for failure in failures],
                            "candidates": [asdict(candidate) for candidate in candidates],
                            "output_file": output_path,
                        },
                        indent=2,
                    )
                )
            else:
                print_human_readable(
                    primary_result.symbol,
                    primary_result.spot_price,
                    candidates,
                    args.show_order_json,
                    None,
                    strategy=args.strategy,
                    profile=args.profile,
                    greeks_source=args.greeks_source,
                    setup_summaries=setup_summaries,
                )
                for strategy_result in strategy_results:
                    if strategy_result.args.profile == "0dte":
                        print(
                            f"0DTE coverage [{strategy_result.args.strategy}]: Alpaca returned quotes for "
                            f"{strategy_result.quoted_contract_count} contracts, Alpaca delta for "
                            f"{strategy_result.alpaca_delta_contract_count}, final usable delta for "
                            f"{strategy_result.delta_contract_count}, local Greeks for "
                            f"{strategy_result.local_delta_contract_count}."
                        )
                maybe_stream_live_quotes(args=args, client=client, candidates=candidates)
                if failures:
                    print("Failures:")
                    for failure in failures:
                        print(f"- {failure.symbol}: {failure.error}")
                print(f"Saved {len(combined_candidates)} candidates to {output_path}")
                print(f"Latest copy: {latest_copy}")
                print("Run ids:")
                for result in strategy_results:
                    print(f"- {result.args.strategy}: {result.run_id}")
        else:
            result = strategy_results[0]
            output_path = args.output or default_output_path(result.symbol, result.args.strategy, args.output_format)

            if args.output_format == "csv":
                write_csv(output_path, result.candidates)
            else:
                write_json(
                    output_path,
                    result.symbol,
                    result.spot_price,
                    result.args,
                    result.candidates,
                    run_id=result.run_id,
                    setup=result.setup,
                )
            latest_copy = write_latest_copy(
                output_path,
                f"latest_{result.symbol.lower()}_{result.args.strategy}.{args.output_format}",
            )

            candidates = result.candidates[: result.args.top]
            if args.json:
                print(
                    json.dumps(
                        {
                            "symbol": result.symbol,
                            "strategy": result.args.strategy,
                            "spot_price": result.spot_price,
                            "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                            "run_id": result.run_id,
                            "filters": build_filter_payload(result.args),
                            "setup": None
                            if result.setup is None
                            else {
                                "status": result.setup.status,
                                "score": result.setup.score,
                                "reasons": list(result.setup.reasons),
                            },
                            "candidates": [asdict(candidate) for candidate in candidates],
                            "output_file": output_path,
                        },
                        indent=2,
                    )
                )
            else:
                print_human_readable(
                    result.symbol,
                    result.spot_price,
                    candidates,
                    result.args.show_order_json,
                    result.setup,
                    strategy=result.args.strategy,
                    profile=result.args.profile,
                    greeks_source=result.args.greeks_source,
                )
                if result.args.profile == "0dte":
                    print(
                        f"0DTE coverage: Alpaca returned quotes for {result.quoted_contract_count} "
                        f"contracts, Alpaca delta for {result.alpaca_delta_contract_count}, final usable delta for "
                        f"{result.delta_contract_count}, local Greeks for "
                        f"{result.local_delta_contract_count}."
                    )
                maybe_stream_live_quotes(args=result.args, client=client, candidates=candidates)
                print(f"Saved {len(result.candidates)} candidates to {output_path}")
                print(f"Latest copy: {latest_copy}")
                print(f"Run id: {result.run_id}")
    else:
        scan_results: list[SymbolScanResult] = []
        failures: list[UniverseScanFailure] = []
        board_candidates: list[SpreadCandidate] = []

        for symbol in symbols:
            strategy_results, symbol_failures = scan_symbol_across_strategies(
                symbol=symbol,
                base_args=args,
                client=client,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                history_store=history_store,
            )
            failures.extend(symbol_failures)
            if not strategy_results:
                continue
            scan_results.extend(strategy_results)
            symbol_board_candidates = merge_strategy_candidates(
                strategy_results,
                per_strategy_top=args.per_symbol_top,
            )[: args.per_symbol_top]
            board_candidates.extend(symbol_board_candidates)

        board_candidates = sort_candidates_for_display(board_candidates)
        board_candidates = board_candidates[: args.top]
        output_path = args.output or default_universe_output_path(universe_label, args.strategy, args.output_format)

        if args.output_format == "csv":
            write_universe_csv(output_path, board_candidates)
        else:
            write_universe_json(
                output_path,
                label=universe_label,
                strategy=args.strategy,
                symbols=symbols,
                candidates=board_candidates,
                failures=failures,
            )
        latest_copy = write_latest_copy(
            output_path,
            f"latest_{universe_label.lower().replace(' ', '_')}_{args.strategy}.{args.output_format}",
        )

        if args.json:
            print(
                json.dumps(
                    {
                        "mode": "universe",
                        "label": universe_label,
                        "strategy": args.strategy,
                        "symbols": symbols,
                        "candidate_count": len(board_candidates),
                        "failures": [asdict(failure) for failure in failures],
                        "candidates": [asdict(candidate) for candidate in board_candidates],
                        "output_file": output_path,
                    },
                    indent=2,
                )
            )
        else:
            print_universe_board(
                label=universe_label,
                strategy=args.strategy,
                profile=args.profile,
                greeks_source=args.greeks_source,
                symbols=symbols,
                board_candidates=board_candidates,
                failures=failures,
            )
            maybe_stream_live_quotes(args=args, client=client, candidates=board_candidates)
            if scan_results:
                print(f"Stored per-symbol runs: {len(scan_results)}")
            print(f"Saved {len(board_candidates)} board entries to {output_path}")
            print(f"Latest copy: {latest_copy}")

    history_store.close()
    calendar_resolver.store.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
