#!/usr/bin/env python3
"""Scan Alpaca option chains for call credit spread candidates.

Usage:
    uv run call_credit_spread_scanner.py --symbol SPY

Required environment variables:
    APCA_API_KEY_ID
    APCA_API_SECRET_KEY

Notes:
    - Uses Alpaca's Trading API for option contract metadata.
    - Uses Alpaca's Market Data API for underlying price and option chain snapshots.
    - Ranks same-expiration bear call spreads using simple liquidity/risk filters.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from calendar_events import build_calendar_event_resolver, classify_underlying_type
from calendar_events.models import CalendarPolicyDecision
from calendar_events.policy import apply_call_credit_spread_policy
from scanner_history import DEFAULT_HISTORY_DB_PATH, RunHistoryStore


DEFAULT_DATA_BASE_URL = "https://data.alpaca.markets"
DEFAULT_TRADING_BASE_URL = "https://api.alpaca.markets"
NEW_YORK = ZoneInfo("America/New_York")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find call credit spread candidates for a single underlying using Alpaca."
    )
    parser.add_argument("--symbol", default="SPY", help="Underlying symbol. Default: SPY")
    parser.add_argument(
        "--profile",
        default="core",
        choices=("micro", "weekly", "swing", "core"),
        help="Scanner profile preset. Default: core",
    )
    parser.add_argument(
        "--min-dte",
        type=int,
        help="Minimum days to expiration to include. Default: profile preset",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        help="Maximum days to expiration to include. Default: profile preset",
    )
    parser.add_argument(
        "--short-delta-min",
        type=float,
        help="Minimum short-call delta. Default: profile preset",
    )
    parser.add_argument(
        "--short-delta-max",
        type=float,
        help="Maximum short-call delta. Default: profile preset",
    )
    parser.add_argument(
        "--short-delta-target",
        type=float,
        help="Preferred short-call delta used in ranking. Default: profile preset",
    )
    parser.add_argument(
        "--min-width",
        type=float,
        help="Minimum strike width for the spread. Default: profile preset",
    )
    parser.add_argument(
        "--max-width",
        type=float,
        help="Maximum strike width for the spread. Default: profile preset",
    )
    parser.add_argument(
        "--min-credit",
        type=float,
        help="Minimum midpoint credit per spread. Default: profile preset",
    )
    parser.add_argument(
        "--min-open-interest",
        type=int,
        help="Minimum open interest required on each leg. Default: profile preset",
    )
    parser.add_argument(
        "--max-relative-spread",
        type=float,
        help="Maximum bid/ask width as a fraction of midpoint for each leg. Default: profile preset",
    )
    parser.add_argument(
        "--min-return-on-risk",
        type=float,
        help="Minimum spread return on risk, e.g. 0.10 = 10%%. Default: profile preset",
    )
    parser.add_argument(
        "--feed",
        default="opra",
        choices=("opra", "indicative"),
        help="Options market data feed. Premium users should use opra. Default: opra",
    )
    parser.add_argument(
        "--stock-feed",
        default="sip",
        choices=("sip", "iex", "delayed_sip", "boats", "overnight"),
        help="Stock feed used to price the underlying. Default: sip",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of candidates to print. Default: 10",
    )
    parser.add_argument(
        "--trading-base-url",
        default=os.environ.get("ALPACA_TRADING_BASE_URL"),
        help="Trading API base URL. If omitted, the script infers paper vs live from the API key.",
    )
    parser.add_argument(
        "--data-base-url",
        default=os.environ.get("ALPACA_DATA_BASE_URL", DEFAULT_DATA_BASE_URL),
        help="Market Data API base URL. Default: https://data.alpaca.markets",
    )
    parser.add_argument(
        "--output",
        help="Output file path. Default: outputs/call_credit_spreads/<symbol>_<timestamp>.csv",
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
        "--calendar-policy",
        default="strict",
        choices=("strict", "warn", "off"),
        help="Calendar event handling mode. Default: strict",
    )
    parser.add_argument(
        "--refresh-calendar-events",
        action="store_true",
        help="Force-refresh calendar sources before scanning.",
    )
    parser.add_argument(
        "--expand-duplicates",
        action="store_true",
        help="Keep multiple spreads for the same short leg instead of collapsing to the top-ranked expression.",
    )
    parser.add_argument(
        "--setup-filter",
        default="on",
        choices=("on", "off"),
        help="Apply underlying setup analysis to the scan. Default: on",
    )
    parser.add_argument(
        "--history-db",
        default=str(DEFAULT_HISTORY_DB_PATH),
        help="SQLite path for run history and replay. Default: outputs/run_history/scanner_history.sqlite",
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
    return parser.parse_args()


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


@dataclass(frozen=True)
class DailyBar:
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int


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


@dataclass(frozen=True)
class ExpectedMoveEstimate:
    expiration_date: str
    amount: float
    percent_of_spot: float
    reference_strike: float
    method: str = "atm_straddle_midpoint"


@dataclass(frozen=True)
class UnderlyingSetupContext:
    status: str
    score: float
    reasons: tuple[str, ...]
    spot_vs_sma20_pct: float | None
    sma20_vs_sma50_pct: float | None
    return_5d_pct: float | None
    distance_to_20d_high_pct: float | None
    latest_close: float | None
    sma20: float | None
    sma50: float | None
    source_window_days: int


@dataclass(frozen=True)
class SpreadCandidate:
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


PROFILE_CONFIGS: dict[str, ProfileConfig] = {
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
        min_open_interest_by_underlying={"etf_index_proxy": 750, "single_name_equity": 400},
        max_relative_spread_by_underlying={"etf_index_proxy": 0.12, "single_name_equity": 0.15},
        min_return_on_risk=0.10,
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
    ),
}


class AlpacaClient:
    def __init__(
        self,
        *,
        key_id: str,
        secret_key: str,
        trading_base_url: str,
        data_base_url: str,
    ) -> None:
        self.trading_base_url = trading_base_url.rstrip("/")
        self.data_base_url = data_base_url.rstrip("/")
        self.headers = {
            "APCA-API-KEY-ID": key_id,
            "APCA-API-SECRET-KEY": secret_key,
            "Accept": "application/json",
            "User-Agent": "call-credit-spread-scanner/1.0",
        }

    def get_json(self, base_url: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = ""
        if params:
            filtered = {k: v for k, v in params.items() if v not in (None, "")}
            query = "?" + urllib.parse.urlencode(filtered)
        url = f"{base_url}{path}{query}"
        request = urllib.request.Request(url, headers=self.headers)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Alpaca request failed: {exc.code} {exc.reason} for {url}\n{body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Failed to reach Alpaca for {url}: {exc.reason}") from exc

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

        return OptionSnapshot(
            symbol=symbol,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            midpoint=midpoint,
            delta=parse_float(pick(greeks, "delta", "d")),
            gamma=parse_float(pick(greeks, "gamma", "g")),
            theta=parse_float(pick(greeks, "theta", "t")),
            vega=parse_float(pick(greeks, "vega", "v")),
            implied_volatility=parse_float(
                pick(snapshot, "impliedVolatility", "implied_volatility", "iv")
            ),
            last_trade_price=parse_float(pick(latest_trade, "p", "price")),
        )


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


def analyze_underlying_setup(symbol: str, spot_price: float, bars: list[DailyBar]) -> UnderlyingSetupContext:
    if len(bars) < 20:
        return UnderlyingSetupContext(
            status="unknown",
            score=0.0,
            reasons=("Not enough daily-bar history for setup analysis",),
            spot_vs_sma20_pct=None,
            sma20_vs_sma50_pct=None,
            return_5d_pct=None,
            distance_to_20d_high_pct=None,
            latest_close=bars[-1].close if bars else None,
            sma20=None,
            sma50=None,
            source_window_days=len(bars),
        )

    closes = [bar.close for bar in bars]
    highs = [bar.high for bar in bars]
    sma20 = average(closes[-20:])
    sma50 = average(closes[-50:]) if len(closes) >= 50 else None
    latest_close = closes[-1]
    return_5d_pct = None
    if len(closes) >= 6 and closes[-6] > 0:
        return_5d_pct = latest_close / closes[-6] - 1.0
    high_20 = max(highs[-20:])
    distance_to_20d_high_pct = (high_20 - spot_price) / spot_price if spot_price > 0 else None
    spot_vs_sma20_pct = ((spot_price - sma20) / sma20) if sma20 else None
    sma20_vs_sma50_pct = ((sma20 - sma50) / sma50) if sma20 and sma50 else None

    price_vs_sma20_score = 0.5 if spot_vs_sma20_pct is None else clamp(0.5 - (spot_vs_sma20_pct / 0.08))
    trend_score = 0.5 if sma20_vs_sma50_pct is None else clamp(0.5 - (sma20_vs_sma50_pct / 0.06))
    momentum_score = 0.5 if return_5d_pct is None else clamp(0.55 - (return_5d_pct / 0.08))
    high_distance_score = (
        0.5 if distance_to_20d_high_pct is None else clamp(distance_to_20d_high_pct / 0.04)
    )

    score = round(
        100.0
        * (
            0.35 * price_vs_sma20_score
            + 0.25 * trend_score
            + 0.20 * momentum_score
            + 0.20 * high_distance_score
        ),
        1,
    )

    reasons: list[str] = []
    if spot_vs_sma20_pct is not None:
        if spot_vs_sma20_pct > 0.02:
            reasons.append("Spot is extended above the 20-day average")
        elif spot_vs_sma20_pct < -0.01:
            reasons.append("Spot is trading below the 20-day average")
    if sma20_vs_sma50_pct is not None:
        if sma20_vs_sma50_pct > 0.015:
            reasons.append("20-day average is leading the 50-day average higher")
        elif sma20_vs_sma50_pct < -0.01:
            reasons.append("20-day average is below the 50-day average")
    if return_5d_pct is not None:
        if return_5d_pct > 0.03:
            reasons.append("Recent 5-day momentum is strongly positive")
        elif return_5d_pct < -0.02:
            reasons.append("Recent 5-day momentum is weak to negative")
    if distance_to_20d_high_pct is not None:
        if distance_to_20d_high_pct < 0.01:
            reasons.append("Spot is trading near the 20-day high")
        elif distance_to_20d_high_pct > 0.03:
            reasons.append("Spot has room below the recent 20-day high")

    if score >= 60:
        status = "favorable"
    elif score >= 40:
        status = "neutral"
    else:
        status = "unfavorable"

    if not reasons:
        reasons.append(f"{symbol} setup is {status} for bearish or neutral premium selling")

    return UnderlyingSetupContext(
        status=status,
        score=score,
        reasons=tuple(reasons),
        spot_vs_sma20_pct=spot_vs_sma20_pct,
        sma20_vs_sma50_pct=sma20_vs_sma50_pct,
        return_5d_pct=return_5d_pct,
        distance_to_20d_high_pct=distance_to_20d_high_pct,
        latest_close=latest_close,
        sma20=sma20,
        sma50=sma50,
        source_window_days=len(bars),
    )


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


def build_run_id(symbol: str, profile: str) -> str:
    return f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}_{symbol.lower()}_{profile}"


def build_filter_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "profile": args.profile,
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
    }


def make_order_payload(short_symbol: str, long_symbol: str, limit_price: float) -> dict[str, Any]:
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


def infer_trading_base_url(key_id: str, explicit_base_url: str | None) -> str:
    if explicit_base_url:
        return explicit_base_url.rstrip("/")
    if key_id.startswith("PK"):
        return "https://paper-api.alpaca.markets"
    return DEFAULT_TRADING_BASE_URL


def default_output_path(symbol: str, output_format: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return str(Path("outputs") / "call_credit_spreads" / f"{symbol.lower()}_{timestamp}.{output_format}")


def option_expiry_close(expiration_date: str) -> datetime:
    local_close = datetime.combine(date.fromisoformat(expiration_date), time(16, 0), tzinfo=NEW_YORK)
    return local_close.astimezone(UTC)


def build_call_credit_spreads(
    *,
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

        for short_contract in sorted_contracts:
            short_snapshot = snapshot_map.get(short_contract.symbol)
            if not short_snapshot:
                continue
            if short_contract.strike_price <= spot_price:
                continue
            if short_contract.open_interest < args.min_open_interest:
                continue
            short_leg_relative_spread = relative_spread(short_snapshot)
            if short_leg_relative_spread > args.max_relative_spread:
                continue
            if short_snapshot.bid_size <= 0:
                continue
            if short_snapshot.delta is None:
                continue
            if not (args.short_delta_min <= short_snapshot.delta <= args.short_delta_max):
                continue

            for long_contract in sorted_contracts:
                if long_contract.strike_price <= short_contract.strike_price:
                    continue

                width = long_contract.strike_price - short_contract.strike_price
                if width < args.min_width:
                    continue
                if width > args.max_width:
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

                midpoint_credit = short_snapshot.midpoint - long_snapshot.midpoint
                natural_credit = short_snapshot.bid - long_snapshot.ask
                if midpoint_credit < args.min_credit:
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

                breakeven = short_contract.strike_price + midpoint_credit
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
                    expected_move_ceiling = spot_price + expected_move.amount
                    short_vs_expected_move = short_contract.strike_price - expected_move_ceiling
                    breakeven_vs_expected_move = breakeven - expected_move_ceiling

                candidates.append(
                    SpreadCandidate(
                        profile=args.profile,
                        expiration_date=expiration_date,
                        days_to_expiration=days_to_expiration,
                        underlying_price=spot_price,
                        short_symbol=short_contract.symbol,
                        long_symbol=long_contract.symbol,
                        short_strike=short_contract.strike_price,
                        long_strike=long_contract.strike_price,
                        width=width,
                        short_delta=short_snapshot.delta,
                        long_delta=long_snapshot.delta,
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
                        breakeven_cushion_pct=(breakeven - spot_price) / spot_price,
                        short_otm_pct=(short_contract.strike_price - spot_price) / spot_price,
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
    delta_target = args.short_delta_target
    delta_half_band = max((args.short_delta_max - args.short_delta_min) / 2.0, 0.01)
    delta_score = 1.0
    if candidate.short_delta is not None:
        delta_score = 1.0 - min(abs(candidate.short_delta - delta_target) / delta_half_band, 1.0)

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
    return round(base_score * calendar_multiplier * setup_multiplier * 100.0, 1)


def rank_candidates(candidates: list[SpreadCandidate], args: argparse.Namespace) -> list[SpreadCandidate]:
    ranked = [replace(candidate, quality_score=score_candidate(candidate, args)) for candidate in candidates]
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


def build_table_rows(candidates: list[SpreadCandidate]) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        rows.append(
            [
                candidate.expiration_date,
                str(candidate.days_to_expiration),
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
                "n/a"
                if candidate.calendar_days_to_nearest_event is None
                else str(candidate.calendar_days_to_nearest_event),
            ]
        )
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
) -> None:
    print(f"{symbol.upper()} spot: {spot_price:.2f}")
    if candidates:
        print(f"Profile: {candidates[0].profile}")
    if setup is not None:
        print(f"Setup: {setup.status} ({setup.score:.1f})")
        if setup.reasons:
            print(f"Setup notes: {'; '.join(setup.reasons)}")
    print(f"Candidates found: {len(candidates)}")
    print()

    if not candidates:
        print("No call credit spreads matched the current filters and calendar policy.")
        return

    headers = ["Expiry", "DTE", "Short", "Long", "Width", "MidCr", "ROR%", "Score", "Δ", "OTM%", "BE%", "S-EM", "MinOI", "Cal", "EvtD"]
    rows = build_table_rows(candidates)
    print(format_table(headers, rows))
    print()

    for index, candidate in enumerate(candidates, start=1):
        print(
            f"{index}. {candidate.short_symbol} -> {candidate.long_symbol} | "
            f"score {candidate.quality_score:.1f} | "
            f"breakeven {candidate.breakeven:.2f} | "
            f"calendar {candidate.calendar_status}"
        )
        if candidate.expected_move is not None:
            print(
                "   expected move: "
                f"{candidate.expected_move:.2f} ({candidate.expected_move_pct * 100:.2f}% of spot) "
                f"from {candidate.expected_move_source_strike:.2f} strike"
            )
        if candidate.calendar_reasons:
            print(f"   reasons: {'; '.join(candidate.calendar_reasons)}")
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
            row["order_payload"] = json.dumps(candidate.order_payload, separators=(",", ":"))
            writer.writerow(row)


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
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "run_id": run_id,
        "filters": build_filter_payload(args),
        "setup": None
        if setup is None
        else {
            "status": setup.status,
            "score": setup.score,
            "reasons": list(setup.reasons),
            "spot_vs_sma20_pct": setup.spot_vs_sma20_pct,
            "sma20_vs_sma50_pct": setup.sma20_vs_sma50_pct,
            "return_5d_pct": setup.return_5d_pct,
            "distance_to_20d_high_pct": setup.distance_to_20d_high_pct,
        },
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def build_calendar_reason_messages(decision: CalendarPolicyDecision) -> tuple[str, ...]:
    return tuple(reason.message for reason in decision.reasons)


def attach_calendar_decisions(
    *,
    symbol: str,
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
            strategy="call_credit_spread",
            window_start=window_start,
            window_end=option_expiry_close(expiration_date).isoformat(),
            underlying_type=underlying_type,
            refresh=refresh_calendar_events,
        )
        decisions_by_expiration[expiration_date] = apply_call_credit_spread_policy(
            context,
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


def summarize_replay(
    *,
    run_payload: dict[str, Any],
    candidates: list[dict[str, Any]],
    bars: list[DailyBar],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    generated_at = datetime.fromisoformat(run_payload["generated_at"].replace("Z", "+00:00"))
    run_date = generated_at.date()
    latest_available_date = None if not bars else max(
        datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date() for bar in bars
    )
    horizons = [
        ("1d", run_date + timedelta(days=1)),
        ("3d", run_date + timedelta(days=3)),
    ]

    summaries: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for label, target_date in horizons:
        horizon_bars = bars_through_date(bars, target_date)
        available_candidates = 0
        touched = 0
        closed_over_short = 0
        closed_over_breakeven = 0

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
            available_candidates += 1
            path_bars = horizon_bars
            max_high = max(bar.high for bar in path_bars) if path_bars else horizon_bar.high
            touched_short = max_high >= candidate["short_strike"]
            closed_above_short = horizon_bar.close >= candidate["short_strike"]
            closed_above_breakeven = horizon_bar.close >= candidate["breakeven"]
            touched += int(touched_short)
            closed_over_short += int(closed_above_short)
            closed_over_breakeven += int(closed_above_breakeven)
            rows.append(
                {
                    "horizon": label,
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "available",
                    "spot_at_horizon": horizon_bar.close,
                    "max_high_to_horizon": max_high,
                    "touched_short_strike": touched_short,
                    "closed_above_short_strike": closed_above_short,
                    "closed_above_breakeven": closed_above_breakeven,
                }
            )

        total = len(candidates)
        summaries.append(
            {
                "horizon": label,
                "available": available_candidates,
                "pending": total - available_candidates,
                "touch_pct": None if available_candidates == 0 else 100.0 * touched / available_candidates,
                "close_over_short_pct": None
                if available_candidates == 0
                else 100.0 * closed_over_short / available_candidates,
                "close_over_breakeven_pct": None
                if available_candidates == 0
                else 100.0 * closed_over_breakeven / available_candidates,
            }
        )

    expiry_available = 0
    expiry_touched = 0
    expiry_closed_over_short = 0
    expiry_closed_over_breakeven = 0
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
        expiry_available += 1
        path_bars = bars_through_date(bars, expiry_date)
        max_high = max(bar.high for bar in path_bars) if path_bars else horizon_bar.high
        touched_short = max_high >= candidate["short_strike"]
        closed_above_short = horizon_bar.close >= candidate["short_strike"]
        closed_above_breakeven = horizon_bar.close >= candidate["breakeven"]
        expiry_touched += int(touched_short)
        expiry_closed_over_short += int(closed_above_short)
        expiry_closed_over_breakeven += int(closed_above_breakeven)
        rows.append(
            {
                "horizon": "expiry",
                "short_symbol": candidate["short_symbol"],
                "long_symbol": candidate["long_symbol"],
                "expiration_date": candidate["expiration_date"],
                "status": "available",
                "spot_at_horizon": horizon_bar.close,
                "max_high_to_horizon": max_high,
                "touched_short_strike": touched_short,
                "closed_above_short_strike": closed_above_short,
                "closed_above_breakeven": closed_above_breakeven,
            }
        )

    total = len(candidates)
    summaries.append(
        {
            "horizon": "expiry",
            "available": expiry_available,
            "pending": total - expiry_available,
            "touch_pct": None if expiry_available == 0 else 100.0 * expiry_touched / expiry_available,
            "close_over_short_pct": None
            if expiry_available == 0
            else 100.0 * expiry_closed_over_short / expiry_available,
            "close_over_breakeven_pct": None
            if expiry_available == 0
            else 100.0 * expiry_closed_over_breakeven / expiry_available,
        }
    )

    return summaries, rows


def print_replay_summary(
    run_payload: dict[str, Any],
    summaries: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    print(
        f"Replay run: {run_payload['run_id']} | {run_payload['symbol']} | "
        f"profile {run_payload['profile']} | generated {run_payload['generated_at']}"
    )
    print(f"Stored candidates: {run_payload['candidate_count']}")
    print()

    table_headers = ["Horizon", "Avail", "Pending", "Touch%", "Close>Short%", "Close>BE%"]
    table_rows = []
    for summary in summaries:
        table_rows.append(
            [
                summary["horizon"],
                str(summary["available"]),
                str(summary["pending"]),
                "n/a" if summary["touch_pct"] is None else f"{summary['touch_pct']:.1f}",
                "n/a" if summary["close_over_short_pct"] is None else f"{summary['close_over_short_pct']:.1f}",
                "n/a"
                if summary["close_over_breakeven_pct"] is None
                else f"{summary['close_over_breakeven_pct']:.1f}",
            ]
        )
    print(format_table(table_headers, table_rows))
    print()

    available_rows = [row for row in rows if row["status"] == "available"][:10]
    if available_rows:
        detail_headers = ["Horizon", "Short", "Long", "Expiry", "Spot", "MaxHigh", "Touch", "Close>Short", "Close>BE"]
        detail_rows = [
            [
                row["horizon"],
                row["short_symbol"],
                row["long_symbol"],
                row["expiration_date"],
                f"{row['spot_at_horizon']:.2f}",
                f"{row['max_high_to_horizon']:.2f}",
                "yes" if row["touched_short_strike"] else "no",
                "yes" if row["closed_above_short_strike"] else "no",
                "yes" if row["closed_above_breakeven"] else "no",
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
    history_store: RunHistoryStore,
) -> int:
    if args.replay_run_id:
        run_payload = history_store.get_run(args.replay_run_id)
    else:
        run_payload = history_store.get_latest_run(args.symbol.upper())

    if not run_payload:
        target = args.replay_run_id or args.symbol.upper()
        raise SystemExit(f"No stored run found for replay target: {target}")

    candidates = history_store.list_candidates(run_payload["run_id"])
    generated_at = datetime.fromisoformat(run_payload["generated_at"].replace("Z", "+00:00"))
    replay_end = max(
        [
            generated_at.date() + timedelta(days=3),
            *[date.fromisoformat(candidate["expiration_date"]) for candidate in candidates],
        ]
    )
    bars = client.get_daily_bars(
        run_payload["symbol"],
        start=(generated_at.date() - timedelta(days=2)).isoformat(),
        end=replay_end.isoformat(),
        stock_feed=args.stock_feed,
    )
    summaries, rows = summarize_replay(run_payload=run_payload, candidates=candidates, bars=bars)
    print_replay_summary(run_payload, summaries, rows)
    return 0


def main() -> int:
    load_local_env()
    args = parse_args()

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")

    symbol = args.symbol.upper()
    underlying_type = classify_underlying_type(symbol)
    apply_profile_defaults(args, underlying_type)

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

    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, args.trading_base_url),
        data_base_url=args.data_base_url,
    )
    history_store = RunHistoryStore(args.history_db)
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=args.data_base_url,
    )

    if args.replay_latest or args.replay_run_id:
        try:
            return run_replay(args=args, client=client, history_store=history_store)
        finally:
            history_store.close()
            calendar_resolver.store.close()

    min_expiration = (date.today() + timedelta(days=args.min_dte)).isoformat()
    max_expiration = (date.today() + timedelta(days=args.max_dte)).isoformat()

    spot_price = client.get_underlying_price(symbol, args.stock_feed)
    setup_context: UnderlyingSetupContext | None = None
    if args.setup_filter == "on":
        daily_bars = client.get_daily_bars(
            symbol,
            start=(date.today() - timedelta(days=120)).isoformat(),
            end=date.today().isoformat(),
            stock_feed=args.stock_feed,
        )
        setup_context = analyze_underlying_setup(symbol, spot_price, daily_bars)

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
            args.feed,
        )
        put_snapshots_by_expiration[expiration_date] = client.get_option_chain_snapshots(
            symbol,
            expiration_date,
            "put",
            args.feed,
        )

    expected_moves_by_expiration = build_expected_move_estimates(
        spot_price=spot_price,
        call_contracts_by_expiration=contracts_by_expiration,
        put_contracts_by_expiration=put_contracts_by_expiration,
        call_snapshots_by_expiration=call_snapshots_by_expiration,
        put_snapshots_by_expiration=put_snapshots_by_expiration,
    )

    all_candidates = build_call_credit_spreads(
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=call_snapshots_by_expiration,
        expected_moves_by_expiration=expected_moves_by_expiration,
        args=args,
    )
    all_candidates = attach_underlying_setup(all_candidates, setup_context)
    all_candidates = attach_calendar_decisions(
        symbol=symbol,
        underlying_type=underlying_type,
        candidates=all_candidates,
        resolver=calendar_resolver,
        calendar_policy=args.calendar_policy,
        refresh_calendar_events=args.refresh_calendar_events,
    )
    all_candidates = rank_candidates(all_candidates, args)
    all_candidates = deduplicate_candidates(all_candidates, args.expand_duplicates)
    output_path = args.output or default_output_path(symbol, args.output_format)
    run_id = build_run_id(symbol, args.profile)
    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    if args.output_format == "csv":
        write_csv(output_path, all_candidates)
    else:
        write_json(output_path, symbol, spot_price, args, all_candidates, run_id=run_id, setup=setup_context)

    history_store.save_run(
        run_id=run_id,
        generated_at=generated_at,
        symbol=symbol,
        profile=args.profile,
        spot_price=spot_price,
        output_path=output_path,
        filters=build_filter_payload(args),
        setup_status=None if setup_context is None else setup_context.status,
        setup_score=None if setup_context is None else setup_context.score,
        candidates=all_candidates,
    )

    candidates = all_candidates[: args.top]

    if args.json:
        print(
            json.dumps(
                {
                    "symbol": symbol,
                    "spot_price": spot_price,
                    "generated_at": generated_at,
                    "run_id": run_id,
                    "filters": build_filter_payload(args),
                    "setup": None
                    if setup_context is None
                    else {
                        "status": setup_context.status,
                        "score": setup_context.score,
                        "reasons": list(setup_context.reasons),
                    },
                    "candidates": [asdict(candidate) for candidate in candidates],
                    "output_file": output_path,
                },
                indent=2,
            )
        )
    else:
        print_human_readable(symbol, spot_price, candidates, args.show_order_json, setup_context)
        print(f"Saved {len(all_candidates)} candidates to {output_path}")
        print(f"Run id: {run_id}")

    history_store.close()
    calendar_resolver.store.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
