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
import os
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


DEFAULT_DATA_BASE_URL = "https://data.alpaca.markets"
DEFAULT_TRADING_BASE_URL = "https://api.alpaca.markets"
NEW_YORK = ZoneInfo("America/New_York")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find call credit spread candidates for a single underlying using Alpaca."
    )
    parser.add_argument("--symbol", default="SPY", help="Underlying symbol. Default: SPY")
    parser.add_argument(
        "--min-dte",
        type=int,
        default=7,
        help="Minimum days to expiration to include. Default: 7",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=35,
        help="Maximum days to expiration to include. Default: 35",
    )
    parser.add_argument(
        "--short-delta-min",
        type=float,
        default=0.12,
        help="Minimum short-call delta. Default: 0.12",
    )
    parser.add_argument(
        "--short-delta-max",
        type=float,
        default=0.30,
        help="Maximum short-call delta. Default: 0.30",
    )
    parser.add_argument(
        "--max-width",
        type=float,
        default=5.0,
        help="Maximum strike width for the spread. Default: 5.0",
    )
    parser.add_argument(
        "--min-credit",
        type=float,
        default=0.25,
        help="Minimum midpoint credit per spread. Default: 0.25",
    )
    parser.add_argument(
        "--min-open-interest",
        type=int,
        default=200,
        help="Minimum open interest required on each leg. Default: 200",
    )
    parser.add_argument(
        "--max-relative-spread",
        type=float,
        default=0.25,
        help="Maximum bid/ask width as a fraction of midpoint for each leg. Default: 0.25",
    )
    parser.add_argument(
        "--min-return-on-risk",
        type=float,
        default=0.10,
        help="Minimum spread return on risk, e.g. 0.10 = 10%%. Default: 0.10",
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
class SpreadCandidate:
    expiration_date: str
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
    short_otm_pct: float
    short_open_interest: int
    long_open_interest: int
    order_payload: dict[str, Any]
    calendar_status: str = "clean"
    calendar_reasons: tuple[str, ...] = ()
    calendar_confidence: str = "unknown"
    calendar_sources: tuple[str, ...] = ()
    calendar_last_updated: str | None = None
    calendar_days_to_nearest_event: int | None = None
    macro_regime: str | None = None


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

    def list_option_contracts(self, symbol: str, min_expiration: str, max_expiration: str) -> list[OptionContract]:
        contracts: list[OptionContract] = []
        page_token: str | None = None
        while True:
            payload = self.get_json(
                self.trading_base_url,
                "/v2/options/contracts",
                {
                    "underlying_symbols": symbol,
                    "type": "call",
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

    def get_call_chain_snapshots(self, symbol: str, expiration_date: str, feed: str) -> dict[str, OptionSnapshot]:
        snapshots: dict[str, OptionSnapshot] = {}
        page_token: str | None = None
        while True:
            payload = self.get_json(
                self.data_base_url,
                f"/v1beta1/options/snapshots/{symbol}",
                {
                    "feed": feed,
                    "type": "call",
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
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    candidates: list[SpreadCandidate] = []

    for expiration_date, contracts in sorted(contracts_by_expiration.items()):
        snapshot_map = snapshots_by_expiration.get(expiration_date, {})
        sorted_contracts = sorted(contracts, key=lambda contract: contract.strike_price)

        for short_contract in sorted_contracts:
            short_snapshot = snapshot_map.get(short_contract.symbol)
            if not short_snapshot:
                continue
            if short_contract.strike_price <= spot_price:
                continue
            if short_contract.open_interest < args.min_open_interest:
                continue
            if relative_spread(short_snapshot) > args.max_relative_spread:
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
                if width > args.max_width:
                    break

                long_snapshot = snapshot_map.get(long_contract.symbol)
                if not long_snapshot:
                    continue
                if long_contract.open_interest < args.min_open_interest:
                    continue
                if relative_spread(long_snapshot) > args.max_relative_spread:
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
                candidates.append(
                    SpreadCandidate(
                        expiration_date=expiration_date,
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
                        short_otm_pct=(short_contract.strike_price - spot_price) / spot_price,
                        short_open_interest=short_contract.open_interest,
                        long_open_interest=long_contract.open_interest,
                        order_payload=make_order_payload(
                            short_contract.symbol,
                            long_contract.symbol,
                            midpoint_credit,
                        ),
                    )
                )

    candidates.sort(
        key=lambda candidate: (
            candidate.return_on_risk,
            candidate.midpoint_credit,
            min(candidate.short_open_interest, candidate.long_open_interest),
        ),
        reverse=True,
    )
    return candidates


def build_table_rows(candidates: list[SpreadCandidate]) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        rows.append(
            [
                candidate.expiration_date,
                f"{candidate.short_strike:.2f}",
                f"{candidate.long_strike:.2f}",
                f"{candidate.width:.2f}",
                f"{candidate.midpoint_credit:.2f}",
                f"{candidate.natural_credit:.2f}",
                f"{candidate.max_loss:.0f}",
                f"{candidate.return_on_risk * 100:.1f}",
                "n/a" if candidate.short_delta is None else f"{candidate.short_delta:.2f}",
                f"{candidate.short_otm_pct * 100:.1f}",
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


def print_human_readable(symbol: str, spot_price: float, candidates: list[SpreadCandidate], show_order_json: bool) -> None:
    print(f"{symbol.upper()} spot: {spot_price:.2f}")
    print(f"Candidates found: {len(candidates)}")
    print()

    if not candidates:
        print("No call credit spreads matched the current filters and calendar policy.")
        return

    headers = ["Expiry", "Short", "Long", "Width", "MidCr", "NatCr", "MaxLoss", "ROR%", "Δ", "OTM%", "MinOI", "Cal", "EvtD"]
    rows = build_table_rows(candidates)
    print(format_table(headers, rows))
    print()

    for index, candidate in enumerate(candidates, start=1):
        print(
            f"{index}. {candidate.short_symbol} -> {candidate.long_symbol} | "
            f"breakeven {candidate.breakeven:.2f} | "
            f"calendar {candidate.calendar_status}"
        )
        if candidate.calendar_reasons:
            print(f"   reasons: {'; '.join(candidate.calendar_reasons)}")
        if candidate.calendar_sources:
            source_line = ", ".join(candidate.calendar_sources)
            print(f"   sources: {source_line} | confidence {candidate.calendar_confidence}")
        if candidate.macro_regime:
            print(f"   macro regime: {candidate.macro_regime}")
        if show_order_json:
            print("   order payload:")
            print(json.dumps(candidate.order_payload, indent=2))
        print()


def write_csv(path: str, candidates: list[SpreadCandidate]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "expiration_date",
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
        "short_otm_pct",
        "short_open_interest",
        "long_open_interest",
        "calendar_status",
        "calendar_reasons",
        "calendar_confidence",
        "calendar_sources",
        "calendar_last_updated",
        "calendar_days_to_nearest_event",
        "macro_regime",
        "order_payload",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for candidate in candidates:
            row = asdict(candidate)
            row["calendar_reasons"] = "; ".join(candidate.calendar_reasons)
            row["calendar_sources"] = ", ".join(candidate.calendar_sources)
            row["order_payload"] = json.dumps(candidate.order_payload, separators=(",", ":"))
            writer.writerow(row)


def write_json(path: str, symbol: str, spot_price: float, args: argparse.Namespace, candidates: list[SpreadCandidate]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": symbol,
        "spot_price": spot_price,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "filters": {
            "min_dte": args.min_dte,
            "max_dte": args.max_dte,
            "short_delta_min": args.short_delta_min,
            "short_delta_max": args.short_delta_max,
            "max_width": args.max_width,
            "min_credit": args.min_credit,
            "min_open_interest": args.min_open_interest,
            "max_relative_spread": args.max_relative_spread,
            "min_return_on_risk": args.min_return_on_risk,
            "feed": args.feed,
            "stock_feed": args.stock_feed,
            "calendar_policy": args.calendar_policy,
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


def main() -> int:
    load_local_env()
    args = parse_args()

    if args.min_dte < 0 or args.max_dte < args.min_dte:
        raise SystemExit("Expected 0 <= min-dte <= max-dte")
    if args.short_delta_min < 0 or args.short_delta_max > 1 or args.short_delta_min > args.short_delta_max:
        raise SystemExit("Expected 0 <= short-delta-min <= short-delta-max <= 1")
    if args.max_width <= 0:
        raise SystemExit("Expected max-width > 0")
    if args.min_credit <= 0:
        raise SystemExit("Expected min-credit > 0")
    if args.min_open_interest < 0:
        raise SystemExit("Expected min-open-interest >= 0")
    if args.max_relative_spread <= 0:
        raise SystemExit("Expected max-relative-spread > 0")

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")

    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, args.trading_base_url),
        data_base_url=args.data_base_url,
    )
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=args.data_base_url,
    )

    symbol = args.symbol.upper()
    underlying_type = classify_underlying_type(symbol)
    min_expiration = (date.today() + timedelta(days=args.min_dte)).isoformat()
    max_expiration = (date.today() + timedelta(days=args.max_dte)).isoformat()

    spot_price = client.get_underlying_price(symbol, args.stock_feed)
    contracts = client.list_option_contracts(symbol, min_expiration, max_expiration)
    contracts_by_expiration = group_contracts_by_expiration(contracts)

    snapshots_by_expiration: dict[str, dict[str, OptionSnapshot]] = {}
    for expiration_date in sorted(contracts_by_expiration):
        snapshots_by_expiration[expiration_date] = client.get_call_chain_snapshots(
            symbol,
            expiration_date,
            args.feed,
        )

    all_candidates = build_call_credit_spreads(
        spot_price=spot_price,
        contracts_by_expiration=contracts_by_expiration,
        snapshots_by_expiration=snapshots_by_expiration,
        args=args,
    )
    all_candidates = attach_calendar_decisions(
        symbol=symbol,
        underlying_type=underlying_type,
        candidates=all_candidates,
        resolver=calendar_resolver,
        calendar_policy=args.calendar_policy,
        refresh_calendar_events=args.refresh_calendar_events,
    )
    output_path = args.output or default_output_path(symbol, args.output_format)

    if args.output_format == "csv":
        write_csv(output_path, all_candidates)
    else:
        write_json(output_path, symbol, spot_price, args, all_candidates)

    candidates = all_candidates[: args.top]

    if args.json:
        print(
            json.dumps(
                {
                    "symbol": symbol,
                    "spot_price": spot_price,
                    "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "filters": {
                        "min_dte": args.min_dte,
                        "max_dte": args.max_dte,
                        "short_delta_min": args.short_delta_min,
                        "short_delta_max": args.short_delta_max,
                        "max_width": args.max_width,
                        "min_credit": args.min_credit,
                        "min_open_interest": args.min_open_interest,
                        "max_relative_spread": args.max_relative_spread,
                        "min_return_on_risk": args.min_return_on_risk,
                        "feed": args.feed,
                        "stock_feed": args.stock_feed,
                        "calendar_policy": args.calendar_policy,
                    },
                    "candidates": [asdict(candidate) for candidate in candidates],
                    "output_file": output_path,
                },
                indent=2,
            )
        )
    else:
        print_human_readable(symbol, spot_price, candidates, args.show_order_json)
        print(f"Saved {len(all_candidates)} candidates to {output_path}")

    calendar_resolver.store.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
