from __future__ import annotations

import argparse
import json
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.services.alpaca import create_alpaca_client_from_env

NEW_YORK = ZoneInfo("America/New_York")
MARKET_DATA_OPENAPI_URL = "https://docs.alpaca.markets/openapi/market-data-api.json"
TRADING_OPENAPI_URL = "https://docs.alpaca.markets/openapi/trading-api.json"


@dataclass
class ProbeResult:
    category: str
    method: str
    path: str
    role: str
    status: str
    docs_confirmed: bool
    sample: dict[str, Any] | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class StreamingSurface:
    path: str
    channels: list[str]
    role: str
    status: str
    notes: list[str] = field(default_factory=list)


@dataclass
class ResearchFeature:
    name: str
    effort: str
    value: str
    apis: list[str]
    unlocks: str
    why_it_fits: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe Alpaca research-relevant APIs and summarize feature opportunities."
    )
    parser.add_argument(
        "--symbol",
        default="SPY",
        help="Primary underlying symbol to use for live probes. Default: SPY",
    )
    parser.add_argument(
        "--option-type",
        default="call",
        choices=("call", "put"),
        help="Option side used when selecting a sample contract. Default: call",
    )
    parser.add_argument(
        "--output-format",
        default="markdown",
        choices=("markdown", "json"),
        help="Render the report as markdown or JSON. Default: markdown",
    )
    parser.add_argument(
        "--output",
        help="Optional file path for the rendered report.",
    )
    parser.add_argument(
        "--skip-account",
        action="store_true",
        help="Skip account/execution research surfaces.",
    )
    return parser.parse_args(argv)


def _fetch_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "alpaca-research/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected JSON shape from {url}")
    return payload


def _market_date() -> datetime:
    return datetime.now(NEW_YORK)


def _iso_day(offset_days: int = 0) -> str:
    return (_market_date().date() + timedelta(days=offset_days)).isoformat()


def _iso_timestamp(offset_days: int = 0, hour: int = 0, minute: int = 0, second: int = 0) -> str:
    base = _market_date() + timedelta(days=offset_days)
    stamp = base.replace(hour=hour, minute=minute, second=second, microsecond=0)
    return stamp.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")


def _stringify_sample(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(key): _stringify_scalar(item) for key, item in value.items()}
    return {"value": _stringify_scalar(value)}


def _stringify_scalar(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(key): _stringify_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_stringify_scalar(item) for item in value[:5]]
    return str(value)


def _choose_option_contract(client: Any, symbol: str, option_type: str) -> dict[str, Any] | None:
    contracts_payload = client.get_json(
        client.trading_base_url,
        "/v2/options/contracts",
        {
            "underlying_symbols": symbol,
            "type": option_type,
            "status": "active",
            "expiration_date_gte": _iso_day(0),
            "expiration_date_lte": _iso_day(14),
            "limit": 200,
        },
    )
    raw_contracts = contracts_payload.get("option_contracts", [])
    contracts = [item for item in raw_contracts if isinstance(item, dict) and item.get("symbol") and item.get("expiration_date")]
    if not contracts:
        return None

    expirations = []
    for item in contracts:
        expiration = str(item["expiration_date"])
        if expiration not in expirations:
            expirations.append(expiration)

    for expiration in expirations[:3]:
        snapshots_payload = client.get_json(
            client.data_base_url,
            f"/v1beta1/options/snapshots/{symbol}",
            {
                "feed": "opra",
                "type": option_type,
                "expiration_date": expiration,
                "limit": 200,
            },
        )
        snapshots = snapshots_payload.get("snapshots", {})
        if not isinstance(snapshots, dict):
            continue
        for contract_symbol, snapshot in snapshots.items():
            if not isinstance(snapshot, dict):
                continue
            latest_trade = snapshot.get("latestTrade")
            latest_quote = snapshot.get("latestQuote")
            if latest_trade or latest_quote:
                return {
                    "symbol": str(contract_symbol),
                    "expiration_date": expiration,
                }

    first = contracts[0]
    return {
        "symbol": str(first["symbol"]),
        "expiration_date": str(first["expiration_date"]),
    }


def _probe(
    *,
    client: Any,
    category: str,
    base_url: str,
    path: str,
    params: dict[str, Any] | None,
    role: str,
    docs_confirmed: bool,
    summarizer: Any,
    notes: list[str] | None = None,
) -> ProbeResult:
    try:
        payload = client.get_json(base_url, path, params)
        summary = summarizer(payload)
        return ProbeResult(
            category=category,
            method="GET",
            path=path,
            role=role,
            status="live-confirmed",
            docs_confirmed=docs_confirmed,
            sample=summary,
            notes=list(notes or []),
        )
    except Exception as exc:
        return ProbeResult(
            category=category,
            method="GET",
            path=path,
            role=role,
            status="probe-failed",
            docs_confirmed=docs_confirmed,
            sample=None,
            notes=[*(notes or []), str(exc)],
        )


def _summarize_most_actives(payload: dict[str, Any]) -> dict[str, Any]:
    items = payload.get("most_actives", [])
    sample = items[0] if isinstance(items, list) and items else None
    return {"count": 0 if not isinstance(items, list) else len(items), "sample": _stringify_scalar(sample)}


def _summarize_movers(payload: dict[str, Any]) -> dict[str, Any]:
    gainers = payload.get("gainers", [])
    losers = payload.get("losers", [])
    return {
        "gainers": 0 if not isinstance(gainers, list) else len(gainers),
        "losers": 0 if not isinstance(losers, list) else len(losers),
        "sample_gainer": _stringify_scalar(gainers[0] if isinstance(gainers, list) and gainers else None),
        "sample_loser": _stringify_scalar(losers[0] if isinstance(losers, list) and losers else None),
    }


def _summarize_stock_snapshot(payload: dict[str, Any], symbol: str) -> dict[str, Any]:
    snapshot = payload.get(symbol) or payload.get("snapshots", {}).get(symbol) or {}
    if not isinstance(snapshot, dict):
        snapshot = {}
    return {
        "latest_trade": _stringify_scalar(snapshot.get("latestTrade", {}).get("p")),
        "latest_quote_bid": _stringify_scalar(snapshot.get("latestQuote", {}).get("bp")),
        "latest_quote_ask": _stringify_scalar(snapshot.get("latestQuote", {}).get("ap")),
        "minute_bar_close": _stringify_scalar(snapshot.get("minuteBar", {}).get("c")),
        "daily_bar_close": _stringify_scalar(snapshot.get("dailyBar", {}).get("c")),
    }


def _summarize_symbol_map(payload: dict[str, Any], key: str, symbol: str, fields: list[str]) -> dict[str, Any]:
    container = payload.get(key, {})
    if not isinstance(container, dict):
        return {}
    sample = container.get(symbol) or {}
    if not isinstance(sample, dict):
        return {}
    return {field: _stringify_scalar(sample.get(field)) for field in fields}


def _summarize_symbol_series(payload: dict[str, Any], key: str, symbol: str, fields: list[str]) -> dict[str, Any]:
    container = payload.get(key, {})
    if not isinstance(container, dict):
        return {}
    items = container.get(symbol, [])
    if not isinstance(items, list) or not items:
        return {"count": 0}
    sample = items[0] if isinstance(items[0], dict) else {}
    return {"count": len(items), "sample": {field: _stringify_scalar(sample.get(field)) for field in fields}}


def _summarize_chain_snapshots(payload: dict[str, Any]) -> dict[str, Any]:
    snapshots = payload.get("snapshots", {})
    if not isinstance(snapshots, dict) or not snapshots:
        return {"count": 0}
    key, value = next(iter(snapshots.items()))
    if not isinstance(value, dict):
        value = {}
    return {
        "count": len(snapshots),
        "sample_symbol": key,
        "sample_bid": _stringify_scalar(value.get("latestQuote", {}).get("bp")),
        "sample_ask": _stringify_scalar(value.get("latestQuote", {}).get("ap")),
        "sample_trade": _stringify_scalar(value.get("latestTrade", {}).get("p")),
        "sample_iv": _stringify_scalar(value.get("impliedVolatility")),
    }


def _summarize_option_contracts(payload: dict[str, Any]) -> dict[str, Any]:
    contracts = payload.get("option_contracts", [])
    sample = contracts[0] if isinstance(contracts, list) and contracts else {}
    if not isinstance(sample, dict):
        sample = {}
    return {
        "count": 0 if not isinstance(contracts, list) else len(contracts),
        "sample_symbol": _stringify_scalar(sample.get("symbol")),
        "sample_expiration": _stringify_scalar(sample.get("expiration_date")),
        "sample_strike": _stringify_scalar(sample.get("strike_price")),
        "sample_open_interest": _stringify_scalar(sample.get("open_interest")),
    }


def _summarize_map_entries(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {"count": 0}
    key, value = next(iter(payload.items()))
    return {"count": len(payload), "sample_key": _stringify_scalar(key), "sample_value": _stringify_scalar(value)}


def _summarize_assets(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, list) or not payload:
        return {"count": 0}
    sample = payload[0]
    if not isinstance(sample, dict):
        return {"count": len(payload)}
    return {
        "count": len(payload),
        "sample_symbol": _stringify_scalar(sample.get("symbol")),
        "sample_status": _stringify_scalar(sample.get("status")),
        "sample_tradable": _stringify_scalar(sample.get("tradable")),
        "sample_attributes": _stringify_scalar(sample.get("attributes")),
    }


def _summarize_news(payload: dict[str, Any]) -> dict[str, Any]:
    items = payload.get("news", [])
    sample = items[0] if isinstance(items, list) and items else {}
    if not isinstance(sample, dict):
        sample = {}
    return {
        "count": 0 if not isinstance(items, list) else len(items),
        "sample_headline": _stringify_scalar(sample.get("headline")),
        "sample_created_at": _stringify_scalar(sample.get("created_at")),
        "sample_source": _stringify_scalar(sample.get("source")),
        "sample_symbols": _stringify_scalar(sample.get("symbols")),
    }


def _summarize_auctions(payload: dict[str, Any], symbol: str) -> dict[str, Any]:
    auctions = payload.get("auctions", {})
    if not isinstance(auctions, dict):
        return {"count": 0}
    items = auctions.get(symbol, [])
    if not isinstance(items, list) or not items:
        return {"count": 0}
    sample = items[0] if isinstance(items[0], dict) else {}
    return {
        "count": len(items),
        "sample_date": _stringify_scalar(sample.get("d")),
        "sample_open": _stringify_scalar((sample.get("o") or [None])[0]),
        "sample_close": _stringify_scalar((sample.get("c") or [None])[0]),
    }


def _summarize_corporate_actions(payload: dict[str, Any]) -> dict[str, Any]:
    grouped = payload.get("corporate_actions", {})
    if not isinstance(grouped, dict) or not grouped:
        return {"group_count": 0}
    group_name, items = next(iter(grouped.items()))
    sample = items[0] if isinstance(items, list) and items else None
    return {
        "group_count": len(grouped),
        "sample_group": _stringify_scalar(group_name),
        "sample": _stringify_scalar(sample),
    }


def _summarize_account(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": _stringify_scalar(payload.get("status")),
        "portfolio_value": _stringify_scalar(payload.get("portfolio_value")),
        "options_trading_level": _stringify_scalar(payload.get("options_trading_level")),
        "buying_power": _stringify_scalar(payload.get("buying_power")),
    }


def _summarize_positions(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, list):
        return {"count": 0}
    sample = payload[0] if payload else None
    return {
        "count": len(payload),
        "sample": _stringify_scalar(sample),
    }


def _summarize_activities(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, list):
        return {"count": 0}
    sample = payload[0] if payload else None
    return {
        "count": len(payload),
        "sample": _stringify_scalar(sample),
    }


def _summarize_portfolio_history(payload: dict[str, Any]) -> dict[str, Any]:
    timestamps = payload.get("timestamp", [])
    equity = payload.get("equity", [])
    last_equity = equity[-1] if isinstance(equity, list) and equity else None
    return {
        "points": 0 if not isinstance(timestamps, list) else len(timestamps),
        "last_equity": _stringify_scalar(last_equity),
    }


def _load_openapi_paths() -> tuple[set[str], set[str]]:
    market = _fetch_json(MARKET_DATA_OPENAPI_URL)
    trading = _fetch_json(TRADING_OPENAPI_URL)
    market_paths = set((market.get("paths") or {}).keys())
    trading_paths = set((trading.get("paths") or {}).keys())
    return market_paths, trading_paths


def _build_features() -> list[ResearchFeature]:
    return [
        ResearchFeature(
            name="Catalyst Reaction Tracker",
            effort="medium",
            value="high",
            apis=[
                "/v1beta1/news",
                "/v2/stocks/bars",
                "/v2/stocks/snapshots",
                "/v1beta1/options/snapshots/{underlying_symbol}",
                "/v1beta1/options/trades",
            ],
            unlocks="Measures how headlines propagate through stock price, liquidity, and option-chain behavior over multiple windows.",
            why_it_fits="Alpaca is strong at the stock-plus-news-plus-option-enrichment combination even though it is not a depth-of-book vendor.",
        ),
        ResearchFeature(
            name="Opening Drive / Closing Auction Board",
            effort="medium",
            value="high",
            apis=[
                "/v1beta1/screener/stocks/movers",
                "/v2/stocks/auctions",
                "/v2/stocks/bars",
                "/v2/stocks/snapshots",
            ],
            unlocks="Ranks names by opening participation, auction behavior, and follow-through into the session.",
            why_it_fits="Alpaca has direct stock-auction history plus strong stock bar and snapshot coverage.",
        ),
        ResearchFeature(
            name="Market Leadership / Rotation Board",
            effort="easy",
            value="high",
            apis=[
                "/v1beta1/screener/stocks/most-actives",
                "/v1beta1/screener/stocks/movers",
                "/v2/stocks/bars",
                "/v2/stocks/snapshots",
                "/v1beta1/news",
            ],
            unlocks="Shows what is truly leading or lagging across ETFs, sectors, and single names.",
            why_it_fits="This is one of the cleanest stock-led research products Alpaca can support without needing deeper market structure data.",
        ),
        ResearchFeature(
            name="Option Positioning Heatmap",
            effort="medium",
            value="high",
            apis=[
                "/v2/options/contracts",
                "/v1beta1/options/snapshots/{underlying_symbol}",
                "/v1beta1/options/trades/latest",
                "/v1beta1/options/trades",
                "/v1beta1/options/bars",
            ],
            unlocks="Maps strike and expiry concentration, call/put dominance, and near-spot positioning for a shortlisted root.",
            why_it_fits="Alpaca provides enough chain-level and recent-trade data to build a strong positioning surface without a full options tape product.",
        ),
        ResearchFeature(
            name="Volatility / Regime Monitor",
            effort="medium",
            value="medium",
            apis=[
                "/v2/stocks/bars",
                "/v2/stocks/snapshots",
                "/v1beta1/options/snapshots/{underlying_symbol}",
                "/v1beta1/options/bars",
            ],
            unlocks="Detects transitions between quiet, expanding, trending, and headline-driven regimes.",
            why_it_fits="The stock bar plus option IV and spread context combination is stronger than either surface alone.",
        ),
        ResearchFeature(
            name="Premarket Playbook Builder",
            effort="easy",
            value="high",
            apis=[
                "/v1beta1/screener/stocks/movers",
                "/v1beta1/news",
                "/v2/stocks/bars",
                "/v2/assets",
                "/v2/options/contracts",
            ],
            unlocks="Builds a prepared open shortlist with headline, gap, liquidity, and optionability context.",
            why_it_fits="Premarket prep benefits from Alpaca's stock and news coverage even when live option flow is sparse.",
        ),
        ResearchFeature(
            name="Corporate Action Radar",
            effort="medium",
            value="medium",
            apis=[
                "/v1/corporate-actions",
                "/v2/stocks/bars",
                "/v2/stocks/snapshots",
                "/v2/options/contracts",
            ],
            unlocks="Studies splits, dividends, and related contract/liquidity changes around event dates.",
            why_it_fits="This is a hidden capability because it depends on combining market data with corporate-action context rather than a single obvious endpoint.",
        ),
        ResearchFeature(
            name="Execution Research Dashboard",
            effort="medium",
            value="high",
            apis=[
                "/v2/account",
                "/v2/positions",
                "/v2/account/activities/FILL",
                "/v2/account/portfolio/history",
            ],
            unlocks="Explains fill quality, holding behavior, PnL path, and setup quality after execution.",
            why_it_fits="Alpaca's trading/account APIs are enough for a strong internal research and feedback layer even without extra vendors.",
        ),
        ResearchFeature(
            name="Post-Signal Outcome Analytics",
            effort="easy",
            value="high",
            apis=[
                "/v2/stocks/bars",
                "/v2/stocks/snapshots",
                "/v1beta1/options/bars",
                "/v1beta1/options/trades",
            ],
            unlocks="Measures what happened after a signal, board entry, or alert across multiple windows.",
            why_it_fits="This compounds the value of every scanner you build on top of Alpaca, not just one product surface.",
        ),
    ]


def _build_streaming_surfaces() -> list[StreamingSurface]:
    return [
        StreamingSurface(
            path="wss://stream.data.alpaca.markets/v1beta1/opra",
            channels=["quotes"],
            role="Targeted live option quote monitoring.",
            status="runtime-confirmed",
            notes=["The repo's existing quote-capture path connected successfully during closed-session validation."],
        ),
        StreamingSurface(
            path="wss://stream.data.alpaca.markets/v1beta1/opra",
            channels=["trades"],
            role="Targeted live option trade monitoring.",
            status="docs-confirmed",
            notes=["Not yet wired as a first-class ingest path in this repo."],
        ),
        StreamingSurface(
            path="wss://stream.data.alpaca.markets/v2/sip",
            channels=["trades", "quotes", "bars", "updatedBars", "dailyBars"],
            role="Live stock context and richer intraday state.",
            status="docs-confirmed",
            notes=[],
        ),
        StreamingSurface(
            path="wss://stream.data.alpaca.markets/v2/sip",
            channels=["statuses", "lulds", "imbalances"],
            role="Halt, band, and imbalance context.",
            status="docs-confirmed",
            notes=["Treat as account-dependent until entitlement-tested on this account."],
        ),
        StreamingSurface(
            path="wss://stream.data.alpaca.markets/v1beta1/news",
            channels=["news"],
            role="Real-time headline and catalyst context.",
            status="docs-confirmed",
            notes=[],
        ),
    ]


def _build_report(args: argparse.Namespace) -> dict[str, Any]:
    client = create_alpaca_client_from_env()
    market_paths, trading_paths = _load_openapi_paths()
    symbol = args.symbol.upper()
    option_contract = _choose_option_contract(client, symbol, args.option_type)

    data_probes: list[ProbeResult] = []
    data_probes.append(
        _probe(
            client=client,
            category="underlying",
            base_url=client.data_base_url,
            path="/v1beta1/screener/stocks/most-actives",
            params=None,
            role="Stock activity prefilter.",
            docs_confirmed="/v1beta1/screener/stocks/most-actives" in market_paths,
            summarizer=_summarize_most_actives,
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="underlying",
            base_url=client.data_base_url,
            path="/v1beta1/screener/stocks/movers",
            params=None,
            role="Momentum and gap prefilter.",
            docs_confirmed="/v1beta1/screener/{market_type}/movers" in market_paths,
            summarizer=_summarize_movers,
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="underlying",
            base_url=client.data_base_url,
            path="/v2/stocks/snapshots",
            params={"symbols": symbol, "feed": "sip"},
            role="Current stock state with trade, quote, and bar context.",
            docs_confirmed="/v2/stocks/snapshots" in market_paths,
            summarizer=lambda payload: _summarize_stock_snapshot(payload, symbol),
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="underlying",
            base_url=client.data_base_url,
            path="/v2/stocks/quotes/latest",
            params={"symbols": symbol, "feed": "sip"},
            role="Current stock best quote.",
            docs_confirmed="/v2/stocks/quotes/latest" in market_paths,
            summarizer=lambda payload: _summarize_symbol_map(payload, "quotes", symbol, ["t", "bp", "ap", "bs", "as"]),
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="underlying",
            base_url=client.data_base_url,
            path="/v2/stocks/trades/latest",
            params={"symbols": symbol, "feed": "sip"},
            role="Current stock last trade.",
            docs_confirmed="/v2/stocks/trades/latest" in market_paths,
            summarizer=lambda payload: _summarize_symbol_map(payload, "trades", symbol, ["t", "p", "s", "c"]),
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="underlying",
            base_url=client.data_base_url,
            path="/v2/stocks/bars",
            params={
                "symbols": symbol,
                "timeframe": "1Min",
                "start": _iso_timestamp(-2, 0, 0, 0),
                "end": _iso_timestamp(0, 23, 59, 59),
                "adjustment": "raw",
                "feed": "sip",
                "limit": 5,
            },
            role="Intraday stock bar history for RVOL and reaction analysis.",
            docs_confirmed="/v2/stocks/bars" in market_paths,
            summarizer=lambda payload: _summarize_symbol_series(payload, "bars", symbol, ["t", "o", "h", "l", "c", "v", "n", "vw"]),
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="underlying",
            base_url=client.data_base_url,
            path="/v1beta1/news",
            params={"symbols": symbol, "limit": 3},
            role="Headline and catalyst context.",
            docs_confirmed="/v1beta1/news" in market_paths,
            summarizer=_summarize_news,
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="context",
            base_url=client.data_base_url,
            path="/v2/stocks/auctions",
            params={"symbols": symbol, "start": _iso_day(-5), "end": _iso_day(0), "feed": "sip", "limit": 3},
            role="Opening and closing auction history.",
            docs_confirmed="/v2/stocks/auctions" in market_paths,
            summarizer=lambda payload: _summarize_auctions(payload, symbol),
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="context",
            base_url=client.data_base_url,
            path="/v1/corporate-actions",
            params={"symbols": symbol, "start": _iso_day(-30), "end": _iso_day(30), "limit": 10},
            role="Corporate-action context around a symbol.",
            docs_confirmed="/v1/corporate-actions" in market_paths,
            summarizer=_summarize_corporate_actions,
            notes=["Corporate actions can lag provider availability."],
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="context",
            base_url=client.trading_base_url,
            path="/v2/assets",
            params={"status": "active", "asset_class": "us_equity", "attributes": "has_options"},
            role="Optionable-underlying universe seed.",
            docs_confirmed="/v2/assets" in trading_paths,
            summarizer=_summarize_assets,
        )
    )

    if option_contract is not None:
        contract_symbol = option_contract["symbol"]
        expiration_date = option_contract["expiration_date"]
        data_probes.append(
            _probe(
                client=client,
                category="options",
                base_url=client.trading_base_url,
                path="/v2/options/contracts",
                params={
                    "underlying_symbols": symbol,
                    "type": args.option_type,
                    "status": "active",
                    "expiration_date_gte": _iso_day(0),
                    "expiration_date_lte": _iso_day(14),
                    "limit": 5,
                },
                role="Contract discovery and OI metadata.",
                docs_confirmed="/v2/options/contracts" in trading_paths,
                summarizer=_summarize_option_contracts,
            )
        )
        data_probes.append(
            _probe(
                client=client,
                category="options",
                base_url=client.data_base_url,
                path=f"/v1beta1/options/snapshots/{symbol}",
                params={"feed": "opra", "type": args.option_type, "expiration_date": expiration_date, "limit": 10},
                role="Chain-level option snapshot enrichment.",
                docs_confirmed="/v1beta1/options/snapshots/{underlying_symbol}" in market_paths,
                summarizer=_summarize_chain_snapshots,
            )
        )
        data_probes.append(
            _probe(
                client=client,
                category="options",
                base_url=client.data_base_url,
                path="/v1beta1/options/quotes/latest",
                params={"symbols": contract_symbol, "feed": "opra"},
                role="Per-contract latest option quote.",
                docs_confirmed="/v1beta1/options/quotes/latest" in market_paths,
                summarizer=lambda payload: _summarize_symbol_map(payload, "quotes", contract_symbol, ["t", "bp", "ap", "bs", "as", "c"]),
            )
        )
        data_probes.append(
            _probe(
                client=client,
                category="options",
                base_url=client.data_base_url,
                path="/v1beta1/options/trades/latest",
                params={"symbols": contract_symbol, "feed": "opra"},
                role="Per-contract latest option trade.",
                docs_confirmed="/v1beta1/options/trades/latest" in market_paths,
                summarizer=lambda payload: _summarize_symbol_map(payload, "trades", contract_symbol, ["t", "p", "s", "c", "x"]),
            )
        )
        data_probes.append(
            _probe(
                client=client,
                category="options",
                base_url=client.data_base_url,
                path="/v1beta1/options/trades",
                params={
                    "symbols": contract_symbol,
                    "start": _iso_timestamp(-2, 0, 0, 0),
                    "end": _iso_timestamp(0, 23, 59, 59),
                    "limit": 5,
                    "sort": "desc",
                },
                role="Recent option trades for premium and burst analysis.",
                docs_confirmed="/v1beta1/options/trades" in market_paths,
                summarizer=lambda payload: _summarize_symbol_series(payload, "trades", contract_symbol, ["t", "p", "s", "c", "x"]),
            )
        )
        data_probes.append(
            _probe(
                client=client,
                category="options",
                base_url=client.data_base_url,
                path="/v1beta1/options/bars",
                params={
                    "symbols": contract_symbol,
                    "timeframe": "1Min",
                    "start": _iso_timestamp(-2, 0, 0, 0),
                    "end": _iso_timestamp(0, 23, 59, 59),
                    "limit": 5,
                    "sort": "desc",
                },
                role="Recent option bars for fallback aggregation and replay.",
                docs_confirmed="/v1beta1/options/bars" in market_paths,
                summarizer=lambda payload: _summarize_symbol_series(payload, "bars", contract_symbol, ["t", "o", "h", "l", "c", "v", "n", "vw"]),
            )
        )

    data_probes.append(
        _probe(
            client=client,
            category="options",
            base_url=client.data_base_url,
            path="/v1beta1/options/meta/conditions/trade",
            params=None,
            role="Trade-condition decoding for normalization.",
            docs_confirmed="/v1beta1/options/meta/conditions/{ticktype}" in market_paths,
            summarizer=_summarize_map_entries,
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="options",
            base_url=client.data_base_url,
            path="/v1beta1/options/meta/conditions/quote",
            params=None,
            role="Quote-condition decoding for validity checks.",
            docs_confirmed="/v1beta1/options/meta/conditions/{ticktype}" in market_paths,
            summarizer=_summarize_map_entries,
        )
    )
    data_probes.append(
        _probe(
            client=client,
            category="options",
            base_url=client.data_base_url,
            path="/v1beta1/options/meta/exchanges",
            params=None,
            role="Option exchange-code decoding.",
            docs_confirmed="/v1beta1/options/meta/exchanges" in market_paths,
            summarizer=_summarize_map_entries,
        )
    )

    if not args.skip_account:
        data_probes.append(
            _probe(
                client=client,
                category="account",
                base_url=client.trading_base_url,
                path="/v2/account",
                params=None,
                role="Account-level research context.",
                docs_confirmed="/v2/account" in trading_paths,
                summarizer=_summarize_account,
            )
        )
        data_probes.append(
            _probe(
                client=client,
                category="account",
                base_url=client.trading_base_url,
                path="/v2/positions",
                params=None,
                role="Current position inventory for execution research.",
                docs_confirmed="/v2/positions" in trading_paths,
                summarizer=_summarize_positions,
            )
        )
        data_probes.append(
            _probe(
                client=client,
                category="account",
                base_url=client.trading_base_url,
                path="/v2/account/activities/FILL",
                params={"date": _iso_day(0), "page_size": 5, "direction": "desc"},
                role="Fill activity for execution analytics.",
                docs_confirmed="/v2/account/activities/{activity_type}" in trading_paths,
                summarizer=_summarize_activities,
            )
        )
        data_probes.append(
            _probe(
                client=client,
                category="account",
                base_url=client.trading_base_url,
                path="/v2/account/portfolio/history",
                params={"period": "1M", "timeframe": "1D"},
                role="Portfolio path and outcome research.",
                docs_confirmed="/v2/account/portfolio/history" in trading_paths,
                summarizer=_summarize_portfolio_history,
            )
        )

    return {
        "generated_at": datetime.now(tz=NEW_YORK).isoformat(),
        "symbol": symbol,
        "option_contract": option_contract,
        "probes": [asdict(item) for item in data_probes],
        "streaming_surfaces": [asdict(item) for item in _build_streaming_surfaces()],
        "research_features": [asdict(item) for item in _build_features()],
        "gaps": [
            "No documented US options order-book endpoint.",
            "No documented US options L2 depth feed.",
            "No documented historical option quotes REST endpoint.",
            "Real-time option scanning must stay targeted because option subscriptions are bounded.",
        ],
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Alpaca Research Surface Report",
        "",
        f"Generated: {report['generated_at']}",
        f"Primary symbol: `{report['symbol']}`",
    ]
    option_contract = report.get("option_contract")
    if option_contract:
        lines.append(f"Sample option contract: `{option_contract['symbol']}`")
    lines.extend(
        [
            "",
            "## Live Probes",
            "",
        ]
    )
    probes = report.get("probes", [])
    for category in ("underlying", "context", "options", "account"):
        category_items = [item for item in probes if item["category"] == category]
        if not category_items:
            continue
        lines.append(f"### {category.title()}")
        lines.append("")
        for item in category_items:
            lines.append(f"- `{item['method']} {item['path']}`")
            lines.append(f"  role: {item['role']}")
            lines.append(f"  status: {item['status']}")
            lines.append(f"  docs_confirmed: {item['docs_confirmed']}")
            if item.get("sample") is not None:
                lines.append(f"  sample: `{json.dumps(item['sample'], separators=(',', ':'), sort_keys=True)}`")
            for note in item.get("notes", []):
                lines.append(f"  note: {note}")
        lines.append("")

    lines.extend(["## Streaming Surfaces", ""])
    for item in report.get("streaming_surfaces", []):
        lines.append(f"- `{item['path']}`")
        lines.append(f"  channels: {', '.join(item['channels'])}")
        lines.append(f"  role: {item['role']}")
        lines.append(f"  status: {item['status']}")
        for note in item.get("notes", []):
            lines.append(f"  note: {note}")
    lines.append("")

    lines.extend(["## Research Feature Opportunities", ""])
    for item in report.get("research_features", []):
        lines.append(f"### {item['name']}")
        lines.append("")
        lines.append(f"- effort: `{item['effort']}`")
        lines.append(f"- value: `{item['value']}`")
        lines.append(f"- APIs: {', '.join(f'`{api}`' for api in item['apis'])}")
        lines.append(f"- unlocks: {item['unlocks']}")
        lines.append(f"- why Alpaca fits: {item['why_it_fits']}")
        lines.append("")

    lines.extend(["## Hard Limits", ""])
    for gap in report.get("gaps", []):
        lines.append(f"- {gap}")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = _build_report(args)
    rendered = json.dumps(report, indent=2) if args.output_format == "json" else _render_markdown(report)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + ("\n" if not rendered.endswith("\n") else ""))
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
