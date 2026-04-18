from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from core.common import format_stream_timestamp, parse_float, parse_int, pick
from core.domain.models import (
    DailyBar,
    IntradayBar,
    LiveOptionQuote,
    OptionContract,
    OptionTrade,
    OptionSnapshot,
)

DEFAULT_DATA_BASE_URL = "https://data.alpaca.markets"
DEFAULT_TRADING_BASE_URL = "https://api.alpaca.markets"


class AlpacaRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.response_body = response_body


def infer_trading_base_url(key_id: str, explicit_base_url: str | None) -> str:
    if explicit_base_url:
        return explicit_base_url.rstrip("/")
    if key_id.startswith("PK"):
        return "https://paper-api.alpaca.markets"
    return DEFAULT_TRADING_BASE_URL


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
            with urllib.request.urlopen(
                request, timeout=self.request_timeout_seconds
            ) as response:
                body_bytes = response.read()
                if not body_bytes:
                    return None
                return json.loads(body_bytes.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            raise AlpacaRequestError(
                f"Alpaca request failed: {exc.code} {exc.reason} for {url}\n{response_body}",
                status_code=exc.code,
                url=url,
                response_body=response_body,
            ) from exc
        except urllib.error.URLError as exc:
            raise AlpacaRequestError(
                f"Failed to reach Alpaca for {url}: {exc.reason}",
                url=url,
            ) from exc

    def get_json(
        self, base_url: str, path: str, params: dict[str, Any] | None = None
    ) -> Any:
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

    def cancel_order(self, order_id: str) -> None:
        self.request_json("DELETE", self.trading_base_url, f"/v2/orders/{order_id}")

    def get_order(self, order_id: str, *, nested: bool = False) -> dict[str, Any]:
        response = self.get_json(
            self.trading_base_url,
            f"/v2/orders/{order_id}",
            {"nested": "true" if nested else None},
        )
        if not isinstance(response, dict):
            raise RuntimeError("Unexpected Alpaca order response shape")
        return response

    def get_order_by_client_order_id(
        self,
        client_order_id: str,
        *,
        nested: bool = False,
    ) -> dict[str, Any]:
        response = self.get_json(
            self.trading_base_url,
            "/v2/orders:by_client_order_id",
            {
                "client_order_id": client_order_id,
                "nested": "true" if nested else None,
            },
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
        quote = self._extract_symbol_payload(
            quote_payload, symbol, plural_key="quotes", singular_key="quote"
        )
        bid = parse_float(pick(quote, "bp", "bid_price"))
        ask = parse_float(pick(quote, "ap", "ask_price"))
        if bid and ask and bid > 0 and ask > 0:
            return (bid + ask) / 2.0

        trade_payload = self.get_json(
            self.data_base_url,
            "/v2/stocks/trades/latest",
            {"symbols": symbol, "feed": stock_feed},
        )
        trade = self._extract_symbol_payload(
            trade_payload, symbol, plural_key="trades", singular_key="trade"
        )
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
            if (
                None in (open_price, high_price, low_price, close_price, volume)
                or not timestamp
            ):
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
            if (
                None in (open_price, high_price, low_price, close_price, volume)
                or not timestamp
            ):
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
                        if (
                            None in (open_price, high_price, low_price, close_price)
                            or not timestamp
                        ):
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

    def get_option_trades(
        self,
        symbols: list[str],
        *,
        start: str,
        end: str,
    ) -> dict[str, list[OptionTrade]]:
        if not symbols:
            return {}

        trades_by_symbol: dict[str, list[OptionTrade]] = {
            symbol: [] for symbol in symbols
        }
        page_token: str | None = None
        while True:
            payload = self.get_json(
                self.data_base_url,
                "/v1beta1/options/trades",
                {
                    "symbols": ",".join(symbols),
                    "start": start,
                    "end": end,
                    "limit": 1000,
                    "page_token": page_token,
                },
            )
            raw_trades = payload.get("trades", {})
            if isinstance(raw_trades, dict):
                for symbol, trades in raw_trades.items():
                    for item in trades:
                        price = parse_float(pick(item, "p", "price"))
                        size = parse_int(pick(item, "s", "size")) or 0
                        timestamp = pick(item, "t", "timestamp")
                        if price is None or price <= 0 or not timestamp:
                            continue
                        trades_by_symbol.setdefault(symbol, []).append(
                            OptionTrade(
                                symbol=str(symbol),
                                price=price,
                                size=size,
                                timestamp=str(timestamp),
                            )
                        )
            page_token = payload.get("next_page_token") or payload.get("page_token")
            if not page_token:
                break
        return trades_by_symbol

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
        raise RuntimeError(
            f"Unexpected Alpaca response shape while looking up {symbol}"
        )

    @staticmethod
    def _parse_option_snapshot(
        symbol: str, snapshot: dict[str, Any]
    ) -> OptionSnapshot | None:
        latest_quote = snapshot.get("latestQuote") or snapshot.get("latest_quote") or {}
        greeks = snapshot.get("greeks") or {}
        latest_trade = snapshot.get("latestTrade") or snapshot.get("latest_trade") or {}
        daily_bar = snapshot.get("dailyBar") or snapshot.get("daily_bar") or {}

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
            daily_volume=parse_int(pick(daily_bar, "v", "volume")),
            greeks_source="alpaca" if delta_value is not None else None,
        )


__all__ = [
    "AlpacaClient",
    "AlpacaRequestError",
    "DEFAULT_DATA_BASE_URL",
    "DEFAULT_TRADING_BASE_URL",
    "infer_trading_base_url",
]
