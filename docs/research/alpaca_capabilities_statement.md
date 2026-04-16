# Alpaca Capabilities Statement

Verified on April 9, 2026 against Alpaca's official docs and official OpenAPI files.

Purpose: this is the repo's canonical Alpaca capability reference for research, scanner design, and alerting work. Read this before doing fresh Alpaca capability research.

## Scope

- Assumes a Trading API account with **Algo Trader Plus**.
- Focuses on capabilities relevant to this repo: stocks, options, news, scanners, alerting, and execution support.
- Treat this as a working statement, not a permanent guarantee. Re-check Alpaca's official docs/OpenAPI when a task depends on product changes, limits, or newly added endpoints.

## Subscription Statement

- Alpaca documents that `Algo Trader Plus` includes:
- real-time `SIP` coverage for US stocks
- real-time `OPRA` coverage for US options
- no 15-minute historical limitation for stocks/options
- `10,000 / min` historical API calls
- unlimited stock WebSocket subscriptions
- `1000` option quote WebSocket subscriptions

## Market Data Capabilities

### Options

- Trading API metadata:
- `GET /v2/options/contracts`
- Contract metadata includes expiry, type, style, strike, multiplier, size, `open_interest`, `open_interest_date`, `close_price`, `close_price_date`, and optional deliverables.

- Market Data REST:
- `GET /v1beta1/options/quotes/latest`
- `GET /v1beta1/options/trades/latest`
- `GET /v1beta1/options/snapshots`
- `GET /v1beta1/options/snapshots/{underlying_symbol}` for chain-style pulls
- `GET /v1beta1/options/trades` for historical trades
- `GET /v1beta1/options/bars` for historical bars
- `GET /v1beta1/options/meta/exchanges`
- `GET /v1beta1/options/meta/conditions/{ticktype}`

- Snapshot schema:
- latest trade
- latest quote
- Greeks: delta, gamma, theta, vega, rho
- implied volatility
- minute bar
- daily bar
- previous daily bar

- Option chain filters documented by Alpaca:
- `type`
- `strike_price_gte`
- `strike_price_lte`
- `expiration_date`
- `expiration_date_gte`
- `expiration_date_lte`
- `root_symbol`
- `updated_since`
- `limit`
- `page_token`

- Historical option bars support intraday and higher timeframes:
- `[1-59]Min`
- `[1-23]Hour`
- `1Day`
- `1Week`
- `[1,2,3,4,6,12]Month`

- Option bar fields include:
- OHLC
- volume
- trade count
- VWAP

- Real-time option WebSocket:
- feed is `opra` or `indicative`
- available channels are `trades` and `quotes`
- option stream is `msgpack` only
- `*` is not allowed for option quote subscriptions

### Stocks

- Market Data REST:
- `GET /v2/stocks/quotes`
- `GET /v2/stocks/quotes/latest`
- `GET /v2/stocks/trades`
- `GET /v2/stocks/trades/latest`
- `GET /v2/stocks/bars`
- `GET /v2/stocks/bars/latest`
- `GET /v2/stocks/snapshots`
- `GET /v2/stocks/{symbol}/snapshot`
- `GET /v2/stocks/auctions`
- `GET /v2/stocks/{symbol}/auctions`
- exchange-code and condition-code metadata endpoints

- Stock snapshot schema includes:
- latest trade
- latest quote
- minute bar
- daily bar
- previous daily bar

- Historical stock auctions are documented and can provide opening/closing auction context.

- Real-time stock WebSocket channels documented by Alpaca:
- `trades`
- `quotes`
- `bars`
- `updatedBars`
- `dailyBars`
- `statuses`
- `lulds`
- `corrections`
- `cancelErrors`
- `imbalances`

### Screeners

- Market Data screener endpoints:
- `GET /v1beta1/screener/stocks/most-actives`
- `GET /v1beta1/screener/{market_type}/movers`

- Alpaca documents these as real-time SIP-based screeners.

### News

- `GET /v1beta1/news` for historical/news-feed style retrieval
- real-time news WebSocket stream is documented
- Alpaca states historical news goes back to 2015 and is currently sourced from Benzinga

### Corporate Actions

- `GET /v1/corporate-actions`
- Alpaca explicitly warns corporate actions can lag provider creation/processing time

## Execution And Account Capabilities

- Trading API supports account, orders, positions, activities, and option contract discovery.
- Alpaca documents `trade_updates` over Trading WebSocket for order lifecycle events such as new, fill, partial fill, cancel, and reject.
- Alpaca documents that options assignment/exercise/expiry-related non-trade activities are **not** available via WebSocket and must be checked via REST polling.

## Known Non-Capabilities And Limits

- Alpaca's official Market Data OpenAPI exposes order-book endpoints for crypto and crypto-perps, not for US stocks or US options.
- For equities/options, Alpaca gives top-of-book and trade data, not documented L2/full-depth order books.
- Alpaca's current official Market Data OpenAPI does **not** document a historical option quotes REST endpoint.
- For options, the documented real-time stream is limited to quotes and trades. There is no documented option bar stream or option order-imbalance stream.
- Alpaca states historical options data is currently available only since **February 2024**.
- `open_interest` is exposed as contract metadata with its own date field, so treat it as dated metadata rather than live intraday state.
- Corporate actions may not be available immediately after announcement.

## Repo Implementation Snapshot

Current implementation already uses several Alpaca surfaces:

- [packages/core/services/scanner.py](/Users/adeb/Projects/spreads/packages/core/services/scanner.py)
- uses option contract discovery
- uses option chain snapshots
- uses latest option quotes
- uses stock daily/intraday bars
- uses option bars

- [packages/core/services/option_quote_capture.py](/Users/adeb/Projects/spreads/packages/core/services/option_quote_capture.py)
- already captures live option quote events over Alpaca's msgpack WebSocket
- already supports dynamic subscribe/unsubscribe for changing symbol sets

- [packages/core/jobs/live_collector.py](/Users/adeb/Projects/spreads/packages/core/jobs/live_collector.py)
- already persists latest-quote and WebSocket quote captures during live collection cycles

Current repo gaps relative to Alpaca's documented capability surface:

- I did **not** find option trade-stream ingestion in the repo yet.
- The current option snapshot parsing path uses latest trade, latest quote, IV, and Greeks, but does not appear to use snapshot `minuteBar`, `dailyBar`, or `prevDailyBar` even though Alpaca documents them.

## Practical Design Implications

- Alpaca is strong enough for an Alpaca-only unusual activity scanner if "unusual activity" means anomalies in:
- option trades
- option quotes / spread quality
- IV / Greeks
- volume vs open interest
- underlying price/volume/trade-count context
- news / halt / imbalance context

- Alpaca is **not** sufficient for a full depth-of-book or complex-order-flow analytics product.
- For broad scanning, prefer a two-stage design:
- stock-first prefilter using SIP data, movers, most-actives, news, and auction/status context
- option enrichment on shortlisted names using contracts, chain snapshots, latest trades, historical trades, bars, and targeted live quote/trade subscriptions

## Canonical Sources

- [About Market Data API](https://docs.alpaca.markets/docs/about-market-data-api)
- [Historical Option Data](https://docs.alpaca.markets/docs/historical-option-data)
- [Real-time Option Data](https://docs.alpaca.markets/docs/real-time-option-data)
- [Real-time Stock Data](https://docs.alpaca.markets/docs/real-time-stock-pricing-data)
- [Historical News Data](https://docs.alpaca.markets/docs/historical-news-data)
- [Real-time News](https://docs.alpaca.markets/docs/streaming-real-time-news)
- [Options Trading](https://docs.alpaca.markets/docs/options-trading)
- [Websocket Streaming](https://docs.alpaca.markets/docs/websocket-streaming)
- [Market Data OpenAPI](https://docs.alpaca.markets/openapi/market-data-api.json)
- [Trading OpenAPI](https://docs.alpaca.markets/openapi/trading-api.json)
