# Alpaca UOA API Inventory

Verified on April 9, 2026 from this workspace using the repo's local `.env` and direct `curl` calls. No throwaway script was used.

Purpose: this is the implementation-level Alpaca API list for the unusual-activity scanner. It complements the broader [Alpaca Capabilities Statement](/Users/adeb/Projects/spreads/docs/research/alpaca_capabilities_statement.md).

## Verification Notes

- Validation happened while the options market was closed.
- Stock and option REST endpoints responded successfully.
- Option latest quote and trade endpoints returned last-known session data, which is expected outside regular options hours.
- The repo's existing scanner CLI exercised the option quote WebSocket runtime path successfully, but no fresh quote updates arrived during the closed-session test window.

## Primary V1 APIs

These are the APIs we should treat as the core Alpaca-only UOA surface.

### Underlying Prefilter

- `GET /v1beta1/screener/stocks/most-actives`
  - role: stock activity prefilter
  - status: live-confirmed

- `GET /v1beta1/screener/stocks/movers`
  - role: stock momentum and gap-style prefilter
  - status: live-confirmed

- `GET /v2/stocks/quotes/latest`
  - role: current stock best bid and ask
  - status: live-confirmed

- `GET /v2/stocks/trades/latest`
  - role: current stock last trade context
  - status: live-confirmed

- `GET /v2/stocks/bars`
  - role: intraday and daily stock bars for RVOL, acceleration, and regime checks
  - status: live-confirmed

- `GET /v2/stocks/snapshots`
  - role: one-call stock snapshot for latest trade, quote, minute bar, daily bar, previous daily bar
  - status: live-confirmed

- `GET /v1beta1/news`
  - role: catalyst and headline context
  - status: live-confirmed

### Option Enrichment

- `GET /v2/options/contracts`
  - role: contract discovery, expiry/strike filters, open interest metadata
  - status: live-confirmed
  - note: `open_interest` and `close_price` can be null on some contracts, especially near expiry

- `GET /v1beta1/options/snapshots/{underlying_symbol}`
  - role: chain-level enrichment with latest trade, latest quote, greeks, IV, and bar fields
  - status: live-confirmed
  - note: `latestTrade` and `impliedVolatility` can be null on individual contracts

- `GET /v1beta1/options/quotes/latest`
  - role: per-contract latest best quote
  - status: live-confirmed

- `GET /v1beta1/options/trades/latest`
  - role: per-contract latest trade
  - status: live-confirmed

- `GET /v1beta1/options/trades`
  - role: recent trade history for premium, volume, trade-count, and burst detection
  - status: live-confirmed

- `GET /v1beta1/options/bars`
  - role: fallback aggregation and short-window baseline support
  - status: live-confirmed

- `GET /v1beta1/options/meta/conditions/trade`
  - role: trade-condition normalization for signal quality
  - status: live-confirmed

- `GET /v1beta1/options/meta/exchanges`
  - role: exchange-code decoding for diagnostics and alert payloads
  - status: live-confirmed

### Context APIs

- `GET /v2/stocks/auctions`
  - role: opening and closing auction context
  - status: live-confirmed

- `GET /v2/assets?status=active&asset_class=us_equity&attributes=has_options`
  - role: broad optionable-underlying universe seed
  - status: live-confirmed
  - note: results still need filtering for `tradable`

## Streaming APIs

### Runtime-Confirmed

- `wss://stream.data.alpaca.markets/v1beta1/opra`
  - channels: `quotes`
  - role: targeted live option quote monitoring
  - status: runtime-confirmed through the repo's existing scanner and quote-capture code
  - note: the connection path worked, but the closed-session test window produced no fresh quote updates

### Docs-Confirmed

- `wss://stream.data.alpaca.markets/v1beta1/opra`
  - channels: `trades`
  - role: targeted live option trade monitoring
  - status: docs-confirmed
  - note: this repo does not yet appear to ingest option trade streams

- `wss://stream.data.alpaca.markets/v1beta1/news`
  - channels: `news`
  - role: real-time catalyst context
  - status: docs-confirmed

- `wss://stream.data.alpaca.markets/v2/sip`
  - channels: `trades`, `quotes`, `bars`, `updatedBars`, `dailyBars`
  - role: richer live stock context
  - status: docs-confirmed

## Use With Caution

These surfaces are documented, but we should not make them core gating logic until we entitlement-test them on this account.

- `wss://stream.data.alpaca.markets/v2/sip`
  - channels: `statuses`, `lulds`, `imbalances`
  - role: halt, limit-band, and imbalance context
  - status: docs-confirmed, not live-tested in this pass
  - note: stock status access in Alpaca's docs includes entitlement language, so treat production availability as account-dependent until verified

## Explicit Non-APIs

These are not part of the Alpaca-only UOA design.

- no documented US options order-book endpoint
- no documented US options L2 depth feed
- no documented historical option quotes REST endpoint

## Practical Conclusion

The confirmed Alpaca-only UOA stack is:

- stocks REST for prefilter and price/volume context
- options REST for chain enrichment and recent flow
- option quote WebSocket for targeted live monitoring
- option trade WebSocket as the next streaming addition
- news REST, and optionally news WebSocket, for catalyst context

That is enough for a strong Alpaca-only unusual-activity scanner, but it is still a top-of-book plus trades design, not a true order-book system.

## Sources

- [About Market Data API](https://docs.alpaca.markets/docs/about-market-data-api)
- [Real-time Stock Data](https://docs.alpaca.markets/docs/real-time-stock-pricing-data)
- [Real-time Option Data](https://docs.alpaca.markets/docs/real-time-option-data)
- [Streaming Real-time News](https://docs.alpaca.markets/docs/streaming-real-time-news)
- [WebSocket Streaming](https://docs.alpaca.markets/docs/websocket-streaming)
- [Market Data OpenAPI](https://docs.alpaca.markets/openapi/market-data-api.json)
- [Trading OpenAPI](https://docs.alpaca.markets/openapi/trading-api.json)
