# Alpaca Premium Brief: Intelligence Engine / Scanner

Researched on April 6, 2026 from Alpaca's official pricing and docs.

Assumption: "premium tier" = **Algo Trader Plus**.

## 1. Capabilities

- **Full-market US stocks**: real-time SIP data across all US exchanges, not just IEX.
- **Live stock scanning support**: latest/snapshot access, higher request limits, and Alpaca screener endpoints for `most actives` and `top movers`.
- **Real-time options data**: OPRA access plus option snapshots / chain snapshots with latest trade, quote, and Greeks.
- **News layer**: real-time and historical Benzinga news.
- **Crypto layer**: real-time and historical crypto data if you want cross-asset context.
- **Execution layer**: stock, options, and crypto order APIs plus `trade_updates` websocket.
- **AI layer**: Alpaca now has an official MCP server if you want a copilot on top later.

## 2. Best Product Idea

Build a **stock-first intelligence engine**:

1. scan the full US stock universe on SIP data
2. enrich top names with Benzinga news and reaction scoring
3. pull options chains only for shortlisted names
4. send ranked alerts, watchlist updates, and paper/live trade suggestions

Why this is the best fit:

- it uses Alpaca where it is strongest: stocks + news + execution
- it still gets value from options without overbuilding around options limits
- it can start as a scanner and grow into a trading engine without changing vendors

## 3. V1 Feature Set

- **Ranked mover board**: change %, RVOL, dollar volume, trade count, spread quality
- **Catalyst panel**: latest headline, event type, sentiment, reaction score
- **Tradeability score**: spread, quote size, print consistency, halt/LULD awareness
- **Options confirmation**: IV jump, unusual volume, call/put imbalance, best contracts by spread + delta
- **Session-aware views**: premarket, open, regular session, after-hours
- **Saved scans / watchlists**: user filters and shortlisted names
- **Alert delivery**: Discord, Slack, email, or web notifications
- **Paper-trade hooks**: suggested order tickets and feedback from `trade_updates`
- **Replay / review**: what happened 5m, 15m, and 60m after each alert

## 4. Non-Negotiable Constraints

- Alpaca documents **1 connection per endpoint** for many subscriptions, including Algo Trader Plus.
- Alpaca documents **1,000 live option quote subscriptions**.
- Historical options data starts in **February 2024**.
- OTC market data is **not** included for regular users.
- Corporate actions can lag.
- Alpaca documents the real-time options stream as **msgpack-only**.
- Slow websocket clients can be disconnected.

What that means for the design:

- run **one ingest service per feed**
- fan out internally to your own workers
- treat options as a **second-stage enrichment layer**, not the primary universe

