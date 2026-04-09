# Alpaca Research Opportunities

Verified on April 9, 2026 using:

- [Alpaca Capabilities Statement](/Users/adeb/Projects/spreads/docs/research/alpaca_capabilities_statement.md)
- [Features Tracker](/Users/adeb/Projects/spreads/docs/product/features_tracker.md)
- [Alpaca UOA API Inventory](/Users/adeb/Projects/spreads/docs/research/alpaca_uoa_api_inventory.md)
- live probe output from `uv run spreads-research-alpaca`

## Thesis

Alpaca is strongest for stock-led, event-led, and operator-research products.

It is not the right base for a full options flow terminal, but it is a strong base for:

- stock leadership and rotation research
- catalyst and reaction research
- option-chain positioning research on shortlisted names
- auction and session-structure research
- execution and post-signal analytics

The key is to build around API combinations, not around isolated endpoints.

## Best Adjacent Features

### Easy Wins

- **Market Leadership / Rotation Board**
  - use: movers, most actives, stock snapshots, stock bars, news
  - outcome: a live board of what is actually leading, lagging, and rotating
  - why it fits: this is one of Alpaca's cleanest stock-led surfaces

- **Premarket Playbook Builder**
  - use: movers, news, stock bars, optionable universe seed, option contracts
  - outcome: a prepared open watchlist with catalyst and liquidity context
  - why it fits: Alpaca is useful before the open even when options flow is quiet

- **Post-Signal Outcome Analytics**
  - use: stock bars, stock snapshots, option bars, option trades
  - outcome: measure what happened after a board entry or alert
  - why it fits: this improves every scanner we build, not just one

### Medium Effort / High Leverage

- **Catalyst Reaction Tracker**
  - use: news, stock bars, stock snapshots, option chain snapshots, option trades
  - outcome: measure how headlines propagate through price, liquidity, and option activity
  - why it fits: Alpaca is unusually good at the stock plus news plus option-enrichment combination

- **Opening Drive / Closing Auction Board**
  - use: stock movers, auctions, bars, snapshots
  - outcome: rank names by open participation, close behavior, and follow-through
  - why it fits: stock auction history is a hidden strength in the Alpaca surface

- **Option Positioning Heatmap**
  - use: option contracts, chain snapshots, recent option trades, option bars
  - outcome: strike and expiry concentration, near-spot activity, call/put dominance
  - why it fits: Alpaca has enough chain-level data for positioning research on a shortlist

- **Execution Research Dashboard**
  - use: account, positions, fill activities, portfolio history
  - outcome: fill quality, hold-time analysis, path analysis, setup feedback
  - why it fits: Alpaca's execution APIs support strong internal analytics even without another broker or data vendor

### Medium Value / Specialized

- **Volatility / Regime Monitor**
  - use: stock bars, stock snapshots, option snapshots, option bars
  - outcome: detect quiet-to-expansion, trend-to-chop, and headline-driven regime shifts
  - why it fits: the value comes from combining stock structure with option IV and spread context

- **Corporate Action Radar**
  - use: corporate actions, stock bars, stock snapshots, option contracts
  - outcome: study splits, dividends, and related contract or liquidity changes
  - why it fits: this is a real hidden surface, but corporate actions can lag so it should stay secondary

## Hidden Capability Combos

These are the API combinations that unlock more than the individual endpoints suggest.

- **Screeners + snapshots + bars + news**
  - unlocks: stock leadership board, momentum confirmation, catalyst ranking, premarket prep

- **News + stock bars + option chain snapshots + option trades**
  - unlocks: catalyst reaction tracker and news-confirmed option-flow research

- **Auctions + bars + snapshots**
  - unlocks: opening drive and closing auction studies, plus session-transition analytics

- **Option contracts + chain snapshots + recent option trades + option bars**
  - unlocks: positioning heatmaps, expiry concentration views, IV and spread regime overlays

- **Corporate actions + stock bars + option contracts**
  - unlocks: event studies around dividends, splits, and contract adjustments

- **Account activities + portfolio history + market data**
  - unlocks: execution-quality research and post-trade feedback loops

## Live Probe Takeaways

The live probe from this workspace confirmed a few important things.

- The stock side is rich and reliable.
  - movers, most actives, stock snapshots, latest quotes, latest trades, bars, auctions, and news all answered cleanly

- The option side is strong for shortlist enrichment, not market-wide surveillance.
  - contracts, chain snapshots, latest quotes, recent trades, bars, trade conditions, quote conditions, and exchange maps are there
  - outside regular options hours, null trades and empty recent-trade windows are normal

- Corporate actions are real and usable.
  - Alpaca returned cash-dividend data for `SPY`, which means this can support event studies

- Execution research is viable.
  - account, positions, fill activities, and portfolio history all answered from this workspace

- The live option quote WebSocket path works in the repo.
  - closed-session validation connected successfully, but did not produce fresh quote updates

## What Alpaca Still Does Not Give Us

- no documented US options order book
- no documented US options L2 depth
- no documented historical option quotes REST
- no reason to build a full options flow terminal on top of Alpaca alone

That means the best Alpaca-native research products stay stock-first, event-first, or shortlist-based.

## Recommended Build Order

If we want adjacent research products after the UOA scanner, the best order is:

1. **Post-Signal Outcome Analytics**
   - highest leverage because it improves every future alerting system
2. **Catalyst Reaction Tracker**
   - strongest external-facing research feature Alpaca can support well
3. **Opening Drive / Closing Auction Board**
   - very differentiated relative to the usual stock scanner features
4. **Market Leadership / Rotation Board**
   - easiest operator-facing board to ship quickly
5. **Execution Research Dashboard**
   - best internal product once signals start generating trades

## Reusable Tooling

Use the new probe CLI to refresh this research surface as the repo evolves:

```bash
uv run spreads-research-alpaca --symbol SPY --output-format markdown --output outputs/alpaca_research_surface_report.md
uv run spreads-research-alpaca --symbol SPY --output-format json --output outputs/alpaca_research_surface_report.json
```

This tool probes the live Alpaca surface, checks official OpenAPI coverage, and renders both the endpoint report and the research-feature opportunities.
