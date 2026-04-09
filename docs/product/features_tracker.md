# Features Tracker

Short log of proposed features, why they matter, and the current direction.

## Template

## <number>. <feature name>

- Status:
- Priority:
- Scope:
- Why:
- Direction:
- V1:
- Next:

## 1. Event Context Layer

- Status: done
- Priority: high
- Scope: system-wide
- Why: scheduled events distort option pricing, assignment risk, and scanner rankings; the whole system needs one shared event view
- Direction: build one reusable event subsystem with ingest, normalized store, resolver, and policy
- V1: earnings before expiry, ex-dividend before expiry, macro event annotation for ETF/index products, basic corporate action awareness
- Next: define the event schema, choose initial sources, implement the resolver interface, and apply it to call credit spreads first

### Notes

- Shared event context should expose: `clean`, `penalized`, or `blocked`, plus reasons and nearest-event info
- Single-stock short premium with earnings before expiry should block by default
- Short calls with ex-dividend risk before expiry should block or receive a heavy penalty
- ETFs and index proxies should usually be annotated for macro weeks rather than hard-blocked

## 2. Profile-Aware Ranking Engine

- Status: done
- Priority: high
- Scope: scanner core
- Why: the current ranking still over-rewards premium and near-top-of-band delta instead of surfacing the best trades
- Direction: rank within profile using a weighted quality score instead of primarily sorting by raw return on risk
- V1: delta-target distance, breakeven cushion, fill quality, liquidity, width appropriateness
- Next: tune weights against live scan review and post-run outcomes

## 3. Expected Move And Breakeven Cushion

- Status: done
- Priority: high
- Scope: scanner core
- Why: delta and OTM % alone are not enough to judge whether the short strike has enough room
- Direction: measure short strike and breakeven distance against expected move and use it in filters and ranking
- V1: add breakeven cushion scoring and a basic expected-move filter where data is available
- Next: calibrate minimum cushion rules by profile and symbol type

## 4. Candidate De-Duplication

- Status: done
- Priority: medium
- Scope: scanner output
- Why: one short strike can create several nearly identical spreads that clutter the top results
- Direction: collapse near-duplicate candidates and keep the best expression per short leg / expiry group
- V1: keep only the top-ranked spread for each short leg and expiration unless explicitly expanded
- Next: tune duplicate grouping rules for wider multi-expiry scans

## 5. Underlying Setup Filter

- Status: done
- Priority: medium
- Scope: scanner core
- Why: a structurally valid spread is not enough if the underlying is in a bad regime for bearish or neutral premium selling
- Direction: add a lightweight underlying context layer before final ranking
- V1: trend, momentum, proximity to resistance, and simple regime checks
- Next: refine the regime model and add clearer resistance / breakout signals

## 6. Run History And Replay

- Status: done
- Priority: medium
- Scope: research / evaluation
- Why: we need to measure whether scanner changes actually improve outcomes over time
- Direction: save scan results in a structured history and compare post-scan outcomes over fixed review windows
- V1: persist run metadata and evaluate what happened after 1d, 3d, and expiry
- Next: extend replay beyond underlying-level outcomes into spread-level mark and expiry analysis

## 7. Universe Scanner Board

- Status: done
- Priority: high
- Scope: scanner product
- Why: scanning one symbol at a time limits the system to manual exploration instead of ranked opportunity discovery
- Direction: scan a curated symbol set, keep the top candidate per name by default, and rank the board across names
- V1: `--symbols`, `--symbols-file`, and curated `--universe` presets with board CSV/JSON output
- Next: add async execution and richer universe presets

## 8. Spread-Level Replay And Exit Engine

- Status: done
- Priority: high
- Scope: research / evaluation
- Why: underlying-only replay misses the spread mark, profit targets, and stop behavior that actually matter
- Direction: replay stored spreads with option-leg bars and simple exit thresholds
- V1: entry-day spread marks plus 1d, 3d, and expiry summaries with estimated profit-target and stop-hit tracking
- Next: add richer exit rules and spread P&L curves over time

## 9. Data Reliability Hardening

- Status: done
- Priority: high
- Scope: scanner core
- Why: candidates should reflect not just market data presence, but whether the data quality is strong enough to trust
- Direction: add explicit data-policy gates around expected move, fill quality, and source confidence
- V1: strict and warning modes for expected-move coverage, fill-ratio quality, expected-move cushion, and low-confidence single-name event coverage
- Next: improve source quality and make the hardening logic more source-aware

## 10. Post-Signal Outcome Analytics

- Status: proposed
- Priority: high
- Scope: research / evaluation
- Why: every board and alert gets more valuable if we can measure what happened after detection
- Direction: treat this as shared research infrastructure, not a one-off report
- V1: continuation, fade, max excursion, time-to-move, and session-outcome summaries for board entries and alerts
- Next: reuse the stored outcomes to tune UOA thresholds and future research features

### Notes

- Highest-leverage adjacent feature after the UOA scanner
- Uses Alpaca surfaces we already confirmed: stock bars, stock snapshots, option bars, and option trades
- Improves every later scanner rather than just creating one more board

## 11. Catalyst Reaction Tracker

- Status: proposed
- Priority: high
- Scope: research / operator view
- Why: Alpaca is unusually strong at combining news, stock reaction, and option-chain context
- Direction: build a reaction study layer that measures how headlines propagate through price, liquidity, and option activity
- V1: multi-window stock reaction plus option snapshot and recent-trade overlays for shortlisted names
- Next: classify headline types and attach post-signal outcome summaries

### Notes

- Strongest external-facing Alpaca-native research product after UOA
- Core inputs: news, stock bars, stock snapshots, option chain snapshots, option trades
- Best built once root and contract UOA signals already exist

## 12. Opening Drive And Closing Auction Board

- Status: proposed
- Priority: high
- Scope: operator board
- Why: stock auction history is a hidden Alpaca strength and can produce a differentiated market board
- Direction: rank names by open participation, close behavior, and follow-through
- V1: opening-drive and closing-auction board with auction prints, stock bars, and stock snapshot context
- Next: add session-transition analytics and tie results into post-signal outcome review

### Notes

- Reuses session-mode and board patterns from the UOA scanner
- Core inputs: movers, auctions, bars, snapshots
- Higher leverage than a generic stock mover board because it uses rarer context

## 13. Market Leadership And Rotation Board

- Status: proposed
- Priority: high
- Scope: operator board
- Why: this is the easiest always-on research board to ship and use every session
- Direction: show what is truly leading, lagging, rotating, and confirming across the market
- V1: ranked stock leadership board from movers, most actives, stock bars, stock snapshots, and news
- Next: group by sector, ETF, and session bucket

### Notes

- Lowest-risk operator-facing extension of the stock-first UOA prefilter
- Core inputs: most actives, movers, stock bars, stock snapshots, news
- More practical daily value than a niche options-only research view

## 14. Execution Research Dashboard

- Status: proposed
- Priority: high
- Scope: internal research / feedback loop
- Why: once signals begin producing trades, execution research becomes one of the most valuable internal surfaces
- Direction: combine broker activity and market context to explain fill quality and post-fill path
- V1: fill quality, hold-time distribution, path analysis, and setup-level feedback
- Next: separate paper and live results, then feed execution outcomes back into ranking

### Notes

- Core inputs: account, positions, fill activities, portfolio history
- Depends more on having a meaningful trade stream than on new market-data work
- Strong fit for Alpaca because account and execution APIs are already available

## 15. Option Positioning Heatmap

- Status: proposed
- Priority: medium
- Scope: research / operator view
- Why: Alpaca can support shortlist-based option positioning research even though it cannot support full options flow infrastructure
- Direction: map strike and expiry concentration for shortlisted roots
- V1: call/put dominance, near-spot concentration, strike clustering, and expiry concentration views
- Next: overlay UOA signal strength and catalyst context on top of the heatmap

### Notes

- Core inputs: option contracts, chain snapshots, latest trades, recent trades, option bars
- Best built after the option-enrichment part of UOA is stable
- Useful, but less urgent than the stock-led and post-signal research surfaces

## 16. Premarket Playbook Builder

- Status: proposed
- Priority: medium
- Scope: operator prep
- Why: Alpaca is useful before the open even when live options flow is weak
- Direction: build a prepared open watchlist with catalyst, gap, liquidity, and optionability context
- V1: premarket shortlist with news, mover context, stock bars, and optionable-universe checks
- Next: connect the playbook directly into market-open monitoring and alert warm starts

### Notes

- Core inputs: movers, news, stock bars, assets, option contracts
- More of a packaging and workflow feature than a new analytical primitive
- Practical daily value, but less differentiated than catalyst tracking or auction studies

## 17. Volatility And Regime Monitor

- Status: proposed
- Priority: medium
- Scope: shared scoring / research layer
- Why: regime changes matter, but the value is mostly as a modifier for other boards and alerts
- Direction: detect quiet, expansion, trend, chop, and event-driven states from stock and option context together
- V1: regime labels from stock bars, stock snapshots, option snapshots, and option bars
- Next: use regime state as a shared modifier in UOA and catalyst scoring

### Notes

- Better as shared infrastructure than as the next standalone product
- Useful, but lower priority than outcome analytics and catalyst work

## 18. Corporate Action Radar

- Status: proposed
- Priority: medium
- Scope: research / event studies
- Why: corporate actions plus market data unlock a real but secondary research surface
- Direction: study dividends, splits, and adjustment-heavy periods with stock and option context
- V1: event studies around dividend and split windows using corporate actions, stock bars, stock snapshots, and option contracts
- Next: flag adjusted-contract periods and compare behavior before and after the event window

### Notes

- Alpaca returned real corporate action data in the live probe, so the surface is usable
- Corporate action lag keeps this below the other backlog items
- Better as a later specialized research view than an early core feature
