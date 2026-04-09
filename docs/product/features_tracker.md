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

## 10. Evaluation, Calibration, And Outcome Engine

- Status: proposed
- Priority: high
- Scope: shared research / truth layer
- Why: every board, pattern, and alert needs one common answer to what happened next and whether our scores actually mean anything
- Direction: unify outcome analytics, forward evaluation, and score calibration into one downstream engine
- V1: continuation, fade, max excursion, adverse excursion, time-to-confirm, time-to-fail, and score-bucket quality summaries
- Next: make this the default downstream layer for UOA, chart patterns, catalyst signals, and boards

### Notes

- Folds together post-signal outcome analytics, the evaluation engine, and signal calibration
- Core inputs: stock bars, stock snapshots, option bars, option trades, and relative-strength context
- Highest-leverage proposed platform layer in the tracker

## 11. Catalyst And Event Intelligence

- Status: proposed
- Priority: high
- Scope: shared context / research layer
- Why: the value is not just detecting headlines or events, but understanding whether price and options are confirming, rejecting, or ignoring them
- Direction: unify catalyst context, reaction tracking, and event-study work into one reusable intelligence layer
- V1: headline presence, catalyst freshness, stock reaction quality, options confirmation, and corporate-action overlays
- Next: classify catalyst types and compare reaction quality by setup, symbol class, and session

### Notes

- Folds together catalyst reaction tracking, catalyst context, and the specialized corporate-action research idea
- Builds on the existing event context layer instead of creating another separate event system
- Core inputs: news, calendar events, corporate actions, stock bars, stock snapshots, option snapshots, and option trades

## 12. Session And Regime Intelligence

- Status: proposed
- Priority: high
- Scope: shared scoring / operator context
- Why: the same move means different things before the open, at the open, midday, and into the close, especially when the regime is trend, chop, or expansion
- Direction: combine session structure, open-close behavior, premarket prep, and regime labeling into one shared context layer
- V1: premarket playbook, opening-drive and closing-auction views, session tags, and basic trend/chop/expansion labels
- Next: use the layer to modulate alert confidence, board ranking, and pattern quality

### Notes

- Folds together the premarket playbook, opening or closing board, session context, and volatility or regime monitor
- Core inputs: stock bars, stock snapshots, auctions, movers, news, and imbalances when available
- Better as one shared context layer than several adjacent boards

## 13. Leadership, Rotation, And Relative Strength

- Status: proposed
- Priority: high
- Scope: shared market context / operator board
- Why: raw price movement matters less than whether a name is leading, lagging, or rotating relative to the market and its peers
- Direction: combine leadership boards with reusable relative-strength scoring
- V1: ranked leadership board plus market-relative and sector-relative strength signals
- Next: add sector, ETF, and session grouping along with persistence and breadth-style summaries

### Notes

- Folds together the leadership board and the relative-strength layer
- Core inputs: most actives, movers, stock bars, stock snapshots, news, and curated ETF context
- Strong fit for Alpaca because it relies on clean stock time series rather than deeper market structure

## 14. Execution And Portfolio Research

- Status: proposed
- Priority: high
- Scope: internal research / decision quality
- Why: even good signals need fill-quality feedback, overlap handling, and exposure discipline before they become a usable trading process
- Direction: unify execution analytics and paper portfolio research into one post-signal decision layer
- V1: fill quality, hold-time distribution, post-fill path analysis, overlapping-signal handling, and simple exposure summaries
- Next: separate paper and live results, compare portfolio policies, and feed execution outcomes back into ranking

### Notes

- Folds together the execution research dashboard and the paper portfolio or allocation idea
- Core inputs: account state, positions, fill activities, portfolio history, and signal metadata
- More valuable once several signal families are producing steady output

## 15. Option Positioning And Flow Context

- Status: proposed
- Priority: medium
- Scope: research / operator context
- Why: Alpaca can support shortlist-based option positioning and flow confirmation even though it cannot support a full options flow terminal
- Direction: treat options as an enrichment surface for roots already shortlisted elsewhere
- V1: call or put dominance, strike clustering, expiry concentration, and recent-trade overlays
- Next: overlay UOA, catalyst, and pattern context on top of the positioning view

### Notes

- Folds the option positioning heatmap into a broader option-context framing
- Core inputs: option contracts, chain snapshots, latest trades, recent trades, and option bars
- Useful, but still downstream of the stock-first context layers

## 16. Price Structure And Pattern Detection

- Status: proposed
- Priority: high
- Scope: shared detection / market structure layer
- Why: price action gets more useful when patterns are anchored to explicit market structure and key levels instead of treated as free-floating shapes
- Direction: combine key levels and stock-first pattern detection into one price-structure layer
- V1: prior day high or low, premarket high or low, opening range, VWAP, gap-fill, weekly levels, and core patterns like breakout, fade, reclaim, and continuation
- Next: add confidence scoring, outcome tracking, and options or catalyst confirmation

### Notes

- Folds together the chart-pattern engine and the market-structure or key-levels layer
- Core inputs: stock bars, stock snapshots, auctions, movers, most actives, and live stock bar updates
- Best fit is stock-first pattern detection, with options as confirmation rather than the primary surface

## 17. Tradeability And Liquidity Confidence Layer

- Status: proposed
- Priority: high
- Scope: shared scoring / alert integrity
- Why: interesting setups are not automatically actionable setups, and the system needs one shared view of whether data and liquidity are good enough to trust
- Direction: build a common confidence layer around freshness, spread quality, quote quality, trade activity, and data health
- V1: stock and option quote quality, trade freshness, spread stability, and stale-data penalties
- Next: make the layer a hard gate for high-confidence alerts and a visible explanation field on boards

### Notes

- This remains separate because it is a cross-cutting integrity layer rather than a board or detector
- Core inputs: quotes, trades, bars, feed-health state, and session mode
- More important than adding another detector if the goal is signal quality

## 18. Research Platform And Experimentation

- Status: proposed
- Priority: high
- Scope: shared data / research platform
- Why: once the system has multiple ideas, it needs aligned datasets and a reproducible way to compare hypotheses instead of accumulating one-off research paths
- Direction: combine the research dataset builder and experiment framework into one platform layer
- V1: aligned stock, option, news, session, and evaluation datasets for shortlisted symbols and configurable experiment runs with saved results
- Next: add parameter sweeps, benchmark comparison, and model-assisted ranking research

### Notes

- Folds together the feature-store idea and the experiment or hypothesis engine
- Depends on the evaluation layer more than on new Alpaca endpoints
- This is infrastructure, but very high-leverage infrastructure
