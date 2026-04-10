# Alpaca-Only Unusual Activity Scanner Design

Status: phase 5 in progress

Based on:

- [Alpaca Capabilities Statement](/Users/adeb/Projects/spreads/docs/research/alpaca_capabilities_statement.md)
- [Alpaca UOA API Inventory](/Users/adeb/Projects/spreads/docs/research/alpaca_uoa_api_inventory.md)
- [Alpaca Option Trade Condition Research](/Users/adeb/Projects/spreads/docs/research/alpaca_option_trade_condition_research.md)
- [Current System State](/Users/adeb/Projects/spreads/docs/current_system_state.md)

## Goal

Build an intraday unusual-activity scanner that uses Alpaca only and produces ranked, explainable alerts.

The scanner should answer:

- what changed
- why it matters
- how strong the confirmation is
- whether the setup is liquid enough to care about

## Scope

This is an Alpaca-only UOA system, not a full options flow terminal.

Use Alpaca for:

- stock movers, most actives, snapshots, bars, statuses, LULDs, imbalances
- option contracts, chain snapshots, latest trades, latest quotes, historical trades, bars
- targeted option quote and trade WebSocket monitoring
- news context

Do not design around:

- full options order book
- L2 depth
- complex order flow
- true sweep detection that requires routing or venue-level depth data

The working model is top-of-book plus trades, not full book reconstruction.

## Product Shape

The scanner should be stock-first and options-enriched.

1. Prefilter underlyings with stock and news context.
2. Pull option chain and contract signals for the shortlisted names.
3. Monitor the best contracts live.
4. Score, rank, dedupe, and alert.

This keeps the system inside Alpaca's practical limits and avoids trying to ingest the whole options market.

## System Flow

```text
                            ALPACA
    +------------------------------------------------------+
    | Stocks | Options | News | Status/LULD | Imbalances   |
    +------------------------------------------------------+
          |         |          |         |
          v         v          v         v
   +-------------------------------------------------------+
   | session mode gate                                      |
   | market_open | premarket | after_hours | closed         |
   +-------------------------------------------------------+
                          |
                          v
   +-------------------------------------------------------+
   | 1. Underlying Prefilter                               |
   | movers, actives, stock bars, snapshots, news, status  |
   +-------------------------------------------------------+
                           |
                           v
                 shortlist of underlyings
                           |
                           v
   +-------------------------------------------------------+
   | 2. Option Enrichment                                  |
   | contracts, chain snapshots, latest trades/quotes,     |
   | recent trades, recent bars                            |
   +-------------------------------------------------------+
                           |
                           v
   +-------------------------------------------------------+
   | data integrity gate                                   |
   | freshness | trade filters | handoff | feed health     |
   +-------------------------------------------------------+
                    |                       |
                    v                       v
         ranked contracts         root-level flow view
                    \                       /
                     \                     /
                      v                   v
   +-------------------------------------------------------+
   | 3. Live Confirmation                                  |
   | targeted option quotes + targeted option trades       |
   | plus live stock context                               |
   +-------------------------------------------------------+
                           |
                           v
   +-------------------------------------------------------+
   | 4. Scoring + Dedupe + Alerting                        |
   | contract score + root score + underlying score        |
   | + liquidity + catalyst + persistence                  |
   +-------------------------------------------------------+
              |                    |                    |
              v                    v                    v
        watchlist state       board state         alert events
                                                    |
                                                    v
                                         UI / Discord / logs
```

## System Diagram

```text
 +-------------------+        +----------------------------------+
 |      ALPACA       |        |         REPO RUNTIME             |
 |-------------------|        |----------------------------------|
 | Stocks REST/WS    |------->| uoa_underlying_collector         |
 | Options REST      |------->|   - pulls stock/news context     |
 | Options WS        |------->|   - builds symbol shortlist      |
 | News REST/WS      |------->+-------------------+--------------+
 +-------------------+                            |
                                                  v
                                  +---------------+---------------+
                                  | session mode controller       |
                                  | - market_open                 |
                                  | - premarket                   |
                                  | - after_hours                 |
                                  | - closed                      |
                                  +---------------+---------------+
                                                  |
                                                  v
                                  +---------------+---------------+
                                  | data integrity / feed health  |
                                  | - freshness                   |
                                  | - trade normalization         |
                                  | - backfill/live handoff       |
                                  | - ws/rest fallback state      |
                                  +---------------+---------------+
                                                  |
                                                  v
                                  +---------------+---------------+
                                  | uoa_option_enricher           |
                                  | - contracts                   |
                                  | - chain snapshots             |
                                  | - latest/recent trades        |
                                  | - recent bars                 |
                                  +---------------+---------------+
                                                  |
                           +----------------------+----------------------+
                           |                                             |
                           v                                             v
             +-------------+--------------+               +--------------+-------------+
             | contract candidates        |               | root flow candidates       |
             +-------------+--------------+               +--------------+-------------+
                           \                                             /
                            \                                           /
                             v                                         v
                                  +---------------+---------------+
                                  | uoa_live_monitor              |
                                  | - targeted option quote WS    |
                                  | - targeted option trade WS    |
                                  | - live stock confirmation     |
                                  +---------------+---------------+
                                                  |
                                                  v
                                  +---------------+---------------+
                                  | scoring / dedupe / state      |
                                  | - watchlist                   |
                                  | - board                       |
                                  | - alert decisions             |
                                  +-------+-------------+---------+
                                          |             |
                                          |             |
                         +----------------+             +----------------+
                         v                                               v
         +---------------+----------------+              +---------------+----------------+
         | persistent state / event store |              | outbound delivery              |
         |--------------------------------|              |--------------------------------|
         | collector_cycles               |              | UI event stream                |
         | collector_cycle_candidates     |              | Discord alerts                 |
         | collector_cycle_events         |              | logs / operator visibility     |
         | option_quote_events            |              +--------------------------------+
         | option_trade_events            |
         | root signal payloads           |
         | contract signal payloads       |
         | uoa_symbol_state               |
         | uoa_baselines                  |
         +--------------------------------+
```

## Session Modes

The scanner should be session-aware. It should not treat closed-session option data like live intraday flow.

### market_open

Use full scanner behavior.

- run stock prefilter
- run option enrichment
- run targeted live option quote and trade monitoring
- allow full UOA alerts

### premarket

Use stock-led discovery mode.

- run stock/news prefilter
- allow option enrichment for preparation and ranking
- keep live option monitoring limited or off
- suppress high-confidence option-flow alerts unless fresh option activity is clearly present

### after_hours

Use the same posture as premarket.

- keep stock/news context active
- allow limited option context refresh
- treat option signals as lower confidence by default
- suppress normal live UOA alerting

### closed

Use maintenance mode.

- do not run live UOA detection
- do not emit normal UOA alerts
- refresh baselines, watchlists, expiries, and news context
- prepare the next regular session

### Session Rules

- session mode must affect scoring, freshness, and alert eligibility
- stale option quotes or prints must not be treated as current flow
- high-confidence alerts should normally require `market_open`
- premarket and after-hours should bias toward setup preparation, not aggressive alerting

## V1 Defaults

These defaults are locked for the first implementation.

- underlying shortlist: keep `30` roots and refresh every `30s` during `market_open`, every `60s` otherwise
- expiry window: scan `0-14 DTE`, with ranking bias toward `0-7 DTE`
- `0DTE`: included, but use stricter liquidity and freshness rules than other expiries
- strike band: use `max(1.5 x expected move, 2% of spot)` around spot, and expand if needed to include at least `8` strikes above and `8` strikes below spot when the chain allows
- standard contract liquidity gate: require `mid >= 0.10`, `spread <= 12% of mid`, and `min(bid_size, ask_size) >= 5`
- `0DTE` liquidity gate: require `mid >= 0.20`, `spread <= 8% of mid`, and `min(bid_size, ask_size) >= 10`
- live monitor budget: max `25` monitored roots, max `4` contracts per root, max `100` contracts total, with a minimum `5m` monitor hold time before eviction
- freshness: standard contracts use quote stale after `15s` and trade stale after `60s`; `0DTE` uses quote stale after `10s` and trade stale after `30s`
- session eligibility: high-confidence alerts require `market_open`; premarket and after-hours are preparation modes only
- stock context: stock REST snapshots and bars are primary in v1; stock WebSocket is optional follow-on work, not a v1 requirement
- stock `statuses`, `lulds`, and `imbalances`: optional score modifiers when available, never hard requirements in v1
- root alert model: alerts are root-first and attach up to `3` supporting contracts
- alert cooldown: dedupe repeated alerts for `15m` per root and alert type unless the alert escalates
- alert escalation: escalate instead of creating a new alert when the root score increases by at least `15` points or when the active expiry concentration changes materially
- persistence model: create `option_trade_events` as a first-class table now; keep root and contract signal payloads on existing collector events first, then promote them later if needed
- baseline model: use a rolling `5m` trade baseline, session cumulative baseline, and previous-day same-time root baseline
- score weights: `contract 30`, `root 25`, `underlying 20`, `liquidity 15`, `live confirmation 5`, `catalyst 5`
- score thresholds: `watchlist >= 60`, `board >= 75`, `high alert >= 80`, `critical >= 90`
- degraded mode: downgrade scores, suppress `critical` alerts, fall back to bounded REST refresh, and surface feed health in board and alert payloads
- notification policy: update board and watchlist on first detection, but send Discord alerts only for `high` and `escalated` events

## Data Integrity Rules

The scanner should prefer lower confidence over false precision.

- treat `open_interest` as lagged metadata and always carry its date
- do not score stale quotes or stale trades as fresh flow
- normalize option trades before scoring so excluded conditions do not inflate volume, premium, or trade count
- define a per-contract handoff from REST backfill to live trade monitoring so the same prints are not counted twice
- track feed health and mark whether a signal is based on live stream data, REST fallback data, or partial data

Minimum controls:

- last-seen trade cursor per monitored contract
- quote and trade freshness windows
- trade-condition include or exclude rules
- duplicate-print protection across backfill and stream ingestion

Trade normalization for v1:

- include only trade conditions `I`, `J`, `S`, `a`, and `b` in anomaly scoring
- exclude all cancel, correction, late, floor, multi-leg, stock-option, compression, and extended-hours trade conditions from anomaly scoring
- if a trade carries multiple condition flags, every flag must be in the allowlist or the trade is excluded from anomaly scoring
- keep excluded trades in raw storage if captured, but do not let them affect premium, volume, trade count, or aggressiveness signals
- treat condition codes as case-sensitive; `J` and `j` are different conditions
- do not reject a contract from monitoring only because its snapshot `latestTrade.c` is excluded; normalize at the trade-event level

Quote validity for v1:

- reject quotes with conditions `F`, `I`, `R`, `T`, `X`, or `Y`
- only use firm, non-halted quotes for spread, midpoint, and quote-size scoring

## Contract Eligibility And Monitor Budget

Live monitoring has to be bounded.

Use explicit eligibility rules for monitored contracts:

- `0-14 DTE` only
- separate liquidity and freshness rules for `0DTE`
- strike selection inside the locked dynamic strike band
- require either recent premium, recent trade activity, or strong root-level confirmation
- require the locked quote-quality thresholds
- prefer standard contracts and explicitly flag adjusted contracts when used
- require current contract status and fresh data

Use explicit budget rules:

- max `30` shortlisted roots per cycle
- max `25` monitored roots at one time
- max `4` monitored contracts per root
- max `100` monitored contracts globally
- deterministic eviction when a better candidate appears after the minimum hold time
- minimum `5m` monitor hold time to avoid churn from constant subscribe and unsubscribe changes

The monitor set should prefer the most liquid, most active, and most explainable contracts rather than the broadest possible coverage.

## Calendar And Session Transitions

Session handling should be calendar-driven, not inferred loosely from data silence.

The runtime should account for:

- regular days
- half days
- holidays
- weekends
- session open and close transitions

Required behaviors:

- reset intraday state at the start of each trading day
- prevent previous-session option prints from being treated as current flow
- clear or downgrade stale watchlist and board entries across session boundaries
- use slower maintenance behavior during weekends and holidays
- downgrade or freeze option-flow interpretation when the underlying is halted or in unstable LULD conditions

## Alert Lifecycle And Degraded Mode

Alerts should move through a small explicit lifecycle:

- `new`
- `escalated`
- `cooldown`
- `resolved`
- `suppressed`

Degraded-mode behavior should also be explicit.

If option live data becomes unreliable:

- downgrade confidence
- reduce or pause high-confidence alerts
- fall back to bounded REST refresh where possible
- surface feed-health state in board and alert payloads

Additional controls:

- collapse repeated contract triggers into a root-level alert when they describe the same move
- suppress alert spam during monitor churn, session transitions, and restart recovery

High-confidence alerts should require both signal strength and acceptable data quality.

Alert payload and dedupe for v1:

- dedupe key: `symbol + alert_type + direction + session_date`
- every alert payload should include `symbol`, `alert_type`, `direction`, `session_mode`, `root_score`, `underlying_score`, `data_quality`, `top_contracts`, and a short explanation field
- `top_contracts` should include at most `3` contracts
- repeated detections inside the cooldown window should update existing state unless they meet escalation rules

## Pipeline

### 1. Underlying Prefilter

Goal: shrink the universe to names worth option inspection.

Inputs:

- movers and most actives
- stock snapshots and intraday bars
- LULD, status, imbalance, and auction context
- news
- operator watchlists

Output:

- shortlist of active symbols

### 2. Option Enrichment

Goal: turn each shortlisted symbol into contract-level and root-level flow candidates.

Inputs:

- option contracts
- option chain snapshots
- latest option trades and quotes
- recent historical trades and bars

Output:

- ranked contracts
- root-level call / put flow view
- preliminary directional bias

### 3. Live Confirmation

Goal: increase confidence on the best names only.

Inputs:

- targeted option quote subscriptions
- targeted option trade subscriptions
- live stock context

Output:

- short-lived live state for top contracts
- persistence and follow-through signals

### 4. Alerting

Goal: emit a small number of explainable alerts.

Alerts should be driven by:

- contract anomaly
- root-level flow
- underlying confirmation
- liquidity quality
- news or catalyst context

## Core Signals

### Underlying

- price acceleration
- RVOL and dollar volume
- breakout or reversal behavior
- halt, LULD, or imbalance context
- fresh news

### Contract

- premium spike
- volume spike
- trade-count spike
- volume versus open interest
- IV jump
- spread and quote-size change
- repeated prints near bid or ask as a proxy, not true aggressor classification

### Root

- call dominance
- put dominance
- expiry concentration
- strike clustering
- near-spot concentration
- same-direction multi-contract confirmation

### Confidence Modifiers

- liquidity
- data freshness
- news alignment
- underlying confirmation
- repeat detections across cycles

## Scoring

Use layered scoring, not one flat activity number.

Recommended score components:

- underlying score
- contract anomaly score
- root-flow score
- liquidity score
- catalyst score
- live-confirmation score

Use a normalized `0-100` score with these fixed weights:

- contract anomaly: `30`
- root flow: `25`
- underlying: `20`
- liquidity: `15`
- live confirmation: `5`
- catalyst: `5`

High-confidence alerts should require:

- acceptable spread and quote quality
- minimum premium or trade activity
- fresh data
- some underlying or catalyst confirmation

Score thresholds for v1:

- `0-59`: no alert
- `60-74`: watchlist
- `75-79`: board only
- `80-89`: high alert
- `90-100`: critical, but only if `market_open` and feed health is acceptable

## Repo Fit

Reuse the current runtime instead of building a separate pipeline.

Keep using:

- `live_collector` job shape
- `collector_cycles`, `collector_cycle_candidates`, `collector_cycle_events`
- `alert_events`, `alert_state`
- option quote capture
- existing event fanout and Discord alerting

Add:

- option trade ingestion for monitored contracts
- root-level signal snapshots
- contract-level signal snapshots
- scanner baseline storage
- symbol lifecycle state

The board/watchlist model should shift from spread ideas to unusual-flow symbols and contracts.

## Minimal Runtime Shape

Recommended logical jobs:

- `uoa_underlying_collector`
- `uoa_option_enricher`
- `uoa_live_monitor`
- `uoa_alert_dispatch`
- `uoa_baseline_refresh`

This can start inside the existing collector flow and split out later only if needed.

## V1 Storage

Keep:

- `collector_cycles`
- `collector_cycle_candidates`
- `collector_cycle_events`
- `alert_events`
- `alert_state`
- `option_quote_events`

Add:

- `option_trade_events`
- `uoa_symbol_state`
- `uoa_baselines`

For v1:

- `option_trade_events` should be a first-class table
- root and contract signals should start as JSON payloads attached to existing collector events
- `uoa_symbol_state` and `uoa_baselines` should be persisted explicitly

## V1 Non-Goals

- market-wide option tape ingestion
- full order-book analytics
- complex-order-flow reconstruction
- direct sweep detection from unavailable market structure data
- long-history options seasonality as a core ranking input

## Implementation Phases

### Phase 1: Foundations

Goal: create the minimum storage and runtime shape for UOA without changing the whole system at once.

- add `option_trade_events`, `uoa_symbol_state`, and `uoa_baselines`
- add a UOA mode or sibling path inside the current collector flow
- add session-mode resolution and feed-health state
- define alert payload shape and dedupe key in code
- current progress: option-trade capture, normalization, and persistence are now live in code

Exit condition:

- the runtime can track UOA state cleanly even before live alerts are turned on

### Phase 2: Underlying Prefilter

Goal: build the stock-first shortlist engine.

- pull movers, most actives, stock snapshots, bars, and news
- score and rank roots using the locked shortlist rules
- persist shortlist and board/watchlist state per cycle
- apply session-aware behavior for `market_open`, `premarket`, `after_hours`, and `closed`

Exit condition:

- the system produces a stable shortlist of `30` roots on the locked cadence

### Phase 3: Option Enrichment

Goal: turn shortlisted roots into explainable option-flow candidates.

- pull option contracts inside the locked `0-14 DTE` window
- apply the locked strike-band, liquidity, and `0DTE` rules
- enrich with chain snapshots, latest trades, recent trades, and bars
- build root and contract signal payloads on collector events
- compute the rolling `5m`, session, and previous-day root baselines

Exit condition:

- each shortlisted root can produce ranked contract candidates and root-flow summaries

### Phase 4: Live Monitoring

Goal: add targeted live confirmation for the best contracts only.

- subscribe to option quote WebSocket for the monitored set
- add option trade-stream ingestion for the monitored set
- enforce the locked monitor budget, hold time, freshness windows, and REST-to-live handoff
- persist live quote and trade evidence for short-window confirmation

Exit condition:

- the system maintains a bounded live monitor set without duplicate counting or churn-driven noise

Current progress:

- live option quote and trade capture are now wired into the collector
- all captured trades are stored, while only allowlisted-condition trades are marked scoreable
- capture health and scoreable-flow summaries are emitted per cycle
- rolling session baselines are now derived from persisted scoreable trade history
- root-level scanner decisions now classify observed flow into `none`, `watchlist`, `board`, or `high`

### Phase 5: Scoring And Alerting

Goal: turn candidates into deduped alerts and operator-visible board state.

- apply the fixed score weights and score thresholds
- emit root-first alerts with up to `3` supporting contracts
- enforce cooldown, escalation, and degraded-mode behavior
- send board/watchlist updates and Discord alerts on the locked policy

Exit condition:

- the system emits `watchlist`, `board`, `high`, and `critical` outcomes consistently from the same scoring model

Current progress:

- root and contract flow summaries now score only `included_in_score` trade prints
- excluded prints remain in raw storage and are surfaced only as audit counts and exclusion reasons
- baseline-aware root classification now emits first-pass `none/watchlist/board/high` decisions
- quote freshness and liquidity now feed into root decisions and can cap stale or weak-flow outcomes
- UOA Discord alerts are now root-first, attach up to `3` supporting contracts, and include DTE, volume, premium, and quote-quality context
- the first outbound policy is `high` only; cooldown, escalation, and degraded-mode alert suppression are still pending

### Phase 6: Calibration

Goal: tune thresholds using real runtime behavior without changing the architecture.

- review live captures for false positives, stale-data cases, and monitor churn
- validate trade-condition filtering and quote-validity rules against real sessions
- adjust thresholds only if the locked defaults prove too loose or too strict
- promote root or contract signals to first-class tables later only if JSON payloads become limiting

Exit condition:

- v1 is stable enough to trust during regular market hours and explicit about its degraded states outside them
