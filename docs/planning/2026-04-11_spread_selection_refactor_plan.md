# Spread Selection Review And Refactor Plan

Status: proposed

As of: Saturday, April 11, 2026

Related:

- [Ops CLI Visibility Plan](./ops_cli_visibility_plan.md)
- [0DTE System Architecture](./0dte_system_architecture.md)
- [Trading Engine Architecture](./trading_engine_architecture.md)
- [Options Scanner Research Log](../research/options_scanner_research_log.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Goal

Document how spreads are currently constructed and selected, identify the highest-leverage weaknesses, and propose a refactor plan that replaces the current split board/watchlist selection model with one canonical ranked opportunity list.

This document is intentionally focused on:

- spread construction
- call-versus-put selection
- ranked opportunity selection and tiering
- execution candidate selection
- calibration using post-market outcomes

This document is not a proposal for a full strategy rewrite.

## Executive Summary

The current system is stronger at discovering spread candidates than it is at promoting the right ones.

The scanner in [scanner.py](../../packages/core/services/scanner.py) already applies a meaningful set of structural filters:

- DTE profile windows
- delta bands and delta targets
- width bounds
- minimum credit
- minimum open interest
- relative spread limits
- fill-ratio checks
- expected-move checks
- calendar policy
- setup scoring
- data-quality scoring

That is not the main failure.

The bigger issue is that one static `quality_score` is reused too far downstream:

- candidate ranking in [scanner.py](../../packages/core/services/scanner.py#L2484)
- board/watchlist promotion in [live_collector.py](../../packages/core/jobs/live_collector.py#L354)
- alert thresholds in [alerts/rules.py](../../packages/core/alerts/rules.py)
- signal confidence in [signal_state.py](../../packages/core/services/signal_state.py)
- auto execution via board position in [execution.py](../../packages/core/services/execution.py#L1769)

That creates a structural problem:

- discovery logic
- promotion logic
- alerting logic
- execution logic

are all partially coupled, but not canonically unified.

The deeper architectural mistake is that `board` and `watchlist` evolved into two quasi-independent selection surfaces instead of two states on one ranked list.

The system should not have:

- one score for discovery
- another implicit policy for promotion
- another interpretation for alerts
- another interpretation for execution

The system should have:

- one canonical ranked opportunity list
- one canonical policy path
- optional presentation states like `board`, `watchlist`, `blocked`, or `discarded`

The clearest current failure is `core` selection. Recent closed sessions show that the `core` board has been materially worse than the same-session `watchlist`, which means the system is already discovering better ideas than it is promoting.

## Architectural Correction

`Board` and `watchlist` should not be separate selection concepts.

They should be two views of the same ordered opportunity set.

The correct model is:

- one candidate universe per cycle
- one canonical ranking path
- one persisted ordered list
- a small number of explicit states on each candidate

In that model:

- `board` means top-ranked and currently promotable
- `watchlist` means still valid but not in the top promotable slice
- `blocked` means structurally interesting but currently not promotable or executable
- `discarded` means below the current cut line

This is the key change that the refactor plan should drive.

## Current System Flow

### High-Level Flow

Mermaid source:

- [2026-04-11_spread_selection_refactor_plan_current_system_flow.mmd](../diagrams/planning/2026-04-11_spread_selection_refactor_plan_current_system_flow.mmd)

### Current Responsibility Boundaries

| Surface | Current owner | Current behavior | Main issue |
|---|---|---|---|
| Candidate construction | [scanner.py](../../packages/core/services/scanner.py#L2292) | Builds candidate spreads from contract metadata, snapshots, and expected move | Reasonably solid |
| Candidate scoring | [scanner.py](../../packages/core/services/scanner.py#L2484) | Produces one static `quality_score` | Score is overloaded |
| Combined call/put selection | [scanner.py](../../packages/core/services/scanner.py#L3812) | Concatenates strategy results and sorts by score | No explicit side arbitration |
| Board/watchlist promotion | [live_collector.py](../../packages/core/jobs/live_collector.py#L354) | Uses floors, score gaps, and hysteresis | Static promotion policy |
| Alerting | [alerts/rules.py](../../packages/core/alerts/rules.py) | Separate score thresholds and promotion rules | Duplicates selection policy |
| Signal confidence | [signal_state.py](../../packages/core/services/signal_state.py#L85) | Normalizes score between watchlist floor and board-strong score | Confidence is derived from score, not realized lift |
| Auto execution | [execution.py](../../packages/core/services/execution.py#L1779) | Chooses board `position == 1` | No execution-side re-ranking |
| Closed-session feedback | [post_market_analysis.py](../../packages/core/services/post_market_analysis.py#L63) | Good diagnostics and recommendations | Not yet feeding selection policy |

## What The Code Is Doing Today

### 1. Construction Is Profile-Aware

Profiles in [scanner.py](../../packages/core/services/scanner.py#L737) already encode materially different regimes:

- `0dte`
- `micro`
- `weekly`
- `swing`
- `core`

These profiles change:

- DTE windows
- delta ranges
- width limits
- min credit
- min open interest
- max relative spread
- fill ratio
- expected-move tolerances

This is the right design direction. The system is not using one flat spread template.

### 2. Candidate Ranking Is Static

[score_candidate()](../../packages/core/services/scanner.py#L2484) weights:

- delta fit
- short strike versus expected move
- breakeven versus expected move
- fill score
- liquidity score
- width score
- DTE score
- return on risk

Then it applies multipliers for:

- calendar status
- setup status
- data status

This is a decent discovery score. It is not yet a good promotion score or execution score.

### 3. Combined Mode Merges Too Naively

[merge_strategy_candidates()](../../packages/core/services/scanner.py#L3812) simply merges call and put candidates and sorts them by raw score.

That means combined mode does not answer:

- which side has better edge for this symbol right now
- whether the side is dominant or only marginally ahead
- whether one side should stay watchlist-only even if its raw score is high

### 4. Board Promotion Uses Static Floors And Hysteresis

[select_board_candidates()](../../packages/core/jobs/live_collector.py#L354) currently depends on:

- `BOARD_SCORE_FLOOR = 65.0`
- `BOARD_STRONG_SCORE = 82.0`
- `BOARD_WINNER_GAP = 6.0`
- `BOARD_SIDE_SWITCH_MARGIN = 10.0`
- `BOARD_REPLACEMENT_MARGIN = 5.0`
- `BOARD_CONFIRMATION_CYCLES = 2`
- `BOARD_HOLD_TOLERANCE = 3.0`

This creates stability, but it does not create calibration.

### 5. Execution Trusts Board Order Too Much

[submit_auto_session_execution()](../../packages/core/services/execution.py#L1714) selects the board candidate with the best stored board position.

That means:

- scanner rank drives board order
- board order drives execution
- execution does not compute a fresh best-executable choice from the board set

This is acceptable only if board ordering is already highly reliable. Current outcomes do not support that assumption.

## Evidence From Recent Closed Sessions

Important distinction:

- these are modeled post-market idea outcomes
- these are not realized account PnL

They are still the right feedback loop for candidate and promotion quality.

### Recent Label-Level Results

Using the latest succeeded post-market run per date:

| Label | Dates reviewed | Board average modeled PnL | Watchlist average modeled PnL | Observation |
|---|---|---:|---:|---|
| `explore_10_combined_core_auto` | April 8-10, 2026 | negative every day | positive every day | clearest promotion failure |
| `explore_10_combined_weekly_auto` | April 7-10, 2026 | mixed | mixed | unstable rank ordering |
| `explore_10_combined_0dte_auto` | April 7-10, 2026 | unstable | unstable | runtime quality and selection both matter |

### Core Is The Clearest Problem

Recent `core` closed sessions:

- April 8, 2026: board `-0.67`, watchlist `+9.71`
- April 9, 2026: board `-1.00`, watchlist `+6.56`
- April 10, 2026: board `-7.62`, watchlist `+1.42`

That is not a minor threshold miss. It means the system is already surfacing better same-session ideas on the watchlist and then promoting the wrong ones to the board.

Concrete April 10, 2026 example:

- board `IWM put_credit` scores `72.3` and `73.5` modeled around `-24` and `-21`
- watchlist `GLD call_credit` score `55.1` modeled around `+21.5`

This is strong evidence that the current score is useful for discovery but not trustworthy as a direct promotion signal.

### Weekly Is More Mixed But Still Misordered

Recent `weekly` closed sessions:

- April 7, 2026: board `-12.38`, watchlist `+2.77`
- April 8, 2026: board `-0.68`, watchlist `-3.35`
- April 9, 2026: board `+5.42`, watchlist `-3.29`
- April 10, 2026: board `-1.60`, watchlist `+2.78`

Weekly is not uniformly broken. It can still produce a good board. The issue is that its rank ordering is not stable enough on weaker days.

Concrete April 10, 2026 example:

- board `QQQ put_credit` score `75.5` modeled around `-15.5`
- watchlist `QQQ put_credit` score `75.2` modeled around `+7.0`

That is almost a tie in score and a large gap in outcome.

### 0DTE Should Not Be Tuned The Same Way

Recent `0dte` closed sessions show a different pattern:

- April 7, 2026: board `+6.67`, watchlist `-20.0`
- April 8, 2026: board `+7.0`, watchlist `+1.0`
- April 9, 2026: board `-46.0`, watchlist `+8.0`
- April 10, 2026: board `+11.0`, watchlist `+10.69`

The `0dte` stream remains too unstable to tune in the same way as `core`.

The correct interpretation is:

- `core`: selection failure is the clearest issue
- `weekly`: board ordering is noisy
- `0dte`: data validity, quote quality, and execution quality remain first-class concerns

## Key Observations

### 1. The Main Problem Is Selection, Not Discovery

The watchlist contains many of the better ideas. That means broad scanner construction is already finding usable structures.

The first move should not be:

- globally tighter delta bands
- globally higher score floors
- globally higher min credit

That would likely reduce idea count without fixing promotion quality.

### 2. The Current Setup Score Is Symbol-Level, Not Spread-Level

[analyze_underlying_setup()](../../packages/core/services/scanner.py#L1894) produces direction-aware setup context for the underlying and strategy.

That is useful, but every candidate on the same symbol and side inherits that same setup snapshot.

This helps with:

- bullish versus bearish context
- trend and opening-range context
- intraday structure

It does not help enough with:

- picking between neighboring strikes
- separating two same-side candidates with similar deltas
- deciding whether the best same-side spread should actually go to the board

### 3. Open Interest Is Being Over-Trusted

Alpaca documents that options contract metadata includes:

- `open_interest`
- `open_interest_date`

The current scanner reads `open_interest`, but does not preserve `open_interest_date` in [OptionContract](../../packages/core/services/scanner.py#L500).

That matters because open interest is not an intraday-updating quality signal. It is useful as a coarse liquidity floor, but it should not be over-weighted as if it were live.

### 4. Useful Alpaca Market Data Is Still Underused

The option chain snapshot endpoint exposes:

- latest quote
- latest trade
- Greeks
- implied volatility

The repo already parses quote, trade, Greeks, and daily volume in [OptionSnapshot](../../packages/core/services/scanner.py#L1291).

What is still missing from selection policy is stronger use of:

- quote persistence
- quote freshness
- trade recency
- volume versus open interest
- live quote path stability over the session

### 5. Selection Policy Is Fragmented

Today the repo has multiple selection-like surfaces:

- scanner score
- board floors and hysteresis
- alert score floors
- signal-state confidence normalization
- execution choosing board position 1

These values are related, but they are not owned by one canonical module.

That is the main architectural weakness to fix.

## External Constraints That Matter

### Theta Versus Gamma Into Expiry

The OIC Greeks material is directionally consistent with the repo's current assumptions:

- theta accelerates as expiration approaches
- gamma risk also rises as expiration approaches

That supports keeping `0dte`, `weekly`, and `core` as separate regimes instead of flattening them into one score surface.

### Alpaca-Specific Constraints

From the official Alpaca docs and the repo's Alpaca capability statement:

- historical options data is currently available only since February 2024
- option chain snapshots provide latest trade, latest quote, and Greeks
- option WebSocket support is quotes and trades, not a deeper order book
- `open_interest` is exposed with its own date field, so it should be treated as dated metadata

These constraints support a design that emphasizes:

- top-of-book quality
- quote persistence
- trade recency
- execution realism

instead of pretending we have full-depth order flow.

## Design Diagnosis

The right diagnosis is:

1. do not rebuild spread construction first
2. replace separate board/watchlist selection with one ranked opportunity list
3. separate discovery from promotion and execution inside that one list
4. add calibration on top of post-market analysis
5. keep the selection path canonical

## Target Architecture

### High-Level Target Flow

Mermaid source:

- [2026-04-11_spread_selection_refactor_plan_target_architecture.mmd](../diagrams/planning/2026-04-11_spread_selection_refactor_plan_target_architecture.mmd)

### Canonical Ownership After Refactor

| Responsibility | Recommended owner |
|---|---|
| raw candidate construction | `services/scanner.py` |
| selection features and policy | `services/spread_selection.py` |
| profile and threshold config | `services/spread_selection_policy.py` or in `spread_selection.py` initially |
| ranked opportunity persistence and state assignment | `jobs/live_collector.py` calling the selection service |
| execution-side ranking | `services/execution.py` calling the selection service |
| outcome calibration | `services/spread_calibration.py` and `services/post_market_analysis.py` |

The key point is that `live_collector.py`, `alerts/rules.py`, `signal_state.py`, and `execution.py` should stop owning separate interpretations of what a "good enough" spread is.

`Board` and `watchlist` should remain, at most, presentation tiers backed by one persisted ranked list.

## Proposed Refactor Plan

## Phase 0: Instrumentation And Baselines

Goal:

- make the existing selection path easier to measure before behavior changes

Changes:

- preserve `open_interest_date` from Alpaca contract metadata
- persist option trade recency and trade count where available
- persist quote freshness and quote-persistence features
- add a simple baseline report by label and date:
  - board average modeled PnL
  - watchlist average modeled PnL
  - board lift versus watchlist
- add a rank-band baseline report for the future target model:
  - top slice average modeled PnL
  - next slice average modeled PnL
  - lift by rank band
- add score monotonicity diagnostics:
  - does higher score actually improve modeled outcomes by profile and bucket

Expected outcome:

- better evidence
- no behavior change

## Phase 1: Extract A Canonical Ranked Opportunity Service

Goal:

- stop letting multiple modules reinterpret candidate quality independently

Changes:

- create `packages/core/services/spread_selection.py`
- move board/watchlist selection logic out of [live_collector.py](../../packages/core/jobs/live_collector.py) into this service
- have the service return one ordered candidate list with explicit fields such as:
  - `rank`
  - `state`
  - `state_reason`
  - `discovery_score`
  - `promotion_score`
  - `execution_score`
- move threshold constants used for promotion into one canonical profile-aware policy surface
- keep [scanner.py](../../packages/core/services/scanner.py) responsible only for:
  - candidate construction
  - candidate enrichment
  - base discovery ranking

Expected outcome:

- one canonical ranked opportunity path
- easier calibration later

## Phase 2: Split The Score Surface

Goal:

- stop using one score for three different jobs

Introduce three explicit scores:

### `discovery_score`

Purpose:

- candidate surfacing
- initial same-side ranking
- broad inclusion on the ordered opportunity list

Base inputs:

- current `quality_score` inputs
- keep mostly the same logic initially

### `promotion_score`

Purpose:

- assignment of `board`, `watchlist`, `blocked`, or `discarded` state
- same-side replacement
- side switching

Additive inputs:

- same-side versus opposite-side dominance
- regime alignment
- score stability over recent cycles
- quote persistence
- calibration penalties and bonuses

### `execution_score`

Purpose:

- choose the best executable candidate from the promotable top slice

Additive inputs:

- live credit retention
- reactive fill quality
- quote freshness
- candidate age
- short-term data quality

Expected outcome:

- discovery stays broad enough
- top-slice promotion becomes more selective
- execution stops trusting board order blindly

## Phase 3: Replace Naive Combined Merge With Side Arbitration

Goal:

- choose the right side per symbol instead of sorting calls and puts together by raw score

Changes:

- for each symbol, keep the best call candidate and best put candidate separately
- compare them with explicit side-arbitration logic
- assign one side to the promotable top slice only when:
  - it clearly dominates the other side
  - it meets profile-specific promotion conditions
  - it does not fail stability or calibration checks
- if neither side has clean dominance:
  - keep both in lower-ranked states
  - or keep the symbol out of the retained list entirely

Expected outcome:

- fewer weak same-symbol side flips
- fewer top-slice ideas that barely outrank the other side on raw score

## Phase 4: Tighten Core First

Goal:

- address the clearest failure before broader tuning

Why `core` first:

- it has the strongest board-underperforming-watchlist pattern
- it has the strongest current top-tier underperforming lower-tier pattern
- it is less contaminated by intraday 0DTE execution noise
- the recent evidence is already strong enough to justify targeted promotion changes

Core-specific changes:

- require a stronger top-tier promotion margin
- penalize neutral inside-range setups when recent sessions show negative lift
- penalize repeated low-lift `put_credit` promotion when same-session lower-tier ideas on another symbol or side are materially stronger
- do not let raw score alone force a top-tier promotion

Expected outcome:

- top-tier lift should improve materially before any broad scanner tightening

## Phase 5: Add A Coarse Calibration Layer

Goal:

- use post-market feedback without turning the system into a black box

Calibration design:

- additive, bucket-based adjustments
- no opaque model replacement

Initial calibration dimensions:

- profile
- strategy
- score bucket
- setup status
- VWAP regime
- opening-range regime
- candidate state
- rank band
- label

Calibration outputs:

- promotion penalty
- promotion bonus
- side-arbitration bonus
- minimum top-tier margin adjustment

Primary KPI:

- top-tier lift versus next-slice candidates

Expected outcome:

- better ranking and promotion without destroying explainability

## Phase 6: Improve Construction Selectively

Goal:

- add only the construction features that selection still needs after the promotion refactor

Recommended improvements:

- preserve `open_interest_date` and penalize stale OI in ranking
- add `volume / open_interest` context
- add option trade recency
- add quote persistence and quote-age signals
- cluster near-identical structures before final promotion
- consider using more of the snapshot context already available from Alpaca

What not to do yet:

- full machine-learning ranking
- broad profile retuning all at once
- one universal score across `0dte`, `weekly`, and `core`

## Phase 7: Re-rank Execution Separately

Goal:

- execution should choose the best currently executable top-slice idea, not the best stored board position

Changes:

- in [execution.py](../../packages/core/services/execution.py#L1769), replace `position == 1` selection with execution-side ranking across current promotable candidates
- include:
  - reactive quote quality
  - quote freshness
  - credit retention versus scanned midpoint
  - candidate age
  - profile-specific fail-closed rules

Expected outcome:

- less slippage between board intent and executable reality

## Proposed New Decision Flow

Mermaid source:

- [2026-04-11_spread_selection_refactor_plan_decision_flow.mmd](../diagrams/planning/2026-04-11_spread_selection_refactor_plan_decision_flow.mmd)

## Implementation Order

Recommended order:

1. instrumentation and baseline reports
2. canonical `spread_selection.py`
3. persist one ranked opportunity list with candidate states
4. split discovery/promotion/execution scores
5. `core`-only promotion tightening
6. execution-side re-ranking
7. weekly calibration
8. 0DTE tuning only after quote-quality and capture-health gating are stable

This sequence keeps the most valuable changes early and avoids overfitting the noisiest label first.

## Metrics To Track

Track these by label and by date:

- top-slice average modeled PnL
- next-slice average modeled PnL
- top-slice lift versus next slice
- board average modeled PnL
- watchlist average modeled PnL
- board lift versus watchlist
- score monotonicity by score bucket
- score monotonicity by rank band
- side-flip count
- replacement count
- churn ratio
- average quote events per tracked leg
- execution lift versus board-position baseline
- percent of sessions where board is beaten by the watchlist

Primary success signal:

- top-slice lift improves, especially in `core`

Secondary success signals:

- churn falls without collapsing idea discovery
- weekly becomes more monotonic by score bucket
- execution skips low-quality board ideas more often

## What Not To Do First

Do not start with:

- raising global score floors
- tightening every profile at once
- retuning `0dte`, `weekly`, and `core` together
- replacing the current score with an opaque model

The evidence does not support those as the highest-leverage first moves.

## Sources

Internal:

- [scanner.py](../../packages/core/services/scanner.py)
- [live_collector.py](../../packages/core/jobs/live_collector.py)
- [execution.py](../../packages/core/services/execution.py)
- [post_market_analysis.py](../../packages/core/services/post_market_analysis.py)
- [signal_state.py](../../packages/core/services/signal_state.py)
- [alerts/rules.py](../../packages/core/alerts/rules.py)
- [Options Scanner Research Log](../research/options_scanner_research_log.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

External:

- [Alpaca: Options Trading](https://docs.alpaca.markets/docs/options-trading)
- [Alpaca: Historical Option Data](https://docs.alpaca.markets/docs/historical-option-data)
- [Alpaca: Real-time Option Data](https://docs.alpaca.markets/docs/real-time-option-data)
- [Alpaca: Option Chain Endpoint](https://docs.alpaca.markets/reference/optionchain)
- [OIC: Volatility & the Greeks](https://www.optionseducation.org/advancedconcepts/volatility-the-greeks)
