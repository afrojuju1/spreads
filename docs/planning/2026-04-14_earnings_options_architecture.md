# Earnings Options Architecture

Status: proposed

As of: Tuesday, April 14, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)
- [Execution Templates](./2026-04-12_execution_templates.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)
- [Public Data Sources And Calendars](../research/public_data_sources_and_calendars.md)

## Goal

Define how the spreads system should support short-dated and mid-term options strategies around earnings without creating a parallel "earnings scanner" architecture.

This spec focuses on:

- architecture
- policy boundaries
- strategy-family scope
- candidate and persistence shape
- replay and evaluation requirements
- phased rollout

This spec does not define final thresholds or exact builder math.

## Scope

The target holding windows are intentionally short:

- `next_daily`
- `near_term`
- `post_event`

Out of scope for the first implementation:

- long-dated earnings trades
- same-day `iron_condor`
- pre-earnings short premium on single names
- calendars and diagonals

## Universe Source And Cohorts

Earnings opportunities should be calendar-driven, not inventory-driven.

Default source universe:

- all symbols in `calendar_events` with earnings inside the configured lookahead or lookback window
- not limited to symbols currently held
- not limited to the existing manually watched symbol set

Selection sequence:

1. read the earnings calendar cohort
2. apply product and optionability filters
3. apply liquidity and quote-quality prefilters
4. build family-specific candidates only for the surviving names
5. rank opportunities across the cohort on one canonical path

The system should group earnings symbols into event cohorts.

Representative cohort key:

- `event_date`
- `event_session_timing`
- `earnings_phase`

Reason:

- earnings names cluster around the same report sessions
- the system needs to compare opportunities across the cohort, not one symbol at a time in isolation
- quote budgeting and enrichment should be cohort-aware because many names become interesting at once

## Core View

Earnings support should extend the canonical opportunity path:

1. event classification
2. regime classification
3. strategy-family selection
4. horizon selection
5. family-specific candidate construction
6. canonical opportunity ranking
7. family-specific execution template
8. replay and post-close evaluation

Do not build a separate earnings-specific scanner, ranking model, or execution path.

The main architectural requirement is to stop treating a candidate as only:

- `short_symbol`
- `long_symbol`

That assumption is already too narrow for:

- `iron_condor`
- `long_straddle`
- `long_strangle`
- future calendar or diagonal structures

The system should move to one canonical multi-leg opportunity representation with optional convenience fields for simpler families.

ASCII flow:

```text
                         +----------------------+
                         | calendar + market    |
                         | data + vol context   |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | EventSnapshot        |
                         | earnings phase       |
                         | timing confidence    |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | RegimeSnapshot       |
                         | direction / range    |
                         | vol / liquidity      |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | StrategyIntent       |
                         | family ranking       |
                         | phase-aware policy   |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | HorizonIntent        |
                         | next_daily           |
                         | near_term            |
                         | post_event           |
                         +----------+-----------+
                                    |
                                    v
                    +---------------+----------------+
                    | family builder registry        |
                    | debit | straddle | strangle    |
                    | condor (later)                |
                    +---------------+----------------+
                                    |
                                    v
                         +----------------------+
                         | canonical Opportunity|
                         | legs[] primary       |
                         | shared economics     |
                         +----------+-----------+
                                    |
                   +----------------+----------------+
                   |                                 |
                   v                                 v
         +----------------------+         +----------------------+
         | ranking / allocation |         | replay / evaluation  |
         | promotion / gating   |         | by phase / family    |
         +----------+-----------+         +----------------------+
                    |
                    v
         +----------------------+
         | execution template   |
         | family-specific      |
         +----------------------+
```

## Strategy Scope

### Initial Families

First implementation should support:

- `call_debit_spread`
- `put_debit_spread`
- `long_straddle`
- `long_strangle`

Reasoning:

- these are the cleanest first families for single-name earnings
- they fit pre-event and through-event use cases better than short premium
- they avoid forcing a weak condor policy before the system is ready

### Second-Wave Family

Add later:

- `iron_condor`

Initial restriction:

- only `post_event_fresh`
- only after regime reclassification
- only on approved, highly liquid symbols
- never same-day in the base system

### Deferred Families

Defer until later:

- calendar spreads
- diagonal spreads

Reason:

- they require explicit multi-expiry modeling
- the current canonical opportunity shape still carries one `expiration_date`

## Event Model

The existing `event_state` is too coarse for earnings-specific strategy selection.

Add an explicit `EarningsEventPhase` concept:

- `clean`
- `pre_event_runup`
- `through_event`
- `post_event_fresh`
- `post_event_settled`

Representative semantics:

- `clean`: no earnings-sensitive window is active
- `pre_event_runup`: earnings is near, but the selected horizon avoids holding through the report
- `through_event`: the selected horizon includes the report
- `post_event_fresh`: earnings has passed recently and the post-event repricing window is still active
- `post_event_settled`: the symbol has moved back to normal policy treatment

Suggested event snapshot fields:

- `event_type`
- `earnings_phase`
- `event_date`
- `event_session_timing`
- `cohort_key`
- `days_to_event`
- `days_since_event`
- `timing_confidence`
- `source_confidence`
- `expected_move_pct`
- `event_horizon_crosses_report`

This should feed strategy and horizon selection, not sit as an afterthought on the candidate.

## Strategy Policy

Policy should be phase-first, then family-specific.

### `pre_event_runup`

Preferred:

- directional debit spreads

Allowed selectively:

- `long_straddle`
- `long_strangle`

Blocked:

- single-name short premium
- `iron_condor`

### `through_event`

Preferred:

- `long_straddle`
- `long_strangle`
- high-conviction directional debit spreads

Blocked:

- credit spreads on single names
- `iron_condor`

### `post_event_fresh`

Preferred:

- directional debit spreads when trend persists

Allowed selectively:

- `iron_condor` if the symbol has reclassified to neutral/range and IV remains rich
- selective credit spreads when the move has resolved into an orderly grind

### `clean`

Use the normal strategy policy path.

## Horizon Rules

For earnings work, the canonical horizon bands should be constrained to:

- `next_daily`
- `near_term`
- `post_event`

Representative defaults:

- pre-event directional debit: `next_daily` to `near_term`
- through-event long-vol: `next_daily` to `near_term`
- post-event condor: `near_term` or `post_event`

Hard rules:

- no same-day condors
- no carry-style earnings trades in this initiative
- no horizon that accidentally crosses the report when the family is marked `avoid_event`

## Research-Backed Profitability Rules

Do not treat earnings as a plain calendar-entry problem.

The system should require a signal bundle before a family becomes eligible.

### Required Signal Bundle

Recommended shared signal groups:

- `direction_signal`
- `jump_risk_signal`
- `pricing_signal`
- `post_event_confirmation_signal`

Representative inputs:

- call-versus-put implied volatility spread
- call-minus-put options volume imbalance when available
- pre-earnings return momentum
- industry-relative pre-earnings move
- IV percentile and term structure
- expected move versus structure cost
- jump-risk proxies from the option surface
- earnings surprise, revenue surprise, and guidance alignment
- gap size versus implied move
- opening-range hold/fail and abnormal post-event volume

### Rules

#### 1. No blind long-vol through earnings

Research around earnings volatility and recent retail-option evidence both argue against blindly buying straddles or strangles into every report.

Default rule:

- allow `long_straddle` or `long_strangle` only when:
  - `jump_risk_signal` is high
  - `pricing_signal` says the event premium is not already too expensive
  - liquidity is strong enough to avoid giving up the edge in spreads

Interpretation:

- a signal of a big move is not enough by itself
- the move must also look underpriced enough to buy

#### 2. Directional pre-event trades must be signal-led

Pre-event directional trades should not be based on the calendar alone.

Default rule:

- prefer `call_debit_spread` or `put_debit_spread` only when:
  - `direction_signal` is clear
  - the signal aligns with the option market bias
  - move pricing is not too rich for the chosen debit structure

Representative directional signals:

- relatively expensive calls versus puts suggests bullish event positioning
- relatively expensive puts versus calls suggests bearish event positioning
- strong pre-earnings return momentum and industry-relative drift can strengthen the same-side case

#### 3. Post-event trades need confirmation, not chase

Recent high-frequency evidence suggests the immediate post-announcement move in liquid names is often incorporated very quickly.

Default rule:

- do not auto-chase the first print after earnings in liquid names
- require `post_event_confirmation_signal` before post-event directional or neutral trades become eligible

Representative confirmation signals:

- earnings surprise sign and guidance sign agree
- realized gap is below, near, or above the implied move in an interpretable way
- the first regular-session range holds in the same direction
- abnormal volume confirms instead of fading immediately

#### 4. Prefer liquid names even if illiquid paper drift looks larger

Academic PEAD research shows larger paper profits in illiquid names, but much of that edge is consumed by trading costs.

Default rule:

- keep the live system focused on liquid names
- treat illiquid names as research context, not as primary live targets

#### 5. Friday and low-attention announcements deserve separate handling

Investor-inattention research shows Friday earnings have less immediate and more delayed response.

Default rule:

- tag Friday after-hours earnings separately
- widen the post-event observation window before promoting continuation trades
- do not assume the normal post-event timing profile applies

### DTE Rules From The Research

The earnings-related volatility effect is most concentrated in shorter maturities and fades with longer maturities.

Default DTE implications:

- `through_event` families should use the shortest non-zero expiry that cleanly spans the report
- avoid same-day event trades in v1
- avoid paying for extra non-event time unless the thesis explicitly needs it
- post-event directional and neutral trades should stay in `near_term` or `post_event`, not `carry`

Practical bias:

- `through_event`: roughly the nearest listed expiry spanning the event
- `post_event`: short and mid short-dated expiries only

### Time-Based Exit Rules

Retail-option evidence around announcement volatility argues strongly against holding event trades too long.

Default rule:

- long-vol earnings trades should have fast exits
- do not hold `through_event` long-vol structures for weeks after the report
- post-event structures should be re-evaluated quickly once the event repricing and initial confirmation window has passed

### Default Thresholds

Treat these as starting defaults for replay tuning, not as fixed truth.

All shared signal scores should be normalized to `0.00-1.00`.

#### Global Promotion Rules

- missing signal coverage should reduce eligibility, not be filled with optimistic defaults
- if earnings timing confidence is below `medium`, block `through_event` and `post_event` promotion
- reject any candidate with stale quotes or any leg failing the liquidity gate
- reject any candidate with all-leg effective spread cost above roughly `8%-10%` of structure midpoint
- tag Friday after-hours events separately and require a stricter post-event confirmation threshold

#### Initial Threshold Table

| phase / family | signal gate | pricing gate | default DTE | time rule |
| --- | --- | --- | --- | --- |
| `pre_event_runup` `call_debit_spread` | `direction_signal >= 0.65` and options-bias alignment | `pricing_signal >= 0.55` and `debit / width <= 0.60` | ideal `4-12`, max `15` | if not crossing earnings, exit by the final regular session before the report |
| `pre_event_runup` `put_debit_spread` | `direction_signal >= 0.65` and options-bias alignment | `pricing_signal >= 0.55` and `debit / width <= 0.60` | ideal `4-12`, max `15` | same as bullish debit spreads |
| `through_event` `long_straddle` | `jump_risk_signal >= 0.70` | `pricing_signal >= 0.60` and `modeled_move / implied_move >= 1.10` | ideal `2-7`, max `10` | exit on the first post-event repricing window unless confirmation argues for a same-day hold |
| `through_event` `long_strangle` | `jump_risk_signal >= 0.70` | `pricing_signal >= 0.60` and `modeled_move / break_even_move >= 1.05` | ideal `2-7`, max `10` | same fast-exit policy as the straddle |
| `post_event_fresh` directional follow-through | `post_event_confirmation_signal >= 0.65` | `pricing_signal >= 0.55` | ideal `2-10`, max `15` | no auto-entry on the first print; wait for opening-range hold or fail |
| `post_event_fresh` `iron_condor` later | `post_event_confirmation_signal >= 0.70` plus neutral / range regime | `pricing_signal >= 0.60` and rich residual IV | ideal `3-12`, max `15` | only after the initial repricing window; reject if the symbol is still in directional discovery |

#### Supporting Defaults

- use at least `2` populated sub-signals before promoting a pre-event directional or jump-risk trade
- use at least `3` populated sub-signals before promoting a post-event confirmation trade
- require stronger confirmation for Friday after-hours reports:
  - raise `post_event_confirmation_signal` by about `0.05`
  - delay earliest post-event promotion until the first normal regular-session confirmation block
- prefer the shortest admissible expiry when multiple expiries clear the same threshold band

## Candidate Construction

Candidate construction should move to a family registry:

- one registry entry per `strategy_family`
- shared contract filtering and quote-quality checks
- family-specific strike and width selection
- shared normalization into a canonical `Opportunity`

Required builder outputs:

- `legs`
- `entry_mid`
- `entry_natural`
- `max_profit`
- `max_loss`
- `capital_usage`
- `expected_move_alignment`
- `iv_context`
- `liquidity_metrics`
- `family_specific_metrics`

Family notes:

- debit spreads need directional strike selection and debit caps
- straddles need ATM pairing and explicit move-pricing checks
- strangles need symmetric or intentionally asymmetric wing selection
- condors need four-leg symmetry and stricter all-leg quote persistence

## Leg Selection Rules

Leg selection should happen only after:

- `StrategyIntent` picks the family
- `HorizonIntent` narrows the admissible expiry window

The builder should not search the full chain for arbitrary "good" combinations first.

The correct sequence is:

1. choose family
2. choose admissible expiry
3. enumerate a small strike lattice around ATM and target deltas
4. score candidate leg sets with family-specific rules
5. emit the best normalized opportunities

### Shared Leg Gates

Every family builder should apply the same hard gates before scoring any leg set:

- earnings phase compatibility
- product-policy compatibility
- horizon compatibility with the report timing
- same-expiry legs in v1
- fresh quotes for every required leg
- acceptable per-leg and whole-structure spread quality
- minimum liquidity and quote persistence
- acceptable position cost or risk inside budget
- broker support for the resulting order structure

### `call_debit_spread`

Use when:

- thesis is bullish
- earnings phase is `pre_event_runup`, `through_event`, or `post_event_fresh`

Representative strike logic:

- long call near ATM or slightly ITM
- short call further OTM
- start around:
  - long leg delta: `0.45-0.65`
  - short leg delta: `0.20-0.35`

Selection conditions:

- debit remains inside the configured debit cap
- upside is not over-capped by the short strike
- whole-spread spread quality is acceptable
- expected move and directional thesis still leave enough payoff asymmetry

### `put_debit_spread`

Use when:

- thesis is bearish
- earnings phase is `pre_event_runup`, `through_event`, or `post_event_fresh`

Representative strike logic:

- long put near ATM or slightly ITM
- short put further OTM
- start around:
  - long leg delta: `0.45-0.65`
  - short leg delta: `0.20-0.35`

Selection conditions:

- debit remains inside the configured debit cap
- downside is not over-capped by the short strike
- whole-spread spread quality is acceptable
- expected move and bearish thesis still justify the debit

### `long_straddle`

Use when:

- direction is uncertain
- a large move is expected
- the implied move is not too expensive relative to our move view

Representative strike logic:

- choose the most liquid same-expiry ATM call
- choose the most liquid same-expiry ATM put

Selection conditions:

- call and put are both liquid and fresh
- combined debit is acceptable versus expected move
- no major quote detachment on either side
- the builder should reject obviously over-priced event premiums

### `long_strangle`

Use when:

- direction is uncertain
- move exposure is desired
- ATM straddle is too expensive or too crowded

Representative strike logic:

- same-expiry OTM call and OTM put
- start around:
  - call delta: `0.20-0.35`
  - put delta: `0.20-0.35`

Selection conditions:

- debit is materially lower than the straddle
- break-evens are still reachable under the move model
- liquidity on both wings is acceptable

Preference rule:

- prefer `long_straddle` unless `long_strangle` buys a meaningful debit reduction or better move asymmetry

### `iron_condor`

Use later, only when:

- earnings phase is `post_event_fresh`
- regime has reclassified to neutral/range
- IV is still rich enough to sell premium

Representative strike logic:

- choose same-expiry short call and short put outside the post-event range / move envelope
- choose protective long wings at a fixed width beyond the shorts
- start around short deltas of `0.10-0.20`

Selection conditions:

- four-leg quote completeness and persistence
- acceptable side-to-side symmetry
- acceptable retained credit after live slippage checks
- reject when one side dominates enough that the structure should really be a single credit spread

### Family Scoring Dimensions

After hard filters, score admissible leg sets on:

- event fit
- vol fit
- expected-move pricing fit
- liquidity and quote persistence
- execution quality
- risk efficiency
- directional or symmetry fit
- complexity penalty

## Default Decisions To Close Gaps

The leg rules are not the only area that benefits from early clarification.

These defaults should be treated as the v1 implementation contract unless later evidence forces a change.

### 1. Move-Pricing Model

Default for v1:

- use one canonical base metric: `atm_straddle_midpoint`
- compute it on the selected expiry from `HorizonIntent`
- store:
  - `expected_move_value`
  - `expected_move_pct`
  - `expected_move_method`
  - `move_pricing_ratio`
- for debit and long-vol families, `move_pricing_ratio` should compare total structure cost against `expected_move_value`
- for post-event condors, compare short-strike distance and break-even distance against a post-event move envelope derived from the selected expiry
- do not ship a second parallel move-pricing method in v1

Implementation rule:

- if a better model is added later, keep `atm_straddle_midpoint` as a persisted baseline for comparability in replay

### 2. Earnings Timing Confidence

Default for v1:

- define:
  - `timing_confidence = high | medium | low | unknown`
- with the current source stack, most earnings events should be expected to land in `low` or `unknown` unless independently strengthened later
- when timing confidence is `low` or `unknown`:
  - block `through_event` families
  - block `post_event_fresh` family activation
  - allow only `avoid_event` horizons when the selected expiry clearly does not cross the report
- if session timing is unknown on the event date:
  - require a conservative buffer so the chosen expiry is clearly before or clearly after the event window

Operational implication:

- the current low-confidence earnings source is enough for guarded `avoid_event` logic
- it is not enough to trust aggressive `through_event` or post-event reclassification by default

### 3. Canonical Opportunity Metrics Schema

Default for v1:

- every opportunity must emit these shared metric groups:
  - `entry_pricing`
  - `risk_metrics`
  - `event_metrics`
  - `vol_metrics`
  - `liquidity_metrics`
  - `family_specific_metrics`
- minimum required ranking metrics:
  - `entry_mid`
  - `entry_natural`
  - `max_profit`
  - `max_loss`
  - `capital_usage`
  - `earnings_phase`
  - `timing_confidence`
  - `event_horizon_crosses_report`
  - `expected_move_value`
  - `expected_move_pct`
  - `move_pricing_ratio`
  - `quote_quality_score`
  - `quote_persistence_score`
  - `structure_spread_pct`
- family-specific metrics belong under `family_specific_metrics` only
- ranking should use only documented shared metrics plus a small family-specific overlay, not hidden ad hoc fields

Examples:

- debit spreads: width, long-delta, short-delta, capped-upside ratio
- straddles: ATM symmetry score, total debit versus move
- strangles: wing deltas, break-even asymmetry
- condors: wing symmetry, side-balance score, retained credit ratio

### 4. Candidate Identity And Dedup

Default for v1:

- candidate identity must be canonical and leg-based
- define a deterministic `structure_fingerprint` as:
  - `root_symbol`
  - `strategy_family`
  - `expiration_fingerprint`
  - canonical ordered `legs[]`
- `legs[]` order must be family-defined and stable
- all dedup logic should operate on `structure_fingerprint`, not `short_symbol` / `long_symbol`

Near-duplicate collapse defaults:

- debit spreads: collapse same expiry, same direction, same width bucket, and near-identical long strike
- straddles: keep only one primary ATM structure per expiry
- strangles: collapse by expiry plus delta-bucket pair
- condors: collapse by expiry plus short-delta bucket plus width bucket

Selection rule:

- retain the highest-scoring candidate in each duplicate bucket and discard the rest before canonical ranking

### 5. Execution Price Policies By Family

Default for v1:

- use one family-specific midpoint-to-natural ladder
- define `quoted_edge = abs(natural - midpoint)`
- directional debit spreads:
  - start at midpoint
  - max pay = `midpoint + 0.33 * quoted_edge`
- long straddle / long strangle:
  - start at midpoint
  - max pay = `midpoint + 0.25 * quoted_edge`
- post-event condor:
  - start at midpoint credit
  - min acceptable credit = `midpoint - 0.20 * quoted_edge`
- if quoted edge is extremely wide, skip rather than widen the ladder
- if any required leg goes stale, abort immediately

Risk-control rule:

- do not infer execution aggressiveness from opportunity score
- execution aggressiveness must come from family and style only

### 6. Replay Segmentation Contract

Default for v1:

- every replayable earnings opportunity must carry these tags:
  - `earnings_phase`
  - `timing_confidence`
  - `event_horizon_crosses_report`
  - `strategy_family`
  - `horizon_band`
  - `days_to_event_bucket`
  - `days_since_event_bucket`
  - `dte_bucket`
  - `expected_move_pct_bucket`
  - `move_pricing_ratio_bucket`
  - `iv_regime_bucket`
  - `quote_quality_bucket`
- required comparison outputs:
  - candidate count
  - promotable count
  - blocked count
  - skipped-for-execution count
  - modeled close PnL
  - modeled final PnL
  - live fill-quality metrics when available

Policy rule:

- no earnings family should ship live until replay can segment and compare it on these dimensions

### 7. Live And Replay Parity

Default for v1:

- use one shared family builder registry for live and replay
- use one shared strike-selection and scoring path for live and replay
- live-only differences are allowed only in:
  - quote freshness
  - execution gating
  - broker capability checks
- replay should not silently substitute a different builder path when live data fields are absent
- when replay lacks a live-only field, mark the opportunity as less executable; do not change the family logic

Architectural rule:

- one family, one builder path, one normalization path

### 8. Calendar Service Upgrade

Default for v1:

- updating `calendar_events` is part of this initiative
- the service should become good enough to support:
  - cohort reads
  - stronger session-timing handling
  - refresh and reconciliation
  - timing confidence classification
- live `through_event` trading should remain blocked until the upgraded calendar path can produce acceptable timing confidence

Implementation rule:

- treat calendar quality as a system dependency, not a footnote on candidate ranking

### 9. Calendar-Driven Scheduling

Default for v1:

- earnings work should be scheduled around cohorts, not around owned symbols
- the system should expect many symbols to become active around the same report session
- prefiltering and quote budgeting should happen before deep option enrichment
- ranking should compare candidates across the cohort so the best event opportunities naturally rise together

Operational rule:

- when many names report around the same time, spend quote and compute budget on the strongest liquid slice, not evenly across the full calendar cohort

## Canonical Opportunity Shape

The system should promote `legs[]` to the primary contract representation.

Recommended canonical fields:

- `root_symbol`
- `strategy_family`
- `style_profile`
- `horizon_band`
- `expiration_set`
- `legs[]`
- `entry_pricing`
- `risk_metrics`
- `event_metrics`
- `vol_metrics`
- `liquidity_metrics`

Compatibility rule:

- keep legacy `short_symbol` and `long_symbol` only as optional convenience fields for two-leg strategies during migration
- do not make new families depend on them

This matters because current live storage and execution records still assume two primary legs in several places.

## Persistence And Execution Boundaries

The execution layer already supports multi-leg order payloads, but several persistence and reporting surfaces remain two-leg oriented.

That means the rollout should treat multi-leg canonicalization as a first-class milestone, not cleanup.

Required migration areas:

- candidate identity
- collector-cycle candidate rows
- scan-run candidate rows
- execution attempts
- replay opportunities
- position enrichment and session-position summaries
- broker-sync and quote-capture helpers

Architectural rule:

- persist canonical `legs_json`
- persist family-specific metrics in `strategy_metrics_json`
- treat `short_symbol` / `long_symbol` as compatibility fields only where needed during migration

## Broker And Product Gates

Before a family is eligible, require all of:

- product-policy approval
- earnings-phase compatibility
- live quote completeness for all required legs
- acceptable all-leg spread quality
- broker approval for the order structure

Practical initial product scope:

- single-name earnings families: liquid single names sourced from the earnings calendar
- post-event condors: no static held-symbol list; require much stricter quantitative liquidity and quote-quality gates than directional earnings families

## Replay And Evaluation

Use `uv run spreads replay` as the canonical evaluation path.

Replay output should be able to segment results by:

- `earnings_phase`
- `strategy_family`
- horizon band
- DTE bucket
- IV regime
- expected move versus realized move
- all-leg quote quality bucket
- event timing confidence bucket

Core questions replay should answer:

1. Which family wins by earnings phase?
2. Does the best family change by IV regime?
3. Are long-vol families being entered only when move-pricing is favorable enough?
4. Are post-event condors only appearing after true range reclassification?
5. Does quote quality degrade enough on complex structures to justify skipping more often?

## Rollout Plan

ASCII rollout:

```text
Phase 1  multi-leg foundation
  current two-leg fields
        |
        v
  canonical legs[] + compatibility fields
        |
        v
Phase 2  earnings event classification
  clean / pre_event_runup / through_event / post_event_fresh
        |
        v
Phase 3  first earnings families
  call_debit_spread
  put_debit_spread
  long_straddle
  long_strangle
        |
        v
  replay-only comparison
        |
        v
  guarded live promotion
        |
        v
Phase 4  post-event neutral premium
  iron_condor
  post-event cohort sourced
  strict quote-quality gates
        |
        v
Phase 5  later extensions
  calendars / diagonals
  only after multi-expiry support
```

### Phase 1: Canonical Multi-Leg Foundation

- make `legs[]` the primary candidate and opportunity representation
- reduce reliance on `short_symbol` / `long_symbol`
- make persistence, replay, and position views tolerate 2+ legs cleanly

### Phase 2: Earnings Event Classification

- add earnings phase classification
- add event-timing rules that distinguish pre-event, through-event, and post-event
- surface event confidence explicitly

### Phase 3: First Earnings Families

- add `call_debit_spread`
- add `put_debit_spread`
- add `long_straddle`
- add `long_strangle`
- seeded weekly live collectors now exist for `call_debit`, `put_debit`, `long_straddle`, and `long_strangle`
- `long_straddle` and `long_strangle` are currently live-observed but shadow-only
- evaluate with replay before live promotion

### Phase 4: Post-Event Neutral Premium

- add `iron_condor` only for `post_event_fresh`
- require strict liquidity and quote-persistence gates
- source candidates from the post-event earnings cohort, not from a static held-symbol list

### Phase 5: Later Extensions

- consider calendars and diagonals only after explicit multi-expiry support

## Implementation Order

Keep the build sequence dependency-first.

### 1. Calendar And Cohort Inputs

Modules:

- `src/spreads/integrations/calendar_events/`
- `src/spreads/services/selection_terms.py` if shared selection windows need extension

Work:

- strengthen earnings timing and session handling
- add `timing_confidence`
- add cohort reads keyed by event date, session timing, and phase
- expose a calendar-driven earnings universe

### 2. Canonical Multi-Leg Opportunity Shape

Modules:

- `src/spreads/domain/opportunity_models.py`
- `src/spreads/services/opportunity_replay.py`
- `src/spreads/services/opportunity_scoring.py`

Work:

- make `legs[]` primary
- add the shared metric groups and `structure_fingerprint`
- keep `short_symbol` and `long_symbol` as compatibility fields only

### 3. Storage And Migration Layer

Modules:

- `src/spreads/storage/models.py`
- `src/spreads/storage/collector_models.py`
- `src/spreads/storage/execution_models.py`
- repositories touching candidate and position persistence

Work:

- persist canonical `legs_json`
- persist family metrics in `strategy_metrics_json`
- migrate candidate identity and dedup off two-leg fields

### 4. Family Builder Registry

Modules:

- new family-builder surface under `src/spreads/services/`
- current chain and quote helpers reused from scanner and market-data services

Work:

- shared prefilters
- shared expiry selection from `HorizonIntent`
- add builders for:
  - `call_debit_spread`
  - `put_debit_spread`
  - `long_straddle`
  - `long_strangle`

### 5. Replay And Evaluation Contract

Modules:

- `src/spreads/services/opportunity_replay.py`
- replay CLI surfaces

Work:

- add earnings-phase tags
- add move-pricing and timing-confidence buckets
- compare family outcomes by cohort, phase, and horizon

### 6. Live Selection And Execution Integration

Modules:

- `src/spreads/services/live_selection.py`
- `src/spreads/services/execution.py`
- `src/spreads/services/session_positions.py`
- quote and trade capture helpers

Work:

- use the shared builder registry
- enforce family-specific pricing gates
- make position and execution summaries handle 2+ legs cleanly

### 7. Post-Event Condor Wave

Modules:

- same builder, replay, and execution surfaces above

Work:

- add `iron_condor`
- keep it restricted to `post_event_fresh`
- require stricter liquidity, symmetry, and retained-credit checks before promotion

## Open Decisions

Only a few details remain intentionally open:

- exact lookahead and lookback windows for cohort formation
- exact quantitative liquidity thresholds for post-event condors versus directional families
- whether post-event credit spreads should ship in the same wave as condors or one wave earlier

## Live Iron Condor TODO

- [x] audit live collector, persistence, and scheduler seams for `iron_condor`
- [x] implement `iron_condor` scanner origination and candidate metrics
- [x] implement condor quote/mark/economics and execution validation on the shared multi-leg path
- [x] add condor e2e coverage across scanner, scoring, execution, and session-position sync
- [x] expose condors in live scheduling/shadow flow without bypassing existing policy gates

## Live Notes

- The seeded weekly debit collector jobs are `live_collector:explore_10_call_debit_weekly_auto` and `live_collector:explore_10_put_debit_weekly_auto`.
- The seeded weekly long-vol collector jobs are `live_collector:explore_10_long_straddle_weekly_auto` and `live_collector:explore_10_long_strangle_weekly_auto`.
- The seeded weekly condor collector job is `live_collector:explore_10_iron_condor_weekly_auto`.
- Debit spreads are live-capable on the shared path. Long-vol families currently stay shadow-only by seeded execution policy and explicit live-execution gating.
- Condors stay restricted to `post_event_fresh` and still flow through the normal earnings policy and signal gates.
- `combined` still means call/put credit spreads only. Debit, long-vol, and condor families run on their own collector jobs.
- The seeded condor and long-vol jobs are shadow-only until live policy is enabled deliberately.
- When live collector job definitions change, run `uv run spreads jobs seed`.
- When worker- or scheduler-imported scanner/shared runtime code changes, restart `worker-main`, `worker-collector`, and `scheduler`.
- Minimum verification for live family changes:
  - `uv run python -m unittest discover -s tests -p 'test_*e2e.py'`
  - `docker compose ps worker-main worker-collector scheduler`
  - `docker compose logs --since=2m worker-main worker-collector scheduler`

## Success Criteria

The architecture is working when:

- earnings strategies use the same canonical policy path as other families
- new families do not require new special-case storage shapes
- replay can compare earnings families by phase and DTE cleanly
- pre-event short premium on single names is blocked by policy, not operator memory
- post-event condors only appear after explicit reclassification and on symbols that clear the stricter quantitative liquidity gates
