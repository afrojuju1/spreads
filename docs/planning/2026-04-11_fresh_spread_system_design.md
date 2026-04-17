# Fresh Spread Opportunity System Design

Status: supporting design reference (implementation refreshed on 2026-04-17)

As of: Friday, April 17, 2026

Related:

- [System Architecture](../current_system_state.md)
- [Spread Selection Review And Refactor Plan](./2026-04-11_spread_selection_refactor_plan.md) - historical implementation review before the `scanners/` and `collections/` package split
- [Regime Detection Specification](./2026-04-11_regime_detection_spec.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)
- [Portfolio Allocation Specification](./2026-04-12_allocation_spec.md)
- [Execution Templates](./2026-04-12_execution_templates.md)
- [Evaluation And Rollout Plan](./2026-04-12_evaluation_and_rollout_plan.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)
- [0DTE System Architecture](./0dte_system_architecture.md)
- [Trading Engine Architecture](./trading_engine_architecture.md)
- [Signal State Platform](./signal_state_platform.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Goal

Describe how the spreads system should be designed if built fresh today, after pressure-testing the initial clean-sheet design against:

- recent production lessons
- recent post-market evidence
- Alpaca's documented options-data limits
- options market-structure and assignment realities
- the need to control portfolio risk, not just candidate ranking

This version replaces the earlier simpler design with a harder one. The central changes are:

- `board` and `watchlist` are views on one canonical opportunity store, not separate selection systems
- regime detection is explicit and produces a structured `RegimeSnapshot`
- strategy selection is a separate policy layer, not an implicit side effect of spread scoring
- fixed labels like `0dte`, `weekly`, and `core` are no longer primary design objects
- execution style and risk posture live in `style_profile`, while actual expiration choice lives in `HorizonIntent`
- the system supports multiple strategy families through one canonical intent and opportunity path
- ranking is style-local first, then portfolio allocation happens above it
- signal generation uses both stock-led and option-led paths
- market-data quality, quote budgeting, and execution quality are first-class architectural concerns
- calibration is offline-first and guarded against overfitting

This document is not the canonical source of truth for the overall runtime architecture. That role belongs to [System Architecture](../current_system_state.md), which owns current service boundaries and top-level runtime topology.

This document now serves two purposes:

- describe the clean target architecture
- map that target to the current module ownership in `packages/core/services`

Where the current system has not reached the clean-sheet split yet, this document calls that out explicitly instead of treating planned components as already live.

## Core Design Principles

1. Use one canonical opportunity store.
2. Treat `board` and `watchlist` only as presentation states on top of one selection path.
3. Separate signal generation, regime detection, strategy selection, candidate construction, style ranking, portfolio allocation, and execution.
4. Use both stock-led and option-led signal paths.
5. Keep regime detection separate from strategy selection. Regime should describe the market, not directly pick a trade.
6. Support multiple strategy families through one registry and one canonical opportunity contract.
7. Use `style_profile` for execution and risk posture, and `HorizonIntent` for dynamic DTE selection.
8. Treat legacy buckets like `0dte`, `weekly`, and `core` as reporting labels, not core design primitives.
9. Make market-data quality a first-class service, not a helper.
10. Make product structure and assignment risk first-class policy inputs.
11. Separate modeled opportunity quality from realized execution quality.
12. Fail closed when live data or execution safety is not good enough.
13. Keep the full decision path explainable, auditable, and calibratable.

## System Summary

The system should answer seven questions cleanly:

1. Which symbols and sides are interesting right now?
2. What regime are those symbols actually in?
3. Which strategy families fit that regime and execution style?
4. What expiration horizon should each candidate target?
5. Which concrete candidates are structurally worth retaining?
6. How much portfolio risk, if any, should be allocated to them?
7. Which candidate is actually executable now, on this product, with current live quotes?

The system should move through these stages:

1. detect meaningful stock-led and option-led state changes
2. build a structured `RegimeSnapshot`
3. generate ranked `StrategyIntent`s for each symbol and `style_profile`
4. generate `HorizonIntent` for each admissible strategy family
5. arm only the symbols, sides, strategy families, and horizons worth options work
6. build valid family-specific candidates
7. retain candidates with broad discovery logic
8. rank retained candidates inside each `style_profile`
9. allocate portfolio risk across styles, symbols, strategy families, and horizons
10. expose the resulting top slice as `board` and the next slice as `watchlist`
11. convert allocated opportunities into execution intents
12. submit only the best executable candidate
13. evaluate opportunity quality and execution quality separately
14. feed guarded calibration back into the ranking and execution layers

## High-Level Architecture

Mermaid source:

- [2026-04-11_fresh_spread_system_design_high_level_architecture.mmd](../diagrams/planning/2026-04-11_fresh_spread_system_design_high_level_architecture.mmd)

## Runtime Flow

Mermaid source:

- [2026-04-11_fresh_spread_system_design_runtime_flow.mmd](../diagrams/planning/2026-04-11_fresh_spread_system_design_runtime_flow.mmd)

## Current Implementation Map

The current backend no longer revolves around monolithic `scanner.py` or `live_collector.py` files. The live collector path is now split into focused packages, and this architecture should be read through those owners first.

| Concern | Current owner | Current state |
|---|---|---|
| Symbol and strategy scanning | `services/scanners/` | `service.py` owns the CLI entrypoint, `runtime.py` assembles per-symbol market slices and strategy runs, `builders/` owns family-specific construction and ranking, and `postprocess.py` owns data-quality and calendar annotations. |
| Collection orchestration | `services/collections/` | `runtime.py` owns collector entrypoints, `cycle.py` owns one collection cycle, `scanning.py` owns universe aggregation, and `capture/` owns quote, trade, and UOA capture helpers. |
| Live selection and state assignment | `services/live_selection.py`, `services/opportunity_scoring.py`, `services/candidate_policy.py` | The live path already computes `discovery_score`, `promotion_score`, `execution_score`, `promotable` and `monitor` state, selection memory, and profile-specific deployment gates. |
| Canonical opportunity persistence | `services/opportunity_generation.py`, `services/opportunities.py`, `storage/signal_repository.py` | Collector cycles persist the canonical live opportunity set, and options-automation runtimes project runtime-owned opportunities from that same cycle source instead of creating a parallel selection system. |
| Pipeline identity and policy metadata | `services/live_pipelines.py`, `services/runtime_identity.py`, `services/pipelines.py` | Labels, pipeline ids, `style_profile`, `horizon_intent`, and `product_class` are normalized here and reused across CLI and API visibility. |
| Health and operator diagnostics | `services/live_runtime.py`, `services/live_collector_health/`, `services/ops/` | Session detail, capture health, selection summaries, tradeability, jobs view, and trading overview all read from persisted collector and runtime state instead of parallel ad hoc logic. |
| Execution policy and attempt lifecycle | `services/execution/` | `__init__.py` still owns submit and refresh flows, `policy.py` owns normalized execution policy, `attempts.py` owns persisted attempt, order, and fill lifecycle, and `guard.py` owns open-attempt safety checks. |
| Replay and calibration surfaces | `services/opportunity_replay/`, `services/post_market_analysis.py` | Replay reconstructs `RegimeSnapshot`, `StrategyIntent`, `HorizonIntent`, allocation, and execution intents from stored cycle data, while post-market analysis remains the canonical closed-session diagnostic surface. |

### Current Boundaries Versus Target Design

- The package split for scanners, collections, execution, and live-collector health is current and should be treated as canonical.
- The live path already has a canonical opportunity store and separate runtime-owned opportunity projections. `board` and `watchlist` are now legacy presentation terms over persisted `promotable` and `monitor` states.
- Explicit persisted `RegimeSnapshot`, `StrategyIntent`, and `HorizonIntent` records are not yet first-class objects in the live collector path. Today they are strongest in replay and policy metadata, and only partly materialized in persisted opportunity rows.
- Portfolio allocation is still lighter than the target design. Current live ranking and execution gating carry more responsibility than a fully separate allocator should long term.
- Execution planning and execution submission still share a broad owner in `services/execution/__init__.py`. The package split is cleaner than before, but that surface remains a candidate for a later hard cut if order construction or replacement policy grows materially.

## Main Components

The sections below describe the target clean-sheet component model. Use the implementation map above for the current module ownership.

### 1. Stock-Led Signal Engine

Purpose:

- detect stock-structure changes that may justify options work

Responsibilities:

- detect VWAP, opening-range, trend, reversal, and session-extreme states
- summarize gap, momentum, and event context
- emit stock-led evidence into regime detection

### 2. Option-Led Signal Engine

Purpose:

- capture information that stock structure alone will miss

Responsibilities:

- detect unusual option trade activity
- detect abnormal option volume relative to baseline and open interest
- detect skew and term-structure dislocations
- detect strike-level quote-quality concentration
- detect spread-quality deterioration or improvement

This path should not replace the stock-led path. It should run in parallel and contribute to regime detection.

### 3. Regime Engine

Purpose:

- convert raw stock-led and option-led evidence into one structured market-state object

Outputs:

- `RegimeSnapshot`

The `RegimeSnapshot` should capture:

- `direction_bias`: bullish, bearish, neutral
- `trend_strength`
- `intraday_structure`: trend, range, breakout, reversal, unstable
- `vol_level`
- `vol_trend`
- `iv_vs_rv`
- `event_state`
- `liquidity_state`
- `confidence`
- supporting evidence

The regime engine describes the market. It does not select a trade by itself.

Operational detail for regime computation lives in:

- [Regime Detection Specification](./2026-04-11_regime_detection_spec.md)

### 4. Strategy Policy Engine

Purpose:

- decide which strategy families fit the current regime, `style_profile`, product policy, and portfolio context

Outputs:

- ranked `StrategyIntent` records

Responsibilities:

- map `RegimeSnapshot` to admissible strategy families
- determine whether the correct action is directional, neutral, or pass
- rank strategy families by desirability and confidence

This is the main abstraction that prevents regime logic from leaking into builder or execution heuristics.

Operational strategy selection policy lives in:

- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)

### 5. Horizon Selection Engine

Purpose:

- choose the desired expiration horizon after regime and strategy family are known

Outputs:

- `HorizonIntent`

Responsibilities:

- map regime, strategy family, event state, volatility state, and liquidity state into target DTE ranges
- prefer the correct expiration type, such as same-day daily, near-term weekly, or later carry horizon
- block horizon choices that are structurally wrong for the product or regime
- keep DTE selection dynamic instead of baking it into labels like `0dte`, `weekly`, or `core`

This is the layer that turns a loose concept like "short-dated" into an explicit operating target.

Operational horizon policy lives in:

- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)

### 6. Armed Symbol Registry

Purpose:

- keep option enrichment selective and bounded

Responsibilities:

- maintain armed symbol-side-style-strategy-horizon records
- merge stock-led and option-led reasons
- expire stale opportunities
- enforce cooldowns
- prevent repeated churn on the same symbol-style-strategy-horizon set

### 7. Market Data Quality Service

Purpose:

- own whether the system should trust the current live data

Responsibilities:

- track quote freshness
- track quote persistence
- track contract opening readiness
- track subscription coverage versus budget
- track option-stream health and gaps
- publish data-quality states used by ranking and execution

This is required because Alpaca's options feed is top-of-book only, options quotes are subscription-limited, and option quote subscriptions cannot use a wildcard.

### 8. Option Enrichment Engine

Purpose:

- do targeted option work only for armed symbol-style-strategy-horizon sets

Responsibilities:

- load relevant contract metadata
- preserve `open_interest` together with `open_interest_date`
- narrow strike and expiration windows
- enrich with chain snapshots
- enrich with targeted live quotes and trades
- attach surface context from implied-volatility and realized-volatility services

### 9. Vol Surface And Realized Vol Service

Purpose:

- supply a better volatility context than a simple ATM expected-move shortcut

Responsibilities:

- produce realized volatility features by horizon
- estimate expected move by selected horizon and style
- summarize skew and term-structure state
- provide regime-aware volatility buckets to strategy policy, ranking, and calibration

### 10. Strategy Registry And Builders

Purpose:

- give each strategy family one canonical implementation path

Supported strategy families should include:

- `long_call`
- `long_put`
- `call_credit_spread`
- `put_credit_spread`
- `call_debit_spread`
- `put_debit_spread`
- `iron_condor`

Each strategy family should supply:

- applicability rules
- construction rules
- strategy-specific hard gates
- feature extractors
- risk model
- execution template
- exit template

This is the abstraction that lets the system support calls, puts, verticals, and condors without parallel pipelines.

### 11. Style Ranking Engine

Purpose:

- own style-local ranking, strategy arbitration, and state assignment

Responsibilities:

- compute `discovery_score`
- retain candidates in the canonical store
- compute `promotion_score`
- assign `style_rank`
- assign `state`
- attach `state_reason`
- arbitrate directional side competition and neutral strategy competition inside each symbol-style set

This engine should rank inside each style profile. It should not decide capital allocation across styles.

### 12. Canonical Opportunity Store

Purpose:

- persist the single source of truth for each cycle's opportunities

Responsibilities:

- store all retained and discarded candidates with reasons
- store regime, intent, and score evidence
- store style-local rank
- store allocation state
- expose derived board and watchlist views

### 13. Portfolio Allocation And Risk Budget Engine

Purpose:

- decide whether a highly ranked opportunity should actually consume risk

Responsibilities:

- cap concentration by symbol, sector, product, strategy family, and side
- control correlation across open and pending positions
- account for account buying power and margin usage
- enforce per-style and per-session risk budgets
- limit event overlap and macro-session exposure
- allocate size and active-slot ownership

Ranking without allocation is not enough once multiple strategy families compete for risk.

### 14. Product And Assignment Policy Engine

Purpose:

- enforce rules that depend on option product structure rather than generic scoring

Responsibilities:

- distinguish American versus European exercise
- distinguish physical versus cash settlement
- account for early-assignment risk
- account for ex-dividend and expiration effects
- prefer structurally cleaner products when strategy intent allows
- default `iron_condor` to cash-settled index or top-tier ETF products only
- restrict reactive short-premium structures to cash-settled indexes and top-tier ETFs only

Operational product rules live in:

- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)

### 15. Execution Planner

Purpose:

- translate an allocated opportunity into a strategy-specific `ExecutionIntent`

Responsibilities:

- choose the order structure needed for the strategy family
- define entry price policy, timeout policy, and replace policy
- pass strategy-specific execution constraints to the executor

This separates execution planning from raw broker submission.

Operational family-specific execution behavior lives in:

- [Execution Templates](./2026-04-12_execution_templates.md)

### 16. Execution Orchestrator

Purpose:

- choose and submit the best currently executable candidate from the allocated slice

Responsibilities:

- compute `execution_score`
- validate `ExecutionIntent`
- check live quote freshness and completeness
- apply venue-aware order policy
- handle passive versus aggressive behavior
- manage replace ladders, collars, and timeouts
- submit orders and react to broker updates

### 17. Position And Exit Engine

Purpose:

- manage open positions independently from entry logic

Responsibilities:

- refresh marks
- apply strategy-specific stop and target rules
- force-close on time or risk conditions
- reconcile local state versus broker state
- feed realized position outcomes downstream

### 18. Opportunity Outcome Evaluator

Purpose:

- measure whether opportunity ranking and allocation actually worked

Responsibilities:

- evaluate candidates by style-rank band and state
- compare allocated top slice versus lower retained slices
- measure monotonicity of the score layers
- update bounded ranking calibration tables

### 19. Execution Quality Evaluator

Purpose:

- measure whether the live execution policy is actually extracting the edge the opportunity model expected

Responsibilities:

- compare scanned marks to live quotes and actual fills
- evaluate slippage and missed-trade behavior
- update bounded execution-policy tuning

## Core Contracts

The design should make three contracts explicit.

### `RegimeSnapshot`

Purpose:

- represent current market state in a way strategy policy can consume without reading raw bars or quotes

Suggested fields:

- `regime_snapshot_id`
- `symbol`
- `style_profile`
- `direction_bias`
- `trend_strength`
- `intraday_structure`
- `vol_level`
- `vol_trend`
- `iv_vs_rv`
- `event_state`
- `liquidity_state`
- `confidence`
- `supporting_features`

### `StrategyIntent`

Purpose:

- represent the system's current belief about which strategy family fits a symbol and style profile under the current regime

Suggested fields:

- `strategy_intent_id`
- `regime_snapshot_id`
- `symbol`
- `style_profile`
- `strategy_family`
- `thesis_direction`
- `desirability_score`
- `confidence`
- `preferred_dte_range`
- `preferred_width_range`
- `sizing_class`
- `blockers`

### `HorizonIntent`

Purpose:

- represent the target expiration horizon selected after regime and strategy-family selection

Suggested fields:

- `horizon_intent_id`
- `strategy_intent_id`
- `symbol`
- `style_profile`
- `horizon_band`
- `target_dte_min`
- `target_dte_max`
- `preferred_expiration_type`
- `event_timing_rule`
- `urgency`
- `blockers`

### `ExecutionIntent`

Purpose:

- represent the strategy-specific execution plan that gets handed to the trade executor

Suggested fields:

- `execution_intent_id`
- `opportunity_id`
- `strategy_family`
- `order_structure`
- `legs`
- `entry_policy`
- `price_policy`
- `timeout_policy`
- `replace_policy`
- `exit_policy`

## Canonical Data Model

The core entity is now one persisted `Opportunity` row per candidate per cycle, with optional runtime-owned projections for entry automations. The target model should start from the fields we already persist rather than inventing a parallel paper contract.

Current persisted live fields:

| Field | Meaning |
|---|---|
| `opportunity_id` | stable candidate record id |
| `pipeline_id`, `label`, `market_date`, `session_date`, `cycle_id` | canonical pipeline and collector-cycle identity |
| `root_symbol`, `underlying_symbol`, `expiration_date` | instrument identity for the selected candidate |
| `bot_id`, `automation_id`, `automation_run_id`, `strategy_config_id` | runtime ownership when the row is a runtime-owned automation projection |
| `strategy_id`, `strategy_family`, `profile`, `style_profile`, `horizon_intent`, `product_class` | normalized policy identity already derived from runtime and profile metadata |
| `selection_state`, `selection_rank`, `lifecycle_state`, `eligibility_state`, `state_reason` | canonical live rank and state assignment |
| `promotion_score`, `execution_score`, `confidence` | live score outputs used for promotion and execution gating |
| `candidate_identity`, `candidate`, `legs`, `order_payload` | canonical structure payload and execution shape |
| `economics`, `strategy_metrics`, `risk_hints` | normalized ranking, execution, and future allocation inputs |
| `execution_shape`, `evidence`, `reason_codes`, `blockers` | explanation payloads for audit, CLI, and API surfaces |
| `source_cycle_id`, `source_candidate_id`, `source_selection_state` | back-reference from runtime-owned projections to the collector-cycle source row |
| `created_at`, `updated_at`, `expires_at` | lifecycle timestamps |

Target additions that are not yet first-class live records:

- `regime_snapshot_id` and a persisted `RegimeSnapshot`
- `strategy_intent_id` and a persisted `StrategyIntent`
- `horizon_intent_id` and a persisted `HorizonIntent`
- a separate allocation record with explicit `allocation_rank` and `allocation_reason`
- explicit execution-readiness snapshots decoupled from the candidate payload

Derived views:

- `board` = top `allocated` slice that remains executable or nearly executable
- `watchlist` = next retained slice, usually `monitor` or `blocked` with informative reasons
- `alerts` = state transitions, allocation transitions, and material rank transitions

## Scoring Framework

The system should use:

- explicit regime detection
- explicit strategy selection
- three scores
- hard gates
- allocation rules

Scoring starts only after the system has chosen which strategy families are even worth building.

## Strategy Selection Layer

Before candidate scoring begins, the system should produce one or more `StrategyIntent`s from the `RegimeSnapshot`.

`pass` should be treated as a first-class strategy-policy outcome.

### Strategy Selection Rules

Representative starting rules:

- bullish trend plus cheap or expanding volatility:
  - prefer `long_call` or `call_debit_spread`
- bullish grind plus rich or stable volatility:
  - prefer `put_credit_spread`
- bearish trend plus cheap or expanding volatility:
  - prefer `long_put` or `put_debit_spread`
- bearish grind plus rich or stable volatility:
  - prefer `call_credit_spread`
- neutral or range regime plus rich, stable, or compressing volatility:
  - prefer `iron_condor`
- event-heavy or low-confidence regime:
  - prefer `pass`, or allow only the smallest defined-risk structures

The base-system product rule should be:

- `iron_condor` is blocked on single-name equities
- reactive short-premium is restricted to cash-settled indexes and top-tier ETFs only

This layer should produce ranked intents, not one forced trade type.

## Horizon Selection Layer

After strategy selection, the system should choose `HorizonIntent`.

Fixed labels like `0dte`, `weekly`, and `core` should not decide DTE directly. They should at most survive as legacy reporting buckets.

Representative horizon bands:

- `same_day`
- `next_daily`
- `near_term`
- `post_event`
- `swing`
- `carry`

Representative rules:

- long premium and debit structures:
  - choose shorter DTE when conviction is high and the expected move is near-term
  - choose longer DTE when the thesis needs time or volatility is already rich
- credit spreads:
  - choose enough DTE to avoid excessive gamma unless the style is explicitly reactive and the product is index or top-tier ETF
- iron condor:
  - no single-name condors in the base design
  - no reactive condors outside cash-settled indexes and top-tier ETFs
  - prefer `near_term` or `carry` horizons under clean neutral-range regimes

## Stage 0: Data Quality And Product Safety Gates

These should run before candidate ranking is trusted.

If the live-data state or product-policy state fails a hard block, the candidate may still be stored for audit purposes but should not be eligible for ranking or execution.

Hard blocks should include:

- missing required snapshot fields in strict mode
- quote freshness beyond the style or horizon threshold
- incomplete required leg-quote state when the strategy family requires live execution readiness
- contract not open or not yet ready for trading
- quote-subscription coverage missing for required symbols
- product-policy hard block, such as unacceptable assignment or settlement risk
- session or control-plane hard block

The output should be:

- `safe_to_rank`
- `unsafe_to_rank`

with explicit reason codes.

## Stage 1: Structural Eligibility Gates

These are pass/fail structural constraints.

If a candidate fails a hard structural gate, it does not enter the retained opportunity book.

Hard gates should include:

- style-profile eligibility
- horizon-intent DTE window
- allowed underlying and product type
- strategy-family allowed for the current regime, style, and horizon
- minimum and maximum delta band
- minimum and maximum width
- minimum credit
- maximum debit where relevant
- acceptable natural price in the required direction
- minimum return on risk
- maximum relative spread
- hard calendar blocks
- required contract metadata present
- strategy-family-specific structure rules, such as condor balance or single-leg premium limits

The output is:

- `eligible`
- `discarded`

with a reason code stored either way.

## Stage 2: `discovery_score`

Purpose:

- determine whether a candidate is worth retaining in the canonical opportunity store

This score should be broad and permissive. It should preserve optionality and information.

### Discovery Inputs

Use normalized components from `0.0` to `1.0`:

- `delta_fit`
- `short_vs_surface_expected_move`
- `breakeven_vs_surface_expected_move`
- `fill_quality`
- `liquidity_quality`
- `width_fit`
- `dte_fit`
- `return_on_risk_fit`
- `trade_recency`
- `volume_vs_open_interest`

Multipliers:

- `calendar_multiplier`
- `setup_multiplier`
- `data_quality_multiplier`
- `product_safety_multiplier`

Representative initial weights:

- `delta_fit`: `0.18`
- `short_vs_surface_expected_move`: `0.16`
- `breakeven_vs_surface_expected_move`: `0.14`
- `fill_quality`: `0.12`
- `liquidity_quality`: `0.10`
- `width_fit`: `0.07`
- `dte_fit`: `0.06`
- `return_on_risk_fit`: `0.05`
- `trade_recency`: `0.06`
- `volume_vs_open_interest`: `0.06`

Discovery score formula:

```text
discovery_score =
  100 *
  (
    0.18 * delta_fit +
    0.16 * short_vs_surface_expected_move +
    0.14 * breakeven_vs_surface_expected_move +
    0.12 * fill_quality +
    0.10 * liquidity_quality +
    0.07 * width_fit +
    0.06 * dte_fit +
    0.05 * return_on_risk_fit +
    0.06 * trade_recency +
    0.06 * volume_vs_open_interest
  ) *
  calendar_multiplier *
  setup_multiplier *
  data_quality_multiplier *
  product_safety_multiplier
```

### Discovery Decision Rule

- if `discovery_score` is below the style retain floor, candidate is `discarded`
- otherwise candidate is retained in the canonical store

Retain floors should be style-aware:

- `reactive`: high retain floor
- `tactical`: medium retain floor
- `carry`: lower retain floor, because later ranking and allocation should be stricter

## Stage 3: Strategy And Side Arbitration

Purpose:

- decide how strategy families compete inside a symbol-style set

This should happen after discovery retention and before final state assignment.

For each symbol-style set:

1. compare the best retained candidates within each admissible strategy family
2. compare directional families across call and put sides
3. compare neutral families, such as `iron_condor`, against directional alternatives and `pass`
4. determine whether one family or side dominates strongly enough for promotion

### Strategy-Arbitration Inputs

- `promotion_score` gap between competing candidates
- regime alignment
- option-led flow alignment
- strategy-family fit
- side consistency across recent cycles
- churn risk
- calibration bonus or penalty by symbol-side-family regime

### Strategy-Arbitration Rules

- if one directional side exceeds the other by the style-specific dominance margin, that side can be `promotable`
- if a neutral strategy family dominates under a range regime, it can be `promotable` even when neither directional side wins
- if the gap is too small, multiple candidates can remain retained but should usually stay `monitor`
- if all admissible families are weak or contradictory, the symbol-style set should effectively resolve to `pass`

## Stage 4: `promotion_score`

Purpose:

- assign `promotable`, `monitor`, `blocked`, or `discarded`
- determine style-local top-slice membership before allocation

This score should be more selective than `discovery_score`.

### Promotion Inputs

Start with the normalized discovery base and add selection-specific signals:

- `discovery_base`
- `regime_alignment`
- `option_flow_alignment`
- `strategy_family_fit`
- `side_or_structure_dominance`
- `score_stability`
- `quote_persistence`
- `quote_freshness`
- `calibration_adjustment`
- `execution_readiness_hint`
- `product_safety_fit`

Representative initial weights:

- `discovery_base`: `0.24`
- `regime_alignment`: `0.14`
- `option_flow_alignment`: `0.10`
- `strategy_family_fit`: `0.08`
- `side_or_structure_dominance`: `0.10`
- `score_stability`: `0.09`
- `quote_persistence`: `0.07`
- `quote_freshness`: `0.06`
- `calibration_adjustment`: `0.08`
- `execution_readiness_hint`: `0.04`
- `product_safety_fit`: `0.04`

Promotion penalties:

- stale quote penalty
- elevated churn penalty
- regime contradiction penalty
- weak calibration penalty
- event-risk penalty

Promotion score formula:

```text
promotion_score =
  100 *
  (
    0.24 * discovery_base +
    0.14 * regime_alignment +
    0.10 * option_flow_alignment +
    0.08 * strategy_family_fit +
    0.10 * side_or_structure_dominance +
    0.09 * score_stability +
    0.07 * quote_persistence +
    0.06 * quote_freshness +
    0.08 * calibration_adjustment +
    0.04 * execution_readiness_hint +
    0.04 * product_safety_fit
  )
  - churn_penalty
  - stale_quote_penalty
  - contradiction_penalty
  - event_risk_penalty
```

### Promotion Decision Rules

Use explicit candidate states:

- `promotable`
- `monitor`
- `blocked`
- `discarded`

Rule set:

- `promotable`
  - passes hard gates
  - passes data-quality and product-safety gates
  - discovery retained
  - exceeds the style-profile promotion floor
  - strategy-family fit is acceptable
  - side or structure dominance is acceptable
  - no critical quote or execution blocker exists
- `monitor`
  - discovery retained
  - interesting enough to keep
  - below top-slice promotion threshold or not dominant enough for the family and regime
- `blocked`
  - structurally interesting
  - should not be promoted now because of stale quotes, contradictory regime, event conflict, product risk, or execution blocker
- `discarded`
  - below retain threshold or fails hard gates

`board` and `watchlist` remain views:

- `board` = allocated top-ranked `promotable` candidates
- `watchlist` = next-ranked retained candidates, usually `monitor`

## Stage 5: Portfolio Allocation And Risk Budgeting

Purpose:

- decide whether a promotable candidate should actually consume risk

This stage sits above style-local ranking.

### Allocation Inputs

- `promotion_score`
- style-local rank
- strategy family
- open and pending positions
- account equity, cash, buying power, and margin usage
- symbol and sector concentration
- correlation and directional overlap
- event overlap
- product-policy constraints
- session-level risk budget

### Allocation Outputs

- `allocated`
- `not_allocated`

with an explicit `allocation_reason`.

### Allocation Decision Rules

- allocate only if the candidate improves the current portfolio opportunity set after correlation and concentration constraints
- cap same-symbol and same-side stacking unless the style explicitly allows it
- reserve tighter budgets for reactive styles
- allow style-specific slot limits
- refuse allocation when buying power or modeled downside budget is too tight

This stage is where the design stops treating ranking as the same thing as portfolio construction.

## Stage 6: `execution_score`

Purpose:

- choose the best currently executable candidate from the allocated slice

This score should be computed only for currently allocated candidates.

### Execution Inputs

- `promotion_base`
- live price retention versus scanned midpoint
- live natural price quality
- leg quote completeness
- quote freshness
- candidate age
- reactive quote agreement
- venue suitability
- order-policy fit

Representative initial weights:

- `promotion_base`: `0.22`
- `price_retention`: `0.18`
- `natural_price_quality`: `0.14`
- `quote_freshness`: `0.10`
- `leg_quote_completeness`: `0.10`
- `candidate_age`: `0.06`
- `reactive_quote_agreement`: `0.08`
- `venue_suitability`: `0.06`
- `order_policy_fit`: `0.06`

Execution penalties:

- quote staleness
- live price outside allowable floor or ceiling
- missing required leg snapshot
- spread collar or microstructure conflict
- fail-closed penalty

Execution score formula:

```text
execution_score =
  100 *
  (
    0.22 * promotion_base +
    0.18 * price_retention +
    0.14 * natural_price_quality +
    0.10 * quote_freshness +
    0.10 * leg_quote_completeness +
    0.06 * candidate_age +
    0.08 * reactive_quote_agreement +
    0.06 * venue_suitability +
    0.06 * order_policy_fit
  )
  - stale_quote_penalty
  - live_price_penalty
  - missing_snapshot_penalty
  - microstructure_penalty
  - fail_closed_penalty
```

### Execution Decision Rules

- only candidates in `allocated` state are eligible
- compute `execution_score` across all currently allocated candidates
- choose the candidate with the highest valid `execution_score`
- skip submission when no candidate meets the style-specific execution minimum
- execution should not trust style-local rank, horizon choice, or display order alone

## Calibration Framework

Calibration should not replace the score system. It should adjust it, carefully and slowly.

## Opportunity Calibration Inputs

By style profile and label:

- style-rank band
- candidate state
- allocation state
- strategy_family
- setup status
- VWAP regime
- opening-range regime
- volatility regime
- quote-quality bucket
- product type

## Opportunity Calibration Outputs

- additive promotion bonus
- additive promotion penalty
- side-dominance margin adjustment
- style top-slice size adjustment

## Execution Calibration Inputs

- live price retention bucket
- quote-freshness bucket
- fill slippage bucket
- order-policy mode
- venue suitability bucket
- candidate age bucket

## Execution Calibration Outputs

- order aggressiveness adjustment
- replace-ladder adjustment
- submission floor adjustment

## Calibration Guardrails

- require minimum sample size per bucket
- use walk-forward evaluation only
- embargo recent data before promotion into active policy
- decay old observations
- keep all adjustments bounded
- keep style-profile calibrations separate
- do not let calibration override hard risk rules
- shadow new calibrations before activation

## Decision Rules

## 1. Regime Detection Rules

The regime engine should:

- combine stock-led and option-led evidence into one `RegimeSnapshot`
- output a confidence score rather than a forced binary state
- prefer `unstable` or low-confidence regimes over false precision
- preserve supporting evidence for later audit and calibration

## 2. Strategy Selection Rules

For each symbol and style profile:

- the strategy policy engine should rank admissible strategy families
- `pass` should be allowed when no family has enough regime fit or confidence
- directional families should compete against each other and against neutral families only through normalized policy outputs, not raw builder scores
- preferred width bands should come from `StrategyIntent`, while DTE comes from `HorizonIntent`

## 3. Symbol Arming Rules

A symbol-side-style set should be armed only when:

- session is open and style-allowed
- stock-led or option-led signal quality is strong enough
- at least one strategy family is currently admissible
- at least one horizon choice is currently admissible
- recent state change is meaningful, not just drift
- no active cooldown applies

A symbol-side-style set should be disarmed when:

- both signal paths deteriorate
- data-quality state breaks down
- top candidates remain blocked for too long
- cooldown begins after repeated failed promotions or failed execution attempts

## 4. Candidate Retention Rules

After hard gates:

- keep enough candidates to preserve optionality
- do not retain near-identical structures just because they are numerically adjacent
- cluster very similar structures and keep representatives
- store rejected candidates with reasons when useful for later evaluation

Retention should be broad enough to preserve information for later style ranking.

## 5. Style Ranking Rules

For each cycle and each style profile:

- rank all retained candidates after strategy and side arbitration
- assign `promotable` to the style-local top slice that clears promotion rules
- assign `monitor` to the next slice
- assign `blocked` to candidates with edge but current blockers
- assign `discarded` to candidates below the keep line

Style-local top-slice size should be configurable and style-aware.

## 6. Portfolio Allocation Rules

After style-local ranking:

- allocate only the candidates that fit current portfolio risk and account capacity
- do not allow style-local rank alone to bypass concentration rules
- use stronger penalties for macro-event overlap and highly correlated exposures
- allow no allocation at all when the current portfolio context is hostile

This is the main place where the system decides whether not trading is the right answer.

## 7. Replacement And Hysteresis Rules

The system should not replace a currently allocated candidate unless:

- the incoming candidate beats it by the style-specific replacement margin
- or the current candidate falls below a hold floor
- or a blocker invalidates the current candidate

Side flips should require a stronger margin than same-side replacements.

This should be handled by stored state and recent-cycle history, not by a separate board-only heuristic layer.

## 8. Execution Rules

Execution should only occur when:

- the candidate is currently `allocated`
- live quotes are fresh enough
- live price remains inside the configured floor or ceiling for the strategy family
- the required leg snapshot is complete
- the product-policy engine allows entry
- the portfolio allocator still allows the position
- no kill switch or control-plane block applies

Execution should skip, not degrade gracefully, when these fail in styles that must fail closed.

## 9. Style-Specific Fail-Closed Rules

### `reactive`

Fail closed if:

- quote capture is empty
- no current required leg snapshot exists
- price retention falls outside the allowed band
- intraday setup context is missing
- reactive quote path is thin or stale
- macro or event-session rules block the style
- short-premium is attempted only on cash-settled index or top-tier ETF products

### `tactical`

Fail closed if:

- quotes are materially stale
- the chosen strategy family is not dominant enough for the regime
- event or regime contradiction is strong
- portfolio overlap is too concentrated

### `carry`

Fail closed if:

- calibration strongly penalizes the candidate
- lower-ranked alternatives in the same family or thesis are materially better after allocation context
- the current candidate is only marginally top-ranked
- assignment or product-policy constraints make the trade structurally unattractive

## 10. Post-Market Evaluation Rules

After close:

- evaluate opportunity outcomes by style-rank band and state
- compare allocated top slice versus lower retained slices
- compare retained versus blocked where useful
- measure monotonicity of each score layer
- separately measure realized execution quality versus modeled opportunity quality

Questions that matter:

- did the allocated slice beat the lower retained slices
- did higher `promotion_score` improve outcomes
- did the portfolio allocator improve the final set
- did `execution_score` identify the better executable ideas
- did strategy and side arbitration pick the right family and thesis
- did realized fills preserve enough of the modeled edge

## Suggested State Vocabulary

Use one concise state set:

- `promotable`
- `monitor`
- `blocked`
- `discarded`
- `allocated`
- `submitted`
- `open`
- `closed`

Suggested `state_reason` examples:

- `style_top_slice`
- `below_style_slice`
- `strategy_not_dominant`
- `quote_stale`
- `live_price_outside_band`
- `regime_contradiction`
- `cooldown_active`
- `risk_blocked`
- `product_policy_blocked`
- `hard_gate_failed`

Suggested `allocation_reason` examples:

- `portfolio_slot_granted`
- `correlation_too_high`
- `symbol_concentration_blocked`
- `buying_power_blocked`
- `event_overlap_blocked`
- `style_budget_blocked`

## API And Storage Shape

The canonical opportunity store should be the backend source for:

- CLI visibility
- API visibility
- alert generation
- signal-state updates
- execution lookup
- post-market evaluation

One cycle should expose:

- full retained opportunities
- discarded candidates with reason summaries where useful
- style-local ranks
- allocated top slice
- lower retained slices
- blocked candidates
- summary diagnostics

In the current implementation this is persisted through the signal and opportunity tables, with collector-cycle opportunities as the canonical source and runtime-owned automation opportunities as derived projections keyed back to source candidates.

## Implementation Status And Remaining Gaps

Implemented foundations:

1. package-owned scanner decomposition under `services/scanners/`
2. package-owned collection decomposition under `services/collections/`
3. canonical live opportunity persistence and runtime-owned opportunity projection via `services/opportunity_generation.py` and `services/opportunities.py`
4. profile-aware live scoring and selection in `services/opportunity_scoring.py` and `services/live_selection.py`
5. execution policy, attempt persistence, and open-attempt guards under `services/execution/`
6. live session, tradeability, and operator visibility under `services/live_runtime.py`, `services/pipelines.py`, `services/live_collector_health/`, and `services/ops/`
7. replay reconstruction and post-market diagnostics under `services/opportunity_replay/` and `services/post_market_analysis.py`

Remaining hard cuts to reach the full clean-sheet design:

1. persist first-class `RegimeSnapshot`, `StrategyIntent`, and `HorizonIntent` in the live collector path instead of deriving most of them only in replay or identity helpers
2. promote allocation into its own first-class live owner instead of leaving ranking, gating, and execution to share that responsibility
3. make quote-budget and symbol-arming ownership explicit instead of distributing those concerns across collector capture, selection, and recovery helpers
4. split execution planning from broker submission if `services/execution/__init__.py` keeps accumulating policy and order-construction logic
5. close the calibration loop so replay and post-market outputs can safely feed bounded live threshold updates

## What This Design Avoids

- separate board and watchlist policy layers
- execution trusting display order
- alerting reinterpreting candidate quality independently
- one score trying to solve discovery, ranking, allocation, and execution together
- a single stock-only signal path
- a regime engine that directly hard-codes trade types
- fixed DTE labels pretending to be strategy design
- separate builder pipelines for calls, puts, spreads, and condors
- a single global rank that ignores style and horizon differences
- ignoring assignment and settlement structure
- tuning directly from recent outcomes without overfitting guardrails

## Success Criteria

The design is working when:

- the allocated slice beats lower retained slices consistently
- the strategy policy layer chooses the right family more often than a spread-only baseline
- `promotion_score` is more monotonic than `discovery_score`
- the portfolio allocator reduces concentrated losers without erasing edge
- execution skips weak allocated candidates when live quality degrades
- realized slippage stays bounded relative to modeled opportunity quality
- side flips become less noisy
- horizon choice adapts to regime and strategy family instead of clinging to fixed DTE buckets
- operator views show one canonical opportunity set with clear ranking, allocation, horizon, and execution reasons

## Sources

Internal:

- [Spread Selection Review And Refactor Plan](./2026-04-11_spread_selection_refactor_plan.md)
- [scanners/service.py](../../packages/core/services/scanners/service.py)
- [scanners/runtime.py](../../packages/core/services/scanners/runtime.py)
- [collections/runtime.py](../../packages/core/services/collections/runtime.py)
- [collections/cycle.py](../../packages/core/services/collections/cycle.py)
- [live_selection.py](../../packages/core/services/live_selection.py)
- [opportunity_scoring.py](../../packages/core/services/opportunity_scoring.py)
- [opportunity_generation.py](../../packages/core/services/opportunity_generation.py)
- [opportunities.py](../../packages/core/services/opportunities.py)
- [live_runtime.py](../../packages/core/services/live_runtime.py)
- [pipelines.py](../../packages/core/services/pipelines.py)
- [execution/__init__.py](../../packages/core/services/execution/__init__.py)
- [execution/attempts.py](../../packages/core/services/execution/attempts.py)
- [opportunity_replay/__init__.py](../../packages/core/services/opportunity_replay/__init__.py)
- [post_market_analysis.py](../../packages/core/services/post_market_analysis.py)
- [signal_state.py](../../packages/core/services/signal_state.py)
- [alerts/rules.py](../../packages/core/alerts/rules.py)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

External:

- [Alpaca: About Market Data API](https://docs.alpaca.markets/docs/about-market-data-api)
- [Alpaca: Historical Option Data](https://docs.alpaca.markets/docs/historical-option-data)
- [Alpaca: Real-time Option Data](https://docs.alpaca.markets/docs/real-time-option-data)
- [Alpaca: Options Trading](https://docs.alpaca.markets/docs/options-trading)
- [OIC: Volatility & the Greeks](https://www.optionseducation.org/advancedconcepts/volatility-the-greeks)
- [OIC: Technical Information FAQ](https://www.optionseducation.org/referencelibrary/faq/technical-information)
- [OIC: Options Assignment FAQ](https://www.optionseducation.org/referencelibrary/faq/options-assignment)
- [OCC: OFRA](https://www.theocc.com/risk-management/ofra/)
- [Cboe: Complex Orders](https://www.cboe.com/us/options/trading/complex_orders/)
- [Cboe: SPX Product Hub](https://ww2.cboe.com/tradable_products/sp_500/)
- [Cboe: Evaluating the Market Impact of SPX 0-DTE Options](https://www.cboe.com/insights/posts/volatility-insights-evaluating-the-market-impact-of-spx-0-dte-options/)
- [Cboe: Henry Schwartz's Zero-Day SPX Iron Condor Strategy, A Deep Dive](https://www.cboe.com/insights/posts/henry-schwartzs-zero-day-spx-iron-condor-strategy-a-deep-dive)
- [NBER: Option Volume and Stock Prices](https://www.nber.org/papers/w10925)
- [NBER: Data-Snooping Biases in Tests of Financial Asset Pricing Models](https://www.nber.org/papers/w3001)
- [NBER: The Distribution of Realized Exchange Rate Volatility](https://www.nber.org/papers/w8160)
