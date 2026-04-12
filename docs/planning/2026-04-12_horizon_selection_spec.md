# Horizon Selection Specification

Status: proposed

As of: Sunday, April 12, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Regime Detection Specification](./2026-04-11_regime_detection_spec.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)

## Goal

Define the operational contract for `HorizonIntent`.

This spec makes DTE selection explicit by defining:

- horizon bands
- the inputs used to choose them
- style-specific constraints
- strategy-family constraints
- product and event timing constraints
- how the engine should choose between admissible horizons

The main rule is simple:

`style_profile` controls execution and risk posture, while `HorizonIntent` controls actual expiration choice.

## Design Rules

1. Horizon selection happens after `StrategyIntent`, not before it.
2. Horizon selection should use regime, strategy family, event state, product policy, and liquidity together.
3. Horizon selection should be able to return no admissible horizon.
4. Same-day horizons should be exceptional, not the default outcome for all short-dated ideas.
5. The engine should prefer the shortest horizon only when urgency, liquidity, and execution quality justify it.
6. Legacy labels like `0dte`, `weekly`, and `core` may survive for reporting, but should not control DTE directly.

## Flow

Mermaid source:

- [2026-04-12_horizon_selection_spec_flow.mmd](../diagrams/planning/2026-04-12_horizon_selection_spec_flow.mmd)

## `HorizonIntent` Contract

One `HorizonIntent` should exist per admissible `StrategyIntent + horizon_band`.

Required fields:

| Field | Meaning |
|---|---|
| `horizon_intent_id` | stable horizon-selection id |
| `strategy_intent_id` | parent strategy intent |
| `symbol` | underlying symbol |
| `style_profile` | `reactive`, `tactical`, `carry` |
| `strategy_family` | strategy family |
| `horizon_band` | `same_day`, `next_daily`, `near_term`, `post_event`, `swing`, `carry` |
| `target_dte_min` | lower DTE bound |
| `target_dte_max` | upper DTE bound |
| `preferred_expiration_type` | `daily`, `weekly`, `monthly`, `post_event`, or similar |
| `event_timing_rule` | `before_event`, `after_event`, `avoid_event`, `none` |
| `urgency` | `high`, `normal`, `low` |
| `confidence` | confidence in the horizon choice |
| `blockers` | explicit reasons a horizon was disallowed or downgraded |
| `evidence` | structured explanation payload |

## Horizon Bands

The system should normalize concrete DTE into a small set of horizon bands.

Representative base mapping:

| Band | Intended use | Typical DTE |
|---|---|---|
| `same_day` | immediate reaction only | `0` |
| `next_daily` | very short carry into next listed daily expiry | `1-2` |
| `near_term` | near-term tactical expression | `3-12` |
| `post_event` | event has passed but window is still fresh | `2-10` |
| `swing` | multi-day directional expression | `10-25` |
| `carry` | slower premium collection or slower defined-risk expression | `20-60` |

These are selection bands, not hard builder limits. Final builder policy may narrow inside them.

## Inputs

The horizon engine should consume:

- `RegimeSnapshot`
- `StrategyIntent`
- `style_profile`
- product-policy classification
- daily-expiration availability
- event timing
- volatility state
- liquidity state
- data-quality state

## Selection Stages

## Stage 0: Product And Session Gates

Before scoring horizons, remove structurally invalid choices.

Hard blocks:

- `same_day` on products that do not list daily expirations
- `same_day` or `next_daily` when data quality is not strong enough for `reactive`
- `iron_condor` on single-name equities
- reactive short-premium on products outside approved cash-settled indexes and top-tier ETFs
- horizons that place the trade into an explicitly blocked event window

## Stage 1: Strategy-Family Horizon Admissibility

Representative admissibility rules:

### `long_call` / `long_put`

- admit shorter horizons when directional conviction is high and vol is cheap or fair
- admit longer horizons when the thesis needs time or vol is already rich

### `call_debit_spread` / `put_debit_spread`

- admit short and medium horizons
- prefer more time than single-leg premium when the setup is directional but not explosive

### `put_credit_spread` / `call_credit_spread`

- avoid the shortest horizons unless style is `reactive` and product policy explicitly allows it
- prefer enough time to reduce gamma pressure and assignment risk

### `iron_condor`

- block `same_day` in the base system
- admit only `near_term` and `carry`
- require rich/stable volatility and clean event state

## Stage 2: Event Timing Rules

Event state should alter admissible horizons directly.

Representative rules:

- `earnings`
  - block pre-event short-premium on single names
  - prefer `post_event` horizons after the event passes and regime is recomputed
- `macro`
  - block `same_day` neutral short-premium before the event
  - prefer waiting for reclassification after the release
- `ex_div`
  - block assignment-sensitive short-premium when ex-div timing is inside the horizon
- `expiry_pressure`
  - tighten reactive-horizon rules and widen fail-closed behavior

## Stage 3: Style Constraints

### `reactive`

- prefer `same_day` and `next_daily` only when:
  - liquidity is healthy
  - data quality is healthy
  - event state is not hostile
  - product policy allows the family
- degrade quickly to no admissible horizon when those fail

### `tactical`

- usually prefer `near_term` or `post_event`
- allow `next_daily` selectively for strong momentum or orderly continuation
- avoid `same_day` by default

### `carry`

- prefer `swing` or `carry`
- block `same_day` and `next_daily`
- use `near_term` only when the family and product policy explicitly allow it

## Stage 4: Horizon Scoring

Each admissible horizon should be scored on:

- regime urgency
- volatility fit
- event timing fit
- liquidity fit
- data-quality fit
- family fit
- style fit

Representative formula:

```text
horizon_fit_score =
  100 *
  (
    0.24 * regime_urgency_fit +
    0.16 * volatility_fit +
    0.18 * event_timing_fit +
    0.14 * liquidity_fit +
    0.10 * data_quality_fit +
    0.10 * family_fit +
    0.08 * style_fit
  )
  - short_horizon_penalty
  - event_conflict_penalty
  - product_mismatch_penalty
```

## Base Horizon Preferences

Representative defaults:

| Style | Long premium | Debit spreads | Credit spreads | Iron condor |
|---|---|---|---|---|
| `reactive` | `same_day` to `next_daily` | `same_day` to `next_daily` | `next_daily` to `near_term`, with `same_day` only when explicitly allowed | `near_term` only |
| `tactical` | `near_term` to `post_event` | `near_term` to `post_event` | `near_term` | `near_term` |
| `carry` | `swing` to `carry` | `swing` to `carry` | `carry` | `carry` |

## Tie-Break Rules

When multiple horizons are similarly admissible:

1. prefer the simpler execution path
2. prefer the horizon with less event conflict
3. prefer the horizon with better liquidity and quote persistence
4. prefer the longer horizon when the edge is similar but short-horizon gamma risk is materially higher
5. prefer the shorter horizon only when urgency and liquidity are both clearly stronger

## Failure Rules

The horizon engine should emit no horizon when:

- the product does not support a safe expiration class for the strategy family
- the event state makes all admissible horizons unsafe
- data quality is too weak for the style profile
- the strategy family only works on horizons blocked by product policy

## Legacy Reporting Mapping

If older reporting still wants legacy labels:

- `same_day` and some `next_daily` opportunities can be reported as `0dte`
- many `near_term` opportunities can be reported as `weekly`
- most `swing` and `carry` opportunities can be reported as `core`

That mapping is for reporting only. It should not drive selection logic.

## Open Policy Decisions

- whether `same_day` `iron_condor` should ever be allowed later for specific cash-settled indexes
- whether `carry` should ever allow `near_term` short-premium when event state is exceptionally clean
- whether `tactical` should allow `next_daily` credit spreads beyond top-tier ETF and cash-settled index products

## Success Criteria

The horizon layer is working when:

- DTE choice changes meaningfully with regime and family
- same-day horizons become rare and justified, not default
- event-sensitive families move naturally into `post_event`, `near_term`, or `carry` instead of forcing bad short horizons
- operator views can explain why a horizon was chosen or rejected
