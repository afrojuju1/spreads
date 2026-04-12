# Regime Detection Specification

Status: proposed

As of: Saturday, April 11, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Goal

Define the operational contract for `RegimeSnapshot`:

- what inputs it uses
- how those inputs are normalized
- how each field is classified
- how style-specific thresholds change behavior
- when the system should emit a low-confidence or `unstable` regime instead of forcing a stronger label

This spec is intended to make the regime layer buildable, testable, and separable from strategy selection.

## Scope And Assumptions

Initial style profile scope:

- `reactive`
- `tactical`
- `carry`

Initial product assumptions:

- Tier 1 focus is cash-settled index options and highly liquid ETF options
- single-name equity options are allowed for directional structures only when event state is clean
- `iron_condor` should default to index or highly liquid ETF products only in the base system
- reactive short-premium should be restricted to cash-settled indexes and top-tier ETFs

These are design assumptions, not permanent policy decisions.

## Design Rules

1. Regime should describe the market, not select a strategy directly.
2. Regime should combine stock-led and option-led evidence.
3. Regime should be multi-axis, not one enum.
4. Confidence should reflect agreement, data quality, and persistence.
5. Low confidence is a valid output.
6. Each style profile should use different lookback windows and different sensitivity.
7. Regime should not choose DTE directly. It should produce evidence for horizon selection.

## Pipeline

Mermaid source:

- [2026-04-11_regime_detection_spec_pipeline.mmd](../diagrams/planning/2026-04-11_regime_detection_spec_pipeline.mmd)

## `RegimeSnapshot` Contract

Each snapshot should be produced per `symbol + style_profile + observed_at`.

Required fields:

| Field | Type | Meaning |
|---|---|---|
| `regime_snapshot_id` | `uuid` | snapshot identifier |
| `symbol` | `text` | underlying symbol |
| `style_profile` | `text` | `reactive`, `tactical`, `carry` |
| `observed_at` | `timestamptz` | snapshot timestamp |
| `direction_bias` | `enum` | `bullish`, `bearish`, `neutral` |
| `direction_score` | `numeric` | signed directional conviction on `[-1.0, 1.0]` |
| `trend_strength` | `numeric` | normalized trend strength on `[0.0, 1.0]` |
| `intraday_structure` | `enum` | `trend`, `range`, `breakout`, `reversal`, `unstable` |
| `vol_level` | `enum` | `low`, `normal`, `high` |
| `vol_trend` | `enum` | `expanding`, `stable`, `contracting` |
| `iv_vs_rv` | `enum` | `cheap`, `fair`, `rich` |
| `event_state` | `enum` | `clean`, `macro`, `earnings`, `ex_div`, `expiry_pressure`, `holiday`, `halt_risk` |
| `liquidity_state` | `enum` | `healthy`, `thin`, `degraded` |
| `data_quality_state` | `enum` | `healthy`, `thin`, `stale`, `degraded` |
| `confidence` | `numeric` | overall confidence on `[0.0, 1.0]` |
| `supporting_features` | `jsonb` | normalized evidence and raw bucket context |

Optional fields that are still worth storing:

- `bias_persistence`
- `structure_persistence`
- `rv_percentile`
- `iv_percentile`
- `iv_rv_ratio`
- `option_flow_bias`
- `data_quality_multiplier`

## Input Families

The regime engine should ingest five input families.

### 1. Stock-Led Inputs

- VWAP distance and persistence
- opening-range location and breakout state
- short-horizon and medium-horizon return slopes
- realized volatility by style-profile horizon
- session high/low proximity
- gap context
- underlying volume percentile

### 2. Option-Led Inputs

- call versus put premium imbalance
- option volume anomaly
- option volume versus open interest
- skew shift
- term-structure shift
- spread-quality concentration by side
- quote persistence and quote deterioration

### 3. Volatility Inputs

- short-horizon realized volatility
- medium-horizon realized volatility
- implied-volatility percentile
- IV/RV ratio
- skew and term-structure summaries

### 4. Event Inputs

- earnings calendar
- macro release calendar
- ex-dividend timing
- expiration pressure
- holiday and shortened-session flags

### 5. Data-Quality Inputs

- stock quote freshness
- option quote freshness
- option quote coverage versus subscription budget
- contract opening readiness
- top-of-book completeness

## Profile Windows

The same regime fields should use different windows by style profile.

| Field family | `reactive` | `tactical` | `carry` |
|---|---|---|---|
| short trend | `1m-5m` | `5m-15m` | `15m-60m` |
| medium trend | `5m-15m` | `15m-60m` | `1D-5D` |
| realized vol | intraday only | intraday + `1D` | `1D-5D-20D` |
| option flow anomaly | last `5m-15m` | last `30m-1D` | last `1D-5D` |
| persistence gate | strict | medium | looser |
| stale-data timeout | very strict | strict | moderate |

Representative stale-data thresholds:

- `reactive`: option quotes older than `15s` should be at least `thin`, and older than `30s` should be `stale`
- `tactical`: option quotes older than `60s` should be at least `thin`, and older than `180s` should be `stale`
- `carry`: option quotes older than `300s` should be at least `thin`, and older than `900s` should be `stale`

These are starting points, not final production constants.

## Computation Stages

## Stage 0: Readiness Gates

Before classifying regime, the engine should evaluate whether data is good enough.

Hard blocks:

- no usable underlying session data
- no usable option data for a style profile that requires option confirmation
- required contracts not yet open for trading
- option quote coverage materially below the required coverage floor
- control-plane or session-state block

If hard-blocked:

- emit a snapshot
- force `intraday_structure = unstable`
- cap `confidence <= 0.30`
- set `data_quality_state = degraded`

## Stage 1: Directional Evidence

Build two normalized directional buckets:

- `bullish_evidence`
- `bearish_evidence`

Recommended initial components:

- stock trend slope
- VWAP persistence
- opening-range breakout direction
- session-extreme acceptance or rejection
- option flow imbalance
- skew shift

Representative weights:

- stock trend slope: `0.24`
- VWAP persistence: `0.18`
- opening-range breakout: `0.16`
- session-extreme behavior: `0.12`
- option flow imbalance: `0.18`
- skew shift: `0.12`

Direction score:

```text
direction_score = bullish_evidence - bearish_evidence
```

Initial classification:

- `bullish` if `direction_score >= +0.20`
- `bearish` if `direction_score <= -0.20`
- `neutral` otherwise

If data quality is `thin`, widen the neutral band by `0.05`.

## Stage 2: Intraday Structure Classification

`intraday_structure` should be derived independently from `direction_bias`.

Representative rules:

- `trend`
  - `trend_strength >= 0.65`
  - VWAP persistence strong
  - no major reversal evidence
- `range`
  - direction near neutral
  - price oscillating around VWAP
  - realized vol below recent style median
- `breakout`
  - recent opening-range or session-extreme breach
  - trend strength rising
  - volatility expanding
- `reversal`
  - strong move failed
  - price reclaims VWAP or key pivot in opposite direction
  - option flow and skew confirm reversal
- `unstable`
  - contradictory evidence
  - event shock in progress
  - degraded market-data state

## Stage 3: Volatility Classification

Volatility should be classified along three dimensions.

### `vol_level`

Use a blend of implied-vol percentile and realized-vol percentile:

- `low` if both are below the `35th` percentile
- `high` if either is above the `70th` percentile and the other is above the `55th`
- `normal` otherwise

### `vol_trend`

Use short-horizon versus medium-horizon realized vol, plus short-term IV change:

- `expanding` if short-horizon realized vol is at least `1.15x` medium-horizon baseline or IV change is strongly positive
- `contracting` if short-horizon realized vol is at most `0.85x` baseline and IV change is negative
- `stable` otherwise

### `iv_vs_rv`

Use the implied-to-realized ratio:

- `cheap` if `iv_rv_ratio <= 0.85`
- `fair` if `0.85 < iv_rv_ratio < 1.20`
- `rich` if `iv_rv_ratio >= 1.20`

## Stage 4: Event State Classification

Event state should be mutually exclusive, with the highest-risk state winning.

Priority order:

1. `halt_risk`
2. `earnings`
3. `macro`
4. `ex_div`
5. `expiry_pressure`
6. `holiday`
7. `clean`

Representative rules:

- `earnings` if earnings fall within the relevant style horizon
- `macro` if a major scheduled release or Fed event falls within the relevant style horizon
- `ex_div` if ex-dividend timing materially changes assignment risk
- `expiry_pressure` if same-day or nearby expiration materially affects liquidity and gamma

## Stage 5: Liquidity State Classification

Liquidity should consider both the underlying and the options.

Use:

- underlying ADV percentile
- option quote width
- option quote size
- quote persistence
- trade recency

Classification:

- `healthy`
  - spreads tight enough for the style profile
  - quote sizes above floor
  - recent prints and stable quotes
- `thin`
  - some weakness in spread or size
  - intermittent quote or trade availability
- `degraded`
  - materially wide quotes
  - unreliable size
  - stale or sparse prints

## Stage 6: Confidence And Hysteresis

Confidence should combine:

- directional agreement
- structure agreement
- volatility agreement
- option confirmation
- data quality
- persistence

Representative initial weights:

- directional agreement: `0.28`
- structure agreement: `0.18`
- volatility agreement: `0.14`
- option confirmation: `0.16`
- data quality multiplier: `0.14`
- persistence multiplier: `0.10`

Representative interpretation:

- `>= 0.75`: strong regime
- `0.55 - 0.74`: usable regime
- `0.35 - 0.54`: weak regime
- `< 0.35`: low-confidence regime, should usually bias strategy policy toward `pass`

Hysteresis rules:

- do not flip `direction_bias` unless the new side beats the old side by at least `0.10`
- do not flip `intraday_structure` on one weak observation
- degrade to `unstable` faster than promoting to `trend` or `range`

## Output Semantics

The regime layer should not output:

- a strategy family
- a board rank
- an execution instruction

It should output:

- market-state description
- confidence
- supporting evidence
- style-specific context
- horizon-selection inputs

## Operational Rules By Profile

## Style Profiles Versus Horizon

`reactive`, `tactical`, and `carry` should not be treated as fixed DTE buckets.

They should mean:

- `reactive`: fast execution posture, strict data rules, smallest size, most willing to use very short expirations
- `tactical`: medium-speed posture, balanced between responsiveness and persistence
- `carry`: slower posture, more persistence, less willingness to use very short expirations

Actual expiration choice should be handled later by `HorizonIntent`.

## Operational Rules By Style

### `reactive`

- prefer faster degradation to `unstable`
- require option confirmation for strong directional regimes
- react quickly to macro-event transitions
- keep stale-data thresholds extremely tight

### `tactical`

- allow more persistence before flipping regime
- use both intraday and daily evidence
- tolerate minor data gaps if the broader regime remains coherent

### `carry`

- bias toward daily and multi-day structure
- use intraday state mainly as refinement, not as the primary driver
- degrade more slowly on short-lived intraday noise

## Open Policy Decisions

These need explicit product-policy confirmation later:

- whether the same symbol may carry multiple simultaneous style snapshots into ranking and allocation
- whether reactive same-day neutral short-premium should ever be widened beyond indexes and top-tier ETFs

## Success Criteria

The regime layer is working when:

- regime flips are meaningfully less noisy than raw underlying moves
- low-confidence environments resolve to `pass` more often than forced strategy picks
- option-led evidence improves directional quality on difficult days
- strategy policy can consume the snapshot without needing raw bar or quote logic
