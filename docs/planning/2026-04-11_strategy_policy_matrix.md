# Strategy Policy Matrix

Status: proposed

As of: Saturday, April 11, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Regime Detection Specification](./2026-04-11_regime_detection_spec.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)
- [Execution Templates](./2026-04-12_execution_templates.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)

## Goal

Turn `RegimeSnapshot` into a concrete strategy-family selection policy.

This document defines:

- which strategy families are preferred under each regime
- which families are merely allowed
- which should be discouraged or blocked
- when the correct output is `pass`
- how `style_profile` constrains behavior
- how DTE should be chosen dynamically through `HorizonIntent`

## Scope And Assumptions

Strategy families in scope:

- `long_call`
- `long_put`
- `call_credit_spread`
- `put_credit_spread`
- `call_debit_spread`
- `put_debit_spread`
- `iron_condor`
- `pass`

Initial product assumptions:

- `iron_condor` is blocked on single-name equities in the base system
- `iron_condor` defaults to cash-settled indexes and top-tier ETFs only
- directional long-premium and debit structures are acceptable on liquid single names when event state is clean
- reactive short-premium is restricted to cash-settled indexes and top-tier ETFs
- short-premium structures on single names should be blocked through earnings and stricter around ex-dividend dates

## Policy Rules

1. Strategy policy consumes `RegimeSnapshot`; it does not compute market state.
2. `pass` is a valid and desirable output.
3. Preferred families should be ranked, not hard-selected, unless all others are blocked.
4. Simpler execution should win ties.
5. Defined-risk structures should beat undefined or assignment-heavy structures by default.
6. Portfolio allocator may still reject a preferred strategy family.
7. `style_profile` should constrain risk posture and execution strictness, not hard-code DTE.

## Strategy Family Summary

| Family | Best use | Weak use | Default complexity |
|---|---|---|---|
| `long_call` | bullish, cheap vol, expansion | rich vol, heavy decay environments | low |
| `long_put` | bearish, cheap vol, expansion | rich vol, heavy decay environments | low |
| `call_debit_spread` | bullish with defined risk and moderate width | range or vol-rich grind | medium |
| `put_debit_spread` | bearish with defined risk and moderate width | range or vol-rich grind | medium |
| `put_credit_spread` | bullish grind, vol rich or stable | unstable breakout or event shock | medium |
| `call_credit_spread` | bearish grind, vol rich or stable | unstable breakout or event shock | medium |
| `iron_condor` | neutral range, rich and stable vol, clean event state | breakouts, cheap vol, macro shock | high |
| `pass` | low confidence, event-heavy, degraded liquidity | none | none |

## Strategy Intent Rules

`StrategyIntent` desirability should combine:

- regime fit
- volatility fit
- product-policy fit
- liquidity fit
- execution complexity penalty
- portfolio compatibility hint

Representative starting interpretation:

- `>= 0.75`: preferred
- `0.60 - 0.74`: allowed
- `0.45 - 0.59`: discouraged
- `< 0.45`: blocked

If no family is above `0.60`, the strategy policy layer should usually emit `pass` first.

## Style Profiles

Legacy labels like `0dte`, `weekly`, and `core` are too loose to serve as primary control objects.

Use these instead:

| `style_profile` | Meaning |
|---|---|
| `reactive` | strictest data and execution rules, smallest size, shortest tolerated holding horizon |
| `tactical` | balanced speed and persistence, suitable for short swing structures |
| `carry` | slowest posture, more persistence, least tolerant of very short expirations |

## Global Regime Archetypes

The policy matrix should use a small set of regime archetypes.

| Archetype | Regime summary |
|---|---|
| `bull_trend_expanding` | bullish direction, trend or breakout, cheap/fair vol, expanding vol trend |
| `bull_grind_rich` | bullish direction, orderly trend, rich/stable vol |
| `bear_trend_expanding` | bearish direction, trend or breakout, cheap/fair vol, expanding vol trend |
| `bear_grind_rich` | bearish direction, orderly trend, rich/stable vol |
| `neutral_range_rich` | neutral direction, range structure, rich/stable vol, clean event state |
| `neutral_range_cheap` | neutral direction, range structure, cheap vol or expanding uncertainty |
| `breakout_unstable` | breakout or unstable structure with elevated disagreement or shock |
| `event_heavy` | macro, earnings, ex-div, or expiry-pressure state dominates |
| `degraded_liquidity` | thin or degraded liquidity/data quality dominates |

## Style Matrix: `reactive`

| Archetype | Preferred | Allowed | Discouraged | Blocked | Default sizing |
|---|---|---|---|---|---|
| `bull_trend_expanding` | `long_call`, `call_debit_spread` | `put_credit_spread` | `iron_condor` | `call_credit_spread`, `long_put`, `put_debit_spread` | `small` |
| `bull_grind_rich` | `put_credit_spread` | `call_debit_spread` | `long_call` | `iron_condor` on non-index, bearish families | `small` |
| `bear_trend_expanding` | `long_put`, `put_debit_spread` | `call_credit_spread` | `iron_condor` | `put_credit_spread`, `long_call`, `call_debit_spread` | `small` |
| `bear_grind_rich` | `call_credit_spread` | `put_debit_spread` | `long_put` | `iron_condor` on non-index, bullish families | `small` |
| `neutral_range_rich` | `iron_condor` on index or top ETF only | `put_credit_spread`, `call_credit_spread` very selectively | debit and long-premium families | directional breakouts without confirmation | `xsmall` |
| `neutral_range_cheap` | `pass` | tiny debit spread in breakout-ready direction | credit spreads | `iron_condor` | `xsmall` |
| `breakout_unstable` | `long_call`, `long_put`, corresponding debit spread in confirmed direction | `pass` | credit spreads | `iron_condor` | `xsmall` |
| `event_heavy` | `pass` | post-event debit only after reclassification | all short-premium families | `iron_condor` before event | `none` |
| `degraded_liquidity` | `pass` | none | all families | none | `none` |

## Style Matrix: `tactical`

| Archetype | Preferred | Allowed | Discouraged | Blocked | Default sizing |
|---|---|---|---|---|---|
| `bull_trend_expanding` | `call_debit_spread`, `long_call` | `put_credit_spread` | `iron_condor` | bearish families | `small` |
| `bull_grind_rich` | `put_credit_spread` | `call_debit_spread`, `long_call` | `iron_condor` | bearish families | `small` |
| `bear_trend_expanding` | `put_debit_spread`, `long_put` | `call_credit_spread` | `iron_condor` | bullish families | `small` |
| `bear_grind_rich` | `call_credit_spread` | `put_debit_spread`, `long_put` | `iron_condor` | bullish families | `small` |
| `neutral_range_rich` | `iron_condor` | `put_credit_spread`, `call_credit_spread` | long premium | breakout-biased families without confirmation | `small` |
| `neutral_range_cheap` | `pass` | narrow debit spread only with directional setup | credit spreads | `iron_condor` | `xsmall` |
| `breakout_unstable` | corresponding debit spread, directional long premium | `pass` | credit spreads | `iron_condor` | `xsmall` |
| `event_heavy` | `pass` | post-event debit or long premium only after cleanup | credit spreads | `iron_condor` through earnings | `none` |
| `degraded_liquidity` | `pass` | none | all families | none | `none` |

## Style Matrix: `carry`

| Archetype | Preferred | Allowed | Discouraged | Blocked | Default sizing |
|---|---|---|---|---|---|
| `bull_trend_expanding` | `call_debit_spread`, `long_call` | `put_credit_spread` | `iron_condor` | bearish families | `small` |
| `bull_grind_rich` | `put_credit_spread` | `call_debit_spread` | `long_call` | bearish families | `small` |
| `bear_trend_expanding` | `put_debit_spread`, `long_put` | `call_credit_spread` | `iron_condor` | bullish families | `small` |
| `bear_grind_rich` | `call_credit_spread` | `put_debit_spread` | `long_put` | bullish families | `small` |
| `neutral_range_rich` | `iron_condor`, `put_credit_spread`, `call_credit_spread` | none | long premium | breakout-biased families without confirmation | `small` |
| `neutral_range_cheap` | `pass` | small debit spread if asymmetry exists | credit spreads | `iron_condor` | `xsmall` |
| `breakout_unstable` | directional debit spreads | directional long premium | credit spreads | `iron_condor` | `xsmall` |
| `event_heavy` | `pass` | post-event directional debit only after regime reset | credit spreads | `iron_condor` through earnings | `none` |
| `degraded_liquidity` | `pass` | none | all families | none | `none` |

## Family-Level Overlays

These rules apply on top of the matrix.

### `long_call` / `long_put`

- prefer when volatility is cheap or only mildly fair
- penalize when theta decay is steep relative to expected move
- prefer simpler products and cleaner liquidity

### `call_debit_spread` / `put_debit_spread`

- prefer when directional conviction is good but long-premium decay is too expensive
- prefer when execution quality is good enough to build a multi-leg entry without condor complexity

### `put_credit_spread` / `call_credit_spread`

- prefer when direction is aligned but the move is expected to be orderly
- block during event-heavy states unless explicit post-event rules re-enable them
- penalize strongly on products with unattractive assignment risk

### `iron_condor`

- require `neutral_range_rich`
- require clean or near-clean event state
- require high liquidity quality
- block on single-name equities in the base system

## Product-Policy Overlay

The matrix should be filtered by product policy before builder invocation.

Default overlay:

- cash-settled index options:
  - all listed families potentially allowed
- liquid ETF options:
  - all listed families potentially allowed, but `iron_condor` requires stricter liquidity
- single-name equity options:
  - allow long premium, debit spreads, and selective credit spreads
  - block `iron_condor` by default
  - block short-premium families through earnings

## DTE And Width Guidance

Preferred widths should stay on `StrategyIntent`.

DTE should move to `HorizonIntent`, not remain implicit in the style profile.

Representative horizon bands:

- `same_day`
- `next_daily`
- `near_term`
- `post_event`
- `swing`
- `carry`

## Horizon Intent Rules

`HorizonIntent` should consume:

- `style_profile`
- `strategy_family`
- regime confidence
- volatility state
- event state
- liquidity state
- daily-expiration availability

### Directional Long Premium And Debit Structures

- use the shortest horizon only when conviction is high and the move is expected soon
- lengthen DTE when the thesis needs time or when implied volatility is already rich
- in `reactive`, prefer `same_day` or `next_daily` only when liquidity and data quality are very strong
- in `tactical`, prefer `near_term` or `post_event`
- in `carry`, prefer `swing` or `carry`

### Credit Spreads

- avoid the shortest horizon unless the style is `reactive` and the product is index or top-tier ETF
- when regime is orderly and volatility is rich, prefer enough DTE to reduce gamma pressure
- in `reactive`, use `same_day` or `next_daily` only for cash-settled indexes and top-tier ETFs
- in `tactical`, prefer `near_term`
- in `carry`, prefer `carry`

### Iron Condor

- never on single-name equities in the base system
- never in `reactive` style outside cash-settled indexes and top-tier ETFs
- prefer `near_term` in `tactical`
- prefer `carry` in `carry`
- avoid `same_day` unless a later, explicitly approved index-only policy enables it

Representative starting bands:

| Style | Long premium | Debit spreads | Credit spreads | Iron condor |
|---|---|---|---|---|
| `reactive` | `same_day` to `next_daily` | `same_day` to `next_daily` | `same_day` to `near_term`, index/top ETF only | `near_term` by default, index/top ETF only |
| `tactical` | `near_term` to `post_event` | `near_term` to `post_event` | `near_term` | `near_term` |
| `carry` | `swing` to `carry` | `swing` to `carry` | `carry` | `carry` |

## Tie-Break Rules

When multiple families are similarly desirable:

1. prefer higher liquidity quality
2. prefer lower execution complexity
3. prefer lower assignment risk
4. prefer lower portfolio overlap
5. prefer lower capital usage for similar expected edge

## `pass` Rules

`pass` should be preferred when:

- top family desirability is below `0.60`
- regime confidence is below `0.55`
- event state is too hostile
- liquidity or data quality is degraded
- portfolio allocator is likely to reject all candidates anyway

## Open Policy Decisions

These need explicit confirmation later:

- whether `iron_condor` should ever get a later opt-in path on carefully selected single names
- whether reactive same-day `iron_condor` should ever exist even for indexes, or stay blocked in the base system
- whether long single-leg premium should be sized at all in `carry`, or mostly replaced by debit spreads

## Success Criteria

The strategy policy layer is working when:

- the top-ranked family outperforms a spread-only baseline
- `pass` decisions reduce low-quality forced trades
- `iron_condor` appears only in genuinely neutral, rich, clean environments and only on approved products
- long premium and debit structures dominate more often in cheap or expanding-volatility directional regimes
- horizon choice adapts to regime and family instead of mirroring a fixed DTE label
