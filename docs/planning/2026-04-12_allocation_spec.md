# Portfolio Allocation Specification

Status: proposed

As of: Sunday, April 12, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)
- [Execution Templates](./2026-04-12_execution_templates.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)

## Goal

Define how the system turns a ranked opportunity book into actual risk allocation.

The allocator exists because:

- ranking is not portfolio construction
- multiple valid opportunities can compete for the same limited risk budget
- cross-family comparison needs one controlled decision layer
- the system needs to prefer "do nothing" over forced exposure when the portfolio is already full or correlated

This spec defines:

- allocator inputs and outputs
- the objective function
- style, product, and family constraints
- slot and budget rules
- rejection and tie-break rules
- what gets persisted for audit and post-market analysis

## Flow

Mermaid source:

- [2026-04-12_allocation_spec_allocator_flow.mmd](../diagrams/planning/2026-04-12_allocation_spec_allocator_flow.mmd)

## Core Rule

The allocator should optimize for portfolio-adjusted opportunity quality, not raw candidate rank.

It should ask:

1. does this opportunity fit the current portfolio
2. does it improve expected portfolio quality after concentration and risk costs
3. is the budget worth spending now instead of holding capacity for a better later opportunity

If any answer is no, the correct output is `not_allocated`.

## Inputs

The allocator consumes:

- `promotion_score`
- `style_local_rank`
- `strategy_family`
- `style_profile`
- `HorizonIntent`
- product class and product policy state
- account equity, cash, buying power, and margin usage
- open positions and pending execution intents
- symbol concentration and sector concentration
- directional overlap
- event overlap
- estimated max loss
- estimated capital usage or margin usage
- quote quality and execution readiness summary
- session-level risk budget configuration

## Outputs

The allocator writes one allocation decision per eligible opportunity.

Required outputs:

| Field | Meaning |
|---|---|
| `allocation_state` | `allocated` or `not_allocated` |
| `allocation_score` | portfolio-adjusted desirability |
| `allocation_reason` | short human-readable reason |
| `slot_class` | which style or family slot it consumed or failed |
| `budget_impact` | capital and downside budget impact |
| `rejection_codes` | structured list of failed rules |
| `allocator_evidence` | structured evidence payload |

## Allocation Objective

The initial allocator should stay heuristic and explainable.

It should maximize:

```text
allocation_score =
  expected_edge_value
  + diversification_bonus
  + execution_readiness_bonus
  - concentration_cost
  - correlation_cost
  - event_overlap_cost
  - capital_usage_cost
  - downside_cost
  - assignment_risk_cost
```

This is intentionally not a black-box optimizer.

The first production version should use a deterministic scoring model with hard constraints first and soft ranking second.

## Hard Constraints

An opportunity cannot be allocated when any of these are true:

- product policy blocks the family or horizon
- account buying power or risk budget is insufficient
- opportunity is already stale beyond style limits
- open pending intents already consume the relevant slot
- same-symbol stacking breaches policy
- event overlap breaches policy
- correlation or directional overlap breaches a hard cap
- execution readiness is below the minimum floor
- modeled max loss breaches the style-specific budget

Hard constraints should fail closed.

## Budget Model

The allocator should manage four kinds of budget:

1. account-level downside budget
2. account-level capital or margin budget
3. style-level slot budget
4. symbol, sector, and direction concentration budget

Initial budget posture:

- `reactive`
  - smallest per-trade downside
  - tightest slot count
  - lowest tolerance for stacking
- `tactical`
  - medium per-trade downside
  - medium slot count
  - moderate tolerance for stacking
- `carry`
  - largest per-trade downside
  - fewer total positions than tactical
  - more tolerance for duration, less tolerance for crowded event exposure

## Slot Model

Initial recommended slot model:

- `reactive`
  - max `1` live idea per symbol-side
  - max `2` total live positions across the profile
- `tactical`
  - max `1` live idea per symbol-side
  - max `3` total live positions across the profile
- `carry`
  - max `1` live idea per symbol
  - max `3` total live positions across the profile

Initial family overlay:

- `iron_condor`
  - always consumes a full slot
  - cannot coexist with another live short-premium structure on the same symbol
- credit spreads
  - cannot stack same-side on the same symbol by default
- debit spreads and long premium
  - cannot stack if thesis direction is materially identical unless the carry policy explicitly allows it

## Concentration Rules

The allocator should measure concentration at minimum by:

- symbol
- product family
- sector or macro bucket
- direction
- volatility exposure profile

Initial hard rules:

- no more than one live short-premium thesis on the same symbol
- no same-symbol, same-direction stacking across styles by default
- no more than two live positions with effectively the same macro direction bucket
- no new reactive short-premium when a correlated reactive short-premium trade is already live

## Cross-Family Comparison

The allocator should not compare raw family scores directly.

It should compare normalized opportunity attributes:

- expected edge estimate
- downside budget usage
- capital or margin usage
- execution complexity
- assignment risk
- data quality
- portfolio overlap cost

This is the point where a debit spread can beat a credit spread even if its `promotion_score` is slightly lower, because the portfolio fit is better.

## Default Priority Order

When multiple opportunities pass hard constraints and fit within budget, prefer:

1. higher `allocation_score`
2. lower concentration cost
3. lower event overlap cost
4. lower execution complexity
5. lower assignment risk
6. lower capital usage for similar expected edge

## Reservation Logic

The allocator should preserve optionality.

It should avoid consuming the last available budget unit when:

- the current opportunity barely clears the allocation floor
- style or event context suggests more opportunities are likely later in the session
- the remaining available budget would be too small to act on a better candidate

Initial policy:

- hold back a reserve budget for `reactive`
- allow `carry` to consume budget more slowly and less frequently

## Rejection Codes

At minimum, the allocator should emit these rejection codes:

- `budget_exhausted`
- `slot_full`
- `same_symbol_conflict`
- `same_direction_conflict`
- `correlation_conflict`
- `event_conflict`
- `assignment_risk_conflict`
- `execution_readiness_too_low`
- `capital_usage_too_high`
- `downside_too_high`
- `product_policy_blocked`
- `stale_candidate`

## Persistence

Allocation decisions should be persisted separately from ranking.

The system should store:

- the final allocation decision
- the score components
- the binding constraint or tie-break outcome
- the portfolio state summary used in the decision

This is necessary so post-market analysis can answer whether the wrong idea was selected or whether the right idea was ranked but correctly rejected by the portfolio layer.

## Initial Defaults

Use these defaults unless later research invalidates them:

- one active style snapshot per symbol-side
- one live allocated thesis per symbol-side by default
- no same-day `iron_condor`
- no reactive short-premium outside cash-settled indexes and top-tier ETFs
- top-tier ETF initial universe:
  - `SPY`
  - `QQQ`
  - `IWM`
  - `DIA`
  - `GLD`
  - `TLT`
- `carry` should prefer debit spreads over long single-leg premium by default

## Open Policy Decisions

These are narrower now and should not block an offline prototype:

- whether tactical style should ever allow two non-identical directional theses on the same macro bucket
- whether carry should reserve fewer slots but more per-trade risk than tactical
- whether broad ETF products should ever host short-premium beyond credit verticals

## Success Criteria

The allocator is working when:

- top-ranked opportunities are not forced into execution when the portfolio fit is poor
- realized selection quality improves versus rank-only selection
- same-symbol and same-direction crowding drops materially
- the operator can explain every allocation and rejection from stored evidence
- post-market analysis can separate ranking errors from allocation errors
