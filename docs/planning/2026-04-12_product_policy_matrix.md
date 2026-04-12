# Product Policy Matrix

Status: proposed

As of: Sunday, April 12, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Execution Templates](./2026-04-12_execution_templates.md)

## Goal

Define which products are allowed for which strategy families and styles.

This document turns vague product assumptions into explicit policy:

- which product classes are approved
- which are restricted
- which strategy families are blocked by default
- when assignment and settlement structure override an otherwise valid setup

## Base Policy

The base system should prefer products with cleaner settlement and lower assignment complexity.

Base product hierarchy:

1. cash-settled European-style index options
2. top-tier liquid ETF options
3. other liquid broad ETF options
4. single-name equity options

The design should assume:

- `iron_condor` is blocked on single-name equities in the base system
- reactive short-premium is restricted to cash-settled indexes and top-tier ETFs
- assignment-sensitive short-premium should be treated cautiously on American-style physical-settlement products

## Product Classes

| Product class | Settlement | Exercise | Relative policy quality |
|---|---|---|---|
| `cash_settled_index` | cash | European | highest |
| `top_tier_etf` | physical | American | high |
| `broad_etf` | physical | American | medium |
| `single_name_equity` | physical | American | lowest |

## Family Permissions

| Strategy family | Cash-settled index | Top-tier ETF | Broad ETF | Single-name equity |
|---|---|---|---|---|
| `long_call` | allow | allow | allow selectively | allow selectively |
| `long_put` | allow | allow | allow selectively | allow selectively |
| `call_debit_spread` | allow | allow | allow selectively | allow selectively |
| `put_debit_spread` | allow | allow | allow selectively | allow selectively |
| `call_credit_spread` | allow | allow | allow selectively | allow selectively with event and ex-div restrictions |
| `put_credit_spread` | allow | allow | allow selectively | allow selectively with event and ex-div restrictions |
| `iron_condor` | allow | allow selectively | discourage | block |

`allow selectively` means builder and style rules must still pass.

## Style Overlay

| Style | Cash-settled index | Top-tier ETF | Broad ETF | Single-name equity |
|---|---|---|---|---|
| `reactive` | full family set except blocked cases | directional families and selective short-premium | directional families only by default | directional families only |
| `tactical` | full family set except blocked cases | most families | most directional families and selective credit | directional families and selective credit |
| `carry` | full family set except blocked cases | most families | selective families only | directional families and limited credit only |

## Strategy-Specific Rules

### Long Premium

- broadly allowed on approved liquid products
- still subject to liquidity, spread, and event rules

### Debit Spreads

- broadly allowed on approved liquid products
- preferred over naked long premium when time is needed and decay is expensive

### Credit Spreads

- broadly allowed on cash-settled indexes and top-tier ETFs
- only selective on single names
- block on single names through earnings and around problematic ex-dividend timing

### Iron Condor

Base policy:

- allow on cash-settled indexes
- allow selectively on top-tier ETFs
- discourage on broader ETF products
- block on single-name equities

Later opt-in path, if ever added:

- single-name condors would require separate approval criteria and should not inherit the base-system policy

## Assignment And Settlement Rules

The product policy engine should apply these overrides:

- prefer European-style exercise over American-style exercise when strategy intent allows
- prefer cash settlement over physical settlement when strategy intent allows
- block assignment-sensitive short-premium when ex-dividend timing or expiry timing makes early assignment operationally ugly
- block families that create two-sided short assignment complexity on products that are otherwise hard to manage

## Event Overlay

| Event state | Cash-settled index | ETF | Single-name equity |
|---|---|---|---|
| `clean` | standard policy | standard policy | standard policy |
| `macro` | keep short-premium selective in `reactive` | more cautious | very cautious |
| `earnings` | mostly unaffected | caution if ETF-specific event concentration matters | block short-premium through event |
| `ex_div` | minimal issue for cash-settled index | caution | strong caution or block |
| `expiry_pressure` | tighten same-day rules | tighten | tighten heavily |

## Liquidity Overlay

Product approval is necessary, not sufficient.

Every product class still requires:

- quote persistence
- acceptable spread quality
- acceptable size
- acceptable trade recency

Poor liquidity should degrade permissions from:

- allow -> allow selectively
- allow selectively -> discourage
- discourage -> block

## Approved Product Universe Process

The system should not rely on product class alone. It should maintain an approved-symbol set per class.

Suggested approach:

- define approved cash-settled index symbols
- define approved top-tier ETF symbols
- define approved broad ETF symbols
- define approved single-name symbols if later needed

This keeps policy explicit and auditable.

## Open Policy Decisions

- which ETF symbols qualify as top-tier in the initial universe
- whether broad ETF products should ever host `iron_condor`
- whether any single-name equities should ever get a later condor opt-in path

## Success Criteria

The product policy layer is working when:

- structurally messy products stop leaking into otherwise valid strategy ideas
- index products are preferred naturally for reactive short-premium and condors
- single-name short-premium exposure becomes much more deliberate and explainable
