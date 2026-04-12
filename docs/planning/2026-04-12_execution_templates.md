# Execution Templates

Status: proposed

As of: Sunday, April 12, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)
- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Portfolio Allocation Specification](./2026-04-12_allocation_spec.md)
- [Evaluation And Rollout Plan](./2026-04-12_evaluation_and_rollout_plan.md)

## Goal

Define execution behavior by strategy family.

This spec describes:

- entry templates
- price policies
- timeout and replace behavior
- fail-closed rules
- default exit templates

The point is to make `ExecutionIntent` predictable and family-specific instead of one generic order path trying to fit everything.

## Flow

Mermaid source:

- [2026-04-12_execution_templates_flow.mmd](../diagrams/planning/2026-04-12_execution_templates_flow.mmd)

## Shared Rules

These apply to all families.

1. No execution without fresh required leg quotes.
2. No execution when product policy blocks the family.
3. No execution when the allocator revokes the slot.
4. Execution should prefer passive entry first, then escalate according to template rules.
5. Each family should have a maximum tolerated slippage band.

## `ExecutionIntent` Fields Used By Templates

Each template should fill:

- `order_structure`
- `entry_policy`
- `price_policy`
- `timeout_policy`
- `replace_policy`
- `exit_policy`
- `validation_state`

## Family Templates

## 1. `long_call` / `long_put`

Order structure:

- `single_leg`

Entry policy:

- start passive near the better side of the spread when liquidity allows
- escalate modestly toward midpoint or beyond only if urgency is high

Price policy:

- tight slippage band
- avoid chasing in thin quotes

Timeout policy:

- short timeout in `reactive`
- medium timeout in `tactical`
- longer timeout in `carry`

Replace policy:

- a small number of upward price adjustments only
- abort if quote quality deteriorates

Exit policy:

- profit target
- adverse move stop
- time-based decay exit when thesis is stale

## 2. `call_debit_spread` / `put_debit_spread`

Order structure:

- `vertical`

Entry policy:

- start passive around midpoint if both legs are healthy
- require complete two-leg quote state

Price policy:

- modest slippage band
- prefer skipping to crossing a wide market

Timeout policy:

- moderate timeout
- shorter in `reactive`

Replace policy:

- limited midpoint-to-natural ladder
- abort if one leg becomes stale

Exit policy:

- profit target based on spread value appreciation
- stop on thesis failure or spread collapse
- time-based exit if event or catalyst window expires

## 3. `put_credit_spread` / `call_credit_spread`

Order structure:

- `vertical`

Entry policy:

- passive credit collection first
- only accept more aggressive pricing when:
  - liquidity is healthy
  - product policy allows the family
  - short-horizon gamma risk is acceptable

Price policy:

- minimum acceptable credit floor
- avoid entries when live credit retention falls below the floor

Timeout policy:

- short in `reactive`
- moderate in `tactical`
- moderate to longer in `carry`

Replace policy:

- small downward credit ladder
- abort after limited attempts
- abort immediately if short leg quote quality degrades sharply

Exit policy:

- credit-capture target
- adverse spread expansion stop
- time-based exit ahead of unacceptable assignment or gamma pressure

## 4. `iron_condor`

Order structure:

- `condor`

Entry policy:

- require all legs healthy
- require strong liquidity and quote persistence
- require product-policy approval

Price policy:

- tighter relative slippage than credit verticals
- prefer skipping to forcing a fill in weak complex pricing

Timeout policy:

- moderate
- abort earlier than simpler structures if quote quality degrades

Replace policy:

- very limited ladder
- abort quickly when one side detaches or quote quality breaks

Exit policy:

- credit-capture target
- side-breach or range-break exit
- forced exit before expiration risk becomes operationally ugly

## Style Overrides

### `reactive`

- shortest timeouts
- strictest quote-freshness rules
- smallest replace ladders
- strongest preference to skip over chase

### `tactical`

- balanced timeouts
- moderate replace ladders
- more tolerance for patient midpoint entry than `reactive`

### `carry`

- longest timeouts, within safety limits
- more tolerance for waiting on better fills
- still fail closed if live quality is weak

## Fail-Closed Rules

Execution should fail closed when:

- required leg quotes are stale
- required leg quotes are incomplete
- live price retention is outside the allowed band
- product policy blocks the family
- assignment or ex-dividend policy blocks the trade
- event state conflicts with the template

Additional family-specific fail-closed rules:

- long premium:
  - block when spread is too wide relative to premium
- debit spreads:
  - block when one leg is stale or detached
- credit spreads:
  - block when credit floor is lost
- iron condor:
  - block when any leg is stale or when side symmetry breaks materially

## Open Policy Decisions

- whether reactive long premium should ever cross aggressively beyond midpoint
- how many replace steps each family should allow initially
- whether `iron_condor` should require earlier time-based exits than credit verticals by policy

## Success Criteria

The execution-template layer is working when:

- `ExecutionIntent` is clearly derivable from family and style
- slippage behavior differs rationally by family
- complex structures skip more often instead of degrading into bad fills
- exits align with the economic shape of each family rather than one generic rule set
