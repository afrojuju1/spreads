# Evaluation And Rollout Plan

Status: proposed

As of: Sunday, April 12, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Spread Selection Review And Refactor Plan](./2026-04-11_spread_selection_refactor_plan.md)
- [Regime Detection Specification](./2026-04-11_regime_detection_spec.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)
- [Execution Templates](./2026-04-12_execution_templates.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)
- [Portfolio Allocation Specification](./2026-04-12_allocation_spec.md)

## Goal

Define how the redesigned decision stack should be evaluated, promoted, and rolled out.

The system should not move from design to live trading through intuition alone.

It needs:

- an offline replay stage
- a shadow evaluation stage
- explicit promotion criteria
- rollback rules
- a controlled live rollout sequence

## Flow

Mermaid source:

- [2026-04-12_evaluation_and_rollout_plan_lifecycle.mmd](../diagrams/planning/2026-04-12_evaluation_and_rollout_plan_lifecycle.mmd)

## Core Rule

No policy or score change should go directly from doc to live execution.

The required path is:

1. offline replay
2. shadow mode
3. limited live allocation
4. wider live rollout

## Evaluation Targets

The evaluation stack should answer four separate questions:

1. does the system rank better opportunities above worse ones
2. does the allocator improve portfolio outcomes versus rank-only selection
3. does the execution planner improve realized fill quality
4. do policy changes generalize beyond the sessions that motivated them

These must be measured separately.

## Dataset Policy

The evaluation dataset should be frozen before tuning.

Minimum dataset partitions:

- development set
- validation set
- holdout set

Recommended additional structure:

- ordinary sessions
- macro-event sessions
- high-volatility sessions
- low-liquidity sessions

Avoid reusing the same recent weak sessions for both design motivation and final validation.

## Offline Replay Stage

The first implementation target is a deterministic replay engine.

It should reconstruct, for each cycle:

- market and option inputs used by the decision stack
- `RegimeSnapshot`
- `StrategyIntent`
- `HorizonIntent`
- `Opportunity`
- allocation decision
- `ExecutionIntent`

The replay output should be stored in canonical tables or equivalent structured artifacts so results are queryable.

## Offline Metrics

### Ranking Metrics

- top-1 versus watch-band outcome separation
- top-3 average outcome versus retained non-top slice
- per-family outcome separation
- per-horizon outcome separation
- percentage of sessions where `pass` beat forced exposure

### Allocation Metrics

- allocated outcome versus top-ranked unallocated outcome
- concentration reduction versus rank-only baseline
- budget efficiency
- opportunity cost of reserved budget
- frequency of rejected low-quality clustered trades

### Execution Metrics

- modeled versus realized entry quality
- slippage by family and style
- skip rate by family
- stale-quote rejection rate
- execution deterioration avoided by fail-closed behavior

### Stability Metrics

- regime flip stability
- family mix stability
- horizon mix stability
- daily policy churn after calibration updates

## Baselines

The redesign should be compared against at least three baselines:

1. current production board selection
2. rank-only selection without allocator
3. family-restricted variants, such as spreads-only or directional-only subsets

If the new system cannot beat those baselines in offline replay, it should not go live.

## Shadow Mode

Shadow mode means the new system runs continuously but does not send live orders.

It should:

- build the full decision stack on current live inputs
- persist all decisions
- emit operator-visible comparisons against the current live system
- record what it would have allocated and why

Shadow mode should last long enough to cover:

- ordinary sessions
- macro-event sessions
- quiet sessions
- degraded data-quality sessions

## Promotion Gates

The system can move from offline replay to shadow mode only when:

- the decision stack runs end to end without structural gaps
- local link between ranking, allocation, and execution artifacts is auditable
- top-slice quality is better than the main baseline on validation data
- no obvious fail-open behavior exists in product or execution policy

The system can move from shadow mode to limited live rollout only when:

- shadow decisions remain stable across multiple session types
- top recommendations are explainable to an operator
- the new stack is not materially increasing candidate churn
- fail-closed behavior is working as intended

## Limited Live Rollout

Initial live rollout should be narrow.

Recommended first sequence:

1. operator-visible only, no execution
2. paper or equivalent shadow allocation with full persistence
3. live execution on one style-profile and one narrow product set
4. live execution on one additional family
5. staged expansion by family and product class

Recommended live-first scope:

- `tactical`
- cash-settled indexes and top-tier ETFs
- directional debit spreads first

Do not start with:

- same-day short-premium
- iron condors
- single-name short-premium

## Rollback Rules

Immediate rollback or disable should occur when:

- live slippage deteriorates materially relative to shadow expectations
- fail-closed behavior is bypassed
- data-quality incidents lead to unsafe candidate promotion
- allocation starts clustering risk in ways the spec forbids
- recent calibration changes produce visible policy churn or instability

Rollback should disable the affected family, product class, or style, not necessarily the entire system.

## Calibration Governance

Calibration changes should be treated as policy releases.

Required steps:

1. define the intended change and affected scopes
2. rerun offline replay on frozen datasets
3. compare against prior baseline
4. shadow the new calibration before live promotion
5. promote only if the change helps on holdout data and does not worsen execution risk

No same-day calibration push should go directly to live execution.

## Reporting

The evaluation stack should publish at minimum:

- ranking report
- allocation report
- execution-quality report
- family mix report
- product mix report
- drift and calibration report

These reports should be readable by operators without needing raw notebooks.

## Initial Defaults

Use these rollout defaults initially:

- one active style snapshot per symbol-side
- same-day `iron_condor` remains blocked
- reactive short-premium remains restricted to cash-settled indexes and top-tier ETFs
- live rollout begins with debit spreads before short-premium
- carry and single-name expansion happen only after tactical index and ETF validation

## Open Policy Decisions

These should be resolved before live rollout, but they do not block offline replay:

- the minimum shadow duration by style
- the exact promotion thresholds for each metric family
- whether limited live rollout should start with one or two approved symbols

## Success Criteria

The rollout plan is working when:

- design changes move through a repeatable path instead of ad hoc live edits
- offline replay, shadow mode, and live performance stay logically connected
- the system expands by evidence, not by optimism
- rollbacks are narrow and controlled instead of chaotic
