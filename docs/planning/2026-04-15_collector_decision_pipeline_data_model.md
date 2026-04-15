# Collector Decision Pipeline Data Model

Status: partial reference (partially incorporated on 2026-04-15)

Incorporation note:

- `OpportunityDecision` in this document remains a useful canonical decision artifact
- the newer implementation path now uses top-level nouns `Bot`, `StrategyConfig`, `Automation`, and `StrategyPosition`
- `opportunity_execution` in this document should be read as an entry-specific precursor to a broader `ExecutionIntent` concept
- see [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md) for the active current-system migration path

As of: Wednesday, April 15, 2026

Related:

- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md)
- [Collector Decision Pipeline Design](./2026-04-15_collector_decision_pipeline_design.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Execution Templates](./2026-04-12_execution_templates.md)

## Goal

Define the minimum runtime data model for:

- strategies
- opportunities
- opportunity_decisions
- opportunity_executions

This is narrower than the older [Opportunity Schema](./2026-04-11_opportunity_schema.md). It is the implementation-facing model for the collector -> opportunity -> opportunity_decision -> opportunity_execution pipeline.

## Core Relationship Map

```text
strategy (code-owned)
    |
    +-- embedded signal_spec
    |
    +------------------------------+
                                   |
opportunity -----------------------+
    |                              |
    | carries strategy_family      |
    | carries style/horizon/product|
    | carries evidence             |
    v
opportunity_decision ---------> opportunity_execution
    |                              |
    | carries run context          | one chosen selected action
    | (run_key/scope/source/policy)|
    v                              v
  peer opportunity_decisions   orders / fills
```

## Ownership

| Entity | Owner |
|---|---|
| `strategy` | code-owned canonical module |
| `opportunity` | `signal_state.py` / signal store |
| `opportunity_decision` | `decision_engine.py` |
| `opportunity_execution` | `execution.py` / execution store |

## Relational Vs Embedded

Keep these as first-class entities:

- `opportunity`
- `opportunity_decision`
- `opportunity_execution`

Keep these embedded first:

- `signal_spec` inside `strategy`
- `strategy_decision` inside `opportunity.evidence` and/or opportunity_decision payloads
- pass-level source details inside `opportunity_decision.source` or `opportunity_decision.payload`
- detailed policy/template payloads inside `opportunity_execution.payload`

Rule:

- if the system needs to join, dedupe, claim, supersede, or reconcile it, make it first-class
- if it is explanatory or template detail, prefer JSON payloads first

## Entities

## 1. `strategy`

Static strategy catalog entry. Not a runtime current-state table in the first implementation.

Required fields:

| Field | Purpose |
|---|---|
| `strategy_family` | stable family key |
| `display_name` | operator-facing name |
| `thesis_kind` | directional, neutral, volatility, etc. |
| `leg_structure` | single, vertical, condor, straddle, strangle |
| `signal_spec` | activation, confirmation, invalidation, evidence rules |
| `builder_constraints` | delta, width, DTE, liquidity limits |
| `product_constraints` | allowed product classes |
| `style_rules` | allowed styles and posture rules |
| `horizon_rules` | allowed horizons |
| `portfolio_rules` | portfolio interaction rules |
| `risk_rules` | risk-specific blockers/adjustments |
| `default_execution_template` | entry template family |
| `default_exit_template` | exit template family |
| `assignment_risk_class` | risk classification |
| `complexity` | relative complexity |

Rule:

- `strategy_family` is not just a payoff label. It includes signal semantics, construction rules, decision rules, and execution mapping.

## 2. `opportunity`

Canonical opportunity row. In runtime, this is the current-state handoff from discovery into decisioning.

Required fields:

| Field | Purpose |
|---|---|
| `id` | primary key |
| `cycle_id` | source collector cycle |
| `pipeline_id` | source collector/pipeline |
| `session_date` / `market_date` | date scope |
| `symbol` / `root_symbol` | underlying |
| `strategy_family` | canonical family |
| `style_profile` | execution/risk posture |
| `horizon_intent` | current horizon posture |
| `product_class` | product policy class |
| `selection_state` | coarse discovery tier |
| `selection_rank` | within-cycle ordering |
| `eligibility_state` | current live vs analysis-only eligibility |
| `execution_shape` | entry structure payload |
| `strategy_metrics` | strategy-specific metrics |
| `economics` | capital / max loss / returns |
| `legs` | canonical legs |
| `evidence` | explainability payload |
| `expires_at` | current-truth expiry |
| `updated_at` | freshness timestamp |

Relationship rules:

- many `opportunity` rows can share one `strategy_family`
- `selection_state` is discovery-tier only, not final action truth

## 3. `strategy_decision` payload

This is the runtime strategy admissibility payload. It does not need to be a standalone table on day one, but it needs one canonical shape.

Recommended shape:

| Field | Purpose |
|---|---|
| `strategy_family` | family being judged |
| `activation` | `{state, reason}` |
| `policy` | `{state, score, blockers, ref}` |
| `posture` | `{style_profile, horizon_intent, product_class}` |
| `policy_ref` | versioned policy context |
| `evidence` | strategy rationale |

Relationship rules:

- one symbol may have multiple `strategy_decision` payloads, one per `strategy_family`
- one `opportunity` should be explainable in terms of one strategy decision path
- strategy activation and policy admissibility must both be explicit

Recommended first implementation:

- embed this payload into opportunity evidence and/or opportunity_decision artifacts

## 4. `opportunity_decision`

Per-opportunity outcome for one planning pass.

Required fields:

| Field | Purpose |
|---|---|
| `id` | primary key |
| `opportunity_id` | opportunity being judged |
| `run_key` | groups rows from the same planning pass |
| `session_date` | trading date |
| `market_session` | session scope |
| `scope` | scope key such as `entry/default` |
| `source` | source-cycle ids and trigger context |
| `policy_ref` | versioned policy context |
| `state` | selected, rejected, blocked, superseded |
| `score` | opportunity_decision score |
| `rank` | rank within pass |
| `reason_codes` | structured outcome reasons |
| `superseded_by_id` | newer opportunity_decision if superseded |
| `decided_at` | opportunity_decision timestamp |
| `payload` | rationale, budget impact, evidence |

Relationship rules:

- many `opportunity_decision` rows may share one `run_key`
- unique on `run_key, opportunity_id`
- every in-scope opportunity should have one decision outcome in a completed planning pass

Recommended idempotency key:

- `session_date`
- `scope`
- normalized `source`
- `policy_ref`

## 5. `opportunity_execution`

Execution-side current-state row. This is the selected opportunity_execution object plus submission lifecycle in one entity.

Required fields:

| Field | Purpose |
|---|---|
| `id` | primary key |
| `opportunity_decision_id` | parent opportunity_decision |
| `opportunity_id` | chosen opportunity |
| `run_key` | copied planning-pass key for grouping |
| `state` | pending, claimed, submitted, partially_filled, filled, canceled, expired, revoked, failed |
| `slot_key` | mutual-exclusion key |
| `rank` | rank among active opportunity_executions |
| `policy_ref` | versioned policy context |
| `expires_at` | stale/invalid after this time |
| `claimed_at` | execution service claimed it |
| `submitted_at` | broker submit succeeded |
| `revoked_at` | invalidated by newer opportunity_decision |
| `superseded_by_id` | newer opportunity_execution if any |
| `claim_token` | concurrency guard |
| `payload` | request, template selectors, validation, rationale, evidence |
| `broker_order_id` | primary broker order linkage if known |
| `request_metadata` | opportunity_execution request context |

Relationship rules:

- one `opportunity_execution` belongs to one `opportunity_decision`
- one `opportunity_execution` points to one `opportunity`
- one `opportunity_execution` may have many child order and fill rows
- only one active open opportunity_execution per `slot_key`

## State Summary

| Entity | States |
|---|---|
| `opportunity_decision.state` | `selected`, `rejected`, `blocked`, `superseded` |
| `opportunity_execution.state` | `pending`, `claimed`, `submitted`, `partially_filled`, `filled`, `canceled`, `expired`, `revoked`, `failed` |

## Key Relationship Rules

1. `strategy` defines allowed strategy behavior; it does not hold runtime current state.
2. `opportunity` is the only canonical opportunity entity in runtime; current-ness belongs to the store/view, not the entity name.
3. `opportunity_decision` is the unit of planning truth per opportunity; shared planning-pass context is flattened into each row via `run_key`, `scope`, `source`, and `policy_ref`.
4. `opportunity_execution` is the only execution-side current-state entity; it carries both pre-submit selection and post-submit lifecycle.
5. Missing rows must never be the only evidence that something was rejected, revoked, or superseded.

## Audit

Minimum requirement:

- every planning pass is auditable through `run_key` and related `opportunity_decision` rows
- every `opportunity_execution` state transition is auditable
- every supersession or revocation is auditable

Append-only events or immutable history rows are both acceptable. Current-state rows alone are not.

## First Implementation

1. Keep the existing opportunity store as canonical.
2. Add a code-owned `strategy` module with embedded signal spec.
3. Reuse the existing execution-attempt storage as the first implementation of `opportunity_execution`.
4. Link `opportunity_decision` to `opportunity_execution`.
5. Add append-only audit for opportunity_decision and opportunity_execution transitions.
