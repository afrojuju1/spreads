# Opportunity Schema

Status: proposed

As of: Saturday, April 11, 2026

Related:

- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md)
- [Regime Detection Specification](./2026-04-11_regime_detection_spec.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Portfolio Allocation Specification](./2026-04-12_allocation_spec.md)
- [Execution Templates](./2026-04-12_execution_templates.md)
- [Evaluation And Rollout Plan](./2026-04-12_evaluation_and_rollout_plan.md)

## Goal

Define the canonical persisted schema for the strategy-selection system.

This schema should support:

- regime snapshots
- strategy intents
- candidate opportunities
- execution intents
- lifecycle state changes
- post-market outcome analysis
- execution-quality analysis

`board` and `watchlist` should remain derived views, not separate base tables.

## Design Rules

1. One canonical path from regime to opportunity to execution.
2. One `Opportunity` row per candidate per cycle.
3. `style_profile` should represent execution and risk posture, not a fixed DTE bucket.
4. `HorizonIntent` should be stored separately from `StrategyIntent`.
5. Legs belong in a separate child table.
6. State transitions should be stored as events, not only as the latest state.
7. Opportunity outcomes and execution results should be stored separately.
8. `board` and `watchlist` should be derived from state and allocation state.

## Entity Relationships

Mermaid source:

- [2026-04-11_opportunity_schema_relationships.mmd](../diagrams/planning/2026-04-11_opportunity_schema_relationships.mmd)

## Core Entities

## 1. `regime_snapshots`

Purpose:

- persist one `RegimeSnapshot` per `symbol + style_profile + observed_at`

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid primary key` | snapshot id |
| `cycle_id` | `text` | collector or analysis cycle id |
| `symbol` | `text not null` | underlying symbol |
| `style_profile` | `text not null` | `reactive`, `tactical`, `carry` |
| `observed_at` | `timestamptz not null` | snapshot time |
| `direction_bias` | `text not null` | enum-like |
| `direction_score` | `numeric(6,4)` | signed |
| `trend_strength` | `numeric(6,4)` | normalized |
| `intraday_structure` | `text not null` | enum-like |
| `vol_level` | `text not null` | enum-like |
| `vol_trend` | `text not null` | enum-like |
| `iv_vs_rv` | `text not null` | enum-like |
| `event_state` | `text not null` | enum-like |
| `liquidity_state` | `text not null` | enum-like |
| `data_quality_state` | `text not null` | enum-like |
| `confidence` | `numeric(6,4) not null` | normalized |
| `supporting_features` | `jsonb not null default '{}'::jsonb` | evidence payload |
| `created_at` | `timestamptz not null default now()` | audit |

Suggested uniqueness:

- unique on `cycle_id, symbol, style_profile`

Suggested indexes:

- `(symbol, style_profile, observed_at desc)`
- `(style_profile, observed_at desc)`
- `(event_state, observed_at desc)`

## 2. `strategy_intents`

Purpose:

- persist ranked strategy-policy outputs for a snapshot

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid primary key` | intent id |
| `regime_snapshot_id` | `uuid not null references regime_snapshots(id)` | parent snapshot |
| `cycle_id` | `text not null` | denormalized for filtering |
| `symbol` | `text not null` | denormalized for filtering |
| `style_profile` | `text not null` | denormalized for filtering |
| `strategy_family` | `text not null` | family enum-like |
| `thesis_direction` | `text not null` | `bullish`, `bearish`, `neutral`, `pass` |
| `desirability_score` | `numeric(6,4) not null` | normalized |
| `confidence` | `numeric(6,4) not null` | normalized |
| `policy_state` | `text not null` | `preferred`, `allowed`, `discouraged`, `blocked`, `pass` |
| `rank` | `integer not null` | within snapshot |
| `preferred_width_min` | `numeric(10,4)` | optional |
| `preferred_width_max` | `numeric(10,4)` | optional |
| `sizing_class` | `text not null` | `none`, `xsmall`, `small`, `normal` |
| `blockers` | `jsonb not null default '[]'::jsonb` | blocker list |
| `evidence` | `jsonb not null default '{}'::jsonb` | scoring rationale |
| `created_at` | `timestamptz not null default now()` | audit |

Suggested uniqueness:

- unique on `regime_snapshot_id, strategy_family`

Suggested indexes:

- `(cycle_id, symbol, style_profile, rank)`
- `(strategy_family, policy_state, created_at desc)`

## 3. `horizon_intents`

Purpose:

- persist dynamic DTE and expiration-selection choices after strategy-family selection

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid primary key` | horizon intent id |
| `strategy_intent_id` | `uuid not null references strategy_intents(id)` | parent strategy intent |
| `cycle_id` | `text not null` | denormalized |
| `symbol` | `text not null` | denormalized |
| `style_profile` | `text not null` | denormalized |
| `strategy_family` | `text not null` | denormalized |
| `horizon_band` | `text not null` | `same_day`, `next_daily`, `near_term`, `post_event`, `swing`, `carry` |
| `target_dte_min` | `smallint not null` | lower bound |
| `target_dte_max` | `smallint not null` | upper bound |
| `preferred_expiration_type` | `text not null` | `daily`, `weekly`, `monthly`, `post_event`, etc. |
| `event_timing_rule` | `text` | before_event, after_event, none |
| `urgency` | `text not null` | `high`, `normal`, `low` |
| `blockers` | `jsonb not null default '[]'::jsonb` | blocker list |
| `evidence` | `jsonb not null default '{}'::jsonb` | rationale |
| `created_at` | `timestamptz not null default now()` | audit |

Suggested uniqueness:

- unique on `strategy_intent_id, horizon_band`

Suggested indexes:

- `(cycle_id, symbol, style_profile, created_at desc)`
- `(horizon_band, preferred_expiration_type, created_at desc)`

## 4. `opportunities`

Purpose:

- persist one canonical candidate opportunity per cycle

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid primary key` | opportunity id |
| `cycle_id` | `text not null` | parent cycle |
| `regime_snapshot_id` | `uuid references regime_snapshots(id)` | snapshot link |
| `strategy_intent_id` | `uuid references strategy_intents(id)` | policy link |
| `horizon_intent_id` | `uuid references horizon_intents(id)` | horizon link |
| `symbol` | `text not null` | underlying |
| `style_profile` | `text not null` | `reactive`, `tactical`, `carry` |
| `strategy_family` | `text not null` | family |
| `thesis_direction` | `text not null` | `bullish`, `bearish`, `neutral` |
| `horizon_band` | `text not null` | selected horizon band |
| `target_dte` | `smallint` | selected target DTE |
| `expiration_date` | `date not null` | expiry |
| `contract_signature` | `text not null` | canonical signature across legs |
| `product_type` | `text not null` | index, ETF, equity |
| `exercise_style` | `text not null` | American, European |
| `settlement_type` | `text not null` | cash, physical |
| `payoff_class` | `text not null` | long premium, short premium, neutral premium |
| `margin_cost` | `numeric(14,4)` | expected capital usage |
| `max_profit` | `numeric(14,4)` | modeled max profit |
| `max_loss` | `numeric(14,4)` | modeled max loss |
| `notional_exposure` | `numeric(14,4)` | optional exposure summary |
| `execution_complexity` | `text not null` | single_leg, vertical, condor |
| `data_quality_state` | `text not null` | latest quality summary |
| `discovery_score` | `numeric(6,4)` | broad quality |
| `promotion_score` | `numeric(6,4)` | ranking quality |
| `execution_score` | `numeric(6,4)` | nullable until planned |
| `style_rank` | `integer` | rank within style profile |
| `allocation_rank` | `integer` | rank after allocation |
| `state` | `text not null` | lifecycle state |
| `state_reason` | `text not null` | primary classification |
| `allocation_reason` | `text` | nullable |
| `quote_freshness_ms` | `integer` | quote age |
| `execution_readiness` | `jsonb not null default '{}'::jsonb` | live execution summary |
| `raw_features` | `jsonb not null default '{}'::jsonb` | builder features |
| `evidence` | `jsonb not null default '{}'::jsonb` | explainability payload |
| `created_at` | `timestamptz not null default now()` | audit |
| `updated_at` | `timestamptz not null default now()` | audit |

Suggested uniqueness:

- unique on `cycle_id, symbol, style_profile, strategy_family, contract_signature`

Suggested indexes:

- `(cycle_id, style_profile, style_rank)`
- `(cycle_id, state, allocation_rank nulls last)`
- `(symbol, style_profile, created_at desc)`
- `(strategy_family, state, created_at desc)`

## 5. `opportunity_legs`

Purpose:

- normalize leg-level details for single-leg and multi-leg structures

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid primary key` | leg id |
| `opportunity_id` | `uuid not null references opportunities(id)` | parent |
| `leg_index` | `smallint not null` | deterministic order |
| `option_symbol` | `text not null` | contract symbol |
| `position_side` | `text not null` | `long`, `short` |
| `option_type` | `text not null` | `call`, `put` |
| `ratio` | `numeric(10,4) not null` | leg ratio |
| `quantity` | `integer not null default 1` | contracts |
| `strike_price` | `numeric(10,4) not null` | strike |
| `expiration_date` | `date not null` | expiry |
| `delta` | `numeric(10,4)` | optional |
| `bid_price` | `numeric(10,4)` | snapshot |
| `ask_price` | `numeric(10,4)` | snapshot |
| `mark_price` | `numeric(10,4)` | snapshot |
| `created_at` | `timestamptz not null default now()` | audit |

Suggested uniqueness:

- unique on `opportunity_id, leg_index`

Suggested indexes:

- `(option_symbol)`
- `(expiration_date, strike_price)`

## 6. `execution_intents`

Purpose:

- persist strategy-specific execution plans derived from allocated opportunities

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid primary key` | execution intent id |
| `opportunity_id` | `uuid not null references opportunities(id)` | parent opportunity |
| `horizon_intent_id` | `uuid references horizon_intents(id)` | denormalized link |
| `cycle_id` | `text not null` | denormalized |
| `strategy_family` | `text not null` | family |
| `order_structure` | `text not null` | single_leg, vertical, condor |
| `status` | `text not null` | `planned`, `submitted`, `filled`, `canceled`, `expired`, `rejected` |
| `entry_policy` | `jsonb not null default '{}'::jsonb` | entry behavior |
| `price_policy` | `jsonb not null default '{}'::jsonb` | target pricing |
| `timeout_policy` | `jsonb not null default '{}'::jsonb` | timeout rules |
| `replace_policy` | `jsonb not null default '{}'::jsonb` | replace ladder |
| `exit_policy` | `jsonb not null default '{}'::jsonb` | planned exits |
| `validation_state` | `text not null` | `valid`, `stale`, `blocked` |
| `broker_order_group_id` | `text` | broker correlation id |
| `created_at` | `timestamptz not null default now()` | audit |
| `updated_at` | `timestamptz not null default now()` | audit |

Suggested indexes:

- `(opportunity_id, created_at desc)`
- `(status, created_at desc)`

## 7. `opportunity_state_events`

Purpose:

- preserve lifecycle history for audit and diagnostics

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `bigserial primary key` | event id |
| `opportunity_id` | `uuid not null references opportunities(id)` | parent |
| `occurred_at` | `timestamptz not null` | transition time |
| `old_state` | `text` | nullable for first state |
| `new_state` | `text not null` | target state |
| `reason` | `text not null` | primary reason |
| `source_component` | `text not null` | ranking, allocator, executor, exit engine |
| `payload` | `jsonb not null default '{}'::jsonb` | additional detail |

Suggested indexes:

- `(opportunity_id, occurred_at desc)`
- `(new_state, occurred_at desc)`

## 8. `opportunity_outcomes`

Purpose:

- store post-market or end-of-life opportunity evaluation

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid primary key` | outcome id |
| `opportunity_id` | `uuid not null references opportunities(id)` | parent |
| `evaluated_at` | `timestamptz not null` | evaluation time |
| `model_version` | `text not null` | outcome evaluator version |
| `modeled_entry_price` | `numeric(10,4)` | modeled |
| `modeled_exit_price` | `numeric(10,4)` | modeled |
| `modeled_pnl` | `numeric(14,4)` | modeled |
| `max_favorable_excursion` | `numeric(14,4)` | modeled |
| `max_adverse_excursion` | `numeric(14,4)` | modeled |
| `outcome_bucket` | `text not null` | win, loss, neutral |
| `summary` | `jsonb not null default '{}'::jsonb` | analysis payload |

Suggested uniqueness:

- unique on `opportunity_id, model_version`

## 9. `execution_results`

Purpose:

- store realized execution quality separately from opportunity outcome

Suggested columns:

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid primary key` | execution result id |
| `execution_intent_id` | `uuid not null references execution_intents(id)` | parent |
| `submitted_at` | `timestamptz` | broker submit |
| `first_fill_at` | `timestamptz` | first fill |
| `completed_at` | `timestamptz` | completion |
| `completion_status` | `text not null` | filled, partial, canceled, rejected |
| `expected_entry_price` | `numeric(10,4)` | expectation |
| `avg_fill_price` | `numeric(10,4)` | realized |
| `slippage_bps` | `numeric(10,4)` | realized quality |
| `fills_payload` | `jsonb not null default '{}'::jsonb` | broker fill detail |

Suggested indexes:

- `(execution_intent_id)`
- `(completion_status, completed_at desc)`

## Enumerated Value Sets

The schema should use constrained enums in application logic first, then database enums once stable.

Important value sets:

- `style_profile`: `reactive`, `tactical`, `carry`
- `strategy_family`: `long_call`, `long_put`, `call_credit_spread`, `put_credit_spread`, `call_debit_spread`, `put_debit_spread`, `iron_condor`
- `horizon_band`: `same_day`, `next_daily`, `near_term`, `post_event`, `swing`, `carry`
- `state`: `promotable`, `monitor`, `blocked`, `discarded`, `allocated`, `submitted`, `open`, `closed`
- `policy_state`: `preferred`, `allowed`, `discouraged`, `blocked`, `pass`
- `direction_bias`: `bullish`, `bearish`, `neutral`
- `liquidity_state`: `healthy`, `thin`, `degraded`
- `data_quality_state`: `healthy`, `thin`, `stale`, `degraded`

## Derived Views

The schema should expose derived views rather than extra storage tables.

### `v_board`

Definition:

- `opportunities.state = 'allocated'`
- highest `allocation_rank`
- execution still valid or near-valid

### `v_watchlist`

Definition:

- retained but not board
- usually `monitor` or `blocked`
- informative reasons preserved

### `v_cycle_summary`

Definition:

- per cycle style-profile counts
- top-ranked opportunities
- blocked reason counts
- allocation and execution summary counts

## State Model

Expected normal progression:

1. `promotable` or `monitor`
2. `allocated` or remain unallocated
3. `submitted`
4. `open`
5. `closed`

Interruptions:

- `blocked`
- `discarded`

Transitions should always be represented in `opportunity_state_events`.

## Operational Notes

- `board` and `watchlist` should not get their own write paths
- `execution_score` should remain nullable until execution planning occurs
- legs should be queryable independently for quote refresh and audit
- outcome evaluation and execution evaluation should never be merged into one table

## Open Schema Decisions

These still need confirmation later:

- whether `cycle_id` remains a plain text key or becomes a foreign key to a canonical cycle table
- whether `strategy_intents` should be fully stored for every cycle or only when at least one family is `allowed`
- whether `policy_state = pass` deserves its own persisted row per snapshot
- whether `horizon_intents` should be persisted for every allowed strategy family or only the top few

## Success Criteria

The schema is working when:

- all operator views can be built from canonical tables and derived views
- no separate board/watchlist persistence path exists
- execution and outcome analysis are both explainable from stored state
- family-level, style-level, and horizon-level evaluation queries are straightforward
