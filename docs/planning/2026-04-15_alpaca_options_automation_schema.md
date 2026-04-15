# Alpaca Options Automation Schema

Status: proposed

As of: Wednesday, April 15, 2026

Related:

- [Alpaca Options Automation System Architecture](./2026-04-15_alpaca_options_automation_system_architecture.md)

## Goal

Define the `v1` schema for the CLI-first, single-operator options automation system.

This document covers entity relationships, required fields, and persistence rules.

## Storage Model

- Postgres is the primary state store.
- Keep append-only lifecycle history for decisions, intents, orders, positions, approvals, and automation runs.
- Keep current-state materializations for fast CLI reads.
- Store large replay artifacts and snapshot payloads outside row bodies and persist references in Postgres.
- Record `config_hash` and `policy_version` anywhere a decision can later be audited or replayed.

## Definition Layers

`v1` should split strategy definition across code, checked-in config, and runtime state.

```text
Strategy (code)
      |
      v
StrategyConfig (config)
      |
      v
Automation (config)
      |
      v
Bot (config + runtime state)
      |
      v
StrategyPosition (runtime state)
```

Rules:

- `Strategy` is code-backed and not a Postgres table in `v1`
- `StrategyConfig` is config-backed and referenced by ID plus `config_hash`
- `Automation` is config-backed and referenced by ID plus `config_hash`
- runtime rows must persist `strategy_config_id`, `automation_id` where applicable, and `config_hash`

## Relationship View

```text
 strategies (code)
         |
         +--< strategy_configs (config)
                    |
                     +--< automations (config) ---< automation_runs --< opportunities --< opportunity_decisions --< execution_intents --< execution_intent_events
                                              |
 bots ----------------------------------------+
  |
  +--< strategy_positions
  |
  +--< replay_runs

 underlying_snapshots --< chain_snapshots
          |                    |
          +--------< opportunities >--------+

 replay_runs --< replay_artifacts
```

More explicit cardinality view:

```text
Strategy (1, code) -----------< StrategyConfig (many, config)
StrategyConfig (1, config) ---< Automation (many, config)

Bot (1) ----------------------< AutomationRun (many)
Bot (1) ----------------------< Opportunity (many)
Bot (1) ----------------------< StrategyPosition (many)
Bot (1) ----------------------< ReplayRun (many)

Automation (1, config) -------< AutomationRun (many)
StrategyConfig (1, config) ---< Opportunity (many)
StrategyConfig (1, config) ---< StrategyPosition (many)

AutomationRun (1) ------------< Opportunity (many)
Opportunity (1) --------------< OpportunityDecision (many)
OpportunityDecision (1) ------< ExecutionIntent (0..1 entry path)
StrategyPosition (1) ---------< ExecutionIntent (many management path)
ExecutionIntent (1) ----------< ExecutionIntentEvent (many)

UnderlyingSnapshot (1) -------< ChainSnapshot (many)
UnderlyingSnapshot (1) -------< Opportunity (many)
ChainSnapshot (1) ------------< Opportunity (many)

ReplayRun (1) ----------------< ReplayArtifact (many)
```

Derived runtime planning:

- `cold` target state is derived from universes and shortlist inputs; it does not need persisted target rows
- `warm` and `hot` target states are persisted through `market_recorder_targets`

## Core Entities

### Strategy

Code-backed definition, not a table in `v1`.

Required fields at the definition level:

- `strategy_id`
- `builder_name`
- `supported_leg_shape`
- `supported_actions[]`
- `version`

Persistence rule:

- versioned in source control; runtime rows persist only the strategy identifier

### StrategyConfig

Config-backed definition, not a Postgres table in `v1`.

Required fields:

- `strategy_config_id`
- `strategy_id`
- `entry_recipe_refs[]`
- `management_recipe_refs[]`
- `builder_params`
- `liquidity_rules`
- `risk_defaults`
- `policy_version`

Persistence rule:

- stored in checked-in YAML; runtime rows persist `strategy_config_id`, `policy_version`, and `config_hash`

### Automation

Config-backed definition, not a Postgres table in `v1`.

Required fields:

- `automation_id`
- `strategy_config_id`
- `automation_type`
- `schedule`
- `universe_ref`
- `approval_mode`
- `execution_mode`
- `enabled`

Persistence rule:

- stored in checked-in YAML; runtime rows persist `automation_id` and `config_hash`

### Bot

Required fields:

- `bot_id`
- `name`
- `status`
- `capital_limit`
- `max_open_positions`
- `max_daily_actions`
- `automation_ids[]`
- `universe_ref`
- `policy_version`
- `config_hash`
- `created_at`
- `updated_at`

Persistence rule:

- keep one current row per bot plus append-only config-change history

### AutomationRun

Required fields:

- `automation_run_id`
- `bot_id`
- `automation_id`
- `strategy_config_id`
- `trigger_type`
- `started_at`
- `completed_at`
- `result`
- `decision_log_ref`
- `policy_version`
- `config_hash`

Persistence rule:

- append one row per invocation whether or not it produced an opportunity

### UnderlyingSnapshot

Required fields:

- `underlying_snapshot_id`
- `symbol`
- `captured_at`
- `last_price`
- `day_volume`
- `relative_volume`
- `daily_change_pct`
- `news_flag`
- `event_flags[]`
- `feature_blob_ref`

Persistence rule:

- store normalized summary fields plus a reference to any larger derived feature payload

### ChainSnapshot

Required fields:

- `chain_snapshot_id`
- `underlying_snapshot_id`
- `symbol`
- `captured_at`
- `quote_mode`
- `expiration_dates[]`
- `chain_blob_ref`

Persistence rule:

- persist one snapshot row per enrichment cycle and keep detailed contract payloads in a referenced blob or child table

### RecorderTarget

Required fields:

- `scope_key`
- `symbol`
- `contract_symbols[]`
- `target_state`
- `priority`
- `reason`
- `expires_at`
- `updated_at`

State rules:

- `cold`: derived only, not persisted as recorder-target rows
- `warm`: persisted target row for shortlisted names and lightweight chain refresh
- `hot`: persisted target row for concrete live contract coverage

TTL rules:

- `warm`: `300s`
- `hot` discovery: `90s`
- `hot` risk and active-order coverage: `120s`, refreshed continuously while the source condition remains true

Priority bands:

- `100`: open-risk monitoring
- `90`: active working orders
- `70`: selected or approved entry candidates
- `50`: top-ranked discretionary discovery candidates
- `30`: warm shortlist coverage

Persistence rule:

- use the existing `market_recorder_targets` table as the canonical current target set for `warm` and `hot`

### Opportunity

Required fields:

- `opportunity_id`
- `bot_id`
- `automation_run_id`
- `strategy_config_id`
- `strategy_id`
- `underlying_symbol`
- `underlying_snapshot_id`
- `chain_snapshot_id`
- `proposed_legs[]`
- `score`
- `reason_codes[]`
- `quote_mode`
- `policy_version`
- `config_hash`
- `expires_at`
- `status`
- `created_at`

Persistence rule:

- persist only opportunities that reach `hot`, approval review, submission, or rejection

### OpportunityDecision

Required fields:

- `opportunity_decision_id`
- `opportunity_id`
- `bot_id`
- `automation_id`
- `run_key`
- `scope_key`
- `policy_ref`
- `state`
- `score`
- `rank`
- `reason_codes[]`
- `superseded_by_id`
- `decided_at`
- `payload`

Persistence rule:

- this is a first-class table in `v1`; every completed decision pass should write one row per in-scope opportunity

### StrategyPosition

Required fields:

- `strategy_position_id`
- `bot_id`
- `opening_execution_intent_id`
- `opening_opportunity_id`
- `strategy_config_id`
- `strategy_id`
- `underlying_symbol`
- `legs[]`
- `state`
- `opened_at`
- `closed_at`
- `risk_budget`
- `exit_policy_version`
- `broker_refs[]`

Persistence rule:

- this is the canonical trade record and must survive the full lifecycle from proposal through reconciliation and closure

### ExecutionIntent

Required fields:

- `execution_intent_id`
- `bot_id`
- `automation_id`
- `opportunity_decision_id`
- `strategy_position_id`
- `action_type`
- `slot_key`
- `claim_token`
- `policy_ref`
- `state`
- `expires_at`
- `superseded_by_id`
- `payload`
- `created_at`
- `updated_at`

Persistence rule:

- this is a first-class table in `v1`; it is the universal action handoff into the OMS and links downstream to existing `execution_attempts`, `execution_orders`, and `execution_fills`

### ExecutionIntentEvent

Required fields:

- `execution_intent_event_id`
- `execution_intent_id`
- `event_type`
- `event_at`
- `payload`

Persistence rule:

- append for claim, submit, replace, partial fill, fill, cancel, revoke, reject, expire, and reconciliation updates

### ReplayRun

Required fields:

- `replay_run_id`
- `bot_id`
- `label`
- `started_at`
- `completed_at`
- `time_window`
- `policy_versions[]`
- `config_hash`
- `dataset_refs[]`
- `result_summary`
- `status`

Persistence rule:

- treat replay runs as immutable after completion except for operator notes

### ReplayArtifact

Required fields:

- `replay_artifact_id`
- `replay_run_id`
- `artifact_type`
- `artifact_ref`
- `created_at`

Persistence rule:

- store reports, logs, and portfolio curves outside table bodies and persist references here

## Notes

- `policy_version` can stay a scalar field in `v1`; it does not need its own table yet.
- `config_hash` is the audit key that ties a bot, strategy config, automation, automation run, opportunity, opportunity decision, execution intent, and replay run back to the same resolved CLI configuration.
- If replay later needs full trade-level persistence, add replay-specific trade and order tables rather than reusing live `StrategyPosition` rows.
