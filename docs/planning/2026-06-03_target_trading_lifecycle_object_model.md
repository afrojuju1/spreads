# Target Trading Lifecycle Object Model

Date: 2026-06-03

Status: target-state decision for `spr-g9s.1`.

Storage refinement: the object boundaries and invariants remain useful, but
the table stance is refined by the proposed
[`spr-n65` single-authority storage ADR](./2026-07-15_single_authority_execution_lifecycle_storage.md).
That ADR uses the option already allowed by this record to reuse current table
names where they cleanly fit the target.

Related:

- [Trading Lifecycle Bead Plan](./2026-06-03_trading_lifecycle_bead_plan.md)
- [Trading Lifecycle State Machines](./2026-06-03_trading_lifecycle_state_machines.md)
- [Nautilus Patterns Inside Spreads](./2026-06-03_nautilus_patterns_inside_spreads_architecture.md)
- [System Architecture](../current_system_state.md)

## Decision Summary

The target trading core should use one lifecycle chain:

```text
TradeSignal
  -> TradeDecision
  -> ExecutionIntent
  -> AdmissionDecision
  -> ExecutionAttempt
  -> BrokerOrderSnapshot / BrokerFill
  -> TradingPosition
  -> CloseDecision / close ExecutionIntent / close ExecutionAttempt
  -> PositionClose
  -> ReconciliationObservation
```

Current Spreads and Nautilus objects are research inputs, not compatibility constraints. Planned Spreads downtime is acceptable during the rewrite. The target model should choose clean object boundaries over old table names, old payload shapes, shims, dual-writes, or old-runtime fallbacks.

The main decision is to collapse today's `SignalState` plus `Opportunity` split into a target `TradeSignal` object. A signal is the normalized setup/candidate fact. A decision is the bot or strategy verdict over that signal. An intent is the durable request to dispatch. Admission is the pre-attempt money-path gate. Attempts own broker submission facts. Positions are projections from fills and closes.

## Common Fields

Every money-path object should carry these fields when applicable:

| Field group | Fields |
| --- | --- |
| Identity | object id, parent id, lineage ids, idempotency key |
| Ownership | bot id, automation id, strategy config id, account id, environment |
| Source | source kind, source job type/key/run id, feed name, scanner cycle id |
| Time | observed/decided/created/updated/expires/submitted/completed timestamps |
| Market context | session date, market session, underlying symbol, root symbol, asset class |
| Strategy context | strategy family, product class, horizon, style profile, config hash |
| Policy | policy snapshot, policy refs, capability profile, risk profile |
| Reasons | reason codes, blockers, note, evidence, metrics |
| Correlation | correlation id, causation id, previous object id, next object id |

## Target Objects

| Target object | Purpose | Current inputs |
| --- | --- | --- |
| `TradeSignal` | Normalized setup or candidate fact from Finviz, scanner, UOA, manual input, or future sources. | `signal_states`, `opportunities`, Finviz feed payloads, discovery candidates |
| `TradeDecision` | Bot/run decision over one signal. | `opportunity_decisions`, Finviz direct rule output |
| `ExecutionIntent` | Durable dispatch request. | `execution_intents` |
| `AdmissionDecision` | Pre-attempt approval, block, or unknown result. | `risk_decisions`, admission errors, lane-local caps |
| `ExecutionAttempt` | Broker submission attempt and adapter request boundary. | `execution_attempts` |
| `BrokerOrderSnapshot` | Broker order fact under an attempt. | `execution_orders`, Alpaca order snapshots |
| `BrokerFill` | Immutable fill fact under an attempt/order. | `execution_fills`, Alpaca activities |
| `TradingPosition` | Session/account position projection from fills and closes. | `portfolio_positions`, `session_positions` |
| `CloseDecision` | Exit rule decision over an open position. | `exit_manager` decisions, force-close commands |
| `PositionClose` | Position impact from close fills. | `position_closes`, `session_position_closes` |
| `ReconciliationObservation` | Broker/local comparison and repair evidence. | `broker_sync_state`, account snapshots, sync notes |
| `LifecycleEvent` | Audit fact for transitions that affect money, position state, or operator action. | `event_log`, `execution_intent_events` |

## TradeSignal

Purpose:

- Normalize every tradeable idea before a bot or strategy decides on it.
- Make Finviz a normal signal source instead of a side door into intent creation.
- Carry execution-shape candidates such as long call contract, vertical legs, or equity order hints without forcing execution.

Core fields:

- `trade_signal_id`
- `source_kind`: `finviz`, `discovery`, `uoa`, `manual`, `research`, `import`
- `source_id`: feed run id, candidate id, manual command id, or scanner cycle id
- `session_date`, `market_session`, `observed_at`, `expires_at`
- `underlying_symbol`, `root_symbol`, `asset_class`
- `strategy_family`, `product_class`, `horizon`, `style_profile`
- `signal_state`
- `rank`, `score`, `confidence`
- `legs`, `execution_shape`, `economics`
- `reason_codes`, `blockers`, `evidence`, `metrics`
- `idempotency_key`

States:

```text
observed -> ready
observed -> blocked
ready -> stale
ready -> consumed
ready -> expired
blocked -> stale
blocked -> expired
stale -> retired
expired -> retired
consumed -> retired
```

State meaning:

| State | Meaning |
| --- | --- |
| `observed` | Source produced a candidate, but normalization has not completed. |
| `ready` | Signal can receive strategy decisions. |
| `blocked` | Source data or normalization found a signal-level blocker. |
| `stale` | Quote/feed/setup age makes the signal unusable without refresh. |
| `expired` | Signal is outside its session or TTL. |
| `consumed` | A selected decision moved forward from this signal. Repeat trades can still create later signals for the same symbol/setup. |
| `retired` | Historical terminal state after cleanup/archive. |

Target stance:

- Replace the current `signal_states` plus `opportunities` split with `TradeSignal` unless Bead 3 finds a materially cleaner storage split.
- Keep source-specific details in `evidence` or `metrics`, not in alternate control paths.
- Preserve canonical legs/economics/evidence semantics from current opportunities.

## TradeDecision

Purpose:

- Record the bot/run verdict over a signal before an intent exists.
- Preserve clear "why nothing happened" answers.
- Separate strategy selection from account/risk admission.

Core fields:

- `trade_decision_id`
- `trade_signal_id`
- `bot_id`, `automation_id`, `strategy_config_id`, `config_hash`
- `run_key`, `scope_key`, `decided_at`
- `decision_state`
- `rank`, `score`, `selected_quantity`
- `selected_execution_shape`
- `reason_codes`, `blockers`, `evidence`, `metrics`
- `supersedes_decision_id`, `superseded_by_decision_id`

States:

| State | Meaning |
| --- | --- |
| `skip` | Runtime gate or setup condition says do not evaluate further. |
| `no_entry` | Evaluated normally, but no attractive entry exists. |
| `selected` | Strategy selected a trade candidate and can create an intent. |
| `selected_blocked` | Strategy selected a candidate but strategy-level policy blocks intent creation. |
| `superseded` | A newer decision for the same scope replaces this decision. |

Transition rules:

- Only `selected` decisions can create open intents.
- `selected_blocked` is not risk admission. It is strategy/config blocking before money-path admission.
- A newer decision can supersede an older non-terminal decision for the same `run_key` and `scope_key`.

Target stance:

- Replace `opportunity_decisions` with `TradeDecision`.
- Fold Finviz direct rule output into this object.
- Preserve reason codes such as timing, spread, budget, option selection, setup reset, and stale quote.

## ExecutionIntent

Purpose:

- Durable request to dispatch an open or close action.
- Own TTL, claim, revocation, supersession, and source validation.
- Stay separate from broker submission attempts.

Core fields:

- `execution_intent_id`
- `intent_kind`: `open`, `close`, `replace`, `cancel`
- `source_object_type`, `source_object_id`
- `bot_id`, `automation_id`, `account_id`
- `slot_key`, `idempotency_key`
- `intent_state`
- `claim_token`, `claimed_at`
- `expires_at`
- `supersedes_intent_id`, `superseded_by_intent_id`
- `payload`
- `policy_snapshot`, `config_hash`
- `created_at`, `updated_at`

States:

```text
pending -> claimed
pending -> expired
pending -> revoked
pending -> superseded
claimed -> submitted
claimed -> failed
claimed -> revoked
submitted -> partially_filled
submitted -> filled
submitted -> canceled
submitted -> failed
partially_filled -> filled
partially_filled -> canceled
partially_filled -> failed
```

State meaning:

| State | Meaning |
| --- | --- |
| `pending` | Intent is eligible for dispatch. |
| `claimed` | A dispatcher owns it by claim token. |
| `submitted` | At least one attempt exists or broker working state is linked. |
| `partially_filled` | Linked attempt has partial fill impact. |
| `filled` | Linked attempt produced intended full position impact. |
| `failed` | Dispatch or linked attempt failed. |
| `canceled` | Linked attempt was canceled without full impact. |
| `revoked` | Source object became invalid before completion. |
| `expired` | TTL elapsed before dispatch/completion. |
| `superseded` | Replacement intent took over. |

Target stance:

- Keep the `ExecutionIntent` concept.
- Rebuild state handling around typed transitions.
- Keep supersession as first-class lineage instead of mutating the original intent.

## AdmissionDecision

Purpose:

- Decide whether an intent can become a broker submission attempt.
- Distinguish money-path risk/account uncertainty from strategy decision results.
- Stop before attempt creation when blocked or unknown.

Core fields:

- `admission_decision_id`
- `execution_intent_id`
- `trade_signal_id`, `trade_decision_id`
- `position_id` for close admission
- `admission_kind`: `open`, `close`, `replace`
- `admission_state`
- `account_id`, `session_date`
- `requested_quantity`, `requested_notional`, `max_loss`
- `policy_snapshot`, `capability_snapshot`
- `metrics`, `reason_codes`, `blockers`, `evidence`, `note`
- `decided_at`
- `execution_attempt_id` when approved admission is attached to an attempt

States:

| State | Meaning |
| --- | --- |
| `approved` | Policy, account, environment, and broker-capability checks allow attempt creation. |
| `blocked` | Known policy/account/capability condition blocks attempt creation. |
| `unknown` | Required state is unavailable or too uncertain to approve safely. |

Transition rules:

- `approved` can attach to exactly one attempt.
- `blocked` and `unknown` cannot create an attempt.
- `unknown` is not a generic failure and is not approval.
- Close admission can share the object if close policy fits; otherwise use the same fields under `CloseAdmission`.

Target stance:

- Replace or rename current `risk_decisions` as `AdmissionDecision`.
- Direct Finviz long calls must use this lifecycle.
- Lane-local caps can feed admission metrics, but they should not bypass admission.

## ExecutionAttempt

Purpose:

- Own one broker submission try.
- Separate local queue state, adapter submission state, broker order state, and position impact.
- Preserve uncertainty with `submit_unknown`.

Core fields:

- `execution_attempt_id`
- `execution_intent_id`
- `admission_decision_id`
- `attempt_kind`: `open`, `close`, `replace`, `cancel`
- `attempt_state`
- `account_id`, `broker`, `execution_runtime`
- `client_order_id`, `primary_broker_order_id`
- `requested_quantity`, `requested_limit_price`
- `canonical_legs`
- `order_payload`
- `policy_snapshot`, `economics`, `source_job`
- `requested_at`, `queued_at`, `submitted_at`, `completed_at`, `updated_at`
- `error_code`, `error_text`
- `supersedes_attempt_id`, `superseded_by_attempt_id`

States:

| State | Meaning |
| --- | --- |
| `pending_submission` | Local attempt exists but adapter has not confirmed broker submission. |
| `submit_unknown` | Worker/job outcome is stale or ambiguous after possible broker submission. |
| `working` | Broker accepted or is processing the order. |
| `partially_filled` | Broker reports partial fill. |
| `canceling` | Cancel requested, broker terminal state not confirmed. |
| `filled` | Broker fill completed intended quantity. |
| `canceled` | Broker canceled without intended full impact. |
| `rejected` | Broker rejected. |
| `expired` | Broker expired or done-for-day. |
| `failed` | Local adapter/runtime failed before or after submission. |
| `stale` | Operator action or reconciliation is required. |

Required `submit_unknown` behavior:

- Reconcile by `client_order_id` before failing or retrying.
- Do not create a replacement attempt until broker non-existence is established or operator chooses recovery.
- Count unresolved `submit_unknown` as capacity/exposure risk until resolved.

Target stance:

- Keep attempt immutability for broker-facing history.
- Rebuild the attempt fields if necessary.
- Broker raw statuses map into target attempt states and remain available in order snapshots.

## BrokerOrderSnapshot

Purpose:

- Store broker order facts under an attempt.
- Preserve raw broker payload while projecting normalized status for the lifecycle.

Core fields:

- `broker_order_snapshot_id`
- `execution_attempt_id`
- `broker`, `broker_order_id`, `parent_broker_order_id`, `client_order_id`
- `broker_status`, `normalized_order_state`
- `symbol`, `asset_class`, `side`, `position_intent`
- `order_type`, `time_in_force`, `order_class`
- `quantity`, `filled_quantity`, `remaining_quantity`, `limit_price`, `filled_avg_price`
- `submitted_at`, `updated_at`
- `raw_payload`

States:

- `working`
- `partially_filled`
- `filled`
- `pending_cancel`
- `canceled`
- `rejected`
- `expired`
- `replaced`
- `unknown`

Target stance:

- Keep broker order snapshots as facts.
- Do not use broker order rows as the strategy or position source of truth.

## BrokerFill

Purpose:

- Immutable broker fill fact.
- Drive position projection and close impact.

Core fields:

- `broker_fill_id`
- `execution_attempt_id`
- `broker_order_id`
- `symbol`, `side`, `position_intent`
- `quantity`, `price`, `value`
- `filled_at`
- `raw_payload`

Target stance:

- Keep unique broker fill identity.
- Fills belong under attempts/orders.
- Position state is recalculated from fill facts and close facts.

## TradingPosition

Purpose:

- Session/account position projection from filled open attempts and close facts.
- Answer "what do we own, why, in which session, and what can close it?"

Core fields:

- `position_id`
- `account_id`
- `session_date`, `market_session`
- `source_trade_signal_id`, `opening_trade_decision_id`
- `opening_execution_intent_id`, `opening_execution_attempt_id`
- `position_state`
- `underlying_symbol`, `root_symbol`, `strategy_family`, `product_class`
- `canonical_legs`
- `opened_quantity`, `remaining_quantity`
- `entry_value`, `realized_pnl`, `unrealized_pnl`
- `mark`, `mark_source`, `marked_at`
- `risk_policy_snapshot`, `exit_policy_snapshot`
- `reconciliation_state`, `last_reconciled_at`, `reconciliation_note`
- `opened_at`, `closed_at`, `updated_at`

States:

| State | Meaning |
| --- | --- |
| `pending_open` | Fill evidence is incomplete but an opening attempt may create exposure. |
| `partial_open` | Opening attempt filled less than requested quantity. |
| `open` | Position has positive remaining quantity. |
| `partial_close` | At least one close reduced quantity but exposure remains. |
| `closed` | Remaining quantity is zero. |

Target stance:

- Merge current `portfolio_positions` and `session_positions` semantics into one target position projection unless Bead 3 chooses a cleaner read-model split.
- Keep session/day ownership explicit.
- Broker positions reconcile the projection; they do not assign strategy/session ownership by themselves.

## CloseDecision

Purpose:

- Record exit-manager, manual, or policy decision over an open position before a close intent exists.
- Keep close reason separate from broker close attempt state.

Core fields:

- `close_decision_id`
- `position_id`
- `decision_state`
- `reason`: `profit_target`, `stop_loss`, `max_hold`, `force_close`, `stale_order`, `operator`, `hold`
- `quantity_to_close`
- `limit_source`, `limit_price`, `mark_source`
- `policy_snapshot`
- `reason_codes`, `blockers`, `evidence`, `metrics`
- `decided_at`
- `execution_intent_id` when a close intent is created

States:

| State | Meaning |
| --- | --- |
| `hold` | Position remains open. |
| `close_selected` | Close intent should be created. |
| `blocked` | Known guard blocks close intent or close admission. |
| `unknown` | Required quote/broker/position state is unavailable. |
| `superseded` | Later close decision replaces this one. |

Target stance:

- Add `CloseDecision` as a first-class object.
- Reuse `ExecutionIntent` and `ExecutionAttempt` for close submission.
- Keep one-active-close-per-position unless Bead 1/3 replacement policy explicitly chooses a more expressive close-order model.

## PositionClose

Purpose:

- Record position impact from close fills.
- Feed remaining quantity and realized PnL projection.

Core fields:

- `position_close_id`
- `position_id`
- `close_decision_id`
- `execution_intent_id`
- `execution_attempt_id`
- `closed_quantity`
- `exit_value`
- `realized_pnl`
- `broker_order_id`
- `closed_at`, `created_at`, `updated_at`

Target stance:

- Keep close facts linked to close attempts.
- Do not let a close order alone imply a close fact without fill evidence or explicit broker reconciliation proof.

## ReconciliationObservation

Purpose:

- Record broker/local comparison and recovery decisions without making broker sync the strategy owner.

Core fields:

- `reconciliation_observation_id`
- `account_id`, `broker`, `observed_at`
- `object_type`, `object_id`
- `broker_order_id`, `client_order_id`, `position_id`
- `reconciliation_state`
- `reason_codes`, `evidence`, `raw_payload`
- `repair_action`, `repair_attempted_at`, `repair_result`

States:

- `matched`
- `broker_missing`
- `local_missing`
- `quantity_mismatch`
- `status_mismatch`
- `submit_unknown_unresolved`
- `repaired`
- `ignored`

Target stance:

- Keep reconciliation as a lifecycle concern.
- Broker sync can update projections and emit observations, but it does not decide strategy ownership.

## LifecycleEvent

Purpose:

- Audit transitions that affect money, exposure, operator action, or broker truth.
- Avoid flooding the domain event log with internal implementation chatter.

Core fields:

- `lifecycle_event_id`
- `event_type`
- `object_type`, `object_id`
- `from_state`, `to_state`
- `correlation_id`, `causation_id`
- `occurred_at`
- `payload`

Target stance:

- Promote a subset of `event_log` and `execution_intent_events` into a clear engine event stream.
- Commands request action; events state facts; tables project operator state.

## Cross-Object Invariants

- A signal can exist without a decision.
- A decision can exist without an intent.
- Only `selected` open decisions can create open intents.
- An intent can exist without an admission decision.
- An admission decision can block or be unknown before any attempt exists.
- Only approved admission can create or attach to an attempt.
- An attempt can exist without a broker order while local or uncertain.
- `submit_unknown` must reconcile by client order id before cleanup, retry, or replacement.
- Broker orders and fills belong under attempts.
- Positions are projections from filled open attempts and close facts.
- Close decisions are separate from close attempts.
- Only one active close exposure per position is allowed unless a replacement close policy explicitly supersedes it.
- Repricing and replacement create lineage; they do not mutate the original intent/attempt into a new history.
- Runtime provenance is required for every object that can affect money or exposure.

## Current Table Stance

| Current table/object | Target stance |
| --- | --- |
| `signal_states` | Replace or merge into `TradeSignal`; archive current rows unless needed for historical reporting. |
| `signal_state_transitions` | Replace with `LifecycleEvent` for target transitions. |
| `opportunities` | Merge into `TradeSignal`; preserve useful legs/economics/evidence fields. |
| `opportunity_decisions` | Replace with `TradeDecision`. |
| Finviz feed payloads | Normalize into `TradeSignal`; no direct feed-to-intent lane. |
| `execution_intents` | Keep concept, rebuild fields/state handling as needed. |
| `execution_intent_events` | Replace or merge into `LifecycleEvent`. |
| `risk_decisions` | Replace or rename as `AdmissionDecision`. |
| `execution_attempts` | Keep concept, rebuild around target attempt fields and states. |
| `execution_orders` | Keep as `BrokerOrderSnapshot` facts. |
| `execution_fills` | Keep as `BrokerFill` facts. |
| `portfolio_positions` | Replace/merge into `TradingPosition` projection. |
| `session_positions` | Merge session/day ownership into `TradingPosition` or a narrow projection chosen by Bead 3. |
| `position_closes` | Keep concept as `PositionClose`, with target links to close decision, intent, and attempt. |
| `session_position_closes` | Merge into `PositionClose` or target session-close projection. |
| `event_log` | Keep global operational log; promote engine lifecycle subset or create dedicated lifecycle event storage. |
| `job_runs` | Keep as runtime provenance; not a lifecycle owner. |
| Nautilus bridge state | Reference only; do not make target storage depend on Rust runtime output shape. |

## Historical Data Cutover

Downtime is acceptable, so historical handling should favor clarity over live compatibility.

| Data group | Cutover stance |
| --- | --- |
| Old signals/opportunities/decisions | Archive read-only by default. Backfill only if needed for operator history. |
| Pending old intents | Reset or revoke during cutover. Do not carry ambiguous pending work into the new lifecycle. |
| Terminal old attempts/orders/fills | Archive as historical broker ledger. Optional backfill into target facts if reporting needs it. |
| `submit_unknown` attempts | Resolve before cutover or import as explicit `submit_unknown` attempts requiring reconciliation. |
| Open positions | Prefer closing before cutover. If positions remain open, import from broker plus local evidence into `TradingPosition` with reconciliation evidence. |
| Closed positions and closes | Archive by default. Backfill only if realized PnL reporting needs old history. |
| Event log | Keep as operational history; do not replay every old event into the target lifecycle unless needed. |
| Quote/log data | Keep retention policy separate from lifecycle rewrite; do not backfill quote streams into lifecycle facts. |

## Bead 2 Handoff

The contract module should define typed enums and transition helpers for:

- `TradeSignalState`
- `TradeDecisionState`
- `ExecutionIntentState`
- `AdmissionState`
- `ExecutionAttemptState`
- `BrokerOrderState`
- `TradingPositionState`
- `CloseDecisionState`
- `ReconciliationState`

It should also define value objects or typed aliases for:

- source reference
- policy snapshot
- lifecycle reason
- canonical leg
- execution shape
- lineage reference

## Bead 3 Handoff

The storage bead should choose clean target tables/projections from this model. It should not preserve old tables just to avoid downtime.

Minimum target storage groups:

- lifecycle facts: signals, decisions, intents, admissions, attempts, orders, fills, close decisions, closes, reconciliation observations
- projections: active signals, active intents, active attempts, active positions, operator lifecycle summary
- history/archive: old rows or target historical facts, depending on cutover choice

The storage design can still reuse current table names where they match the target object cleanly. Reuse is allowed because it is clean, not because compatibility is required.
