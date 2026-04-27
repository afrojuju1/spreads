# Multi-Paper Alpaca Account Plan

Status: proposed

As of: Monday, April 27, 2026

Related:

- [System Architecture](../current_system_state.md)
- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md)
- [Alpaca Options Automation System Architecture](./2026-04-15_alpaca_options_automation_system_architecture.md)

## Goal

Enable the runtime to use up to three Alpaca paper trading accounts under one operator account without breaking the current architecture boundary:

- discovery and selection stay shared and account-agnostic
- execution admission, broker sync, positions, and ops become account-aware

This plan is intentionally deferred work. It is a design checkpoint, not an active rollout.

## External Constraint

Alpaca currently supports multiple paper trading accounts per user, each with separate credentials and separate paper-account state.

For this repo, that means:

- one global `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` pair is no longer sufficient
- account identity has to become explicit in broker-facing runtime state

## Current State

The repo is single-account at the broker boundary today.

Concrete signs:

- one global client factory in `packages/core/services/alpaca.py`
- one global broker-sync job in `packages/config/jobs/broker_sync.yaml`
- one global sync key in `packages/core/services/broker_sync.py`
- account snapshots and sync state are keyed by broker only in `packages/core/storage/broker_models.py` and `packages/core/storage/broker_repository.py`
- account overview and ops views read one latest Alpaca snapshot in `packages/core/services/account_state.py` and `packages/core/services/ops/`
- execution, reconciliation, repricing, and risk admission all resolve Alpaca credentials from global environment variables
- bot and automation config has no account binding today

This is structurally fine for one paper account, but it cannot safely expand to several.

## Non-Goals

This work should not:

- create one independent discovery stack per paper account
- create several market recorders for the same market-data lane
- make selection account-specific
- duplicate scanner or opportunity logic just to partition paper accounts

Those approaches would fight the current architecture in [System Architecture](../current_system_state.md).

## Recommendation Summary

Keep one shared selection system.

Add a broker-account plane under execution and ops.

Bind account ownership at the bot plane.

The durable model is:

1. one shared discovery and signal path
2. one named broker-account registry
3. one `broker_account_ref` per bot
4. per-account sync, buying power, order submission, reconciliation, and position views

## Recommended Architecture

## 1. Introduce a broker-account registry

Add config-backed broker-account refs, for example under:

```text
packages/config/broker_accounts/
  paper_main.yaml
  paper_alt_1.yaml
  paper_alt_2.yaml
```

Each ref should declare:

- `broker`
- `environment`
- `trading_base_url`
- `data_base_url`
- env-var names for key id and secret key
- optional display name

Do not hardcode several credential pairs into the runtime.

## 2. Bind account identity at the bot owner plane

Extend bot config to include:

- `broker_account_ref`

Reason:

- bots are the current operator ownership plane
- selection is already shared upstream
- entry and management attribution already sits naturally on `bot_id` and `automation_id`

This keeps account routing explicit without infecting the discovery boundary.

## 3. Make broker/account services account-aware

The following services should resolve a named broker account instead of reading one global Alpaca credential pair:

- `packages/core/services/broker_sync.py`
- `packages/core/services/account_state.py`
- `packages/core/services/risk_manager.py`
- `packages/core/services/execution/`
- `packages/core/services/execution_intents/repricing.py`
- `packages/core/services/execution_portfolio.py`
- `packages/core/services/execution/guard.py`

The market-data discovery path should remain shared unless a later requirement proves otherwise.

## 4. Persist broker account identity on broker-owned state

Add explicit account identity to broker-facing persisted state:

- account snapshots
- broker sync state
- execution attempts
- execution orders
- execution fills
- portfolio positions
- risk decisions
- position closes

Minimum fields:

- `broker_account_ref`
- `broker_account_number`

The ref is our stable runtime identity.

The account number is the broker-returned identity we reconcile against.

## 5. Make broker-order uniqueness composite

Current execution tables assume one Alpaca namespace.

That is unsafe once several paper accounts are active, even if Alpaca order ids happen to be globally unique in practice.

Shift uniqueness and lookup to:

- `(broker, broker_account_ref, broker_order_id)`
- `(broker, broker_account_ref, broker_fill_id)`

and thread the same composite identity through reconciliation.

## 6. Run broker sync per account

Replace the single broker-sync identity with one declared job per broker account, for example:

- `broker_sync:alpaca:paper_main`
- `broker_sync:alpaca:paper_alt_1`
- `broker_sync:alpaca:paper_alt_2`

Each job should:

- resolve credentials from the broker-account registry
- snapshot only that account
- reconcile only positions and execution rows bound to that account

## 7. Keep ops readable

Operator surfaces should not collapse several accounts into one misleading summary.

Required additions:

- per-account broker-sync health
- per-account buying power and account equity
- per-account open positions
- per-account daily PnL
- per-account blocked-by-buying-power counts

System-wide rollups can still exist, but they must be explicit aggregates.

## Phased Implementation Plan

## Phase 1: Config and identity foundation

- add broker-account config files and loader
- add `broker_account_ref` to bot config
- add account-aware Alpaca client resolution helpers

Exit criteria:

- runtime can resolve several named Alpaca paper accounts from config plus env
- no live behavior change yet

## Phase 2: Storage and sync split

- migrate broker/account/execution tables to carry account identity
- make broker sync run per account
- update account overview storage reads to be account-aware

Exit criteria:

- the system can snapshot and reconcile several paper accounts without row collisions

## Phase 3: Execution routing

- route risk admission and order submission through the bot’s `broker_account_ref`
- bind created execution attempts, orders, fills, positions, and closes to that account
- update exit-manager and repricing paths to resolve the correct account

Exit criteria:

- a bot can open, manage, and reconcile positions on its assigned paper account end to end

## Phase 4: Ops and rollout

- add per-account ops views and rollups
- move one low-risk paper bot to a second paper account
- validate sync, buying power, fills, repricing, exits, and reporting
- then distribute additional bots across accounts

Exit criteria:

- at least two paper accounts run concurrently with correct attribution and clean ops visibility

## Verification Plan

Use live/runtime validation, not new test work by default.

Primary checks:

- `docker compose ps`
- `uv run spreads status --json`
- `uv run spreads trading --json`
- `uv run spreads jobs --json`
- targeted `uv run spreads automations --bot-id ... --automation-id ... --date ... --json`

What must be proven in rollout:

- per-account broker sync stays healthy
- a bot never submits orders to the wrong paper account
- buying-power blocks are computed from the assigned account, not a global account
- reconciliation only matches against positions and activities from the assigned account
- ops surfaces remain interpretable when multiple accounts are active

## Open Questions

- whether account binding should live only on bots or also be overridable on individual automations
- whether market-recorder/operator views need any account-specific filtering beyond execution and sync views
- whether we want one shared strategy mix across accounts or deliberate account specialization by product family

## Recommendation

When this work resumes, start with Phase 1 and Phase 2 together.

Do not start with ad hoc env duplication or multiple full stacks.

The right boundary is:

- shared discovery upstream
- explicit account routing downstream
