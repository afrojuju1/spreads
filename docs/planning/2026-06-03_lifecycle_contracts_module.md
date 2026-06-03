# Lifecycle Contracts Module

Date: 2026-06-03

Status: implementation note for `spr-g9s.2`.

Related:

- [Target Trading Lifecycle Object Model](./2026-06-03_target_trading_lifecycle_object_model.md)
- [Trading Lifecycle Bead Plan](./2026-06-03_trading_lifecycle_bead_plan.md)

## Shipped Module

`packages/core/services/trading_lifecycle.py` defines the first target lifecycle contracts:

- `TradeSignalState`
- `TradeDecisionState`
- `ExecutionIntentState`
- `AdmissionState`
- `ExecutionAttemptState`
- `BrokerOrderState`
- `TradingPositionState`
- `CloseDecisionState`
- `PositionCloseState`
- `ReconciliationState`

It also defines:

- `LifecycleObject`
- `LifecycleTransitionDecision`
- `LifecycleTransitionError`
- `normalize_lifecycle_state(...)`
- `allowed_next_states(...)`
- `is_terminal_lifecycle_state(...)`
- `validate_lifecycle_transition(...)`
- `require_lifecycle_transition(...)`

## Normalization Stance

The module accepts current state names where they still map cleanly to the target model. Examples:

- Alpaca working statuses such as `accepted`, `new`, `pending_new`, and `replaced` normalize to `ExecutionAttemptState.WORKING`.
- `pending_cancel` normalizes to `ExecutionAttemptState.CANCELING`.
- `done_for_day` normalizes to `ExecutionAttemptState.EXPIRED`.
- `submit_unknown` remains explicit.
- `approved`, `blocked`, and `unknown` remain explicit admission states.

State names that do not fit the target model should be removed or mapped deliberately in later beads.

## Runtime Risk

No live writer or dispatcher uses this module yet. That is intentional for `spr-g9s.2`: this bead introduces typed contracts before storage and runtime rewrites. Until later beads adopt the module, current runtime behavior remains governed by existing string state checks in execution, risk, and close-management services.

Residual risk:

- Later beads can drift if they add new lifecycle states without updating this module first.
- Existing live code can still write old string states until `spr-g9s.3` through `spr-g9s.8` replace those paths.

Close condition for this bead is targeted compile validation, not live behavior validation.
