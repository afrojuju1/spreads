# Strategy Catalog And Profile Contract

Date: 2026-06-11

Status: implemented clean cut on 2026-06-12 by `spr-cwg`.

Related:

- [Current System State](../current_system_state.md)
- [Trading Engine Inspiration Repos](./2026-06-08_trading_engine_inspiration_repos.md)

## Decision

Spreads runtime strategy config is authored through one catalog/profile model under `packages/config/strategies`.

```text
catalog.yaml
  -> profiles.yaml
     -> source model
     -> strategy archetype profile
     -> routine profiles
     -> quality/liquidity profiles
     -> structure model
     -> portfolio model
     -> protection model
     -> executor profile
     -> exit controller
  -> runtime TradingStrategyConfig
  -> daily strategy ledger
```

`trading_strategy_id` remains the runtime identity. Catalog entries and profiles are authored configuration contracts, not broker facts, account facts, or a second runtime owner.

Paper, shadow, and live are values under `execution.mode`. They are not file names, directory names, strategy ids, or alternate loaders.

## Runtime Owners

| Object | Owns | Does not own | Runtime owner |
|---|---|---|---|
| Strategy catalog entry | One authored strategy identity, activation, execution posture, thesis, archetype reference, trade structure, structure model reference, optional portfolio model reference, and small thesis-level overrides. | Repeated schedules, copied thresholds, broker lifecycle facts, or account state. | `packages/config/strategies/catalog.yaml`, loaded by `services/trading_strategies.py`. |
| Strategy archetype profile | Reusable family defaults for source, quality, portfolio, protection, executor, exit controller, and routine profiles. | `trading_strategy_id`, broker orders, fills, or positions. | `packages/config/strategies/profiles.yaml`. |
| Source model | The source of underlyings and freshness expectations. Can reference static symbols, dynamic ticker sources, event sources, or future factor screens. | Option-chain construction or trade selection. | `packages/config/strategies/profiles.yaml`, `packages/config/ticker_sources`, `services/ticker_sources.py`, `services/trading_engine/data_runtime.py`. |
| Quality profile | Ordered account-agnostic filters and scoring stages over facts and candidate structure snapshots. | Buying power, open positions, submit admission, or exits. | `services/trading_engine/entry_quality_pipeline.py`, `packages/core/domain/profiles.py`, and candidate builders. |
| Portfolio model | Account-aware sizing, family caps, symbol caps, daily entry caps, max risk, and admission evidence. | Entry signal quality or broker order construction. | `services/risk_manager.py` entry admission plus `portfolio_admission` payloads. |
| Protection model | Strategy/family run guards such as cooldowns, stale-source/data halts, drawdown halts, loss streak halts, event-window blocks, and operator control-plane protections. | Candidate scoring or order lifecycle. | Runtime policy, entry blockers, risk admission, and profile config. |
| Executor profile | Approval mode, execution posture, runtime adapter, order style, quote freshness gate, repricing/cancel policy, and structure-submit compatibility. | Strategy selection, position ownership, or PnL. | `services/execution_intents/`, `services/execution/`, `execution_submit`, and profile config. |
| Exit controller | Manage cadence, mark refresh expectations, profit/stop/expiry recipes, close admission handoff, and close-intent policy. | Open-entry selection or broker sync truth. | `services/exit_manager.py`, `services/trading_engine/portfolio_runtime.py`, `services/trading_engine/close_policy.py`, `services/trading_engine/risk_runtime.py`, and profile config. |
| Trade-structure spec | Code-level reusable candidate-builder contract for an option construction family. | Strategy activation, execution mode, or source selection. | `services/trade_structure_specs.py` and candidate builders. |
| Evidence ledger | Per-strategy daily read model for source, candidates, blockers, signals, decisions, admissions, intents, attempts, positions, marks, closes, PnL, and config hash. | Persistence ownership of raw facts. | `services/ops/`, engine fact repositories, execution repositories, and positions read models. |

## Runtime Flow

```text
catalog entry + profiles
  -> TradingStrategyConfig
  -> DataEngine source resolution
  -> candidate build + structure snapshot
  -> QualityProfile + signal evidence
  -> TradeSignal + TradeDecision
  -> ProtectionModel checks
  -> PortfolioModel admission
  -> ExecutionIntent
  -> ExecutorProfile
  -> Attempt + Order + Fill
  -> Position
  -> ExitController
  -> close admission + close intent + close attempt
  -> EvidenceLedger projection
```

This preserves the current runtime spine:

```text
DataEngine -> StrategyEngine -> RiskEngine -> ExecutionEngine -> PortfolioEngine -> Ops projections
```

## Authored Shape

Catalog entries stay small:

```yaml
trading_strategy_id: short_dated_index_call_credit
name: Short-Dated Index Call Credit
activation:
  state: active
  paused: false
execution:
  mode: paper
archetype: index_credit_vertical
trade_structure: call_credit_spread
source_model: liquid_index_etfs
structure_model: call_credit_weekly
thesis: Sell short-dated defined-risk premium on liquid index ETFs when skew, liquidity, and expected-value evidence clear the family profile.
entry:
  build:
    selection_limit: 60
  recipes:
    - trend_resistance
```

Profiles carry reusable behavior:

```yaml
archetypes:
  index_credit_vertical:
    source_model: liquid_index_etfs
    quality_profile: call_credit_spread_v1
    portfolio_model: index_credit_defined_risk
    protection_model: standard_intraday
    executor_profile: alpaca_auto_direct
    exit_controller: short_dated_credit_exit
    routine_profile:
      entry: index_full_day_entry
      manage: index_full_day_manage
```

The loader composes these into existing `TradingStrategyConfig` objects. Scheduler and workers consume only the composed runtime config.

## Current Rules

Keep:

- `trading_strategy_id` as the canonical runtime owner.
- `trade_structure` names such as `long_call`, `call_credit_spread`, `long_straddle`, and `short_put`.
- `execution.mode`, `execution.approval`, and `execution.runtime` as execution posture inputs resolved through executor profiles.
- `entry.quality_profile` identifiers for account-agnostic selection.
- Canonical `execution_shape.legs[]`, signed net price, intent, attempt, order, fill, and position facts.
- `config_hash` computed from the composed runtime payload.

Do not reintroduce:

- Per-strategy runtime YAML files.
- Paper-specific strategy config paths.
- Sidecar migration catalogs.
- Broker-environment declarations as strategy config knobs.
- Hidden analysis or shadow flags that bypass admission or execution posture.
- Vertical-only assumptions in long-vol, short-put, or other non-vertical families.

## Completion Notes

The clean cut removed the old authored runtime path and made `packages/config/strategies/catalog.yaml` plus `packages/config/strategies/profiles.yaml` the scheduler-loaded source of truth.

Follow-up tuning should start from `spreads ops strategy-ledger --date <YYYY-MM-DD>` and stored engine facts. Change catalog/profile values only when ledger evidence supports the change.
