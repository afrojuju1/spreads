# Nautilus Architecture Review

Date: 2026-06-03

Status: investigation note for Spreads consolidation planning.

Scope: upstream NautilusTrader architecture patterns, Ade's local `nautilus_trader` fork, and the current host-managed Alpaca options engine runtime.

## Executive Summary

NautilusTrader has excellent trading-engine architecture patterns: domain-driven models, event-driven command and event flow, cache-before-publish data handling, explicit risk/execution/portfolio components, adapter boundaries, and a strong backtest/sandbox/live parity mindset.

Ade's local fork also contains practical Alpaca options work that is directly relevant:

- an options-engine loop
- account-scoped fleet configs
- candidate ledgers
- strategy state persistence
- broker permission checks
- submit/manage/close logic
- multi-leg order submission through Nautilus order commands
- systemd deployment for account-specific engines

The main problem is not quality. The main problem is fit. The current Nautilus path is Rust-heavy, separate from the Spreads product/control plane, partly outside Docker, and not fully integrated with Spreads' Postgres state, dashboards, logs, jobs, or operator workflows. It is a good engine-pattern library and proving ground, but it is not the best long-term place to house Ade's single live trading system if the goal is one repo and one operational surface.

## Upstream Nautilus Patterns Worth Understanding

Official NautilusTrader docs describe the framework as using several major architecture patterns:

- domain-driven design
- event-driven architecture
- messaging patterns
- ports and adapters
- crash-only design
- common backtest/sandbox/live system core

Core components include:

- `NautilusKernel`: orchestration and lifecycle.
- `MessageBus`: pub/sub, request/response, command/event routing.
- `Cache`: instruments, accounts, orders, positions, and market data state.
- `DataEngine`: market data routing and subscription handling.
- `ExecutionEngine`: order lifecycle, routing, execution reports, and reconciliation.
- `RiskEngine`: pre-trade and runtime risk checks.
- `Portfolio`: account, exposure, position, and performance state.
- Adapters: venue-specific data and execution translation.

The most important design idea is that the engine has a clean internal system boundary. Market data, commands, and execution events flow through typed internal components instead of being scattered across unrelated application services.

Official research sources:

- [NautilusTrader Architecture](https://nautilustrader.io/docs/latest/concepts/architecture/)
- [NautilusTrader Message Bus](https://nautilustrader.io/docs/latest/concepts/message_bus/)
- [NautilusTrader Cache](https://nautilustrader.io/docs/latest/concepts/cache/)
- [NautilusTrader Live Trading](https://nautilustrader.io/docs/latest/concepts/live/)
- [NautilusTrader Event Sourcing](https://nautilustrader.io/docs/latest/concepts/event_sourcing/)
- [NautilusTrader Data](https://nautilustrader.io/docs/latest/concepts/data/)
- [NautilusTrader Execution](https://nautilustrader.io/docs/latest/concepts/execution/)
- [NautilusTrader Adapters](https://nautilustrader.io/docs/latest/concepts/adapters/)
- [NautilusTrader DST](https://nautilustrader.io/docs/latest/concepts/dst/)

## Upstream Runtime Flow

The core runtime pattern can be summarized as:

```text
venue data adapter
  |
  v
DataEngine
  |
  +-- write latest data to Cache
  |
  v
MessageBus publish
  |
  v
strategy handler
  |
  v
order command
  |
  v
RiskEngine
  |
  v
ExecutionEngine
  |
  v
execution adapter
  |
  v
venue
  |
  v
execution events -> Cache -> Portfolio -> Strategy/Operator
```

The cache-before-publish rule is especially useful: when a strategy receives a quote or event, the latest state is already available from the cache/read model.

The single-threaded kernel idea is also useful, but should be adapted carefully. Spreads should not copy an actor runtime just because Nautilus has one. The useful pattern is deterministic command ordering and explicit component ownership, not the exact threading model.

## Local Repository Shape

The local `nautilus_trader` repo contains both upstream framework code and Ade's Alpaca options work.

Important local areas:

- `crates/`: Rust crates for model, data, execution, portfolio, risk, live, backtest, event store, persistence, adapters, and related infrastructure.
- `nautilus_trader/`: Python/Cython package surface for the framework.
- `crates/adapters/alpaca`: Ade's Alpaca adapter and options-engine binaries.

The Alpaca adapter crate builds many operator and runtime binaries, including:

- `alpaca-options-engine`
- `alpaca-options-backtest`
- `alpaca-operator-status`
- `alpaca-fleet-status`
- `alpaca-candidate-alerts`
- `alpaca-performance-report`
- `alpaca-sync-strategy-state`
- `alpaca-submit-order-list-bridge`

The bridge binary is the part Spreads currently calls for Nautilus runtime submissions.

## Current Deployment Shape

The local Nautilus Docker Compose file runs support services only:

```text
nautilus-database
nautilus-redis
nautilus-pgadmin
```

The trading engines themselves are host-managed user systemd services, not Docker containers.

Current host services observed during investigation:

- `alpaca-options.service`
- `alpaca-options@paper-defined-risk.service`
- `alpaca-options@paper-undefined-risk.service`

These run `/home/ade/.local/bin/alpaca-options-engine` from the `nautilus_trader` checkout. This means live broker activity can exist outside the Spreads stack and outside Spreads' Docker logging conventions.

## Alpaca Options Engine

The local options engine is centered in:

- `crates/adapters/alpaca/src/options_engine.rs`
- `crates/adapters/alpaca/src/options_runtime.rs`
- `crates/adapters/alpaca/src/runtime.rs`
- `crates/adapters/alpaca/src/options_engine/submission.rs`
- `crates/adapters/alpaca/src/options_engine/reconciliation.rs`

Its runtime loop is conceptually:

```text
load config and strategy state
  |
  v
reconcile broker state
  |
  v
repeat every interval:
  |
  +-- manage existing entries
  +-- evaluate strategy
  +-- skip / no-entry / risk-block / selected-blocked / selected
  +-- submit if selected and allowed
  +-- persist strategy state
  +-- sleep
```

The strategy decision shape is good:

- `Skip`: runtime gate blocks evaluation.
- `NoEntry`: nothing attractive enough.
- `RiskBlocked`: risk/account constraints block.
- `SelectedBlocked`: a candidate was selected but broker/config/policy blocked it.
- `Selected`: candidate is ready to submit or dry-run.

This distinction is worth carrying into Spreads because it keeps "nothing happened" from becoming a mystery.

## Local Strategy State

The Rust runtime keeps strategy state with entries that include:

- trade date
- underlying
- strategy family
- order list id
- option symbols
- quantity
- credit or debit
- score
- parent order id
- close order list/id/reason/attempt count
- submitted/canceled/closed flags
- timestamps

It also tracks duplicate daily submits and known broker permission rejections. This is practical and operationally useful.

Spreads already has similar tables, but the state is not shaped as a single account engine state machine yet.

## Submission Path

The local Alpaca submission code builds Nautilus commands such as:

- `SubmitOrder`
- `SubmitOrderList`

For options strategies, submit plans encode legs such as:

- credit spread: sell short leg, buy long leg
- debit spread: buy long leg, sell short leg
- iron condor: four defined-risk legs
- naked option: single short leg
- close: reduce-only legs in the opposite direction

The implementation constructs a minimal Nautilus execution session with cache and execution client pieces, submits to Alpaca, collects execution events, and returns a result snapshot.

This is exactly the behavior Spreads needs, but not necessarily in Rust or as a subprocess.

## Config And Fleet Model

The deployment docs describe three config layers:

1. Environment variables for credentials, endpoints, account identity, and emergency gates.
2. TOML config for strategy, scanner, universe, risk, management, state, and ledger settings.
3. Fleet registry for account roles, permissions, risk budgets, service names, and paths.

This gives strong account-level control, but it is one more config system next to Spreads YAML jobs and automations.

The useful pattern to borrow is not TOML specifically. The useful pattern is explicit account-level capability and risk policy.

## Pros

- Strong trading-engine patterns and clear domain boundaries.
- Typed commands, events, model objects, and adapter boundaries.
- Explicit risk, execution, cache, data, portfolio, and live/backtest concepts.
- Strong deterministic-engine mindset.
- Clear account-scoped runtime loops.
- Practical Alpaca options management logic already exists.
- Candidate and performance ledgers preserve selected and blocked opportunities.
- Broker permission checks are first-class, especially for options approval constraints.
- Multi-leg order submission behavior is already proven enough to serve as a reference.
- Systemd service model is lightweight and easy to keep alive.

## Cons And Risks

- Rust-heavy implementation raises maintenance cost for a Spreads-centered product.
- Local options engine is not fully clean upstream `TradingNode`; it is a fork-specific account engine that uses pieces of Nautilus.
- Runtime state, logs, Postgres schema, and operator tools are separate from Spreads.
- Host systemd engines are outside the main Spreads Docker stack.
- Broker ownership can be duplicated between standalone Nautilus engines and Spreads submissions.
- Config is split across env, TOML, fleet registry, and Spreads YAML.
- Spreads dashboards and CLI do not naturally see all Nautilus lifecycle details.
- The Python options `TradingNode` path is not the complete answer today.
- Continuing the bridge path long term keeps deploys dependent on rebuilt Rust binaries.

## Borrow, Do Not Copy

Borrow these patterns:

- typed command boundaries
- event journal for trading facts
- cache/read model before strategy decisions
- risk before execution
- execution adapter as a narrow port
- order state machine
- explicit strategy decision results
- account-scoped policy and capability model
- reconciliation as a first-class loop
- backtest/live parity as a design constraint

Do not copy these wholesale:

- Rust as the primary implementation language for Ade's Spreads runtime
- separate Nautilus Postgres as the live source of truth
- separate host-managed engines as live broker owners
- a full actor framework
- a second scheduler
- a second operator CLI as the primary surface

## Local Sources

- `/home/ade/Projects/nautilus_trader/Cargo.toml`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/Cargo.toml`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/src/options_engine.rs`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/src/options_runtime.rs`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/src/runtime.rs`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/src/options_engine/submission.rs`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/src/options_engine/reconciliation.rs`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/src/storage/postgres.rs`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/bin/options_engine.rs`
- `/home/ade/Projects/nautilus_trader/crates/adapters/alpaca/bin/submit_order_list_bridge.rs`
- `/home/ade/Projects/nautilus_trader/docs/developer_guide/alpaca_nuc_deployment.md`
- `/home/ade/Projects/nautilus_trader/docs/developer_guide/alpaca_options_trading_node_migration.md`
- `/home/ade/Projects/nautilus_trader/docs/developer_guide/alpaca_base_architecture_and_spreads_plan.md`
- `/home/ade/Projects/nautilus_trader/.docker/docker-compose.yml`
