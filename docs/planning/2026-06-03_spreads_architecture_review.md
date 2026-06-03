# Spreads Architecture Review

Date: 2026-06-03

Status: investigation note, not the canonical current-state document.

Canonical current-state source: [../current_system_state.md](../current_system_state.md)

## Purpose

This document reviews the current `spreads` architecture as it exists today, with a focus on what is worth preserving if the trading engine is consolidated back into this repo. It is intentionally descriptive first, evaluative second. It does not propose code changes by itself.

## Executive Summary

`spreads` is already the strongest home for the operator-facing trading product. It has the web app, API, CLI, scheduling layer, runtime job workers, live health checks, Postgres-backed operational state, Redis-backed work queues, central logging work, and the canonical execution/account read models that the operator actually uses.

Its biggest architectural strength is not a single perfect trading engine. Its strength is the surrounding control plane: everything needed to understand, schedule, observe, reconcile, and operate live paper trading is already here.

The main weakness is that trading-engine behavior is distributed across jobs and services instead of being concentrated behind one durable command/event engine boundary. `spreads` has good tables, good jobs, and good operator surfaces, but the flow is still partly lane-specific:

- Canonical discovery and opportunity automation use `opportunities`, `signal_states`, `execution_intents`, and execution attempts.
- Finviz direct trading is currently its own feed-driven lane.
- Options long-call execution currently uses `alpaca_direct`, while some configured spread automations use the Nautilus bridge runtime.
- The bridge gives a clean fail-closed handoff, but it also means live execution ownership is split across Python, a Rust binary, and separate host-managed Nautilus engines.

The best future direction is to preserve the `spreads` product/control-plane shape and add a clearer in-repo engine kernel around the trading lifecycle. That kernel should borrow the useful Nautilus patterns without copying Nautilus wholesale.

## Current Runtime Shape

At a high level, `spreads` is a Python and Next.js modular monolith:

```text
operator
  |
  +-- Next.js web app
  +-- FastAPI API
  +-- Typer CLI: uv run spreads ...
        |
        v
Postgres source of truth <--> Python services
Redis queues/pubsub      <--> ARQ workers and scheduler
        |
        v
Alpaca brokerage and market data integrations
```

The deployed stack is Docker Compose based. The main services are:

- `api`: FastAPI application.
- `web`: operator UI.
- `scheduler`: reads YAML job definitions and enqueues ARQ jobs.
- `worker-runtime`: runtime jobs such as broker sync, execution, direct trading, exits, alerts, and recovery.
- `worker-discovery`: discovery and symbol feed work.
- `worker-valuation`: valuation jobs.
- `worker-research`: profile-gated and normally disabled.
- `market-recorder`: Alpaca options websocket owner for normal runtime quote capture.
- `postgres`: durable operational state.
- `redis`: queues, leases, pub/sub, and runtime coordination.

Important local references:

- [../../docker-compose.yml](../../docker-compose.yml)
- [../../packages/core/jobs/registry.py](../../packages/core/jobs/registry.py)
- [../../packages/config/jobs](../../packages/config/jobs)
- [../../packages/core/cli/main.py](../../packages/core/cli/main.py)

## Worker And Job Topology

The job registry defines the queue ownership:

```text
arq:queue:runtime
  broker_sync
  execution_submit
  alert_delivery
  alert_reconcile
  position_exit_manager
  discovery_recovery
  options_automation_entry
  options_automation_execute
  finviz_direct_trading

arq:queue:discovery
  discovery_run
  symbol_feed

arq:queue:valuation
  company valuation jobs

arq:queue:research
  research jobs, profile-gated
```

This is a pragmatic architecture. It is easy to reason about operationally, easy to restart by lane, and easy to inspect with the shipped CLI. The tradeoff is that engine behavior emerges from scheduled jobs plus shared tables, rather than one explicit trading engine process or component.

## Domain Ownership

The current codebase is split into useful domains. The most important owners are:

| Domain | Current owners |
| --- | --- |
| Operator UI | `packages/web`, `packages/api`, `packages/core/cli` |
| Scheduling and control | `packages/core/jobs`, `services/control_plane.py`, `services/runtime_policy.py` |
| Dynamic symbol feeds | `services/symbol_feeds.py`, `symbol_feed` jobs |
| Market data capture | `services/market_recorder.py`, `services/discovery_recovery.py` |
| Discovery and collection | `services/scanners/`, `services/discovery_runs/`, `live_selection.py`, `opportunity_scoring.py`, `candidate_policy.py` |
| Canonical opportunity state | `signal_state.py`, `opportunity_generation.py`, `opportunities.py`, `storage/signal_repository.py` |
| Runtime and health read models | `automation_runtimes.py`, `discovery_sessions.py`, `live_runtime.py`, `discovery_run_health/`, `pipelines.py`, `ops/` |
| Execution dispatch and admission | `execution_intents/`, `risk_manager.py`, `account_capacity.py`, `risk_decisions` |
| Broker execution ledger | `execution/`, `storage/execution_models.py` |
| Portfolio/session attribution | `execution_portfolio.py`, `session_positions.py`, `broker_sync.py` |
| Exits and close management | `exit_manager.py`, runtime exit jobs |
| Event envelope | `events/bus.py`, `storage/event_models.py` |

This split is mostly good. The important concern is not that there are many files. The concern is that there is no small "engine contract" tying these owners together as command handlers, event producers, projections, and adapters.

## State Model

`spreads` already has strong durable concepts:

- `execution_intents`: durable requests to trade, with TTLs, source context, slot semantics, and claim state.
- `risk_decisions`: current pre-attempt admission facts for opportunity execution.
- `execution_attempts`: immutable attempt records for broker submission tries.
- `execution_orders`: broker order snapshots.
- `execution_fills`: broker fill records.
- `session_positions`: position records with day/session attribution.
- `portfolio_positions`: current portfolio projection.
- `signal_states`: canonical opportunity lifecycle state.
- `opportunities`: normalized opportunity records.
- `opportunity_decisions`: decision outputs and skip/entry reasoning.
- `job_runs`: scheduler/runtime job records.
- `event_log`: global event envelope persisted to Postgres and broadcast through Redis.

This is a strong foundation for a local engine. The main missing piece is treating command/event facts as the primary trading-engine journal, then letting the existing tables act as projections and operator read models.

## Event Envelope

`packages/core/events/bus.py` already builds and publishes a useful event envelope:

```text
event_id
event_class
event_type
topic
occurred_at
ingested_at
source
entity_type
entity_key
session_date
market_session
schema_version
producer_version
correlation_id
causation_id
payload
```

That is very close to the shape needed for an internal trading-engine event stream. Today, it is best understood as a global operational event log and fanout mechanism, not as the primary engine ledger.

Recommendation for future architecture: keep this envelope and promote a subset of trading events to first-class engine facts, rather than introducing a second event system.

## Execution Lifecycle

The current execution boundary is one of the cleanest parts of the system:

```text
opportunity or direct feed candidate
  |
  v
decision service
  |
  v
execution_intent
  |
  v
risk/admission gate
  |
  v
execution_submit job
  |
  +-- alpaca_direct runtime
  |
  +-- nautilus runtime bridge
        |
        v
      alpaca-submit-order-list-bridge
  |
  v
execution_attempts/orders/fills
  |
  v
broker_sync
  |
  v
session_positions and portfolio projections
```

The repo documents a non-negotiable boundary that should be preserved:

- `execution_intents` are the strategy/control-plane handoff.
- `execution_attempts`, `execution_orders`, and `execution_fills` are the immutable broker ledger.
- `session_positions` owns day/session attribution.
- `broker_sync` reconciles broker reality and should not rewrite session ownership.

This is the exact kind of architecture discipline that should survive any consolidation.

## Runtime Modes

`spreads` currently has two execution runtimes:

- `alpaca_direct`: Python-native Alpaca submission.
- `nautilus`: versioned JSON handoff through the `alpaca-submit-order-list-bridge` subprocess.

The Nautilus runtime supports versioned order-list payloads such as:

- `spreads.nautilus.submit_order_list.v1`
- `spreads.nautilus.submit_order.v1`

The bridge is fail-closed. Missing quotes, unsupported strategy families, bad leg sides, unsupported actions, bad net pricing, or bridge failures should stop submission rather than silently falling back.

This pattern is good. The subprocess dependency and split runtime ownership are the parts to remove over time.

## Finviz Direct Trading Lane

The active Finviz lane is a direct feed-driven path:

```text
symbol_feed:finviz_momentum
  |
  v
finviz_direct_trading
  |
  +-- entry rules
  +-- timing rules
  +-- budget/cap checks
  +-- long-call option selection when configured
  |
  v
execution_intent
```

Current long-call selection looks for optionable candidates and filters contracts by rules such as:

- expiration range
- minimum open interest
- minimum daily option volume
- delta band and preferred delta
- quote freshness
- bid/ask availability
- max spread percentage
- optional max premium

It then sorts by preferred delta fit, delta distance, strike distance, DTE, spread quality, volume, and open interest.

This lane is useful and working, but architecturally it is parallel to the canonical discovery/opportunity flow. The cleaner long-term shape is:

```text
Finviz feed -> normalized opportunities -> decision engine -> execution_intents -> admission -> shared execution
```

## Live Evidence Snapshot

A live paper check during this investigation showed the following shape:

- Market was open and trading was allowed.
- Scheduler and worker lanes were healthy.
- Broker sync was current.
- Finviz feed was refreshing with 10 symbols.
- Finviz direct trading saw candidates and skipped with concrete reasons.
- Active intents were 0.
- Open positions were 0.
- Nautilus bridge was ready, but there were no Spreads-side Nautilus attempts for the day at that moment.

This supports the architectural read: the operational shell is healthy and useful, while active trading ownership is still split by lane and runtime.

## Pros

- Strong operator product surface: web, API, CLI, Docker stack, health checks, dashboards.
- Postgres is already the operational source of truth.
- Redis and ARQ give simple, inspectable scheduling and work distribution.
- Execution intent and broker ledger boundaries are already sensible.
- Broker sync and session position ownership are explicitly separated.
- Runtime health is practical and operator-friendly.
- Configuration-driven jobs and automations are easy to roll out.
- The Nautilus bridge handoff is versioned, explicit, and fail-closed.
- The event envelope already has correlation and causation fields.
- The codebase is mostly in the language and deployment shape the operator already uses.

## Cons And Risks

- Trading-engine behavior is spread across jobs and services rather than concentrated behind one engine contract.
- `event_log` is not yet the primary trading command/event journal.
- Finviz direct trading is a parallel lane instead of a normalized opportunity source.
- Long-call Finviz execution currently uses `alpaca_direct`, while some spread automations use the Nautilus bridge; "Nautilus ready" does not mean every live lane is using Nautilus.
- Broker ownership is split across Spreads direct submission, the Nautilus bridge, and standalone host Nautilus services.
- Backtest/live parity is partial because runtime paths and scheduled jobs are not all driven through one deterministic engine loop.
- Quote/log growth needs active retention policy because market-data and log streams can grow without bound.
- Runtime config is distributed across YAML, env, code defaults, and external process assumptions.
- Subprocess execution through Rust bridge binaries makes observability and deploy ownership more awkward than a pure Spreads implementation.

## Architecture Takeaways

The right way forward is not to replace `spreads`. It is to make `spreads` more engine-shaped.

Keep:

- Postgres.
- Redis/ARQ.
- Docker Compose runtime.
- `live-doctor`, `trading`, jobs, and operator CLI.
- Execution intent, broker ledger, broker sync, and session-position concepts, even if the concrete tables are replaced.
- YAML-driven jobs and automation config.
- Central logging and dashboards.

Improve:

- Promote a trading command/event contract inside the repo.
- Normalize direct feed lanes into canonical opportunities.
- Make risk, OMS, execution adapter, portfolio sync, and exit management explicit engine components.
- Reduce split broker ownership.
- Replace the Rust bridge with Python-native adapter behavior once parity is proven.
- Replace storage, services, and operator read models where the current shape fights the target lifecycle.

Do not add yet:

- A second durable database.
- Kafka or a new workflow engine.
- A full actor framework.
- New automated tests unless explicitly requested.

Planned Spreads downtime is acceptable during the trading lifecycle refactor. The target should avoid backwards-compatible hacks, dual-write paths, and old-runtime fallback layers unless they are deliberately chosen as part of the clean design.

## Local Sources

- [../current_system_state.md](../current_system_state.md)
- [README.md](README.md)
- [../../docker-compose.yml](../../docker-compose.yml)
- [../../pyproject.toml](../../pyproject.toml)
- [../../packages/core/jobs/registry.py](../../packages/core/jobs/registry.py)
- [../../packages/core/events/bus.py](../../packages/core/events/bus.py)
- [../../packages/core/storage/execution_models.py](../../packages/core/storage/execution_models.py)
- [../../packages/core/storage/signal_models.py](../../packages/core/storage/signal_models.py)
- [../../packages/core/services/execution/runtimes.py](../../packages/core/services/execution/runtimes.py)
- [../../packages/core/services/execution/nautilus_bridge.py](../../packages/core/services/execution/nautilus_bridge.py)
- [../../packages/core/services/finviz_direct_trading.py](../../packages/core/services/finviz_direct_trading.py)
- [../../packages/core/services/decision_engine.py](../../packages/core/services/decision_engine.py)
- [../../packages/config/jobs/finviz_direct_trading_finviz_momentum.yaml](../../packages/config/jobs/finviz_direct_trading_finviz_momentum.yaml)
