# Spreads Current System Architecture

Source of truth: [docs/current_system_state.md](../../current_system_state.md)

This diagram is intentionally plain Markdown + ASCII so it renders cleanly in Codex and terminals.

## System Overview

```text
+================================================================================+
|                               OPERATOR SURFACES                                |
|                                                                                |
|      Browser operator                         spreads CLI                       |
|             |                                      |                            |
+=============|======================================|============================+
              |                                      |
              v                                      v
+----------------------------+          +----------------------------------------+
| WEB / API ADAPTERS         |          | CLI ADAPTERS                           |
|                            |          |                                        |
|  Next.js web               |          |  ops state / ops storage               |
|        |                   |          |  jobs / positions / execution          |
|        v                   |          |  deploy / lifecycle / config           |
|  FastAPI API               |          |                                        |
+-------------+--------------+          +--------------------+-------------------+
              |                                              |
              v                                              v
+================================================================================+
|                              CANONICAL READ MODELS                              |
|                                                                                |
|   TradingOpsState     StorageOpsState     Positions     Jobs     Runtimes       |
|                                                                                |
+======================================+=========================================+
                                       |
                                       v
+================================================================================+
|                              CORE DOMAIN SERVICES                               |
|                                                                                |
|  StrategyRuntime      DataEngine              EntrySelectionEngine              |
|  PortfolioEngine      RiskEngine              ExecutionIntents                  |
|  ExecutionServices    BrokerSync              CaptureTargets                    |
|  TickerSources        CandidateBuilders       Alerts                            |
|                                                                                |
+===============================+==============================+=================+
                                |                              |
                                v                              v
+-------------------------------+------------+   +-------------------------------+
| PERSISTENCE + TRANSPORT                    |   | EXTERNAL SYSTEMS              |
|                                            |   |                               |
|  Postgres                                  |   |  Alpaca trading API           |
|    facts, jobs, candidates, decisions,     |   |  Alpaca market data           |
|    admissions, intents, attempts, fills,   |   |  Finviz screener              |
|    positions, capture state, ops state     |   |  alert webhooks/operators     |
|                                            |   |  external/TradingAgents       |
|  Redis                                     |   |                               |
|    queues, leases, pub/sub                 |   |                               |
+--------------------------------------------+   +-------------------------------+
```

## Runtime Containers

```text
Live target: ade-nucbox-k8-plus

+--------------------------------------------------------------------------------+
| Docker Compose                                                                  |
|                                                                                |
|  +--------------+     +--------------+     +--------------------------------+   |
|  | web          | --> | api          | --> | Postgres / Redis reads         |   |
|  | Next.js      |     | FastAPI      |     | via service-owned read models  |   |
|  +--------------+     +--------------+     +--------------------------------+   |
|                                                                                |
|  +--------------+     +--------------+     +--------------------------------+   |
|  | scheduler    | --> | Redis queues | --> | runtime workers                |   |
|  | job planner  |     | leases       |     | entry/manage, dispatch, alerts |   |
|  +--------------+     +--------------+     +--------------------------------+   |
|                                  |                                             |
|                                  +-------> data worker                         |
|                                  |          ticker sources                     |
|                                  |                                             |
|                                  +-------> valuation worker                    |
|                                  |          optional, disabled by default      |
|                                  |                                             |
|                                  +-------> research worker                     |
|                                             optional, disabled by default      |
|                                                                                |
|  +------------------------+       +-----------------------------------------+   |
|  | market recorder        | ----> | option quote/trade ticks + summaries   |   |
|  | Alpaca option stream   |       | stored in Postgres                     |   |
|  +------------------------+       +-----------------------------------------+   |
|                                                                                |
+--------------------------------------------------------------------------------+
```

## Trading Lifecycle Spine

```text
Authored strategy config
  packages/config/trading_strategies
            |
            v
+-----------------------+
| StrategyRuntime       |
| strategy run owner    |
+-----------+-----------+
            |
            v
+-----------------------+       +-----------------------+
| DataEngine            | ----> | TickerSources         |
| source resolution     |       | static / dynamic      |
+-----------+-----------+       +-----------+-----------+
            |                               |
            v                               v
+-----------------------+       +-----------------------+
| CandidateBuilders     | <---- | MarketSliceProvider   |
| option structures     |       | Alpaca-backed live    |
+-----------+-----------+       +-----------------------+
            |
            v
+-----------------------+
| EntrySelectionEngine  |
| quality + selection   |
+-----------+-----------+
            |
            v
+-----------------------+
| TradeDecision         |
| selected / no-entry   |
+-----------+-----------+
            |
            v
+-----------------------+
| PortfolioAdmission    |
| account-aware gate    |
+-----------+-----------+
            |
            v
+-----------------------+
| ExecutionIntent       |
| control-plane handoff |
+-----------+-----------+
            |
            v
+-----------------------+       +-----------------------+
| execution_intent_     | ----> | execution_submit      |
| dispatch:global       |       | alpaca_direct         |
+-----------+-----------+       +-----------+-----------+
            |                               |
            v                               v
+-----------------------+       +-----------------------+
| Attempts / Orders /   | <---- | Alpaca broker         |
| Fills                 |       | paper/live API        |
+-----------+-----------+       +-----------+-----------+
            |                               |
            v                               v
+-----------------------+       +-----------------------+
| BrokerSync            | ----> | SessionPositions      |
| account reconciliation|       | owned position state  |
+-----------------------+       +-----------------------+
```

## Market Data And Capture

```text
        selected candidates
              |
        watch candidates
              |
        working intents / attempts
              |
        open positions
              |
              v
+-----------------------+
| CaptureTargets        |
| desired stream state  |
| priority + TTL        |
+-----------+-----------+
            |
            v
+-----------------------+       +-----------------------+
| MarketRecorder        | <---- | Alpaca option         |
| one websocket owner   |       | websocket             |
+-----------+-----------+       +-----------------------+
            |
            v
+-----------------------+
| Postgres              |
| option_quote_ticks    |
| option_trade_ticks    |
| capture_summaries     |
+-----------------------+
```

## Optional Offline Lanes

```text
These lanes are not part of default live trading health.

+------------------------+      +------------------------+      +----------------+
| valuation worker       | ---> | company valuation      | ---> | Postgres       |
| disabled by default    |      | issuer research data   |      | offline facts  |
+------------------------+      +------------------------+      +----------------+

+------------------------+      +------------------------+      +----------------+
| research worker        | ---> | TradingAgents scan     | ---> | outputs/logs   |
| disabled by default    |      | Spreads orchestration  |      | artifacts      |
+------------------------+      +------------------------+      +----------------+
                                      |
                                      v
                              +-------------------+
                              | external repo     |
                              | TradingAgents     |
                              +-------------------+
```

## Strategy Activation Contract

```text
authored
   |
   | visible in strategy breadth
   v
observation
   |
   | manual observe-strategy only
   | no scheduler jobs
   | no broker submission
   v
paper_ready
   |
   | quality profile exists
   | canonical execution_shape.legs[] exists
   | portfolio admission coverage exists
   v
paper_active
   |
   | strategy enabled
   | scheduler entry/manage jobs active
   | paper execution contract compatible
   v
live_active
   |
   | reserved for explicit live-money rollout
   | same lifecycle plus live deployment guards
```

Required non-long-call gate order:

```text
quality profile
  -> account-agnostic selection
  -> portfolio admission
  -> execution admission
  -> queued broker submission
```

## Ownership Rules

```text
Strategy config owns:
  strategy id, source, trade structure, schedules, risk limits, execution posture

Selection owns:
  account-agnostic quality, candidate filtering, selected/monitored/rejected output

Admission owns:
  whether this account and portfolio may take the selected idea now

Execution owns:
  broker submission, refresh, cancel, attempts, orders, fills

Session positions own:
  Spreads day/session-local position attribution and projected PnL

TradingOpsState owns:
  operator-visible health and lifecycle evidence
```
