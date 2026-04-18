# Alpaca Options Automation System Architecture

Status: proposed

As of: Wednesday, April 15, 2026

Related:

- [Alpaca Options Automation Schema](./2026-04-15_alpaca_options_automation_schema.md)
- [Options Alpha Product Research](../research/options_alpha_product_research.md)
- [Alpaca Capabilities Statement](../research/alpaca_capabilities_statement.md)

## Goal

Define the target architecture for our options automation system using Alpaca as both market data provider and broker.

This is intentionally slimmer than a full Options Alpha clone. It assumes current Alpaca constraints and focuses on a buildable first system with:

- stock-first opportunity selection
- option-chain enrichment on shortlisted names
- bot-based automation with clear limits
- CLI-based operator control
- paper and live execution
- narrow backtest and strategy evaluation

Current shipped-surface note:

- `backtest` is the canonical historical-evaluation product.
- References below to `replay` should be read as pre-cutover naming or generic deterministic historical-evaluation semantics unless explicitly updated.

Out of scope for the first version:

- any dedicated web UI or no-code automation builder
- user accounts, auth, permissions, or multi-tenant support
- market-wide option contract surveillance
- broad strategy-family coverage
- full historical options research before February 2024
- institutional-grade order-flow analytics

## Scope Assumptions

The architecture should assume:

- This is a single-operator system for now, not a multi-user product.
- Alpaca is the only live broker and market data source.
- The primary operator surface is a CLI, not a web app.
- We design around OPRA top-of-book data, not depth-of-book.
- We design around Alpaca's option quote subscription limits.
- We use Alpaca multi-leg orders where supported, but we do not assume every roll or combo pattern is cleanly supported.
- We support a narrow strategy set first: verticals and iron condors.
- We target short-dated trades only in `v1`: `1-14` DTE.
- Backtest only promises fidelity over Alpaca's available history window.

## Core Product Primitives

The first system should center on six internal and operator-facing primitives:

1. `Bot`: isolated capital bucket with open-position and daily-action limits.
2. `StrategyConfig`: reusable strategy playbook that defines how one strategy should be built and managed.
3. `Automation`: scheduled or event-driven workflow that runs one `StrategyConfig`.
4. `Recipe`: constrained decision unit evaluated inside a strategy or automation.
5. `Opportunity`: ranked trade candidate after liquidity, quality, and policy checks.
6. `StrategyPosition`: canonical representation for a multi-leg trade and its lifecycle.

This is enough to support an Options Alpha-like operating model without taking on full no-code product scope.

## Strategy Model

Do not model "strategy" as one giant object.

Use this layered model instead:

```text
Strategy -> StrategyConfig -> Automation -> Bot -> StrategyPosition
```

### Strategy

This is the structural trade type implemented in code.

Examples:

- `call_debit_spread`
- `put_credit_spread`
- `iron_condor`

`Strategy` is not an operator-edited object in `v1`. It is a builder implementation boundary in code.

### StrategyConfig

This is the reusable strategy playbook.

It should define:

- which `Strategy` to use
- contract-selection and builder parameters
- liquidity and quote-quality gates
- default risk limits
- default entry and management recipe references

This is the closest thing we should have to a reusable "strategy" in `v1`.

### Automation

This is the runtime wrapper around a `StrategyConfig`.

It should define:

- when to run
- which universe to scan
- whether it is entry, management, or risk focused
- approval mode and execution mode
- any schedule or event trigger constraints

`Automation` should answer when and why the strategy runs, not how the trade is structurally built.

### Bot

The bot is the capital and risk container.

It should define:

- capital budget
- max open positions
- max daily actions
- attached automations
- pause or resume state

### StrategyPosition

This is the runtime instance created when an approved opportunity becomes a live or paper trade.

It is not the strategy definition. It is the position created from a strategy.

### Implementation Boundary

For `v1`:

- `Strategy` lives in code
- `StrategyConfig` lives in checked-in YAML
- `Automation` lives in checked-in YAML
- `Bot` lives in checked-in YAML plus runtime state in Postgres
- `StrategyPosition`, `Opportunity`, `OpportunityDecision`, `ExecutionIntent`, and backtest run records live in Postgres

## Operator Model

For now, treat this as a CLI-first system.

The CLI should be the primary way to:

- define and update bots
- attach or edit automations
- inspect shortlisted symbols and ranked opportunities
- approve, reject, or pause execution
- inspect positions, orders, and decision logs
- run backtest and paper-trading workflows

This keeps the first build focused on runtime correctness, data quality, and automation semantics instead of UI concerns.

## Local Config Model

Use one local operator config root with checked-in strategy definitions and untracked secrets.

Recommended layout:

```text
packages/config/system.yaml
packages/config/strategies/*.yaml
packages/config/automations/*.yaml
packages/config/policies/*.yaml
packages/config/bots/*.yaml
packages/config/universes/*.yaml
.env.local
```

Rules:

- checked-in YAML defines strategy configs, automations, bot specs, policy defaults, watchlists, and scheduling
- `.env.local` holds Alpaca credentials and any local-only overrides
- secrets do not live in checked-in YAML
- every bot, automation run, opportunity, opportunity decision, execution intent, and backtest run records the resolved config hash and policy version
- runtime state, approvals, and logs belong in Postgres, not local files

Config precedence:

1. checked-in defaults
2. local YAML overrides when present
3. environment variables for secrets and machine-local settings

## CLI Surface

Use the existing `uv run spreads ...` entrypoint.

Recommended command families:

- `uv run spreads strategies list`
- `uv run spreads strategies show <strategy-config-id>`
- `uv run spreads strategies apply <path>`
- `uv run spreads bots list`
- `uv run spreads bots show <bot-id>`
- `uv run spreads bots apply <path>`
- `uv run spreads bots pause <bot-id>`
- `uv run spreads bots resume <bot-id>`
- `uv run spreads automations list`
- `uv run spreads automations show <automation-id>`
- `uv run spreads automations apply <path>`
- `uv run spreads automations test <bot-id> <automation-id>`
- `uv run spreads automations run <bot-id> <automation-id>`
- `uv run spreads shortlist`
- `uv run spreads opportunities`
- `uv run spreads approvals list`
- `uv run spreads approvals approve <opportunity-id>`
- `uv run spreads approvals reject <opportunity-id>`
- `uv run spreads positions`
- `uv run spreads orders`
- `uv run spreads logs decisions --bot <bot-id>`
- `uv run spreads backtest run --bot-id <bot-id> --automation-id <automation-id>`
- `uv run spreads backtest compare --left-json <path> --right-json <path>`

Command rules:

- default output should be human-readable terminal output
- every inspection command should support `--json`
- long-running inspection commands should support `--watch`
- commands should call local services directly rather than depending on the API server

## Recommended Macro-Systems

1. `Control Plane`

Owns bot configuration, schedules, approvals, secrets, policy versions, audit logs, and CLI-facing configuration flows.

2. `Market Data Platform`

Owns Alpaca ingestion, symbol master, option contract metadata, normalized snapshots, derived features, and quote-budget management.

3. `Decision Engine`

Executes the recipe DSL against point-in-time data and produces opportunities or position-management actions.

4. `Bot Runtime`

Runs automations on schedules and events, enforces bot-level limits, and records every decision step.

5. `Execution Core`

Owns execution intents, smart pricing, Alpaca order submission, fills, reconciliation, and lifecycle handling for assignment and expiry.

6. `Research Platform`

Owns deterministic backtests, paper trading, and strategy comparison within Alpaca's historical window.

## System View

```text
                 +----------------------+
                 | Control Plane        |
                 | bots, config, ops    |
                 +----------+-----------+
                            |
                            v
 +--------------------------+--------------------------+
 | Bot Runtime                                         |
 | schedules, triggers, limits, decision logs          |
 +--------------------------+--------------------------+
                            |
                            v
 +-------------+    +-------+--------+    +-------------------+
 | Alpaca Data +--->+ Market Data    +--->+ Decision Engine   |
 | stocks/options   | snapshots       |    | recipes + ranking |
 | news/contracts   | features        |    +---------+---------+
 +-------------+    +-------+--------+              |
                            |                       v
                            |              +--------+---------+
                            |              | Execution Core   |
                            |              | mleg, pricing,   |
                            |              | fills, recon     |
                            |              +--------+---------+
                            |                       |
                            v                       v
                  +---------+---------+   +---------+---------+
                  | Research Platform |   | Alpaca Trading    |
                  | backtest, paper,  |   | orders, updates   |
                  | comparison        |   | positions         |
                  +-------------------+   +-------------------+
```

## Canonical Runtime Flow

1. Ingest Alpaca stock, option, news, and contract metadata surfaces.
2. Normalize them into point-in-time underlying and chain snapshots.
3. Run a stock-first shortlist pass using stock snapshots, screeners, bars, and news.
4. Spend option enrichment only on active bot universes and shortlisted names.
5. Trigger bot automations by schedule, position events, or targeted market events.
6. Evaluate recipes and return one of three outcomes: `no_op`, `candidate_trade`, or `position_management_action`.
7. Pass candidate trades through risk, capital, overlap, liquidity, and spread-quality checks.
8. Submit supported single-leg or multi-leg orders to Alpaca.
9. Let smart pricing manage replace cadence, timeout policy, and cancel logic.
10. Reconcile positions from trade updates plus REST polling for assignment, exercise, and expiry edge cases.

## Two-Stage Scanner

The scanner should choose names first and trades second.

This split is required because Alpaca is strong enough for shortlist-based options automation, but not for broad market-wide option surveillance with unlimited live quote coverage.

```text
 broad stock universe
        |
        v
 Stage 1: underlying shortlist
        |
        v
 Stage 2: option-chain enrichment and trade construction
        |
        v
 ranked Opportunity objects
```

### Runtime States

Use three scanner states:

- `cold`: broad stock universe with cheap stock, news, and calendar context
- `warm`: shortlisted names with refreshed option chain snapshots
- `hot`: active contracts needed for open positions, working orders, and top entry candidates

This keeps the system from treating every symbol and every contract as equally important.

### Stage 1: Underlying Shortlist

Stage 1 is broad, cheap, and coarse.

Its job is to rank underlyings, not construct trades.

Inputs:

- stock snapshots
- stock bars
- Alpaca screeners such as movers and most actives
- news and catalyst context
- earnings and calendar context when relevant
- basic optionability and liquidity gates

Outputs:

- shortlist of symbols worth option work
- coarse score per symbol
- reason codes such as `momentum`, `catalyst`, `earnings`, or `liquidity`
- priority tier such as `warm` or `hot_candidate`

Typical filters at this stage:

- unusual price movement
- relative volume expansion
- catalyst or event presence
- basic liquidity and optionability

Stage 1 should tolerate false positives. It is a ranking filter, not the final decision engine.

### Stage 1 Score Model

Use a 100-point shortlist score:

- momentum and price displacement: `0-30`
- relative volume and activity expansion: `0-25`
- catalyst and news context: `0-20`
- options liquidity and optionability: `0-15`
- event timing context such as earnings proximity: `0-10`

Reason codes should be emitted whenever a component contributes materially to the score.

Initial reason-code set:

- `momentum`
- `reversal`
- `volume_expansion`
- `catalyst`
- `earnings`
- `liquid_options`

### Promotion Rules

- promote `cold` to `warm` when score is `>= 50` and the symbol passes basic optionability and liquidity checks
- promote `warm` to `hot` when one of the following is true:
- there is an open position in the name
- there is a working order in the name
- the symbol ranks in the top `5` names for the current cycle and score is `>= 70`
- an explicit event override exists, such as same-day earnings handling
- demote `warm` back to `cold` after two consecutive cycles below `40`
- demote `hot` back to `warm` when there is no open risk, no working order, and the name falls out of the top `10` for three consecutive cycles

### Stage 2: Option Enrichment And Trade Construction

Stage 2 is narrow, expensive, and precise.

Its job is to turn shortlisted names into concrete trade candidates.

Inputs:

- option contract metadata
- option chain snapshots
- Greeks and implied volatility from snapshots
- open interest metadata
- recent option trade context
- targeted live option quotes and trades for the active working set
- bot policy and strategy constraints

Outputs:

- supported trade structures for the symbol
- `Opportunity` objects with score, rationale, and execution parameters
- spread-quality and liquidity checks
- contract set promoted into the `hot` runtime state

Stage 2 should only build supported structures. For the first system that means verticals and iron condors, not a brute-force search across every possible multi-leg combination.

### Quote-Budget Policy

Treat live option quote capacity as a scarce system resource.

Default budget allocation:

- `50%` reserved for open positions
- `20%` reserved for active working orders
- `20%` reserved for top-ranked new opportunities
- `10%` reserved for discovery on lower-priority shortlisted names

Priority order:

1. contracts for open positions
2. contracts for active working orders
3. contracts for top-ranked new opportunities
4. discovery contracts for lower-priority shortlisted names

Operational rules:

- reserve budget for risk management before discovery
- subscribe narrowly and unsubscribe aggressively
- keep `hot` sets small and time-bounded
- degrade gracefully from live quotes to snapshot-only enrichment when budget is tight
- let bot priority and open risk decide eviction order

Default degradation thresholds:

- above `80%` total usage, freeze new discretionary `hot` promotions unless another discovery contract is evicted
- above `90%` total usage, discovery falls back to snapshot-only enrichment
- above `95%` total usage, only open-position and working-order contracts retain live subscriptions

Default eviction order:

1. lowest-priority discovery contracts
2. stale `hot` candidates with no approval activity
3. lowest-ranked `warm` names
4. never evict open-position or active-order contracts unless the position closes or the order terminates

The core design rule is simple: open-risk monitoring always wins over new-idea discovery.

## Key Data Objects

- `UnderlyingSnapshot`
- `OptionContractSnapshot`
- `ChainSnapshot`
- `Opportunity`
- `OpportunityDecision`
- `Bot`
- `AutomationRun`
- `ExecutionIntent`
- `OrderExecution`
- `StrategyPosition`
- `ReplayRun`

The important modeling choice is `StrategyPosition`. Even a slim first version cannot treat a trade as a single contract because spreads and condors must be managed as one lifecycle object.

## Alpaca Fit

### Strong Fit

- Real-time OPRA quotes and trades are sufficient for shortlist-based options automation.
- Option snapshots include Greeks and implied volatility, which is enough for first-pass scoring and rule checks.
- Contract metadata plus open interest supports practical chain filtering.
- Stock, news, and screener endpoints make a stock-first prefilter viable.
- Alpaca now supports live multi-leg options orders for core spread and condor workflows.

### Important Constraints

- No documented L2 or full-depth order book for equities or options.
- No documented historical option quote endpoint.
- Historical options data only goes back to February 2024.
- Real-time option streaming is quotes and trades only, and option quotes are subscription-limited.
- The option stream is msgpack only.
- Assignment, exercise, and expiry-related events require REST polling rather than WebSocket-only handling.
- Current multi-leg restrictions mean some combo and roll patterns need special-case execution logic.

## First Strategy Builders

### Verticals

Support first:

- `call_debit_spread`
- `put_debit_spread`
- `call_credit_spread`
- `put_credit_spread`

Builder rules:

- same underlying and same expiry for both legs
- `1:1` ratio only
- allowed DTE: `1-14`
- allowed width: `1-10` strike points
- both legs require non-zero bid and ask
- quote age must be `<= 30s`
- each leg requires open interest `>= 100`
- each leg spread should be `<= 15%` of leg midpoint
- net spread should be positive and within bot max-risk limits

### Iron Condors

Support first:

- one expiry, four legs, same underlying
- `1:1:1:1` ratio only
- symmetric wings only in `v1`

Builder rules:

- allowed DTE: `1-14`
- short strikes should target absolute delta `0.10-0.30`
- wing width: `1-10` strike points
- all legs require non-zero bid and ask
- quote age must be `<= 30s`
- short legs require open interest `>= 200`
- combined net credit must be positive
- max loss must fit inside bot capital and position limits

Out of scope in `v1`:

- broken-wing condors
- calendar rolls
- diagonals
- uncovered-leg combos

## Data Model

Schema design is split into [Alpaca Options Automation Schema](./2026-04-15_alpaca_options_automation_schema.md).

At the architecture level, the only non-negotiable rule is:

- Postgres is the primary store
- lifecycle history stays append-only
- CLI reads should use current-state materializations rather than rebuilding from raw events every time

## Smart Pricing

Use limit orders only in `v1`.

State machine:

```text
draft -> pending_approval -> ready -> working -> repricing
                                      |           |
                                      v           v
                               partially_filled  cancelled
                                      |
                                      v
                                    filled
```

Default pricing rules:

- start from net midpoint when quotes are fresh
- shade the initial limit `25%` of the spread toward the favorable side for the strategy
- reprice only when quote age is `<= 30s`

Default cadence:

- entries: replace every `20s`, maximum `4` replaces
- exits: replace every `10s`, maximum `6` replaces

Cancel escalation rules:

- cancel when quote age exceeds `45s`
- cancel when spread-quality checks fail for two consecutive pricing cycles
- cancel when max replace count is reached without material progress
- do not fall back to market orders in `v1`; return control to the operator or exit policy

## Paper Trading Model

The paper engine should reuse the same `Opportunity`, `OpportunityDecision`, `ExecutionIntent`, and `StrategyPosition` flows as live trading.

Fill model:

- if the submitted limit crosses the natural quote, fill immediately at the natural price
- if the submitted limit is at or better than the midpoint for two consecutive quote observations, fill at midpoint
- otherwise wait until later quotes make the order marketable or the smart-pricing state machine cancels it

For backtest windows without historical option quotes, use stored snapshots or synthetic midpoint estimates and mark the result as reduced fidelity.

Known differences from live behavior:

- no queue-position modeling
- no exchange-routing behavior
- no hidden liquidity
- multi-leg fills are approximated from net quote state, not a complex-order book
- assignment timing can only be modeled from available broker and contract state, not exchange internals

## Lifecycle Reconciliation

Use trade updates as the primary live signal, with REST polling as the reconciliation layer.

Polling cadence:

- orders and positions: every `30s` during market hours while any position or working order exists
- account activities for option lifecycle events: every `5m` during market hours when options are open
- expiry-focused sweep: every `2m` from `15:30 ET` through `18:30 ET` for positions expiring that day
- opening sweep: once before the next trading session for any prior-day expiring positions

Reconciliation rules:

- any broker/local mismatch creates a reconciliation event and updates the `StrategyPosition` state
- assignment and exercise events always win over inferred local state
- OTM expiry closes the `StrategyPosition` without downstream share exposure
- ITM expiry must check for resulting stock exposure or broker-generated liquidation activity

## Backtest Guarantees

Backtest should be deterministic for the data it actually has and explicit about what it does not.

Guaranteed:

- same dataset refs, config hash, and policy versions produce the same shortlist, builder outputs, and paper-simulated decisions
- simulated paper fills are deterministic under the paper-fill model above

Not guaranteed:

- exact live fill parity
- exact spread-quality parity when historical option quotes were not captured
- exact quote-budget behavior when only snapshot history exists

Fidelity labels:

- `high`: stock-first shortlist logic and builder outputs from stored chain snapshots
- `medium`: opportunity ranking that depends on snapshot freshness and recent trade context
- `reduced`: smart-pricing and fill simulation when historical live option quotes are unavailable

## Recommended First Build

### Phase 1

- isolated bots with capital and position limits
- scheduled automations
- CLI commands for bot and automation management
- stock-first shortlist generation
- targeted option chain enrichment
- strategy set: verticals and iron condors
- internal paper trading
- decision logs and manual approval before live order submission

### Phase 2

- live Alpaca multi-leg execution
- automated exits and smart pricing
- assignment and expiry reconciliation

### Phase 3

- deterministic backtest over Alpaca's historical window
- richer event triggers such as earnings and calendar rules
- template promotion from backtest-tested strategy specs

## Recommendation

Start as a modular monolith with separate workers for ingestion, bot execution, and research jobs.

That is the right shape for this system. The complexity is in deterministic state, quote budgeting, pricing logic, and backtest parity, not in service count. Split into more services only after the event model, bot runtime, and execution semantics are stable.

## Sources

- [Alpaca Real-time Option Data](https://docs.alpaca.markets/docs/real-time-option-data)
- [Alpaca Historical Option Data](https://docs.alpaca.markets/docs/historical-option-data)
- [Alpaca Options Level 3 Trading](https://docs.alpaca.markets/docs/options-level-3-trading)
