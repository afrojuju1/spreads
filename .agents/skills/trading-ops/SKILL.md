---
name: trading-ops
description: Live operations workflow for Ade's spreads trading system. Use when checking live paper trading health, market-open readiness, momentum-calls behavior, worker or scheduler status, rollout verification, or "how is the system doing?" questions in /home/ade/Projects/spreads.
---

# Trading Ops

Use this skill from `/home/ade/Projects/spreads`.

## Defaults

- This skill is owned by the Spreads repo under `.agents/skills/trading-ops`. Do not add new active trading-ops guidance to the retired `trading_operator` wrapper repo.
- Start with live system state, not code inspection.
- Prefer shipped CLIs and Docker service checks.
- Use `uv run spreads ...` commands. When already on `ade-nucbox-k8-plus` in `/home/ade/Projects/spreads`, run local CLI and Docker commands directly. From another host, use `uv run spreads deploy exec --env ade-nucbox-k8-plus -- ...` for operator reads; command-level `--env` passthrough on non-deploy commands is intentionally not shipped.
- Do not add or update tests unless Ade explicitly asks. Report live checks and remaining runtime risk instead.
- Treat `momentum_long_calls` as the current active paper flow. It sources tickers dynamically, trades option calls through `alpaca_direct`, and reports through `TradingOpsState`.
- Nautilus host services and support containers are sunset for live operations. They should remain stopped/disabled unless Ade explicitly asks to re-enable Nautilus as a separate experiment.
- TradingAgents is the external research AI layer linked from Spreads at `external/TradingAgents`. Spreads owns the orchestration, job config, outputs, alerts, and operator visibility around that layer.

## Quick Check

On the live box, run the canonical trading state first:

```bash
uv run spreads ops state
```

Use JSON when exact fields matter:

```bash
uv run spreads ops state --json
uv run spreads ops strategy-ledger --date <YYYY-MM-DD> --json
uv run spreads execution list --date <YYYY-MM-DD>
uv run spreads execution list --date <YYYY-MM-DD> --json
uv run spreads ops storage --json
uv run spreads jobs --json
uv run spreads jobs --status failed --limit 10 --json
```

From another host, add the deploy target:

```bash
uv run spreads deploy exec --env ade-nucbox-k8-plus -- ops state --json
uv run spreads deploy exec --env ade-nucbox-k8-plus -- execution list --date <YYYY-MM-DD>
```

## Strategy Evidence Ledger

Use `spreads ops strategy-ledger` before tuning or comparing strategy profiles. It reports every active `trading_strategy_id` for one market date with trade structure, config hash, source/candidate/signal/decision/admission/intent/attempt/position counts, top blockers, PnL, marks, and latest lifecycle IDs.

The target archetype/profile model is represented by transitional sidecar config:

- `packages/config/strategy_profiles/paper_profiles.yaml`
- `packages/config/strategy_specs/paper_strategies.yaml`

These files are not scheduler-loaded yet. Treat them as the migration map for reducing repeated strategy YAML into reusable profiles, and preserve runtime behavior unless ledger evidence justifies a deliberate profile change.

## Execution Activity

Use `spreads execution list --date <YYYY-MM-DD>` for the day-level attempts/orders/fills printout. It includes attempts whose `market_date` matches the date or whose `requested_at` falls inside that UTC activity day, which matches the daily strategy ledger counts and captures same-day closes for prior-day positions. Use `--json` for exact counts and `--trading-strategy-id <id>` to narrow the printout to one strategy.

Use `spreads execution inspect <execution_attempt_id>` when one attempt needs full broker/order/fill detail or a refresh/cancel decision.

## Momentum Calls Flow

Read these fields from `TradingOpsState`:

- `details.primary_trading_flow.source_state`: source freshness, symbol count, and latest source run.
- `details.primary_trading_flow.candidate_state`: candidate run freshness and candidate count.
- `details.primary_trading_flow.intent_state`: active intent count and intent states.
- `details.primary_trading_flow.position_state`: open/closed positions and latest exit reason.
- `summary.trading_allowed`: market, control, broker sync, account, and execution gate result.
- `details.engine.summary`: source runs, candidate runs, candidates, signals, decisions, selected decisions, intents, positions, and capture targets.

Check that:

- Ticker source freshness is healthy during the market window.
- Candidate runs are current and have expected counts.
- Decisions either skip with clear reasons or create only allowed intents.
- Positions reconcile as `matched`, broker sync is fresh, and open position count is within configured caps.
- Exits show concrete `last_exit_reason` values such as `profit_target`, `stop_loss`, or force-close policy.

Useful job checks:

```bash
uv run spreads jobs --job-type ticker_source --limit 5 --json
uv run spreads jobs --job-type trading_strategy_entry --limit 5 --json
uv run spreads jobs --job-type trading_strategy_manage --limit 5 --json
uv run spreads jobs --job-type execution_intent_dispatch --limit 5 --json
```

## Market-Open Validation

After deploying runtime, scheduler, worker, candidate-builder, ticker-source, or execution changes, validate the first live window from the live box:

```bash
uv run spreads ops state
uv run spreads ops state --json
uv run spreads execution list --date <YYYY-MM-DD>
uv run spreads jobs --job-type ticker_source --limit 5 --json
uv run spreads jobs --job-type trading_strategy_entry --limit 5 --json
uv run spreads jobs --job-type execution_intent_dispatch --limit 5 --json
uv run spreads jobs --job-type trading_strategy_manage --limit 5 --json
```

Confirm:

- `ticker_source:finviz_momentum` refreshes during the market window.
- `momentum_long_calls` builds a current candidate run from the resolved ticker source.
- candidate diagnostics explain zero-candidate cases with concrete rejection counts.
- trade decisions either skip with clear reasons or select only allowed entries.
- selected decisions create at most the configured allowed intents.
- `alpaca_direct` dispatch records attempts, orders, fills, and broker sync facts.
- open positions reconcile to broker state and close paths use the normal intent/attempt/fill flow.

## Rollout Rhythm

After changing job config, scheduler code, worker-imported code, or trading policy:

1. Run narrow validation:

```bash
uv run spreads config validate --json
uv run python -m py_compile <touched-python-files>
```

2. From another host, deploy to the live target through the shipped deploy CLI:

```bash
uv run spreads deploy up --env ade-nucbox-k8-plus --build
uv run spreads deploy restart --env ade-nucbox-k8-plus scheduler worker-runtime worker-data
```

On the live box, or for a purely local Docker stack, restart only affected services:

```bash
docker compose restart scheduler worker-runtime worker-data
```

3. Verify the deployed state and recent job runs from the live box:

```bash
uv run spreads ops state
uv run spreads ops strategy-ledger --date <YYYY-MM-DD> --json
uv run spreads execution list --date <YYYY-MM-DD>
uv run spreads jobs --job-type ticker_source --limit 5 --json
uv run spreads jobs --job-type trading_strategy_entry --limit 5 --json
```

## Interpretation

- A skipped run can be healthy when it is a singleton, off-window, superseded, or stale-slot skip.
- Historical failed jobs matter only if operator state or jobs health says they are actionable.
- A disabled lane should be idle, not blocked.
- Closed-market `market_recorder_idle` logs are expected resource-policy behavior, not a capture outage.
- `trading_allowed=false` before market open is expected; during market hours it should be explained by market, control, broker-sync, account, execution, or risk state.
- Separate realized account performance from model, source, or decision output.

## Close-Out

Report:

- exact timestamp and market state
- concise health summary
- source/candidate/decision status for `momentum_long_calls`
- daily strategy ledger totals when strategy tuning, breadth, or profile quality is relevant
- open positions, active intents, broker sync, and PnL if relevant
- execution activity totals when attempts, orders, fills, or same-day closes are relevant
- worker/scheduler state and disabled lanes
- commands used and anything not verified
