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
- Use `uv run spreads ...` commands; target the NUC with `--env ade-nucbox-k8-plus` when the question is about the live paper deployment.
- Do not add or update tests unless Ade explicitly asks. Report live checks and remaining runtime risk instead.
- Treat `momentum_long_calls` as the current active paper flow. It sources tickers dynamically, trades option calls through `alpaca_direct`, and reports through `TradingOpsState`.
- Nautilus host services and support containers are sunset for live operations. They should remain stopped/disabled unless Ade explicitly asks to re-enable Nautilus as a separate experiment.
- TradingAgents is the external research AI layer linked from Spreads at `external/TradingAgents`. Spreads owns the orchestration, job config, outputs, alerts, and operator visibility around that layer.

## Quick Check

Run the canonical trading state first:

```bash
uv run spreads status --env ade-nucbox-k8-plus
```

Use JSON when exact fields matter:

```bash
uv run spreads trading --env ade-nucbox-k8-plus --json
uv run spreads storage --env ade-nucbox-k8-plus --json
uv run spreads jobs --env ade-nucbox-k8-plus --json
uv run spreads jobs --env ade-nucbox-k8-plus --status failed --limit 10 --json
```

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
uv run spreads jobs --env ade-nucbox-k8-plus --job-type ticker_source --limit 5 --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type trading_strategy_entry --limit 5 --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type trading_strategy_manage --limit 5 --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type execution_intent_dispatch --limit 5 --json
```

## Market-Open Validation

After deploying runtime, scheduler, worker, candidate-builder, ticker-source, or execution changes, validate the first live window from the NUC target:

```bash
uv run spreads status --env ade-nucbox-k8-plus
uv run spreads trading --env ade-nucbox-k8-plus --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type ticker_source --limit 5 --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type trading_strategy_entry --limit 5 --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type execution_intent_dispatch --limit 5 --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type trading_strategy_manage --limit 5 --json
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

2. For the live target, deploy through the shipped deploy CLI:

```bash
uv run spreads deploy up --env ade-nucbox-k8-plus --build
uv run spreads deploy restart --env ade-nucbox-k8-plus scheduler worker-runtime worker-data
```

For a purely local Docker stack, restart only affected services:

```bash
docker compose restart scheduler worker-runtime worker-data
```

3. Verify the deployed state and recent job runs:

```bash
uv run spreads status --env ade-nucbox-k8-plus
uv run spreads jobs --env ade-nucbox-k8-plus --job-type ticker_source --limit 5 --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type trading_strategy_entry --limit 5 --json
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
- open positions, active intents, broker sync, and PnL if relevant
- worker/scheduler state and disabled lanes
- commands used and anything not verified
