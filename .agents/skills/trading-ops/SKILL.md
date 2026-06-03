---
name: trading-ops
description: Live operations workflow for Ade's spreads trading system. Use when checking live paper trading health, market-open readiness, Finviz direct trading behavior, worker or scheduler status, rollout verification, or "how is the system doing?" questions in /home/ade/Projects/spreads.
---

# Trading Ops

Use this skill from `/home/ade/Projects/spreads`.

## Defaults

- This skill is owned by the Spreads repo under `.agents/skills/trading-ops`. Do not add new active trading-ops guidance to the retired `trading_operator` wrapper repo.
- Start with live system state, not code inspection.
- Prefer shipped CLIs and Docker service checks.
- Use `uv run spreads ...` commands; target the NUC with `--env ade-nucbox-k8-plus` when the question is about the live paper deployment.
- Do not add or update tests unless Ade explicitly asks. Report live checks and remaining runtime risk instead.
- Treat Finviz long-call direct trading as the current active paper flow. Its orders submit through `alpaca_direct`, which is the only active Spreads execution runtime.
- Nautilus host services and support containers are sunset for live operations. They should remain stopped/disabled unless Ade explicitly asks to re-enable Nautilus as a separate experiment.
- TradingAgents is the external research AI layer linked from Spreads at `external/TradingAgents`. Spreads owns the orchestration, job config, outputs, alerts, and operator visibility around that layer.

## Quick Check

Run the compact surface first:

```bash
uv run spreads live-doctor --env ade-nucbox-k8-plus
```

Use JSON when exact fields matter:

```bash
uv run spreads live-doctor --env ade-nucbox-k8-plus --json
```

If `live-doctor` is not available in an older checkout, use:

```bash
uv run spreads status --env ade-nucbox-k8-plus --json
uv run spreads trading --env ade-nucbox-k8-plus --json
uv run spreads finviz-ledger --env ade-nucbox-k8-plus --json --limit 5
uv run spreads jobs lanes --env ade-nucbox-k8-plus --json
uv run spreads jobs --env ade-nucbox-k8-plus --status failed --limit 10 --json
```

## Finviz Direct Flow

Check these signals:

- Feed freshness: latest `symbol_feed:finviz_momentum` run succeeded recently and retained candidates.
- Direct trading: latest `finviz_direct_trading:finviz_momentum` run succeeded and saw candidates.
- Decisions: skips have clear reasons, or one or more intents are created only inside current caps.
- Positions: session positions reconcile as `matched`, broker-sync is fresh, and open position count is within configured caps.
- Exits: closed positions show a concrete `last_exit_reason` such as `profit_target`, `stop_loss`, or force-close policy.

Use:

```bash
uv run spreads finviz-ledger --env ade-nucbox-k8-plus --limit 10
uv run spreads jobs --env ade-nucbox-k8-plus --job-type symbol_feed --limit 3
uv run spreads jobs --env ade-nucbox-k8-plus --job-type finviz_direct_trading --limit 3
```

## Rollout Rhythm

After changing job config, scheduler code, worker-imported code, or trading policy:

1. Run narrow validation:

```bash
uv run spreads config validate --json
uv run python -m py_compile <touched-python-files>
```

2. Restart only affected Docker services:

```bash
docker compose restart scheduler worker-runtime worker-discovery
```

3. Verify that the stored config and the next actual job run carry the new payload:

```bash
uv run spreads jobs --env ade-nucbox-k8-plus --job-type symbol_feed --limit 3 --json
uv run spreads jobs --env ade-nucbox-k8-plus --job-type finviz_direct_trading --limit 3 --json
uv run spreads live-doctor --env ade-nucbox-k8-plus
```

## Interpretation

- A skipped run can be healthy when it is a singleton, off-window, superseded, or stale-slot skip.
- Historical failed jobs matter only if operator status still says actionable.
- A disabled lane should be idle, not blocked.
- `trading_allowed=false` before market open is expected; during market hours it should be explained by market, control, broker-sync, account, execution, or risk state.
- Separate realized account performance from model, discovery, or audit output.

## Close-Out

Report:

- exact timestamp and market state
- concise health summary
- Finviz feed/direct status
- open positions, active intents, broker-sync, and PnL if relevant
- worker/scheduler state and any disabled lanes
- commands used and anything not verified
