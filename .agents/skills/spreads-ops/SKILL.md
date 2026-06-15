---
name: spreads-ops
description: Live and post-market operations workflow for Spreads. Use for system health, market-open readiness, daily performance, blocked or degraded trading, capture or alert issues, worker or scheduler status, and runtime-vs-strategy diagnosis.
---

# Spreads Ops

Use this skill from `/home/ade/Projects/spreads` when the user asks:

- "how is the system doing?"
- "how did we do today?"
- "what broke?"
- "why is trading blocked?"
- "why were alerts missing?"
- "is this runtime, data, policy, or strategy?"

For architecture and ownership, read [docs/current_system_state.md](../../../docs/current_system_state.md). This skill is a workflow playbook, not the domain ownership map.

## Operating Defaults

- Start from the running system, not code inspection.
- Prefer shipped CLI and Docker checks before direct SQL or source reads.
- Use local commands directly when already on `ade-nucbox-k8-plus` in this repo.
- From another host, use deploy-owned commands such as `uv run spreads deploy exec --env ade-nucbox-k8-plus -- ops state --json`.
- Do not use command-level `--env` passthrough on non-deploy commands.
- Do not add or update tests unless Ade explicitly asks.
- Do not revive removed `scan`, `audit`, `backtest`, `research`, `replay`, `analyze`, `post-market analyze`, `doctor`, or fragmented old ops commands.

`TradingOpsState` and `StorageOpsState` are the canonical operator read models.

## First Read

Run the current state surfaces first:

```bash
docker compose ps
uv run spreads ops state --json
uv run spreads ops storage --json
uv run spreads jobs --json
uv run spreads jobs lanes
```

For a market date:

```bash
uv run spreads ops strategy-ledger --date YYYY-MM-DD --json
uv run spreads execution list --date YYYY-MM-DD --json
uv run spreads execution positions --date YYYY-MM-DD --json
```

Use logs only after the state surfaces point at a lane:

```bash
docker compose logs --tail=200 scheduler worker-runtime worker-data market-recorder api
```

## Interpretation

Read operator-health fields before raw historical job counts:

- `operator_status`
- `operator_status_counts`
- `actionable_failed_count`
- `summary.trading_allowed`
- `details.engine.summary.capture_status`
- `details.primary_trading_flow.source_state`
- `details.primary_trading_flow.candidate_state`
- `details.primary_trading_flow.intent_state`
- `details.primary_trading_flow.position_state`
- `broker_sync.status`
- `execution_admission.status`
- `execution_admission.reason`

Use these splits:

- `trading_allowed=false` before market open is expected.
- Market-closed `market_recorder_idle` logs are expected off-hours behavior.
- `capture_status=healthy` plus blocked trading points at policy, account, broker sync, control, or execution admission, not the recorder.
- `capture_status=empty`, `baseline_only`, or `recovery_only` is a data capture problem.
- stale source or candidate state during market hours points at ticker source, data worker, scheduler, or strategy-entry orchestration.
- healthy source and candidate state with no selected decisions is usually strategy output, not a runtime outage.
- For strategy-output diagnosis, use `spreads ops strategy-ledger --date YYYY-MM-DD --json` and inspect `candidates.candidate_productivity_state`, `diagnostic_status_counts`, and raw/postprocess/runtime/returned counts before calling a strategy barren.
- selected decisions without active or filled intents point at admission, dispatch, or broker submission.
- stale open executions or unknown submit status point at execution lifecycle reconciliation.
- historical failed jobs are not live blockers when canonical state recovered and `actionable_failed_count=0`.

## Daily Or Post-Market

Always use an exact market date. Separate actual account outcome from modeled strategy output.

Check:

- account PnL, open positions, closes, and broker sync
- strategy ledger totals by `trading_strategy_id`
- selected versus rejected decisions and top blockers
- intents, attempts, orders, fills, and same-day closes
- storage and capture health
- actionable job failures and disabled lanes

## Alert Triage

Treat Discord delivery as a job-backed outbox.

For missing alerts:

1. confirm the planner created a delivery row in `alert_events`
2. inspect delivery status
3. verify `alert_delivery` jobs are running or retrying
4. verify `alert_reconcile` is seeded and healthy

Classify:

- missing row: planner or upstream selection issue
- `pending` or stale `dispatching`: orchestration or worker issue
- `retry_wait` or `dead_letter`: delivery failure
- `suppressed`: webhook/configuration issue

## Boundaries

- Strategy tuning, catalog/profile changes, and why a strategy selected or skipped belong in `spreads-strategy-lab`.
- ClickHouse/Postgres/Redis size, retention, capture pressure, and schema/rollup design belong in `spreads-data-platform`.
- Applying backend changes to the running Docker stack belongs in `spreads-live-rollout`.
- Architecture source-of-truth and repo guidance updates belong in `spreads-architecture-docs`.

## Response Shape

Report:

1. exact date, timestamp, and market state
2. concise status: healthy, degraded, blocked, or post-close
3. actual account result when relevant
4. runtime, capture, recovery, risk, and execution-admission status
5. affected strategy IDs or runtime lanes
6. evidence and commands used
7. next action, with the issue labeled as runtime, capture, recovery, policy, delivery, strategy, or mixed
