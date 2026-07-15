# Workflow Runtime Cutover

Tracker: `spr-07a`

Status: implemented and rolled out on `ade-nucbox-k8-plus` on 2026-07-15

## Decision

Spreads owns a workflow runtime. Temporal is the current provider behind that
runtime, not a product or domain boundary.

The durable model is:

```text
routine definitions
  -> routine schedules
  -> workflow lanes
  -> workflow/activity execution
  -> Postgres job and lifecycle facts
  -> TradingOpsState / JobsState projections
```

Lifecycle workflows remain distinct from scheduled job workflows. Postgres
remains the business source of truth, ClickHouse remains the market-data store,
and NATS remains engine-event transport. Workflow history is orchestration
evidence, not a second trading ledger.

Provider-specific names may exist only inside the workflow provider adapter and
provider connection settings. Active CLIs, Compose service names, deploy target
fields, operator payloads, docs, and skills use the domain vocabulary below.

## Domain Vocabulary

| Domain term | Meaning |
| --- | --- |
| Workflow runtime | The complete orchestration boundary for scheduled, ad-hoc, lifecycle, maintenance, and supervised capture work. |
| Routine | A declared recurring or manually started unit of work. |
| Routine schedule | Calendar-aware firing policy for a routine. |
| Workflow lane | A worker capacity and ownership boundary such as `lifecycle`, `runtime`, `data`, `maintenance`, `valuation`, or `research`. |
| Lifecycle workflow | Durable open or close orchestration for one execution intent. |
| Capture session | Durable supervised ownership and heartbeat for market capture. |
| Maintenance routine | Backup, health snapshot, or log-retention work run by the workflow runtime. |
| Provider adapter | Temporal-specific client, schedule rendering, worker registration, and diagnostics. |

## Runtime Containers

| Container | Ownership | Required posture |
| --- | --- | --- |
| `workflow-lifecycle` | Open and close lifecycle workflows and broker activities. | Required for paper/live execution. |
| `workflow-runtime` | Broker sync, strategy routines, alert work, lifecycle starts, and engine outbox publishing. | Required for trading. |
| `workflow-data` | Ticker sources and calendar refresh. | Required for trading. |
| `workflow-maintenance` | Backup, health snapshot, and log retention. | Required for managed deployments. |
| `workflow-valuation` | Optional company-valuation workflows. | Explicitly enabled or disabled. |
| `workflow-research` | Optional research workflows. | Explicitly enabled or disabled. |
| `routine-schedules` | Reconciles declared routine schedules and records reconciliation evidence. | Required during deploy/startup; periodic reconciliation is then owned by the maintenance lane. |
| `capture-worker` | Runs market capture as a workflow-supervised capture session. | Required only on the configured capture-owner deployment. |

## Ownership Inventory

| Current path | Target classification | Target owner / action |
| --- | --- | --- |
| Static job YAML and generated strategy/ticker-source jobs | Workflow-owned | Rename job scheduling concepts to routine definitions and render first-class routine schedules. |
| `packages/core/jobs/temporal_schedules.py` | Provider adapter | Replace public module/CLI name with routine schedule reconciliation; isolate provider rendering internally. |
| `packages/core/workflows/worker.py` | Provider adapter | Replace public CLI arguments with workflow lane selection; provider task queues stay internal diagnostics. |
| Trade and close workflows | Workflow-owned lifecycle | Keep deterministic IDs and provider implementation; remove direct broker mutation entrypoints outside activities. |
| Broker sync, strategy entry/manage, alert reconcile/delivery, lifecycle start, outbox publish | Workflow-owned runtime routines | Run only through the runtime lane. |
| Ticker-source and calendar refresh | Workflow-owned data routines | Run only through the data lane. |
| Company valuation and TradingAgents research | Optional workflow lanes | Enable/disable by lane policy, not raw excluded job-type lists. |
| `ops/backup_postgres.sh` | Maintenance routine implementation | Invoked by the maintenance workflow lane; host cron ownership deleted. |
| `ops/health_check.sh` | Maintenance routine implementation | Replaced by a service-owned health snapshot activity; host cron ownership deleted. |
| `ops/rotate_ops_logs.sh` | Maintenance routine implementation | Invoked by the maintenance workflow lane; host cron ownership deleted. |
| `ops/trading_ops_monitor.sh` | Stale-to-delete | TradingOpsState is already the canonical health projection; routine health snapshots replace the monitor. |
| `ops/compose_up.sh` and systemd Compose unit | Machine bootstrap only | Starts/keeps the stack alive; owns no recurring domain work. |
| `market-recorder` standalone service | Workflow-supervised | Rename to capture worker, start a durable capture-supervisor workflow, record session/heartbeat evidence, and keep one websocket owner. |
| Ad-hoc valuation, alert delivery, and lifecycle-start helpers | Workflow-owned starts | Start workflow first with deterministic ID; workflow activity creates/claims the job-run projection. |
| Jobs/TradingOpsState schedule and task-queue projections | Operator read model | Report routine schedules, workflow lanes, live pollers, reconciliation freshness, due-work gaps, and lifecycle progress. |
| Host crontab installation | Stale-to-delete | Remove installer and recurring cron block; systemd/Compose remains machine bootstrap only. |

## Routine Schedule Contract

Routine definitions use one of these schedule kinds:

- `interval`: fixed cadence, optionally all-hours.
- `market_session`: fixed cadence within an explicit market-open/close window.
- `market_open`: one firing at an offset from market open.
- `market_close`: one firing at an offset from market close.
- `calendar`: ordinary calendar/cron schedule in an explicit timezone.
- `manual`: no reconciled schedule; starts only through the workflow launcher.

The provider adapter must render those semantics directly. It must not collapse
market-open, market-close, or market-session routines into a one-minute schedule
that usually skips inside job code. Job code may retain safety gates, but the
schedule owns the expected firing slot.

## Health Contract

Workflow runtime health is based on observed evidence:

1. Required lane policy for the deployment.
2. A live provider poller for each enabled required lane.
3. Successful routine-schedule reconciliation with current config hash.
4. Expected next/previous schedule slots.
5. Current-day job activity when a routine was due.
6. Lifecycle workflow progress for active intents.
7. Capture-session heartbeat during its expected posture.

Declared config alone can never produce `healthy`. Optional disabled lanes are
`disabled`, not blocked. Enabled optional lanes without a poller are blocked.

## Deploy Contract

`spreads deploy up` must:

1. Start required infrastructure and workflow lanes at configured replica counts.
2. Run schema/runtime initialization.
3. Reconcile routine schedules and fail if reconciliation fails.
4. Verify required provider pollers and capture-worker posture.
5. Verify JobsState no longer reports a missing required lane or stale reconciliation.

Rollback is the previous container image/checkout plus the same deploy command.
There is no compatibility CLI or duplicate scheduler path.

## Cut Order

1. `spr-07a.1`: land this target model and inventory.
2. `spr-07a.2` and `.4`: cut public naming and schedule semantics together.
3. `spr-07a.3` and `.10`: enforce deploy/runtime evidence and truthful ops health.
4. `spr-07a.5`, `.6`, `.7`, and `.9`: move maintenance, capture, ad-hoc work, and optional lanes under the model.
5. `spr-07a.8`: remove remaining lifecycle mutation bypasses and prove exclusive ownership.
6. Update current-state docs and active skills, roll out, and close the epic with live evidence.

## Hard Gates

- No ARQ, host cron, direct queue-row orchestration, or fallback scheduler path.
- No provider-shaped public command, service, environment, or ops payload names.
- No healthy status based only on declarations.
- No lifecycle broker mutation outside workflow activities.
- No second market-capture owner.
- No optional lane presented as a live-trading blocker when disabled.

## Rollout Evidence

- The canonical `spreads deploy up --env ade-nucbox-k8-plus --no-sync --no-build`
  path completed successfully, including routine reconciliation and deploy
  verification.
- All required lanes have live pollers: `lifecycle`, `runtime` (two replicas),
  `data`, `maintenance`, and `capture`. Optional `valuation` and `research`
  lanes are explicitly disabled and are not trading blockers.
- All 29 enabled routine definitions are reconciled and the observed config
  hash matches the declared config hash.
- The live trading projection is healthy with zero blocked workflow lanes and
  no missing due routines. The storage projection is healthy and capture is
  writing current-session quote rows.
- Backup, health-snapshot, log-retention, calendar-refresh, ticker-source,
  strategy, broker-sync, outbox, alert, lifecycle-start, and capture work have
  current-day workflow-owned execution evidence.
- Host cron ownership and the displaced scheduler, worker, recorder, and direct
  execution-mutation paths were removed rather than retained as compatibility
  layers.
