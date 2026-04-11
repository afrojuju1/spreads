# Ops CLI Visibility Plan

Status: implemented for current scope; phase 1, phase 2, phase 3, and phase 4 audit shipped, doctor deferred

Related:

- [Database Setup](/Users/adeb/Projects/spreads/docs/database.md)
- [0DTE System Architecture](/Users/adeb/Projects/spreads/docs/planning/0dte_system_architecture.md)
- [Unusual Activity Scanner Design](/Users/adeb/Projects/spreads/docs/planning/unusual_activity_scanner_design.md)
- [Alert Delivery Refactor Plan](/Users/adeb/Projects/spreads/docs/planning/alert_delivery_refactor_plan.md)

## Shipped Scope

As of April 11, 2026, the operator visibility CLI ships:

- `spreads status`
- `spreads trading`
- `spreads sessions`
- `spreads sessions <session-id>`
- `spreads jobs`
- `spreads jobs run <job-run-id>`
- `spreads uoa`
- `spreads uoa cycle <cycle-id>`
- `spreads audit <session-id>`

Deferred for now:

- `spreads doctor`

## Goal

Define one operator-facing CLI for visibility into:

- system health
- trading health
- live collector health
- UOA scanner state
- alerts and executions
- session investigation and replay
- post-market effectiveness and tuning signals

This should give us a fast terminal-first view of what the system is doing right now without needing the web UI.

## Why

Right now the information exists, but it is spread across:

- Docker logs
- API endpoints
- Postgres tables
- collector events
- session detail payloads
- account and broker sync services

That is workable for debugging, but weak for day-to-day operations.

We need a CLI that answers questions like:

- is the system healthy
- are collectors running and current
- are websockets degraded
- is trading allowed right now
- what positions and risks are open
- did alerts fire
- is UOA actually active or just empty
- did a stream only "run" or did it produce usable ideas
- is board promotion outperforming watchlist ideas or lagging it
- is the same label degrading for multiple sessions in a row
- what broke in the last few minutes

## Design Principles

### 1. One entrypoint

Use one command family, for example:

```text
spreads ...
```

Do not create a separate command for every narrow query.

Visibility should live inside the umbrella CLI, not behind a separate binary.

### 2. Human-readable first, JSON always available

Default output should be compact terminal output for operators.

Every command should also support:

- `--json`
- `--limit`
- `--watch`

That keeps the CLI usable both by humans and automation.

### 3. Service-first, not API-scraping

The CLI should call local services and repositories directly by default.

It should reuse existing service shapes where possible:

- `account_state`
- `sessions`
- `audit_replay`
- `control_plane`
- `execution_portfolio`
- `uoa_state`
- job and alert repositories

Do not make the CLI depend on the FastAPI server being up just to inspect local state.

### 4. Root-cause oriented

The CLI should not just print raw rows.

It should answer:

- status
- why
- what changed
- what needs attention

### 5. Thin adapters over current services

Avoid embedding business logic in Typer command bodies.

The right structure is:

```text
src/spreads/cli/main.py
src/spreads/cli/ops.py
src/spreads/services/ops_visibility.py
```

The CLI should be a thin input/output adapter over a reusable visibility service layer.

## Library Choice

### CLI Framework

Use `Typer` for the umbrella `spreads` CLI.

Why:

- best shell completion and command UX for an operator-facing CLI
- typed command signatures keep subcommands and options readable
- better help and discoverability than raw `argparse`
- fits a multi-command operator surface better than extending the older script style
- clean fit for a single repo-wide command entrypoint

The direction is to move the repo onto one canonical CLI:

- `spreads status`
- `spreads trading`
- `spreads sessions`
- `spreads sessions <session-id>`
- `spreads scan`
- `spreads collect`
- `spreads analyze`

Do not plan around a long-term split between `spreads` and legacy one-off binaries.

### Rendering

Use `Rich` for the default terminal output and watch-mode rendering.

Recommended approach:

- `Rich` tables, panels, and status styling for default terminal output
- `json.dumps(..., indent=2)` for `--json`
- a small shared render helper module, for example:

```text
src/spreads/cli/ops_render.py
```

Use `Rich` from day one for this CLI.

### Data Access

Use direct service and repository calls by default.

This CLI should not depend on:

- the API server being up
- Docker networking
- HTTP calls to localhost

Optional later:

- add `--via-api` for remote-stack inspection

But local-service access should remain the primary path.

### Dependencies

Add:

- `typer`
- `rich`

Optional later:

- `shellingham` if we want stronger shell detection ergonomics

Do not add a separate TUI framework in V1.

## Proposed Module Structure

Keep the structure shallow and centered on one root app.

```text
src/spreads/cli/main.py
src/spreads/cli/ops.py
src/spreads/cli/ops_render.py
src/spreads/services/ops_visibility.py
```

Responsibilities:

- `main.py`
  - define the root Typer app
  - register command groups and top-level commands
  - own the canonical `spreads` entrypoint
- `ops.py`
  - define visibility-oriented commands
  - dispatch operator command handlers
  - own exit codes
- `ops_render.py`
  - Rich renderables and layout helpers
  - JSON rendering
  - watch-loop refresh presentation
- `ops_visibility.py`
  - aggregate status payloads from existing services and repositories
  - normalize operator-facing status and attention items

Keep business logic out of the render layer.

## Command Architecture

Use Typer subcommands under one app.

High-level shape:

```text
spreads
  status
  trading
  sessions
  uoa
  alerts
  jobs
  audit
  doctor
  scan
  collect
  analyze
```

Implemented now:

- `status`
- `trading`
- `sessions`
- `jobs`
- `uoa`
- `audit`

Planned or deferred:

- `alerts`
- `doctor`
- `scan`
- `collect`
- `analyze`

Each subcommand should follow the same internal flow:

```text
Typer command callback
  -> build visibility payload
  -> optionally normalize/annotate
  -> render Rich or JSON output
  -> return exit code
```

Shared global flags:

- `--db`
- `--json`
- `--watch`
- `--no-color`
- `--utc`

Subcommand-specific flags should stay narrow.

Completion should be a first-class feature of the command, not a later add-on.

## Payload Contract

Every top-level visibility builder in `ops_visibility.py` should return the same outer shape:

```json
{
  "status": "healthy",
  "generated_at": "2026-04-10T15:00:00Z",
  "summary": {},
  "attention": [],
  "details": {}
}
```

Why:

- consistent renderer contract
- simpler watch mode
- cleaner machine-readable output
- easy future reuse by the API or web app

Recommended meanings:

- `status`
  - top-level operator status
- `generated_at`
  - exact build time
- `summary`
  - compact key facts
- `attention`
  - short actionable issues
- `details`
  - richer structured data for deeper inspection

## Exit Codes

The CLI should communicate health through exit codes as well as text.

Recommended scheme:

- `0`
  - healthy / success
- `1`
  - degraded / attention needed
- `2`
  - blocked / halted / hard failure
- `3`
  - invalid input or lookup miss

That makes commands usable in scripts and automations.

## Data Flow

The CLI should aggregate from three layers.

### 1. Runtime State

Examples:

- control mode
- scheduler lease
- worker leases
- queued/running jobs
- broker sync freshness

Primary sources:

- job repository
- control-plane service
- broker repository

### 2. Session State

Examples:

- live collector slot status
- board/watchlist state
- post-market verdicts and recommendations
- board vs watchlist outcome spread
- alerts
- executions
- risk decisions
- audit timeline

Primary sources:

- `sessions.py`
- `audit_replay.py`
- collector repository
- alert repository
- execution repository
- post-market repository

### 3. Trading State

Examples:

- account overview
- open positions
- mark freshness
- reconciliation
- session risk

Primary sources:

- `account_state.py`
- `execution_portfolio.py`
- `risk_manager.py`
- `broker_sync.py`

## Watch Flow

`--watch` should not be a separate command family in V1.

Use a simple polling loop:

```text
run builder
  -> clear screen or rerender a Rich console region
  -> render payload
  -> sleep N seconds
  -> repeat
```

Rules:

- default watch interval should be explicit, not implicit
- keep one command invocation stateless between polls
- recompute from storage/services each cycle
- print the timestamp every refresh

This keeps the first version simple and trustworthy.

## Doctor Flow

`doctor` should be opinionated, not just descriptive.

Flow:

1. collect system/runtime facts
2. run targeted checks
3. classify findings by severity
4. print likely causes and next actions

Initial checks should include:

- control mode halted or degraded
- scheduler lease missing or stale
- no active worker leases
- collector jobs queued but not advancing
- latest collector runs degraded or empty
- quote capture advancing while trade capture is not
- repeated empty or degraded capture for the same label across recent sessions
- broker sync stale
- reconciliation mismatches present
- alert delivery failures present
- latest post-market verdict weak for one or more labels
- board ideas trailing watchlist ideas by a material margin

Output shape should be:

- overall doctor status
- findings
- suspected causes
- recommended next actions

## Recommended Operator Flows

### Morning Check

```text
spreads status
spreads trading
spreads sessions
```

Use this to confirm:

- runtime healthy
- trading allowed
- collector sessions active

### End-Of-Day Review

```text
spreads sessions --date YYYY-MM-DD
spreads sessions <session-id>
```

Use this to confirm:

- which labels were degraded or empty
- latest post-market verdict per label
- whether board ideas beat or lagged watchlist ideas
- whether recommendations are repeating across days

### UOA Check

```text
spreads uoa --label explore_10_combined_0dte_auto
spreads sessions <session-id>
```

Use this to confirm:

- trade capture is active
- UOA roots exist or are truly quiet
- contracts are liquid enough

### Incident Triage

```text
spreads doctor
spreads jobs --status failed
spreads audit <session-id>
```

Use this to confirm:

- what broke
- whether it is systemic or session-specific
- what changed just before failure

## Anti-Patterns To Avoid

- do not duplicate API serialization code inside the CLI
- do not build command-specific SQL queries if a service already owns the logic
- do not let the renderer inspect repositories directly
- do not make watch mode depend on Docker log tailing
- do not add a heavy TUI framework before the Typer + Rich CLI proves what operators actually use

## Operator Surface

The CLI should cover five operator views.

### 1. System Health

Purpose:

- answer whether the runtime is healthy enough to trust

Command shape:

```text
spreads status
```

Core inputs:

- control state
- scheduler lease
- active worker leases
- queued/running jobs
- latest successful collector runs
- latest broker sync state
- latest alert counts and failures

Key outputs:

- overall status: `healthy | degraded | halted`
- control mode
- scheduler freshness
- worker freshness
- queued/running counts by job type
- latest collector status by label
- collector websocket degradation flags
- broker sync freshness
- recent failures summary

This should become the default operator command.

### 2. Trading Health

Purpose:

- show whether the live trading stack is safe and ready

Command shape:

```text
spreads trading
```

Core inputs:

- account overview
- control-plane state
- open execution attempts
- session positions
- session portfolio marks
- session risk snapshots
- reconciliation status
- broker sync state

Key outputs:

- trading allowed / blocked
- account equity, cash, buying power, day PnL
- open position count
- pending execution count
- reconciliation mismatches
- risk breaches or blocks
- quote mark freshness / degradation
- top open positions by exposure and PnL

This is the operator answer to "can the system trade safely right now?"

### 3. Session And Collector Visibility

Purpose:

- show what each live collector session is doing

Command shape:

```text
spreads sessions
spreads sessions <session-id>
```

Mode behavior:

- no positional argument
  - list all matching sessions
- `session_id` positional argument present
  - show detailed view for that session

Core inputs:

- `list_existing_sessions(...)`
- `get_session_detail(...)`
- live collector job runs
- collector cycle candidates and events
- alerts
- executions
- risk decisions

Key outputs for list mode:

- session id
- label
- status
- latest slot
- capture status
- websocket/baseline quote counts
- board/watchlist counts
- alert count
- latest post-market verdict when analysis exists
- board vs watchlist modeled PnL spread when analysis exists
- updated at

Key outputs for detail mode:

- current cycle summary
- board/watchlist candidates
- latest slot runs
- quote capture health and empty-capture flags
- alerts
- executions
- portfolio
- risk snapshot
- reconciliation snapshot
- post-market verdict, recommendations, and board vs watchlist comparison when analysis exists
- top and bottom modeled ideas when analysis exists

This is the main path for strategy-specific operational visibility.

### 4. UOA Visibility

Purpose:

- expose the unusual activity system as an operator surface, not just a backend artifact

Command shape:

```text
spreads uoa
spreads uoa cycle <cycle-id>
spreads uoa symbol <symbol>
```

Core inputs:

- `get_latest_uoa_state(...)`
- `get_uoa_state_for_cycle(...)`
- collector runs and events
- trade capture summaries

Key outputs:

- quote capture vs trade capture health
- top UOA roots
- top contracts
- decision states: `none | watchlist | board | high`
- scoreable vs excluded trade counts
- top exclusion reasons
- supporting contract context: DTE, volume, OI, IV, `%OTM`, spread quality

This should be the terminal-first way to validate whether UOA is active, quiet, or broken.

### 5. Investigation And Replay

Purpose:

- support fast root-cause work when something looks wrong

Command shape:

```text
spreads alerts
spreads jobs
spreads jobs run <job-run-id>
spreads audit <session-id>
spreads doctor
```

Core inputs:

- alert repository
- jobs repository
- audit replay service
- collector/job health
- broker sync and control state

Key outputs:

- recent alerts and statuses
- recent failed/skipped job runs
- queued/running backlog
- session event timeline
- current system blockers
- targeted diagnostics

`doctor` is where runtime sanity checks should live.

That command is the right place to catch issues like:

- scheduler lease missing
- worker processes stale after code change
- collector rows advancing but trade capture rows not advancing
- repeated empty or degraded capture for the same label across recent sessions
- post-market board ideas trailing watchlist ideas by a material margin
- recurring weak verdicts or repeated tuning recommendations for one label
- broker sync stale
- control mode halted

## Proposed Command Tree

```text
spreads status
spreads trading
spreads sessions [SESSION_ID] [--date YYYY-MM-DD]
spreads uoa [--label LABEL]
spreads uoa cycle <cycle-id>
spreads uoa symbol <symbol> [--date YYYY-MM-DD]
spreads alerts [--date YYYY-MM-DD] [--label LABEL] [--symbol SYMBOL]
spreads jobs [--job-type TYPE] [--status STATUS]
spreads jobs run <job-run-id>
spreads audit <session-id>
spreads doctor
spreads scan ...
spreads collect ...
spreads analyze ...
```

Optional later:

```text
spreads watch status
spreads watch trading
spreads watch uoa
```

## Namespace Decision

Use flat top-level commands under `spreads`.

Preferred:

- `spreads status`
- `spreads trading`
- `spreads sessions`
- `spreads uoa`
- `spreads jobs`

Do not add an extra namespace layer such as:

- `spreads ops status`
- `spreads live sessions`

Why:

- shorter operator commands
- better completion UX
- clearer default discovery
- lower nesting for the commands we expect to use most often

If a command family grows too wide later, we can regroup then.

For V1, flat is the right tradeoff.

## Legacy Command Migration Policy

`spreads` should become the canonical CLI entrypoint.

Migration policy:

1. Add `spreads` first.
2. Implement new visibility commands only under `spreads`.
3. Add `scan`, `collect`, and `analyze` under `spreads` as wrappers around existing service entrypoints.
4. Keep the old binaries only as temporary compatibility shims during migration.
5. Remove the old one-off script entrypoints after docs, automation, and operator usage have moved over.

Compatibility mapping:

- `spreads-scan` -> `spreads scan`
- `spreads-collect` -> `spreads collect`
- `spreads-analyze` -> `spreads analyze`

The long-term goal is one root command, not permanent dual entrypoints.

## Proposed Data Layer

Add one new shared service module:

```text
src/spreads/services/ops_visibility.py
```

Its job should be to build normalized payloads for:

- `build_system_status(...)`
- `build_trading_health(...)`
- `build_sessions_view(...)`
- `build_uoa_overview(...)`
- `build_jobs_overview(...)`
- `build_alerts_overview(...)`
- `run_ops_doctor(...)`

This keeps command formatting separate from data assembly.

It also gives the future web UI a reusable backend-friendly aggregation layer.

## Existing Building Blocks To Reuse

### System

- `/jobs/health` payload logic in [main.py](/Users/adeb/Projects/spreads/apps/api/main.py)
- control snapshot in [control_plane.py](/Users/adeb/Projects/spreads/src/spreads/services/control_plane.py)
- live collector payload enrichment in [live_collector_health.py](/Users/adeb/Projects/spreads/src/spreads/services/live_collector_health.py)

### Trading

- account overview in [account_state.py](/Users/adeb/Projects/spreads/src/spreads/services/account_state.py)
- portfolio and mark refresh in [execution_portfolio.py](/Users/adeb/Projects/spreads/src/spreads/services/execution_portfolio.py)
- session risk in [risk_manager.py](/Users/adeb/Projects/spreads/src/spreads/services/risk_manager.py)
- broker sync in [broker_sync.py](/Users/adeb/Projects/spreads/src/spreads/services/broker_sync.py)

### Session / Investigation

- session list/detail in [sessions.py](/Users/adeb/Projects/spreads/src/spreads/services/sessions.py)
- audit replay in [audit_replay.py](/Users/adeb/Projects/spreads/src/spreads/services/audit_replay.py)
- alerts via [alert_repository.py](/Users/adeb/Projects/spreads/src/spreads/storage/alert_repository.py)
- jobs via [job_repository.py](/Users/adeb/Projects/spreads/src/spreads/storage/job_repository.py)
- post-market runs via [post_market_repository.py](/Users/adeb/Projects/spreads/src/spreads/storage/post_market_repository.py)

### UOA

- latest/cycle state in [uoa_state.py](/Users/adeb/Projects/spreads/src/spreads/services/uoa_state.py)
- decision and summary payloads from the live collector result

## Output Design

Default output should favor:

- short sections
- single-line summaries
- table-like alignment
- explicit status words
- exact timestamps

Recommended conventions:

- one top-line overall status
- a short `Attention` section when degraded
- keep numbers human, not raw JSON, by default
- show both counts and a small sample where useful

Every command should also support:

- `--json`
- `--watch <seconds>`
- `--no-color`

Optional later:

- `--wide`
- `--utc`
- `--local-timezone`

## Status Model

The CLI should normalize many existing statuses into a small operator vocabulary:

- `healthy`
- `degraded`
- `blocked`
- `halted`
- `idle`
- `unknown`

Each top-level command should emit:

- one overall status
- reason codes
- a small number of attention items

## Implementation Mechanics

### Packaging

Update [pyproject.toml](/Users/adeb/Projects/spreads/pyproject.toml):

- add dependencies:
  - `typer`
  - `rich`
- add the canonical script entry:

```toml
spreads = "spreads.cli.main:main"
```

During migration, the old script entries can remain temporarily.

### Root App Layout

Implement:

```text
src/spreads/cli/main.py
```

Responsibilities:

- create the root `Typer` app
- register flat top-level commands
- register migrated workflow commands:
  - `scan`
  - `collect`
  - `analyze`
- expose `main()` for the script entrypoint

### Visibility Command Layout

Implement:

```text
src/spreads/cli/ops.py
src/spreads/cli/ops_render.py
src/spreads/services/ops_visibility.py
```

Recommended split:

- `ops.py`
  - command callbacks
  - option parsing
  - watch-loop orchestration
  - exit-code mapping
- `ops_render.py`
  - Rich table/panel helpers
  - JSON output helper
  - timestamp/status styling helpers
- `ops_visibility.py`
  - read services/repositories
  - normalize payloads
  - produce `summary`, `attention`, and `details`

### Exit-Code Helper

Add one shared mapping helper for visibility commands:

```text
healthy -> 0
degraded -> 1
blocked/halted -> 2
invalid/missing -> 3
```

Keep that logic in one place so every command behaves consistently.

### Watch Implementation

Implement watch mode once and reuse it across commands.

Recommended shape:

- command callback builds a zero-arg function that returns the payload
- shared watch helper reruns the builder on an interval
- renderer handles either one-shot or repeated output

Do not let each command implement its own polling loop.

### Phase 1 File Scope

Phase 1 should touch only:

- [pyproject.toml](/Users/adeb/Projects/spreads/pyproject.toml)
- [main.py](/Users/adeb/Projects/spreads/src/spreads/cli/main.py)
- [ops.py](/Users/adeb/Projects/spreads/src/spreads/cli/ops.py)
- [ops_render.py](/Users/adeb/Projects/spreads/src/spreads/cli/ops_render.py)
- [ops_visibility.py](/Users/adeb/Projects/spreads/src/spreads/services/ops_visibility.py)

Avoid migrating `scan` / `collect` / `analyze` in the same first patch unless the command registration is trivial.

### Phase 1 Verification

Minimum verification for the first implementation:

- `uv run spreads --help`
- `uv run spreads status --help`
- `uv run spreads trading --help`
- `uv run spreads status --json`
- `uv run spreads trading --json`

Then targeted runtime checks:

- `uv run spreads status`
  - confirms scheduler/workers/control/broker sync visibility renders
- `uv run spreads trading`
  - confirms account/positions/risk/reconciliation visibility renders

If Docker is the active runtime, validate these against the current stack rather than spawning duplicate local services.

### Phase Exit Criteria

Phase 1 is done when:

- `spreads` exists as a working root command
- `status` and `trading` render both human output and JSON
- exit codes reflect health state
- watch mode works for both commands
- the output is useful enough to replace ad hoc log/database checks for the covered surfaces

## Implementation Order

### Phase 1: System And Trading

Ship:

- `spreads status`
- `spreads trading`

Why first:

- highest operator value
- lowest UX ambiguity
- immediately useful during live hours

### Phase 2: Sessions, Jobs, And End-Of-Day Visibility

Ship:

- `spreads sessions`
- `spreads sessions <session-id>`
- `spreads jobs`
- `spreads jobs run <job-run-id>`

Why second:

- builds on existing session and job services
- gives us daily operational debugging coverage
- captures the closed-session scorecard operators needed on April 10, 2026 without extra API curls

### Phase 3: UOA Visibility

Ship:

- `spreads uoa`
- `spreads uoa cycle <cycle-id>`

Why third:

- depends on the new UOA payloads already added
- valuable once system/session views exist

### Phase 4: Audit And Doctor

Ship:

- `spreads audit <session-id>`
- `spreads doctor`

Why fourth:

- best for deeper investigation once the main surfaces are in place

Status note:

- `audit` shipped on April 11, 2026
- `doctor` deferred for now and not required for the shipped operator surface

## Non-Goals For V1

- full curses/TUI dashboard
- live log streaming replacement
- alert delivery refactor
- portfolio analytics beyond current session/position health
- cross-machine remote management

## Recommended First V1 Slice

If we want the fastest path to value, build this first:

1. `spreads status`
2. `spreads trading`
3. `spreads sessions`

Requirement for that first `sessions` slice:

- list mode should show capture health plus latest post-market verdict when analysis exists
- detail mode should show recommendations and the top/bottom modeled ideas when analysis exists

That gives us:

- runtime health
- trading safety
- collector/session visibility
- enough end-of-day context to tell "system degraded" from "strategy weak"

without waiting for the deeper UOA and audit subcommands.

## Open Questions

- Should the CLI default to local-service access only, or also support a `--via-api` mode for remote stacks later?
- Do we want one global `status` summary that includes a compact trading and UOA section, or keep those as separate commands only?
- Do we want `doctor` to inspect Docker runtime metadata directly when Docker is available, or stay storage-first and process-agnostic?

## Recommendation

Build `spreads` as the single repo CLI and make visibility its first strong surface.

Start with:

- `status`
- `trading`
- `sessions`

Then extend into:

- `jobs`
- `uoa`
- `audit`
- `doctor`

Then migrate the existing one-off commands behind the same root:

- `scan`
- `collect`
- `analyze`

That gives us a terminal-native operator surface for system visibility now, and a clean aggregation layer the web UI can reuse later.
