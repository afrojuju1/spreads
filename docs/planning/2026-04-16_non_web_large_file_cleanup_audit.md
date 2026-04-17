## Non-Web Large File Cleanup Audit

Status: in progress

As of: Thursday, April 16, 2026

## Goal

Audit the non-web backend for:

- oversized files
- thin wrapper or effectively unused files
- duplicated helpers and weak ownership boundaries

Then propose a cleanup structure that fits the repo's existing rules:

- `services/` owns business logic
- `storage/` owns persistence
- `jobs/` owns worker and scheduler entrypoints
- `packages/api` stays a thin adapter

This document started as a cleanup and abstraction proposal and is now also serving as the running checkpoint for the implemented package cutovers below.

## Progress Snapshot

Completed clean cuts:

- `services/scanners/` now owns scanner behavior; the old top-level scanner monolith and wrapper-only setup module were removed.
- `services/collections/` now owns collector behavior; the old `jobs/live_collector.py` logic container was deleted.
- `core.jobs.worker` is now a package split into lifecycle, managed execution, planner helpers, observability, and task entrypoints.
- `services/ops/` is now the canonical operator visibility surface; the old `ops_visibility.py` module was replaced by a package-owned surface and its audit/UOA views were split into owned modules.

Still open:

- `services/execution/` cutover from `execution.py`
- `services/replay/` and `services/post_close/` cutover from `opportunity_replay.py` and `analysis.py`
- storage aggregate splits where they still buy clarity after the service cutovers

## Audit Method

Scope excluded:

- `packages/web`
- `.venv`
- runtime outputs

Primary inputs:

- line count by file
- import fan-in and fan-out
- repo-local AGENTS instructions
- current planning docs, especially:
  - `docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md`
  - `docs/planning/2026-04-16_backtest_system_recommendation.md`
  - `docs/planning/2026-04-11_spread_selection_refactor_plan.md`

## Largest Non-Web Code Files

Highest line-count files in backend/runtime scope at audit time:

1. `packages/core/services/scanner.py` - 6089
2. `packages/core/services/execution.py` - 3684
3. `packages/core/services/opportunity_replay.py` - 3375
4. `packages/core/services/ops_visibility.py` - 3190
5. `packages/core/jobs/live_collector.py` - 2313
6. `packages/core/cli/ops_render.py` - 1600
7. `packages/core/jobs/worker.py` - 1556
8. `packages/core/services/execution_intents.py` - 1248
9. `packages/core/storage/signal_repository.py` - 1168
10. `packages/core/services/analysis.py` - 1133

## What The Big Files Actually Own

### 1. `packages/core/services/scanner.py`

Current ownership is too wide.

It currently mixes:

- CLI argument parsing
- environment helpers duplicated from `core.common`
- profile and universe config
- scanner dataclasses and result models
- Alpaca REST client implementation
- Alpaca websocket quote streamer
- setup analysis
- structure builders for verticals, iron condors, and long-vol
- candidate scoring and ranking
- output rendering and file writing
- replay helpers
- live scan orchestration

This is the clearest monolith in the repo.

It also still acts as the implementation source for `core.integrations.alpaca.client`, which is currently just a re-export wrapper over `scanner.py`.

### 2. `packages/core/jobs/live_collector.py`

This file still owns both job-loop orchestration and business logic.

It currently mixes:

- collector CLI args
- bot/options automation scope handling
- scanner invocation
- candidate filtering and promotion
- capture target selection
- live quote and trade capture
- UOA summary and decision capture
- signal sync
- alert dispatch
- optional auto execution
- event persistence helpers
- collection loop control

This matches the current planning docs: `live_collector.py` owns too much and is the wrong center of gravity for the long-term runtime.

### 3. `packages/core/services/execution.py`

This file is large, but unlike `scanner.py` it is large for a more coherent reason: it owns the execution handoff and ledger sync path.

Still, it currently bundles several distinct concerns:

- policy normalization
- open guard evaluation
- reactive quote lookup
- order request building
- queueing and job orchestration
- attempt state transitions
- event publication
- live-session execution entrypoints
- close execution entrypoints
- worker submit execution

This should stay a canonical surface, but it should stop being one file.

### 4. `packages/core/services/opportunity_replay.py`

This file is large and structurally mixed, but it is also already recognized in planning docs as an offline evaluation surface rather than the future backtest engine.

It currently mixes:

- bucket and classification helpers
- recovered candidate reconstruction
- regime snapshot creation
- opportunity reconstruction
- allocation and comparison logic
- execution and outcome matching
- scorecard and aggregate reporting
- recent-session batch aggregation

This is useful code, but it is doing too many phases in one module.

### 5. `packages/core/services/ops_visibility.py`

This file is not one service. It is five operator views under one filename:

- system status
- trading health
- jobs overview
- audit view
- UOA overview/detail

It also owns a large number of formatting and status helper functions shared across those views.

The module is oversized because it is an umbrella, not because one domain model is unusually complex.

### 6. `packages/core/services/analysis.py`

This file is explicitly legacy in repo guidance, but newer code still imports pieces of it.

It currently provides:

- `resolve_date()`, used by API routes and pipeline services
- post-close summary assembly
- markdown rendering
- replay client creation
- signal tuning views

That is a bad long-term shape:

- legacy report rendering should not own shared date parsing
- post-market analysis should not depend on a legacy CLI-oriented module for its core summary path

### 7. Repository Files

The largest storage modules are:

- `packages/core/storage/signal_repository.py` - 1168
- `packages/core/storage/execution_repository.py` - 996
- `packages/core/storage/collector_repository.py` - 819

These are large, but they are more coherent than the big service monoliths.

The main cleanup opportunity here is not generic splitting by line count. The better split is by aggregate boundary:

- signal state
- opportunities and decisions
- automation runs
- execution attempts and orders
- execution intents
- portfolio positions and closes

## Thin Wrapper Or Effectively Unused Files

### Existing Wrappers That Should Not Survive The Refactor

- `packages/core/integrations/alpaca/client.py`
  - currently re-exports implementation from `scanner.py`
  - should become the true implementation owner, not remain a pass-through
- `packages/core/domain/profiles.py`
  - currently re-exports scanner-owned profile constants
  - should either become the canonical owner or be deleted

### Likely Dead Or Very Low-Value

- `packages/core/services/setup.py`
  - only re-exports `analyze_underlying_setup` and `attach_underlying_setup`
  - no runtime imports found
  - good deletion candidate once callers are confirmed absent outside the repo

- `packages/core/domain/models.py`
  - re-exports scanner dataclasses
  - no in-repo runtime imports found
  - either delete or make it the true model home and move the dataclasses out of `scanner.py`

### Not Unused, Even If They Have Low In-Repo Fan-In

Do not treat these as dead:

- `packages/api/main.py`
  - runtime entrypoint
- `packages/core/cli/main.py`
  - CLI entrypoint
- `packages/core/jobs/worker.py`
  - ARQ worker settings and task entrypoint

These are low-reference files because frameworks or process startup own the call path.

## Duplicate Helper And Boundary Problems

### `scanner.py` duplicates `core.common`

`scanner.py` still defines:

- `load_local_env`
- `env_or_die`
- `parse_float`
- `parse_int`
- `clamp`

Those helpers already exist in `packages/core/common.py`.

This is low-risk cleanup and should be done early.

### Alpaca integration ownership is backwards

Current reality:

- `core.integrations.alpaca.client` is a wrapper
- `core.services.scanner` owns `AlpacaClient`, `AlpacaOptionQuoteStreamer`, and `infer_trading_base_url`

That is the wrong ownership direction.

Integration code should not live under a scanner monolith and then be re-exported back into an integrations namespace.

### Legacy analysis code still leaks into active surfaces

Current imports show `analysis.py` still provides:

- `resolve_date()` for API routes and pipeline services
- `build_session_summary()` for `post_market_analysis.py`

That means "legacy" is not actually isolated yet.

### Job entrypoints still own too much business logic

`jobs/live_collector.py` is the clearest example, but the same pattern appears in `jobs/worker.py` where worker task registration and task behavior live together.

This is not automatically wrong, but it becomes expensive when the worker grows to support more runtime types.

## Recommended Target Structure

### No Facade Rule

Do not keep the current monolith filenames around as compatibility shims once a package cutover happens.

Acceptable:

- a package root `__init__.py` that exposes a small, intentional public API for that package
- a real CLI or worker entrypoint module that owns process startup

Not acceptable:

- `scanner.py` that only re-exports from `scanners/*`
- `ops_visibility.py` that only re-exports from `ops/*`
- `analysis.py` that only re-exports from a new package

This cleanup should be a direct import rewrite and delete/rename pass, not a long-lived alias layer.

### A. Replace `scanner.py` with `services/scanners/`

Recommended structure:

```text
packages/core/services/scanners/
  __init__.py                 # only if it is the real public API, not a dump of re-exports
  cli.py                      # current scanner main()/arg parsing
  models.py                   # OptionSnapshot, SpreadCandidate, SymbolScanResult, etc.
  profiles.py                 # universes, profiles, session buckets
  setup.py                    # underlying setup analysis
  scoring.py                  # candidate ranking and notes
  outputs.py                  # csv/json/table rendering
  replay.py                   # replay helpers if they still belong with scanner outputs
  runtime.py                  # scan_symbol_live / across_strategies
  builders/
    verticals.py
    condors.py
    long_vol.py

packages/core/integrations/alpaca/
  client.py                   # actual Alpaca REST implementation
  streaming.py                # option quote streamer
```

Ownership after split:

- `scanners/` owns candidate construction and base discovery ranking
- `integrations/alpaca` owns Alpaca transport
- `scanners/models.py` or `core/domain/*` owns dataclasses
- `common.py` owns generic parsing and env helpers

Important rule:

- do not split by strategy first
- split first by responsibility

That avoids creating parallel call-credit, put-credit, condor, and long-vol micro-files that still duplicate the same market-shape logic.

Implementation rule:

- update imports repo-wide and delete `packages/core/services/scanner.py`
- do not keep both `scanner.py` and `scanners/` alive

### B. Replace collector logic with `services/collections/` and a real job entrypoint

Recommended structure:

```text
packages/core/jobs/collections.py                 # real collection job entrypoint

packages/core/services/collections/
  __init__.py                 # only if needed as the real package API
  args.py                     # collection args/build_collection_args
  discovery.py                # scanner invocation, candidate merge/filter
  selection.py                # ranking and state transitions
  capture.py                  # live quote/trade/UOA capture
  events.py                   # event payload assembly/persistence helpers
  targets.py                  # capture target planning
  cycle.py                    # per-cycle orchestration
  runtime.py                  # run_collection_tick / run_collection
```

This matches repo guidance better:

- `jobs/` becomes entrypoint and scheduling glue
- `services/` becomes business logic owner

It also lines up with the planning docs:

- discovery infrastructure should separate from product decision infrastructure
- target planning should become its own layer

Implementation rule:

- rename `jobs/live_collector.py` in the same refactor if practical
- if the filename must temporarily stay for worker registration, keep it only as the real job entrypoint, not as a logic container

### C. Replace `execution.py` with `services/execution/`

Recommended structure:

```text
packages/core/services/execution/
  __init__.py                 # small public API only if necessary
  policy.py                   # normalization and policy refs
  guards.py                   # open timing and gate evaluation
  pricing.py                  # quote snapshots and limit pricing
  orders.py                   # order request building
  sync.py                     # fills, attempts, linked intent sync
  submit.py                   # submit_* entrypoints and worker submit path
  refresh.py                  # refresh_* paths and read-side helpers
```

Why this split works:

- it follows the current function clusters already visible in the file
- it avoids adding a second execution orchestrator
- existing callers can import from `core.services.execution` package paths after a direct rewrite

### D. Replace `ops_visibility.py` with `services/ops/`

Recommended structure:

```text
packages/core/services/ops/
  __init__.py                 # only if needed as the real package API
  common.py                   # shared status/time helpers
  system_status.py
  trading_health.py
  jobs.py
  audit.py
  uoa.py
```

Do the same for `packages/core/cli/ops_render.py`:

```text
packages/core/cli/render/
  common.py
  system.py
  trading.py
  jobs.py
  uoa.py
  audit.py
```

This is mostly a packaging cleanup, but it materially improves navigability.

### E. Replace `analysis.py` and `opportunity_replay.py` with focused packages

Recommended structure:

```text
packages/core/services/post_close/
  summary.py                  # build_session_summary and supporting aggregation
  render.py                   # markdown/text rendering
  legacy_cli.py               # old `spreads analyze` implementation until retired

packages/core/services/replay/
  build.py                    # main replay builder entrypoint
  reconstruction.py           # candidate / opportunity reconstruction
  outcomes.py                 # execution and outcome matching
  scorecard.py                # comparison and scorecard logic
  batch.py                    # recent-session aggregation

packages/core/services/market_dates.py            # resolve trading date helper
```

Then update:

- API routes to import `resolve_market_date()` from `market_dates.py`
- `post_market_analysis.py` to depend on `post_close/summary.py`
- CLI commands to point at the new package modules directly

Implementation rule:

- delete `packages/core/services/analysis.py` after moving its real owners
- replace `packages/core/services/opportunity_replay.py` with `services/replay/`

### F. Split repositories by aggregate, not by utility method count

Recommended storage target:

```text
packages/core/storage/signal_state_repository.py
packages/core/storage/opportunity_repository.py
packages/core/storage/opportunity_decision_repository.py
packages/core/storage/automation_run_repository.py

packages/core/storage/execution_attempt_repository.py
packages/core/storage/execution_intent_repository.py
packages/core/storage/portfolio_position_repository.py
```

Do not force this first.

These files are large, but they are lower-priority than the service monoliths because their ownership is more coherent already.

## Recommended Cleanup Order

### Phase 1: Low-risk cleanup

1. Remove scanner-local duplicates of `core.common` helpers.
2. Move Alpaca implementation ownership from `scanner.py` into `core.integrations.alpaca`.
3. Extract `resolve_market_date()` out of `analysis.py`.
4. Decide whether `services/setup.py` and `domain/models.py` should be deleted or promoted into real owners.

### Phase 2: Package cutover

1. Completed: replace `scanner.py` with `services/scanners/` and rewrite imports in one pass.
2. Completed: replace collector logic with `services/collections/` and delete the old job-owned logic module.
3. Completed: replace `jobs/worker.py` with a real `jobs/worker/` package that keeps `core.jobs.worker` as the canonical ARQ surface.
4. Completed: replace `ops_visibility.py` with `services/ops/`.
5. Next: move CLI render code into `cli/render/`.

### Phase 3: Canonical path hardening

1. Replace `execution.py` with `services/execution/`.
2. Replace `analysis.py` and `opportunity_replay.py` with `post_close/` and `replay/`.
3. Reduce legacy board/watchlist vocabulary in replay and collector projections where safe.

### Phase 4: Repository decomposition

1. Split `signal_repository.py` by aggregate boundary.
2. Split `execution_repository.py` by aggregate boundary.

## Files To Leave Alone For Now

These are large enough to notice, but not the best first cleanup targets:

- `packages/core/services/opportunity_scoring.py`
  - large but still mostly one concern
- `packages/core/services/live_recovery.py`
  - large, but the functions cluster around one runtime concern
- `packages/core/services/session_positions.py`
  - large but fairly cohesive around canonical position state
- `scripts/one_time/*.py`
  - some are large, but they are research scripts rather than runtime ownership problems

For the one-time scripts, the cleanup question is mostly archival:

- keep if docs still reference them
- otherwise move them under an explicit `scripts/research/archived/` shape later

## Recommended Decisions

1. Treat `scanner.py`, `live_collector.py`, `execution.py`, `ops_visibility.py`, and `analysis.py` as the primary cleanup set.
2. Treat `services/setup.py` as a deletion candidate.
3. Treat `domain/models.py` and `domain/profiles.py` as decision points:
   - either delete them as unused wrappers
   - or promote them into real ownership modules and move scanner-owned types/config there
4. Move Alpaca implementation out of `scanner.py` before any deeper scanner split.
5. Do a direct import rewrite during each package cutover and delete the old monoliths instead of leaving alias files behind.

## Bottom Line

The main backend cleanup problem is not "too many large files" in general.

It is that a small number of central files currently mix:

- transport integration
- business logic
- CLI or worker entrypoints
- compatibility behavior
- rendering or reporting

The best structure is not a blind file split.

The best structure is:

- preserve one canonical public surface per responsibility
- extract internal phases into ownership-aligned modules
- delete thin wrappers that are neither canonical nor used
- demote legacy reporting surfaces so active runtime paths stop depending on them
- prefer package directories such as `scanners/`, `collections/`, `execution/`, `ops/`, and `replay/` over `scanner_*` style naming
