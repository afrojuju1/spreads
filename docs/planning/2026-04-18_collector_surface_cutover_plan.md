## Collector Surface Cutover Plan

Status: completed

As of: Saturday, April 18, 2026

Related:

- [System Architecture](../current_system_state.md)
- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md)
- [Alpaca Options Automation Schema](./2026-04-15_alpaca_options_automation_schema.md)

## Completed Implementation

This refactor is now shipped in the checked-in codebase.

Completed work:

- split the read planes explicitly:
  - `services/automation_runtimes.py` now owns owner-first runtime list and detail
  - `services/discovery_sessions.py` now owns the discovery-session compatibility wrapper
- added the owner-plane public surface:
  - API: `/automations`
  - CLI: `uv run spreads automations`
  - web: `/automations`
- moved serialized runtime rows to owner-first public lineage:
  - `opportunities` and `positions` now expose `owner` and `discovery` blocks
  - API and CLI filters now accept `bot_id`, `automation_id`, `strategy_config_id`, and `label`
- fixed execution lineage durability:
  - `execution_attempts` now persist `bot_id`, `automation_id`, and `strategy_config_id`
  - the owner refs are backfilled by migration `20260418_0027_execution_attempt_owner_refs`
- reframed the operator surface:
  - the web app now leads with `Automations`
  - `Pipelines` is retained as the discovery compatibility surface
  - ops status now exposes discovery-session aliases while leading with automation runtime metrics
  - audit now reports as a discovery-audit surface with linked automation outcomes where present

What remains intentionally compatible rather than removed:

- `pipeline_id` still exists as discovery lineage and compatibility identity
- `/pipelines` remains available for discovery diagnostics
- collector infrastructure remains intact as the discovery and capture dependency plane

## Problem

The product decision and execution layer has already moved to `bot` plus `automation`, but a large part of the runtime, ops, API, and UI surface still treats a collector-derived `pipeline_id` as the primary runtime noun.

That is the wrong abstraction.

The system still needs discovery and collection. It does not need collector-shaped ownership to remain the primary way operators, APIs, and downstream services identify runtime state.

Today the code is mixing two different responsibilities:

- discovery infrastructure:
  - collector labels
  - collector cycles
  - quote and trade capture
  - cycle health
- product/runtime ownership:
  - bot
  - automation
  - strategy config
  - opportunity decisions
  - execution intents
  - positions and PnL

The result is not just naming debt. It leaks into persistence defaults, read-model assembly, API filters, UI navigation, and execution lineage.

## What Is Actually Still Collector-Centric

### 1. Runtime Catalog And Session Detail Are Synthesized From `live_collector`

The runtime list/detail stack still manufactures `pipeline` rows from enabled `live_collector` job definitions and collector labels.

Primary files:

- `packages/core/services/live_pipelines.py`
- `packages/core/services/live_runtime.py`
- `packages/core/services/pipelines.py`
- `packages/api/routes/pipelines.py`
- `packages/core/cli/runtime.py`

Current shape:

- `list_enabled_live_collector_pipelines(...)` creates the runtime catalog from `live_collector` jobs.
- `pipeline_id` is derived from the collector label instead of from bot or automation ownership.
- the session detail payload combines:
  - collector cycle state
  - collector events
  - automation summary
  - execution portfolio
  - risk state

This creates one oversized read surface that pretends discovery and automation ownership are the same thing.

### 2. Persistence Still Defaults Identity Back To `pipeline_id`

Canonical opportunity persistence still writes a collector-derived `pipeline_id` by default, including runtime-owned automation opportunities.

Primary files:

- `packages/core/services/runtime_identity.py`
- `packages/core/services/opportunity_generation.py`
- `packages/core/services/signal_state.py`
- `packages/core/storage/signal_repository.py`

Current shape:

- `build_pipeline_id(label)` is still the default compatibility identity.
- opportunity rows persist `bot_id`, `automation_id`, and `strategy_config_id`, but still also default `pipeline_id` from `label`.
- runtime-owned automation opportunities therefore look bot-owned and collector-owned at the same time.

This is acceptable as a compatibility field. It is not acceptable as the primary query and display identity.

### 3. Execution And Position Lineage Still Depend On Pipeline Compatibility

Execution and position flows still carry collector compatibility identity further than they should.

Primary files:

- `packages/core/services/execution/__init__.py`
- `packages/core/services/session_positions.py`
- `packages/core/services/positions.py`
- `packages/core/services/execution_portfolio.py`
- `packages/core/storage/execution_models.py`
- `packages/core/storage/execution_repository.py`

Current shape:

- `execution_attempts` still persist `pipeline_id`, `label`, `cycle_id`, and `candidate_id`, but not first-class `bot_id`, `automation_id`, or `strategy_config_id`.
- `session_positions` already persist `bot_id`, `automation_id`, and `strategy_config_id`, but position enrichment still reconstructs label and session identity from `pipeline_id`.
- position and portfolio listing APIs still filter publicly by `pipeline_id`, even though the storage layer already supports `bot_id`, `automation_id`, and `strategy_config_id`.

This means the durable trade lineage is still biased toward discovery compatibility instead of runtime ownership.

### 4. Ops And Audit Still Treat Collectors As The Main Runtime Surface

Ops has useful automation metrics, but they are layered on top of a collector-first summary model.

Primary files:

- `packages/core/services/ops/collectors.py`
- `packages/core/services/ops/system.py`
- `packages/core/services/ops/audit.py`
- `packages/core/services/audit_snapshot.py`
- `packages/core/cli/ops_render.py`

Current shape:

- `latest_collectors` is still a first-class top-level operator payload.
- top-level system summary still leads with collector counts and collector opportunity totals.
- audit is centered on `pipeline_id`, even when the runtime behavior being inspected is actually bot and automation decisioning layered on top of that discovery session.

Collectors should remain visible. They should not remain the primary product status plane.

### 5. API And UI Still Expose The Wrong Public Noun

The external surface still pushes `pipelines` as the main live-runtime concept.

Primary files:

- `packages/api/routes/pipelines.py`
- `packages/api/routes/opportunities.py`
- `packages/api/routes/positions.py`
- `packages/web/components/layout-nav.tsx`
- `packages/web/components/opportunities/opportunities-index.tsx`

Current shape:

- `/pipelines` remains the main live runtime route.
- `opportunities` and `positions` expose `pipeline_id` filters publicly, not owner filters.
- the web nav still presents `Pipelines` as the main operational surface.
- opportunities still link back to pipeline pages as the main lineage jump.

That keeps the operator mental model anchored on discovery rather than on automation ownership.

## The Right Refactor

This should be a boundary split, not a mass rename.

The system needs two explicit read planes:

### A. Discovery Sessions

This is the collector-owned diagnostic surface.

It should own:

- discovery label
- cycle id
- quote and trade capture health
- selection summary for the discovery cycle
- live action gate
- recovery gaps
- raw cycle events

It should not pretend to be the product owner of automations, positions, or bot performance.

### B. Automation Runtime

This is the bot and automation-owned operational surface.

It should own:

- bot id
- automation id
- strategy config id
- latest automation run
- runtime-owned opportunities
- opportunity decisions
- execution intents
- open positions
- realized and unrealized PnL
- dispatch gaps

It may link back to discovery session and cycle lineage, but it should not be derived from `pipeline_id` as the primary identity.

## Target Identity Model

### Keep

- `label`
- `cycle_id`
- `session_id`
- `pipeline_id`

But only as discovery lineage and compatibility.

### Make Canonical For Runtime-Owned Rows

- `bot_id`
- `automation_id`
- `strategy_config_id`
- `strategy_id`
- `config_hash`

### Add Or Normalize Where Needed

- a public `owner` block on serialized opportunity and position payloads:
  - `owner_kind`
  - `bot_id`
  - `automation_id`
  - `strategy_config_id`
- a public `discovery` block:
  - `label`
  - `cycle_id`
  - `session_id`
  - `pipeline_id` as deprecated compatibility

The point is not to delete discovery lineage. The point is to stop using it as the primary product identity.

## Proposed Implementation Phases

### Phase 1. Split Read Models Without Breaking Compatibility

Create a clean read-model separation first.

Work:

- introduce a discovery-session service layer that owns the current collector-session view now living in:
  - `services/live_pipelines.py`
  - `services/live_runtime.py`
  - `services/pipelines.py`
- introduce an automation-runtime service layer that composes existing:
  - `services/automation_runtime.py`
  - `services/bot_analytics.py`
  - `services/opportunities.py`
  - `services/positions.py`
  - `storage/signal_repository.py` owner filters
  - `storage/execution_repository.py` owner filters
- keep `/pipelines` as a compatibility wrapper over discovery sessions for now
- add explicit `/automations` or `/bots` runtime list/detail routes for the owner plane

Why first:

- lowest migration risk
- no schema change required
- immediately fixes the operator mental model

### Phase 2. Move Public Filters Off `pipeline_id`

Shift the public API and CLI to owner-aware queries.

Work:

- extend `services/opportunities.py` and `services/positions.py` to accept:
  - `bot_id`
  - `automation_id`
  - `strategy_config_id`
  - `label`
- expose those filters in:
  - `packages/api/routes/opportunities.py`
  - `packages/api/routes/positions.py`
  - `packages/core/cli/runtime.py`
- keep `pipeline_id` as deprecated compatibility input
- update web pages to group and filter by bot or automation first, with discovery lineage shown second

Why second:

- the storage layer already supports most of this
- it removes a large amount of fake pipeline primacy with minimal backend risk

### Phase 3. Fix Execution Attempt Ownership

Make execution lineage first-class for bot automation ownership.

Work:

- add nullable `bot_id`, `automation_id`, and `strategy_config_id` to `execution_attempts`
- populate them from the selected opportunity or request metadata in `services/execution/__init__.py`
- backfill from `request_json` and linked opportunity where possible
- stop reconstructing operator identity from `pipeline_id` in downstream position/session serializers

Why this matters:

- `session_positions` already preserve runtime ownership better than `execution_attempts`
- until attempts carry owner identity directly, runtime reporting will keep leaking back toward discovery compatibility

### Phase 4. Reframe Ops And Audit

Change ops from collector-first to split-plane reporting.

Work:

- keep collector health, but rename and scope it as discovery health
- change top-level system/trading summaries to lead with:
  - automation runtime
  - decisions
  - intents
  - open bot-managed positions
  - PnL
- treat discovery health as a supporting dependency plane
- rename audit to a discovery-session audit surface, with linked bot and automation outcomes where present

This keeps the collector path visible without misrepresenting ownership.

## Recommended File Cut

### Discovery Plane

- `packages/core/services/live_pipelines.py`
- `packages/core/services/live_runtime.py`
- `packages/core/services/pipelines.py`
- `packages/core/services/ops/collectors.py`
- `packages/core/services/ops/audit.py`
- `packages/core/services/audit_snapshot.py`
- `packages/api/routes/pipelines.py`

### Automation Plane

- `packages/core/services/automation_runtime.py`
- `packages/core/services/bot_analytics.py`
- `packages/core/services/opportunities.py`
- `packages/core/services/positions.py`
- `packages/api/routes/opportunities.py`
- `packages/api/routes/positions.py`
- `packages/core/cli/runtime.py`

### Execution Lineage

- `packages/core/services/execution/__init__.py`
- `packages/core/services/session_positions.py`
- `packages/core/services/execution_portfolio.py`
- `packages/core/storage/execution_models.py`
- `packages/core/storage/execution_repository.py`

### Compatibility Identity

- `packages/core/services/runtime_identity.py`
- `packages/core/storage/signal_repository.py`
- `packages/core/services/opportunity_generation.py`
- `packages/core/services/signal_state.py`

## Non-Goals

- do not delete collector tables
- do not remove `live_collector` as the discovery worker entrypoint yet
- do not introduce a second selector or a second opportunity store
- do not collapse `job`, `collector`, `bot`, and `automation` into one noun

## Migration Risk

The main risk is not data correctness. The main risk is compatibility churn across API, CLI, and web surfaces.

That is why the cut should be:

1. split discovery and automation read models
2. add owner-first filters and payloads
3. migrate UI and ops usage
4. only then demote `pipeline_id` to compatibility status in public surfaces

## Validation

After each phase, validate:

- discovery session detail still reports cycle, capture, and gate state correctly
- automation runtime detail matches `automation_runs`, `opportunity_decisions`, `execution_intents`, and positions
- opportunity and position filters return the same rows under compatibility and owner-based queries
- ops summaries no longer require collector-derived aggregation to explain bot runtime behavior

## Recommendation

Start with Phase 1 plus Phase 2 together.

That gives the highest leverage cut:

- it fixes the public ownership model
- it avoids premature schema churn
- it reuses storage and analytics that already exist
- it makes the remaining execution-lineage schema work smaller and more obvious
