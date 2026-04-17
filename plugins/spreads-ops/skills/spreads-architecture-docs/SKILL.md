---
name: spreads-architecture-docs
description: Maintain the canonical architecture, ownership, and boundary docs for the spreads repo, including AGENTS.md guidance, current-vs-target architecture references, and source-of-truth consolidation.
---

# Spreads Architecture Docs

Use this skill inside `/Users/adeb/Projects/spreads` when the task is about:

- updating the system architecture doc
- clarifying service ownership or boundaries
- consolidating overlapping architecture docs
- deciding which doc is the source of truth
- aligning `AGENTS.md` files to the current architecture
- refreshing plugin skills that encode repo architecture or ops ownership

## Canonical Source Of Truth

For the current overall runtime architecture, service ownership, and top-level boundaries, use:

- [docs/current_system_state.md](../../../../docs/current_system_state.md)

If another design or planning doc disagrees about current ownership, `docs/current_system_state.md` wins.

## Supporting Docs

Use these only for the roles they now own:

- [docs/planning/2026-04-11_fresh_spread_system_design.md](../../../../docs/planning/2026-04-11_fresh_spread_system_design.md)
  - target opportunity-selection architecture inside the broader system
- [docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md](../../../../docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md)
  - migration path that reuses the current backend
- [docs/planning/README.md](../../../../docs/planning/README.md)
  - planning-doc entrypoint and doc-role map

Do not let these supporting docs become rival top-level architecture sources.

## Current Boundary Map

When documenting the current system, use these owners:

- discovery and collection:
  - `packages/core/services/scanners/`
  - `packages/core/services/collections/`
  - `packages/core/services/live_selection.py`
  - `packages/core/services/opportunity_scoring.py`
- canonical opportunity state:
  - `packages/core/services/signal_state.py`
  - `packages/core/services/opportunity_generation.py`
  - `packages/core/services/opportunities.py`
- runtime, pipeline, and ops read models:
  - `packages/core/services/live_runtime.py`
  - `packages/core/services/live_collector_health/`
  - `packages/core/services/pipelines.py`
  - `packages/core/services/ops/`
- execution and portfolio state:
  - `packages/core/services/execution/`
  - `packages/core/services/session_positions.py`
  - `packages/core/services/broker_sync.py`
  - `packages/core/services/risk_manager.py`
  - `packages/core/services/exit_manager.py`
- market-data capture:
  - `packages/core/services/market_recorder.py`

## Update Order

When the current architecture changes, update docs in this order:

1. `docs/current_system_state.md`
2. `AGENTS.md` files that encode current ownership or canonical docs
3. plugin skills that encode repo ops or architecture assumptions
4. planning doc statuses, cross-links, and entrypoint wording in `docs/planning/README.md`
5. supporting architecture docs if their implementation-map sections need refresh

## AGENTS.md Rules

When editing repo instructions:

- root `AGENTS.md` should point architecture and ownership questions to `docs/current_system_state.md`
- package-level `AGENTS.md` files should name the real current owners, not stale monoliths
- avoid stale references to old surfaces like `scanner.py`, `live_collector.py`, `execution.py`, or `ops_visibility.py` as if they still own the split responsibilities

## Plugin Skill Rules

When editing `plugins/spreads-ops` skills:

- keep commands aligned with the real CLI surface under `uv run spreads ...`
- prefer `status`, `trading`, `pipelines`, `jobs`, `uoa`, `audit`, and `replay`
- keep runtime ownership aligned with `docs/current_system_state.md`
- refresh stale service references when package splits change ownership

## Quality Bar

- one canonical overall architecture doc
- no duplicated top-level ownership maps with conflicting claims
- clear separation between current architecture and target architecture
- explicit note when a doc is historical, supporting, or canonical
- no fake abstractions or naming that hides the real service owner
