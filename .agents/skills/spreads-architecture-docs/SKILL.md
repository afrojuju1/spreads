---
name: spreads-architecture-docs
description: Maintain the canonical architecture, ownership, and boundary docs for the spreads repo, including AGENTS.md guidance, current-vs-target architecture references, and source-of-truth consolidation.
---

# Spreads Architecture Docs

Use this skill inside the repo root when the task is about:

- updating the system architecture doc
- clarifying service ownership or boundaries
- consolidating overlapping architecture docs
- deciding which doc is the source of truth
- aligning `AGENTS.md` files to the current architecture
- refreshing repo-local skills that encode repo architecture or ops ownership

## Canonical Source Of Truth

For the current overall runtime architecture, service ownership, and top-level boundaries, use:

- [docs/current_system_state.md](../../../docs/current_system_state.md)

If another design or planning doc disagrees about current ownership, `docs/current_system_state.md` wins.

## Supporting Docs

Use these only for the roles they now own:

- [docs/planning/2026-06-08_trading_engine_inspiration_repos.md](../../../docs/planning/2026-06-08_trading_engine_inspiration_repos.md)
  - external trading-engine repos/frameworks to check first when designing trading-engine, source/scanner, strategy-quality, execution, risk, or portfolio refactors
- [docs/planning/2026-06-08_entry_quality_pipeline_refactor.md](../../../docs/planning/2026-06-08_entry_quality_pipeline_refactor.md)
  - completed implementation plan for the `quality_profile` / `EntryQualityPipeline` refactor; use it for historical cutover context and filter-stage naming, but use `docs/current_system_state.md` for current shared `MarketContextEngine` / `RegimeSnapshot` ownership
- [docs/planning/2026-06-11_strategy_archetype_profile_contract.md](../../../docs/planning/2026-06-11_strategy_archetype_profile_contract.md)
  - implemented strategy catalog/profile contract; use it for authored strategy config shape under `packages/config/strategies`, while `docs/current_system_state.md` remains the current runtime source of truth
- [docs/planning/2026-04-11_fresh_spread_system_design.md](../../../docs/planning/2026-04-11_fresh_spread_system_design.md)
  - candidate, signal, decision, and admission ownership inside the broader system
- [docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md](../../../docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md)
  - supporting migration path that reuses the current backend
- [docs/planning/README.md](../../../docs/planning/README.md)
  - planning-doc entrypoint and doc-role map

Do not let these supporting docs become rival top-level architecture sources.
Older planning docs may still mention pre-cutover `replay`, `audit_replay`, or old singular `backtest` surfaces; when they do, prefer the current names and boundaries from `docs/current_system_state.md`. The current backend historical-evaluation adapter is `spreads backtests run`.
For target-state proposals, keep the distinction explicit: `docs/current_system_state.md` describes what is live now, while planning docs describe completed and remaining beads for the target.

## Inspiration Repos

For trading-engine architecture, scanner/source, strategy-quality, execution, risk, or portfolio refactor work:

1. Start from [docs/planning/2026-06-08_trading_engine_inspiration_repos.md](../../../docs/planning/2026-06-08_trading_engine_inspiration_repos.md).
2. Borrow the smallest Spreads-native boundary or pattern that solves the current problem.
3. If research finds a better repo/framework for the active design problem, propose adding it to the inspiration list with:
   - the specific pattern to borrow
   - what not to copy
   - why it improves on the current list

## Boundary Map Belongs In Current System State

Do not duplicate the domain ownership map in `AGENTS.md` files or repo-local skills.

When documenting current ownership, update:

- [docs/current_system_state.md](../../../docs/current_system_state.md)

That document owns the vocabulary and service ownership for signals, decisions, admissions, executor profiles, intents, attempts, orders, fills, positions, closes, reconciliation, broker sync, trading ops state, and storage ops state.

Repo-local skills should remain workflow playbooks that point to the canonical doc.

## Update Order

When the current architecture changes, update docs in this order:

1. `docs/current_system_state.md`
2. `AGENTS.md` files that encode current ownership or canonical docs
3. repo-local skills that encode repo ops or architecture assumptions
4. planning doc statuses, cross-links, and entrypoint wording in `docs/planning/README.md`
5. supporting architecture docs if their implementation-map sections need refresh

## AGENTS.md Rules

When editing repo instructions:

- root `AGENTS.md` should point architecture and ownership questions to `docs/current_system_state.md`
- package-level `AGENTS.md` files should name the real current owners, not stale monoliths
- do not turn `AGENTS.md` into an architecture encyclopedia; put domain ownership in `docs/current_system_state.md`
- avoid stale references to old monolith surfaces as if they still own the split responsibilities

## Repo Skill Rules

When editing repo-local skills under `.agents/skills`:

- keep commands aligned with the real CLI surface under `uv run spreads ...`
- prefer the current shipped CLI surface and the canonical `TradingOpsState` / `StorageOpsState` read models
- keep runtime ownership aligned with `docs/current_system_state.md`
- keep `spreads-ops`, `spreads-strategy-lab`, `spreads-data-platform`, `spreads-live-rollout`, and `spreads-architecture-docs` as distinct workflows instead of duplicating the same triage guidance across skills
- refresh stale service references when package splits change ownership
- keep selection, execution-admission, and alert-projection boundaries explicit instead of letting them collapse into one vague “trading” layer
- keep shared market-context ownership explicit: `MarketContextEngine` owns broad-market regime facts, strategies declare `market_context` policy, and entry quality consumes `market_context_regime_fit` instead of local regime calculators

## Quality Bar

- one canonical overall architecture doc
- no duplicated top-level ownership maps with conflicting claims
- clear separation between current architecture and target architecture
- target proposals are labeled as proposals until their beads are implemented and `docs/current_system_state.md` is updated
- explicit note when a doc is historical, supporting, or canonical
- no fake abstractions or naming that hides the real service owner
- no documentation drift that treats alerts as the source of truth or pushes execution-admission logic back into account snapshot/read-model services
