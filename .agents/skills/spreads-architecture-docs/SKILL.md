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

- [docs/planning/2026-04-11_fresh_spread_system_design.md](../../../docs/planning/2026-04-11_fresh_spread_system_design.md)
  - candidate, signal, decision, and admission ownership inside the broader system
- [docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md](../../../docs/planning/2026-04-15_current_system_options_automation_implementation_approach.md)
  - supporting migration path that reuses the current backend
- [docs/planning/README.md](../../../docs/planning/README.md)
  - planning-doc entrypoint and doc-role map

Do not let these supporting docs become rival top-level architecture sources.
Older planning docs may still mention pre-cutover `replay` or `audit_replay` surfaces; when they do, prefer the current names and boundaries from `docs/current_system_state.md`.

## Boundary Map Belongs In Current System State

Do not duplicate the domain ownership map in `AGENTS.md` files or repo-local skills.

When documenting current ownership, update:

- [docs/current_system_state.md](../../../docs/current_system_state.md)

That document owns the vocabulary and service ownership for signals, decisions, admissions, intents, attempts, orders, fills, positions, closes, reconciliation, broker sync, trading ops state, and storage ops state.

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
- avoid stale references to old surfaces like `scanner.py`, `live_collector.py`, `execution.py`, or `ops_visibility.py` as if they still own the split responsibilities

## Repo Skill Rules

When editing repo-local skills under `.agents/skills`:

- keep commands aligned with the real CLI surface under `uv run spreads ...`
- prefer the current shipped CLI surface while it exists, and update this skill when `spr-zuy` replaces fragmented ops commands with canonical state commands
- keep runtime ownership aligned with `docs/current_system_state.md`
- refresh stale service references when package splits change ownership
- keep selection, execution-admission, and alert-projection boundaries explicit instead of letting them collapse into one vague “trading” layer

## Quality Bar

- one canonical overall architecture doc
- no duplicated top-level ownership maps with conflicting claims
- clear separation between current architecture and target architecture
- explicit note when a doc is historical, supporting, or canonical
- no fake abstractions or naming that hides the real service owner
- no documentation drift that treats alerts as the source of truth or pushes execution-admission logic back into account snapshot/read-model services
