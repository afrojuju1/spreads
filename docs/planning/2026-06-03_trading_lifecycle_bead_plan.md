# Trading Lifecycle Bead Plan

Date: 2026-06-03

Status: beads created with breaking-rewrite posture.

Related:

- [Trading Lifecycle State Machines](./2026-06-03_trading_lifecycle_state_machines.md)
- [Target Trading Lifecycle Object Model](./2026-06-03_target_trading_lifecycle_object_model.md)
- [Lifecycle Contracts Module](./2026-06-03_lifecycle_contracts_module.md)
- [Nautilus Patterns Inside Spreads](./2026-06-03_nautilus_patterns_inside_spreads_architecture.md)
- [Spreads Architecture Review](./2026-06-03_spreads_architecture_review.md)

## Review Findings

The architecture docs are directionally right now, but the implementation work needs to be sliced around lifecycle semantics, not source modules.

Important refinements from review:

- Do not require one broker owner as a foundation.
- Do not require backwards-compatible internals.
- Planned Spreads downtime is acceptable during this refactor.
- Prefer clean replacement over compatibility shims, dual-writes, or parallel old/new runtime paths.
- Current Spreads tables/services are context, not a cage.
- The real foundation is the lifecycle chain: `Signal -> Decision -> Intent -> Admission -> Attempt -> Broker Facts -> Position -> Close`.
- `submit_unknown` must survive as a first-class state in any rewrite.
- Risk/admission should be first-class instead of hidden inside attempt creation.
- Close lifecycle needs first-class design before adapter work.
- Finviz should become a normal signal/decision source before broad strategy expansion.
- Python-native execution adapter work should come after lifecycle contracts, not before.

## Bead Creation Convention

Use `bd create` from the repo root. The local Beads CLI supports:

```bash
bd create "Title" \
  --type feature \
  --priority 1 \
  --labels architecture,execution \
  --description "..." \
  --acceptance "..."
```

Recommended labels for this series:

- `architecture`
- `execution`
- `lifecycle`
- `finviz`
- `options`
- `ops`
- `validation`

Use `--spec-id docs/planning/2026-06-03_trading_lifecycle_bead_plan.md` when creating these beads so future work can trace back to this plan.

## Created Beads

Epic:

- `spr-g9s`: `Rebuild Spreads trading lifecycle around clean state machines`

Children:

- `spr-g9s.1`: `Define target trading lifecycle object model`
- `spr-g9s.2`: `Implement lifecycle contracts and typed states`
- `spr-g9s.3`: `Create clean lifecycle storage and projection plan`
- `spr-g9s.4`: `Normalize Finviz direct flow into lifecycle decisions`
- `spr-g9s.5`: `Make risk and admission first-class`
- `spr-g9s.6`: `Rewrite intent dispatch around lifecycle contracts`
- `spr-g9s.7`: `Rewrite execution attempt lifecycle and reconciliation`
- `spr-g9s.8`: `Make position and close lifecycle first-class`
- `spr-g9s.9`: `Expose lifecycle state in CLI API and dashboard`
- `spr-g9s.10`: `Build Python-native Alpaca order adapter`
- `spr-g9s.11`: `Cut over lifecycle runtime and retire legacy paths`

## Proposed Epic

Title: `Rebuild Spreads trading lifecycle around clean state machines`

Type: `epic`

Priority: `1`

Labels: `architecture,execution,lifecycle`

Description:

Rework the Spreads trading core around explicit lifecycle objects and transitions for signals, decisions, intents, attempts, broker facts, positions, and closes. Current Spreads and Nautilus implementations are input, not compatibility constraints. Planned downtime is acceptable, and the work should choose clean replacements over compatibility hacks. The goal is a cleaner Python-native trading lifecycle that can retire Rust bridge dependence for live Spreads flows.

Acceptance criteria:

- The child beads define and implement a clean lifecycle model.
- Direct Finviz flow, canonical automation flow, risk/admission, and close management use the same lifecycle language.
- Broker submission uncertainty, repricing lineage, close lifecycle, and position projection are first-class.
- Operator CLI/API/dashboard surfaces expose the new lifecycle states clearly.
- Compatibility shims and dual-running old/new internals are not introduced unless they are the clean target design.
- Live paper validation proves entry, skip, submit, fill sync, close, reprice/cancel where applicable, and broker reconciliation.
- No automated tests are added unless explicitly requested; validation is through live/runtime smoke checks and shipped CLIs.

## Bead 1: Target Lifecycle Object Model

Title: `Define target trading lifecycle object model`

Type: `decision`

Priority: `1`

Labels: `architecture,lifecycle`

Depends on: none

Description:

Define the clean target object model for the trading lifecycle. Use current Spreads and Nautilus as research inputs, but do not preserve current table names, payload shapes, or service layout by default. Decide the authoritative facts for signal, decision, intent, admission, attempt, order, fill, position, close, and reconciliation.

Acceptance criteria:

- A design doc defines target objects, fields, state enums, transitions, and invariants.
- The doc explicitly marks which current fields/tables are kept, replaced, merged, or archived.
- `submit_unknown`, admission approved/blocked/unknown, intent supersession, one-active-close policy, and position projection semantics are covered.
- The doc includes a cutover stance for historical data: archive/read-only, backfill, or discard for each major object.
- No code changes are required for this bead.

## Bead 2: Lifecycle Contract Module

Title: `Implement lifecycle contracts and typed states`

Type: `feature`

Priority: `1`

Labels: `architecture,execution,lifecycle`

Depends on: Bead 1

Description:

Introduce the core Python lifecycle contract module for states, transitions, command/event names, and validation helpers. This can replace current string-set logic where sensible. It does not need to keep old internal state names working unless a name is deliberately retained in the target model.

Acceptance criteria:

- Typed/enumerated states exist for intent, admission, attempt, position, and close lifecycle.
- Transition helpers return structured reasons and reject invalid transitions.
- Current lifecycle names are normalized through the new contracts, intentionally replaced, or removed.
- `submit_unknown` and stale/uncertain broker submission states are explicit.
- `uv run python -m py_compile` passes for touched Python files.
- Runtime risk is documented if no live behavior path uses the new module yet.

## Bead 3: Lifecycle Storage Shape

Title: `Create clean lifecycle storage and projection plan`

Type: `feature`

Priority: `1`

Labels: `architecture,execution,postgres`

Depends on: Beads 1, 2

Description:

Design and implement the durable storage shape for lifecycle facts and projections. Full rewrite is allowed where cleaner than evolving current JSON-heavy shapes. The goal is to separate facts from projections and avoid carrying legacy payload clutter into the new core.

Acceptance criteria:

- Schema/model changes or new tables represent lifecycle facts cleanly.
- Fact records are separated from operator projections where practical.
- Storage state values use or deliberately map to the lifecycle contract module.
- Old tables can be replaced, dropped, archived, or rebuilt after historical-data handling is documented.
- Migration/cutover notes explain whether existing live data is archived, backfilled, discarded, or reset.
- Operator CLIs are updated to report the new lifecycle shape; they do not need to support old and new schemas at the same time.
- Validation uses schema inspection and runtime smoke checks, not new automated tests.

## Bead 4: Finviz Signal And Decision Normalization

Title: `Normalize Finviz direct flow into lifecycle decisions`

Type: `feature`

Priority: `1`

Labels: `finviz,lifecycle,execution`

Depends on: Beads 1, 2, 3

Description:

Move Finviz direct trading from a special feed-to-intent lane into the lifecycle model. The Finviz feed should produce signal/decision records with the same skip/block/selected language used by the rest of the trading system. This can rewrite the Finviz flow where cleaner.

Acceptance criteria:

- Finviz candidates produce lifecycle-visible signal/decision records before intents.
- Decision output preserves existing clear reasons such as timing, spread, budget, option selection, and setup reset.
- Trade selection, caps, timing windows, and option-selection behavior can be cleaned up when useful; deliberate behavior changes are documented in the bead close note.
- At most configured caps are armed; repeat same-day trades remain allowed when setup and budget permit.
- The live ledger/CLI/dashboard show Finviz decisions through the same lifecycle vocabulary as other flows.
- Live validation confirms feed refresh, candidate visibility, skip reasons or selected intents, and no duplicate unintended entries.

## Bead 5: Risk And Admission Lifecycle

Title: `Make risk and admission first-class`

Type: `feature`

Priority: `1`

Labels: `execution,lifecycle,risk`

Depends on: Beads 3, 4

Description:

Move risk/admission into the lifecycle model before attempt creation. Every live entry path should distinguish selected strategy decisions from admission approvals, blocks, and unknowns. Finviz long calls should not rely only on lane-local caps once they are part of the unified lifecycle.

Acceptance criteria:

- Admission outcomes are explicit: approved, blocked, and unknown.
- Admission records include policy snapshot, metrics, blockers, evidence, and reason codes.
- Approved admissions attach to created attempts.
- Blocked or unknown admissions stop before attempt creation and leave operator-visible reasons.
- Canonical automation and Finviz direct entries use the same admission vocabulary.
- Close validation is either represented as close admission or documented as a separate lifecycle with equivalent reason visibility.

## Bead 6: Intent Dispatch Rewrite

Title: `Rewrite intent dispatch around lifecycle contracts`

Type: `feature`

Priority: `1`

Labels: `execution,lifecycle`

Depends on: Beads 3, 4, 5

Description:

Replace or simplify the current execution-intent dispatch path around the new lifecycle contracts. Preserve semantics, not current implementation details: TTL, claim, revoke, supersession, source-active checks, and dispatch events.

Acceptance criteria:

- Intent creation, claim, expiration, revocation, supersession, and dispatch are lifecycle-contract driven.
- Open intents validate their signal/decision source.
- Close intents validate their position source.
- Reprice replacement creates lineage instead of mutating the old intent.
- Operator surfaces show intent state and reason clearly.
- Live validation shows pending intents dispatch or skip with clear reasons.

## Bead 7: Attempt Lifecycle Rewrite

Title: `Rewrite execution attempt lifecycle and reconciliation`

Type: `feature`

Priority: `1`

Labels: `execution,lifecycle,broker-sync`

Depends on: Beads 3, 6

Description:

Rewrite the attempt lifecycle so local queue state, adapter submission, broker order state, fills, uncertainty, stale handling, and reconciliation are explicit. This should be adapter-independent and ready for both direct Alpaca and future Python-native multi-leg orders.

Acceptance criteria:

- Attempts move through explicit local, unknown, working, canceling, partial, terminal, and stale phases.
- `submit_unknown` reconciles by client order id before any failure cleanup.
- Orders and fills are projected under attempts.
- Attempt refresh updates linked intent state without hiding broker uncertainty.
- Stale queued or working attempts have deterministic operator-visible next actions.
- Live validation proves current direct option/equity attempts still submit and refresh correctly.

## Bead 8: Position And Close Lifecycle Rewrite

Title: `Make position and close lifecycle first-class`

Type: `feature`

Priority: `1`

Labels: `execution,exits,lifecycle`

Depends on: Beads 3, 7

Description:

Rewrite position projection and close management around explicit lifecycle contracts. Close decisions, close intents, close attempts, close fills, and position recalculation should be separate concepts.

Acceptance criteria:

- Position projection derives from filled open attempts and close facts.
- Partial open, open, partial close, and closed semantics are explicit or deliberately replaced.
- Close decisions record reason, policy, quote source, limit source, and decision time.
- One-active-close-per-position is enforced or replaced by an explicit close-order policy.
- Close attempts preserve position linkage and sell-to-close/buy-to-close semantics.
- Live validation proves profit/stop/force-close can create and complete a close without duplicate close exposure.

## Bead 9: Operator Lifecycle Views

Title: `Expose lifecycle state in CLI API and dashboard`

Type: `feature`

Priority: `2`

Labels: `ops,dashboard,lifecycle`

Depends on: Beads 5, 6, 7, 8

Description:

Expose the lifecycle model through the operator surfaces. The goal is for live operators to see the current state and next action for signals, decisions, intents, attempts, positions, closes, and broker reconciliation without SQL/log spelunking.

Acceptance criteria:

- `live-doctor` or a shipped CLI summarizes lifecycle health and anomalies.
- API/dashboard surfaces show active/pending/stale/unknown/terminal lifecycle counts.
- Finviz ledger or equivalent shows decision-to-intent-to-attempt-to-position lineage.
- Close lifecycle proof is visible per open/closed position.
- Runtime validation confirms views work against live paper data.

## Bead 10: Python-Native Single And Multi-Leg Execution Adapter

Title: `Build Python-native Alpaca order adapter`

Type: `feature`

Priority: `2`

Labels: `execution,options,alpaca`

Depends on: Beads 7, 8

Description:

Build the Python-native Alpaca adapter for single-leg and multi-leg submission using the new attempt lifecycle. It should support direct long calls first, then verticals and iron condors as the order-list shape is proven. This is the Rust bridge retirement path. The bridge can be used as a reference implementation, but it is not a required fallback.

Acceptance criteria:

- Adapter handles single-leg option open/close through lifecycle attempts.
- Adapter handles multi-leg order-list payloads for defined-risk spreads behind explicit live enablement or an operational kill switch.
- Cancel/replace and close attempts preserve lifecycle lineage.
- Broker order snapshots and fills project under attempts.
- Live paper validation proves order submit, broker accept/reject, fill sync, position sync, and close for tiny size.
- Rust bridge fallback is not required inside this adapter path; keep or remove it based on the cutover decision, not compatibility pressure.

## Bead 11: Cutover And Legacy Retirement

Title: `Cut over lifecycle runtime and retire legacy paths`

Type: `task`

Priority: `2`

Labels: `ops,lifecycle,validation`

Depends on: Beads 4, 5, 6, 7, 8, 9, 10

Description:

Cut over live paper flows to the new lifecycle runtime and remove or disable legacy paths that should no longer be used. Planned downtime is acceptable. No backwards-compatible internals, dual-write paths, or old-runtime fallback are required.

Acceptance criteria:

- Active Finviz paper flow uses the lifecycle runtime.
- Legacy direct paths that bypass lifecycle decisions are disabled or removed.
- Bridge/Rust-dependent Spreads runtime is disabled or removed after Python-native validation, without preserving a fallback path by default.
- Operator docs and planning docs are updated to reflect the new current state.
- Live validation covers feed, decisions, intents, attempts, fills, positions, closes, reconciliation, logs, and resource health.

## Suggested Dependency Graph

```text
Bead 1
  +--> Bead 2

Bead 2
  +--> Bead 3

Bead 3
  +--> Bead 4

Bead 4
  +--> Bead 5 --> Bead 6 --> Bead 7 --> Bead 8

Beads 5, 6, 7, 8
  +--> Bead 9

Beads 7, 8
  +--> Bead 10

Beads 4, 5, 6, 7, 8, 9, 10
  +--> Bead 11
```

## Creation Notes

When ready to create the beads:

- Create the epic first.
- Create Beads 1-11 as children of the epic.
- Add explicit dependencies using `bd dep` or `bd create --deps`.
- Keep Bead 1 and Bead 2 separate so design can settle before code starts.
- Keep Bead 3 after Bead 2 so storage does not encode lifecycle strings that the contract module later replaces.
- Keep Bead 4 after Bead 3 so Finviz decision records land in the chosen storage shape instead of creating another transitional table.
- Keep Bead 10 after lifecycle attempt/close work; adapter work before lifecycle work will recreate the current split.
- It is acceptable for intermediate beads to break live Spreads trading until the lifecycle path is rebuilt and validated.

No bead should require automated tests unless the user explicitly asks for test work. Each implementation bead should include live validation commands and residual runtime risk in its close note.

## Ready-To-Split Checklist

- The epic and Beads 1-11 are scoped so each bead can close independently.
- The work starts with target objects and contracts before storage, runtime rewrites, or adapter work.
- The storage bead follows contracts, and Finviz normalization follows storage.
- Finviz normalization happens before risk/admission and dispatch rewrites, so the active lane is not left as a special case.
- Risk/admission is its own bead because it is a money-path gate, not an incidental dispatch detail.
- Attempt, position, close, and adapter work are deliberately separated so broker submission parity does not hide lifecycle gaps.
- Operator views land before cutover, so live validation has a first-class surface.
- Cutover is last and can remove legacy internals instead of preserving backwards-compatible behavior.

## Behavior Guardrails

- Do not add compatibility shims, dual-write tables, shadow-only lifecycle paths, or feature gates solely to preserve old behavior during the rewrite.
- Feature flags are acceptable for operational kill switches or controlled enablement, not as a substitute for deleting the old path.
- Behavior changes to Finviz scoring, timing, option selection, or caps are allowed when they simplify the target design, but they must be explicit in the bead close note.
- Keep live paper validation as the close condition for implementation beads.
- Do not add automated tests unless explicitly requested.
- Planned downtime is allowed while implementation beads replace core trading internals.
- Retire Rust bridge behavior in the cutover bead after Python-native validation; do not preserve it as a default fallback.
