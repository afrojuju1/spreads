# Spreads + Nautilus Integration Roadmap

Status: proposed

Related:

- [Current System State](../current_system_state.md)
- [Trading Engine Architecture](./trading_engine_architecture.md)
- [Backtest System Recommendation](./2026-04-16_backtest_system_recommendation.md)

## Goal

Use `spreads` as the operator-facing product, control plane, and persistent state system, while progressively moving reusable trading-engine responsibilities into Nautilus.

The target is not a rewrite. The target is a controlled migration where `spreads` keeps ownership of:

- bots, automations, universes, and product policy
- scanner and opportunity generation
- Postgres read models, audit, alerts, operator UI, and CLI
- session/day ownership and human-facing trade attribution

Nautilus should become the preferred owner for:

- typed order and execution primitives
- broker-facing option order submission where supported
- deterministic backtest/live execution semantics
- reusable market-data, option-chain, cache, portfolio, and risk primitives
- engine-level invariants and conformance tests

## Guiding Principles

1. Keep `spreads` as the product shell.
2. Keep signal truth, execution truth, and session ownership separate.
3. Integrate by versioned contracts first, shared process later.
4. Migrate one vertical slice at a time.
5. Treat every Nautilus-routed order path as fail-closed.
6. Preserve operator visibility before increasing automation.
7. Upstream generic Nautilus improvements; keep product-specific workflow in `spreads`.

## Current Baseline

Already in place:

- `execution_intents` selects the execution runtime before broker submission.
- `alpaca_direct` remains the default runtime for unmigrated automations.
- `nautilus` runtime builds a Nautilus `SubmitOrderList` handoff.
- The bridge command is `alpaca-submit-order-list-bridge`.
- Migrated two-leg vertical entry automations include index call/put credit and earnings call/put debit entries.
- `spreads` persists returned broker parent/nested order snapshots into the execution ledger.
- The bridge path fails closed on missing pricing, missing command, invalid output, or broker rejection.

This roadmap assumes that baseline remains the compatibility boundary until a better integration mechanism is proven.

## Phase 1: Harden The Handoff Contract

Priority: highest

Purpose: make the current bridge path boring before adding more runtime surface area.

Deliverables:

- Add a versioned handoff schema for Nautilus `SubmitOrderList` payloads.
- Add golden contract tests for every currently Nautilus-routed strategy family.
- Add invalid-payload tests for missing leg quotes, wrong leg count, zero net limit, bad side, and impossible credit/debit pricing.
- Add response schema tests for accepted, rejected, invalid-output, timeout, and command-not-found outcomes.
- Add explicit correlation fields: `execution_attempt_id`, `order_list_id`, `parent_client_order_id`, strategy family, automation id, bot id, and policy versions.
- Persist bridge command version/build metadata when available.
- Surface bridge readiness in `spreads trading`, API health, and the operator dashboard.

Exit criteria:

- A developer can change either repo and immediately know if the handoff contract broke.
- A live operator can see whether Nautilus submission is available before enabling an automation.
- No bridge failure can silently fall back to direct Alpaca submission.

## Phase 2: Complete Nautilus-Routed Order Lifecycle

Priority: high

Purpose: make Nautilus-routed entries operationally complete after submission.

Deliverables:

- Normalize parent and leg order ids from Nautilus/Alpaca into `execution_orders`.
- Ensure fill ingestion and `broker_sync` refresh non-terminal Nautilus-routed attempts consistently.
- Make close/cancel/refresh behavior explicit for Nautilus-routed positions.
- Add tests that prove `session_positions` remains the session/day ownership source of truth even when broker reconciliation repairs order/fill state.
- Add operator views that distinguish:
  - `spreads` intent state
  - Nautilus bridge submission state
  - Alpaca broker order state
  - `spreads` session position state
- Add replayable audit payloads for bridge requests and sanitized bridge responses.

Exit criteria:

- A Nautilus-routed position can be followed from opportunity to intent, order, fill, position, close, and audit without reading logs.
- Broker reconciliation repairs execution facts without reassigning session ownership.

## Phase 3: Expand Strategy Coverage Carefully

Priority: high after Phase 2

Purpose: migrate more structure types only after the two-leg vertical lifecycle is proven.

Order of migration:

1. Two-leg vertical entries already selected for Nautilus.
2. Two-leg vertical closes for those positions.
3. Iron condor entries and closes.
4. Long straddle and long strangle entries and closes.
5. Short single-leg strategies only after margin and assignment behavior is explicitly gated.

Deliverables:

- Structure-specific handoff tests for 2-leg, 4-leg, and long-vol structures.
- Strategy-family capability flags in config.
- A single runtime capability resolver used by scanner, execution, and UI.
- Per-family paper proof records before default-enabling Nautilus routing.

Exit criteria:

- No strategy family can opt into Nautilus routing without a capability declaration, contract test, and paper proof note.

## Phase 4: Deterministic Replay And Backtest Parity

Priority: medium-high

Purpose: reduce divergence between scanner evaluation, paper execution, and live behavior.

Deliverables:

- Export discovery-cycle candidates and option-leg market data into a stable replay dataset.
- Build a Nautilus backtest adapter for selected `spreads` candidates.
- Compare `spreads` backtest output against Nautilus execution simulation for the same candidate set.
- Add artifact comparison for:
  - entry credit/debit
  - fill assumptions
  - max profit/loss
  - stop/target behavior
  - close mark
  - realized PnL
- Add regression datasets for the main strategy families.

Exit criteria:

- A candidate can be evaluated through both the current `spreads` backtest path and Nautilus simulation with explainable differences.
- Replay results can be linked from opportunity/session detail pages.

## Phase 5: Shared Market-Data And Option-Chain Primitives

Priority: medium

Purpose: avoid two separate option-chain worlds.

Deliverables:

- Map `spreads` option snapshot and quote/trade records to Nautilus instrument and data types.
- Evaluate Nautilus option-chain aggregation for targeted quote watch and UOA enrichment.
- Keep `market_recorder.py` as the only normal Alpaca option websocket owner until a replacement is proven.
- Add a documented cutover plan before changing websocket ownership.
- Define a cache boundary: what lives in Nautilus cache, what remains in Postgres, and what is materialized for UI reads.

Exit criteria:

- `spreads` can consume Nautilus-normalized option data without losing current Postgres auditability.
- There is no duplicate live Alpaca option websocket owner in normal runtime.

## Phase 6: Risk And Policy Convergence

Priority: medium

Purpose: keep business policy in `spreads` while using Nautilus where engine-level validation is stronger.

Deliverables:

- Keep `spreads` risk policy as the account/product decision layer.
- Map accepted `spreads` execution intents into Nautilus risk/order constraints.
- Persist both decisions when both layers evaluate a submission.
- Add conformance tests for quote freshness, max loss, buying power, position caps, duplicate underlying/strategy limits, and kill-switch behavior.
- Make denied orders easy to attribute to either `spreads_policy` or `nautilus_engine`.

Exit criteria:

- Operators can see whether a block came from product policy, engine validation, or broker rejection.
- Engine-level validation cannot loosen a stricter `spreads` policy.

## Phase 7: Integration Mechanism Upgrade

Priority: medium-low, after contract maturity

Purpose: replace the subprocess bridge only when the contract is stable.

Options to evaluate:

- Keep the subprocess bridge if it remains reliable and observable.
- Move to a long-lived local sidecar with a health endpoint and version endpoint.
- Expose a Python/PyO3 in-process API for the required Nautilus order-list operations.
- Use a narrow RPC boundary if deployment isolation is more valuable than in-process speed.

Decision criteria:

- failure isolation
- deploy simplicity on `ade-nucbox-k8-plus`
- startup time and request latency
- log/audit quality
- schema compatibility
- testability in CI

Exit criteria:

- The selected mechanism improves operability without widening the behavioral surface area.

## Phase 8: Upstream And Repo Ownership

Priority: ongoing

Purpose: keep the fork maintainable.

Rules:

- Generic Alpaca adapter fixes belong in the Nautilus fork and should be prepared for upstream PRs.
- Product-specific scanner, dashboard, automation, and Discord behavior stays in `spreads`.
- Shared schema changes must be documented in both repos before deploy.
- Avoid making Nautilus depend on `spreads` concepts such as bots, automation ids, Discord alerts, or operator read models.
- Avoid making `spreads` duplicate Nautilus engine behavior once a Nautilus path is proven.

## Near-Term Implementation Sequence

1. Add schema and golden tests for `build_nautilus_submit_order_list_handoff`.
2. Add bridge response schema tests around `submit_nautilus_order_list`.
3. Add bridge readiness to CLI/API/UI health.
4. Add execution ledger assertions for Nautilus parent and leg order snapshots.
5. Add operator audit view fields for handoff request/response metadata.
6. Prove one full paper lifecycle for `index_put_credit_entry`.
7. Prove one full paper lifecycle for `index_call_credit_entry`.
8. Decide whether vertical closes should route through Nautilus before adding 4-leg structures.

## Open Decisions

- Should the first non-subprocess integration be sidecar RPC or PyO3 in-process?
- Which Postgres table should own sanitized bridge request/response audit payloads long term?
- Should `execution_intents` store the full handoff payload or only a hash plus artifact reference?
- Should Nautilus simulation become the canonical backtest engine, or remain a conformance companion until parity is proven?
- What is the minimum paper proof sample size before enabling Nautilus routing by default for a strategy family?

## Definition Of Done

This roadmap is complete when:

- `spreads` remains the operator and product control plane.
- Nautilus owns broker-facing order semantics for all supported option structures.
- Backtest, paper, and live execution share a tested contract.
- Operators can audit every Nautilus-routed trade from signal to broker response and final session PnL.
- The Nautilus fork contains only reusable engine/adapter work, not `spreads` product policy.
