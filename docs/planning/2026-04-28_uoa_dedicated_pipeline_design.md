# UOA Dedicated Run Design

Status: target-state design

As of: Tuesday, April 28, 2026

Related:

- [System Architecture](../current_system_state.md)
- [UOA V2 System Design](./2026-04-28_uoa_v2_system_design.md)
- [Alpaca-Only Unusual Activity Scanner Design](./unusual_activity_scanner_design.md)

## Role Of This Doc

This document defines the **smallest durable design** for giving UOA one dedicated runtime owner.

Use it for:

- replacing the temporary long-straddle-backed UOA default
- deciding how bots should influence UOA scope
- implementing one dedicated UOA run without adding new runtime layers

Use [System Architecture](../current_system_state.md) for the current runtime shape. As of April 28, 2026, UOA still runs inside strategy discovery-run cycles.

## Problem

The current UOA scanner is strategy-agnostic, but the default operator view is still backed by a strategy-owned discovery run.

That is wrong for two reasons:

- UOA is shared market-data research, not one strategy's decision layer
- the default `spreads uoa` view should not depend on whichever strategy label happened to produce the most recent useful UOA payload

## Recommendation

Do not invent a new runtime surface for this.

Instead:

- add one dedicated `discovery_run` label such as `uoa_default_weekly_auto`
- keep using the existing discovery worker queue
- keep using the existing recorder-backed UOA capture path
- let bots decide which symbols matter
- do not let bots become the direct runtime owner of the UOA job

The only new config concept should be:

- `uoa_only: true`

## Why Not "Run UOA As A Bot"

Bots are the operator and account ownership plane.

They own:

- limits
- runtime flags
- automation references
- execution-related ownership

They do not own:

- shared recorder-backed market-data collection
- discovery-run cycle persistence
- generic shared research sessions

If UOA becomes literally bot-owned, two bad outcomes follow:

1. duplicate UOA capture per bot
2. hidden dedupe or shared-session logic to avoid that duplication

The second outcome is just a discovery run with worse boundaries.

So the simple split is:

- bots tell UOA what symbols matter
- one dedicated discovery run does the actual UOA work

## Minimal Runtime Shape

One dedicated UOA run should do only this:

1. read active bot automations for the target profile
2. union their symbols into one shared UOA symbol set
3. build bounded UOA capture targets from that set
4. run the existing UOA capture path
5. persist the cycle and UOA payloads
6. emit UOA alerts only

That run should not do any strategy selection or execution work.

## Minimal Config

The dedicated config should stay plain:

```yaml
extends: short_dated_weekly
discovery_run_id: uoa_default_weekly
job_key: discovery_run:uoa_default_weekly
label: uoa_default_weekly_auto
uoa_only: true
scanner_strategy: combined
```

Notes:

- `scanner_strategy: combined` is only compatibility with the existing discovery-run surface
- `uoa_only: true` is the real semantic switch
- no new job type is needed
- no new queue is needed

## Bot Interaction

Bots should be used only as the symbol source.

Initial rule:

- for `weekly` UOA, union symbols across active entry automations whose scanner profile is `weekly`

Implementation can live in one helper such as:

- `build_uoa_symbols(profile: str) -> list[str]`

Start there.

Do not add:

- bot-specific UOA runtime ownership
- bot-specific UOA persistence
- a general scope framework

If later we need one exact bot to drive the UOA list, add that only when the simpler union rule proves insufficient.

## UOA-Only Run Behavior

When `uoa_only: true`, the discovery run should:

- resolve bot-informed symbols
- build a bounded generic UOA shortlist
- call the existing UOA capture path
- persist the cycle
- keep `uoa_summary`, `uoa_quote_summary`, and `uoa_decisions`
- allow UOA alerts

When `uoa_only: true`, the discovery run should skip:

- `select_live_opportunities(...)`
- `sync_discovery_run_signal_layer(...)`
- `sync_entry_runtime_opportunities(...)`
- `submit_auto_session_execution(...)`
- strategy opportunity alerts

This keeps UOA as research and alerting only.

## Storage And Read Models

Keep storage simple in the first cut.

Reuse:

- `job_runs`
- `discovery_runs`
- `discovery_run_events`
- persisted UOA payloads in job results

Do not add UOA-specific tables in the first implementation.

For operator reads:

- make unlabeled `spreads uoa` resolve to the latest `uoa_only` run
- leave `spreads trading` and execution views blind to `uoa_only` runs

That is enough.

## Implementation Plan

Implement in this order.

1. Add `uoa_only` to discovery-run config loading
2. Add a small helper to build UOA symbols from active bots for a profile
3. Branch early in `packages/core/services/discovery_runs/cycle.py` for `uoa_only`
4. In that branch, run only the UOA capture and persistence path
5. Add `uoa_default_weekly_auto`
6. Make unlabeled `spreads uoa` default to the latest `uoa_only` run
7. Remove the temporary long-straddle alias

## Verification

Use only narrow runtime-safe checks.

Required checks after implementation:

- `uv run spreads uoa --json`
- `uv run spreads uoa --label uoa_default_weekly_auto --json`
- `uv run spreads pipelines --json`
- confirm the dedicated run produces UOA payloads
- confirm it produces no opportunities or auto execution
- confirm `spreads trading` does not treat it as an execution lane

## Recommendation

Keep this change boring.

One dedicated discovery run.
One new flag: `uoa_only`.
Bots as symbol source only.
No new queue.
No new job type.
No new ownership framework.

That is enough to stop UOA from being strategy-owned without overengineering the fix.
