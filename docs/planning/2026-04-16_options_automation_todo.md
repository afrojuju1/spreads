# Options Automation TODO

Status: complete for active short-dated index rollout

As of: Sunday, April 19, 2026

## Active Rollout Complete

- [x] Add phase-1 replay validation for `call_credit_spread`, including exact stored-artifact replay and reduced-fidelity Alpaca historical range replay. See [Multi-Strategy Builder Replay Validation](./2026-04-19_multi_strategy_builder_replay_validation.md).
- [x] Prove the `call_credit_spread` lane on the canonical runtime path with deterministic end-to-end lifecycle coverage: opportunity scoring, entry recipe decision, execution intent linkage, submit payload shaping, fill handling, position creation, management, and close.
- [x] Review selection thresholds, sizing, and risk caps for the active short-dated premium lanes using replay and paper observations. Result: `call_credit_spread` and `put_credit_spread` require no additional threshold or sizing changes; `iron_condor` promotion floor is `71.5`.
- [x] Define the paper-to-live promotion checklist for each active bot, including explicit go/no-go criteria, manual approval expectations, and required safety limits.
- [x] Make `iron_condor` runtime-ready by adding generic multi-leg live deployment validation, condor-aware exposure math, and condor-aware risk fallback / fail-closed behavior.
- [x] Prove the `iron_condor` lane through a full paper lifecycle once the shared runtime gaps are fixed.
- [x] Get the new `iron_condor` lane to its first paper fill and verify position creation, management, repricing, and close behavior end to end.

## Paper-To-Live Promotion Checklist

- `call_credit_spread`: keep `live_enabled: false` until explicit manual approval. Require passing replay validation, passing deterministic lifecycle e2e coverage, no replay sample showing systematic near-floor scoring misses, and unchanged safety limits of `max_risk_per_trade: 500`, `max_open_positions: 3`, `max_daily_actions: 6`, and `daily_loss_limit: 250`.
- `put_credit_spread`: keep `live_enabled: false` until explicit manual approval. Require passing replay validation, persisted paper open/close evidence, stable management exits, and the same bot and trade risk limits already configured.
- `iron_condor`: keep `live_enabled: false` until explicit manual approval. Require passing replay validation, persisted paper open/close evidence, the tactical promotion floor of `71.5`, and the same bot and trade risk limits already configured.
- Promotion is per bot and manual. There is no automatic paper-to-live flip based on a paper metric threshold alone.

Current paper-runtime evidence as of Sunday, April 19, 2026 evening:

- `call_credit_spread`: no persisted paper positions yet under `short_dated_index_call_credit_bot`, but this is now a market-observation gap rather than an engineering blocker. Replay validation and deterministic lifecycle coverage both pass.
- `put_credit_spread`: persisted paper lifecycle evidence exists under `short_dated_index_credit_bot`, with 3 closed positions and filled open/close attempts.
- `iron_condor`: persisted paper lifecycle evidence now exists under `short_dated_index_iron_condor_bot`, with 1 closed paper position opened and closed on April 16, 2026. Remaining monitoring is observational rather than a code or rollout blocker.

## Future Strategy Backlog

- Design canonical support for naked short calls and puts, including family modeling, undefined-risk controls, margin-aware sizing, and a separate live validation path.
- Add butterfly family support end to end: canonical family modeling, builder/runtime support, exposure math, and lifecycle validation.
