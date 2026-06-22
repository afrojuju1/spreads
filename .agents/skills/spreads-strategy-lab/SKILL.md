---
name: spreads-strategy-lab
description: Evidence-first strategy research and catalog/profile workflow for Spreads. Use for strategy tuning, new strategy families, quality profiles, catalog entries, or explaining why a strategy selected, skipped, or underperformed.
---

# Spreads Strategy Lab

Use this skill from `/home/ade/Projects/spreads` when work touches:

- strategy design or tuning
- quality profiles or entry thresholds
- strategy catalog entries
- new strategy families
- why a strategy selected, skipped, or underperformed on a market date
- offline research that should become a clean runtime path

If the system is currently broken, start with `spreads-ops`. If the question is storage, capture pressure, data quality, or retention, use `spreads-data-platform`.

## Source Material

Read the current architecture first:

- [docs/current_system_state.md](../../../docs/current_system_state.md)
- [docs/planning/2026-06-11_strategy_archetype_profile_contract.md](../../../docs/planning/2026-06-11_strategy_archetype_profile_contract.md)
- [docs/planning/2026-06-08_trading_engine_inspiration_repos.md](../../../docs/planning/2026-06-08_trading_engine_inspiration_repos.md)

Use the shipped runtime evidence before proposing changes:

```bash
uv run spreads config validate --json
uv run spreads ops strategy-ledger --date YYYY-MM-DD --json
uv run spreads execution list --date YYYY-MM-DD --json
uv run spreads ops state --json
```

## Runtime Config Rules

- The runtime strategy config is the catalog/profile model under `packages/config/strategies/`.
- `packages/config/strategies/catalog.yaml` owns strategy entries, activation, execution mode, and profile references.
- `packages/config/strategies/profiles.yaml` owns reusable structure, quality, portfolio/protection, and executor lifecycle profiles.
- `execution.mode` is the paper, shadow, or live switch. Do not create paper-specific strategy files.
- Executor profiles own broker order style, quote freshness, submit TTL, stale-order handling, and reprice/cancel policy. Do not put broker-order mechanics into strategy selection, quality profiles, or exit controllers.
- Do not reintroduce per-strategy runtime YAML, sidecar compatibility config, or duplicate loader paths.
- Preserve `trading_strategy_id` unless the work intentionally creates a new strategy identity.
- Use `config_hash` and strategy ledger evidence when comparing behavior.

## Workflow

1. Scope the strategy, market date, symbol set, and exact question.
2. Collect source, candidate, signal, decision, admission, intent, attempt, fill, position, and close evidence.
3. In `spreads ops strategy-ledger`, read `candidates.candidate_productivity_state`, `diagnostic_status_counts`, raw/postprocess/runtime/returned candidate counts, `feature_snapshot_count`, `feature_quality_status_counts`, `market_data_quality_state_counts`, `top_market_data_quality_reasons`, and `market_data_quality_component_state_counts` before tuning.
4. Separate no raw candidates from postprocess/ranking filtering, persisted feature or market-data SLA blockers, and selected candidates that later fail admission or execution.
5. Classify the blocker as data completeness, option-chain viability, feature/SLA quality, structure filter, quality filter, strategy selection, protection admission, portfolio admission, execution admission, executor lifecycle policy, dispatch/broker submission, or management/exit logic.
6. Propose the smallest catalog/profile/runtime change that fixes the real blocker.
7. Validate config before rollout:

```bash
uv run spreads config validate --json
```

8. If code or live config changed, hand off to `spreads-live-rollout` for Docker restart and live verification.

## Research Bar

Do not tune from vibes. A strategy recommendation should name:

- the market date or date range
- current raw/postprocess/runtime/returned, selected/rejected, and persisted trade-candidate counts
- persisted feature snapshot counts and market-data SLA states/reasons
- top blockers and thresholds
- expected effect of the change
- downside risk
- validation command
- whether it changes live, paper, or shadow behavior

Use notebooks or scratch scripts only for exploration. Do not turn them into a second runtime, and do not commit exploratory artifacts unless Ade asks.

## Clean-Cut Rules

- If a target model replaces an old strategy config path, delete the old path and update callers.
- Do not keep transitional references in shipped runtime.
- Avoid vendor-led names unless the vendor is the real owner of the concept.
- Keep scanner/source truth, candidate truth, strategy decision truth, protection admission, portfolio admission, execution admission, and executor lifecycle policy separate.
- Use `trading_feature_snapshots` as the point-in-time feature fact store. Strategy tuning should read stored feature/SLA labels instead of calling providers to reinterpret past entry context.
- Alerts are projections of stored decisions and execution state, not the source of truth.

## Response Shape

Report the evidence first, then the recommendation:

1. strategy IDs and dates inspected
2. selected, rejected, admitted, blocked, attempted, filled, and closed counts
3. current blocker classification
4. proposed catalog/profile/code change
5. validation performed or still needed
6. live rollout impact
