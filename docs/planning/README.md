# Planning Docs

This directory holds architecture notes, refactor plans, and design specifications for the spreads system.

Use these entrypoints:

- [Current System State](../current_system_state.md) for the runtime topology that exists in code today
- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md) for the refactor path that uses the existing backend instead of starting clean-sheet
- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md) for the current clean-sheet architecture
- [Spread Selection Review And Refactor Plan](./2026-04-11_spread_selection_refactor_plan.md) for the diagnosis of the current selection path
- [Ops CLI Visibility Plan](./ops_cli_visibility_plan.md) for shipped and deferred operator tooling

Implementation and evaluation companion:

- use `uv run spreads replay` for single-session offline decision replays
- use `uv run spreads replay recent --limit <N>` for batch policy comparison across recent sessions
- treat `uv run spreads analyze` as the legacy post-close report surface, not the canonical replay/evaluation path

Detailed design specifications:

- [Regime Detection Specification](./2026-04-11_regime_detection_spec.md)
- [Strategy Policy Matrix](./2026-04-11_strategy_policy_matrix.md)
- [Horizon Selection Specification](./2026-04-12_horizon_selection_spec.md)
- [Earnings Options Architecture](./2026-04-14_earnings_options_architecture.md)
- [Product Policy Matrix](./2026-04-12_product_policy_matrix.md)
- [Portfolio Allocation Specification](./2026-04-12_allocation_spec.md)
- [Execution Templates](./2026-04-12_execution_templates.md)
- [Evaluation And Rollout Plan](./2026-04-12_evaluation_and_rollout_plan.md)
- [Opportunity Schema](./2026-04-11_opportunity_schema.md)

Legacy or earlier architecture context:

These are historical context unless a task explicitly names one of them as the active source of truth.

- [0DTE System Architecture](./0dte_system_architecture.md)
- [Trading Engine Architecture](./trading_engine_architecture.md)
- [Trading Engine Gap Plan](./trading_engine_gap_plan.md)
- [Signal State Platform](./signal_state_platform.md)
- [Unusual Activity Scanner Design](./unusual_activity_scanner_design.md)

Diagram sources live under [../diagrams/planning/](../diagrams/planning/).
