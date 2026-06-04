# Planning Docs

This directory holds architecture notes, refactor plans, and design specifications for the spreads system.

Use these entrypoints:

- [System Architecture](../current_system_state.md) as the canonical source of truth for the current overall runtime architecture and service boundaries
- [Spreads Architecture Review](./2026-06-03_spreads_architecture_review.md) for the 2026-06-03 investigation of the current Spreads runtime, strengths, risks, and engine-consolidation takeaways
- [Nautilus Architecture Review](./2026-06-03_nautilus_architecture_review.md) for the 2026-06-03 investigation of upstream Nautilus patterns and Ade's local Alpaca options runtime
- [Nautilus Patterns Inside Spreads](./2026-06-03_nautilus_patterns_inside_spreads_architecture.md) for the target architecture to house Nautilus-like engine patterns inside Spreads while moving live runtime ownership away from Rust
- [Trading Lifecycle State Machines](./2026-06-03_trading_lifecycle_state_machines.md) for the object/state lifecycle reference that should be understood before heavy trading-engine refactors
- [Target Trading Lifecycle Object Model](./2026-06-03_target_trading_lifecycle_object_model.md) for the `spr-g9s.1` decision record defining the clean target objects, states, invariants, replacement stance, and historical-data cutover posture
- [Lifecycle Contracts Module](./2026-06-03_lifecycle_contracts_module.md) for the `spr-g9s.2` implementation note covering the typed lifecycle state module and its current non-wired runtime risk
- [Lifecycle Storage Shape](./2026-06-03_lifecycle_storage_shape.md) for the `spr-g9s.3` schema note covering lifecycle fact tables, the position projection, cutover posture, and schema CLI
- [Trading Lifecycle Bead Plan](./2026-06-03_trading_lifecycle_bead_plan.md) for the ready-to-create bead breakdown, dependency order, and acceptance criteria for the lifecycle rewrite
- [Strategy Sourcing, Candidate Scanning, And Capture Architecture](./2026-06-03_strategy_sourcing_scanning_capture_architecture.md) for the target source resolver, candidate-build, strategy-entry, and capture-controller direction that replaces discovery-run ownership
- [Current-System Options Automation Implementation Approach](./2026-04-15_current_system_options_automation_implementation_approach.md) for the migration path that uses the existing backend instead of starting clean-sheet
- [Backtest System Recommendation](./2026-04-16_backtest_system_recommendation.md) for the design background behind the cutover to one canonical config-driven backtest engine
- [Spreads + Nautilus Integration Roadmap](./2026-05-16_spreads_nautilus_integration_roadmap.md) for historical May 2026 context on the bridge-first Nautilus migration path; the 2026-06-03 target direction is now [Nautilus Patterns Inside Spreads](./2026-06-03_nautilus_patterns_inside_spreads_architecture.md)
- [Multi-Strategy Builder Replay Validation](./2026-04-19_multi_strategy_builder_replay_validation.md) for the shared framework to confirm builder behavior across strategy families against scan-time artifacts and Alpaca history without adding a parallel replay subsystem
- [Config-Driven Runtime Prerequisite Plan](./2026-04-16_config_driven_runtime_prerequisite_plan.md) for the implementation architecture needed before the improved backtest is meaningful
- [Multi-Paper Alpaca Account Plan](./2026-04-27_multi_paper_alpaca_account_plan.md) for the deferred design to route bots across several Alpaca paper accounts without duplicating discovery
- [UOA V2 System Design](./2026-04-28_uoa_v2_system_design.md) for the implementation-ready Phase 1 design of the bounded Alpaca-only unusual-options-activity scanner, including payload contracts, scoring, states, compatibility, and rollout
- [UOA Dedicated Pipeline Design](./2026-04-28_uoa_dedicated_pipeline_design.md) for the simple target-state design: one dedicated discovery run for UOA, with bots used only as the symbol source
- [Company Valuation Engine V1 Spec](./2026-04-29_company_valuation_engine_v1_spec.md) for the research-backed design of the future fundamentals-and-ownership company-valuation engine, separate from the repo's existing historical backtest and outcome-evaluation surfaces
- [Company Valuation Engine V1 Implementation Contract](./2026-04-29_company_valuation_engine_v1_implementation_contract.md) for the implementation-ready payload, config, storage, and API contract for the future company-valuation engine
- [Market Intel OpenClaw Architecture](./2026-05-01_market_intel_openclaw_architecture.md) for the active target architecture that moves the thesis engine toward an OpenClaw-native `market-intel` plugin, hooks, skills, subagents, and eventual optional MCP adapter
- [Agentic Research Thesis Engine](./2026-05-01_agentic_research_thesis_engine.md) for the evidence-first research thesis concept, source policy, data-gap map, and first-implementation boundary
- [Agentic Research Thesis Engine Implementation Contract](./2026-05-01_agentic_research_thesis_engine_implementation_contract.md) for the CLI, source adapter, evidence graph, parallel stages, Ollama-backed agentic layer, and output artifact contract
- [Agentic Research Thesis Model Selection](./2026-05-01_agentic_research_thesis_model_selection.md) for the Ollama model-fit matrix, selected initial model profiles, box tuning, download policy, and fine-tuning plan
- [Company Valuation ML Template Discovery Plan](./2026-04-29_company_valuation_ml_template_discovery_plan.md) for the offline 5-year point-in-time research plan to discover sub-templates and valuation-anchor regimes without making ML the live source of truth
- [Company Valuation Industrial And Energy Clustering Findings](./2026-04-30_company_valuation_industrial_energy_clustering_findings.md) for the first 10-year offline clustering results and the candidate industrial/energy template changes they support
- [Company Valuation 30-Name Template Recalibration Findings](./2026-04-30_company_valuation_30_name_template_recalibration_findings.md) for the broader 30-name-per-industry validation pass, the research-only candidate template split, and the before/after valuation impact
- [Company Valuation Industrial Holdout Split Findings](./2026-04-30_company_valuation_industrial_holdout_split_findings.md) for the focused 19-name industrial holdout follow-up that supports an aerospace-defense versus diversified-industrial split more strongly than the broad 30-name industrial cohort
- [Company Valuation Software Sub-Template Findings](./2026-04-30_company_valuation_software_subtemplate_findings.md) for the focused software research pass, the research-only growth-transition split, and the decision to keep software promotion deferred
- [Company Valuation Software Compounder Follow-Up Findings](./2026-04-30_company_valuation_software_compounder_followup_findings.md) for the narrower 8-name software compounder pass and the conclusion that only the platform-suite branch looks directionally valid
- [Company Valuation Standards-Based Taxonomy Plan](./2026-04-30_company_valuation_standards_based_taxonomy_plan.md) for the shift from custom category invention toward public standards first, valuation-template second, and overlay-driven special situations
- [Non-Web Large File Cleanup Audit](./2026-04-16_non_web_large_file_cleanup_audit.md) for the backend cleanup audit and cutover notes around the large-file and thin-wrapper split work
- [Workspace Packages Restructure Plan](./2026-04-16_workspace_packages_restructure_plan.md) for the completed workspace move into `packages/core`, `packages/api`, `packages/web`, and `packages/config`
- [Fresh Spread Opportunity System Design](./2026-04-11_fresh_spread_system_design.md) for the target opportunity-selection architecture inside the broader system
- [Spread Selection Review And Refactor Plan](./2026-04-11_spread_selection_refactor_plan.md) for the diagnosis of the current selection path
- [Ops CLI Visibility Plan](./ops_cli_visibility_plan.md) for historical operator-tooling design context and shipped/deferred visibility notes

Implementation and evaluation companion:

- use `uv run spreads backtest run --bot-id <bot-id> --automation-id <automation-id>` for canonical historical evaluation
- use `uv run spreads backtest compare --left-json <path> --right-json <path>` for artifact-to-artifact comparison
- treat `uv run spreads analyze` as the legacy post-close report surface, not the canonical backtest/evaluation path

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

Older planning docs may still mention pre-cutover `replay`, `audit_replay`, `packages/core/cli/replay.py`, or `services/opportunity_replay.py` surfaces. Current shipped naming is `backtest` for historical evaluation and `audit_snapshot.py` behind the `spreads audit` operator view.

- [0DTE System Architecture](./0dte_system_architecture.md)
- [Trading Engine Architecture](./trading_engine_architecture.md)
- [Trading Engine Gap Plan](./trading_engine_gap_plan.md)
- [Signal State Platform](./signal_state_platform.md)
- [Unusual Activity Scanner Design](./unusual_activity_scanner_design.md)

Diagram sources live under [../diagrams/planning/](../diagrams/planning/).
