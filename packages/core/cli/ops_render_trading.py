from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from core.cli.ops_render_helpers import (
    STATUS_STYLES,
    _render_attention,
    _render_count_map,
    _render_duration,
    _render_engine_summary,
    _render_group_labels,
    _render_money,
    _render_percent,
    _render_quality_waterfall_summary,
    _render_source_state,
    _render_value,
    _status_text,
    _truncate,
)


def render_trading_ops_state(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    account_snapshot = dict(details.get("account_snapshot") or {})
    account = dict(account_snapshot.get("account") or {})
    pnl = dict(account_snapshot.get("pnl") or {})
    schedules = dict(details.get("schedules") or {})
    broker_sync = dict(details.get("broker_sync") or {})
    broker_exposure = dict(details.get("broker_exposure") or {})
    market_context = dict(details.get("market_context") or {})
    market_context_regime = dict(market_context.get("regime") or {})
    market_context_evidence = dict(market_context.get("major_evidence") or {})
    strategy_breadth = dict(details.get("strategy_breadth") or {})
    strategy_breadth_summary = dict(strategy_breadth.get("summary") or {})
    alert_delivery = dict(details.get("alert_delivery") or {})
    execution_health = dict(details.get("execution_health") or {})
    execution_contract = dict(details.get("execution_contract") or {})
    primary_execution_contract = dict(execution_contract.get("primary_strategy_contract") or {})
    latest_lifecycle_evidence = dict(execution_contract.get("latest_lifecycle_evidence") or {})
    natural_evidence = dict(latest_lifecycle_evidence.get("natural_strategy") or {})
    synthetic_evidence = dict(latest_lifecycle_evidence.get("synthetic_validation") or {})
    mark_health = dict(details.get("mark_health") or {})
    engine = dict(details.get("engine") or {})
    engine_summary = dict(engine.get("summary") or {})

    def _breadth_summary_value(key: str) -> Any:
        if key in summary:
            return summary.get(key)
        return strategy_breadth_summary.get(key)

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Trading Allowed", "yes" if summary.get("trading_allowed") else "no")
    overview.add_row(
        "Market",
        (
            f"{_render_value(summary.get('market_session_status'))} "
            f"{_render_value(summary.get('market_open_at'))}"
            f"..{_render_value(summary.get('market_close_at'))}"
        ),
    )
    overview.add_row(
        "Market Context",
        (
            f"{_render_value(summary.get('market_context_status'))}/"
            f"{_render_value(summary.get('market_context_state'))} | "
            f"{_render_value(summary.get('market_context_regime_label'))} "
            f"{_render_value(summary.get('market_context_risk_posture'))} | "
            f"conf {_render_value(summary.get('market_context_confidence'))}"
        ),
    )
    overview.add_row("Broker Env", _render_value(summary.get("broker_environment")))
    overview.add_row("Control", _render_value(summary.get("control_mode")))
    overview.add_row(
        "Execution Mode",
        (
            f"{_render_value(summary.get('execution_posture') or primary_execution_contract.get('execution_posture'))} | "
            f"{_render_value(summary.get('approval_mode') or primary_execution_contract.get('approval_mode'))} | "
            f"{_render_value(summary.get('execution_runtime') or primary_execution_contract.get('execution_runtime'))}"
        ),
    )
    overview.add_row(
        "Mode Contract",
        (
            f"{_render_value(summary.get('execution_contract_status') or execution_contract.get('status'))} | "
            f"compatible {_render_value(summary.get('environment_compatible'))} | "
            f"{_render_value(summary.get('environment_mismatch_reason'))}"
        ),
    )
    overview.add_row(
        "Lifecycle Proof",
        (f"natural {_render_value(natural_evidence.get('observed_at'))} | synthetic {_render_value(synthetic_evidence.get('observed_at'))}"),
    )
    overview.add_row("Entry Posture", _truncate(summary.get("primary_entry_message"), length=96))
    overview.add_row(
        "Strategy Breadth",
        (
            f"active {_render_value(_breadth_summary_value('active_strategy_count'))} | "
            f"available {_render_value(_breadth_summary_value('available_strategy_count'))} | "
            f"shadow {_render_value(_breadth_summary_value('available_shadow_strategy_count'))} | "
            f"paper {_render_value(_breadth_summary_value('available_paper_strategy_count'))}"
        ),
    )
    overview.add_row(
        "Schedules",
        (
            f"{_render_value(schedules.get('status'))} | "
            f"enabled {_render_value(schedules.get('enabled_schedule_count'))}/"
            f"{_render_value(schedules.get('declared_schedule_count'))}"
        ),
    )
    overview.add_row(
        "Workflow Lanes",
        (
            f"configured {_render_value(summary.get('workflow_lane_count'))} | "
            f"disabled {_render_value(summary.get('disabled_workflow_lane_count'))} | "
            f"blocked {_render_value(summary.get('blocked_workflow_lane_count'))} | "
            f"idle {_render_value(summary.get('idle_workflow_lane_count'))}"
        ),
    )
    overview.add_row(
        "Jobs",
        (
            f"running {_render_value(len(list(details.get('running_jobs') or [])))} | "
            f"queued {_render_value(len(list(details.get('queued_jobs') or [])))} | "
            f"failed {_render_value(summary.get('actionable_failed_job_count'))}"
        ),
    )
    overview.add_row("Equity", _render_money(account.get("equity")))
    overview.add_row("Cash", _render_money(account.get("cash")))
    overview.add_row("Buying Power", _render_money(account.get("buying_power")))
    overview.add_row("Day PnL", _render_money(pnl.get("day_change")))
    overview.add_row("Day PnL %", _render_percent(pnl.get("day_change_percent")))
    overview.add_row(
        "Positions",
        (
            f"{_render_value(summary.get('open_position_count'))}/"
            f"{_render_value(summary.get('max_open_positions'))} open | "
            f"closed {_render_value(summary.get('closed_position_count'))}"
        ),
    )
    overview.add_row(
        "Broker Exposure",
        (
            f"options {_render_value(summary.get('broker_option_position_count'))} | "
            f"managed {_render_value(summary.get('spreads_managed_broker_option_position_count'))} | "
            f"external {_render_value(summary.get('external_manual_broker_option_position_count'))}"
        ),
    )
    overview.add_row(
        "Entries",
        (
            f"{_render_value(summary.get('session_entry_count'))}/"
            f"{_render_value(summary.get('max_daily_entries'))} filled | "
            f"remaining {_render_value(summary.get('remaining_daily_entries'))}"
        ),
    )
    overview.add_row(
        "Execution",
        (
            f"{_render_value(summary.get('execution_health_status'))} | "
            f"open {_render_value(summary.get('open_execution_count'))} | "
            f"stale {_render_value(execution_health.get('stale_open_execution_count'))} | "
            f"unknown-submit {_render_value(execution_health.get('submit_unknown_execution_count'))}"
        ),
    )
    overview.add_row(
        "Marks",
        (
            f"{_render_value(summary.get('mark_health_status'))} | "
            f"missing {_render_value(mark_health.get('missing_mark_count'))} | "
            f"stale {_render_value(mark_health.get('stale_mark_count'))}"
        ),
    )
    overview.add_row(
        "Engine",
        (
            f"ticker sources {_render_value(summary.get('engine_ticker_source_run_count'))} | "
            f"candidates {_render_value(summary.get('engine_trade_candidate_count'))} | "
            f"signals {_render_value(summary.get('engine_signal_count'))} | "
            f"decisions {_render_value(summary.get('engine_decision_count'))} | "
            f"selected {_render_value(summary.get('engine_selected_count'))} | "
            f"capture {_render_value(summary.get('capture_active_target_count'))}"
        ),
    )
    overview.add_row(
        "Broker Sync",
        f"{_render_value(broker_sync.get('status'))} @ {_render_value(broker_sync.get('updated_at'))}",
    )
    overview.add_row(
        "Alerts",
        f"dead-letter {_render_value(alert_delivery.get('dead_letter_count'))} | retry {_render_value(alert_delivery.get('retry_wait_count'))}",
    )
    overview.add_row("Latest Exit", _render_value(summary.get("latest_exit_reason")))
    overview.add_row("Net PnL", _render_money(summary.get("net_pnl")))
    console.print(
        Panel(
            overview,
            title="Trading Ops State",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    _render_engine_summary(
        console,
        title="Engine Spine",
        value=engine_summary,
    )

    if market_context:
        table = Table(title="Market Context", header_style="bold")
        table.add_column("Snapshot", max_width=34, overflow="ellipsis", no_wrap=True)
        table.add_column("State")
        table.add_column("Regime")
        table.add_column("Risk")
        table.add_column("Trend")
        table.add_column("Vol")
        table.add_column("Fresh")
        table.add_column("Benchmarks", max_width=36, overflow="ellipsis", no_wrap=True)
        table.add_row(
            _render_value(market_context.get("market_context_snapshot_id")),
            f"{_render_value(market_context.get('status'))}/{_render_value(market_context.get('state'))}",
            _render_value(market_context_regime.get("regime_label")),
            _render_value(market_context_regime.get("risk_posture")),
            _render_value(market_context_regime.get("trend_strength")),
            _render_value(market_context_regime.get("volatility_state")),
            f"{_render_value(market_context.get('age_seconds'))}s old",
            (
                f"obs {_render_value(market_context_evidence.get('observed_benchmark_count'))}/"
                f"{_render_value(market_context_evidence.get('expected_benchmark_count'))} | "
                f"support {_render_value(market_context_evidence.get('supportive_benchmark_count'))} | "
                f"block {_render_value(market_context_evidence.get('blocking_benchmark_count'))}"
            ),
        )
        console.print(table)

    strategy_breadth_rows = list(strategy_breadth.get("strategies") or [])
    if strategy_breadth_rows:
        table = Table(title="Strategy Breadth", header_style="bold")
        table.add_column("Strategy", max_width=28, overflow="ellipsis")
        table.add_column("Posture", max_width=28, overflow="ellipsis")
        table.add_column("Source", max_width=18, overflow="ellipsis")
        table.add_column("Evidence", max_width=18, overflow="ellipsis")
        table.add_column("Reason", max_width=22, overflow="ellipsis")
        for row in strategy_breadth_rows:
            source = dict(row.get("source") or {})
            entry = dict(row.get("entry") or {})
            entry_schedule = dict(entry.get("schedule") or {})
            observation = dict(row.get("latest_observation") or {})
            table.add_row(
                f"{str(row.get('trading_strategy_id') or row.get('name') or '-')}\n{_render_value(row.get('trade_structure'))}",
                (f"{_render_value(row.get('ops_posture'))}\n{_render_value(row.get('execution_mode'))}/{_render_value(row.get('approval_mode'))}"),
                f"{_render_value(source.get('ref'))}\n{_render_value(entry_schedule.get('cadence'))} {'on' if entry.get('enabled') else 'off'}",
                (
                    f"{_render_value(observation.get('candidate_count'))} cand / "
                    f"{_render_value(observation.get('signal_count'))} sig\n"
                    f"{_render_value(observation.get('entry_run_mode') or observation.get('status'))}"
                ),
                _render_value(row.get("not_active_reason")),
            )
        console.print(table)

    strategy_contracts = list(execution_contract.get("strategy_contracts") or [])
    if strategy_contracts:
        table = Table(title="Execution Contract", header_style="bold")
        table.add_column("Strategy")
        table.add_column("Posture")
        table.add_column("Approval")
        table.add_column("Runtime")
        table.add_column("Broker Env")
        table.add_column("Compatible")
        table.add_column("Mismatch", max_width=32, overflow="ellipsis", no_wrap=True)
        for row in strategy_contracts:
            contract = dict(row or {})
            table.add_row(
                str(contract.get("trading_strategy_id") or "-"),
                _render_value(contract.get("execution_posture")),
                _render_value(contract.get("approval_mode")),
                _render_value(contract.get("execution_runtime")),
                _render_value(contract.get("broker_environment")),
                _render_value(contract.get("environment_compatible")),
                _render_value(contract.get("environment_mismatch_reason")),
            )
        console.print(table)

    flow_rows = list(details.get("trading_flows") or [])
    if flow_rows:
        no_entry_rows = [row for row in details.get("strategy_no_entry_summary") or [] if isinstance(row, dict)]
        if no_entry_rows:
            table = Table(title="Strategy No-Entry Summary", header_style="bold")
            table.add_column("Strategy", max_width=26, overflow="ellipsis", no_wrap=True)
            table.add_column("Kind", min_width=6, max_width=10, overflow="ellipsis", no_wrap=True)
            table.add_column("Why", max_width=34, overflow="ellipsis", no_wrap=True)
            table.add_column("Codes", min_width=5, max_width=24, overflow="ellipsis", no_wrap=True)
            table.add_column("Context", max_width=28, overflow="ellipsis", no_wrap=True)
            for row in no_entry_rows:
                message = str(row.get("message") or row.get("reason") or row.get("state") or "-")
                table.add_row(
                    str(row.get("trading_strategy_id") or "-"),
                    _render_value(row.get("category")),
                    message,
                    _render_count_map(row.get("top_reason_codes"), limit=3, item_length=72),
                    (f"{_render_value(row.get('market_context_regime_label'))}/{_render_value(row.get('market_context_risk_posture'))}"),
                )
            console.print(table)

        posture_rows = [row for row in flow_rows if isinstance(row.get("entry_posture"), dict)]
        if posture_rows:
            table = Table(title="Entry Posture", header_style="bold")
            table.add_column("Strategy")
            table.add_column("State")
            table.add_column("Message", max_width=72, overflow="ellipsis")
            table.add_column("Top Groups", max_width=52, overflow="ellipsis", no_wrap=True)
            for row in posture_rows:
                entry_posture = dict(row.get("entry_posture") or {})
                table.add_row(
                    str(row.get("trading_strategy_id") or row.get("name") or "-"),
                    _render_value(entry_posture.get("state")),
                    _render_value(entry_posture.get("message")),
                    _render_group_labels(entry_posture.get("blocker_groups"), limit=3, item_length=72),
                )
            console.print(table)

        protection_rows = [
            row
            for row in flow_rows
            if isinstance(row.get("protection_admission"), dict)
            and dict(row.get("protection_admission") or {}).get("status") not in {None, "", "not_evaluated"}
        ]
        if protection_rows:
            table = Table(title="Protection Admission", header_style="bold")
            table.add_column("Strategy")
            table.add_column("Status")
            table.add_column("Reason", max_width=40, overflow="ellipsis", no_wrap=True)
            table.add_column("Blockers", max_width=52, overflow="ellipsis", no_wrap=True)
            for row in protection_rows:
                protection = dict(row.get("protection_admission") or {})
                table.add_row(
                    str(row.get("trading_strategy_id") or row.get("name") or "-"),
                    _status_text(protection.get("status")),
                    _render_value(protection.get("reason")),
                    _render_group_labels(protection.get("blockers"), limit=3, item_length=72),
                )
            console.print(table)

        table = Table(title="Trading Flows", header_style="bold")
        table.add_column("Strategy")
        table.add_column("Status")
        table.add_column("Ticker Source")
        table.add_column("Symbols", justify="right")
        table.add_column("Candidates", justify="right")
        table.add_column("Blockers", max_width=34, overflow="ellipsis", no_wrap=True)
        table.add_column("Active Intents", justify="right")
        table.add_column("Positions")
        table.add_column("Capacity")
        for row in flow_rows:
            source_state = dict(row.get("source_state") or {})
            candidate_state = dict(row.get("candidate_state") or {})
            intent_state = dict(row.get("intent_state") or {})
            position_state = dict(row.get("position_state") or {})
            capacity = dict(row.get("capacity") or {})
            table.add_row(
                str(row.get("trading_strategy_id") or row.get("name") or "-"),
                _status_text(row.get("status")),
                _render_source_state(source_state),
                _render_value(source_state.get("symbol_count")),
                (f"{_render_value(candidate_state.get('candidate_count'))} ({_render_value(candidate_state.get('diagnostic_status'))})"),
                _render_count_map(
                    candidate_state.get("top_rejection_counts"),
                    limit=3,
                    item_length=52,
                    normalize_names=True,
                ),
                _render_value(intent_state.get("active_intent_count")),
                (
                    f"{_render_value(position_state.get('open_position_count'))} open | "
                    f"{_render_value(position_state.get('closed_position_count'))} closed"
                ),
                (
                    f"{_render_value(capacity.get('session_entry_count'))}/"
                    f"{_render_value(capacity.get('max_daily_entries'))} entries | "
                    f"{_render_value(capacity.get('open_position_count'))}/"
                    f"{_render_value(capacity.get('max_open_positions'))} open"
                ),
            )
        console.print(table)
        _render_quality_waterfall_summary(console, flow_rows)

    broker_positions = list(broker_exposure.get("positions") or [])
    if broker_positions:
        table = Table(title="Broker Exposure Ownership", header_style="bold")
        table.add_column("Symbol")
        table.add_column("Asset")
        table.add_column("Side")
        table.add_column("Qty", justify="right")
        table.add_column("Market Value", justify="right")
        table.add_column("Ownership")
        table.add_column("Spreads Position")
        for row in broker_positions:
            table.add_row(
                str(row.get("symbol") or "-"),
                str(row.get("asset_class") or "-"),
                str(row.get("side") or "-"),
                _render_value(row.get("qty")),
                _render_money(row.get("market_value")),
                str(row.get("ownership") or "-"),
                str(row.get("spreads_position_id") or "-"),
            )
        console.print(table)

    top_positions = list(details.get("top_positions") or [])
    if top_positions:
        table = Table(title="Top Positions", header_style="bold")
        table.add_column("Session")
        table.add_column("Underlying")
        table.add_column("Status")
        table.add_column("Exposure", justify="right")
        table.add_column("Net PnL", justify="right")
        table.add_column("Risk")
        for row in top_positions:
            table.add_row(
                str(row.get("session_id") or "-"),
                str(row.get("underlying_symbol") or "-"),
                str(row.get("status") or "-"),
                _render_money(row.get("exposure")),
                _render_money(row.get("net_pnl")),
                str(row.get("risk_status") or "-"),
            )
        console.print(table)

    open_attempts = list(details.get("open_execution_attempts") or [])
    if open_attempts:
        table = Table(title="Open Executions", header_style="bold")
        table.add_column("Session")
        table.add_column("Underlying")
        table.add_column("Intent")
        table.add_column("Status")
        table.add_column("Phase")
        table.add_column("Age")
        table.add_column("Next")
        for row in open_attempts[:8]:
            table.add_row(
                str(row.get("session_id") or "-"),
                str(row.get("underlying_symbol") or "-"),
                str(row.get("trade_intent") or "-"),
                str(row.get("status") or "-"),
                str(row.get("lifecycle_phase") or "-"),
                _render_duration(row.get("age_seconds")),
                str(row.get("next_action") or "-"),
            )
        console.print(table)


__all__ = ["render_trading_ops_state"]
