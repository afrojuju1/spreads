from __future__ import annotations

import json
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

STATUS_STYLES = {
    "healthy": "green",
    "degraded": "yellow",
    "blocked": "red",
    "halted": "bold red",
    "idle": "cyan",
    "unknown": "magenta",
}


def build_console(*, no_color: bool) -> Console:
    return Console(no_color=no_color)


def _status_text(status: str | None) -> Text:
    normalized = str(status or "unknown").strip().lower()
    return Text(normalized.upper(), style=STATUS_STYLES.get(normalized, "white"))


def _render_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _render_money(value: Any) -> str:
    if value is None:
        return "-"
    return f"${float(value):,.2f}"


def _render_bytes(value: Any) -> str:
    if value is None:
        return "-"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TB"


def _render_entry_budget(value: Any, *, fallback_limit: Any = None) -> str:
    if not isinstance(value, dict):
        return "-"
    limit = value.get("max_daily_entries", fallback_limit)
    used = value.get("used_entry_count", value.get("filled_entry_count"))
    remaining = value.get("remaining_entry_count")
    if limit is None and used is None and remaining is None:
        return "-"
    return f"{_render_value(used)}/{_render_value(limit)} used, " f"{_render_value(remaining)} left"


def _render_percent(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:.2f}%"


def _render_pct_points(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value):.2f}%"


def _render_duration(value: Any) -> str:
    if value is None:
        return "-"
    seconds = float(value)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {int(remainder)}s"
    hours, minutes = divmod(int(minutes), 60)
    return f"{hours}h {minutes}m"


def _truncate(value: Any, *, length: int = 48) -> str:
    text = _render_value(value)
    if len(text) <= length:
        return text
    return text[: max(length - 1, 0)].rstrip() + "…"


def _render_count_map(
    value: Any,
    *,
    limit: int = 4,
    item_length: int = 56,
) -> str:
    if not isinstance(value, dict) or not value:
        return "-"
    ranked = sorted(
        ((str(key), int(raw_value)) for key, raw_value in value.items() if str(key or "").strip()),
        key=lambda item: (-item[1], item[0]),
    )
    rendered = ", ".join(f"{name} {_render_value(count)}" for name, count in ranked[:limit])
    if len(ranked) > limit:
        rendered += ", …"
    return _truncate(rendered, length=item_length)


def _render_session_schedule(value: Any, *, length: int = 72) -> str:
    payload = value if isinstance(value, dict) else {}
    if not payload:
        return "-"
    interval_minutes = payload.get("interval_minutes")
    start_offset = int(payload.get("session_start_offset_minutes") or 0)
    end_offset = int(payload.get("session_end_offset_minutes") or 0)
    interval_text = (
        f"every {int(interval_minutes)}m"
        if isinstance(interval_minutes, (int, float)) and float(interval_minutes).is_integer()
        else f"every {_render_value(interval_minutes)}m"
    )

    def _offset(anchor: str, minutes: int) -> str:
        if minutes == 0:
            return anchor
        sign = "+" if minutes > 0 else ""
        return f"{anchor}{sign}{minutes}m"

    rendered = f"{interval_text}, " f"{_offset('open', start_offset)}..{_offset('close', end_offset)}"
    return _truncate(rendered, length=length)


def _render_session_state(value: Any, *, length: int = 40) -> str:
    payload = value if isinstance(value, dict) else {}
    state = str(payload.get("state") or "").strip()
    if not state:
        return "-"
    return _truncate(state, length=length)


def _render_expected_slot(value: Any, *, length: int = 28) -> str:
    payload = value if isinstance(value, dict) else {}
    expected_slot_at = payload.get("expected_current_slot_at") or payload.get("expected_last_slot_at")
    return _truncate(expected_slot_at or "-", length=length)


def _render_engine_summary(
    console: Console,
    *,
    title: str,
    value: Any,
) -> None:
    payload = value if isinstance(value, dict) else {}
    if not payload:
        return
    table = Table(title=title, show_edge=False, header_style="bold")
    table.add_column("Metric", style="bold")
    table.add_column("Value")
    table.add_row("Strategies", _render_value(payload.get("strategy_count")))
    table.add_row("Entry Strategies", _render_value(payload.get("entry_strategy_count")))
    table.add_row(
        "Management Strategies",
        _render_value(payload.get("management_strategy_count")),
    )
    table.add_row("Ticker Source Runs", _render_value(payload.get("ticker_source_run_count")))
    table.add_row("Candidate Runs", _render_value(payload.get("candidate_run_count")))
    table.add_row("Trade Candidates", _render_value(payload.get("trade_candidate_count")))
    table.add_row("Signals", _render_value(payload.get("signal_count")))
    table.add_row(
        "Signal States",
        _render_count_map(payload.get("signal_state_counts")),
    )
    table.add_row("Decisions", _render_value(payload.get("decision_count")))
    table.add_row(
        "Decision States",
        _render_count_map(payload.get("decision_state_counts")),
    )
    table.add_row("Selected Decisions", _render_value(payload.get("selected_count")))
    table.add_row(
        "Entry Intents",
        _render_value(payload.get("entry_intent_count")),
    )
    table.add_row(
        "Entry Intent States",
        _render_count_map(payload.get("entry_intent_state_counts")),
    )
    table.add_row(
        "Management Intents",
        _render_value(payload.get("management_intent_count")),
    )
    table.add_row(
        "Management Intent States",
        _render_count_map(payload.get("management_intent_state_counts")),
    )
    table.add_row(
        "Open Positions",
        _render_value(payload.get("open_position_count")),
    )
    table.add_row(
        "Position Symbols",
        _render_count_map(payload.get("open_position_symbols")),
    )
    table.add_row(
        "Capture Targets",
        _render_value(payload.get("capture_active_target_count")),
    )
    table.add_row(
        "Capture Reasons",
        _render_count_map(payload.get("capture_target_counts")),
    )
    table.add_row("Capture Status", _render_value(payload.get("capture_status")))
    table.add_row(
        "Latest Capture Summary",
        _render_value(payload.get("latest_capture_summary_id")),
    )
    console.print(table)


def _job_run_status_text(status: str | None) -> Text:
    normalized = str(status or "unknown").strip().lower()
    style = {
        "queued": "cyan",
        "running": "blue",
        "succeeded": "green",
        "failed": "red",
        "skipped": "yellow",
    }.get(normalized, "magenta")
    return Text(normalized.upper(), style=style)


def _render_schedule(row: dict[str, Any]) -> str:
    session_schedule = row.get("session_schedule") if isinstance(row.get("session_schedule"), dict) else {}
    if session_schedule:
        return _render_session_schedule(session_schedule)
    schedule_type = str(row.get("schedule_type") or "")
    schedule = dict(row.get("schedule") or {})
    if schedule_type == "interval_minutes":
        return f"every {_render_value(schedule.get('minutes'))}m"
    if schedule_type == "market_open_plus_minutes":
        return f"open+{_render_value(schedule.get('minutes'))}m"
    if schedule_type == "market_close_plus_minutes":
        return f"close+{_render_value(schedule.get('minutes'))}m"
    if schedule_type == "manual":
        return "manual"
    return schedule_type or "-"


def _render_json_panel(
    console: Console,
    *,
    title: str,
    value: Any,
    max_lines: int = 24,
    max_chars: int = 2200,
) -> None:
    text = json.dumps(value, indent=2, default=str)
    if len(text) > max_chars:
        text = text[: max_chars - 4].rstrip() + "\n..."
    lines = text.splitlines()
    if len(lines) > max_lines:
        text = "\n".join(lines[:max_lines] + ["..."])
    console.print(Panel(Syntax(text, "json", word_wrap=True), title=title))


def _render_attention(console: Console, payload: dict[str, Any]) -> None:
    attention = list(payload.get("attention") or [])
    if not attention:
        return
    table = Table(title="Attention", show_edge=False, header_style="bold")
    table.add_column("Severity", style="bold")
    table.add_column("Code", style="cyan")
    table.add_column("Message")
    for item in attention:
        table.add_row(
            str(item.get("severity") or "-"),
            str(item.get("code") or "-"),
            str(item.get("message") or "-"),
        )
    console.print(table)


def render_json_payload(console: Console, payload: dict[str, Any]) -> None:
    console.file.write(json.dumps(payload, indent=2, default=str) + "\n")


def render_trading_ops_state(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    account_snapshot = dict(details.get("account_snapshot") or {})
    account = dict(account_snapshot.get("account") or {})
    pnl = dict(account_snapshot.get("pnl") or {})
    scheduler = dict(details.get("scheduler") or {})
    broker_sync = dict(details.get("broker_sync") or {})
    alert_delivery = dict(details.get("alert_delivery") or {})
    execution_health = dict(details.get("execution_health") or {})
    mark_health = dict(details.get("mark_health") or {})
    engine = dict(details.get("engine") or {})
    engine_summary = dict(engine.get("summary") or {})

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
    overview.add_row("Environment", _render_value(summary.get("environment")))
    overview.add_row("Control", _render_value(summary.get("control_mode")))
    overview.add_row(
        "Scheduler",
        f"{_render_value(scheduler.get('status'))} @ {_render_value(scheduler.get('expires_at'))}",
    )
    overview.add_row(
        "Workers",
        (
            f"lanes {_render_value(summary.get('worker_lane_count'))} | "
            f"blocked {_render_value(summary.get('blocked_worker_lane_count'))} | "
            f"idle {_render_value(summary.get('idle_worker_lane_count'))}"
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
        "dead-letter " f"{_render_value(alert_delivery.get('dead_letter_count'))} | retry {_render_value(alert_delivery.get('retry_wait_count'))}",
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

    flow_rows = list(details.get("trading_flows") or [])
    if flow_rows:
        table = Table(title="Trading Flows", header_style="bold")
        table.add_column("Strategy")
        table.add_column("Status")
        table.add_column("Ticker Source")
        table.add_column("Symbols", justify="right")
        table.add_column("Candidates", justify="right")
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
                f"{_render_value(source_state.get('status'))} ({_render_value(source_state.get('age_seconds'))}s)",
                _render_value(source_state.get("symbol_count")),
                _render_value(candidate_state.get("candidate_count")),
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


def render_jobs_view(console: Console, payload: dict[str, Any]) -> None:
    details = dict(payload.get("details") or {})
    if str(details.get("view") or "list") == "detail":
        _render_job_run_detail(console, payload)
        return
    _render_jobs_list(console, payload)


def render_job_lanes_view(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    scheduler = dict(details.get("scheduler") or {})
    lane_rows = list(details.get("worker_lanes") or [])

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row(
        "Scheduler",
        f"{_render_value(scheduler.get('status'))} @ {_render_value(scheduler.get('expires_at'))}",
    )
    overview.add_row("Worker Lanes", _render_value(summary.get("worker_lane_count")))
    overview.add_row("Workers", _render_value(summary.get("active_worker_count")))
    overview.add_row(
        "Jobs",
        f"running {_render_value(summary.get('running_job_count'))} | queued {_render_value(summary.get('queued_job_count'))}",
    )
    console.print(
        Panel(
            overview,
            title="Worker Lanes",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    if lane_rows:
        table = Table(title="Lane Summary", header_style="bold")
        table.add_column("Lane")
        table.add_column("Queue")
        table.add_column("Status")
        table.add_column("Workers", justify="right")
        table.add_column("Running", justify="right")
        table.add_column("Queued", justify="right")
        table.add_column("Tasks", justify="right")
        table.add_column("Max Jobs", justify="right")
        for row in lane_rows:
            table.add_row(
                str(row.get("lane") or row.get("settings_name") or "-"),
                str(row.get("queue_name") or "-"),
                _status_text(row.get("status")),
                _render_value(row.get("active_worker_count")),
                _render_value(row.get("running_job_count")),
                _render_value(row.get("queued_job_count")),
                _render_value(row.get("task_count")),
                _render_value(row.get("max_jobs")),
            )
        console.print(table)

    workers = list(details.get("workers") or [])
    if workers:
        table = Table(title="Active Workers", header_style="bold")
        table.add_column("Owner")
        table.add_column("Lane")
        table.add_column("Queue")
        table.add_column("Settings")
        table.add_column("Expires")
        for row in workers:
            lease_state = row.get("lease_state") if isinstance(row.get("lease_state"), dict) else {}
            table.add_row(
                str(row.get("owner") or "-"),
                _render_value(lease_state.get("lane")),
                _render_value(lease_state.get("queue_name")),
                _render_value(lease_state.get("settings_name")),
                _render_value(row.get("expires_at")),
            )
        console.print(table)


def render_storage_ops_state(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    maintenance = dict(details.get("maintenance") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row(
        "Latest Run",
        (
            f"{_render_value(summary.get('latest_run_status'))} @ "
            f"{_render_value(summary.get('latest_run_at'))}"
        ),
    )
    overview.add_row(
        "Latest Prune",
        (
            f"matched {_render_value(summary.get('latest_matching_count'))} | "
            f"deleted {_render_value(summary.get('latest_deleted_count'))}"
        ),
    )
    overview.add_row(
        "Storage",
        (
            f"{_render_bytes(summary.get('total_size_bytes'))} | "
            f"live rows {_render_value(summary.get('estimated_live_rows'))} | "
            f"dead rows {_render_value(summary.get('estimated_dead_rows'))}"
        ),
    )
    overview.add_row(
        "Vacuum Full",
        (
            "pending "
            f"{_render_value(', '.join(summary.get('vacuum_full_pending_tables') or []))}"
            if summary.get("vacuum_full_pending")
            else "not pending"
        ),
    )
    overview.add_row("Schedule", _render_value(summary.get("schedule")))
    overview.add_row("Retention Log", _render_value(summary.get("retention_log_path")))
    overview.add_row("Market Hours Safe", "yes" if summary.get("market_hours_safe") else "no")
    console.print(
        Panel(
            overview,
            title="Storage Ops State",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    table_rows = list(details.get("tables") or [])
    if table_rows:
        table = Table(title="Storage Tables", header_style="bold")
        table.add_column("Name")
        table.add_column("Class")
        table.add_column("Retention", justify="right")
        table.add_column("Rows Est.", justify="right")
        table.add_column("Dead Est.", justify="right")
        table.add_column("Size", justify="right")
        table.add_column("Latest Deleted", justify="right")
        table.add_column("Vacuum")
        for row in table_rows:
            latest_prune = dict(row.get("latest_prune") or {})
            vacuum_full = dict(row.get("vacuum_full") or {})
            table.add_row(
                _render_value(row.get("name")),
                _render_value(row.get("data_class")),
                f"{_render_value(row.get('retention_days'))}d",
                _render_value(row.get("estimated_live_rows")),
                _render_value(row.get("estimated_dead_rows")),
                _render_bytes(row.get("total_size_bytes")),
                _render_value(latest_prune.get("deleted_count")),
                "pending" if vacuum_full.get("pending") else "ok",
            )
        console.print(table)

    if maintenance:
        console.print(
            Panel(
                _render_value(maintenance.get("lock_profile")),
                title=f"Maintenance Runbook: {_render_value(maintenance.get('vacuum_full_runbook'))}",
            )
        )


def _render_jobs_list(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    scheduler = dict(details.get("scheduler") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Job Type", _render_value(summary.get("job_type")))
    overview.add_row("Status Filter", _render_value(summary.get("status_filter")))
    overview.add_row("Declared Jobs", _render_value(summary.get("definition_count")))
    overview.add_row("Enabled", _render_value(summary.get("enabled_definition_count")))
    overview.add_row("Recent Runs", _render_value(summary.get("run_count")))
    overview.add_row(
        "Scheduler",
        f"{_render_value(scheduler.get('status'))} @ {_render_value(scheduler.get('expires_at'))}",
    )
    overview.add_row("Workers", _render_value(len(list(details.get("workers") or []))))
    overview.add_row("Singleton Leases", _render_value(summary.get("singleton_lease_count")))
    overview.add_row("Worker Lanes", _render_value(summary.get("worker_lane_count")))
    if summary.get("status_filter") == "failed" or summary.get("actionable_failed_count"):
        overview.add_row(
            "Actionable Failed",
            _render_value(summary.get("actionable_failed_count")),
        )
        overview.add_row(
            "Historical Failed",
            _render_value(summary.get("historical_failed_count")),
        )
    console.print(
        Panel(
            overview,
            title="Jobs",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    lane_rows = list(details.get("worker_lanes") or [])
    if lane_rows:
        table = Table(title="Worker Lanes", header_style="bold")
        table.add_column("Lane")
        table.add_column("Queue")
        table.add_column("Status")
        table.add_column("Workers", justify="right")
        table.add_column("Running", justify="right")
        table.add_column("Queued", justify="right")
        table.add_column("Tasks", justify="right")
        table.add_column("Max Jobs", justify="right")
        for row in lane_rows:
            table.add_row(
                str(row.get("lane") or row.get("settings_name") or "-"),
                str(row.get("queue_name") or "-"),
                _status_text(row.get("status")),
                _render_value(row.get("active_worker_count")),
                _render_value(row.get("running_job_count")),
                _render_value(row.get("queued_job_count")),
                _render_value(row.get("task_count")),
                _render_value(row.get("max_jobs")),
            )
        console.print(table)

    definition_rows = [] if summary.get("status_filter") else list(details.get("declared_jobs") or [])
    if definition_rows:
        table = Table(title="Declared Jobs", header_style="bold")
        table.add_column("Job Key")
        table.add_column("Type")
        table.add_column("Enabled")
        table.add_column("Health")
        table.add_column("Schedule")
        table.add_column("Session")
        table.add_column("Latest")
        table.add_column("Expected")
        table.add_column("Capture")
        table.add_column("Scope")
        for row in definition_rows:
            latest = row.get("latest_run_at")
            latest_status = row.get("latest_run_status")
            latest_text = "-"
            if latest_status or latest:
                latest_text = f"{_render_value(latest_status)} @ {_render_value(latest)}"
            table.add_row(
                str(row.get("job_key") or "-"),
                str(row.get("job_type") or "-"),
                "yes" if row.get("enabled") else "no",
                _status_text(row.get("operator_status")),
                _render_schedule(row),
                _render_session_state(row.get("session_schedule")),
                latest_text,
                _render_expected_slot(row.get("session_schedule")),
                _render_value(row.get("latest_capture_status")),
                _render_value(row.get("singleton_scope")),
            )
        console.print(table)

    run_rows = list(details.get("job_runs") or [])
    if not run_rows:
        console.print("No job runs matched the current filters.")
    else:
        table = Table(title="Recent Runs", header_style="bold")
        table.add_column("Job Run")
        table.add_column("Type")
        table.add_column("Status")
        table.add_column("Health")
        table.add_column("Session")
        table.add_column("Capture")
        table.add_column("Scheduled")
        table.add_column("Worker")
        for row in run_rows:
            health = _status_text(row.get("operator_status"))
            if row.get("superseded_by_job_run_id"):
                health = Text("HISTORICAL", style="cyan")
            values = [
                str(row.get("job_run_id") or "-"),
                str(row.get("job_type") or "-"),
                _job_run_status_text(row.get("status")),
                health,
                str(row.get("session_id") or "-"),
                _render_value(row.get("capture_status")),
                str(row.get("scheduled_for") or "-"),
                str(row.get("worker_name") or "-"),
            ]
            table.add_row(*values)
        console.print(table)

    singleton_leases = list(details.get("singleton_leases") or [])
    if singleton_leases:
        table = Table(title="Singleton Leases", header_style="bold")
        table.add_column("Lease")
        table.add_column("Owner")
        table.add_column("Job Run")
        table.add_column("Expires")
        for row in singleton_leases:
            table.add_row(
                str(row.get("lease_key") or "-"),
                str(row.get("owner") or "-"),
                str(row.get("job_run_id") or "-"),
                str(row.get("expires_at") or "-"),
            )
        console.print(table)


def _render_job_run_detail(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    run = dict(details.get("run") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Job Run", _render_value(summary.get("job_run_id")))
    overview.add_row("Job Key", _render_value(summary.get("job_key")))
    overview.add_row("Type", _render_value(summary.get("job_type")))
    overview.add_row("Status", _job_run_status_text(summary.get("status")))
    overview.add_row("Health", _status_text(summary.get("operator_status")))
    overview.add_row("Session", _render_value(summary.get("session_id")))
    overview.add_row("Scheduled", _render_value(summary.get("scheduled_for")))
    overview.add_row("Started", _render_value(run.get("started_at")))
    overview.add_row("Finished", _render_value(run.get("finished_at")))
    overview.add_row("Heartbeat", _render_value(run.get("heartbeat_at")))
    overview.add_row("Duration", _render_duration(run.get("duration_seconds")))
    overview.add_row("Worker", _render_value(summary.get("worker_name")))
    overview.add_row("Retry", _render_value(summary.get("retry_count")))
    overview.add_row("Capture", _render_value(summary.get("capture_status")))
    overview.add_row("Result", _render_value(summary.get("result_status")))
    overview.add_row("Reason", _render_value(summary.get("result_reason")))
    console.print(
        Panel(
            overview,
            title="Job Run Detail",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    definition = dict(details.get("definition") or {})
    if definition:
        table = Table(title="Definition", show_edge=False, header_style="bold")
        table.add_column("Field", style="bold")
        table.add_column("Value")
        table.add_row("Enabled", "yes" if definition.get("enabled") else "no")
        table.add_row("Schedule", _render_schedule(definition))
        table.add_row("Session", _render_session_state(definition.get("session_schedule")))
        table.add_row("Expected Slot", _render_expected_slot(definition.get("session_schedule")))
        table.add_row("Calendar", _render_value(definition.get("market_calendar")))
        table.add_row("Scope", _render_value(definition.get("singleton_scope")))
        table.add_row("Latest Run", _render_value(definition.get("latest_run_id")))
        console.print(table)

    capture_status = run.get("capture_status")
    if capture_status is not None:
        table = Table(title="Capture Summary", header_style="bold")
        table.add_column("Status")
        table.add_column("Quotes Stream/Base", justify="right")
        table.add_column("Trades Stream/Total", justify="right")
        table.add_row(
            _render_value(capture_status),
            f"{_render_value(run.get('stream_quote_events_saved'))}/{_render_value(run.get('baseline_quote_events_saved'))}",
            f"{_render_value(run.get('stream_trade_events_saved'))}/{_render_value(run.get('total_trade_events_saved'))}",
        )
        console.print(table)

    error_text = run.get("error_text")
    if error_text:
        console.print(Panel(str(error_text), title="Error", border_style="red"))

    _render_json_panel(console, title="Payload", value=details.get("payload") or {})
    _render_json_panel(console, title="Result", value=details.get("result") or {})
