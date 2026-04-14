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


def _render_percent(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:.2f}%"


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


def _stream_quote_count(mapping: dict[str, Any] | None) -> Any:
    payload = {} if mapping is None else mapping
    return payload.get("stream_quote_events_saved", payload.get("websocket_quote_events_saved"))


def _stream_trade_count(mapping: dict[str, Any] | None) -> Any:
    payload = {} if mapping is None else mapping
    return payload.get("stream_trade_events_saved", payload.get("websocket_trade_events_saved"))


def _truncate(value: Any, *, length: int = 48) -> str:
    text = _render_value(value)
    if len(text) <= length:
        return text
    return text[: max(length - 1, 0)].rstrip() + "…"


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
    console.print(json.dumps(payload, indent=2, default=str))


def render_system_status(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    scheduler = dict(details.get("scheduler") or {})
    broker_sync = dict(details.get("broker_sync") or {})
    alert_delivery = dict(details.get("alert_delivery") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Control", _render_value(summary.get("control_mode")))
    overview.add_row(
        "Scheduler",
        f"{_render_value(scheduler.get('status'))} @ {_render_value(scheduler.get('expires_at'))}",
    )
    overview.add_row("Workers", _render_value(summary.get("worker_count")))
    overview.add_row(
        "Jobs",
        f"running {_render_value(summary.get('running_job_count'))} | queued {_render_value(summary.get('queued_job_count'))}",
    )
    overview.add_row(
        "Broker Sync",
        f"{_render_value(broker_sync.get('status'))} @ {_render_value(broker_sync.get('updated_at'))}",
    )
    overview.add_row(
        "Alerts",
        "dead-letter "
        f"{_render_value(alert_delivery.get('dead_letter_count'))} | retry {_render_value(alert_delivery.get('retry_wait_count'))}",
    )
    console.print(
        Panel(
            overview,
            title="System Health",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    collector_rows = list(details.get("latest_collectors") or [])
    if collector_rows:
        table = Table(title="Collectors", header_style="bold")
        table.add_column("Job Key")
        table.add_column("Status")
        table.add_column("Capture")
        table.add_column("Quote Stream/Base", justify="right")
        table.add_column("Last Slot")
        for row in collector_rows:
            table.add_row(
                str(row.get("job_key") or "-"),
                str(row.get("status") or "-"),
                str(row.get("capture_status") or "-"),
                f"{_render_value(row.get('stream_quote_events_saved'))}/{_render_value(row.get('baseline_quote_events_saved'))}",
                str(row.get("last_slot_at") or "-"),
            )
        console.print(table)

    failure_rows = list(details.get("recent_failures") or [])
    if failure_rows:
        table = Table(title="Recent Failures", header_style="bold")
        table.add_column("Job Type")
        table.add_column("Status")
        table.add_column("When")
        table.add_column("Error")
        for row in failure_rows[:8]:
            table.add_row(
                str(row.get("job_type") or "-"),
                str(row.get("status") or "-"),
                str(row.get("activity_at") or row.get("scheduled_for") or "-"),
                str(row.get("error_text") or "-"),
            )
        console.print(table)


def render_trading_health(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    account = dict(details.get("account") or {})
    broker_sync = dict(details.get("broker_sync") or {})
    market_session = dict(details.get("market_session") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row(
        "Trading Allowed", "yes" if summary.get("trading_allowed") else "no"
    )
    overview.add_row("Market", _render_value(market_session.get("status")))
    overview.add_row("Account Source", _render_value(summary.get("account_source")))
    overview.add_row("Environment", _render_value(summary.get("environment")))
    overview.add_row("Equity", _render_money(account.get("equity")))
    overview.add_row("Cash", _render_money(account.get("cash")))
    overview.add_row("Buying Power", _render_money(account.get("buying_power")))
    overview.add_row("Day PnL", _render_money(details.get("pnl", {}).get("day_change")))
    overview.add_row(
        "Day PnL %", _render_percent(details.get("pnl", {}).get("day_change_percent"))
    )
    overview.add_row(
        "Open Positions", _render_value(summary.get("open_position_count"))
    )
    overview.add_row(
        "Open Executions", _render_value(summary.get("open_execution_count"))
    )
    overview.add_row(
        "Stale Open Execs", _render_value(summary.get("stale_open_execution_count"))
    )
    overview.add_row(
        "Unknown Submit",
        _render_value(summary.get("submit_unknown_execution_count")),
    )
    overview.add_row(
        "Blocked Underlyings",
        _render_value(summary.get("capacity_blocked_underlying_count")),
    )
    overview.add_row("Risk Breaches", _render_value(summary.get("risk_breach_count")))
    overview.add_row(
        "Mismatches", _render_value(summary.get("reconciliation_mismatch_count"))
    )
    overview.add_row(
        "Execution Health", _render_value(summary.get("execution_health_status"))
    )
    overview.add_row(
        "Broker Sync",
        f"{_render_value(broker_sync.get('status'))} @ {_render_value(broker_sync.get('updated_at'))}",
    )
    console.print(
        Panel(
            overview,
            title="Trading Health",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

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


def render_sessions_view(console: Console, payload: dict[str, Any]) -> None:
    details = dict(payload.get("details") or {})
    if str(details.get("view") or "list") == "detail":
        _render_session_detail(console, payload)
        return
    _render_sessions_list(console, payload)


def render_jobs_view(console: Console, payload: dict[str, Any]) -> None:
    details = dict(payload.get("details") or {})
    if str(details.get("view") or "list") == "detail":
        _render_job_run_detail(console, payload)
        return
    _render_jobs_list(console, payload)


def render_uoa_view(console: Console, payload: dict[str, Any]) -> None:
    _render_uoa_detail(console, payload)


def render_audit_view(console: Console, payload: dict[str, Any]) -> None:
    _render_audit_detail(console, payload)


def _render_sessions_list(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Sessions", _render_value(summary.get("session_count")))
    overview.add_row("Date Filter", _render_value(summary.get("session_date")))
    console.print(
        Panel(
            overview,
            title="Sessions",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    rows = list(details.get("sessions") or [])
    if not rows:
        console.print("No sessions matched the current filters.")
        return

    table = Table(header_style="bold")
    table.add_column("Session")
    table.add_column("Label")
    table.add_column("Status")
    table.add_column("Capture")
    table.add_column("Recovery")
    table.add_column("Prom/Mon", justify="right")
    table.add_column("Alerts", justify="right")
    table.add_column("Verdict")
    table.add_column("Spread", justify="right")
    table.add_column("Updated")
    for row in rows:
        promotable_count = row.get("promotable_count")
        monitor_count = row.get("monitor_count")
        table.add_row(
            str(row.get("session_id") or "-"),
            str(row.get("label") or "-"),
            str(row.get("operator_status") or row.get("status") or "-"),
            str(row.get("latest_capture_status") or "-"),
            (
                f"{row.get('recovery_state') or '-'} "
                f"({int(row.get('missed_slot_count') or 0)}/{int(row.get('unrecoverable_slot_count') or 0)})"
            ),
            f"{_render_value(promotable_count)}/{_render_value(monitor_count)}",
            _render_value(row.get("alert_count")),
            str(row.get("post_market_verdict") or "-"),
            _render_money(row.get("promotable_monitor_pnl_spread")),
            str(row.get("updated_at") or "-"),
        )
    console.print(table)


def _render_session_detail(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Session", _render_value(summary.get("session_id")))
    overview.add_row("Label", _render_value(summary.get("label")))
    overview.add_row("Date", _render_value(summary.get("session_date")))
    overview.add_row("Capture", _render_value(summary.get("latest_capture_status")))
    overview.add_row("Recovery", _render_value(summary.get("recovery_state")))
    overview.add_row("Missed Slots", _render_value(summary.get("missed_slot_count")))
    overview.add_row(
        "Unrecoverable",
        _render_value(summary.get("unrecoverable_slot_count")),
    )
    overview.add_row(
        "Latest Fresh",
        _render_value(summary.get("latest_fresh_slot_at")),
    )
    overview.add_row(
        "Latest Resume",
        _render_value(summary.get("latest_resume_slot_at")),
    )
    overview.add_row("Risk", _render_value(summary.get("risk_status")))
    overview.add_row(
        "Reconciliation", _render_value(summary.get("reconciliation_status"))
    )
    overview.add_row("Control", _render_value(summary.get("control_mode")))
    overview.add_row("Alerts", _render_value(summary.get("alert_count")))
    overview.add_row("Executions", _render_value(summary.get("execution_count")))
    overview.add_row(
        "Open Execs", _render_value(summary.get("open_execution_count"))
    )
    overview.add_row(
        "Stale Execs", _render_value(summary.get("stale_open_execution_count"))
    )
    overview.add_row(
        "Blocked Keys", _render_value(summary.get("blocking_execution_key_count"))
    )
    overview.add_row(
        "Open Positions", _render_value(summary.get("open_position_count"))
    )
    overview.add_row("Verdict", _render_value(summary.get("post_market_verdict")))
    overview.add_row(
        "Prom vs Mon",
        _render_money(summary.get("promotable_monitor_pnl_spread")),
    )
    console.print(
        Panel(
            overview,
            title="Session Detail",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    recommendations = list(details.get("recommendations") or [])
    if recommendations:
        table = Table(title="Recommendations", header_style="bold")
        table.add_column("Priority")
        table.add_column("Code")
        table.add_column("Reason")
        for row in recommendations[:5]:
            table.add_row(
                str(row.get("priority") or "-"),
                str(row.get("code") or row.get("title") or "-"),
                str(row.get("reason") or row.get("title") or "-"),
            )
        console.print(table)

    slot_runs = list(details.get("slot_runs") or [])
    if slot_runs:
        table = Table(title="Recent Slot Runs", header_style="bold")
        table.add_column("Slot")
        table.add_column("Status")
        table.add_column("Capture")
        table.add_column("Quote Stream/Base", justify="right")
        for row in slot_runs[:8]:
            quote_capture = dict(row.get("quote_capture") or {})
            table.add_row(
                str(row.get("slot_at") or row.get("scheduled_for") or "-"),
                str(row.get("status") or "-"),
                str(row.get("capture_status") or "-"),
                f"{_render_value(_stream_quote_count(quote_capture))}/{_render_value(quote_capture.get('baseline_quote_events_saved'))}",
            )
        console.print(table)

    recovery_slots = list(details.get("recovery_slots") or [])
    if recovery_slots:
        table = Table(title="Recovery Slots", header_style="bold")
        table.add_column("Slot")
        table.add_column("Status")
        table.add_column("Capture")
        table.add_column("Updated")
        for row in recovery_slots[:8]:
            table.add_row(
                str(row.get("slot_at") or "-"),
                str(row.get("status") or "-"),
                str(row.get("capture_status") or "-"),
                str(row.get("updated_at") or "-"),
            )
        console.print(table)

    open_executions = list(details.get("open_executions") or [])
    if open_executions:
        table = Table(title="Open Executions", header_style="bold")
        table.add_column("Underlying")
        table.add_column("Intent")
        table.add_column("Status")
        table.add_column("Phase")
        table.add_column("Age")
        table.add_column("Next")
        for row in open_executions[:8]:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("trade_intent") or "-"),
                str(row.get("status") or "-"),
                str(row.get("lifecycle_phase") or "-"),
                _render_duration(row.get("age_seconds")),
                str(row.get("next_action") or "-"),
            )
        console.print(table)

    blocking_execution_keys = list(details.get("blocking_execution_keys") or [])
    if blocking_execution_keys:
        console.print(
            Panel(
                ", ".join(str(row) for row in blocking_execution_keys[:12]),
                title="Capacity Reserved",
                border_style="yellow",
            )
        )

    top_ideas = list(details.get("top_modeled_ideas") or [])
    if top_ideas:
        table = Table(title="Top Modeled Ideas", header_style="bold")
        table.add_column("Underlying")
        table.add_column("Selection")
        table.add_column("Strategy")
        table.add_column("Modeled PnL", justify="right")
        table.add_column("Outcome")
        for row in top_ideas:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("selection_state") or "-"),
                str(row.get("strategy") or "-"),
                _render_money(row.get("modeled_pnl")),
                str(row.get("replay_verdict") or "-"),
            )
        console.print(table)

    bottom_ideas = list(details.get("bottom_modeled_ideas") or [])
    if bottom_ideas:
        table = Table(title="Bottom Modeled Ideas", header_style="bold")
        table.add_column("Underlying")
        table.add_column("Selection")
        table.add_column("Strategy")
        table.add_column("Modeled PnL", justify="right")
        table.add_column("Outcome")
        for row in bottom_ideas:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("selection_state") or "-"),
                str(row.get("strategy") or "-"),
                _render_money(row.get("modeled_pnl")),
                str(row.get("replay_verdict") or "-"),
            )
        console.print(table)


def _render_jobs_list(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    scheduler = dict(details.get("scheduler") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Job Type", _render_value(summary.get("job_type")))
    overview.add_row("Status Filter", _render_value(summary.get("status_filter")))
    overview.add_row("Definitions", _render_value(summary.get("definition_count")))
    overview.add_row("Enabled", _render_value(summary.get("enabled_definition_count")))
    overview.add_row("Recent Runs", _render_value(summary.get("run_count")))
    overview.add_row(
        "Scheduler",
        f"{_render_value(scheduler.get('status'))} @ {_render_value(scheduler.get('expires_at'))}",
    )
    overview.add_row("Workers", _render_value(len(list(details.get("workers") or []))))
    overview.add_row(
        "Singleton Leases", _render_value(summary.get("singleton_lease_count"))
    )
    console.print(
        Panel(
            overview,
            title="Jobs",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    definition_rows = list(details.get("job_definitions") or [])
    if definition_rows:
        table = Table(title="Definitions", header_style="bold")
        table.add_column("Job Key")
        table.add_column("Type")
        table.add_column("Enabled")
        table.add_column("Health")
        table.add_column("Schedule")
        table.add_column("Latest")
        table.add_column("Capture")
        table.add_column("Scope")
        for row in definition_rows:
            latest = row.get("latest_run_at")
            latest_status = row.get("latest_run_status")
            latest_text = "-"
            if latest_status or latest:
                latest_text = (
                    f"{_render_value(latest_status)} @ {_render_value(latest)}"
                )
            table.add_row(
                str(row.get("job_key") or "-"),
                str(row.get("job_type") or "-"),
                "yes" if row.get("enabled") else "no",
                _status_text(row.get("operator_status")),
                _render_schedule(row),
                latest_text,
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
            table.add_row(
                str(row.get("job_run_id") or "-"),
                str(row.get("job_type") or "-"),
                _job_run_status_text(row.get("status")),
                _status_text(row.get("operator_status")),
                str(row.get("session_id") or "-"),
                _render_value(row.get("capture_status")),
                str(row.get("scheduled_for") or "-"),
                str(row.get("worker_name") or "-"),
            )
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


def _render_uoa_detail(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    quote_capture = dict(details.get("quote_capture") or {})
    trade_capture = dict(details.get("trade_capture") or {})
    uoa_overview = dict(details.get("uoa_overview") or {})
    quote_overview = dict(details.get("uoa_quote_overview") or {})
    decision_overview = dict(details.get("uoa_decision_overview") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Label", _render_value(summary.get("label")))
    overview.add_row("Cycle", _render_value(summary.get("cycle_id")))
    overview.add_row("Session", _render_value(summary.get("session_id")))
    overview.add_row("Job Run", _render_value(summary.get("job_run_id")))
    overview.add_row("Slot", _render_value(summary.get("slot_at")))
    overview.add_row(
        "Capture",
        f"quotes {_render_value(summary.get('quote_capture_status'))} | trades {_render_value(summary.get('trade_capture_status'))}",
    )
    overview.add_row(
        "UOA",
        f"{_render_value(summary.get('uoa_summary_status'))} | decisions {_render_value(summary.get('decision_status'))}",
    )
    overview.add_row(
        "Observed",
        (
            f"contracts {_render_value(summary.get('observed_contract_count'))} | "
            f"scoreable roots {_render_value(summary.get('scoreable_root_count'))}"
        ),
    )
    overview.add_row(
        "Decisions",
        (
            f"monitor {_render_value(summary.get('monitor_count'))} | "
            f"promotable {_render_value(summary.get('promotable_count'))} | "
            f"high {_render_value(summary.get('high_count'))}"
        ),
    )
    overview.add_row(
        "Top Decision",
        (
            f"{_render_value(summary.get('top_decision_symbol'))} "
            f"{_render_value(summary.get('top_decision_state'))} "
            f"({_render_value(summary.get('top_decision_score'))})"
        ),
    )
    console.print(
        Panel(
            overview,
            title="UOA",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    capture = Table(title="Capture Summary", header_style="bold")
    capture.add_column("Type")
    capture.add_column("Status")
    capture.add_column("Expected", justify="right")
    capture.add_column("Observed", justify="right")
    capture.add_column("Fresh/Liquid", justify="right")
    capture.add_row(
        "Quotes",
        _render_value(quote_capture.get("capture_status")),
        _render_value(quote_capture.get("expected_quote_symbol_count")),
        _render_value(quote_capture.get("total_quote_events_saved")),
        f"{_render_value(quote_overview.get('fresh_contract_count'))}/{_render_value(quote_overview.get('liquid_contract_count'))}",
    )
    capture.add_row(
        "Trades",
        _render_value(trade_capture.get("capture_status")),
        _render_value(trade_capture.get("expected_trade_symbol_count")),
        _render_value(trade_capture.get("total_trade_events_saved")),
        (
            f"roots {_render_value(decision_overview.get('root_count'))} | "
            f"scoreable {_render_value(uoa_overview.get('scoreable_trade_count'))}"
        ),
    )
    console.print(capture)

    exclusion_rows = list(details.get("top_exclusion_reasons") or [])
    if exclusion_rows:
        table = Table(title="Top Exclusions", header_style="bold")
        table.add_column("Reason")
        table.add_column("Count", justify="right")
        for row in exclusion_rows:
            table.add_row(str(row.get("name") or "-"), _render_value(row.get("count")))
        console.print(table)

    condition_rows = list(details.get("top_conditions") or [])
    if condition_rows:
        table = Table(title="Top Conditions", header_style="bold")
        table.add_column("Condition")
        table.add_column("Count", justify="right")
        for row in condition_rows:
            table.add_row(str(row.get("name") or "-"), _render_value(row.get("count")))
        console.print(table)

    decision_rows = list(details.get("top_monitor_roots") or [])
    if decision_rows:
        table = Table(title="Decision Roots", header_style="bold")
        table.add_column("Symbol")
        table.add_column("State")
        table.add_column("Score", justify="right")
        table.add_column("Premium", justify="right")
        table.add_column("Trades", justify="right")
        table.add_column("Vol/OI", justify="right")
        table.add_column("Quote")
        table.add_column("Why")
        for row in decision_rows:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("decision_state") or "-"),
                _render_value(row.get("decision_score")),
                _render_money(row.get("scoreable_premium")),
                _render_value(row.get("scoreable_trade_count")),
                _render_value(row.get("supporting_volume_oi_ratio")),
                _render_value(row.get("quality_state")),
                _truncate(row.get("explanation"), length=60),
            )
        console.print(table)

    top_roots = list(details.get("top_roots") or [])
    if top_roots:
        table = Table(title="Top UOA Roots", header_style="bold")
        table.add_column("Symbol")
        table.add_column("Flow")
        table.add_column("Root Score", justify="right")
        table.add_column("Premium", justify="right")
        table.add_column("Trades", justify="right")
        table.add_column("Contracts", justify="right")
        table.add_column("Vol/OI", justify="right")
        for row in top_roots:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("dominant_flow") or "-"),
                _render_value(row.get("root_score")),
                _render_money(row.get("scoreable_premium")),
                _render_value(row.get("scoreable_trade_count")),
                _render_value(row.get("scoreable_contract_count")),
                _render_value(row.get("supporting_volume_oi_ratio")),
            )
        console.print(table)

    top_contracts = list(details.get("top_contracts") or [])
    if top_contracts:
        table = Table(title="Top Contracts", header_style="bold")
        table.add_column("Option")
        table.add_column("Root")
        table.add_column("Type")
        table.add_column("DTE", justify="right")
        table.add_column("Premium", justify="right")
        table.add_column("Trades", justify="right")
        table.add_column("%OTM", justify="right")
        table.add_column("Vol/OI", justify="right")
        table.add_column("Quality")
        for row in top_contracts:
            table.add_row(
                _truncate(row.get("option_symbol"), length=24),
                str(row.get("underlying_symbol") or "-"),
                str(row.get("option_type") or "-"),
                _render_value(row.get("dte")),
                _render_money(row.get("scoreable_premium")),
                _render_value(row.get("scoreable_trade_count")),
                _render_percent(row.get("percent_otm")),
                _render_value(row.get("volume_oi_ratio")),
                _render_value(row.get("quality_state") or row.get("contract_score")),
            )
        console.print(table)

    for title, rows in (
        ("Promotable Opportunities", list(details.get("promotable_candidates") or [])),
        ("Monitor Opportunities", list(details.get("monitor_candidates") or [])),
    ):
        if not rows:
            continue
        table = Table(title=title, header_style="bold")
        table.add_column("Rank", justify="right")
        table.add_column("Symbol")
        table.add_column("Strategy")
        table.add_column("Credit", justify="right")
        table.add_column("DTE", justify="right")
        table.add_column("Quality", justify="right")
        table.add_column("Max Loss", justify="right")
        table.add_column("Setup")
        for row in rows:
            table.add_row(
                _render_value(row.get("selection_rank")),
                str(row.get("underlying_symbol") or "-"),
                str(row.get("strategy") or "-"),
                _render_money(row.get("midpoint_credit")),
                _render_value(row.get("dte")),
                _render_value(row.get("quality_score")),
                _render_money(row.get("max_loss")),
                str(row.get("setup_status") or "-"),
            )
        console.print(table)

    events = list(details.get("cycle_events") or [])
    if events:
        table = Table(title="Cycle Events", header_style="bold")
        table.add_column("When")
        table.add_column("Symbol")
        table.add_column("Type")
        table.add_column("Message")
        for row in events:
            table.add_row(
                str(row.get("generated_at") or "-"),
                str(row.get("symbol") or "-"),
                str(row.get("event_type") or "-"),
                _truncate(row.get("message"), length=72),
            )
        console.print(table)


def _render_audit_detail(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    current_cycle = dict(details.get("current_cycle") or {})
    portfolio_summary = dict(details.get("portfolio_summary") or {})
    post_market = dict(details.get("post_market") or {})
    timeline_stats = dict(details.get("timeline_stats") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Session", _render_value(summary.get("session_id")))
    overview.add_row("Label", _render_value(summary.get("label")))
    overview.add_row("Date", _render_value(summary.get("session_date")))
    overview.add_row("Session Status", _render_value(summary.get("session_status")))
    overview.add_row("Control", _render_value(summary.get("control_mode")))
    overview.add_row("Risk", _render_value(summary.get("risk_status")))
    overview.add_row(
        "Reconciliation", _render_value(summary.get("reconciliation_status"))
    )
    overview.add_row("Verdict", _render_value(summary.get("post_market_verdict")))
    overview.add_row("Net PnL", _render_money(summary.get("net_pnl_total")))
    overview.add_row(
        "Counts",
        (
            f"alerts {_render_value(summary.get('alert_count'))} | "
            f"risk {_render_value(summary.get('risk_decision_count'))} | "
            f"exec {_render_value(summary.get('execution_count'))}"
        ),
    )
    overview.add_row(
        "Timeline",
        (
            f"{_render_value(summary.get('returned_timeline_item_count'))}/"
            f"{_render_value(summary.get('timeline_item_count'))} "
            f"items"
        ),
    )
    console.print(
        Panel(
            overview,
            title="Audit",
            border_style=STATUS_STYLES.get(str(payload.get("status")), "white"),
        )
    )

    _render_attention(console, payload)

    if current_cycle:
        table = Table(title="Current Cycle", show_edge=False, header_style="bold")
        table.add_column("Field", style="bold")
        table.add_column("Value")
        table.add_row("Cycle", _render_value(current_cycle.get("cycle_id")))
        table.add_row("Generated", _render_value(current_cycle.get("generated_at")))
        table.add_row("Job Run", _render_value(current_cycle.get("job_run_id")))
        table.add_row("Strategy", _render_value(current_cycle.get("strategy")))
        table.add_row("Profile", _render_value(current_cycle.get("profile")))
        table.add_row("Universe", _render_value(current_cycle.get("universe_label")))
        table.add_row(
            "Candidates",
            (
                f"promotable {_render_value(current_cycle.get('promotable_count'))} | "
                f"monitor {_render_value(current_cycle.get('monitor_count'))}"
            ),
        )
        console.print(table)

    portfolio_table = Table(
        title="Portfolio Summary", show_edge=False, header_style="bold"
    )
    portfolio_table.add_column("Field", style="bold")
    portfolio_table.add_column("Value")
    portfolio_table.add_row(
        "Positions", _render_value(portfolio_summary.get("position_count"))
    )
    portfolio_table.add_row(
        "Open Positions", _render_value(portfolio_summary.get("open_position_count"))
    )
    portfolio_table.add_row(
        "Realized PnL", _render_money(portfolio_summary.get("realized_pnl_total"))
    )
    portfolio_table.add_row(
        "Unrealized PnL", _render_money(portfolio_summary.get("unrealized_pnl_total"))
    )
    portfolio_table.add_row(
        "Net PnL", _render_money(portfolio_summary.get("net_pnl_total"))
    )
    portfolio_table.add_row(
        "Mismatches",
        _render_value(portfolio_summary.get("mismatch_position_count")),
    )
    portfolio_table.add_row(
        "Mark Source", _render_value(portfolio_summary.get("mark_source"))
    )
    console.print(portfolio_table)

    recommendations = list(post_market.get("recommendations") or [])
    post_market_table = Table(title="Post-Market", show_edge=False, header_style="bold")
    post_market_table.add_column("Field", style="bold")
    post_market_table.add_column("Value")
    post_market_table.add_row(
        "Verdict", _render_value(post_market.get("overall_verdict"))
    )
    post_market_table.add_row(
        "Prom vs Mon",
        _render_money(post_market.get("promotable_monitor_pnl_spread")),
    )
    post_market_table.add_row("Recommendations", _render_value(len(recommendations)))
    console.print(post_market_table)

    if recommendations:
        table = Table(title="Recommendations", header_style="bold")
        table.add_column("Priority")
        table.add_column("Code")
        table.add_column("Reason")
        for row in recommendations[:5]:
            table.add_row(
                str(row.get("priority") or "-"),
                str(row.get("code") or row.get("title") or "-"),
                _truncate(row.get("reason") or row.get("title"), length=80),
            )
        console.print(table)

    slot_runs = list(details.get("slot_runs") or [])
    if slot_runs:
        table = Table(title="Collector Slots", header_style="bold")
        table.add_column("Slot")
        table.add_column("Status")
        table.add_column("Capture")
        table.add_column("Quote Stream/Base", justify="right")
        table.add_column("Trades Stream/Total", justify="right")
        for row in slot_runs:
            quote_capture = dict(row.get("quote_capture") or {})
            trade_capture = dict(row.get("trade_capture") or {})
            table.add_row(
                str(row.get("slot_at") or row.get("scheduled_for") or "-"),
                str(row.get("status") or "-"),
                str(row.get("capture_status") or "-"),
                f"{_render_value(_stream_quote_count(quote_capture))}/{_render_value(quote_capture.get('baseline_quote_events_saved'))}",
                f"{_render_value(_stream_trade_count(trade_capture))}/{_render_value(trade_capture.get('total_trade_events_saved'))}",
            )
        console.print(table)

    alerts = list(details.get("alerts") or [])
    if alerts:
        table = Table(title="Alerts", header_style="bold")
        table.add_column("Created")
        table.add_column("Symbol")
        table.add_column("Type")
        table.add_column("Target")
        table.add_column("Status")
        for row in alerts:
            table.add_row(
                str(row.get("created_at") or "-"),
                str(row.get("symbol") or "-"),
                str(row.get("alert_type") or "-"),
                str(row.get("delivery_target") or "-"),
                str(row.get("status") or "-"),
            )
        console.print(table)

    open_executions = list(details.get("open_executions") or [])
    if open_executions:
        table = Table(title="Open Executions", header_style="bold")
        table.add_column("Underlying")
        table.add_column("Intent")
        table.add_column("Status")
        table.add_column("Phase")
        table.add_column("Age")
        table.add_column("Next")
        for row in open_executions[:8]:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("trade_intent") or "-"),
                str(row.get("status") or "-"),
                str(row.get("lifecycle_phase") or "-"),
                _render_duration(row.get("age_seconds")),
                str(row.get("next_action") or "-"),
            )
        console.print(table)

    selected_opportunities = list(details.get("selected_opportunities") or [])
    if selected_opportunities:
        table = Table(title="Selected Opportunities", header_style="bold")
        table.add_column("Underlying")
        table.add_column("Selection")
        table.add_column("State")
        table.add_column("Rank", justify="right")
        table.add_column("Confidence", justify="right")
        table.add_column("Reasons")
        for row in selected_opportunities:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("selection_state") or "-"),
                str(row.get("lifecycle_state") or "-"),
                _render_value(row.get("selection_rank")),
                _render_value(row.get("confidence")),
                _truncate(", ".join(row.get("reason_codes") or []), length=56),
            )
        console.print(table)

    risk_decisions = list(details.get("risk_decisions") or [])
    if risk_decisions:
        table = Table(title="Risk Decisions", header_style="bold")
        table.add_column("At")
        table.add_column("Underlying")
        table.add_column("Kind")
        table.add_column("Status")
        table.add_column("Reasons")
        for row in risk_decisions:
            table.add_row(
                str(row.get("decided_at") or "-"),
                str(row.get("underlying_symbol") or "-"),
                str(row.get("decision_kind") or "-"),
                str(row.get("status") or "-"),
                _truncate(", ".join(row.get("reason_codes") or []), length=56),
            )
        console.print(table)

    execution_outcomes = list(details.get("execution_outcomes") or [])
    if execution_outcomes:
        table = Table(title="Execution Outcomes", header_style="bold")
        table.add_column("At")
        table.add_column("Underlying")
        table.add_column("Intent")
        table.add_column("Status")
        table.add_column("Orders/Fills", justify="right")
        table.add_column("Error")
        for row in execution_outcomes:
            table.add_row(
                str(row.get("requested_at") or row.get("submitted_at") or "-"),
                str(row.get("underlying_symbol") or "-"),
                str(row.get("trade_intent") or "-"),
                str(row.get("status") or "-"),
                f"{_render_value(row.get('order_count'))}/{_render_value(row.get('fill_count'))}",
                _truncate(row.get("error_text"), length=48),
            )
        console.print(table)

    control_actions = list(details.get("control_actions") or [])
    if control_actions:
        table = Table(title="Control Actions", header_style="bold")
        table.add_column("At")
        table.add_column("Topic")
        table.add_column("Summary")
        for row in control_actions:
            table.add_row(
                str(row.get("at") or "-"),
                str(row.get("topic") or "-"),
                _truncate(row.get("summary"), length=84),
            )
        console.print(table)

    timeline = list(details.get("timeline") or [])
    if timeline:
        title = "Timeline"
        if timeline_stats.get("timeline_window"):
            window = dict(timeline_stats.get("timeline_window") or {})
            title = (
                "Timeline "
                f"({_render_value(window.get('started_at'))} -> {_render_value(window.get('ended_at'))})"
            )
        table = Table(title=title, header_style="bold")
        table.add_column("At")
        table.add_column("Topic")
        table.add_column("Summary")
        for row in timeline:
            table.add_row(
                str(row.get("at") or "-"),
                str(row.get("topic") or "-"),
                _truncate(row.get("summary"), length=88),
            )
        console.print(table)
