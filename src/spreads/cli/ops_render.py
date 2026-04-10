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
        table.add_column("Quote WS/Base", justify="right")
        table.add_column("Last Slot")
        for row in collector_rows:
            table.add_row(
                str(row.get("job_key") or "-"),
                str(row.get("status") or "-"),
                str(row.get("capture_status") or "-"),
                f"{_render_value(row.get('websocket_quote_events_saved'))}/{_render_value(row.get('baseline_quote_events_saved'))}",
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

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row(
        "Trading Allowed", "yes" if summary.get("trading_allowed") else "no"
    )
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
    overview.add_row("Risk Breaches", _render_value(summary.get("risk_breach_count")))
    overview.add_row(
        "Mismatches", _render_value(summary.get("reconciliation_mismatch_count"))
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
        table.add_column("Requested At")
        for row in open_attempts[:8]:
            table.add_row(
                str(row.get("session_id") or "-"),
                str(row.get("underlying_symbol") or "-"),
                str(row.get("trade_intent") or "-"),
                str(row.get("status") or "-"),
                str(row.get("requested_at") or "-"),
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
    table.add_column("Board/WL", justify="right")
    table.add_column("Alerts", justify="right")
    table.add_column("Verdict")
    table.add_column("Spread", justify="right")
    table.add_column("Updated")
    for row in rows:
        board_count = row.get("board_count")
        watchlist_count = row.get("watchlist_count")
        table.add_row(
            str(row.get("session_id") or "-"),
            str(row.get("label") or "-"),
            str(row.get("operator_status") or row.get("status") or "-"),
            str(row.get("latest_capture_status") or "-"),
            f"{_render_value(board_count)}/{_render_value(watchlist_count)}",
            _render_value(row.get("alert_count")),
            str(row.get("post_market_verdict") or "-"),
            _render_money(row.get("board_watchlist_pnl_spread")),
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
    overview.add_row("Risk", _render_value(summary.get("risk_status")))
    overview.add_row(
        "Reconciliation", _render_value(summary.get("reconciliation_status"))
    )
    overview.add_row("Control", _render_value(summary.get("control_mode")))
    overview.add_row("Alerts", _render_value(summary.get("alert_count")))
    overview.add_row("Executions", _render_value(summary.get("execution_count")))
    overview.add_row(
        "Open Positions", _render_value(summary.get("open_position_count"))
    )
    overview.add_row("Verdict", _render_value(summary.get("post_market_verdict")))
    overview.add_row(
        "Board vs WL", _render_money(summary.get("board_watchlist_pnl_spread"))
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
        table.add_column("Quote WS/Base", justify="right")
        for row in slot_runs[:8]:
            quote_capture = dict(row.get("quote_capture") or {})
            table.add_row(
                str(row.get("slot_at") or row.get("scheduled_for") or "-"),
                str(row.get("status") or "-"),
                str(row.get("capture_status") or "-"),
                f"{_render_value(quote_capture.get('websocket_quote_events_saved'))}/{_render_value(quote_capture.get('baseline_quote_events_saved'))}",
            )
        console.print(table)

    top_ideas = list(details.get("top_modeled_ideas") or [])
    if top_ideas:
        table = Table(title="Top Modeled Ideas", header_style="bold")
        table.add_column("Underlying")
        table.add_column("Class")
        table.add_column("Strategy")
        table.add_column("Modeled PnL", justify="right")
        table.add_column("Outcome")
        for row in top_ideas:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("classification") or "-"),
                str(row.get("strategy") or "-"),
                _render_money(row.get("modeled_pnl")),
                str(row.get("replay_verdict") or "-"),
            )
        console.print(table)

    bottom_ideas = list(details.get("bottom_modeled_ideas") or [])
    if bottom_ideas:
        table = Table(title="Bottom Modeled Ideas", header_style="bold")
        table.add_column("Underlying")
        table.add_column("Class")
        table.add_column("Strategy")
        table.add_column("Modeled PnL", justify="right")
        table.add_column("Outcome")
        for row in bottom_ideas:
            table.add_row(
                str(row.get("underlying_symbol") or "-"),
                str(row.get("classification") or "-"),
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
        table.add_column("Quotes WS/Base", justify="right")
        table.add_column("Trades WS/Total", justify="right")
        table.add_row(
            _render_value(capture_status),
            f"{_render_value(run.get('websocket_quote_events_saved'))}/{_render_value(run.get('baseline_quote_events_saved'))}",
            f"{_render_value(run.get('websocket_trade_events_saved'))}/{_render_value(run.get('total_trade_events_saved'))}",
        )
        console.print(table)

    error_text = run.get("error_text")
    if error_text:
        console.print(Panel(str(error_text), title="Error", border_style="red"))

    _render_json_panel(console, title="Payload", value=details.get("payload") or {})
    _render_json_panel(console, title="Result", value=details.get("result") or {})
