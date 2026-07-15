from __future__ import annotations

import json
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from core.cli.ops_render_helpers import (
    STATUS_STYLES,
    _job_run_status_text,
    _render_attention,
    _render_disabled_workflow_lanes,
    _render_duration,
    _render_expected_slot,
    _render_schedule,
    _render_session_state,
    _render_value,
    _status_text,
)


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


def render_jobs_view(console: Console, payload: dict[str, Any]) -> None:
    details = dict(payload.get("details") or {})
    if str(details.get("view") or "list") == "detail":
        _render_job_run_detail(console, payload)
        return
    _render_jobs_list(console, payload)


def _render_jobs_list(console: Console, payload: dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    details = dict(payload.get("details") or {})
    schedules = dict(details.get("routine_schedules") or {})

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Overall", _status_text(payload.get("status")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Job Type", _render_value(summary.get("job_type")))
    overview.add_row("Status Filter", _render_value(summary.get("status_filter")))
    overview.add_row("Declared Jobs", _render_value(summary.get("definition_count")))
    overview.add_row("Enabled", _render_value(summary.get("enabled_definition_count")))
    overview.add_row("Recent Runs", _render_value(summary.get("run_count")))
    overview.add_row(
        "Schedules",
        (
            f"{_render_value(schedules.get('status'))} | "
            f"enabled {_render_value(schedules.get('enabled_schedule_count'))}/"
            f"{_render_value(schedules.get('declared_schedule_count'))}"
        ),
    )
    overview.add_row("Workflow Lanes", _render_value(summary.get("workflow_lane_count")))
    overview.add_row(
        "Workflow Executions",
        (
            f"{_render_value(summary.get('workflow_execution_status'))} | "
            f"open {_render_value(summary.get('open_workflow_execution_count'))} | "
            f"issues {_render_value(summary.get('workflow_execution_issue_count'))}"
        ),
    )
    overview.add_row("Disabled Workflow Lanes", _render_value(summary.get("disabled_workflow_lane_count")))
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

    workflow_lane_rows = list(details.get("workflow_lanes") or [])
    if workflow_lane_rows:
        table = Table(title="Workflow Lanes", header_style="bold")
        table.add_column("Lane")
        table.add_column("Status")
        table.add_column("Pollers", justify="right")
        table.add_column("Running", justify="right")
        table.add_column("Queued", justify="right")
        table.add_column("Routines", justify="right")
        table.add_column("Max", justify="right")
        for row in workflow_lane_rows:
            table.add_row(
                str(row.get("lane") or "-"),
                _status_text(row.get("status")),
                _render_value(row.get("poller_count")),
                _render_value(row.get("running_job_count")),
                _render_value(row.get("queued_job_count")),
                _render_value(row.get("routine_type_count")),
                _render_value(row.get("max_concurrency")),
            )
        console.print(table)

    _render_disabled_workflow_lanes(console, list(details.get("disabled_workflow_lanes") or []))

    workflow_execution_issues = list(dict(details.get("workflow_executions") or {}).get("issues") or [])
    if workflow_execution_issues:
        table = Table(title="Workflow Execution Issues", header_style="bold")
        table.add_column("Health")
        table.add_column("Issue")
        table.add_column("Workflow")
        table.add_column("Correlation")
        table.add_column("Queue")
        table.add_column("Age", justify="right")
        table.add_column("Attempt", justify="right")
        for row in workflow_execution_issues:
            age = row.get("task_age_seconds")
            if age is None:
                age = row.get("activity_age_seconds")
            if age is None:
                age = row.get("projection_age_seconds")
            attempt = row.get("task_attempt")
            if attempt is None:
                attempt = row.get("activity_attempt")
            table.add_row(
                _status_text(row.get("severity")),
                str(row.get("issue") or "-"),
                str(row.get("workflow_id") or "-"),
                str(row.get("correlation_id") or "-"),
                str(row.get("task_queue") or "-"),
                _render_value(age),
                _render_value(attempt),
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
    overview.add_row("Activity Retries", _render_value(summary.get("retry_count")))
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
            f"{_render_value(run.get('stream_quote_ticks_saved'))}/{_render_value(run.get('baseline_quote_ticks_saved'))}",
            f"{_render_value(run.get('stream_trade_ticks_saved'))}/{_render_value(run.get('total_trade_ticks_saved'))}",
        )
        console.print(table)

    error_text = run.get("error_text")
    if error_text:
        console.print(Panel(str(error_text), title="Error", border_style="red"))

    _render_json_panel(console, title="Payload", value=details.get("payload") or {})
    _render_json_panel(console, title="Result", value=details.get("result") or {})


__all__ = ["render_jobs_view"]
