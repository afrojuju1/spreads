from __future__ import annotations

from typing import Any

from rich.console import Console
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
QUALITY_STAGE_COLUMNS = (
    ("source_preflight", "Source"),
    ("underlying_setup", "Setup"),
    ("chain_viability", "Chain"),
    ("contract_fit", "Contract"),
    ("premium_quality", "Premium"),
    ("selection", "Selection Filters"),
)


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
    return f"{_render_value(used)}/{_render_value(limit)} used, {_render_value(remaining)} left"


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
    normalize_names: bool = False,
) -> str:
    if not isinstance(value, dict) or not value:
        return "-"
    ranked = sorted(
        ((str(key), int(raw_value)) for key, raw_value in value.items() if str(key or "").strip()),
        key=lambda item: (-item[1], item[0]),
    )
    if normalize_names:
        ranked = [(_compact_count_name(name), count) for name, count in ranked]
    rendered = ", ".join(f"{name} {_render_value(count)}" for name, count in ranked[:limit])
    if len(ranked) > limit:
        rendered += ", …"
    return _truncate(rendered, length=item_length)


def _render_group_labels(value: Any, *, limit: int = 3, item_length: int = 72) -> str:
    if not isinstance(value, list) or not value:
        return "-"
    labels: list[str] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or row.get("group") or "").strip()
        if label:
            labels.append(f"{label} {_render_value(row.get('count'))}")
        if len(labels) >= limit:
            break
    return _truncate(", ".join(labels) or "-", length=item_length)


def _render_stage_count_map(value: Any, *, item_length: int = 30) -> str:
    if not isinstance(value, dict) or not value:
        return "-"
    order = {"pass": 0, "watch": 1, "block": 2}
    ranked = sorted(
        ((str(key), int(raw_value)) for key, raw_value in value.items() if str(key or "").strip()),
        key=lambda item: (order.get(item[0], 99), item[0]),
    )
    aliases = {"pass": "P", "watch": "W", "block": "B"}
    rendered = " / ".join(f"{aliases.get(name, name)} {_render_value(count)}" for name, count in ranked if count > 0)
    return _truncate(rendered or "-", length=item_length)


def _render_source_state(value: Any) -> str:
    if not isinstance(value, dict):
        return "-"
    status = _render_value(value.get("status"))
    basis = str(value.get("source_basis") or "").strip()
    evidence_state = str(value.get("source_evidence_state") or "").strip()
    if basis == "configured_universe":
        label = "static"
        if evidence_state == "static_symbols_configured":
            label = "static configured"
        return f"{status} {label}"
    age_seconds = value.get("age_seconds")
    if age_seconds is None:
        return f"{status} dynamic"
    return f"{status} dynamic {_render_value(age_seconds)}s"


def _waterfall_stage_counts(waterfall: dict[str, Any], stage: str) -> dict[str, Any]:
    stage_counts = waterfall.get("stage_counts")
    if isinstance(stage_counts, dict) and isinstance(stage_counts.get(stage), dict):
        return dict(stage_counts[stage])
    for row in list(waterfall.get("stage_rows") or []):
        if isinstance(row, dict) and row.get("stage") == stage and isinstance(row.get("counts"), dict):
            return dict(row["counts"])
    return {}


def _waterfall_stage_blockers(waterfall: dict[str, Any], stage: str) -> dict[str, Any]:
    for row in list(waterfall.get("stage_rows") or []):
        if isinstance(row, dict) and row.get("stage") == stage and isinstance(row.get("top_blocker_reasons"), dict):
            return dict(row["top_blocker_reasons"])
    return {}


def _render_quality_waterfall_summary(console: Console, flow_rows: list[dict[str, Any]]) -> None:
    rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for flow in flow_rows:
        candidate_state = flow.get("candidate_state")
        waterfall = candidate_state.get("quality_waterfall") if isinstance(candidate_state, dict) else None
        if isinstance(waterfall, dict) and (waterfall.get("profile_id") or waterfall.get("stage_counts")):
            rows.append((flow, waterfall))
    if not rows:
        return

    for flow, waterfall in rows:
        selection = dict(waterfall.get("selection") or {})
        admission = dict(waterfall.get("admission") or {})
        strategy = str(flow.get("trading_strategy_id") or flow.get("name") or "-")
        profile = _render_value(waterfall.get("profile_id"))
        table = Table(
            title=f"Entry Quality Waterfall: {strategy} ({profile})",
            header_style="bold",
            show_lines=False,
        )
        table.add_column("Stage", no_wrap=True)
        table.add_column("Counts", justify="right", no_wrap=True, min_width=13)
        table.add_column("Top Reasons", max_width=42, overflow="ellipsis", no_wrap=True)
        for stage, label in QUALITY_STAGE_COLUMNS:
            table.add_row(
                label,
                _render_stage_count_map(_waterfall_stage_counts(waterfall, stage), item_length=36),
                _render_count_map(
                    _waterfall_stage_blockers(waterfall, stage),
                    limit=3,
                    item_length=48,
                    normalize_names=True,
                ),
            )
        table.add_row(
            "Decision Selection",
            _render_count_map(selection.get("decision_state_counts"), limit=4, item_length=36, normalize_names=True),
            "-",
        )
        table.add_row(
            "Admission",
            _render_count_map(admission.get("admission_state_counts"), limit=4, item_length=36, normalize_names=True),
            "-",
        )
        console.print(table)


def _compact_count_name(value: str) -> str:
    text = value.strip()
    if text == "Calendar data confidence is low for this single-name candidate":
        return "calendar_confidence_low"
    return text.replace(" ", "_").replace("-", "_").lower()


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

    rendered = f"{interval_text}, {_offset('open', start_offset)}..{_offset('close', end_offset)}"
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


def _render_disabled_task_queues(console: Console, rows: list[Any]) -> None:
    disabled_task_queues = [dict(row) for row in rows if isinstance(row, dict)]
    if not disabled_task_queues:
        return
    table = Table(title="Disabled Task Queues", header_style="bold")
    table.add_column("Worker")
    table.add_column("Task Queue")
    table.add_column("Status")
    table.add_column("Job Types")
    table.add_column("Note")
    for row in disabled_task_queues:
        table.add_row(
            str(row.get("worker") or "-"),
            str(row.get("task_queue") or "-"),
            _status_text(row.get("status")),
            _render_count_map({str(value): 1 for value in list(row.get("disabled_job_types") or [])}, limit=6, item_length=72),
            str(row.get("operator_note") or "-"),
        )
    console.print(table)


__all__ = [
    "QUALITY_STAGE_COLUMNS",
    "STATUS_STYLES",
    "build_console",
    "_compact_count_name",
    "_job_run_status_text",
    "_render_attention",
    "_render_bytes",
    "_render_count_map",
    "_render_disabled_task_queues",
    "_render_duration",
    "_render_engine_summary",
    "_render_entry_budget",
    "_render_expected_slot",
    "_render_group_labels",
    "_render_money",
    "_render_pct_points",
    "_render_percent",
    "_render_quality_waterfall_summary",
    "_render_schedule",
    "_render_session_schedule",
    "_render_session_state",
    "_render_source_state",
    "_render_stage_count_map",
    "_render_value",
    "_status_text",
    "_truncate",
]
