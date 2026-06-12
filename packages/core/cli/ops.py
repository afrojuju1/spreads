from __future__ import annotations

import typer
from typing import Any
from rich.panel import Panel
from rich.table import Table

from core.cli.command_harness import (
    run_visibility_command,
    validate_positive_limit,
)
from core.cli.ops_render import (
    render_job_lanes_view,
    render_jobs_view,
    render_storage_ops_state,
    render_trading_ops_state,
)
from core.services.ops import (
    OpsLookupError,
    build_job_lanes_overview,
    build_job_run_view,
    build_jobs_overview,
    build_strategy_evidence_ledger,
    build_storage_ops_state,
    build_trading_ops_state,
)


def _run_ops_visibility_command(**kwargs: Any) -> None:
    run_visibility_command(
        handled_error_types=(OpsLookupError,),
        **kwargs,
    )


def _jobs_payload(
    *,
    db: str | None,
    job_type: str | None,
    status: str | None,
    limit: int,
) -> dict[str, Any]:
    return build_jobs_overview(
        db_target=db,
        job_type=job_type,
        status=status,
        limit=validate_positive_limit(limit, option_name="--limit"),
    )


def _render_value(value: Any) -> str:
    if value in (None, ""):
        return "-"
    return str(value)


def _render_money(value: Any) -> str:
    if value in (None, ""):
        return "-"
    return f"${float(value):,.2f}"


def _short_id(value: Any, *, length: int = 12) -> str:
    text = _render_value(value)
    if text == "-" or len(text) <= length:
        return text
    return text[:length]


def _compact_count_name(value: str) -> str:
    text = value.strip()
    replacements = {
        "Calendar data confidence is low for this single-name candidate": "calendar_confidence_low",
    }
    if text in replacements:
        return replacements[text]
    if text.startswith("Structure strike sits too far inside expected move"):
        return "expected_move_cushion"
    if text.startswith("Modeled move does not clear the structure break-even"):
        return "modeled_move_breakeven"
    return text.replace(" ", "_").replace("-", "_").lower()


def _render_count_map(value: Any, *, limit: int = 3, length: int = 54) -> str:
    if not isinstance(value, dict) or not value:
        return "-"
    ranked = sorted(
        ((str(key), int(raw_count)) for key, raw_count in value.items() if str(key or "").strip()),
        key=lambda item: (-item[1], item[0]),
    )
    rendered = ", ".join(f"{_compact_count_name(name)} {count}" for name, count in ranked[:limit])
    if len(ranked) > limit:
        rendered += ", ..."
    if len(rendered) > length:
        return rendered[: max(length - 3, 0)].rstrip() + "..."
    return rendered


def _render_status_map(value: Any, *, length: int = 80) -> str:
    if not isinstance(value, dict) or not value:
        return "-"
    rendered = ", ".join(f"{key} {raw_value}" for key, raw_value in sorted(value.items()))
    if len(rendered) > length:
        return rendered[: max(length - 3, 0)].rstrip() + "..."
    return rendered


def _render_strategy_evidence_ledger(console: Any, payload: dict[str, Any]) -> None:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    schema = payload.get("schema") if isinstance(payload.get("schema"), dict) else {}
    overview = Table.grid(padding=(0, 2))
    overview.add_row("Status", _render_value(payload.get("status")))
    overview.add_row("Market Date", _render_value(payload.get("market_date")))
    overview.add_row("Generated", _render_value(payload.get("generated_at")))
    overview.add_row("Strategies", _render_value(payload.get("strategy_count")))
    overview.add_row(
        "Lifecycle",
        (
            f"signals {_render_value(summary.get('signal_count'))}, "
            f"decisions {_render_value(summary.get('decision_count'))}, "
            f"selected {_render_value(summary.get('selected_count'))}, "
            f"attempts {_render_value(summary.get('attempt_count'))}, "
            f"fills {_render_value(summary.get('fill_count'))}"
        ),
    )
    overview.add_row(
        "Positions",
        (
            f"{_render_value(summary.get('position_count'))} seen, "
            f"{_render_value(summary.get('open_position_count'))} open, "
            f"{_render_value(summary.get('close_count'))} closes"
        ),
    )
    overview.add_row(
        "PnL",
        (
            f"realized {_render_money(summary.get('realized_pnl'))}, "
            f"unrealized {_render_money(summary.get('unrealized_pnl'))}, "
            f"net {_render_money(summary.get('net_pnl'))}"
        ),
    )
    overview.add_row("Schemas", _render_status_map(schema))
    console.print(Panel(overview, title="Strategy Evidence Ledger"))

    rows = [row for row in payload.get("strategies") or [] if isinstance(row, dict)]
    console.print("Per-Strategy Daily Evidence")
    for row in rows:
        source = dict(row.get("source") or {})
        candidates = dict(row.get("candidates") or {})
        signals = dict(row.get("signals") or {})
        decisions = dict(row.get("decisions") or {})
        admissions = dict(row.get("admissions") or {})
        attempts = dict(row.get("attempts") or {})
        positions = dict(row.get("positions") or {})
        closes = dict(row.get("closes") or {})
        pnl = dict(row.get("pnl") or {})
        console.print(
            f"- {_render_value(row.get('trading_strategy_id'))} ({_render_value(row.get('trade_structure'))}): "
            f"src {_render_value(source.get('latest_symbol_count'))}; "
            f"cand {_render_value(candidates.get('candidate_count'))}/{_render_value(candidates.get('candidate_run_count'))}; "
            f"s/d/sel/adm "
            f"{_render_value(signals.get('signal_count'))}/"
            f"{_render_value(decisions.get('decision_count'))}/"
            f"{_render_value(decisions.get('selected_count'))}/"
            f"{_render_value(admissions.get('admission_count'))}; "
            f"exec i/a/f {_render_value(row.get('intents', {}).get('intent_count'))}/"
            f"{_render_value(attempts.get('attempt_count'))}/"
            f"{_render_value(attempts.get('fill_count'))}; "
            f"pos p/o/c {_render_value(positions.get('position_count'))}/"
            f"{_render_value(positions.get('open_position_count'))}/"
            f"{_render_value(closes.get('close_count'))}; "
            f"net {_render_money(pnl.get('net_pnl'))}; "
            f"blockers {_render_count_map(row.get('top_blocker_reasons'), limit=2, length=44)}"
        )


ops_app = typer.Typer(
    add_completion=False,
    help="Inspect canonical operator state.",
    no_args_is_help=True,
)


@ops_app.command("state", help="Show canonical live trading operator state.")
def trading_ops_state_command(
    market_date: str | None = typer.Option(
        None,
        "--date",
        help="Market date to inspect. Defaults to today in New York.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    run_trading_ops_state_visibility(
        market_date=market_date,
        db=db,
        json_output=json_output,
        watch=watch,
        no_color=no_color,
    )


def run_trading_ops_state_visibility(
    *,
    market_date: str | None,
    db: str | None,
    json_output: bool,
    watch: float | None,
    no_color: bool,
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_trading_ops_state(db_target=db, market_date=market_date),
        renderer=render_trading_ops_state,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


@ops_app.command("strategy-ledger", help="Show the per-strategy daily evidence ledger.")
def strategy_evidence_ledger_command(
    market_date: str | None = typer.Option(
        None,
        "--date",
        help="Market date to inspect. Defaults to today in New York.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_strategy_evidence_ledger(db_target=db, market_date=market_date),
        renderer=_render_strategy_evidence_ledger,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


@ops_app.command("storage", help="Show canonical storage and retention operator state.")
def storage_ops_state_command(
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    run_storage_ops_state_visibility(
        db=db,
        json_output=json_output,
        watch=watch,
        no_color=no_color,
    )


def run_storage_ops_state_visibility(
    *,
    db: str | None,
    json_output: bool,
    watch: float | None,
    no_color: bool,
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_storage_ops_state(db_target=db),
        renderer=render_storage_ops_state,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


jobs_app = typer.Typer(
    add_completion=False,
    help="Inspect job definitions and job runs.",
    invoke_without_command=True,
    no_args_is_help=False,
)


@jobs_app.callback(invoke_without_command=True)
def jobs_command(
    ctx: typer.Context,
    job_type: str | None = typer.Option(None, "--job-type", help="Filter runs and definitions by job type."),
    status: str | None = typer.Option(None, "--status", help="Filter runs by status."),
    limit: int = typer.Option(25, "--limit", help="Maximum job runs to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    _run_ops_visibility_command(
        builder=lambda: _jobs_payload(
            db=db,
            job_type=job_type,
            status=status,
            limit=limit,
        ),
        renderer=render_jobs_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


@jobs_app.command("run", help="Inspect one job run.")
def jobs_run_command(
    job_run_id: str = typer.Argument(..., help="Job run id to inspect."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_job_run_view(
            db_target=db,
            job_run_id=job_run_id,
        ),
        renderer=render_jobs_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )


@jobs_app.command("lanes", help="Inspect worker lanes.")
def jobs_lanes_command(
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    watch: float | None = typer.Option(None, "--watch", help="Refresh every N seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    _run_ops_visibility_command(
        builder=lambda: build_job_lanes_overview(db_target=db),
        renderer=render_job_lanes_view,
        json_output=json_output,
        watch_seconds=watch,
        no_color=no_color,
    )
