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
    build_console,
    render_job_lanes_view,
    render_jobs_view,
    render_json_payload,
    render_storage_ops_state,
    render_trading_ops_state,
)
from core.services.ops import (
    OpsLookupError,
    build_job_lanes_overview,
    build_job_run_view,
    build_jobs_overview,
    build_storage_ops_state,
    build_trading_ops_state,
)
from core.services.trading_engine.strategy_runtime import run_trading_strategy_entry_observation


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


def _render_strategy_observation_result(payload: dict[str, Any], *, no_color: bool) -> None:
    console = build_console(no_color=no_color)
    candidate_generation = payload.get("candidate_generation") if isinstance(payload.get("candidate_generation"), dict) else {}
    candidate_build = candidate_generation.get("candidate_build") if isinstance(candidate_generation.get("candidate_build"), dict) else {}
    strategy_run = candidate_generation.get("strategy_run") if isinstance(candidate_generation.get("strategy_run"), dict) else {}
    engine_facts = candidate_generation.get("engine_facts") if isinstance(candidate_generation.get("engine_facts"), dict) else {}

    overview = Table.grid(padding=(0, 2))
    overview.add_row("Status", _render_value(payload.get("status")))
    overview.add_row("Strategy", _render_value(payload.get("trading_strategy_id")))
    overview.add_row("Market Date", _render_value(payload.get("market_date")))
    overview.add_row("Run Mode", _render_value(payload.get("entry_run_mode")))
    overview.add_row("Provenance", _render_value(payload.get("validation_provenance")))
    overview.add_row("Signals", _render_value(payload.get("signal_count")))
    overview.add_row("Decisions", _render_value(payload.get("decision_count")))
    overview.add_row("Admissions", _render_value(payload.get("admission_count")))
    overview.add_row("Execution Intent", _render_value(payload.get("execution_intent_id")))
    console.print(Panel(overview, title="Strategy Observation"))

    table = Table(title="Observation Evidence", header_style="bold")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Strategy Run", _render_value(strategy_run.get("strategy_run_id")))
    table.add_row("Candidate Run", _render_value(engine_facts.get("candidate_run_id")))
    table.add_row("Candidates", _render_value(candidate_build.get("candidate_count") or engine_facts.get("trade_candidate_count")))
    table.add_row("Trade Signals", _render_value(engine_facts.get("trade_signal_count")))
    table.add_row("Selected Decisions", _render_value(len(list(payload.get("selected_decision_ids") or []))))
    table.add_row("Reason", _render_value(payload.get("reason") or candidate_generation.get("reason")))
    console.print(table)


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


@ops_app.command("observe-strategy", help="Run one authored strategy entry routine in observation-only mode.")
def observe_strategy_command(
    trading_strategy_id: str = typer.Argument(..., help="Authored trading strategy id to observe."),
    market_date: str | None = typer.Option(None, "--date", help="Market date for the observation run. Defaults to today."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    ignore_schedule: bool = typer.Option(False, "--ignore-schedule", help="Run even when the strategy entry routine is outside its schedule window."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    payload = run_trading_strategy_entry_observation(
        db_target=db,
        trading_strategy_id=trading_strategy_id,
        market_date=market_date,
        respect_schedule=not ignore_schedule,
    )
    if json_output:
        render_json_payload(build_console(no_color=True), payload)
        return
    _render_strategy_observation_result(payload, no_color=no_color)


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
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
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
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
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
    environment: str | None = typer.Option(
        None,
        "--env",
        help="Run this command against a named deploy target.",
    ),
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
