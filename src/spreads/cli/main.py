from __future__ import annotations

from collections.abc import Callable

import typer

from spreads.cli.ops import (
    audit_command,
    jobs_app,
    status_command,
    trading_command,
    uoa_app,
)
from spreads.cli.runtime import (
    opportunities_command,
    pipelines_command,
    positions_command,
)
from spreads.cli.replay import replay_app
from spreads.jobs.live_collector import main as collect_main
from spreads.jobs.scheduler import main as scheduler_main
from spreads.jobs.seed import main as seed_jobs_main
from spreads.services.alpaca_research import main as research_alpaca_main
from spreads.services.analysis import main as analyze_main
from spreads.services.market_recorder import main as market_recorder_main
from spreads.services.post_market_analysis import main as post_market_analyze_main
from spreads.services.scanner import main as scan_main

PASSTHROUGH_CONTEXT_SETTINGS = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "help_option_names": [],
}

app = typer.Typer(
    add_completion=True,
    help="Spreads operator CLI.",
    no_args_is_help=True,
)


def _run_passthrough(
    *,
    ctx: typer.Context,
    entrypoint: Callable[[list[str] | None], int],
) -> None:
    try:
        code = entrypoint(list(ctx.args))
    except SystemExit as exc:
        raw_code = exc.code
        if raw_code in (None, 0):
            code = 0
        elif isinstance(raw_code, int):
            code = raw_code
        else:
            code = 1
    raise typer.Exit(code)


app.command("status", help="Show system and runtime health.")(status_command)
app.command("trading", help="Show live trading safety and readiness.")(trading_command)
app.command("pipelines", help="List runtime pipelines or inspect one pipeline.")(
    pipelines_command
)
app.command("opportunities", help="List opportunities or inspect one opportunity.")(
    opportunities_command
)
app.command("positions", help="List positions or inspect one position.")(
    positions_command
)
app.command("audit", help="Replay one pipeline date for operator investigation.")(
    audit_command
)
app.add_typer(jobs_app, name="jobs")
app.add_typer(replay_app, name="replay")
app.add_typer(uoa_app, name="uoa")


@app.command(
    "scan",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the spread scanner.",
)
def scan_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=scan_main)


@app.command(
    "collect",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run a live collector session.",
)
def collect_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=collect_main)


@app.command(
    "analyze",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the legacy post-close analysis report.",
)
def analyze_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=analyze_main)


post_market_app = typer.Typer(
    add_completion=False,
    help="Run post-market analysis workflows.",
)


@post_market_app.command(
    "analyze",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run persisted post-market analysis for a collector label.",
)
def post_market_analyze_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=post_market_analyze_main)


app.add_typer(post_market_app, name="post-market")


research_app = typer.Typer(
    add_completion=False,
    help="Run research-oriented diagnostics and reports.",
)


@research_app.command(
    "alpaca",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Inspect Alpaca capability coverage for spreads.",
)
def research_alpaca_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=research_alpaca_main)


app.add_typer(research_app, name="research")


@app.command(
    "scheduler",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the ARQ scheduler loop.",
)
def scheduler_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=scheduler_main)


@app.command(
    "market-recorder",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Run the recovery market recorder loop.",
)
def market_recorder_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=market_recorder_main)


@jobs_app.command(
    "seed",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Seed default job definitions.",
)
def jobs_seed_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=seed_jobs_main)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
