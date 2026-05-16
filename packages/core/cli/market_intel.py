from __future__ import annotations

import json
from collections.abc import Callable
from typing import cast

import typer

from core.services.alpaca_research import main as research_alpaca_main
from core.services.market_intel import (
    MarketIntelRequest,
    create_market_intel_run,
    resolve_output_root,
    run_summary_payload,
)
from core.services.market_intel.config import DEFAULT_OUTPUT_ROOT
from core.services.market_intel.contracts import MarketIntelDepth, SourceType, parse_as_of
from core.services.market_intel.eval_harness import (
    DEFAULT_EVAL_OUTPUT_ROOT,
    run_market_intel_eval,
)


PASSTHROUGH_CONTEXT_SETTINGS = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
    "help_option_names": [],
}

market_intel_app = typer.Typer(
    add_completion=False,
    help="Run market intelligence diagnostics and thesis workflows.",
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


@market_intel_app.command(
    "alpaca",
    context_settings=PASSTHROUGH_CONTEXT_SETTINGS,
    help="Inspect Alpaca capability coverage for spreads.",
)
def market_intel_alpaca_command(ctx: typer.Context) -> None:
    _run_passthrough(ctx=ctx, entrypoint=research_alpaca_main)


@market_intel_app.command("thesis", help="Create a file-backed market intel run.")
def market_intel_thesis_command(
    ticker: str = typer.Option(..., "--ticker", help="Ticker to research."),
    as_of: str | None = typer.Option(
        None,
        "--as-of",
        help="As-of date YYYY-MM-DD. Defaults to today's UTC date.",
    ),
    output_root: str = typer.Option(
        str(DEFAULT_OUTPUT_ROOT),
        "--output-root",
        help="Output root for market intel artifacts.",
    ),
    sources: str = typer.Option(
        "sec,market",
        "--sources",
        help="Comma-separated source adapter names to enable.",
    ),
    depth: MarketIntelDepth = typer.Option(
        "standard",
        "--depth",
        help="Research depth: quick, standard, or deep.",
    ),
    no_llm: bool = typer.Option(
        False,
        "--no-llm",
        help="Skip LLM stages once they are implemented.",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Refetch source artifacts once adapters are implemented.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    try:
        request = MarketIntelRequest(
            ticker=ticker,
            as_of=parse_as_of(as_of),
            output_root=resolve_output_root(output_root),
            sources=_parse_sources(sources),
            depth=depth,
            no_llm=no_llm,
            refresh=refresh,
        )
        run = create_market_intel_run(request)
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None

    payload = run_summary_payload(run)
    if json_output:
        typer.echo(json.dumps(payload, sort_keys=True))
        return
    typer.echo(f"run_id={payload['run_id']}")
    typer.echo(f"ticker={payload['ticker']}")
    typer.echo(f"as_of={payload['as_of']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"run_dir={payload['run_dir']}")
    for warning in payload["warnings"]:
        typer.echo(f"warning={warning}")


@market_intel_app.command("eval", help="Run the market-intel eval harness.")
def market_intel_eval_command(
    tickers: str = typer.Option(
        "SOFI",
        "--tickers",
        help="Comma-separated tickers to evaluate.",
    ),
    as_of: str | None = typer.Option(
        "2026-05-01",
        "--as-of",
        help="As-of date YYYY-MM-DD.",
    ),
    output_root: str = typer.Option(
        str(DEFAULT_EVAL_OUTPUT_ROOT),
        "--output-root",
        help="Output root for eval artifacts.",
    ),
    sources: str = typer.Option(
        "sec,market",
        "--sources",
        help="Comma-separated source adapter names to enable.",
    ),
    depth: MarketIntelDepth = typer.Option(
        "quick",
        "--depth",
        help="Research depth: quick, standard, or deep.",
    ),
    no_llm: bool = typer.Option(
        False,
        "--no-llm",
        help="Skip LLM stages during eval.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    try:
        payload = run_market_intel_eval(
            tickers=_parse_tickers(tickers),
            as_of=parse_as_of(as_of),
            output_root=resolve_output_root(output_root),
            sources=_parse_sources(sources),
            depth=depth,
            no_llm=no_llm,
        )
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None

    if json_output:
        typer.echo(json.dumps(payload, sort_keys=True))
        return
    typer.echo(f"eval_id={payload['eval_id']}")
    typer.echo(f"passed={payload['passed']}")
    typer.echo(f"eval_dir={payload['eval_dir']}")
    typer.echo(f"cases={payload['passed_count']}/{payload['case_count']}")


def _parse_tickers(value: str) -> tuple[str, ...]:
    parsed = tuple(
        ticker.strip().upper()
        for ticker in str(value or "").split(",")
        if ticker.strip()
    )
    if not parsed:
        raise ValueError("At least one ticker is required")
    return parsed


def _parse_sources(value: str) -> tuple[SourceType, ...]:
    allowed: set[str] = {
        "sec",
        "ir",
        "market",
        "news",
        "calendar",
        "valuation_context",
    }
    parsed = tuple(
        source.strip()
        for source in str(value or "").split(",")
        if source.strip()
    )
    if not parsed:
        return ("sec", "market")
    unsupported = sorted(set(parsed) - allowed)
    if unsupported:
        raise ValueError(f"Unsupported source adapter(s): {', '.join(unsupported)}")
    return cast(tuple[SourceType, ...], parsed)
