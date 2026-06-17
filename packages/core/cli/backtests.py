from __future__ import annotations

import json
from typing import Any

import typer
from pydantic import ValidationError

from core.services.backtest import BacktestEngine, BacktestMode, BacktestRequest, BacktestSweepConfig

backtests_app = typer.Typer(
    add_completion=False,
    help="Run backend BacktestEngine evaluations.",
    no_args_is_help=True,
)


def _parse_sweep_dimensions(raw_dimensions: list[str] | None) -> dict[str, tuple[Any, ...]]:
    dimensions: dict[str, tuple[Any, ...]] = {}
    for raw_dimension in raw_dimensions or []:
        path, separator, raw_value = raw_dimension.partition("=")
        path = path.strip()
        if not separator or not path:
            raise typer.BadParameter("Sweep dimensions must use path=json_value syntax, for example exit.profit_target_pct=[0.25,0.5].")
        try:
            parsed_value = json.loads(raw_value)
        except json.JSONDecodeError:
            parsed_value = raw_value
        values = tuple(parsed_value) if isinstance(parsed_value, list) else (parsed_value,)
        if not values:
            raise typer.BadParameter(f"Sweep dimension {path} must include at least one value.")
        dimensions[path] = values
    return dimensions


@backtests_app.command("run", help="Run BacktestEngine and emit a JSON result.")
def run_backtest_command(
    start_date: str = typer.Option(..., "--start-date", help="First market date to evaluate, YYYY-MM-DD."),
    end_date: str | None = typer.Option(None, "--end-date", help="Last market date to evaluate. Defaults to start date."),
    mode: str = typer.Option(BacktestMode.STORED_FACTS.value, "--mode", help="Backtest mode."),
    strategy_id: list[str] | None = typer.Option(None, "--strategy-id", help="Trading strategy id. Repeatable."),
    symbol: list[str] | None = typer.Option(None, "--symbol", help="Underlying symbol. Repeatable."),
    max_days: int = typer.Option(31, "--max-days", min=1, help="Maximum market days to evaluate."),
    market_data_symbol_limit: int = typer.Option(
        250,
        "--market-data-symbol-limit",
        min=1,
        help="Maximum symbols included in market-data coverage summaries.",
    ),
    candidate_limit: int = typer.Option(10, "--candidate-limit", min=1, help="Maximum historical candidates per strategy/day."),
    per_symbol_top: int = typer.Option(1, "--per-symbol-top", min=1, help="Maximum selected candidates per symbol."),
    requested_by: str | None = typer.Option(None, "--requested-by", help="Optional requester label persisted with the backtest run."),
    artifact_root: str | None = typer.Option(None, "--artifact-root", help="Optional artifact root. Defaults to runtime config."),
    sweep_base_mode: str = typer.Option(
        BacktestMode.PORTFOLIO_SIMULATION.value,
        "--sweep-base-mode",
        help="Base mode for parameter_sweep.",
    ),
    sweep_rank_metric: str = typer.Option("net_pnl", "--sweep-rank-metric", help="Metric used to rank parameter_sweep variants."),
    sweep_max_variants: int = typer.Option(12, "--sweep-max-variants", min=1, max=100, help="Maximum parameter_sweep variants."),
    sweep_dimension: list[str] | None = typer.Option(
        None,
        "--sweep-dimension",
        help="Parameter sweep dimension as path=json_value. Repeatable; scalar values are accepted.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
) -> None:
    try:
        backtest_mode = BacktestMode(mode)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in BacktestMode)
        raise typer.BadParameter(f"mode must be one of: {allowed}") from exc

    dimensions = _parse_sweep_dimensions(sweep_dimension)
    if dimensions and backtest_mode != BacktestMode.PARAMETER_SWEEP:
        raise typer.BadParameter("--sweep-dimension is only valid with --mode parameter_sweep")

    try:
        sweep_config = BacktestSweepConfig(
            base_mode=BacktestMode(sweep_base_mode),
            max_variants=sweep_max_variants,
            rank_metric=sweep_rank_metric,
            dimensions=dimensions,
        )
    except (ValueError, ValidationError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    request = BacktestRequest(
        start_date=start_date,
        end_date=end_date,
        strategy_ids=tuple(strategy_id or ()),
        symbols=tuple(symbol or ()),
        mode=backtest_mode,
        max_days=max_days,
        market_data_symbol_limit=market_data_symbol_limit,
        candidate_limit=candidate_limit,
        per_symbol_top=per_symbol_top,
        requested_by=requested_by,
        artifact_root=artifact_root,
        sweep=sweep_config,
    )
    result = BacktestEngine().run(request, db_target=db)
    typer.echo(json.dumps(result, indent=2, default=str))


__all__ = ["backtests_app", "run_backtest_command"]
