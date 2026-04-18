from __future__ import annotations

import json
from typing import Any

import typer

from core.services.discovery_sessions import (
    get_discovery_session_detail,
    list_discovery_sessions,
)
from core.services.automation_runtimes import (
    get_automation_runtime_detail,
    list_automation_runtimes,
)
from core.services.opportunities import (
    get_opportunity_detail,
    list_opportunities,
)
from core.services.positions import get_position_detail, list_positions


def _print_payload(payload: dict[str, Any], *, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    typer.echo(json.dumps(payload, indent=2, default=str))


def pipelines_command(
    pipeline_id: str | None = typer.Argument(None, help="Pipeline id to inspect."),
    date: str | None = typer.Option(None, "--date", help="Optional market date."),
    limit: int = typer.Option(25, "--limit", help="Maximum pipelines to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    payload = (
        list_discovery_sessions(db_target=db, limit=limit, market_date=date)
        if pipeline_id is None
        else get_discovery_session_detail(
            db_target=db,
            pipeline_id=pipeline_id,
            market_date=date,
            profit_target=0.5,
            stop_multiple=2.0,
        )
    )
    _print_payload(payload, json_output=json_output)


def automations_command(
    bot_id: str | None = typer.Option(None, "--bot-id", help="Target bot id."),
    automation_id: str | None = typer.Option(
        None,
        "--automation-id",
        help="Target automation id.",
    ),
    date: str | None = typer.Option(None, "--date", help="Optional market date."),
    limit: int = typer.Option(50, "--limit", help="Maximum runtimes to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    if (bot_id is None) ^ (automation_id is None):
        raise typer.BadParameter(
            "--bot-id and --automation-id must be supplied together for detail view."
        )
    payload = (
        list_automation_runtimes(db_target=db, limit=limit, market_date=date)
        if bot_id is None or automation_id is None
        else get_automation_runtime_detail(
            db_target=db,
            bot_id=bot_id,
            automation_id=automation_id,
            market_date=date,
            limit=limit,
        )
    )
    _print_payload(payload, json_output=json_output)


def opportunities_command(
    opportunity_id: str | None = typer.Argument(None, help="Opportunity id to inspect."),
    pipeline_id: str | None = typer.Option(None, "--pipeline-id", help="Optional pipeline filter."),
    label: str | None = typer.Option(None, "--label", help="Optional discovery label filter."),
    bot_id: str | None = typer.Option(None, "--bot-id", help="Optional bot owner filter."),
    automation_id: str | None = typer.Option(
        None,
        "--automation-id",
        help="Optional automation owner filter.",
    ),
    strategy_config_id: str | None = typer.Option(
        None,
        "--strategy-config-id",
        help="Optional strategy-config owner filter.",
    ),
    date: str | None = typer.Option(None, "--date", help="Optional market date."),
    limit: int = typer.Option(50, "--limit", help="Maximum opportunities to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    payload = (
        list_opportunities(
            db_target=db,
            pipeline_id=pipeline_id,
            label=label,
            market_date=date,
            bot_id=bot_id,
            automation_id=automation_id,
            strategy_config_id=strategy_config_id,
            limit=limit,
        )
        if opportunity_id is None
        else get_opportunity_detail(db_target=db, opportunity_id=opportunity_id)
    )
    _print_payload(payload, json_output=json_output)


def positions_command(
    position_id: str | None = typer.Argument(None, help="Position id to inspect."),
    pipeline_id: str | None = typer.Option(None, "--pipeline-id", help="Optional pipeline filter."),
    label: str | None = typer.Option(None, "--label", help="Optional discovery label filter."),
    bot_id: str | None = typer.Option(None, "--bot-id", help="Optional bot owner filter."),
    automation_id: str | None = typer.Option(
        None,
        "--automation-id",
        help="Optional automation owner filter.",
    ),
    strategy_config_id: str | None = typer.Option(
        None,
        "--strategy-config-id",
        help="Optional strategy-config owner filter.",
    ),
    date: str | None = typer.Option(None, "--date", help="Optional market date."),
    limit: int = typer.Option(50, "--limit", help="Maximum positions to list."),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    payload = (
        list_positions(
            db_target=db,
            pipeline_id=pipeline_id,
            label=label,
            market_date=date,
            bot_id=bot_id,
            automation_id=automation_id,
            strategy_config_id=strategy_config_id,
            limit=limit,
        )
        if position_id is None
        else get_position_detail(db_target=db, position_id=position_id)
    )
    _print_payload(payload, json_output=json_output)
