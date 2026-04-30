from __future__ import annotations

import json
from typing import Any

import typer

from core.runtime.config import default_database_url, default_redis_url
from core.services.company_valuation import (
    CompanyValuationBootstrapRequest,
    CompanyValuationResearchExportRequest,
    DEFAULT_RESEARCH_TEMPLATE_IDS,
    CompanyValuationScreenRefreshRequest,
    ResolveUnresolvedInstitutionalPositionsRequest,
    enqueue_company_valuation_bootstrap_job,
    enqueue_company_valuation_resolve_unresolved_job,
    enqueue_company_valuation_screen_materialize_job,
    export_company_valuation_research_dataset,
)
from core.storage.company_valuation_repository import CompanyValuationRepository

company_valuation_app = typer.Typer(
    add_completion=False,
    help="Queue company valuation ingestion, recompute, and ownership resolution jobs.",
)


def _render_payload(payload: dict[str, Any], *, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps(payload, sort_keys=True, default=str))
        return
    typer.echo(f"job_run_id={payload['job_run_id']}")
    typer.echo(f"job_key={payload['job_key']}")
    typer.echo(f"job_type={payload['job_type']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"scheduled_for={payload['scheduled_for']}")


@company_valuation_app.command("bootstrap", help="Queue company valuation bootstrap work.")
def company_valuation_bootstrap_command(
    ticker: list[str] = typer.Option(
        ...,
        "--ticker",
        help="Ticker to bootstrap. Repeat the option for multiple tickers.",
    ),
    bootstrap_universe: bool = typer.Option(
        False,
        "--bootstrap-universe",
        help="Refresh the SEC issuer/security universe before ticker work.",
    ),
    universe_limit: int | None = typer.Option(
        None,
        "--universe-limit",
        help="Limit the SEC universe bootstrap rows.",
    ),
    filings_since: str | None = typer.Option(
        None,
        "--filings-since",
        help="Only ingest filings available on or after this ISO timestamp.",
    ),
    ownership_since: str | None = typer.Option(
        None,
        "--ownership-since",
        help="Only ingest ownership filings on or after this ISO timestamp.",
    ),
    config_root: str | None = typer.Option(
        None,
        "--config-root",
        help="Override the config root passed into company valuation services.",
    ),
    db: str = typer.Option(
        default_database_url(),
        "--db",
        help="Database URL override.",
    ),
    redis_url: str = typer.Option(
        default_redis_url(),
        "--redis-url",
        help="Redis URL override.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    try:
        job = enqueue_company_valuation_bootstrap_job(
            CompanyValuationBootstrapRequest(
                tickers=tuple(ticker),
                bootstrap_universe=bootstrap_universe,
                universe_limit=universe_limit,
                filings_since=filings_since,
                ownership_since=ownership_since,
                config_root=config_root,
            ),
            db_target=db,
            redis_url=redis_url,
        )
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    _render_payload(job.to_payload(), json_output=json_output)


@company_valuation_app.command(
    "screen-refresh",
    help="Queue a company valuation screen recompute/materialization job.",
)
def company_valuation_screen_refresh_command(
    ticker: list[str] = typer.Option(
        [],
        "--ticker",
        help="Optional ticker filter. Repeat the option for multiple tickers.",
    ),
    template_id: str | None = typer.Option(
        None,
        "--template-id",
        help="Optional template filter.",
    ),
    issuer_limit: int | None = typer.Option(
        None,
        "--issuer-limit",
        help="Optional issuer cap for the recompute batch.",
    ),
    as_of: str | None = typer.Option(
        None,
        "--as-of",
        help="Optional as-of timestamp override.",
    ),
    config_root: str | None = typer.Option(
        None,
        "--config-root",
        help="Override the config root passed into company valuation services.",
    ),
    db: str = typer.Option(
        default_database_url(),
        "--db",
        help="Database URL override.",
    ),
    redis_url: str = typer.Option(
        default_redis_url(),
        "--redis-url",
        help="Redis URL override.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    try:
        job = enqueue_company_valuation_screen_materialize_job(
            CompanyValuationScreenRefreshRequest(
                as_of=as_of,
                template_id=template_id,
                tickers=tuple(ticker) or None,
                issuer_limit=issuer_limit,
                config_root=config_root,
            ),
            db_target=db,
            redis_url=redis_url,
        )
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    _render_payload(job.to_payload(), json_output=json_output)


@company_valuation_app.command(
    "resolve-unresolved",
    help="Queue unresolved 13F CUSIP resolution work.",
)
def company_valuation_resolve_unresolved_command(
    report_period: str | None = typer.Option(
        None,
        "--report-period",
        help="Optional YYYY-MM-DD report period filter.",
    ),
    limit_rows: int = typer.Option(
        20000,
        "--limit-rows",
        help="Maximum unresolved rows to attempt in one run.",
    ),
    batch_cusips: int = typer.Option(
        50,
        "--batch-cusips",
        help="Maximum CUSIPs to send to OpenFIGI in one batch.",
    ),
    max_attempts: int = typer.Option(
        5,
        "--max-attempts",
        help="Maximum retry attempts before marking unresolved rows failed.",
    ),
    db: str = typer.Option(
        default_database_url(),
        "--db",
        help="Database URL override.",
    ),
    redis_url: str = typer.Option(
        default_redis_url(),
        "--redis-url",
        help="Redis URL override.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    try:
        job = enqueue_company_valuation_resolve_unresolved_job(
            ResolveUnresolvedInstitutionalPositionsRequest(
                report_period=report_period,
                limit_rows=limit_rows,
                batch_cusips=batch_cusips,
                max_attempts=max_attempts,
            ),
            db_target=db,
            redis_url=redis_url,
        )
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    _render_payload(job.to_payload(), json_output=json_output)


@company_valuation_app.command(
    "export-research-dataset",
    help="Export point-in-time company valuation research rows for offline calibration.",
)
def company_valuation_export_research_dataset_command(
    years: int = typer.Option(
        10,
        "--years",
        min=1,
        help="Historical window in years. Defaults to 10 for the research export.",
    ),
    template_id: list[str] = typer.Option(
        list(DEFAULT_RESEARCH_TEMPLATE_IDS),
        "--template-id",
        help="Template cohort to export. Repeat for multiple cohorts.",
    ),
    ticker: list[str] = typer.Option(
        [],
        "--ticker",
        help="Optional ticker filter. Repeat for multiple tickers.",
    ),
    issuer_limit: int | None = typer.Option(
        None,
        "--issuer-limit",
        help="Optional cap on issuers included in the export.",
    ),
    output_root: str = typer.Option(
        "outputs/company_valuation/research",
        "--output-root",
        help="Root directory for the research dataset export.",
    ),
    output_format: str = typer.Option(
        "parquet",
        "--format",
        help="Dataset format: parquet or jsonl.",
    ),
    include_market_context: bool = typer.Option(
        True,
        "--include-market-context/--no-include-market-context",
        help="Include historical market context derived from Alpaca daily stock bars.",
    ),
    config_root: str | None = typer.Option(
        None,
        "--config-root",
        help="Override the config root passed into company valuation services.",
    ),
    db: str = typer.Option(
        default_database_url(),
        "--db",
        help="Database URL override.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    default_db = default_database_url()
    if output_format not in {"parquet", "jsonl"}:
        typer.secho(
            f"Unsupported format {output_format!r}; expected parquet or jsonl.",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(2)
    try:
        result = export_company_valuation_research_dataset(
            CompanyValuationResearchExportRequest(
                years=years,
                template_ids=tuple(template_id),
                tickers=tuple(ticker) or None,
                issuer_limit=issuer_limit,
                output_root=output_root,
                output_format=output_format,
                config_root=config_root,
                include_market_context=include_market_context,
            ),
            repository=None if db == default_db else CompanyValuationRepository(db),
        )
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    payload = result.to_payload()
    if json_output:
        typer.echo(json.dumps(payload, sort_keys=True, default=str))
        return
    typer.echo(f"status={payload['status']}")
    typer.echo(f"years={payload['years']}")
    typer.echo(f"row_count={payload['row_count']}")
    typer.echo(f"issuers_considered={payload['issuers_considered']}")
    typer.echo(f"issuers_exported={payload['issuers_exported']}")
    typer.echo(f"output_root={payload['output_root']}")
    typer.echo(f"manifest_path={payload['manifest_path']}")


__all__ = ["company_valuation_app"]
