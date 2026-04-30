from __future__ import annotations

import json
from typing import Any

import typer

from core.runtime.config import default_database_url, default_redis_url
from core.services.company_valuation import (
    CompanyValuationClusteringRequest,
    CompanyValuationBootstrapRequest,
    CompanyValuationClassificationBackfillRequest,
    CompanyValuationResearchExportRequest,
    DEFAULT_RESEARCH_TEMPLATE_IDS,
    CompanyValuationScreenRefreshRequest,
    CompanyValuationTaxonomySyncRequest,
    ResolveUnresolvedInstitutionalPositionsRequest,
    analyze_company_valuation_research_dataset,
    backfill_company_valuation_raw_classification,
    enqueue_company_valuation_bootstrap_job,
    enqueue_company_valuation_resolve_unresolved_job,
    enqueue_company_valuation_screen_materialize_job,
    export_company_valuation_research_dataset,
    list_company_valuation_screen,
    sync_company_valuation_taxonomy_state,
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
    supported_only: bool = typer.Option(
        True,
        "--supported-only/--all-issuers",
        help="Default to the curated supported issuer universe instead of the full issuer table.",
    ),
    stressed_operator_only: bool = typer.Option(
        False,
        "--stressed-operator-only",
        help="Restrict the recompute/materialization scope to stressed-operator overlay issuers.",
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
                supported_only=supported_only,
                stressed_operator_only=stressed_operator_only,
                config_root=config_root,
            ),
            db_target=db,
            redis_url=redis_url,
        )
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    _render_payload(job.to_payload(), json_output=json_output)
    if not json_output:
        typer.echo(f"supported_only={supported_only}")
        typer.echo(f"stressed_operator_only={stressed_operator_only}")


@company_valuation_app.command(
    "screen-show",
    help="Show the latest company valuation screen with support-aware filters.",
)
def company_valuation_screen_show_command(
    ticker: list[str] = typer.Option(
        [],
        "--ticker",
        help="Optional ticker filter. Repeat the option for multiple tickers.",
    ),
    template_id: str | None = typer.Option(
        None,
        "--template-id",
        help="Optional base template filter. Use stressed_operator with --stressed-operator-only semantics.",
    ),
    limit: int = typer.Option(
        25,
        "--limit",
        min=1,
        max=1000,
        help="Maximum screening rows to return.",
    ),
    as_of: str | None = typer.Option(
        None,
        "--as-of",
        help="Optional as-of date override.",
    ),
    supported_only: bool = typer.Option(
        True,
        "--supported-only/--all-issuers",
        help="Default to the curated supported issuer universe instead of the full issuer table.",
    ),
    stressed_operator_only: bool = typer.Option(
        False,
        "--stressed-operator-only",
        help="Restrict the screen to stressed-operator overlay issuers.",
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
    try:
        payload = list_company_valuation_screen(
            as_of=as_of,
            template_id=template_id,
            tickers=tuple(ticker) or None,
            limit=limit,
            supported_only=supported_only,
            stressed_operator_only=stressed_operator_only,
            repository=None if db == default_db else CompanyValuationRepository(db),
            config_root=config_root,
        )
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    if json_output:
        typer.echo(json.dumps(payload, sort_keys=True, default=str))
        return
    typer.echo(f"as_of={payload['as_of']}")
    typer.echo(f"count={payload['count']}")
    typer.echo(f"supported_only={payload['supported_only']}")
    typer.echo(f"stressed_operator_only={payload['stressed_operator_only']}")
    typer.echo(
        "support_status_counts="
        + json.dumps(payload["support_status_counts"], sort_keys=True)
    )
    for row in payload["rows"]:
        typer.echo(
            "row="
            + json.dumps(
                {
                    "ticker": row.get("ticker"),
                    "template_id": row.get("template_id"),
                    "effective_template_id": row.get("effective_template_id"),
                    "support_status": row.get("support_status"),
                    "valuation_gap": row.get("valuation_gap"),
                    "quality_score": row.get("quality_score"),
                    "stressed_operator_flag": row.get("stressed_operator_flag"),
                },
                sort_keys=True,
                default=str,
            )
        )


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


@company_valuation_app.command(
    "cluster-research-dataset",
    help="Run offline clustering and template-discovery analysis on a valuation research dataset.",
)
def company_valuation_cluster_research_dataset_command(
    dataset_root: str = typer.Option(
        ...,
        "--dataset-root",
        help="Root path for the parquet or jsonl research dataset.",
    ),
    output_root: str | None = typer.Option(
        None,
        "--output-root",
        help="Optional output directory for clustering artifacts.",
    ),
    template_id: list[str] = typer.Option(
        [],
        "--template-id",
        help="Optional template filter. Repeat for multiple cohorts.",
    ),
    min_k: int = typer.Option(
        2,
        "--min-k",
        min=2,
        help="Minimum k to consider for MiniBatchKMeans.",
    ),
    max_k: int = typer.Option(
        6,
        "--max-k",
        min=2,
        help="Maximum k to consider for MiniBatchKMeans.",
    ),
    min_rows_per_cluster: int = typer.Option(
        12,
        "--min-rows-per-cluster",
        min=4,
        help="Minimum target cluster size for HDBSCAN and research summaries.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    try:
        result = analyze_company_valuation_research_dataset(
            CompanyValuationClusteringRequest(
                dataset_root=dataset_root,
                output_root=output_root,
                template_ids=tuple(template_id) or None,
                min_k=min_k,
                max_k=max_k,
                min_rows_per_cluster=min_rows_per_cluster,
            )
        )
    except Exception as exc:
        typer.secho(f"Command failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(2) from None
    payload = result.to_payload()
    if json_output:
        typer.echo(json.dumps(payload, sort_keys=True, default=str))
        return
    typer.echo(f"status={payload['status']}")
    typer.echo(f"assignment_count={payload['assignment_count']}")
    typer.echo(f"output_root={payload['output_root']}")
    typer.echo(f"summary_path={payload['summary_path']}")
    typer.echo(f"markdown_path={payload['markdown_path']}")


@company_valuation_app.command(
    "taxonomy-sync",
    help="Populate taxonomy shadow state without changing active valuation template behavior.",
)
def company_valuation_taxonomy_sync_command(
    ticker: list[str] = typer.Option(
        [],
        "--ticker",
        help="Optional ticker filter. Repeat the option for multiple issuers.",
    ),
    cik: list[str] = typer.Option(
        [],
        "--cik",
        help="Optional CIK filter. Repeat the option for multiple issuers.",
    ),
    issuer_id: list[str] = typer.Option(
        [],
        "--issuer-id",
        help="Optional issuer_id filter. Repeat the option for multiple issuers.",
    ),
    issuer_limit: int | None = typer.Option(
        None,
        "--issuer-limit",
        min=1,
        help="Optional issuer cap for the taxonomy shadow sync.",
    ),
    supported_only: bool = typer.Option(
        False,
        "--supported-only",
        help="Restrict the taxonomy shadow sync to the curated supported issuer universe.",
    ),
    sample_limit: int = typer.Option(
        20,
        "--sample-limit",
        min=1,
        help="Maximum mismatch and unclassified samples to include in the result.",
    ),
    output_root: str | None = typer.Option(
        None,
        "--output-root",
        help="Optional directory for manifest, markdown summary, and full mismatch/unclassified reports.",
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
    try:
        result = sync_company_valuation_taxonomy_state(
            CompanyValuationTaxonomySyncRequest(
                tickers=tuple(ticker) or None,
                ciks=tuple(cik) or None,
                issuer_ids=tuple(issuer_id) or None,
                issuer_limit=issuer_limit,
                supported_only=supported_only,
                config_root=config_root,
                sample_limit=sample_limit,
                output_root=output_root,
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
    typer.echo(f"issuers_considered={payload['issuers_considered']}")
    typer.echo(f"taxonomy_nodes_upserted={payload['taxonomy_nodes_upserted']}")
    typer.echo(f"taxonomy_mappings_upserted={payload['taxonomy_mappings_upserted']}")
    typer.echo(
        f"valuation_template_mappings_upserted={payload['valuation_template_mappings_upserted']}"
    )
    typer.echo(
        f"issuer_classifications_upserted={payload['issuer_classifications_upserted']}"
    )
    typer.echo(
        f"issuer_overlay_flags_replaced={payload['issuer_overlay_flags_replaced']}"
    )
    typer.echo(f"unclassified_count={payload['unclassified_count']}")
    typer.echo(f"supported_unclassified_count={payload['supported_unclassified_count']}")
    typer.echo(f"template_mismatch_count={payload['template_mismatch_count']}")
    typer.echo(
        f"supported_template_mismatch_count={payload['supported_template_mismatch_count']}"
    )
    typer.echo(
        f"expected_template_mismatch_count={payload['expected_template_mismatch_count']}"
    )
    typer.echo(f"taxonomy_override_count={payload['taxonomy_override_count']}")
    typer.echo(
        f"current_template_override_count={payload['current_template_override_count']}"
    )
    typer.echo(
        "overlay_true_counts="
        + json.dumps(payload["overlay_true_counts"], sort_keys=True)
    )
    typer.echo(
        "classification_source_counts="
        + json.dumps(payload["classification_source_counts"], sort_keys=True)
    )
    typer.echo(
        "support_status_counts="
        + json.dumps(payload["support_status_counts"], sort_keys=True)
    )
    typer.echo(
        "template_mismatch_pair_counts="
        + json.dumps(payload["template_mismatch_pair_counts"], sort_keys=True)
    )
    if payload.get("output_root"):
        typer.echo(f"output_root={payload['output_root']}")
        typer.echo(f"manifest_path={payload['manifest_path']}")
        typer.echo(f"markdown_path={payload['markdown_path']}")
        typer.echo(f"mismatch_report_path={payload['mismatch_report_path']}")
        typer.echo(f"unclassified_report_path={payload['unclassified_report_path']}")
    if payload["notes"]:
        typer.echo("notes=" + "; ".join(payload["notes"]))


@company_valuation_app.command(
    "classification-backfill",
    help="Backfill raw SEC SIC metadata for existing issuers and optionally refresh taxonomy shadow state.",
)
def company_valuation_classification_backfill_command(
    ticker: list[str] = typer.Option(
        [],
        "--ticker",
        help="Optional ticker filter. Repeat the option for multiple issuers.",
    ),
    cik: list[str] = typer.Option(
        [],
        "--cik",
        help="Optional CIK filter. Repeat the option for multiple issuers.",
    ),
    issuer_id: list[str] = typer.Option(
        [],
        "--issuer-id",
        help="Optional issuer_id filter. Repeat the option for multiple issuers.",
    ),
    issuer_limit: int | None = typer.Option(
        None,
        "--issuer-limit",
        min=1,
        help="Optional issuer cap for the SEC classification refresh.",
    ),
    supported_only: bool = typer.Option(
        False,
        "--supported-only",
        help="Restrict the backfill to the curated supported issuer universe.",
    ),
    missing_only: bool = typer.Option(
        True,
        "--missing-only/--all",
        help="Only refresh issuers missing SIC and SIC description by default.",
    ),
    sync_taxonomy_shadow: bool = typer.Option(
        True,
        "--sync-taxonomy-shadow/--no-sync-taxonomy-shadow",
        help="Refresh taxonomy shadow state for issuers whose raw classification changed.",
    ),
    taxonomy_output_root: str | None = typer.Option(
        None,
        "--taxonomy-output-root",
        help="Optional output directory for taxonomy shadow-sync report artifacts.",
    ),
    sample_limit: int = typer.Option(
        20,
        "--sample-limit",
        min=1,
        help="Maximum sample issuer updates to include in the result payload.",
    ),
    db: str = typer.Option(
        default_database_url(),
        "--db",
        help="Database URL override.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
) -> None:
    default_db = default_database_url()
    try:
        result = backfill_company_valuation_raw_classification(
            CompanyValuationClassificationBackfillRequest(
                tickers=tuple(ticker) or None,
                ciks=tuple(cik) or None,
                issuer_ids=tuple(issuer_id) or None,
                issuer_limit=issuer_limit,
                supported_only=supported_only,
                missing_only=missing_only,
                sync_taxonomy_shadow=sync_taxonomy_shadow,
                taxonomy_output_root=taxonomy_output_root,
                sample_limit=sample_limit,
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
    typer.echo(f"issuers_requested={payload['issuers_requested']}")
    typer.echo(f"issuers_considered={payload['issuers_considered']}")
    typer.echo(f"sec_profiles_loaded={payload['sec_profiles_loaded']}")
    typer.echo(f"issuers_updated={payload['issuers_updated']}")
    typer.echo(f"sic_updates={payload['sic_updates']}")
    typer.echo(f"sic_description_updates={payload['sic_description_updates']}")
    typer.echo(f"naics_updates={payload['naics_updates']}")
    typer.echo(f"unchanged_count={payload['unchanged_count']}")
    typer.echo(f"skipped_count={payload['skipped_count']}")
    typer.echo(f"error_count={len(payload['errors'])}")
    if payload["taxonomy_sync"] is not None:
        typer.echo(
            "taxonomy_sync="
            + json.dumps(payload["taxonomy_sync"], sort_keys=True, default=str)
        )
    if payload["notes"]:
        typer.echo("notes=" + "; ".join(payload["notes"]))


__all__ = ["company_valuation_app"]
