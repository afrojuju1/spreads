from __future__ import annotations

import csv
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import typer

from core.cli.ops_render import build_console, render_json_payload
from core.services.opportunity_replay import (
    OpportunityReplayLookupError,
    build_opportunity_replay,
    build_recent_opportunity_replay_batch,
)


def _write_json_export(path: str, payload: Mapping[str, Any]) -> None:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, default=str) + "\n")


def _write_csv_export(path: str, rows: list[dict[str, Any]]) -> None:
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _append_deployment_quality_lines(
    lines: list[str],
    deployment_quality: Mapping[str, Any] | None,
) -> None:
    if not isinstance(deployment_quality, Mapping):
        return
    allocator = deployment_quality.get("allocator_selected") or {}
    actual = deployment_quality.get("actual_deployed") or {}
    if not allocator and not actual:
        return
    lines.append("")
    lines.append("Deployment quality:")
    if allocator:
        lines.append(
            "- "
            f"allocator selected count {allocator.get('count')} "
            f"| pooled modeled close RoR {allocator.get('pooled_estimated_close_return_on_risk')} "
            f"| pooled modeled final RoR {allocator.get('pooled_estimated_final_return_on_risk')} "
            f"| pooled actual RoR {allocator.get('pooled_actual_net_return_on_risk')}"
        )
    if actual:
        lines.append(
            "- "
            f"actual deployed count {actual.get('count')} "
            f"| pooled actual RoR {actual.get('pooled_actual_net_return_on_risk')} "
            f"| pooled actual minus modeled close RoR {actual.get('pooled_actual_minus_estimated_close_return_on_risk')}"
        )


def _render_replay_text(payload: Mapping[str, Any]) -> str:
    session = payload.get("session") or {}
    summary = payload.get("summary") or {}
    comparison = payload.get("comparison") or {}
    scorecard = payload.get("scorecard") or {}
    opportunities = payload.get("opportunities") or []
    allocations = {
        row["opportunity_id"]: row
        for row in payload.get("allocation_decisions") or []
        if isinstance(row, Mapping)
    }

    lines = [
        f"Session: {session.get('label')} | {session.get('session_date')} | cycle {session.get('cycle_id')}",
        f"Style: {summary.get('style_profile')} | candidates {summary.get('candidate_count')} | promotable {summary.get('promotable_count')} | allocated {summary.get('allocated_count')}",
        f"Analysis verdict: {summary.get('analysis_verdict') or 'n/a'}",
        "",
        "Top opportunities:",
    ]
    for row in opportunities[:6]:
        if not isinstance(row, Mapping):
            continue
        allocation = allocations.get(str(row.get("opportunity_id"))) or {}
        lines.append(
            "- "
            f"{row.get('symbol')} {row.get('strategy_family')} "
            f"| candidate {row.get('candidate_id')} "
            f"| rank {row.get('rank')} "
            f"| state {row.get('state')} "
            f"| promo {row.get('promotion_score')} "
            f"| alloc {allocation.get('allocation_state', 'n/a')} "
            f"| alloc_score {allocation.get('allocation_score', 'n/a')} "
            f"| baseline {row.get('baseline_selection_state')} "
            f"| reason {allocation.get('allocation_reason', row.get('state_reason'))}"
        )
    if comparison:
        lines.append("")
        lines.append("Comparison:")
        lines.append(
            "- "
            f"promotable baseline ids {comparison.get('promotable_baseline', {}).get('candidate_ids', [])} "
            f"| symbols {comparison.get('promotable_baseline', {}).get('symbols', [])}"
        )
        lines.append(
            "- "
            f"rank-only top ids {comparison.get('rank_only_top', {}).get('candidate_ids', [])} "
            f"| symbols {comparison.get('rank_only_top', {}).get('symbols', [])}"
        )
        lines.append(
            "- "
            f"allocator ids {comparison.get('provisional_allocator', {}).get('candidate_ids', [])} "
            f"| symbols {comparison.get('provisional_allocator', {}).get('symbols', [])}"
        )
        promoted_monitor = comparison.get("allocator_promoted_monitor") or []
        if promoted_monitor:
            lines.append(
                "- "
                f"allocator-promoted monitor {[item.get('candidate_id') for item in promoted_monitor]}"
            )
        rejected_promotable_baseline = (
            comparison.get("allocator_rejected_promotable_baseline") or []
        )
        if rejected_promotable_baseline:
            lines.append("- rejected promotable baseline:")
            for item in rejected_promotable_baseline[:4]:
                lines.append(
                    "  "
                    f"{item.get('candidate_id')} {item.get('symbol')} {item.get('strategy_family')} "
                    f"| reason {item.get('allocation_reason')}"
                )
    if scorecard:
        allocator_metrics = scorecard.get("allocator_selected") or {}
        promotable_baseline_metrics = scorecard.get("promotable_baseline") or {}
        rank_only_metrics = scorecard.get("rank_only_top") or {}
        deltas = scorecard.get("deltas") or {}
        lines.append("")
        lines.append("Scorecard:")
        lines.append(
            "- "
            f"modeled final avg pnl | promotable baseline {promotable_baseline_metrics.get('average_estimated_pnl')} "
            f"| rank-only {rank_only_metrics.get('average_estimated_pnl')} "
            f"| allocator {allocator_metrics.get('average_estimated_pnl')}"
        )
        lines.append(
            "- "
            f"modeled close avg pnl | promotable baseline {promotable_baseline_metrics.get('average_estimated_close_pnl')} "
            f"| rank-only {rank_only_metrics.get('average_estimated_close_pnl')} "
            f"| allocator {allocator_metrics.get('average_estimated_close_pnl')}"
        )
        lines.append(
            "- "
            f"actual net avg pnl | promotable baseline {promotable_baseline_metrics.get('average_actual_net_pnl')} "
            f"| rank-only {rank_only_metrics.get('average_actual_net_pnl')} "
            f"| allocator {allocator_metrics.get('average_actual_net_pnl')}"
        )
        lines.append(
            "- "
            f"allocator final minus promotable baseline {deltas.get('allocator_minus_promotable_baseline_avg_estimated_pnl')} "
            f"| allocator close minus promotable baseline {deltas.get('allocator_minus_promotable_baseline_avg_estimated_close_pnl')} "
            f"| allocator actual minus promotable baseline {deltas.get('allocator_minus_promotable_baseline_avg_actual_net_pnl')}"
        )
        lines.append(
            "- "
            f"allocator modeled positive_rate {allocator_metrics.get('positive_rate')} "
            f"| allocator still_open_rate {allocator_metrics.get('still_open_rate')} "
            f"| allocator actual positive_rate {allocator_metrics.get('actual_positive_rate')}"
        )
        lines.append(
            "- "
            f"allocator actual coverage {allocator_metrics.get('actual_coverage_rate')} "
            f"| allocator actual closed_rate {allocator_metrics.get('actual_closed_rate')} "
            f"| monitor promotion hit rate {deltas.get('monitor_promotion_hit_rate')} "
            f"| rejected promotable baseline positive rate {deltas.get('rejected_promotable_baseline_positive_rate')}"
        )
        lines.append(
            "- "
            f"open fill rate | promotable baseline {promotable_baseline_metrics.get('open_fill_rate')} "
            f"| rank-only {rank_only_metrics.get('open_fill_rate')} "
            f"| allocator {allocator_metrics.get('open_fill_rate')}"
        )
        lines.append(
            "- "
            f"late open fill rate | promotable baseline {promotable_baseline_metrics.get('late_open_fill_rate')} "
            f"| rank-only {rank_only_metrics.get('late_open_fill_rate')} "
            f"| allocator {allocator_metrics.get('late_open_fill_rate')}"
        )
        lines.append(
            "- "
            f"force-close exit rate | promotable baseline {promotable_baseline_metrics.get('force_close_exit_rate')} "
            f"| rank-only {rank_only_metrics.get('force_close_exit_rate')} "
            f"| allocator {allocator_metrics.get('force_close_exit_rate')}"
        )
        lines.append(
            "- "
            f"entry credit capture | promotable baseline {promotable_baseline_metrics.get('average_entry_credit_capture_pct')} "
            f"| rank-only {rank_only_metrics.get('average_entry_credit_capture_pct')} "
            f"| allocator {allocator_metrics.get('average_entry_credit_capture_pct')}"
        )
        lines.append(
            "- "
            f"actual minus modeled close | promotable baseline {promotable_baseline_metrics.get('average_actual_minus_estimated_close_pnl')} "
            f"| rank-only {rank_only_metrics.get('average_actual_minus_estimated_close_pnl')} "
            f"| allocator {allocator_metrics.get('average_actual_minus_estimated_close_pnl')}"
        )
        _append_deployment_quality_lines(
            lines,
            scorecard.get("deployment_quality"),
        )
    warnings = payload.get("warnings") or []
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        for warning in warnings:
            lines.append(f"- {warning}")
    return "\n".join(lines)


def _render_replay_batch_text(payload: Mapping[str, Any]) -> str:
    target = payload.get("target") or {}
    aggregate = payload.get("aggregate") or {}
    sessions = payload.get("sessions") or []
    skipped_sessions = payload.get("skipped_sessions") or []
    warnings = payload.get("warnings") or []
    lines = [
        f"Recent sessions: {aggregate.get('session_count')} of requested {aggregate.get('requested_recent')} | label filter {target.get('label') or 'all'}",
        f"Skipped sessions: {aggregate.get('skipped_session_count')}",
        f"Allocator selections: {(aggregate.get('allocator_selected_metrics') or {}).get('count')} opportunities across {aggregate.get('sessions_with_allocator_selections')} sessions",
        f"Monitor promotions: {aggregate.get('promoted_monitor_total')} across {aggregate.get('sessions_with_monitor_promotions')} sessions",
        f"Rejected promotable baseline candidates: {aggregate.get('rejected_promotable_baseline_total')} across {aggregate.get('sessions_with_rejected_promotable_baseline')} sessions",
        f"Average overlap | allocator vs promotable baseline {aggregate.get('average_allocator_vs_promotable_baseline_overlap')} | allocator vs rank-only {aggregate.get('average_allocator_vs_rank_only_overlap')}",
        f"Pooled modeled final pnl | promotable baseline {(aggregate.get('promotable_baseline_metrics') or {}).get('average_estimated_pnl')} | rank-only {(aggregate.get('rank_only_top_metrics') or {}).get('average_estimated_pnl')} | allocator {(aggregate.get('allocator_selected_metrics') or {}).get('average_estimated_pnl')}",
        f"Pooled modeled close pnl | promotable baseline {(aggregate.get('promotable_baseline_metrics') or {}).get('average_estimated_close_pnl')} | rank-only {(aggregate.get('rank_only_top_metrics') or {}).get('average_estimated_close_pnl')} | allocator {(aggregate.get('allocator_selected_metrics') or {}).get('average_estimated_close_pnl')}",
        f"Pooled actual net pnl | promotable baseline {(aggregate.get('promotable_baseline_metrics') or {}).get('average_actual_net_pnl')} | rank-only {(aggregate.get('rank_only_top_metrics') or {}).get('average_actual_net_pnl')} | allocator {(aggregate.get('allocator_selected_metrics') or {}).get('average_actual_net_pnl')}",
        f"Pooled actual minus modeled close | promotable baseline {(aggregate.get('promotable_baseline_metrics') or {}).get('average_actual_minus_estimated_close_pnl')} | rank-only {(aggregate.get('rank_only_top_metrics') or {}).get('average_actual_minus_estimated_close_pnl')} | allocator {(aggregate.get('allocator_selected_metrics') or {}).get('average_actual_minus_estimated_close_pnl')}",
        f"Still-open rate | promotable baseline {(aggregate.get('promotable_baseline_metrics') or {}).get('still_open_rate')} | rank-only {(aggregate.get('rank_only_top_metrics') or {}).get('still_open_rate')} | allocator {(aggregate.get('allocator_selected_metrics') or {}).get('still_open_rate')}",
        f"Pooled deltas | final {aggregate.get('allocator_minus_promotable_baseline_avg_estimated_pnl')} | close {aggregate.get('allocator_minus_promotable_baseline_avg_estimated_close_pnl')} | actual {aggregate.get('allocator_minus_promotable_baseline_avg_actual_net_pnl')}",
        f"Allocator actual coverage {(aggregate.get('allocator_selected_metrics') or {}).get('actual_coverage_rate')} | allocator actual closed_rate {(aggregate.get('allocator_selected_metrics') or {}).get('actual_closed_rate')}",
        f"Execution quality | open fill {(aggregate.get('allocator_selected_metrics') or {}).get('open_fill_rate')} | late open fill {(aggregate.get('allocator_selected_metrics') or {}).get('late_open_fill_rate')} | force-close exits {(aggregate.get('allocator_selected_metrics') or {}).get('force_close_exit_rate')}",
        f"Entry capture | promotable baseline {(aggregate.get('promotable_baseline_metrics') or {}).get('average_entry_credit_capture_pct')} | rank-only {(aggregate.get('rank_only_top_metrics') or {}).get('average_entry_credit_capture_pct')} | allocator {(aggregate.get('allocator_selected_metrics') or {}).get('average_entry_credit_capture_pct')}",
        f"Hit rates | monitor promotions {aggregate.get('monitor_promotion_hit_rate')} | rejected promotable baseline positive rate {aggregate.get('rejected_promotable_baseline_positive_rate')}",
        f"Verdicts: {aggregate.get('verdict_counts')}",
        "",
        "Sessions:",
    ]
    for item in sessions:
        if not isinstance(item, Mapping):
            continue
        session = item.get("session") or {}
        summary = item.get("summary") or {}
        comparison = item.get("comparison") or {}
        promoted = comparison.get("allocator_promoted_monitor") or []
        rejected = comparison.get("allocator_rejected_promotable_baseline") or []
        lines.append(
            "- "
            f"{session.get('label')} {session.get('session_date')} "
            f"| verdict {summary.get('analysis_verdict') or 'n/a'} "
            f"| allocated {summary.get('allocated_count')} "
            f"| late_open_fill_rate {(item.get('scorecard') or {}).get('allocator_selected', {}).get('late_open_fill_rate')} "
            f"| force_close_exit_rate {(item.get('scorecard') or {}).get('allocator_selected', {}).get('force_close_exit_rate')} "
            f"| promoted_monitor {[row.get('candidate_id') for row in promoted]} "
            f"| rejected_promotable_baseline {[row.get('candidate_id') for row in rejected]}"
        )
    if skipped_sessions:
        lines.append("")
        lines.append("Skipped:")
        for item in skipped_sessions:
            if not isinstance(item, Mapping):
                continue
            lines.append(
                "- "
                f"{item.get('label')} {item.get('session_date')} | reason {item.get('reason')}"
            )
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        for warning in warnings:
            lines.append(f"- {warning}")
    _append_deployment_quality_lines(
        lines,
        aggregate.get("deployment_quality"),
    )
    return "\n".join(lines)


def _export_payload(
    *,
    payload: Mapping[str, Any],
    export_json: str | None,
    export_csv: str | None,
) -> None:
    if export_json:
        _write_json_export(export_json, payload)
    if export_csv:
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            rows = []
        _write_csv_export(
            export_csv,
            [dict(row) for row in rows if isinstance(row, Mapping)],
        )


def _emit_payload(
    *,
    payload: Mapping[str, Any],
    json_output: bool,
    no_color: bool,
    batch: bool,
) -> None:
    console = build_console(no_color=no_color)
    if json_output:
        render_json_payload(console, dict(payload))
        return
    console.print(
        _render_replay_batch_text(payload) if batch else _render_replay_text(payload)
    )


def _validate_single_target(
    *,
    session_id: str | None,
    label: str | None,
    session_date: str | None,
) -> None:
    if session_id is not None and (label is not None or session_date is not None):
        raise ValueError("session id cannot be combined with --label or --date.")
    if session_date is not None and label is None:
        raise ValueError("--date requires --label.")


def _validate_recent_limit(value: int) -> int:
    if value <= 0:
        raise ValueError("--limit must be greater than 0.")
    return value


def _run_single_replay(
    *,
    session_id: str | None,
    label: str | None,
    session_date: str | None,
    db: str | None,
    json_output: bool,
    no_color: bool,
    export_json: str | None,
    export_csv: str | None,
) -> None:
    _validate_single_target(
        session_id=session_id,
        label=label,
        session_date=session_date,
    )
    payload = build_opportunity_replay(
        db_target=db,
        session_id=session_id,
        label=label,
        session_date=session_date,
    )
    _export_payload(
        payload=payload,
        export_json=export_json,
        export_csv=export_csv,
    )
    _emit_payload(
        payload=payload,
        json_output=json_output,
        no_color=no_color,
        batch=False,
    )


def _run_recent_replay(
    *,
    limit: int,
    label: str | None,
    db: str | None,
    json_output: bool,
    no_color: bool,
    export_json: str | None,
    export_csv: str | None,
) -> None:
    payload = build_recent_opportunity_replay_batch(
        db_target=db,
        recent=_validate_recent_limit(limit),
        label=label,
    )
    _export_payload(
        payload=payload,
        export_json=export_json,
        export_csv=export_csv,
    )
    _emit_payload(
        payload=payload,
        json_output=json_output,
        no_color=no_color,
        batch=True,
    )


def _handle_replay_error(exc: Exception) -> None:
    typer.secho(str(exc), err=True, fg=typer.colors.RED)
    raise typer.Exit(3) from None


replay_app = typer.Typer(
    add_completion=False,
    help="Replay offline opportunity-selection decisions.",
    invoke_without_command=True,
    no_args_is_help=False,
)


@replay_app.callback(invoke_without_command=True)
def replay_command(
    ctx: typer.Context,
    session_id: str | None = typer.Option(
        None,
        "--session-id",
        help="Session id to replay. If omitted, replay the latest available session.",
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Collector label to replay.",
    ),
    date: str | None = typer.Option(
        None,
        "--date",
        help="Session date in YYYY-MM-DD.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    export_json: str | None = typer.Option(
        None,
        "--export-json",
        help="Write the full replay payload to a JSON file.",
    ),
    export_csv: str | None = typer.Option(
        None,
        "--export-csv",
        help="Write flattened opportunity rows to a CSV file.",
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return
    try:
        _run_single_replay(
            session_id=session_id,
            label=label,
            session_date=date,
            db=db,
            json_output=json_output,
            no_color=no_color,
            export_json=export_json,
            export_csv=export_csv,
        )
    except (OpportunityReplayLookupError, ValueError) as exc:
        _handle_replay_error(exc)


@replay_app.command("recent", help="Build a batch replay across recent sessions.")
def replay_recent_command(
    limit: int = typer.Option(
        5,
        "--limit",
        "--recent",
        help="Maximum replayable sessions to include.",
    ),
    label: str | None = typer.Option(
        None,
        "--label",
        help="Collector label to replay.",
    ),
    db: str | None = typer.Option(None, "--db", help="Database URL override."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output."),
    export_json: str | None = typer.Option(
        None,
        "--export-json",
        help="Write the full replay payload to a JSON file.",
    ),
    export_csv: str | None = typer.Option(
        None,
        "--export-csv",
        help="Write flattened opportunity rows to a CSV file.",
    ),
    no_color: bool = typer.Option(False, "--no-color", help="Disable ANSI colors."),
) -> None:
    try:
        _run_recent_replay(
            limit=limit,
            label=label,
            db=db,
            json_output=json_output,
            no_color=no_color,
            export_json=export_json,
            export_csv=export_csv,
        )
    except (OpportunityReplayLookupError, ValueError) as exc:
        _handle_replay_error(exc)


def main() -> None:
    replay_app()


__all__ = ["main", "replay_app"]
