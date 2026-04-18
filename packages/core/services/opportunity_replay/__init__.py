from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from core.db.decorators import with_storage
from core.domain.opportunity_models import DecisionReplay
from core.services.opportunity_execution_plan import (
    build_allocation_decisions,
    build_execution_intents,
)

from .builders import (
    _build_historical_dimension_lookup,
    _build_horizon_intents as _build_horizon_intents,
    _build_opportunities as _build_opportunities,
    _build_regime_snapshots as _build_regime_snapshots,
    _build_strategy_intents as _build_strategy_intents,
)
from .loading import (
    OpportunityReplayLookupError as OpportunityReplayLookupError,
    _load_cycle_candidates,
    _resolve_recent_analysis_targets,
    _resolve_target,
)
from .matches import _build_outcome_matches
from .reporting import (
    _aggregate_dimension_rows,
    _build_comparison,
    _build_deployment_quality_views,
    _build_scorecard,
    _build_summary,
    _flatten_opportunity_rows as _flatten_opportunity_rows,
    _summarize_outcome_rows,
)
from .shared import _as_float, _as_text


@with_storage()
def build_opportunity_replay(
    *,
    db_target: str | None = None,
    session_id: str | None = None,
    label: str | None = None,
    session_date: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    cycle, analysis_run = _resolve_target(
        storage=storage,
        session_id=session_id,
        label=label,
        session_date=session_date,
    )
    candidate_rows, recovery_warnings = _load_cycle_candidates(
        storage=storage,
        cycle=cycle,
    )
    if not candidate_rows:
        raise OpportunityReplayLookupError(
            f"Collector cycle {cycle['cycle_id']} has no stored candidates."
        )
    calibration_lookup, calibration_meta = _build_historical_dimension_lookup(
        storage=storage,
        label=str(cycle["label"]),
        session_date=_as_text(cycle.get("session_date")),
    )

    regime_snapshots = _build_regime_snapshots(cycle=cycle, candidates=candidate_rows)
    strategy_intents = _build_strategy_intents(
        cycle=cycle,
        candidates=candidate_rows,
        regime_snapshots=regime_snapshots,
    )
    horizon_intents = _build_horizon_intents(
        cycle=cycle,
        strategy_intents=strategy_intents,
        candidates=candidate_rows,
    )
    opportunities = _build_opportunities(
        cycle=cycle,
        candidates=candidate_rows,
        strategy_intents=strategy_intents,
        horizon_intents=horizon_intents,
        dimension_lookup=calibration_lookup,
    )
    allocation_decisions = build_allocation_decisions(opportunities)
    outcome_matches = _build_outcome_matches(
        opportunities=opportunities,
        analysis_run=analysis_run,
        storage=storage,
        session_id=_as_text(cycle.get("session_id")),
    )
    execution_intents = build_execution_intents(
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
    )
    comparison = _build_comparison(
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        outcome_matches=outcome_matches,
    )
    scorecard = _build_scorecard(
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        comparison=comparison,
        outcome_matches=outcome_matches,
    )
    summary, warnings = _build_summary(
        cycle=cycle,
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        calibration_lookup=calibration_lookup,
        calibration_meta=calibration_meta,
    )
    summary["analysis_verdict"] = None
    if analysis_run is not None:
        summary["analysis_verdict"] = (analysis_run.get("diagnostics") or {}).get(
            "overall_verdict"
        )
    rows = _flatten_opportunity_rows(
        session={
            "label": cycle.get("label"),
            "session_date": cycle.get("session_date"),
            "cycle_id": cycle.get("cycle_id"),
        },
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        comparison=comparison,
        outcome_matches=outcome_matches,
    )
    scorecard["deployment_quality"] = {
        "allocator_selected": _build_deployment_quality_views(
            [row for row in rows if row.get("is_allocator_selected")]
        ),
        "actual_deployed": _build_deployment_quality_views(
            [row for row in rows if row.get("actual_position_matched")]
        ),
    }
    if analysis_run is not None:
        matched_count = sum(
            1 for match in outcome_matches.values() if bool(match.get("matched"))
        )
        if matched_count < len(opportunities):
            warnings.append(
                f"Only {matched_count} of {len(opportunities)} latest-cycle opportunities matched persisted post-market ideas."
            )
    allocator_metrics = scorecard.get("allocator_selected") or {}
    if int(allocator_metrics.get("count") or 0) == 0:
        warnings.append(
            "No opportunities cleared the provisional allocator for this session, so allocator-vs-promotable-baseline comparisons are based on zero selected opportunities."
        )
    allocator_still_open_rate = allocator_metrics.get("still_open_rate")
    if (
        allocator_still_open_rate is not None
        and float(allocator_still_open_rate) >= 0.5
    ):
        warnings.append(
            "Allocator scorecard outcomes are dominated by still_open post-market ideas, so average estimated PnL reflects modeled close-state rather than realized lifecycle results."
        )
    allocator_actual_coverage = _as_float(allocator_metrics.get("actual_coverage_rate"))
    if allocator_actual_coverage is not None and allocator_actual_coverage < 0.5:
        warnings.append(
            "Actual traded-position coverage for allocator-selected opportunities is sparse, so realized PnL comparisons are lower-confidence than modeled replay comparisons."
        )
    allocator_late_open_fill_rate = _as_float(
        allocator_metrics.get("late_open_fill_rate")
    )
    if (
        allocator_late_open_fill_rate is not None
        and allocator_late_open_fill_rate > 0.0
    ):
        warnings.append(
            "Allocator-selected opportunities include filled opens after the configured force-close deadline, which points to execution-path drift rather than pure selection quality."
        )
    allocator_force_close_exit_rate = _as_float(
        allocator_metrics.get("force_close_exit_rate")
    )
    if (
        allocator_force_close_exit_rate is not None
        and allocator_force_close_exit_rate >= 0.5
    ):
        warnings.append(
            "Allocator-selected opportunities are being closed mostly by force-close exits, so actual PnL is sensitive to late-day execution quality."
        )
    allocator_actual_minus_close = _as_float(
        allocator_metrics.get("average_actual_minus_estimated_close_pnl")
    )
    if allocator_actual_minus_close is not None and allocator_actual_minus_close < 0.0:
        warnings.append(
            "Allocator-selected actual PnL is trailing modeled close-state PnL, which suggests execution drag or exit handling slippage."
        )
    warnings.extend(recovery_warnings)

    replay = DecisionReplay(
        target={
            "requested_session_id": session_id,
            "requested_label": label,
            "requested_session_date": session_date,
        },
        session={
            "label": cycle.get("label"),
            "session_date": cycle.get("session_date"),
            "session_id": cycle.get("session_id")
            or f"historical:{cycle['label']}:{cycle['session_date']}",
            "cycle_id": cycle.get("cycle_id"),
            "legacy_profile": cycle.get("profile"),
            "strategy": cycle.get("strategy"),
            "generated_at": cycle.get("generated_at"),
            "analysis_run_id": None
            if analysis_run is None
            else analysis_run.get("analysis_run_id"),
        },
        regime_snapshots=regime_snapshots,
        strategy_intents=strategy_intents,
        horizon_intents=horizon_intents,
        opportunities=opportunities,
        allocation_decisions=allocation_decisions,
        execution_intents=execution_intents,
        summary=summary,
        comparison=comparison,
        scorecard=scorecard,
        rows=rows,
        warnings=warnings,
    )
    return replay.to_payload()


@with_storage()
def build_recent_opportunity_replay_batch(
    *,
    db_target: str | None = None,
    recent: int = 5,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    targets = _resolve_recent_analysis_targets(
        storage=storage,
        recent=recent,
        label=label,
    )
    if not targets:
        scope = "latest succeeded sessions" if label is None else f"label {label}"
        raise OpportunityReplayLookupError(
            f"No succeeded post-market sessions are available for {scope}."
        )

    sessions: list[dict[str, Any]] = []
    verdict_counts: dict[str, int] = defaultdict(int)
    promoted_monitor_total = 0
    rejected_promotable_baseline_total = 0
    allocator_vs_promotable_baseline_total = 0
    allocator_vs_rank_only_total = 0
    skipped_sessions: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []

    for target in targets:
        try:
            payload = build_opportunity_replay(
                db_target=db_target,
                label=target["label"],
                session_date=target["session_date"],
                storage=storage,
            )
        except OpportunityReplayLookupError as exc:
            skipped_sessions.append(
                {
                    "label": target["label"],
                    "session_date": target["session_date"],
                    "reason": str(exc),
                }
            )
            continue
        summary = dict(payload.get("summary") or {})
        comparison = dict(payload.get("comparison") or {})
        scorecard = dict(payload.get("scorecard") or {})
        verdict = _as_text(summary.get("analysis_verdict")) or "unknown"
        verdict_counts[verdict] += 1
        promoted_monitor = list(comparison.get("allocator_promoted_monitor") or [])
        rejected_promotable_baseline = list(
            comparison.get("allocator_rejected_promotable_baseline") or []
        )
        overlap = dict(comparison.get("overlap") or {})
        promoted_monitor_total += len(promoted_monitor)
        rejected_promotable_baseline_total += len(rejected_promotable_baseline)
        allocator_vs_promotable_baseline_total += int(
            overlap.get("allocator_vs_promotable_baseline_count") or 0
        )
        allocator_vs_rank_only_total += int(
            overlap.get("allocator_vs_rank_only_count") or 0
        )
        for row in payload.get("rows") or []:
            if isinstance(row, Mapping):
                all_rows.append(dict(row))
        sessions.append(
            {
                "session": payload.get("session"),
                "summary": summary,
                "scorecard": scorecard,
                "comparison": {
                    "comparison_size": comparison.get("comparison_size"),
                    "promotable_baseline_candidate_ids": (
                        comparison.get("promotable_baseline") or {}
                    ).get("candidate_ids", []),
                    "rank_only_candidate_ids": (
                        comparison.get("rank_only_top") or {}
                    ).get("candidate_ids", []),
                    "allocator_candidate_ids": (
                        comparison.get("provisional_allocator") or {}
                    ).get("candidate_ids", []),
                    "allocator_promoted_monitor": promoted_monitor,
                    "allocator_rejected_promotable_baseline": rejected_promotable_baseline,
                    "overlap": overlap,
                },
                "warnings": list(payload.get("warnings") or []),
            }
        )
        if len(sessions) >= recent:
            break

    session_count = len(sessions)
    if session_count == 0:
        scope = "latest succeeded sessions" if label is None else f"label {label}"
        raise OpportunityReplayLookupError(
            f"No replayable sessions are available for {scope}."
        )

    def pooled_metrics(flag_field: str) -> dict[str, Any]:
        scoped_rows = [row for row in all_rows if row.get(flag_field)]
        matched_rows = [row for row in scoped_rows if row.get("matched_outcome")]
        metrics = _summarize_outcome_rows(scoped_rows)
        return {
            "count": len(scoped_rows),
            "matched_count": len(matched_rows),
            "coverage_rate": None
            if not scoped_rows
            else round(len(matched_rows) / len(scoped_rows), 4),
            **metrics,
        }

    promotable_baseline_metrics = pooled_metrics("is_promotable_baseline")
    rank_only_top_metrics = pooled_metrics("is_rank_only_top")
    allocator_selected_metrics = pooled_metrics("is_allocator_selected")
    promoted_monitor_metrics = pooled_metrics("is_allocator_promoted_monitor")
    rejected_promotable_baseline_metrics = pooled_metrics(
        "is_allocator_rejected_promotable_baseline"
    )
    sessions_with_allocator_selections = sum(
        1
        for item in sessions
        if int((item.get("summary") or {}).get("allocated_count") or 0) > 0
    )
    aggregate = {
        "session_count": session_count,
        "requested_recent": recent,
        "label_filter": label,
        "verdict_counts": dict(sorted(verdict_counts.items())),
        "skipped_session_count": len(skipped_sessions),
        "sessions_with_allocator_selections": sessions_with_allocator_selections,
        "sessions_with_monitor_promotions": sum(
            1 for item in sessions if item["comparison"]["allocator_promoted_monitor"]
        ),
        "sessions_with_rejected_promotable_baseline": sum(
            1
            for item in sessions
            if item["comparison"]["allocator_rejected_promotable_baseline"]
        ),
        "promoted_monitor_total": promoted_monitor_total,
        "rejected_promotable_baseline_total": rejected_promotable_baseline_total,
        "average_allocator_vs_promotable_baseline_overlap": round(
            allocator_vs_promotable_baseline_total / session_count,
            3,
        ),
        "average_allocator_vs_rank_only_overlap": round(
            allocator_vs_rank_only_total / session_count,
            3,
        ),
        "promotable_baseline_metrics": promotable_baseline_metrics,
        "rank_only_top_metrics": rank_only_top_metrics,
        "allocator_selected_metrics": allocator_selected_metrics,
        "promoted_monitor_metrics": promoted_monitor_metrics,
        "rejected_promotable_baseline_metrics": rejected_promotable_baseline_metrics,
        "allocator_minus_promotable_baseline_avg_estimated_pnl": None
        if allocator_selected_metrics["average_estimated_pnl"] is None
        or promotable_baseline_metrics["average_estimated_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_pnl"])
            - float(promotable_baseline_metrics["average_estimated_pnl"]),
            4,
        ),
        "allocator_minus_rank_only_avg_estimated_pnl": None
        if allocator_selected_metrics["average_estimated_pnl"] is None
        or rank_only_top_metrics["average_estimated_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_pnl"])
            - float(rank_only_top_metrics["average_estimated_pnl"]),
            4,
        ),
        "allocator_minus_promotable_baseline_avg_estimated_close_pnl": None
        if allocator_selected_metrics["average_estimated_close_pnl"] is None
        or promotable_baseline_metrics["average_estimated_close_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_close_pnl"])
            - float(promotable_baseline_metrics["average_estimated_close_pnl"]),
            4,
        ),
        "allocator_minus_rank_only_avg_estimated_close_pnl": None
        if allocator_selected_metrics["average_estimated_close_pnl"] is None
        or rank_only_top_metrics["average_estimated_close_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_estimated_close_pnl"])
            - float(rank_only_top_metrics["average_estimated_close_pnl"]),
            4,
        ),
        "allocator_minus_promotable_baseline_avg_actual_net_pnl": None
        if allocator_selected_metrics["average_actual_net_pnl"] is None
        or promotable_baseline_metrics["average_actual_net_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_actual_net_pnl"])
            - float(promotable_baseline_metrics["average_actual_net_pnl"]),
            4,
        ),
        "allocator_minus_rank_only_avg_actual_net_pnl": None
        if allocator_selected_metrics["average_actual_net_pnl"] is None
        or rank_only_top_metrics["average_actual_net_pnl"] is None
        else round(
            float(allocator_selected_metrics["average_actual_net_pnl"])
            - float(rank_only_top_metrics["average_actual_net_pnl"]),
            4,
        ),
        "allocator_minus_promotable_baseline_avg_actual_minus_estimated_close_pnl": None
        if allocator_selected_metrics["average_actual_minus_estimated_close_pnl"]
        is None
        or promotable_baseline_metrics[
            "average_actual_minus_estimated_close_pnl"
        ]
        is None
        else round(
            float(
                allocator_selected_metrics["average_actual_minus_estimated_close_pnl"]
            )
            - float(
                promotable_baseline_metrics[
                    "average_actual_minus_estimated_close_pnl"
                ]
            ),
            4,
        ),
        "allocator_minus_rank_only_avg_actual_minus_estimated_close_pnl": None
        if allocator_selected_metrics["average_actual_minus_estimated_close_pnl"]
        is None
        or rank_only_top_metrics["average_actual_minus_estimated_close_pnl"] is None
        else round(
            float(
                allocator_selected_metrics["average_actual_minus_estimated_close_pnl"]
            )
            - float(rank_only_top_metrics["average_actual_minus_estimated_close_pnl"]),
            4,
        ),
        "monitor_promotion_hit_rate": promoted_monitor_metrics["positive_rate"],
        "rejected_promotable_baseline_positive_rate": rejected_promotable_baseline_metrics[
            "positive_rate"
        ],
        "by_label": _aggregate_dimension_rows(all_rows, field="label"),
        "by_family": _aggregate_dimension_rows(all_rows, field="strategy_family"),
        "by_symbol": _aggregate_dimension_rows(all_rows, field="symbol"),
        "deployment_quality": {
            "allocator_selected": _build_deployment_quality_views(
                [row for row in all_rows if row.get("is_allocator_selected")]
            ),
            "actual_deployed": _build_deployment_quality_views(
                [row for row in all_rows if row.get("actual_position_matched")]
            ),
        },
    }
    aggregate["legacy_promotable_baseline_metrics"] = aggregate["promotable_baseline_metrics"]
    aggregate["promoted_from_legacy_monitor_metrics"] = aggregate["promoted_monitor_metrics"]
    aggregate["rejected_legacy_promotable_metrics"] = aggregate[
        "rejected_promotable_baseline_metrics"
    ]
    aggregate["sessions_with_legacy_monitor_promotions"] = aggregate[
        "sessions_with_monitor_promotions"
    ]
    aggregate["sessions_with_rejected_legacy_promotable"] = aggregate[
        "sessions_with_rejected_promotable_baseline"
    ]
    aggregate["promoted_from_legacy_monitor_total"] = aggregate[
        "promoted_monitor_total"
    ]
    aggregate["rejected_legacy_promotable_total"] = aggregate[
        "rejected_promotable_baseline_total"
    ]
    aggregate["average_allocator_vs_legacy_promotable_baseline_overlap"] = aggregate[
        "average_allocator_vs_promotable_baseline_overlap"
    ]
    aggregate["allocator_minus_legacy_promotable_baseline_avg_estimated_pnl"] = aggregate[
        "allocator_minus_promotable_baseline_avg_estimated_pnl"
    ]
    aggregate["allocator_minus_legacy_promotable_baseline_avg_estimated_close_pnl"] = aggregate[
        "allocator_minus_promotable_baseline_avg_estimated_close_pnl"
    ]
    aggregate["allocator_minus_legacy_promotable_baseline_avg_actual_net_pnl"] = aggregate[
        "allocator_minus_promotable_baseline_avg_actual_net_pnl"
    ]
    aggregate["allocator_minus_legacy_promotable_baseline_avg_actual_minus_estimated_close_pnl"] = aggregate[
        "allocator_minus_promotable_baseline_avg_actual_minus_estimated_close_pnl"
    ]
    aggregate["legacy_monitor_promotion_hit_rate"] = aggregate[
        "monitor_promotion_hit_rate"
    ]
    aggregate["rejected_legacy_promotable_miss_rate"] = aggregate[
        "rejected_promotable_baseline_positive_rate"
    ]
    warnings: list[str] = []
    if session_count < recent:
        warnings.append(
            f"Only {session_count} replayable sessions were available out of the requested {recent}."
        )
    if skipped_sessions:
        warnings.append(
            f"Skipped {len(skipped_sessions)} sessions because stored collector candidates were unavailable."
        )
    if sessions_with_allocator_selections < session_count:
        warnings.append(
            f"Allocator selections only appeared in {sessions_with_allocator_selections} of {session_count} replayed sessions, so pooled allocator metrics are sparse."
        )
    allocator_still_open_rate = allocator_selected_metrics.get("still_open_rate")
    if (
        allocator_still_open_rate is not None
        and float(allocator_still_open_rate) >= 0.5
    ):
        warnings.append(
            "Allocator scorecard outcomes are dominated by still_open post-market ideas, so average estimated PnL reflects modeled close-state rather than realized lifecycle results."
        )
    allocator_actual_coverage = allocator_selected_metrics.get("actual_coverage_rate")
    if allocator_actual_coverage is not None and float(allocator_actual_coverage) < 0.5:
        warnings.append(
            "Actual traded-position coverage for allocator-selected opportunities is sparse, so realized PnL comparisons are lower-confidence than modeled replay comparisons."
        )
    allocator_late_open_fill_rate = allocator_selected_metrics.get(
        "late_open_fill_rate"
    )
    if (
        allocator_late_open_fill_rate is not None
        and float(allocator_late_open_fill_rate) > 0.0
    ):
        warnings.append(
            "Some allocator-selected opportunities were opened after their configured force-close deadline, which points to execution-path drift in the live system."
        )
    allocator_force_close_exit_rate = allocator_selected_metrics.get(
        "force_close_exit_rate"
    )
    if (
        allocator_force_close_exit_rate is not None
        and float(allocator_force_close_exit_rate) >= 0.5
    ):
        warnings.append(
            "Allocator-selected actual trades are dominated by force-close exits, so live execution timing remains a primary risk."
        )
    allocator_actual_minus_close = allocator_selected_metrics.get(
        "average_actual_minus_estimated_close_pnl"
    )
    if (
        allocator_actual_minus_close is not None
        and float(allocator_actual_minus_close) < 0.0
    ):
        warnings.append(
            "Allocator-selected actual PnL is trailing modeled close-state PnL on the replay sample, which suggests execution drag or exit slippage."
        )
    return {
        "target": {
            "recent": recent,
            "label": label,
        },
        "aggregate": aggregate,
        "sessions": sessions,
        "skipped_sessions": skipped_sessions,
        "rows": all_rows,
        "warnings": warnings,
    }


__all__ = [
    "OpportunityReplayLookupError",
    "build_opportunity_replay",
    "build_recent_opportunity_replay_batch",
]
