from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from statistics import mean
from typing import Any

from core.domain.opportunity_models import AllocationDecision, Opportunity

from .shared import (
    UNKNOWN_BUCKET_ORDER,
    _as_float,
    _as_text,
    _dte_bucket,
    _entry_return_on_risk_bucket,
    _midpoint_credit_bucket,
    _ratio_or_none,
    _width_bucket,
)


def _with_legacy_aliases(
    payload: dict[str, Any],
    *,
    aliases: dict[str, str],
) -> dict[str, Any]:
    result = dict(payload)
    for legacy_key, canonical_key in aliases.items():
        if canonical_key in result:
            result[legacy_key] = result[canonical_key]
    return result


def _summarize_outcome_rows(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    modeled_final_rows = [row for row in rows if row.get("estimated_pnl") is not None]
    pnl_values = [float(row["estimated_pnl"]) for row in modeled_final_rows]
    signed_rows = [row for row in rows if row.get("positive_outcome") is not None]
    outcome_bucket_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        bucket = _as_text(row.get("outcome_bucket"))
        if bucket is not None:
            outcome_bucket_counts[bucket] += 1
    still_open_count = int(outcome_bucket_counts.get("still_open") or 0)
    modeled_close_rows = [
        row for row in rows if row.get("estimated_close_pnl") is not None
    ]
    modeled_expiry_rows = [
        row for row in rows if row.get("estimated_expiry_pnl") is not None
    ]
    actual_rows = [row for row in rows if row.get("actual_net_pnl") is not None]
    actual_realized_rows = [
        row for row in rows if row.get("actual_realized_pnl") is not None
    ]
    actual_status_counts: dict[str, int] = defaultdict(int)
    for row in actual_rows:
        for status, count in dict(
            row.get("actual_position_status_counts") or {}
        ).items():
            actual_status_counts[str(status)] += int(count or 0)
    actual_closed_rates = [
        float(row["actual_closed_rate"])
        for row in actual_rows
        if row.get("actual_closed_rate") is not None
    ]
    execution_rows = [
        row
        for row in rows
        if bool(row.get("execution_attempted"))
        or int(row.get("open_attempt_count") or 0) > 0
        or int(row.get("close_attempt_count") or 0) > 0
    ]
    open_attempt_total = sum(int(row.get("open_attempt_count") or 0) for row in rows)
    open_filled_total = sum(
        int(row.get("open_filled_attempt_count") or 0) for row in rows
    )
    open_expired_total = sum(
        int(row.get("open_expired_attempt_count") or 0) for row in rows
    )
    open_failed_total = sum(
        int(row.get("open_failed_attempt_count") or 0) for row in rows
    )
    late_open_request_total = sum(
        int(row.get("requested_after_force_close_count") or 0) for row in rows
    )
    late_open_fill_total = sum(
        int(row.get("opened_after_force_close_count") or 0) for row in rows
    )
    close_attempt_total = sum(int(row.get("close_attempt_count") or 0) for row in rows)
    close_filled_total = sum(
        int(row.get("close_filled_attempt_count") or 0) for row in rows
    )
    force_close_exit_total = sum(
        int(row.get("force_close_exit_count") or 0) for row in rows
    )
    actual_minus_close_rows = [
        row for row in rows if row.get("actual_minus_estimated_close_pnl") is not None
    ]

    def _weighted_average(total_field: str, count_field: str) -> float | None:
        weighted_count = sum(int(row.get(count_field) or 0) for row in rows)
        if weighted_count <= 0:
            return None
        weighted_total = sum(float(row.get(total_field) or 0.0) for row in rows)
        return round(weighted_total / weighted_count, 4)

    def _average_ratio(field: str) -> float | None:
        values = [float(row[field]) for row in rows if row.get(field) is not None]
        if not values:
            return None
        return round(mean(values), 4)

    def _pooled_return_on_risk(pnl_field: str) -> tuple[float | None, float]:
        numerator_total = 0.0
        denominator_total = 0.0
        for row in rows:
            pnl_value = _as_float(row.get(pnl_field))
            max_loss = _as_float(row.get("max_loss"))
            if pnl_value is None or max_loss is None or max_loss <= 0.0:
                continue
            numerator_total += pnl_value
            denominator_total += max_loss
        if denominator_total <= 0.0:
            return None, 0.0
        return round(numerator_total / denominator_total, 4), round(
            denominator_total, 2
        )

    pooled_estimated_final_ror, estimated_final_max_loss_total = _pooled_return_on_risk(
        "estimated_pnl"
    )
    pooled_estimated_close_ror, estimated_close_max_loss_total = _pooled_return_on_risk(
        "estimated_close_pnl"
    )
    pooled_actual_net_ror, actual_max_loss_total = _pooled_return_on_risk(
        "actual_net_pnl"
    )
    (
        pooled_actual_minus_estimated_close_ror,
        actual_minus_close_max_loss_total,
    ) = _pooled_return_on_risk("actual_minus_estimated_close_pnl")
    overall_max_loss_total = round(
        sum(
            max(_as_float(row.get("max_loss")) or 0.0, 0.0)
            for row in rows
            if _as_float(row.get("max_loss")) is not None
        ),
        2,
    )

    return {
        "average_estimated_pnl": None if not pnl_values else round(mean(pnl_values), 4),
        "estimated_pnl_count": len(modeled_final_rows),
        "estimated_pnl_coverage_rate": None
        if not rows
        else round(len(modeled_final_rows) / len(rows), 4),
        "positive_rate": None
        if not signed_rows
        else round(
            sum(1 for row in signed_rows if bool(row.get("positive_outcome")))
            / len(signed_rows),
            4,
        ),
        "positive_count": sum(
            1 for row in signed_rows if bool(row.get("positive_outcome"))
        ),
        "negative_or_flat_count": sum(
            1 for row in signed_rows if not bool(row.get("positive_outcome"))
        ),
        "outcome_bucket_counts": dict(sorted(outcome_bucket_counts.items())),
        "still_open_count": still_open_count,
        "still_open_rate": None if not rows else round(still_open_count / len(rows), 4),
        "average_estimated_close_pnl": None
        if not modeled_close_rows
        else round(
            mean(float(row["estimated_close_pnl"]) for row in modeled_close_rows), 4
        ),
        "estimated_close_count": len(modeled_close_rows),
        "estimated_close_coverage_rate": None
        if not rows
        else round(len(modeled_close_rows) / len(rows), 4),
        "estimated_close_positive_rate": None
        if not modeled_close_rows
        else round(
            sum(
                1
                for row in modeled_close_rows
                if bool(row.get("estimated_close_positive"))
            )
            / len(modeled_close_rows),
            4,
        ),
        "average_estimated_expiry_pnl": None
        if not modeled_expiry_rows
        else round(
            mean(float(row["estimated_expiry_pnl"]) for row in modeled_expiry_rows), 4
        ),
        "estimated_expiry_count": len(modeled_expiry_rows),
        "estimated_expiry_coverage_rate": None
        if not rows
        else round(len(modeled_expiry_rows) / len(rows), 4),
        "estimated_expiry_positive_rate": None
        if not modeled_expiry_rows
        else round(
            sum(
                1
                for row in modeled_expiry_rows
                if bool(row.get("estimated_expiry_positive"))
            )
            / len(modeled_expiry_rows),
            4,
        ),
        "average_actual_net_pnl": None
        if not actual_rows
        else round(mean(float(row["actual_net_pnl"]) for row in actual_rows), 4),
        "average_actual_realized_pnl": None
        if not actual_realized_rows
        else round(
            mean(float(row["actual_realized_pnl"]) for row in actual_realized_rows), 4
        ),
        "actual_count": len(actual_rows),
        "actual_coverage_rate": None
        if not rows
        else round(len(actual_rows) / len(rows), 4),
        "actual_positive_rate": None
        if not actual_rows
        else round(
            sum(1 for row in actual_rows if bool(row.get("actual_positive_outcome")))
            / len(actual_rows),
            4,
        ),
        "actual_position_status_counts": dict(sorted(actual_status_counts.items())),
        "actual_closed_rate": None
        if not actual_closed_rates
        else round(mean(actual_closed_rates), 4),
        "execution_attempted_count": len(execution_rows),
        "execution_attempted_rate": None
        if not rows
        else round(len(execution_rows) / len(rows), 4),
        "open_attempt_count": open_attempt_total,
        "open_filled_attempt_count": open_filled_total,
        "open_expired_attempt_count": open_expired_total,
        "open_failed_attempt_count": open_failed_total,
        "open_fill_rate": None
        if open_attempt_total <= 0
        else round(open_filled_total / open_attempt_total, 4),
        "open_expired_rate": None
        if open_attempt_total <= 0
        else round(open_expired_total / open_attempt_total, 4),
        "open_failed_rate": None
        if open_attempt_total <= 0
        else round(open_failed_total / open_attempt_total, 4),
        "late_open_request_count": late_open_request_total,
        "late_open_request_rate": None
        if open_attempt_total <= 0
        else round(late_open_request_total / open_attempt_total, 4),
        "late_open_fill_count": late_open_fill_total,
        "late_open_fill_rate": None
        if open_filled_total <= 0
        else round(late_open_fill_total / open_filled_total, 4),
        "close_attempt_count": close_attempt_total,
        "close_filled_attempt_count": close_filled_total,
        "close_fill_rate": None
        if close_attempt_total <= 0
        else round(close_filled_total / close_attempt_total, 4),
        "force_close_exit_count": force_close_exit_total,
        "force_close_exit_rate": None
        if close_attempt_total <= 0
        else round(force_close_exit_total / close_attempt_total, 4),
        "average_open_fill_minutes": _weighted_average(
            "open_fill_minutes_total",
            "open_fill_minutes_count",
        ),
        "average_close_fill_minutes": _weighted_average(
            "close_fill_minutes_total",
            "close_fill_minutes_count",
        ),
        "average_minutes_to_force_close_at_request": _weighted_average(
            "request_to_force_close_total",
            "request_to_force_close_count",
        ),
        "average_minutes_to_force_close_at_fill": _weighted_average(
            "fill_to_force_close_total",
            "fill_to_force_close_count",
        ),
        "average_entry_credit_capture_pct": _weighted_average(
            "entry_credit_capture_total",
            "entry_credit_capture_count",
        ),
        "average_entry_limit_retention_pct": _weighted_average(
            "entry_limit_retention_total",
            "entry_limit_retention_count",
        ),
        "average_actual_minus_estimated_close_pnl": None
        if not actual_minus_close_rows
        else round(
            mean(
                float(row["actual_minus_estimated_close_pnl"])
                for row in actual_minus_close_rows
            ),
            4,
        ),
        "actual_minus_estimated_close_count": len(actual_minus_close_rows),
        "max_loss_total": overall_max_loss_total,
        "average_estimated_final_return_on_risk": _average_ratio(
            "estimated_final_return_on_risk"
        ),
        "pooled_estimated_final_return_on_risk": pooled_estimated_final_ror,
        "estimated_final_return_on_risk_max_loss_total": estimated_final_max_loss_total,
        "average_estimated_close_return_on_risk": _average_ratio(
            "estimated_close_return_on_risk"
        ),
        "pooled_estimated_close_return_on_risk": pooled_estimated_close_ror,
        "estimated_close_return_on_risk_max_loss_total": estimated_close_max_loss_total,
        "average_actual_net_return_on_risk": _average_ratio(
            "actual_net_return_on_risk"
        ),
        "pooled_actual_net_return_on_risk": pooled_actual_net_ror,
        "actual_net_return_on_risk_max_loss_total": actual_max_loss_total,
        "average_actual_minus_estimated_close_return_on_risk": _average_ratio(
            "actual_minus_estimated_close_return_on_risk"
        ),
        "pooled_actual_minus_estimated_close_return_on_risk": pooled_actual_minus_estimated_close_ror,
        "actual_minus_estimated_close_return_on_risk_max_loss_total": actual_minus_close_max_loss_total,
    }


def _slice_metrics(
    *,
    items: list[Opportunity],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    rows = [
        outcome_matches[item.opportunity_id]
        for item in items
        if outcome_matches.get(item.opportunity_id) is not None
    ]
    matched = [row for row in rows if row.get("matched")]
    metrics = _summarize_outcome_rows(rows)
    return {
        "count": len(items),
        "matched_count": len(matched),
        "coverage_rate": None if not items else round(len(matched) / len(items), 4),
        **metrics,
    }


def _flatten_opportunity_rows(
    *,
    session: Mapping[str, Any],
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
    comparison: Mapping[str, Any],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    allocation_by_id = {item.opportunity_id: item for item in allocation_decisions}
    rank_only_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("rank_only_top", {}) or {}).get("items", [])
        if isinstance(item, Mapping)
    }
    allocator_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("provisional_allocator", {}) or {}).get("items", [])
        if isinstance(item, Mapping)
    }
    promoted_monitor_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("allocator_promoted_monitor") or [])
        if isinstance(item, Mapping)
    }
    rejected_promotable_baseline_ids = {
        str(item.get("opportunity_id"))
        for item in (comparison.get("allocator_rejected_promotable_baseline") or [])
        if isinstance(item, Mapping)
    }

    rows: list[dict[str, Any]] = []
    for opportunity in opportunities:
        allocation = allocation_by_id.get(opportunity.opportunity_id)
        outcome = outcome_matches.get(opportunity.opportunity_id, {})
        max_loss = _as_float(opportunity.max_loss)
        entry_return_on_risk = _as_float(opportunity.expected_edge_value)
        midpoint_credit = _as_float(opportunity.evidence.get("midpoint_credit"))
        width = _as_float(opportunity.evidence.get("width"))
        days_to_expiration = _as_float(opportunity.evidence.get("days_to_expiration"))
        entry_ror_bucket, entry_ror_bucket_order = _entry_return_on_risk_bucket(
            entry_return_on_risk
        )
        midpoint_credit_bucket, midpoint_credit_bucket_order = _midpoint_credit_bucket(
            midpoint_credit
        )
        width_bucket, width_bucket_order = _width_bucket(width)
        dte_bucket, dte_bucket_order = _dte_bucket(days_to_expiration)
        rows.append(
            _with_legacy_aliases(
                {
                    "label": session.get("label"),
                    "session_date": session.get("session_date"),
                    "cycle_id": session.get("cycle_id"),
                    "candidate_id": opportunity.candidate_id,
                    "opportunity_id": opportunity.opportunity_id,
                    "symbol": opportunity.symbol,
                    "style_profile": opportunity.style_profile,
                    "strategy_family": opportunity.strategy_family,
                    "legacy_strategy": opportunity.legacy_strategy,
                    "event_state": opportunity.evidence.get("earnings_phase"),
                    "event_timing_rule": opportunity.evidence.get("event_timing_rule"),
                    "direction_signal": _as_float(
                        (
                            (opportunity.evidence.get("signal_bundle") or {}).get(
                                "signals"
                            )
                            or {}
                        )
                        .get("direction_signal", {})
                        .get("score")
                    ),
                    "jump_risk_signal": _as_float(
                        (
                            (opportunity.evidence.get("signal_bundle") or {}).get(
                                "signals"
                            )
                            or {}
                        )
                        .get("jump_risk_signal", {})
                        .get("score")
                    ),
                    "pricing_signal": _as_float(
                        (
                            (opportunity.evidence.get("signal_bundle") or {}).get(
                                "signals"
                            )
                            or {}
                        )
                        .get("pricing_signal", {})
                        .get("score")
                    ),
                    "post_event_confirmation_signal": _as_float(
                        (
                            (opportunity.evidence.get("signal_bundle") or {}).get(
                                "signals"
                            )
                            or {}
                        )
                        .get("post_event_confirmation_signal", {})
                        .get("score")
                    ),
                    "options_bias_alignment": (
                        opportunity.evidence.get("signal_bundle") or {}
                    ).get("options_bias_alignment"),
                    "neutral_regime_signal": _as_float(
                        (opportunity.evidence.get("signal_bundle") or {}).get(
                            "neutral_regime_signal"
                        )
                    ),
                    "residual_iv_richness": _as_float(
                        (opportunity.evidence.get("signal_bundle") or {}).get(
                            "residual_iv_richness"
                        )
                    ),
                    "signal_gate_active": bool(
                        (opportunity.evidence.get("signal_gate") or {}).get("active")
                    ),
                    "signal_gate_eligible": bool(
                        (opportunity.evidence.get("signal_gate") or {}).get("eligible")
                    ),
                    "signal_gate_blockers": (
                        opportunity.evidence.get("signal_gate") or {}
                    ).get("blockers"),
                    "expiration_date": opportunity.expiration_date,
                    "short_symbol": opportunity.short_symbol,
                    "long_symbol": opportunity.long_symbol,
                    "max_loss": max_loss,
                    "entry_return_on_risk": entry_return_on_risk,
                    "entry_return_on_risk_bucket": entry_ror_bucket,
                    "entry_return_on_risk_bucket_order": entry_ror_bucket_order,
                    "midpoint_credit": midpoint_credit,
                    "midpoint_credit_bucket": midpoint_credit_bucket,
                    "midpoint_credit_bucket_order": midpoint_credit_bucket_order,
                    "width": width,
                    "width_bucket": width_bucket,
                    "width_bucket_order": width_bucket_order,
                    "days_to_expiration": None
                    if days_to_expiration is None
                    else int(days_to_expiration),
                    "dte_bucket": dte_bucket,
                    "dte_bucket_order": dte_bucket_order,
                    "baseline_selection_state": opportunity.legacy_selection_state,
                    "rank": opportunity.rank,
                    "state": opportunity.state,
                    "promotion_score": opportunity.promotion_score,
                    "allocation_state": None
                    if allocation is None
                    else allocation.allocation_state,
                    "allocation_score": None
                    if allocation is None
                    else allocation.allocation_score,
                    "allocation_reason": None
                    if allocation is None
                    else allocation.allocation_reason,
                    "matched_outcome": outcome.get("matched"),
                    "estimated_close_pnl": outcome.get("estimated_close_pnl"),
                    "estimated_expiry_pnl": outcome.get("estimated_expiry_pnl"),
                    "estimated_pnl": outcome.get("estimated_pnl"),
                    "estimated_close_return_on_risk": _ratio_or_none(
                        outcome.get("estimated_close_pnl"),
                        max_loss,
                    ),
                    "estimated_final_return_on_risk": _ratio_or_none(
                        outcome.get("estimated_pnl"),
                        max_loss,
                    ),
                    "positive_outcome": outcome.get("positive_outcome"),
                    "outcome_bucket": outcome.get("outcome_bucket"),
                    "replay_verdict": outcome.get("replay_verdict"),
                    "setup_status": outcome.get("setup_status"),
                    "vwap_regime": outcome.get("vwap_regime"),
                    "trend_regime": outcome.get("trend_regime"),
                    "opening_range_regime": outcome.get("opening_range_regime"),
                    "actual_position_matched": outcome.get("actual_position_matched"),
                    "actual_position_count": outcome.get("actual_position_count"),
                    "actual_position_status_counts": outcome.get(
                        "actual_position_status_counts"
                    ),
                    "actual_closed_rate": outcome.get("actual_closed_rate"),
                    "actual_realized_pnl": outcome.get("actual_realized_pnl"),
                    "actual_unrealized_pnl": outcome.get("actual_unrealized_pnl"),
                    "actual_net_pnl": outcome.get("actual_net_pnl"),
                    "actual_net_return_on_risk": _ratio_or_none(
                        outcome.get("actual_net_pnl"),
                        max_loss,
                    ),
                    "actual_positive_outcome": outcome.get("actual_positive_outcome"),
                    "actual_minus_estimated_close_pnl": outcome.get(
                        "actual_minus_estimated_close_pnl"
                    ),
                    "actual_minus_estimated_close_return_on_risk": _ratio_or_none(
                        outcome.get("actual_minus_estimated_close_pnl"),
                        max_loss,
                    ),
                    "execution_attempted": outcome.get("execution_attempted"),
                    "execution_attempt_ids": outcome.get("execution_attempt_ids"),
                    "close_execution_attempt_ids": outcome.get(
                        "close_execution_attempt_ids"
                    ),
                    "open_attempt_count": outcome.get("open_attempt_count"),
                    "open_filled_attempt_count": outcome.get("open_filled_attempt_count"),
                    "open_expired_attempt_count": outcome.get(
                        "open_expired_attempt_count"
                    ),
                    "open_failed_attempt_count": outcome.get("open_failed_attempt_count"),
                    "open_status_counts": outcome.get("open_status_counts"),
                    "average_open_fill_minutes": outcome.get("average_open_fill_minutes"),
                    "requested_after_force_close_count": outcome.get(
                        "requested_after_force_close_count"
                    ),
                    "opened_after_force_close_count": outcome.get(
                        "opened_after_force_close_count"
                    ),
                    "average_minutes_to_force_close_at_request": outcome.get(
                        "average_minutes_to_force_close_at_request"
                    ),
                    "average_minutes_to_force_close_at_fill": outcome.get(
                        "average_minutes_to_force_close_at_fill"
                    ),
                    "entry_credit_capture_total": outcome.get("entry_credit_capture_total"),
                    "entry_credit_capture_count": outcome.get("entry_credit_capture_count"),
                    "average_entry_credit_capture_pct": outcome.get(
                        "average_entry_credit_capture_pct"
                    ),
                    "entry_limit_retention_total": outcome.get(
                        "entry_limit_retention_total"
                    ),
                    "entry_limit_retention_count": outcome.get(
                        "entry_limit_retention_count"
                    ),
                    "average_entry_limit_retention_pct": outcome.get(
                        "average_entry_limit_retention_pct"
                    ),
                    "close_attempt_count": outcome.get("close_attempt_count"),
                    "close_filled_attempt_count": outcome.get("close_filled_attempt_count"),
                    "close_status_counts": outcome.get("close_status_counts"),
                    "average_close_fill_minutes": outcome.get("average_close_fill_minutes"),
                    "force_close_exit_count": outcome.get("force_close_exit_count"),
                    "open_fill_minutes_total": outcome.get("open_fill_minutes_total"),
                    "open_fill_minutes_count": outcome.get("open_fill_minutes_count"),
                    "request_to_force_close_total": outcome.get(
                        "request_to_force_close_total"
                    ),
                    "request_to_force_close_count": outcome.get(
                        "request_to_force_close_count"
                    ),
                    "fill_to_force_close_total": outcome.get("fill_to_force_close_total"),
                    "fill_to_force_close_count": outcome.get("fill_to_force_close_count"),
                    "close_fill_minutes_total": outcome.get("close_fill_minutes_total"),
                    "close_fill_minutes_count": outcome.get("close_fill_minutes_count"),
                    "is_promotable_baseline": opportunity.legacy_selection_state
                    == "promotable",
                    "is_monitor_baseline": opportunity.legacy_selection_state
                    == "monitor",
                    "is_rank_only_top": opportunity.opportunity_id in rank_only_ids,
                    "is_allocator_selected": opportunity.opportunity_id in allocator_ids,
                    "is_allocator_promoted_monitor": opportunity.opportunity_id
                    in promoted_monitor_ids,
                    "is_allocator_rejected_promotable_baseline": opportunity.opportunity_id
                    in rejected_promotable_baseline_ids,
                },
                aliases={
                    "legacy_selection_state": "baseline_selection_state",
                    "is_legacy_promotable_baseline": "is_promotable_baseline",
                    "is_legacy_monitor_baseline": "is_monitor_baseline",
                    "is_promoted_from_legacy_monitor": "is_allocator_promoted_monitor",
                    "is_rejected_legacy_promotable": "is_allocator_rejected_promotable_baseline",
                },
            )
        )
    return rows


def _aggregate_dimension_rows(
    rows: list[dict[str, Any]],
    *,
    field: str,
    order_field: str | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(field) or "unknown")].append(row)

    result: list[dict[str, Any]] = []
    for group_value, group_rows in grouped.items():
        matched = [row for row in group_rows if row.get("matched_outcome")]
        metrics = _summarize_outcome_rows(group_rows)
        result.append(
            _with_legacy_aliases(
                {
                    "group_value": group_value,
                    "count": len(group_rows),
                    "matched_count": len(matched),
                    "coverage_rate": None
                    if not group_rows
                    else round(len(matched) / len(group_rows), 4),
                    "allocator_selected_count": sum(
                        1 for row in group_rows if row.get("is_allocator_selected")
                    ),
                    "promotable_baseline_count": sum(
                        1 for row in group_rows if row.get("is_promotable_baseline")
                    ),
                    "rank_only_top_count": sum(
                        1 for row in group_rows if row.get("is_rank_only_top")
                    ),
                    "allocator_promoted_monitor_count": sum(
                        1
                        for row in group_rows
                        if row.get("is_allocator_promoted_monitor")
                    ),
                    "allocator_rejected_promotable_baseline_count": sum(
                        1
                        for row in group_rows
                        if row.get("is_allocator_rejected_promotable_baseline")
                    ),
                    **metrics,
                },
                aliases={
                    "legacy_promotable_baseline_count": "promotable_baseline_count",
                    "promoted_from_legacy_monitor_count": "allocator_promoted_monitor_count",
                    "rejected_legacy_promotable_count": "allocator_rejected_promotable_baseline_count",
                },
            )
        )
    if order_field is not None:
        result.sort(
            key=lambda row: (
                min(
                    int(group_row.get(order_field) or UNKNOWN_BUCKET_ORDER)
                    for group_row in grouped[row["group_value"]]
                ),
                row["group_value"],
            )
        )
    else:
        result.sort(key=lambda row: (-int(row["count"]), row["group_value"]))
    return result


def _build_deployment_quality_views(
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    matched_rows = [row for row in rows if row.get("matched_outcome")]
    return {
        "count": len(rows),
        "matched_count": len(matched_rows),
        "coverage_rate": None if not rows else round(len(matched_rows) / len(rows), 4),
        **_summarize_outcome_rows(rows),
        "by_profile": _aggregate_dimension_rows(rows, field="style_profile"),
        "by_strategy_family": _aggregate_dimension_rows(rows, field="strategy_family"),
        "by_entry_return_on_risk_bucket": _aggregate_dimension_rows(
            rows,
            field="entry_return_on_risk_bucket",
            order_field="entry_return_on_risk_bucket_order",
        ),
        "by_midpoint_credit_bucket": _aggregate_dimension_rows(
            rows,
            field="midpoint_credit_bucket",
            order_field="midpoint_credit_bucket_order",
        ),
        "by_width_bucket": _aggregate_dimension_rows(
            rows,
            field="width_bucket",
            order_field="width_bucket_order",
        ),
        "by_dte_bucket": _aggregate_dimension_rows(
            rows,
            field="dte_bucket",
            order_field="dte_bucket_order",
        ),
    }


def _build_scorecard(
    *,
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
    comparison: Mapping[str, Any],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    allocation_by_id = {item.opportunity_id: item for item in allocation_decisions}
    rank_only_ids = {
        str(row.get("opportunity_id"))
        for row in (comparison.get("rank_only_top", {}) or {}).get("items", [])
        if isinstance(row, Mapping)
    }
    rank_only_top = [
        item for item in opportunities if item.opportunity_id in rank_only_ids
    ]
    allocator_selected = [
        item
        for item in opportunities
        if allocation_by_id.get(item.opportunity_id) is not None
        and allocation_by_id[item.opportunity_id].allocation_state == "allocated"
    ]
    promotable_baseline = [
        item for item in opportunities if item.legacy_selection_state == "promotable"
    ]
    allocator_promoted_monitor = [
        item for item in allocator_selected if item.legacy_selection_state == "monitor"
    ]
    allocator_rejected_promotable_baseline = [
        item
        for item in promotable_baseline
        if allocation_by_id.get(item.opportunity_id) is None
        or allocation_by_id[item.opportunity_id].allocation_state != "allocated"
    ]

    scorecard = _with_legacy_aliases(
        {
            "promotable_baseline": _slice_metrics(
                items=promotable_baseline,
                outcome_matches=outcome_matches,
            ),
            "rank_only_top": _slice_metrics(
                items=rank_only_top,
                outcome_matches=outcome_matches,
            ),
            "allocator_selected": _slice_metrics(
                items=allocator_selected,
                outcome_matches=outcome_matches,
            ),
            "allocator_promoted_monitor": _slice_metrics(
                items=allocator_promoted_monitor,
                outcome_matches=outcome_matches,
            ),
            "allocator_rejected_promotable_baseline": _slice_metrics(
                items=allocator_rejected_promotable_baseline,
                outcome_matches=outcome_matches,
            ),
        },
        aliases={
            "legacy_promotable_baseline": "promotable_baseline",
            "promoted_from_legacy_monitor": "allocator_promoted_monitor",
            "rejected_legacy_promotable": "allocator_rejected_promotable_baseline",
        },
    )

    def metric_delta(
        *,
        allocator_field: str,
        baseline: dict[str, Any],
    ) -> float | None:
        allocator_value = _as_float(
            scorecard["allocator_selected"].get(allocator_field)
        )
        baseline_value = _as_float(baseline.get(allocator_field))
        if allocator_value is None or baseline_value is None:
            return None
        return round(allocator_value - baseline_value, 4)

    scorecard["deltas"] = _with_legacy_aliases(
        {
            "allocator_minus_promotable_baseline_avg_estimated_pnl": metric_delta(
                allocator_field="average_estimated_pnl",
                baseline=scorecard["promotable_baseline"],
            ),
            "allocator_minus_rank_only_avg_estimated_pnl": metric_delta(
                allocator_field="average_estimated_pnl",
                baseline=scorecard["rank_only_top"],
            ),
            "allocator_minus_promotable_baseline_avg_estimated_close_pnl": metric_delta(
                allocator_field="average_estimated_close_pnl",
                baseline=scorecard["promotable_baseline"],
            ),
            "allocator_minus_rank_only_avg_estimated_close_pnl": metric_delta(
                allocator_field="average_estimated_close_pnl",
                baseline=scorecard["rank_only_top"],
            ),
            "allocator_minus_promotable_baseline_avg_actual_net_pnl": metric_delta(
                allocator_field="average_actual_net_pnl",
                baseline=scorecard["promotable_baseline"],
            ),
            "allocator_minus_rank_only_avg_actual_net_pnl": metric_delta(
                allocator_field="average_actual_net_pnl",
                baseline=scorecard["rank_only_top"],
            ),
            "allocator_minus_promotable_baseline_avg_actual_minus_estimated_close_pnl": metric_delta(
                allocator_field="average_actual_minus_estimated_close_pnl",
                baseline=scorecard["promotable_baseline"],
            ),
            "allocator_minus_rank_only_avg_actual_minus_estimated_close_pnl": metric_delta(
                allocator_field="average_actual_minus_estimated_close_pnl",
                baseline=scorecard["rank_only_top"],
            ),
            "allocator_minus_promotable_baseline_late_open_fill_rate": metric_delta(
                allocator_field="late_open_fill_rate",
                baseline=scorecard["promotable_baseline"],
            ),
            "allocator_minus_rank_only_late_open_fill_rate": metric_delta(
                allocator_field="late_open_fill_rate",
                baseline=scorecard["rank_only_top"],
            ),
            "allocator_minus_promotable_baseline_force_close_exit_rate": metric_delta(
                allocator_field="force_close_exit_rate",
                baseline=scorecard["promotable_baseline"],
            ),
            "allocator_minus_rank_only_force_close_exit_rate": metric_delta(
                allocator_field="force_close_exit_rate",
                baseline=scorecard["rank_only_top"],
            ),
            "monitor_promotion_hit_rate": scorecard["allocator_promoted_monitor"][
                "positive_rate"
            ],
            "rejected_promotable_baseline_positive_rate": scorecard[
                "allocator_rejected_promotable_baseline"
            ]["positive_rate"],
        },
        aliases={
            "allocator_minus_legacy_promotable_baseline_avg_estimated_pnl": "allocator_minus_promotable_baseline_avg_estimated_pnl",
            "allocator_minus_legacy_promotable_baseline_avg_estimated_close_pnl": "allocator_minus_promotable_baseline_avg_estimated_close_pnl",
            "allocator_minus_legacy_promotable_baseline_avg_actual_net_pnl": "allocator_minus_promotable_baseline_avg_actual_net_pnl",
            "allocator_minus_legacy_promotable_baseline_avg_actual_minus_estimated_close_pnl": "allocator_minus_promotable_baseline_avg_actual_minus_estimated_close_pnl",
            "allocator_minus_legacy_promotable_baseline_late_open_fill_rate": "allocator_minus_promotable_baseline_late_open_fill_rate",
            "allocator_minus_legacy_promotable_baseline_force_close_exit_rate": "allocator_minus_promotable_baseline_force_close_exit_rate",
            "legacy_monitor_promotion_hit_rate": "monitor_promotion_hit_rate",
            "rejected_legacy_promotable_miss_rate": "rejected_promotable_baseline_positive_rate",
        },
    )
    return scorecard


def _comparison_item(
    opportunity: Opportunity,
    allocation: AllocationDecision | None,
    outcome: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "opportunity_id": opportunity.opportunity_id,
        "candidate_id": opportunity.candidate_id,
        "symbol": opportunity.symbol,
        "strategy_family": opportunity.strategy_family,
        "rank": opportunity.rank,
        "promotion_score": opportunity.promotion_score,
        "baseline_selection_state": opportunity.legacy_selection_state,
        "legacy_selection_state": opportunity.legacy_selection_state,
        "allocation_state": None if allocation is None else allocation.allocation_state,
        "allocation_reason": None
        if allocation is None
        else allocation.allocation_reason,
        "allocation_score": None if allocation is None else allocation.allocation_score,
        "estimated_pnl": None if outcome is None else outcome.get("estimated_pnl"),
        "outcome_bucket": None if outcome is None else outcome.get("outcome_bucket"),
        "positive_outcome": None
        if outcome is None
        else outcome.get("positive_outcome"),
    }


def _build_comparison(
    *,
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
    outcome_matches: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    allocation_by_opportunity_id = {
        item.opportunity_id: item for item in allocation_decisions
    }
    promotable_baseline = [
        item for item in opportunities if item.legacy_selection_state == "promotable"
    ]
    monitor_baseline = [
        item for item in opportunities if item.legacy_selection_state == "monitor"
    ]
    allocated = [
        item
        for item in opportunities
        if allocation_by_opportunity_id.get(item.opportunity_id) is not None
        and allocation_by_opportunity_id[item.opportunity_id].allocation_state
        == "allocated"
    ]
    comparison_size = max(len(promotable_baseline), len(allocated))
    promotable_rank_only = [
        item for item in opportunities if item.state == "promotable"
    ]
    rank_only_top = (
        []
        if comparison_size <= 0
        else (promotable_rank_only[:comparison_size] or opportunities[:comparison_size])
    )

    allocated_ids = {item.opportunity_id for item in allocated}
    promotable_baseline_ids = {
        item.opportunity_id for item in promotable_baseline
    }
    rank_only_ids = {item.opportunity_id for item in rank_only_top}

    return _with_legacy_aliases(
        {
            "comparison_size": comparison_size,
            "promotable_baseline": {
                "count": len(promotable_baseline),
                "symbols": sorted({item.symbol for item in promotable_baseline}),
                "candidate_ids": [item.candidate_id for item in promotable_baseline],
                "items": [
                    _comparison_item(
                        item,
                        allocation_by_opportunity_id.get(item.opportunity_id),
                        outcome_matches.get(item.opportunity_id),
                    )
                    for item in promotable_baseline
                ],
            },
            "monitor_baseline": {
                "count": len(monitor_baseline),
                "symbols": sorted({item.symbol for item in monitor_baseline}),
                "candidate_ids": [item.candidate_id for item in monitor_baseline],
            },
            "rank_only_top": {
                "count": len(rank_only_top),
                "symbols": sorted({item.symbol for item in rank_only_top}),
                "candidate_ids": [item.candidate_id for item in rank_only_top],
                "items": [
                    _comparison_item(
                        item,
                        allocation_by_opportunity_id.get(item.opportunity_id),
                        outcome_matches.get(item.opportunity_id),
                    )
                    for item in rank_only_top
                ],
            },
            "provisional_allocator": {
                "count": len(allocated),
                "symbols": sorted({item.symbol for item in allocated}),
                "candidate_ids": [item.candidate_id for item in allocated],
                "items": [
                    _comparison_item(
                        item,
                        allocation_by_opportunity_id.get(item.opportunity_id),
                        outcome_matches.get(item.opportunity_id),
                    )
                    for item in allocated
                ],
            },
            "allocator_promoted_monitor": [
                _comparison_item(
                    item,
                    allocation_by_opportunity_id.get(item.opportunity_id),
                    outcome_matches.get(item.opportunity_id),
                )
                for item in allocated
                if item.legacy_selection_state == "monitor"
            ],
            "allocator_rejected_promotable_baseline": [
                _comparison_item(
                    item,
                    allocation_by_opportunity_id.get(item.opportunity_id),
                    outcome_matches.get(item.opportunity_id),
                )
                for item in promotable_baseline
                if item.opportunity_id not in allocated_ids
            ],
            "rank_only_rejected_by_allocator": [
                _comparison_item(
                    item,
                    allocation_by_opportunity_id.get(item.opportunity_id),
                    outcome_matches.get(item.opportunity_id),
                )
                for item in rank_only_top
                if item.opportunity_id not in allocated_ids
            ],
            "allocator_added_outside_rank_only": [
                _comparison_item(
                    item,
                    allocation_by_opportunity_id.get(item.opportunity_id),
                    outcome_matches.get(item.opportunity_id),
                )
                for item in allocated
                if item.opportunity_id not in rank_only_ids
            ],
            "overlap": _with_legacy_aliases(
                {
                    "allocator_vs_promotable_baseline_count": len(
                        allocated_ids & promotable_baseline_ids
                    ),
                    "allocator_vs_rank_only_count": len(allocated_ids & rank_only_ids),
                },
                aliases={
                    "allocator_vs_legacy_promotable_baseline_count": "allocator_vs_promotable_baseline_count"
                },
            ),
        },
        aliases={
            "legacy_promotable_baseline": "promotable_baseline",
            "legacy_monitor_baseline": "monitor_baseline",
            "promoted_from_legacy_monitor": "allocator_promoted_monitor",
            "rejected_legacy_promotable": "allocator_rejected_promotable_baseline",
        },
    )


def _build_summary(
    *,
    cycle: Mapping[str, Any],
    opportunities: list[Opportunity],
    allocation_decisions: list[AllocationDecision],
    calibration_lookup: dict[str, dict[str, dict[str, Any]]],
    calibration_meta: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    allocation_by_opportunity_id = {
        item.opportunity_id: item for item in allocation_decisions
    }
    promotable_baseline_symbols = sorted(
        {
            item.symbol
            for item in opportunities
            if item.legacy_selection_state == "promotable"
        }
    )
    allocated_symbols = sorted(
        {
            item.symbol
            for item in opportunities
            if allocation_by_opportunity_id.get(item.opportunity_id) is not None
            and allocation_by_opportunity_id[item.opportunity_id].allocation_state
            == "allocated"
        }
    )
    allocated_opportunity_ids = [
        decision.opportunity_id
        for decision in allocation_decisions
        if decision.allocation_state == "allocated"
    ]
    promoted_monitor_symbols = sorted(
        {
            item.symbol
            for item in opportunities
            if item.legacy_selection_state == "monitor" and item.state == "promotable"
        }
    )
    summary = _with_legacy_aliases(
        {
            "label": cycle.get("label"),
            "session_date": cycle.get("session_date"),
            "cycle_id": cycle.get("cycle_id"),
            "legacy_profile": cycle.get("profile"),
            "style_profile": opportunities[0].style_profile if opportunities else None,
            "candidate_count": len(opportunities),
            "promotable_count": sum(
                1 for item in opportunities if item.state == "promotable"
            ),
            "allocated_count": len(allocated_opportunity_ids),
            "allocated_opportunity_ids": allocated_opportunity_ids,
            "allocated_symbols": allocated_symbols,
            "promotable_baseline_symbols": promotable_baseline_symbols,
            "promoted_monitor_symbols": promoted_monitor_symbols,
            "analysis_verdict": None,
            "historical_calibration_session_count": int(
                calibration_meta.get("source_session_count") or 0
            ),
        },
        aliases={
            "legacy_promotable_baseline_symbols": "promotable_baseline_symbols",
            "newly_promoted_legacy_monitor_symbols": "promoted_monitor_symbols",
        },
    )

    warnings = [
        "Regime fields are inferred from persisted candidate payloads because collector cycles do not store full regime snapshots yet.",
        "The allocator is provisional and uses deterministic heuristic budgets; it is for offline comparison only.",
    ]
    if int(calibration_meta.get("source_session_count") or 0) <= 0:
        warnings.append(
            "No prior succeeded post-market sessions were available for calibration, so promotion scores rely on raw candidate features only."
        )
    else:
        classification_lookup = calibration_lookup.get("classification", {})
        promotable_row = classification_lookup.get("promotable")
        monitor_row = classification_lookup.get("monitor")
        promotable_pnl = (
            None
            if promotable_row is None
            else _as_float(promotable_row.get("average_estimated_pnl"))
        )
        monitor_pnl = (
            None
            if monitor_row is None
            else _as_float(monitor_row.get("average_estimated_pnl"))
        )
        if (
            promotable_pnl is not None
            and monitor_pnl is not None
            and monitor_pnl > promotable_pnl
        ):
            warnings.append(
                "Prior-session calibration for this label favors monitor-baseline ideas over the promotable baseline, so monitor candidates may surface as promotable."
            )
    return summary, warnings
