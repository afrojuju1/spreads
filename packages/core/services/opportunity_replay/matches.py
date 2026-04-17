from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from core.domain.opportunity_models import Opportunity
from core.services.analysis_helpers import (
    candidate_identity,
    resolved_estimated_pnl,
)
from core.services.execution import list_session_execution_attempts
from core.services.live_pipelines import parse_live_run_scope_id
from core.services.positions import enrich_position_row

from .shared import (
    _as_float,
    _as_text,
    _minutes_between,
    _timestamp_is_after,
)


def _opportunity_identity(opportunity: Opportunity) -> tuple[str, str, str, str, str]:
    return (
        opportunity.symbol,
        opportunity.legacy_strategy,
        opportunity.expiration_date,
        opportunity.short_symbol,
        opportunity.long_symbol,
    )


def _position_identity(position: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(position.get("underlying_symbol") or ""),
        str(position.get("strategy") or ""),
        str(position.get("expiration_date") or ""),
        str(position.get("short_symbol") or ""),
        str(position.get("long_symbol") or ""),
    )


def _attempt_trade_intent(attempt: Mapping[str, Any]) -> str:
    request = attempt.get("request")
    if isinstance(request, Mapping):
        requested = _as_text(request.get("trade_intent"))
        if requested is not None:
            return requested.lower()
    return (_as_text(attempt.get("trade_intent")) or "open").lower()


def _attempt_force_close_at(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    if not isinstance(request, Mapping):
        return None
    exit_policy = request.get("exit_policy")
    if not isinstance(exit_policy, Mapping):
        return None
    return _as_text(exit_policy.get("force_close_at"))


def _attempt_source_reason(attempt: Mapping[str, Any]) -> str | None:
    request = attempt.get("request")
    if not isinstance(request, Mapping):
        return None
    source = request.get("source")
    if not isinstance(source, Mapping):
        return None
    return _as_text(source.get("reason"))


def _attempt_fill_timestamp(attempt: Mapping[str, Any]) -> str | None:
    fill_times = [
        _as_text(fill.get("filled_at"))
        for fill in attempt.get("fills") or []
        if isinstance(fill, Mapping)
    ]
    filtered = [value for value in fill_times if value]
    if filtered:
        return max(filtered)
    return _as_text(attempt.get("completed_at")) or _as_text(
        attempt.get("submitted_at")
    )


def _empty_execution_match() -> dict[str, Any]:
    return {
        "execution_attempted": False,
        "execution_attempt_ids": [],
        "close_execution_attempt_ids": [],
        "open_attempt_count": 0,
        "open_filled_attempt_count": 0,
        "open_expired_attempt_count": 0,
        "open_failed_attempt_count": 0,
        "open_status_counts": {},
        "open_fill_minutes_total": 0.0,
        "open_fill_minutes_count": 0,
        "average_open_fill_minutes": None,
        "requested_after_force_close_count": 0,
        "opened_after_force_close_count": 0,
        "request_to_force_close_total": 0.0,
        "request_to_force_close_count": 0,
        "average_minutes_to_force_close_at_request": None,
        "fill_to_force_close_total": 0.0,
        "fill_to_force_close_count": 0,
        "average_minutes_to_force_close_at_fill": None,
        "entry_credit_capture_total": 0.0,
        "entry_credit_capture_count": 0,
        "average_entry_credit_capture_pct": None,
        "entry_limit_retention_total": 0.0,
        "entry_limit_retention_count": 0,
        "average_entry_limit_retention_pct": None,
        "close_attempt_count": 0,
        "close_filled_attempt_count": 0,
        "close_status_counts": {},
        "close_fill_minutes_total": 0.0,
        "close_fill_minutes_count": 0,
        "average_close_fill_minutes": None,
        "force_close_exit_count": 0,
    }


def _build_position_matches(
    *,
    opportunities: list[Opportunity],
    storage: Any,
    session_id: str | None,
    positions: list[Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    if session_id is None or not storage.execution.portfolio_schema_ready():
        return {}

    resolved_scope = parse_live_run_scope_id(session_id)
    if resolved_scope is None:
        return {}
    resolved_positions = (
        list(positions)
        if positions is not None
        else [
            enrich_position_row(dict(row))
            for row in storage.execution.list_positions(
                pipeline_id=f"pipeline:{resolved_scope['label']}",
                market_date=resolved_scope["market_date"],
            )
        ]
    )
    positions_by_identity: dict[
        tuple[str, str, str, str, str], list[Mapping[str, Any]]
    ] = defaultdict(list)
    for position in resolved_positions:
        positions_by_identity[_position_identity(position)].append(position)

    matches: dict[str, dict[str, Any]] = {}
    for opportunity in opportunities:
        matched_positions = positions_by_identity.get(
            _opportunity_identity(opportunity), []
        )
        if not matched_positions:
            matches[opportunity.opportunity_id] = {
                "actual_position_matched": False,
                "actual_position_count": 0,
                "actual_position_ids": [],
                "actual_position_status_counts": {},
                "actual_closed_rate": None,
                "actual_realized_pnl": None,
                "actual_unrealized_pnl": None,
                "actual_net_pnl": None,
                "actual_positive_outcome": None,
            }
            continue

        status_counts: dict[str, int] = defaultdict(int)
        for row in matched_positions:
            status_counts[str(row.get("status") or "unknown")] += 1
        position_count = len(matched_positions)
        realized_total = round(
            sum(_as_float(row.get("realized_pnl")) or 0.0 for row in matched_positions),
            4,
        )
        unrealized_values = [
            _as_float(row.get("unrealized_pnl")) for row in matched_positions
        ]
        unrealized_total = (
            None
            if not any(value is not None for value in unrealized_values)
            else round(sum(value or 0.0 for value in unrealized_values), 4)
        )
        net_total = round(realized_total + (unrealized_total or 0.0), 4)
        closed_count = sum(
            count
            for status, count in status_counts.items()
            if status in {"closed", "expired"}
        )
        matches[opportunity.opportunity_id] = {
            "actual_position_matched": True,
            "actual_position_count": position_count,
            "actual_position_ids": [
                str(row.get("position_id"))
                for row in matched_positions
                if row.get("position_id") is not None
            ],
            "actual_position_status_counts": dict(sorted(status_counts.items())),
            "actual_closed_rate": round(closed_count / position_count, 4),
            "actual_realized_pnl": realized_total,
            "actual_unrealized_pnl": unrealized_total,
            "actual_net_pnl": net_total,
            "actual_positive_outcome": net_total > 0.0,
        }
    return matches


def _build_execution_matches(
    *,
    opportunities: list[Opportunity],
    storage: Any,
    session_id: str | None,
    positions: list[Mapping[str, Any]] | None = None,
    attempts: list[Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    if session_id is None or not storage.execution.schema_ready():
        return {}

    resolved_scope = parse_live_run_scope_id(session_id)
    if resolved_scope is None:
        return {}
    resolved_positions = (
        list(positions)
        if positions is not None
        else [
            enrich_position_row(dict(row))
            for row in storage.execution.list_positions(
                pipeline_id=f"pipeline:{resolved_scope['label']}",
                market_date=resolved_scope["market_date"],
            )
        ]
    )
    resolved_attempts = (
        [dict(item) for item in attempts]
        if attempts is not None
        else list_session_execution_attempts(
            db_target="",
            session_id=session_id,
            limit=500,
            storage=storage,
        )
    )

    opportunity_by_candidate_id = {
        int(item.candidate_id): item
        for item in opportunities
        if int(item.candidate_id) > 0
    }
    opportunity_by_id = {item.opportunity_id: item for item in opportunities}
    opportunity_by_identity = {
        _opportunity_identity(item): item for item in opportunities
    }
    position_by_id = {
        str(row.get("position_id")): row
        for row in resolved_positions
        if row.get("position_id") is not None
    }
    position_to_opportunity_id: dict[str, str] = {}
    for row in resolved_positions:
        opportunity: Opportunity | None = None
        candidate_id = row.get("candidate_id")
        if candidate_id is not None:
            try:
                opportunity = opportunity_by_candidate_id.get(int(candidate_id))
            except (TypeError, ValueError):
                opportunity = None
        if opportunity is None:
            opportunity = opportunity_by_identity.get(_position_identity(row))
        if opportunity is None or row.get("position_id") is None:
            continue
        position_to_opportunity_id[str(row["position_id"])] = opportunity.opportunity_id

    matches = {item.opportunity_id: _empty_execution_match() for item in opportunities}

    for raw_attempt in resolved_attempts:
        attempt = dict(raw_attempt)
        trade_intent = _attempt_trade_intent(attempt)
        opportunity_id: str | None = None
        if trade_intent == "open":
            candidate_id = attempt.get("candidate_id")
            if candidate_id is not None:
                try:
                    matched_opportunity = opportunity_by_candidate_id.get(
                        int(candidate_id)
                    )
                except (TypeError, ValueError):
                    matched_opportunity = None
            else:
                matched_opportunity = None
            if matched_opportunity is None:
                matched_opportunity = opportunity_by_identity.get(
                    _position_identity(attempt)
                )
            if matched_opportunity is not None:
                opportunity_id = matched_opportunity.opportunity_id
        elif trade_intent == "close":
            request = attempt.get("request")
            request_position_id = (
                _as_text(request.get("position_id"))
                if isinstance(request, Mapping)
                else None
            )
            position_id = _as_text(attempt.get("position_id")) or request_position_id
            if position_id is not None:
                opportunity_id = position_to_opportunity_id.get(position_id)

        if opportunity_id is None:
            continue

        match = matches[opportunity_id]
        match["execution_attempted"] = True
        status = (_as_text(attempt.get("status")) or "unknown").lower()

        if trade_intent == "open":
            match["execution_attempt_ids"].append(str(attempt["execution_attempt_id"]))
            match["open_attempt_count"] += 1
            open_status_counts = match["open_status_counts"]
            open_status_counts[status] = int(open_status_counts.get(status) or 0) + 1
            if status == "filled":
                match["open_filled_attempt_count"] += 1
            elif status == "expired":
                match["open_expired_attempt_count"] += 1
            elif status in {"canceled", "failed", "rejected"}:
                match["open_failed_attempt_count"] += 1

            fill_minutes = _minutes_between(
                attempt.get("requested_at"),
                _attempt_fill_timestamp(attempt),
            )
            if fill_minutes is not None and status == "filled":
                match["open_fill_minutes_total"] += float(fill_minutes)
                match["open_fill_minutes_count"] += 1

            force_close_at = _attempt_force_close_at(attempt)
            requested_after_force_close = _timestamp_is_after(
                attempt.get("requested_at"),
                force_close_at,
            )
            if requested_after_force_close:
                match["requested_after_force_close_count"] += 1
            request_to_force_close_minutes = _minutes_between(
                attempt.get("requested_at"),
                force_close_at,
            )
            if request_to_force_close_minutes is not None:
                match["request_to_force_close_total"] += float(
                    request_to_force_close_minutes
                )
                match["request_to_force_close_count"] += 1

            fill_time = _attempt_fill_timestamp(attempt)
            opened_after_force_close = _timestamp_is_after(fill_time, force_close_at)
            if opened_after_force_close and status == "filled":
                match["opened_after_force_close_count"] += 1
            fill_to_force_close_minutes = _minutes_between(fill_time, force_close_at)
            if fill_to_force_close_minutes is not None and status == "filled":
                match["fill_to_force_close_total"] += float(fill_to_force_close_minutes)
                match["fill_to_force_close_count"] += 1

            session_position_id = _as_text(attempt.get("position_id"))
            position = (
                None
                if session_position_id is None
                else position_by_id.get(session_position_id)
            )
            entry_credit = None
            if isinstance(position, Mapping):
                entry_credit = _as_float(position.get("entry_credit"))
            if entry_credit is None:
                entry_credit = _as_float(attempt.get("limit_price"))
            opportunity = opportunity_by_id.get(opportunity_id)
            midpoint_credit = None
            if opportunity is not None:
                midpoint_credit = _as_float(opportunity.evidence.get("midpoint_credit"))
            if (
                entry_credit is not None
                and midpoint_credit is not None
                and midpoint_credit > 0.0
            ):
                match["entry_credit_capture_total"] += entry_credit / midpoint_credit
                match["entry_credit_capture_count"] += 1
            limit_price = _as_float(attempt.get("limit_price"))
            if entry_credit is not None and limit_price not in (None, 0.0):
                match["entry_limit_retention_total"] += entry_credit / limit_price
                match["entry_limit_retention_count"] += 1

        elif trade_intent == "close":
            match["close_execution_attempt_ids"].append(
                str(attempt["execution_attempt_id"])
            )
            match["close_attempt_count"] += 1
            close_status_counts = match["close_status_counts"]
            close_status_counts[status] = int(close_status_counts.get(status) or 0) + 1
            if status == "filled":
                match["close_filled_attempt_count"] += 1
            close_fill_minutes = _minutes_between(
                attempt.get("requested_at"),
                _attempt_fill_timestamp(attempt),
            )
            if close_fill_minutes is not None and status == "filled":
                match["close_fill_minutes_total"] += float(close_fill_minutes)
                match["close_fill_minutes_count"] += 1
            if _attempt_source_reason(attempt) == "force_close":
                match["force_close_exit_count"] += 1

    for match in matches.values():
        open_fill_count = int(match["open_fill_minutes_count"] or 0)
        if open_fill_count > 0:
            match["average_open_fill_minutes"] = round(
                float(match["open_fill_minutes_total"]) / open_fill_count,
                4,
            )
        request_force_close_count = int(match["request_to_force_close_count"] or 0)
        if request_force_close_count > 0:
            match["average_minutes_to_force_close_at_request"] = round(
                float(match["request_to_force_close_total"])
                / request_force_close_count,
                4,
            )
        fill_force_close_count = int(match["fill_to_force_close_count"] or 0)
        if fill_force_close_count > 0:
            match["average_minutes_to_force_close_at_fill"] = round(
                float(match["fill_to_force_close_total"]) / fill_force_close_count,
                4,
            )
        capture_count = int(match["entry_credit_capture_count"] or 0)
        if capture_count > 0:
            match["average_entry_credit_capture_pct"] = round(
                float(match["entry_credit_capture_total"]) / capture_count,
                4,
            )
        retention_count = int(match["entry_limit_retention_count"] or 0)
        if retention_count > 0:
            match["average_entry_limit_retention_pct"] = round(
                float(match["entry_limit_retention_total"]) / retention_count,
                4,
            )
        close_fill_count = int(match["close_fill_minutes_count"] or 0)
        if close_fill_count > 0:
            match["average_close_fill_minutes"] = round(
                float(match["close_fill_minutes_total"]) / close_fill_count,
                4,
            )
        match["open_status_counts"] = dict(sorted(match["open_status_counts"].items()))
        match["close_status_counts"] = dict(
            sorted(match["close_status_counts"].items())
        )
    return matches


def _build_outcome_matches(
    *,
    opportunities: list[Opportunity],
    analysis_run: Mapping[str, Any] | None,
    storage: Any,
    session_id: str | None,
) -> dict[str, dict[str, Any]]:
    summary = analysis_run.get("summary") if isinstance(analysis_run, Mapping) else None
    outcomes = summary.get("outcomes") if isinstance(summary, Mapping) else None
    ideas = list(outcomes.get("ideas") or []) if isinstance(outcomes, Mapping) else []
    positions: list[Mapping[str, Any]] | None = None
    attempts: list[Mapping[str, Any]] | None = None
    if session_id is not None and storage.execution.schema_ready():
        resolved_scope = parse_live_run_scope_id(session_id)
        if resolved_scope is not None:
            positions = [
                enrich_position_row(dict(row))
                for row in storage.execution.list_positions(
                    pipeline_id=f"pipeline:{resolved_scope['label']}",
                    market_date=resolved_scope["market_date"],
                )
            ]
        attempts = list_session_execution_attempts(
            db_target="",
            session_id=session_id,
            limit=500,
            storage=storage,
        )
    position_matches = _build_position_matches(
        opportunities=opportunities,
        storage=storage,
        session_id=session_id,
        positions=positions,
    )
    execution_matches = _build_execution_matches(
        opportunities=opportunities,
        storage=storage,
        session_id=session_id,
        positions=positions,
        attempts=attempts,
    )

    lookup: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for idea in ideas:
        if not isinstance(idea, Mapping):
            continue
        try:
            identity = candidate_identity(idea)
        except KeyError:
            continue
        lookup[identity] = dict(idea)

    matches: dict[str, dict[str, Any]] = {}
    for opportunity in opportunities:
        idea = lookup.get(_opportunity_identity(opportunity))
        position_match = position_matches.get(opportunity.opportunity_id) or {}
        execution_match = execution_matches.get(opportunity.opportunity_id) or {}
        estimated_close_pnl = (
            None if idea is None else _as_float(idea.get("estimated_close_pnl"))
        )
        estimated_expiry_pnl = (
            None if idea is None else _as_float(idea.get("estimated_expiry_pnl"))
        )
        estimated_pnl = None if idea is None else resolved_estimated_pnl(idea)
        actual_net_pnl = _as_float(position_match.get("actual_net_pnl"))
        matches[opportunity.opportunity_id] = {
            "matched": idea is not None,
            "estimated_close_pnl": None
            if estimated_close_pnl is None
            else round(float(estimated_close_pnl), 4),
            "estimated_close_positive": None
            if estimated_close_pnl is None
            else float(estimated_close_pnl) > 0.0,
            "estimated_expiry_pnl": None
            if estimated_expiry_pnl is None
            else round(float(estimated_expiry_pnl), 4),
            "estimated_expiry_positive": None
            if estimated_expiry_pnl is None
            else float(estimated_expiry_pnl) > 0.0,
            "estimated_pnl": None
            if estimated_pnl is None
            else round(float(estimated_pnl), 4),
            "positive_outcome": None
            if estimated_pnl is None
            else float(estimated_pnl) > 0.0,
            "outcome_bucket": None if idea is None else idea.get("outcome_bucket"),
            "replay_verdict": None if idea is None else idea.get("replay_verdict"),
            "setup_status": None if idea is None else idea.get("setup_status"),
            "vwap_regime": None if idea is None else idea.get("vwap_regime"),
            "trend_regime": None if idea is None else idea.get("trend_regime"),
            "opening_range_regime": None
            if idea is None
            else idea.get("opening_range_regime"),
            "classification": None if idea is None else idea.get("classification"),
            "actual_minus_estimated_close_pnl": None
            if actual_net_pnl is None or estimated_close_pnl is None
            else round(actual_net_pnl - float(estimated_close_pnl), 4),
            **position_match,
            **execution_match,
        }
    return matches
