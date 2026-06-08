from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from core.domain.models import SpreadCandidate
from core.domain.profiles import (
    LONG_VOL_STRATEGIES,
    format_session_bucket,
    zero_dte_delta_target,
)
from core.integrations.calendar_events.models import CalendarPolicyDecision
from core.integrations.calendar_events.policy import apply_strategy_calendar_policy
from core.services.option_structures import candidate_legs, legs_identity_key
from core.services.ranking_policy import evaluate_candidate_ranking_policy
from core.services.strategy_candidate_builders.runtime_context import candidate_session_bucket, option_expiry_close
from core.services.strategy_candidate_builders.settings import normalize_calendar_confidence_policy

_CONSENSUS_BACKED_EARNINGS_STATUSES = {"consensus", "date_only"}
_RESEARCH_GRADE_EARNINGS_TIMING_CONFIDENCE = {"medium", "high"}


def _calendar_confidence_reason(
    candidate: SpreadCandidate,
    *,
    underlying_type: str,
    args: Any,
) -> str | None:
    if underlying_type != "single_name_equity":
        return None
    if str(candidate.calendar_confidence or "").strip().lower() != "low":
        return None

    policy = normalize_calendar_confidence_policy(getattr(args, "calendar_confidence_policy", None))
    if policy == "off":
        return None
    if policy == "consensus":
        consensus_status = str(candidate.earnings_consensus_status or "missing").strip().lower()
        timing_confidence = str(candidate.earnings_timing_confidence or "unknown").strip().lower()
        if consensus_status in _CONSENSUS_BACKED_EARNINGS_STATUSES and timing_confidence in _RESEARCH_GRADE_EARNINGS_TIMING_CONFIDENCE:
            return None
    return "Calendar data confidence is low for this single-name candidate"


def assess_data_quality(
    candidate: SpreadCandidate,
    *,
    underlying_type: str,
    args: Any,
) -> tuple[str, tuple[str, ...]]:
    if args.data_policy == "off":
        return "clean", ()

    reasons: list[str] = []
    blocked = False
    penalized = False
    long_vol = candidate.strategy in LONG_VOL_STRATEGIES

    if candidate.expected_move is None or candidate.expected_move <= 0:
        reason = "Missing expected-move estimate"
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)
    elif long_vol:
        if candidate.modeled_move_vs_break_even_move is not None and candidate.modeled_move_vs_break_even_move < 0.85:
            reason = "Modeled move does not clear the structure break-even cleanly " f"({candidate.modeled_move_vs_break_even_move:.2f} < 0.85)"
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)
    else:
        short_ratio = (candidate.short_vs_expected_move or 0.0) / candidate.expected_move
        breakeven_ratio = (candidate.breakeven_vs_expected_move or 0.0) / candidate.expected_move
        if short_ratio < args.min_short_vs_expected_move_ratio:
            reason = f"Structure strike sits too far inside expected move " f"({short_ratio:.2f} < {args.min_short_vs_expected_move_ratio:.2f})"
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)
        if breakeven_ratio < args.min_breakeven_vs_expected_move_ratio:
            reason = f"Breakeven sits too far inside expected move " f"({breakeven_ratio:.2f} < {args.min_breakeven_vs_expected_move_ratio:.2f})"
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)

    if candidate.fill_ratio < args.min_fill_ratio:
        reason = f"Natural-to-mid fill ratio is too weak " f"({candidate.fill_ratio:.2f} < {args.min_fill_ratio:.2f})"
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)

    calendar_confidence_reason = _calendar_confidence_reason(
        candidate,
        underlying_type=underlying_type,
        args=args,
    )
    if calendar_confidence_reason:
        reason = calendar_confidence_reason
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)

    if blocked:
        return "blocked", tuple(reasons)
    if penalized:
        return "penalized", tuple(reasons)
    return "clean", ()


def attach_data_quality(
    *,
    candidates: list[SpreadCandidate],
    underlying_type: str,
    args: Any,
) -> list[SpreadCandidate]:
    annotated = annotate_data_quality(
        candidates=candidates,
        underlying_type=underlying_type,
        args=args,
    )
    if args.data_policy != "strict":
        return annotated

    enriched: list[SpreadCandidate] = []
    for candidate in annotated:
        if candidate.data_status == "blocked":
            continue
        enriched.append(candidate)
    return enriched


def annotate_data_quality(
    *,
    candidates: list[SpreadCandidate],
    underlying_type: str,
    args: Any,
) -> list[SpreadCandidate]:
    enriched: list[SpreadCandidate] = []
    for candidate in candidates:
        status, reasons = assess_data_quality(candidate, underlying_type=underlying_type, args=args)
        enriched.append(replace(candidate, data_status=status, data_reasons=reasons))
    return enriched


def annotate_ranking_policy(
    *,
    candidates: list[SpreadCandidate],
    args: Any,
) -> list[SpreadCandidate]:
    enriched: list[SpreadCandidate] = []
    for candidate in candidates:
        evaluation = evaluate_candidate_ranking_policy(candidate, policy_source=args)
        enriched.append(
            replace(
                candidate,
                ranking_policy=dict(evaluation["policy"]),
                ranking_policy_status=str(evaluation["status"]),
                ranking_policy_blockers=tuple(evaluation["blockers"]),
                ranking_policy_margin_to_pass=dict(evaluation.get("margin_to_pass") or {}),
            )
        )
    return enriched


def attach_ranking_policy(
    *,
    candidates: list[SpreadCandidate],
    args: Any,
) -> list[SpreadCandidate]:
    annotated = annotate_ranking_policy(candidates=candidates, args=args)
    return [candidate for candidate in annotated if str(candidate.ranking_policy_status or "passed").lower() != "blocked"]


def build_selection_notes(candidate: SpreadCandidate, args: Any) -> tuple[str, ...]:
    notes: list[str] = []
    long_vol = candidate.strategy in LONG_VOL_STRATEGIES
    delta_target = args.short_delta_target
    if args.build_profile == "0dte":
        session_bucket = candidate_session_bucket(args) or "off_hours"
        notes.append(f"session-{format_session_bucket(session_bucket)}")
        delta_target = zero_dte_delta_target(session_bucket)
    if candidate.strategy == "long_straddle":
        notes.append("atm-move")
    elif candidate.strategy == "long_strangle":
        notes.append("winged-move")
    elif candidate.strategy == "long_call":
        notes.append("directional-call")
    elif candidate.strategy == "long_put":
        notes.append("directional-put")
    elif candidate.strategy == "short_call":
        notes.append("short-call")
    elif candidate.strategy == "short_put":
        notes.append("short-put")
    elif candidate.short_delta is not None and abs(abs(candidate.short_delta) - delta_target) <= 0.02:
        notes.append("delta-fit")
    if long_vol and candidate.modeled_move_vs_break_even_move is not None:
        if candidate.modeled_move_vs_break_even_move >= 1.0:
            notes.append("move-cheap")
        else:
            notes.append("move-rich")
    elif candidate.expected_move and candidate.short_vs_expected_move is not None:
        if candidate.strategy in {"long_call", "long_put"}:
            if (candidate.breakeven_vs_expected_move or 0.0) >= 0:
                notes.append("em-cleared")
            else:
                notes.append("em-short")
        elif candidate.short_vs_expected_move >= 0:
            notes.append("outside-em")
        else:
            notes.append("inside-em")
    if candidate.fill_ratio >= 0.80:
        notes.append("good-fill")
    elif candidate.fill_ratio >= args.min_fill_ratio:
        notes.append("acceptable-fill")
    if min(candidate.short_open_interest, candidate.long_open_interest) >= max(args.min_open_interest * 3, 500):
        notes.append("liquid")
    if candidate.calendar_status == "clean":
        notes.append("calendar-clean")
    elif candidate.calendar_status == "penalized":
        notes.append("calendar-risk")
    if candidate.earnings_phase not in {"", "clean", "post_event_settled"}:
        notes.append(f"earnings-{candidate.earnings_phase.replace('_', '-')}")
    if candidate.setup_status == "favorable":
        notes.append("setup-favorable")
    elif candidate.setup_status == "neutral":
        notes.append("setup-neutral")
    if candidate.data_status == "penalized":
        notes.append("data-caution")
    if str(candidate.ranking_policy_status or "").lower() == "blocked":
        notes.append("policy-blocked")
    if candidate.greeks_source != "alpaca":
        notes.append("local-greeks")
    if len(notes) > 4 and candidate.greeks_source != "alpaca" and "local-greeks" not in notes[:4]:
        notes = [*notes[:3], "local-greeks"]
    return tuple(notes[:4])


def attach_selection_notes(candidates: list[SpreadCandidate], args: Any) -> list[SpreadCandidate]:
    return [replace(candidate, selection_notes=build_selection_notes(candidate, args)) for candidate in candidates]


def deduplicate_candidates(candidates: list[SpreadCandidate], expand_duplicates: bool) -> list[SpreadCandidate]:
    if expand_duplicates:
        return candidates

    deduplicated: list[SpreadCandidate] = []
    seen_leg_sets: set[str] = set()
    for candidate in candidates:
        identity = legs_identity_key(
            strategy=candidate.strategy,
            legs=candidate_legs(candidate.to_payload()),
        )
        if identity in seen_leg_sets:
            continue
        seen_leg_sets.add(identity)
        deduplicated.append(candidate)
    return deduplicated


def build_calendar_reason_messages(decision: CalendarPolicyDecision) -> tuple[str, ...]:
    return tuple(reason.message for reason in decision.reasons)


def resolve_calendar_decisions_by_expiration(
    *,
    symbol: str,
    strategy: str,
    underlying_type: str,
    candidates: list[SpreadCandidate],
    resolver: Any,
    calendar_policy: str,
    refresh_calendar_events: bool,
    window_start: str | None = None,
) -> dict[str, CalendarPolicyDecision]:
    if calendar_policy == "off" or not candidates:
        return {}

    resolved_window_start = window_start or datetime.now(UTC).isoformat()
    decisions_by_expiration: dict[str, CalendarPolicyDecision] = {}
    for expiration_date in sorted({candidate.expiration_date for candidate in candidates}, reverse=True):
        context = resolver.resolve_calendar_context(
            symbol=symbol,
            strategy=strategy,
            window_start=resolved_window_start,
            window_end=option_expiry_close(expiration_date).isoformat(),
            underlying_type=underlying_type,
            refresh=refresh_calendar_events,
        )
        decisions_by_expiration[expiration_date] = apply_strategy_calendar_policy(
            context,
            strategy=strategy,
            underlying_type=underlying_type,
            mode=calendar_policy,
        )
    return decisions_by_expiration


def attach_calendar_decisions_from_map(
    *,
    candidates: list[SpreadCandidate],
    decisions_by_expiration: dict[str, CalendarPolicyDecision],
    calendar_policy: str,
) -> list[SpreadCandidate]:
    if calendar_policy == "off" or not candidates:
        return candidates

    filtered_candidates: list[SpreadCandidate] = []
    for candidate in candidates:
        decision = decisions_by_expiration.get(candidate.expiration_date)
        if decision is None:
            filtered_candidates.append(candidate)
            continue
        if calendar_policy == "strict" and decision.status == "blocked":
            continue
        filtered_candidates.append(
            replace(
                candidate,
                calendar_status=decision.status,
                calendar_reasons=build_calendar_reason_messages(decision),
                calendar_confidence=decision.source_confidence,
                calendar_sources=decision.sources,
                calendar_last_updated=decision.last_updated,
                calendar_days_to_nearest_event=decision.days_to_nearest_event,
                macro_regime=decision.macro_regime,
                earnings_phase=decision.earnings_phase,
                earnings_event_date=decision.earnings_event_date,
                earnings_session_timing=decision.earnings_session_timing,
                earnings_cohort_key=decision.earnings_cohort_key,
                earnings_days_to_event=decision.earnings_days_to_event,
                earnings_days_since_event=decision.earnings_days_since_event,
                earnings_timing_confidence=decision.earnings_timing_confidence,
                earnings_horizon_crosses_report=decision.earnings_horizon_crosses_report,
                earnings_primary_source=decision.earnings_primary_source,
                earnings_supporting_sources=decision.earnings_supporting_sources,
                earnings_consensus_status=decision.earnings_consensus_status,
                earnings_enrichment=dict(decision.earnings_enrichment),
            )
        )
    return filtered_candidates


def attach_calendar_decisions(
    *,
    symbol: str,
    strategy: str,
    underlying_type: str,
    candidates: list[SpreadCandidate],
    resolver: Any,
    calendar_policy: str,
    refresh_calendar_events: bool,
) -> list[SpreadCandidate]:
    if calendar_policy == "off" or not candidates:
        return candidates

    decisions_by_expiration = resolve_calendar_decisions_by_expiration(
        symbol=symbol,
        strategy=strategy,
        underlying_type=underlying_type,
        candidates=candidates,
        resolver=resolver,
        calendar_policy=calendar_policy,
        refresh_calendar_events=refresh_calendar_events,
    )
    return attach_calendar_decisions_from_map(
        candidates=candidates,
        decisions_by_expiration=decisions_by_expiration,
        calendar_policy=calendar_policy,
    )


__all__ = [
    "attach_calendar_decisions",
    "attach_calendar_decisions_from_map",
    "attach_data_quality",
    "attach_ranking_policy",
    "annotate_data_quality",
    "annotate_ranking_policy",
    "attach_selection_notes",
    "resolve_calendar_decisions_by_expiration",
    "deduplicate_candidates",
]
