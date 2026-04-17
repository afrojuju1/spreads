from __future__ import annotations

import argparse
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Any

from core.domain.models import SpreadCandidate
from core.domain.profiles import (
    LONG_VOL_STRATEGIES,
    format_session_bucket,
    zero_dte_delta_target,
    zero_dte_session_bucket,
)
from core.integrations.calendar_events.models import CalendarPolicyDecision
from core.integrations.calendar_events.policy import apply_strategy_calendar_policy
from core.services.scanners.market_data import option_expiry_close
from core.services.option_structures import candidate_legs, legs_identity_key


def assess_data_quality(
    candidate: SpreadCandidate,
    *,
    underlying_type: str,
    args: argparse.Namespace,
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
        if (
            candidate.modeled_move_vs_break_even_move is not None
            and candidate.modeled_move_vs_break_even_move < 0.85
        ):
            reason = (
                "Modeled move does not clear the structure break-even cleanly "
                f"({candidate.modeled_move_vs_break_even_move:.2f} < 0.85)"
            )
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)
    else:
        short_ratio = (
            candidate.short_vs_expected_move or 0.0
        ) / candidate.expected_move
        breakeven_ratio = (
            candidate.breakeven_vs_expected_move or 0.0
        ) / candidate.expected_move
        if short_ratio < args.min_short_vs_expected_move_ratio:
            reason = (
                f"Short strike sits too far inside expected move "
                f"({short_ratio:.2f} < {args.min_short_vs_expected_move_ratio:.2f})"
            )
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)
        if breakeven_ratio < args.min_breakeven_vs_expected_move_ratio:
            reason = (
                f"Breakeven sits too far inside expected move "
                f"({breakeven_ratio:.2f} < {args.min_breakeven_vs_expected_move_ratio:.2f})"
            )
            if args.data_policy == "strict":
                blocked = True
            else:
                penalized = True
            reasons.append(reason)

    if candidate.fill_ratio < args.min_fill_ratio:
        reason = (
            f"Natural-to-mid fill ratio is too weak "
            f"({candidate.fill_ratio:.2f} < {args.min_fill_ratio:.2f})"
        )
        if args.data_policy == "strict":
            blocked = True
        else:
            penalized = True
        reasons.append(reason)

    if (
        underlying_type == "single_name_equity"
        and candidate.calendar_confidence == "low"
    ):
        reason = "Calendar data confidence is low for this single-name candidate"
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
    args: argparse.Namespace,
) -> list[SpreadCandidate]:
    enriched: list[SpreadCandidate] = []
    for candidate in candidates:
        status, reasons = assess_data_quality(
            candidate, underlying_type=underlying_type, args=args
        )
        if args.data_policy == "strict" and status == "blocked":
            continue
        enriched.append(replace(candidate, data_status=status, data_reasons=reasons))
    return enriched


def build_selection_notes(
    candidate: SpreadCandidate, args: argparse.Namespace
) -> tuple[str, ...]:
    notes: list[str] = []
    long_vol = candidate.strategy in LONG_VOL_STRATEGIES
    delta_target = args.short_delta_target
    if args.profile == "0dte":
        session_bucket = zero_dte_session_bucket()
        notes.append(f"session-{format_session_bucket(session_bucket)}")
        delta_target = zero_dte_delta_target(session_bucket)
    if candidate.strategy == "long_straddle":
        notes.append("atm-move")
    elif candidate.strategy == "long_strangle":
        notes.append("winged-move")
    elif (
        candidate.short_delta is not None
        and abs(abs(candidate.short_delta) - delta_target) <= 0.02
    ):
        notes.append("delta-fit")
    if long_vol and candidate.modeled_move_vs_break_even_move is not None:
        if candidate.modeled_move_vs_break_even_move >= 1.0:
            notes.append("move-cheap")
        else:
            notes.append("move-rich")
    elif candidate.expected_move and candidate.short_vs_expected_move is not None:
        if candidate.short_vs_expected_move >= 0:
            notes.append("outside-em")
        else:
            notes.append("inside-em")
    if candidate.fill_ratio >= 0.80:
        notes.append("good-fill")
    elif candidate.fill_ratio >= args.min_fill_ratio:
        notes.append("acceptable-fill")
    if min(candidate.short_open_interest, candidate.long_open_interest) >= max(
        args.min_open_interest * 3, 500
    ):
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
    if candidate.greeks_source != "alpaca":
        notes.append("local-greeks")
    if (
        len(notes) > 4
        and candidate.greeks_source != "alpaca"
        and "local-greeks" not in notes[:4]
    ):
        notes = [*notes[:3], "local-greeks"]
    return tuple(notes[:4])


def attach_selection_notes(
    candidates: list[SpreadCandidate], args: argparse.Namespace
) -> list[SpreadCandidate]:
    return [
        replace(candidate, selection_notes=build_selection_notes(candidate, args))
        for candidate in candidates
    ]


def deduplicate_candidates(
    candidates: list[SpreadCandidate], expand_duplicates: bool
) -> list[SpreadCandidate]:
    if expand_duplicates:
        return candidates

    deduplicated: list[SpreadCandidate] = []
    seen_leg_sets: set[str] = set()
    for candidate in candidates:
        identity = legs_identity_key(
            strategy=candidate.strategy,
            legs=candidate_legs(asdict(candidate)),
        )
        if identity in seen_leg_sets:
            continue
        seen_leg_sets.add(identity)
        deduplicated.append(candidate)
    return deduplicated


def build_calendar_reason_messages(decision: CalendarPolicyDecision) -> tuple[str, ...]:
    return tuple(reason.message for reason in decision.reasons)


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

    window_start = datetime.now(UTC).isoformat()
    decisions_by_expiration: dict[str, CalendarPolicyDecision] = {}
    for expiration_date in sorted(
        {candidate.expiration_date for candidate in candidates}, reverse=True
    ):
        context = resolver.resolve_calendar_context(
            symbol=symbol,
            strategy=strategy,
            window_start=window_start,
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

    filtered_candidates: list[SpreadCandidate] = []
    for candidate in candidates:
        decision = decisions_by_expiration[candidate.expiration_date]
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
            )
        )
    return filtered_candidates


__all__ = [
    "attach_calendar_decisions",
    "attach_data_quality",
    "attach_selection_notes",
    "deduplicate_candidates",
]
