from __future__ import annotations

from calendar_events.config import BLOCKING_EVENT_CODES, PENALTY_EVENT_CODES
from calendar_events.models import CalendarEventContext, CalendarPolicyDecision


def apply_call_credit_spread_policy(
    context: CalendarEventContext,
    *,
    underlying_type: str,
    mode: str,
) -> CalendarPolicyDecision:
    if mode == "off":
        return CalendarPolicyDecision(status="clean")

    codes = {reason.code for reason in context.reasons}
    if mode == "strict":
        if codes & BLOCKING_EVENT_CODES:
            status = "blocked"
        elif context.status == "unknown":
            status = "blocked"
        elif codes & PENALTY_EVENT_CODES:
            status = "penalized"
        else:
            status = "clean"
    elif mode == "warn":
        if context.status == "unknown":
            status = "unknown"
        elif context.reasons:
            status = "penalized"
        else:
            status = "clean"
    else:
        status = context.status

    return CalendarPolicyDecision(
        status=status,
        reasons=context.reasons,
        days_to_nearest_event=context.days_to_nearest_event,
        events_before_expiry=context.events_before_expiry,
        assignment_risk=context.assignment_risk,
        macro_regime=context.macro_regime,
        source_confidence=context.source_confidence,
        sources=context.sources,
        last_updated=context.last_updated,
    )
