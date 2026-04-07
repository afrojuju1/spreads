from __future__ import annotations

from calendar_events.config import PENALTY_EVENT_CODES
from calendar_events.models import CalendarEventContext, CalendarPolicyDecision


def apply_credit_spread_policy(
    context: CalendarEventContext,
    *,
    strategy: str,
    underlying_type: str,
    mode: str,
) -> CalendarPolicyDecision:
    if mode == "off":
        return CalendarPolicyDecision(status="clean")

    codes = {reason.code for reason in context.reasons}
    blocking_codes = {"earnings_before_expiry"}
    if strategy == "call_credit":
        blocking_codes.add("ex_dividend_before_expiry")
    penalty_codes = set(PENALTY_EVENT_CODES)
    if strategy == "put_credit" and "ex_dividend_before_expiry" in codes:
        penalty_codes.add("ex_dividend_before_expiry")

    if mode == "strict":
        if codes & blocking_codes:
            status = "blocked"
        elif context.status == "unknown":
            status = "blocked"
        elif codes & penalty_codes:
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


def apply_call_credit_spread_policy(
    context: CalendarEventContext,
    *,
    underlying_type: str,
    mode: str,
) -> CalendarPolicyDecision:
    return apply_credit_spread_policy(
        context,
        strategy="call_credit",
        underlying_type=underlying_type,
        mode=mode,
    )
