from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

CalendarStatus = Literal["clean", "penalized", "blocked", "unknown"]
SourceConfidence = Literal["high", "medium", "low", "unknown"]
EarningsEventPhase = Literal[
    "clean",
    "pre_event_runup",
    "through_event",
    "post_event_fresh",
    "post_event_settled",
]
EventSessionTiming = Literal["before_open", "after_close", "during_market", "unknown"]


@dataclass(frozen=True)
class CalendarEventRecord:
    event_id: str
    event_type: str
    symbol: str | None
    asset_scope: str | None
    scheduled_at: str
    window_start: str
    window_end: str
    source: str
    source_confidence: SourceConfidence
    status: str
    payload_json: str
    ingested_at: str
    source_updated_at: str


@dataclass(frozen=True)
class CalendarEventQuery:
    symbol: str
    strategy: str
    window_start: str
    window_end: str
    underlying_type: str


@dataclass(frozen=True)
class CalendarEventReason:
    code: str
    event_type: str
    severity: str
    message: str
    scheduled_at: str | None = None
    source: str | None = None


@dataclass(frozen=True)
class CalendarEventContext:
    status: CalendarStatus
    reasons: tuple[CalendarEventReason, ...] = field(default_factory=tuple)
    days_to_nearest_event: int | None = None
    events_before_expiry: int = 0
    assignment_risk: bool = False
    macro_regime: str | None = None
    source_confidence: SourceConfidence = "unknown"
    sources: tuple[str, ...] = field(default_factory=tuple)
    last_updated: str | None = None
    earnings_phase: EarningsEventPhase = "clean"
    earnings_event_date: str | None = None
    earnings_session_timing: EventSessionTiming = "unknown"
    earnings_cohort_key: str | None = None
    earnings_days_to_event: int | None = None
    earnings_days_since_event: int | None = None
    earnings_timing_confidence: SourceConfidence = "unknown"
    earnings_horizon_crosses_report: bool = False


@dataclass(frozen=True)
class CalendarPolicyDecision:
    status: CalendarStatus
    reasons: tuple[CalendarEventReason, ...] = field(default_factory=tuple)
    days_to_nearest_event: int | None = None
    events_before_expiry: int = 0
    assignment_risk: bool = False
    macro_regime: str | None = None
    source_confidence: SourceConfidence = "unknown"
    sources: tuple[str, ...] = field(default_factory=tuple)
    last_updated: str | None = None
    earnings_phase: EarningsEventPhase = "clean"
    earnings_event_date: str | None = None
    earnings_session_timing: EventSessionTiming = "unknown"
    earnings_cohort_key: str | None = None
    earnings_days_to_event: int | None = None
    earnings_days_since_event: int | None = None
    earnings_timing_confidence: SourceConfidence = "unknown"
    earnings_horizon_crosses_report: bool = False
