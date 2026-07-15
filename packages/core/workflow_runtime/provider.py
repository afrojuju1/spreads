from __future__ import annotations

from collections.abc import Sequence
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
)

from core.jobs.registry import get_workflow_lane_for_job_type
from core.jobs.orchestration import market_boundary_slot, market_session_slots
from core.services.market_dates import NEW_YORK, market_session_windows
from core.workflows.scheduled_job import ScheduledJobWorkflow

PROVIDER_NAME = "temporal"
MARKET_SCHEDULE_HORIZON_DAYS = 400


def provider_queue_for_lane(lane: str) -> str:
    normalized = str(lane or "").strip().lower()
    if not normalized:
        raise ValueError("workflow lane is required")
    return f"spreads-{normalized}-workflows"


def _range(start: int, end: int | None = None, step: int = 1) -> ScheduleRange:
    return ScheduleRange(start=start, end=start if end is None else end, step=step)


def _exact_ranges(values: Sequence[int]) -> tuple[ScheduleRange, ...]:
    return tuple(_range(value) for value in sorted(set(values)))


def _stepped_ranges(values: Sequence[int]) -> tuple[ScheduleRange, ...]:
    remaining = sorted(set(values))
    if len(remaining) < 2:
        return _exact_ranges(remaining)
    step = remaining[1] - remaining[0]
    if step > 0 and remaining == list(range(remaining[0], remaining[-1] + 1, step)):
        return (_range(remaining[0], remaining[-1], step),)
    return _exact_ranges(remaining)


def _calendar_specs_for_slots(slots: Sequence[datetime], *, comment: str) -> Sequence[ScheduleCalendarSpec]:
    per_day_hour: dict[tuple[int, int, int, int, int], set[int]] = defaultdict(set)
    for slot in slots:
        local = slot.astimezone(NEW_YORK)
        per_day_hour[(local.year, local.month, local.day, local.hour, local.second)].add(local.minute)

    days_by_hour_pattern: dict[tuple[int, int, int, int, tuple[int, ...]], set[int]] = defaultdict(set)
    for (year, month, day, hour, second), minutes in per_day_hour.items():
        days_by_hour_pattern[(year, month, hour, second, tuple(sorted(minutes)))].add(day)

    hours_by_calendar_pattern: dict[tuple[int, int, int, tuple[int, ...], tuple[int, ...]], set[int]] = defaultdict(set)
    for (year, month, hour, second, minutes), days in days_by_hour_pattern.items():
        hours_by_calendar_pattern[(year, month, second, minutes, tuple(sorted(days)))].add(hour)

    return tuple(
        ScheduleCalendarSpec(
            second=(_range(second),),
            minute=_stepped_ranges(minutes),
            hour=_stepped_ranges(tuple(hours)),
            day_of_month=_exact_ranges(days),
            month=(_range(month),),
            year=(_range(year),),
            comment=comment,
        )
        for (year, month, second, minutes, days), hours in sorted(hours_by_calendar_pattern.items())
    )


def _market_sessions_in_horizon(definition: dict[str, Any]) -> tuple[tuple[date, datetime, datetime], ...]:
    today = datetime.now(UTC).astimezone(NEW_YORK).date()
    calendar_name = str(definition.get("market_calendar") or "NYSE")
    return market_session_windows(
        calendar_name,
        today,
        today + timedelta(days=MARKET_SCHEDULE_HORIZON_DAYS),
    )


def _market_session_calendars(definition: dict[str, Any]) -> Sequence[ScheduleCalendarSpec]:
    slots = tuple(
        slot
        for session_day, market_open, market_close in _market_sessions_in_horizon(definition)
        for slot in market_session_slots(
            definition,
            session_day,
            market_window=(market_open, market_close),
        )
    )
    return _calendar_specs_for_slots(slots, comment="exchange-calendar market session")


def _market_boundary_calendars(definition: dict[str, Any], *, close: bool) -> Sequence[ScheduleCalendarSpec]:
    slots = tuple(
        slot
        for session_day, market_open, market_close in _market_sessions_in_horizon(definition)
        if (
            slot := market_boundary_slot(
                definition,
                session_day,
                close=close,
                market_window=(market_open, market_close),
            )
        )
        is not None
    )
    return _calendar_specs_for_slots(
        slots,
        comment="exchange-calendar market close" if close else "exchange-calendar market open",
    )


def _calendar_schedule(definition: dict[str, Any]) -> Sequence[ScheduleCalendarSpec]:
    schedule = dict(definition.get("schedule") or {})
    python_days = tuple(int(value) for value in schedule.get("days_of_week", range(7)))
    provider_days = tuple(_range((day + 1) % 7) for day in python_days)
    return (
        ScheduleCalendarSpec(
            second=(_range(max(min(int(schedule.get("second", 0)), 59), 0)),),
            minute=(_range(max(min(int(schedule.get("minute", 0)), 59), 0)),),
            hour=(_range(max(min(int(schedule.get("hour", 0)), 23), 0)),),
            day_of_week=provider_days,
            comment="calendar routine",
        ),
    )


def build_provider_schedule(definition: dict[str, Any], *, schedule_id: str) -> Schedule:
    job_type = str(definition["job_type"])
    lane = get_workflow_lane_for_job_type(job_type)
    if lane is None:
        raise RuntimeError(f"Job type is not registered to a workflow lane: {job_type}")
    schedule_type = str(definition.get("schedule_type") or "manual")
    schedule_payload = dict(definition.get("schedule") or {})
    if schedule_type == "manual":
        raise ValueError("Manual routines do not have provider schedules")
    if schedule_type == "interval":
        cadence = max(int(schedule_payload.get("minutes", 1)), 1)
        offset = max(int(schedule_payload.get("offset_seconds", 0)), 0)
        spec = ScheduleSpec(
            intervals=[ScheduleIntervalSpec(every=timedelta(minutes=cadence), offset=timedelta(seconds=offset))],
            time_zone_name="America/New_York",
        )
    elif schedule_type == "market_session":
        spec = ScheduleSpec(calendars=_market_session_calendars(definition), time_zone_name="America/New_York")
    elif schedule_type == "market_open":
        spec = ScheduleSpec(calendars=_market_boundary_calendars(definition, close=False), time_zone_name="America/New_York")
    elif schedule_type == "market_close":
        spec = ScheduleSpec(calendars=_market_boundary_calendars(definition, close=True), time_zone_name="America/New_York")
    elif schedule_type == "calendar":
        spec = ScheduleSpec(calendars=_calendar_schedule(definition), time_zone_name="America/New_York")
    else:
        raise ValueError(f"Unsupported routine schedule type: {schedule_type}")
    return Schedule(
        action=ScheduleActionStartWorkflow(
            ScheduledJobWorkflow.run,
            {"job_key": str(definition["job_key"])},
            id=schedule_id,
            task_queue=provider_queue_for_lane(lane),
        ),
        spec=spec,
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP, catchup_window=timedelta(minutes=5)),
        state=ScheduleState(
            paused=not bool(definition.get("enabled")),
            note=f"Spreads routine {definition['job_key']}",
        ),
    )


async def connect_provider(*, address: str, namespace: str) -> Client:
    return await Client.connect(address, namespace=namespace)


__all__ = [
    "PROVIDER_NAME",
    "build_provider_schedule",
    "connect_provider",
    "provider_queue_for_lane",
]
