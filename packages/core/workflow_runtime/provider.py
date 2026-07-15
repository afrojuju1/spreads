from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
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
from core.services.market_dates import market_session_window
from core.workflows.scheduled_job import ScheduledJobWorkflow

PROVIDER_NAME = "temporal"


def provider_queue_for_lane(lane: str) -> str:
    normalized = str(lane or "").strip().lower()
    if not normalized:
        raise ValueError("workflow lane is required")
    return f"spreads-{normalized}-workflows"


def _range(start: int, end: int | None = None, step: int = 1) -> ScheduleRange:
    return ScheduleRange(start=start, end=start if end is None else end, step=step)


def _weekday_range() -> tuple[ScheduleRange, ...]:
    # Temporal calendar weekday values use Sunday=0.
    return (_range(1, 5),)


def _market_session_calendars(definition: dict[str, Any]) -> Sequence[ScheduleCalendarSpec]:
    schedule = dict(definition.get("schedule") or {})
    payload = dict(definition.get("payload") or {})
    cadence = max(int(schedule.get("minutes", 1)), 1)
    offset_seconds = max(int(schedule.get("offset_seconds", 0)), 0)
    second = offset_seconds % 60
    phase = 0
    opening_minute = 30 + ((phase - 30) % cadence)
    calendars: list[ScheduleCalendarSpec] = []
    if opening_minute <= 59:
        calendars.append(
            ScheduleCalendarSpec(
                second=(_range(second),),
                minute=(_range(opening_minute, 59, cadence),),
                hour=(_range(9),),
                day_of_week=_weekday_range(),
                comment="market-session opening hour",
            )
        )
    calendars.append(
        ScheduleCalendarSpec(
            second=(_range(second),),
            minute=(_range(phase, 59, cadence),),
            hour=(_range(10, 15),),
            day_of_week=_weekday_range(),
            comment="market-session regular hours",
        )
    )
    grace_minutes = max(int(payload.get("market_close_grace_minutes", 0)), 0)
    if grace_minutes:
        calendars.append(
            ScheduleCalendarSpec(
                second=(_range(second),),
                minute=(_range(0, min(grace_minutes - 1, 59), cadence),),
                hour=(_range(16),),
                day_of_week=_weekday_range(),
                comment="market-session close grace",
            )
        )
    return calendars


def _market_boundary_calendars(definition: dict[str, Any], *, close: bool) -> Sequence[ScheduleCalendarSpec]:
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    schedule = dict(definition.get("schedule") or {})
    calendar_name = str(definition.get("market_calendar") or "NYSE")
    offset = timedelta(minutes=int(schedule.get("minutes", 0)))
    calendars: list[ScheduleCalendarSpec] = []
    for day_offset in range(0, 185):
        session = market_session_window(calendar_name, (now + timedelta(days=day_offset)).date())
        if session is None:
            continue
        market_open, market_close = session
        slot = (market_close if close else market_open) + offset
        local = slot.astimezone(market_open.tzinfo)
        calendars.append(
            ScheduleCalendarSpec(
                second=(_range(local.second),),
                minute=(_range(local.minute),),
                hour=(_range(local.hour),),
                day_of_month=(_range(local.day),),
                month=(_range(local.month),),
                year=(_range(local.year),),
                comment="market-close routine" if close else "market-open routine",
            )
        )
    return calendars


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
