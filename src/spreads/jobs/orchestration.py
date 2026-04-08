from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

from spreads.storage.records import JobDefinitionRecord

NEW_YORK = ZoneInfo("America/New_York")
SCHEDULER_RUNTIME_LEASE_KEY = "runtime:scheduler"
WORKER_RUNTIME_LEASE_PREFIX = "runtime:worker:"
SINGLETON_LEASE_PREFIX = "singleton:"


def utc_now() -> datetime:
    return datetime.now(UTC)


def singleton_lease_key(job_type: str, scope: str) -> str:
    return f"{SINGLETON_LEASE_PREFIX}{job_type}:{scope}"


def worker_runtime_lease_key(worker_name: str) -> str:
    return f"{WORKER_RUNTIME_LEASE_PREFIX}{worker_name}"


def _market_schedule(calendar_name: str, session_day: date) -> tuple[datetime, datetime] | None:
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(start_date=session_day.isoformat(), end_date=session_day.isoformat())
    if schedule.empty:
        return None
    session = schedule.iloc[0]
    market_open = session["market_open"].to_pydatetime().astimezone(NEW_YORK)
    market_close = session["market_close"].to_pydatetime().astimezone(NEW_YORK)
    return market_open, market_close


def floor_to_interval(now: datetime, minutes: int) -> datetime:
    minute = (now.minute // minutes) * minutes
    return now.replace(minute=minute, second=0, microsecond=0)


def resolve_scheduled_for(
    definition: JobDefinitionRecord,
    *,
    now: datetime | None = None,
) -> datetime | None:
    current = (now or utc_now()).astimezone(NEW_YORK)
    schedule = definition.schedule
    schedule_type = definition.schedule_type

    if schedule_type == "interval_minutes":
        minutes = max(int(schedule.get("minutes", 0)), 1)
        slot = floor_to_interval(current, minutes)
        if bool(definition.payload.get("allow_off_hours")):
            return slot.astimezone(UTC)
        market_window = _market_schedule(definition.market_calendar, current.date())
        if market_window is None:
            return None
        market_open, market_close = market_window
        if not (market_open <= current < market_close):
            return None
        if slot < market_open:
            slot = market_open.replace(second=0, microsecond=0)
        if slot >= market_close:
            return None
        return slot.astimezone(UTC)

    market_window = _market_schedule(definition.market_calendar, current.date())
    if market_window is None:
        return None
    market_open, market_close = market_window

    if schedule_type == "market_open_plus_minutes":
        target = market_open + timedelta(minutes=int(schedule.get("minutes", 0)))
    elif schedule_type == "market_close_plus_minutes":
        target = market_close + timedelta(minutes=int(schedule.get("minutes", 0)))
    else:
        raise ValueError(f"Unsupported schedule_type: {schedule_type}")

    if current < target:
        return None
    return target.astimezone(UTC)


def build_job_run_id(job_key: str, scheduled_for: datetime) -> str:
    slot = scheduled_for.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{job_key}:{slot}"


def due_job_payload(definition: JobDefinitionRecord, *, now: datetime | None = None) -> tuple[str, datetime, dict[str, object]] | None:
    scheduled_for = resolve_scheduled_for(definition, now=now)
    if scheduled_for is None:
        return None
    job_run_id = build_job_run_id(definition.job_key, scheduled_for)
    payload = dict(definition.payload)
    payload["job_key"] = definition.job_key
    payload["scheduled_for"] = scheduled_for.isoformat().replace("+00:00", "Z")
    payload["singleton_scope"] = definition.singleton_scope
    return job_run_id, scheduled_for, payload
