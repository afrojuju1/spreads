from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

from spreads.services.live_pipelines import build_live_session_id, resolve_live_collector_label
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


def isoformat_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _interval_market_cutoff(payload: dict[str, object], *, market_close: datetime) -> datetime:
    raw_grace_minutes = payload.get("post_close_grace_minutes", 0)
    try:
        grace_minutes = max(int(raw_grace_minutes), 0)
    except (TypeError, ValueError):
        grace_minutes = 0
    return market_close + timedelta(minutes=grace_minutes)


def resolve_scheduled_for(
    definition: JobDefinitionRecord,
    *,
    now: datetime | None = None,
) -> datetime | None:
    current = (now or utc_now()).astimezone(NEW_YORK)
    schedule = dict(definition.get("schedule") or {})
    schedule_type = str(definition["schedule_type"])

    if schedule_type == "interval_minutes":
        minutes = max(int(schedule.get("minutes", 0)), 1)
        slot = floor_to_interval(current, minutes)
        payload = dict(definition.get("payload") or {})
        if bool(payload.get("allow_off_hours")):
            return slot.astimezone(UTC)
        market_window = _market_schedule(str(definition.get("market_calendar") or "NYSE"), current.date())
        if market_window is None:
            return None
        market_open, market_close = market_window
        market_cutoff = _interval_market_cutoff(payload, market_close=market_close)
        if not (market_open <= current < market_cutoff):
            return None
        if slot < market_open:
            slot = market_open.replace(second=0, microsecond=0)
        if slot >= market_cutoff:
            return None
        return slot.astimezone(UTC)

    market_window = _market_schedule(str(definition.get("market_calendar") or "NYSE"), current.date())
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


def build_job_attempt_id(job_run_id: str, retry_count: int) -> str:
    if retry_count <= 0:
        return job_run_id
    return f"{job_run_id}:retry:{retry_count}"


def _session_slots(
    *,
    session_start: datetime,
    session_end: datetime,
    now: datetime,
    interval_seconds: int,
) -> list[datetime]:
    if now < session_start:
        return []
    cutoff = min(now, session_end)
    elapsed_seconds = int(max((cutoff - session_start).total_seconds(), 0))
    slot_count = (elapsed_seconds // max(interval_seconds, 1)) + 1
    return [session_start + timedelta(seconds=index * interval_seconds) for index in range(slot_count)]


def resolve_live_tick_plan(
    definition: JobDefinitionRecord,
    *,
    now: datetime | None = None,
) -> dict[str, object] | None:
    current = (now or utc_now()).astimezone(NEW_YORK)
    market_window = _market_schedule(str(definition.get("market_calendar") or "NYSE"), current.date())
    if market_window is None:
        return None
    market_open, market_close = market_window
    payload = dict(definition.get("payload") or {})
    interval_seconds = max(int(payload.get("interval_seconds", 300)), 1)
    session_start = market_open + timedelta(
        minutes=int(payload.get("session_start_offset_minutes", (definition.get("schedule") or {}).get("minutes", 0)))
    )
    session_end = market_close + timedelta(minutes=int(payload.get("session_end_offset_minutes", 0)))
    recovery_deadline = session_end + timedelta(seconds=interval_seconds)
    if current < session_start or current > recovery_deadline:
        return None
    label = resolve_live_collector_label(payload)
    session_id = build_live_session_id(label, session_start.date())
    slots = _session_slots(
        session_start=session_start,
        session_end=session_end,
        now=current,
        interval_seconds=interval_seconds,
    )
    current_slot = None if not slots else slots[-1]
    return {
        "label": label,
        "session_id": session_id,
        "session_date": session_start.date().isoformat(),
        "interval_seconds": interval_seconds,
        "session_start": session_start.astimezone(UTC),
        "session_end": session_end.astimezone(UTC),
        "slots": [slot.astimezone(UTC) for slot in slots],
        "current_slot": None if current_slot is None else current_slot.astimezone(UTC),
        "payload": payload,
    }


def due_job_payload(definition: JobDefinitionRecord, *, now: datetime | None = None) -> tuple[str, datetime, dict[str, object]] | None:
    scheduled_for = resolve_scheduled_for(definition, now=now)
    if scheduled_for is None:
        return None
    job_run_id = build_job_run_id(str(definition["job_key"]), scheduled_for)
    payload = dict(definition.get("payload") or {})
    payload["job_key"] = str(definition["job_key"])
    payload["scheduled_for"] = scheduled_for.isoformat().replace("+00:00", "Z")
    payload["singleton_scope"] = definition.get("singleton_scope")
    return job_run_id, scheduled_for, payload
