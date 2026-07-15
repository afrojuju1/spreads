from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from core.services.market_dates import NEW_YORK, market_session_window
from core.storage.records import RecordMapping

CAPTURE_SESSION_RUNTIME_LEASE_PREFIX = "runtime:capture_session"


def utc_now() -> datetime:
    return datetime.now(UTC)


def capture_session_runtime_lease_key(scope: str | None = None) -> str:
    normalized = str(scope or "").strip().lower()
    if not normalized:
        return CAPTURE_SESSION_RUNTIME_LEASE_PREFIX
    return f"{CAPTURE_SESSION_RUNTIME_LEASE_PREFIX}:{normalized}"


def _market_schedule(calendar_name: str, session_day: date) -> tuple[datetime, datetime] | None:
    return market_session_window(calendar_name, session_day)


def floor_to_interval(now: datetime, minutes: int) -> datetime:
    interval_seconds = max(minutes, 1) * 60
    slot_timestamp = int(now.timestamp()) // interval_seconds * interval_seconds
    return datetime.fromtimestamp(slot_timestamp, tz=now.tzinfo)


def interval_offset_seconds(schedule: dict[str, object], *, minutes: int) -> int:
    raw_offset = schedule.get("offset_seconds", 0)
    try:
        offset_seconds = int(raw_offset)
    except (TypeError, ValueError):
        offset_seconds = 0
    interval_seconds = max(minutes, 1) * 60
    return min(max(offset_seconds, 0), max(interval_seconds - 1, 0))


def _interval_market_cutoff(payload: dict[str, object], *, market_close: datetime) -> datetime:
    raw_grace_minutes = payload.get("market_close_grace_minutes", 0)
    try:
        grace_minutes = max(int(raw_grace_minutes), 0)
    except (TypeError, ValueError):
        grace_minutes = 0
    return market_close + timedelta(minutes=grace_minutes)


def market_boundary_slot(
    definition: RecordMapping,
    session_day: date,
    *,
    close: bool,
    market_window: tuple[datetime, datetime] | None = None,
) -> datetime | None:
    resolved_window = market_window or _market_schedule(
        str(definition.get("market_calendar") or "NYSE"),
        session_day,
    )
    if resolved_window is None:
        return None
    market_open, market_close = resolved_window
    schedule = dict(definition.get("schedule") or {})
    return (market_close if close else market_open) + timedelta(minutes=int(schedule.get("minutes", 0)))


def market_session_slots(
    definition: RecordMapping,
    session_day: date,
    *,
    market_window: tuple[datetime, datetime] | None = None,
) -> tuple[datetime, ...]:
    resolved_window = market_window or _market_schedule(
        str(definition.get("market_calendar") or "NYSE"),
        session_day,
    )
    if resolved_window is None:
        return ()
    market_open, market_close = resolved_window
    schedule = dict(definition.get("schedule") or {})
    minutes = max(int(schedule.get("minutes", 1)), 1)
    offset = interval_offset_seconds(schedule, minutes=minutes)
    cursor = floor_to_interval(market_open, minutes) + timedelta(seconds=offset)
    if cursor < market_open:
        cursor += timedelta(minutes=minutes)
    payload = dict(definition.get("payload") or {})
    cutoff = _interval_market_cutoff(payload, market_close=market_close)
    slots: list[datetime] = []
    while cursor < cutoff:
        slots.append(cursor)
        cursor += timedelta(minutes=minutes)
    return tuple(slots)


def resolve_scheduled_for(
    definition: RecordMapping,
    *,
    now: datetime | None = None,
) -> datetime | None:
    current = (now or utc_now()).astimezone(NEW_YORK)
    schedule = dict(definition.get("schedule") or {})
    schedule_type = str(definition["schedule_type"])

    if schedule_type == "interval":
        minutes = max(int(schedule.get("minutes", 0)), 1)
        slot = floor_to_interval(current, minutes) + timedelta(seconds=interval_offset_seconds(schedule, minutes=minutes))
        if current < slot:
            return None
        return slot.astimezone(UTC)

    if schedule_type == "market_session":
        market_window = _market_schedule(str(definition.get("market_calendar") or "NYSE"), current.date())
        if market_window is None:
            return None
        market_open, market_close = market_window
        payload = dict(definition.get("payload") or {})
        market_cutoff = _interval_market_cutoff(payload, market_close=market_close)
        if not (market_open <= current < market_cutoff):
            return None
        previous = max(
            (slot for slot in market_session_slots(definition, current.date(), market_window=market_window) if slot <= current),
            default=None,
        )
        return None if previous is None else previous.astimezone(UTC)

    if schedule_type == "manual":
        return None

    if schedule_type == "calendar":
        days_of_week = {int(value) for value in schedule.get("days_of_week", range(7))}
        if current.weekday() not in days_of_week:
            return None
        target = current.replace(
            hour=max(min(int(schedule.get("hour", 0)), 23), 0),
            minute=max(min(int(schedule.get("minute", 0)), 59), 0),
            second=max(min(int(schedule.get("second", 0)), 59), 0),
            microsecond=0,
        )
        return None if current < target else target.astimezone(UTC)

    if schedule_type == "market_open":
        target = market_boundary_slot(definition, current.date(), close=False)
    elif schedule_type == "market_close":
        target = market_boundary_slot(definition, current.date(), close=True)
    else:
        raise ValueError(f"Unsupported schedule_type: {schedule_type}")

    if target is None:
        return None
    if current < target:
        return None
    return target.astimezone(UTC)


def expected_routine_slots(
    definition: RecordMapping,
    *,
    now: datetime | None = None,
) -> dict[str, datetime | None]:
    """Return the previous and next expected slots from the domain schedule."""
    current = (now or utc_now()).astimezone(NEW_YORK)
    schedule = dict(definition.get("schedule") or {})
    schedule_type = str(definition.get("schedule_type") or "manual")
    calendar_name = str(definition.get("market_calendar") or "NYSE")

    if schedule_type == "manual":
        return {"previous": None, "next": None}

    if schedule_type == "interval":
        minutes = max(int(schedule.get("minutes", 1)), 1)
        offset = interval_offset_seconds(schedule, minutes=minutes)
        previous = floor_to_interval(current, minutes) + timedelta(seconds=offset)
        if previous > current:
            previous -= timedelta(minutes=minutes)
        return {
            "previous": previous.astimezone(UTC),
            "next": (previous + timedelta(minutes=minutes)).astimezone(UTC),
        }

    if schedule_type == "calendar":
        days_of_week = {int(value) for value in schedule.get("days_of_week", range(7))}
        slots: list[datetime] = []
        for day_offset in range(-7, 8):
            day = current.date() + timedelta(days=day_offset)
            if day.weekday() not in days_of_week:
                continue
            slots.append(
                datetime(
                    day.year,
                    day.month,
                    day.day,
                    max(min(int(schedule.get("hour", 0)), 23), 0),
                    max(min(int(schedule.get("minute", 0)), 59), 0),
                    max(min(int(schedule.get("second", 0)), 59), 0),
                    tzinfo=NEW_YORK,
                )
            )
        previous = max((slot for slot in slots if slot <= current), default=None)
        next_slot = min((slot for slot in slots if slot > current), default=None)
        return {
            "previous": None if previous is None else previous.astimezone(UTC),
            "next": None if next_slot is None else next_slot.astimezone(UTC),
        }

    slots: list[datetime] = []
    for day_offset in range(-7, 9):
        session_day = current.date() + timedelta(days=day_offset)
        market_window = _market_schedule(calendar_name, session_day)
        if market_window is None:
            continue
        if schedule_type == "market_open":
            slot = market_boundary_slot(definition, session_day, close=False, market_window=market_window)
            if slot is not None:
                slots.append(slot)
            continue
        if schedule_type == "market_close":
            slot = market_boundary_slot(definition, session_day, close=True, market_window=market_window)
            if slot is not None:
                slots.append(slot)
            continue
        if schedule_type != "market_session":
            raise ValueError(f"Unsupported schedule_type: {schedule_type}")
        slots.extend(market_session_slots(definition, session_day, market_window=market_window))
    previous = max((slot for slot in slots if slot <= current), default=None)
    next_slot = min((slot for slot in slots if slot > current), default=None)
    return {
        "previous": None if previous is None else previous.astimezone(UTC),
        "next": None if next_slot is None else next_slot.astimezone(UTC),
    }


def build_job_run_id(job_key: str, scheduled_for: datetime) -> str:
    slot = scheduled_for.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{job_key}:{slot}"
