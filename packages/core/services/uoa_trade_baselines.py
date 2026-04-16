from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from core.storage.run_history_repository import RunHistoryRepository, session_bounds
from core.storage.serializers import parse_datetime

NEW_YORK = ZoneInfo("America/New_York")
OPTION_SESSION_OPEN = time(9, 30)


def build_uoa_trade_baselines(
    *,
    history_store: RunHistoryRepository,
    label: str,
    session_date: str,
    as_of: str,
    underlyings: list[str],
    rolling_window_minutes: int = 5,
) -> dict[str, dict[str, Any]]:
    unique_underlyings = sorted({str(symbol or "").strip() for symbol in underlyings if str(symbol or "").strip()})
    if not unique_underlyings:
        return {}
    as_of_dt = parse_datetime(as_of)
    if as_of_dt is None:
        return {symbol: {} for symbol in unique_underlyings}

    _, session_end = session_bounds(session_date)
    session_start = datetime.combine(as_of_dt.astimezone(NEW_YORK).date(), OPTION_SESSION_OPEN, tzinfo=NEW_YORK).astimezone(as_of_dt.tzinfo)
    rolling_start = max(session_start, as_of_dt - timedelta(minutes=max(int(rolling_window_minutes), 1)))
    baselines: dict[str, dict[str, Any]] = {symbol: {} for symbol in unique_underlyings}

    rolling_rows = history_store.summarize_scoreable_trade_flow(
        label=label,
        underlyings=unique_underlyings,
        captured_from=rolling_start,
        captured_to=as_of_dt,
    )
    for symbol, payload in rolling_rows.items():
        baselines.setdefault(symbol, {})["rolling_5m"] = {
            "session_date": session_date,
            **dict(payload),
        }

    if as_of_dt > session_start:
        session_rows = history_store.summarize_scoreable_trade_flow(
            label=label,
            underlyings=unique_underlyings,
            captured_from=session_start,
            captured_to=as_of_dt,
        )
        for symbol, payload in session_rows.items():
            baselines.setdefault(symbol, {})["session_to_time"] = {
                "session_date": session_date,
                **dict(payload),
            }

    previous_session_date = history_store.latest_trade_session_date_before(
        label=label,
        before_session_date=session_date,
    )
    if previous_session_date:
        _, previous_session_end = session_bounds(previous_session_date)
        previous_session_start = datetime.combine(
            datetime.fromisoformat(previous_session_date).date(),
            OPTION_SESSION_OPEN,
            tzinfo=NEW_YORK,
        ).astimezone(as_of_dt.tzinfo)
        elapsed = max((as_of_dt - session_start).total_seconds(), 60.0)
        previous_same_time_end = min(previous_session_start + timedelta(seconds=elapsed), previous_session_end)
        if previous_same_time_end > previous_session_start:
            previous_rows = history_store.summarize_scoreable_trade_flow(
                label=label,
                underlyings=unique_underlyings,
                captured_from=previous_session_start,
                captured_to=previous_same_time_end,
            )
            for symbol, payload in previous_rows.items():
                baselines.setdefault(symbol, {})["previous_session_same_time"] = {
                    "session_date": previous_session_date,
                    **dict(payload),
                }

    return baselines
