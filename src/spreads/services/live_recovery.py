from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from spreads.jobs.orchestration import NEW_YORK, _market_schedule
from spreads.services.live_collector_health import TRADEABILITY_STATE_RECOVERY_ONLY
from spreads.services.positions import enrich_position_row
from spreads.services.control_plane import get_control_state_snapshot, set_control_mode
from spreads.services.option_quote_records import build_quote_symbol_metadata
from spreads.services.option_trade_records import build_trade_symbol_metadata
from spreads.storage.serializers import parse_date, parse_datetime

LIVE_SLOT_STATUS_EXPECTED = "expected"
LIVE_SLOT_STATUS_QUEUED = "queued"
LIVE_SLOT_STATUS_RUNNING = "running"
LIVE_SLOT_STATUS_SUCCEEDED = "succeeded"
LIVE_SLOT_STATUS_MISSED = "missed"
LIVE_SLOT_STATUS_RECOVERED = "recovered_analysis_only"
LIVE_SLOT_STATUS_UNRECOVERABLE = "unrecoverable"
LIVE_SLOT_TERMINAL_STATUSES = {
    LIVE_SLOT_STATUS_SUCCEEDED,
    LIVE_SLOT_STATUS_RECOVERED,
    LIVE_SLOT_STATUS_UNRECOVERABLE,
}

RECOVERY_STATE_CLEAR = "clear"
RECOVERY_STATE_RECONCILING = "reconciling"
RECOVERY_STATE_RECOVERING = "recovering"
RECOVERY_STATE_BLOCKED_WAITING_FRESH_SLOT = "blocked_waiting_fresh_slot"

CAPTURE_TARGET_REASON_PROMOTABLE = "promotable"
CAPTURE_TARGET_REASON_MONITOR = "monitor"
CAPTURE_TARGET_REASON_PENDING_EXECUTION = "pending_execution"
CAPTURE_TARGET_REASON_OPEN_POSITION = "open_position"

CAPTURE_OWNER_LIVE_SESSION = "live_session"
CAPTURE_OWNER_EXECUTION_ATTEMPT = "execution_attempt"
CAPTURE_OWNER_SESSION_POSITION = "session_position"
CAPTURE_OWNER_RECOVERY_SESSION = "recovery_session"

RECOVERY_CONTROL_REASON_CODE = "collector_gap_active"
RECOVERY_CONTROL_CLEAR_REASON_CODE = "collector_gap_cleared"

RECOVERY_TRACKED_REASONS = {
    CAPTURE_TARGET_REASON_PROMOTABLE,
    CAPTURE_TARGET_REASON_MONITOR,
    CAPTURE_TARGET_REASON_PENDING_EXECUTION,
    CAPTURE_TARGET_REASON_OPEN_POSITION,
}
OPEN_POSITION_CAPTURE_STATUSES = [
    "pending_open",
    "partial_open",
    "open",
    "partial_close",
]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _future_utc(minutes: int) -> str:
    return (
        (datetime.now(UTC) + timedelta(minutes=max(int(minutes), 1)))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _option_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("underlying_symbol") or ""),
        str(row.get("strategy") or ""),
        str(row.get("option_symbol") or ""),
    )


def _slot_timestamp(row: Mapping[str, Any]) -> datetime | None:
    return parse_datetime(_as_text(row.get("slot_at")))


def _slot_details(row: Mapping[str, Any]) -> dict[str, Any]:
    details = row.get("slot_details")
    if isinstance(details, Mapping):
        return dict(details)
    raw = row.get("slot_details_json")
    if isinstance(raw, Mapping):
        return dict(raw)
    return {}


def _fresh_slot(row: Mapping[str, Any]) -> bool:
    if str(row.get("status") or "") != LIVE_SLOT_STATUS_SUCCEEDED:
        return False
    capture_status = str(row.get("capture_status") or "").strip().lower()
    return capture_status == "healthy"


def _resume_eligible_slot(row: Mapping[str, Any]) -> bool:
    if str(row.get("status") or "") != LIVE_SLOT_STATUS_SUCCEEDED:
        return False
    capture_status = str(row.get("capture_status") or "").strip().lower()
    return capture_status in {"healthy", "baseline_only"}


def resolve_live_slot_stale_after_seconds(interval_seconds: int | None) -> int:
    normalized_interval = max(int(interval_seconds or 0), 1)
    return max(normalized_interval, 90)


def _session_expiry_at(
    *,
    session_date: str | None,
    session_end_offset_minutes: int = 0,
    grace_minutes: int = 60,
) -> str:
    resolved_date = parse_date(
        session_date or datetime.now(NEW_YORK).date().isoformat()
    )
    market_window = _market_schedule("NYSE", resolved_date)
    if market_window is None:
        expiry = datetime.now(NEW_YORK) + timedelta(hours=12)
        return (
            expiry.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        )
    _, market_close = market_window
    expiry = market_close + timedelta(
        minutes=max(int(session_end_offset_minutes), 0) + max(int(grace_minutes), 0)
    )
    return expiry.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def summarize_session_slot_health(
    slot_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ordered_rows = sorted(
        [dict(row) for row in slot_rows],
        key=lambda row: _slot_timestamp(row) or datetime.fromtimestamp(0, UTC),
    )
    latest_slot_at = None
    latest_slot_status = None
    latest_gap_slot_at = None
    latest_fresh_slot_at = None
    latest_resume_slot_at = None
    missed_slot_count = 0
    unrecoverable_slot_count = 0
    recovered_slot_count = 0
    for row in ordered_rows:
        slot_at = _as_text(row.get("slot_at"))
        if slot_at is not None:
            latest_slot_at = slot_at
        latest_slot_status = str(row.get("status") or latest_slot_status or "")
        status = str(row.get("status") or "")
        if status == LIVE_SLOT_STATUS_MISSED:
            missed_slot_count += 1
            latest_gap_slot_at = slot_at or latest_gap_slot_at
        elif status == LIVE_SLOT_STATUS_UNRECOVERABLE:
            unrecoverable_slot_count += 1
            latest_gap_slot_at = slot_at or latest_gap_slot_at
        elif status == LIVE_SLOT_STATUS_RECOVERED:
            recovered_slot_count += 1
            latest_gap_slot_at = slot_at or latest_gap_slot_at
        if _fresh_slot(row):
            latest_fresh_slot_at = slot_at or latest_fresh_slot_at
        if _resume_eligible_slot(row):
            latest_resume_slot_at = slot_at or latest_resume_slot_at

    unresolved_gap_active = missed_slot_count > 0
    if unresolved_gap_active:
        recovery_state = RECOVERY_STATE_RECOVERING
    else:
        recovery_state = RECOVERY_STATE_CLEAR

    gap_active = recovery_state != RECOVERY_STATE_CLEAR
    return {
        "gap_active": gap_active,
        "recovery_state": recovery_state,
        "missed_slot_count": missed_slot_count,
        "recovered_slot_count": recovered_slot_count,
        "unrecoverable_slot_count": unrecoverable_slot_count,
        "latest_slot_at": latest_slot_at,
        "latest_slot_status": latest_slot_status,
        "latest_gap_slot_at": latest_gap_slot_at,
        "latest_fresh_slot_at": latest_fresh_slot_at,
        "latest_resume_slot_at": latest_resume_slot_at,
    }


def list_session_slot_health_by_session_id(
    *,
    recovery_store: Any,
    session_ids: list[str],
    session_date: str | None = None,
) -> dict[str, dict[str, Any]]:
    if not session_ids or not recovery_store.schema_ready():
        return {}
    rows = recovery_store.list_live_session_slots(
        session_ids=session_ids,
        session_date=session_date,
        limit=max(len(session_ids) * 500, 1000),
        ascending=True,
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        session_id = _as_text(row.get("session_id"))
        if session_id is None:
            continue
        grouped[session_id].append(dict(row))
    return {
        session_id: summarize_session_slot_health(slot_rows)
        for session_id, slot_rows in grouped.items()
    }


def load_session_slot_health(
    *,
    recovery_store: Any,
    session_id: str,
) -> dict[str, Any]:
    if not recovery_store.schema_ready():
        return {
            "gap_active": False,
            "recovery_state": RECOVERY_STATE_CLEAR,
            "missed_slot_count": 0,
            "recovered_slot_count": 0,
            "unrecoverable_slot_count": 0,
            "latest_slot_at": None,
            "latest_slot_status": None,
            "latest_gap_slot_at": None,
            "latest_fresh_slot_at": None,
            "latest_resume_slot_at": None,
        }
    rows = recovery_store.list_live_session_slots(
        session_id=session_id,
        limit=500,
        ascending=True,
    )
    return summarize_session_slot_health(rows)


def merge_live_action_gate_with_recovery(
    *,
    base_gate: Mapping[str, Any] | None,
    slot_health: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    gate = {} if not isinstance(base_gate, Mapping) else dict(base_gate)
    if not isinstance(slot_health, Mapping):
        return None if not gate else gate
    recovery_state = str(slot_health.get("recovery_state") or RECOVERY_STATE_CLEAR)
    if recovery_state == RECOVERY_STATE_CLEAR:
        return None if not gate else gate
    return {
        "status": "blocked",
        "reason_code": RECOVERY_CONTROL_REASON_CODE,
        "message": (
            "Live actions are blocked while collector gaps are being reconciled."
            if recovery_state == RECOVERY_STATE_RECOVERING
            else "Live actions stay blocked until one successful post-gap live slot completes."
        ),
        "allow_alerts": False,
        "allow_auto_execution": False,
        "tradeability_state": TRADEABILITY_STATE_RECOVERY_ONLY,
        "recovery_state": recovery_state,
        "gap_active": bool(slot_health.get("gap_active")),
        "missed_slot_count": int(slot_health.get("missed_slot_count") or 0),
        "unrecoverable_slot_count": int(
            slot_health.get("unrecoverable_slot_count") or 0
        ),
    }


def _merge_capture_metadata(
    quote_metadata: Mapping[str, Mapping[str, Any]],
    trade_metadata: Mapping[str, Mapping[str, Any]],
    *,
    feed: str,
    data_base_url: str | None,
    expires_at: str,
) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for option_symbol, metadata in quote_metadata.items():
        rows[str(option_symbol)] = {
            "option_symbol": str(option_symbol),
            "underlying_symbol": metadata.get("underlying_symbol"),
            "strategy": metadata.get("strategy"),
            "leg_role": metadata.get("leg_role"),
            "quote_enabled": True,
            "trade_enabled": False,
            "feed": feed,
            "data_base_url": data_base_url,
            "expires_at": expires_at,
            "metadata": dict(metadata),
        }
    for option_symbol, metadata in trade_metadata.items():
        row = rows.setdefault(
            str(option_symbol),
            {
                "option_symbol": str(option_symbol),
                "underlying_symbol": metadata.get("underlying_symbol"),
                "strategy": metadata.get("strategy"),
                "leg_role": metadata.get("leg_role"),
                "quote_enabled": False,
                "trade_enabled": False,
                "feed": feed,
                "data_base_url": data_base_url,
                "expires_at": expires_at,
                "metadata": dict(metadata),
            },
        )
        row["trade_enabled"] = True
        if not row.get("metadata"):
            row["metadata"] = dict(metadata)
    return sorted(rows.values(), key=_option_sort_key)


def build_capture_target_rows_for_candidates(
    *,
    candidates: list[dict[str, Any]],
    feed: str,
    data_base_url: str | None,
    expires_at: str,
) -> list[dict[str, Any]]:
    return _merge_capture_metadata(
        build_quote_symbol_metadata(candidates),
        build_trade_symbol_metadata(candidates),
        feed=feed,
        data_base_url=data_base_url,
        expires_at=expires_at,
    )


def build_slot_details_from_cycle_result(result: Mapping[str, Any]) -> dict[str, Any]:
    capture_targets = (
        result.get("capture_targets")
        if isinstance(result.get("capture_targets"), Mapping)
        else {}
    )
    return {
        "expected_quote_symbols": list(result.get("expected_quote_symbols") or []),
        "expected_trade_symbols": list(result.get("expected_trade_symbols") or []),
        "capture_targets": {
            str(reason): [dict(row) for row in rows if isinstance(row, Mapping)]
            for reason, rows in capture_targets.items()
            if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes))
        },
        "quote_capture": dict(result.get("quote_capture") or {}),
        "trade_capture": dict(result.get("trade_capture") or {}),
    }


def refresh_live_session_capture_targets(
    *,
    recovery_store: Any,
    session_id: str,
    session_date: str,
    label: str,
    profile: str,
    promotable_candidates: list[dict[str, Any]],
    monitor_candidates: list[dict[str, Any]],
    capture_candidates: list[dict[str, Any]] | None = None,
    feed: str = "opra",
    data_base_url: str | None = None,
    session_end_offset_minutes: int = 0,
) -> dict[str, Any]:
    if not recovery_store.schema_ready():
        return {"status": "skipped", "reason": "recovery_schema_unavailable"}
    expiry = _session_expiry_at(
        session_date=session_date,
        session_end_offset_minutes=session_end_offset_minutes,
    )
    promotable_rows = build_capture_target_rows_for_candidates(
        candidates=promotable_candidates,
        feed=feed,
        data_base_url=data_base_url,
        expires_at=expiry,
    )
    monitor_source_candidates = (
        monitor_candidates if capture_candidates is None else capture_candidates
    )
    monitor_rows = build_capture_target_rows_for_candidates(
        candidates=monitor_source_candidates,
        feed=feed,
        data_base_url=data_base_url,
        expires_at=expiry,
    )
    recovery_store.replace_capture_targets(
        owner_kind=CAPTURE_OWNER_LIVE_SESSION,
        owner_key=session_id,
        reason=CAPTURE_TARGET_REASON_PROMOTABLE,
        session_id=session_id,
        session_date=session_date,
        label=label,
        profile=profile,
        rows=promotable_rows,
    )
    recovery_store.replace_capture_targets(
        owner_kind=CAPTURE_OWNER_LIVE_SESSION,
        owner_key=session_id,
        reason=CAPTURE_TARGET_REASON_MONITOR,
        session_id=session_id,
        session_date=session_date,
        label=label,
        profile=profile,
        rows=monitor_rows,
    )
    return {
        "status": "ok",
        "promotable_target_count": len(promotable_rows),
        "monitor_target_count": len(monitor_rows),
        "capture_targets": {
            CAPTURE_TARGET_REASON_PROMOTABLE: promotable_rows,
            CAPTURE_TARGET_REASON_MONITOR: monitor_rows,
        },
    }


def _execution_attempt_capture_rows(
    attempt: Mapping[str, Any], *, expires_at: str
) -> list[dict[str, Any]]:
    rows = []
    for leg_role, option_symbol in (
        ("short", attempt.get("short_symbol")),
        ("long", attempt.get("long_symbol")),
    ):
        rendered_symbol = _as_text(option_symbol)
        if rendered_symbol is None:
            continue
        rows.append(
            {
                "option_symbol": rendered_symbol,
                "underlying_symbol": _as_text(attempt.get("underlying_symbol")),
                "strategy": _as_text(attempt.get("strategy")),
                "leg_role": leg_role,
                "quote_enabled": True,
                "trade_enabled": True,
                "feed": "opra",
                "data_base_url": None,
                "expires_at": expires_at,
                "metadata": {
                    "label": attempt.get("label"),
                    "trade_intent": attempt.get("trade_intent"),
                },
            }
        )
    return rows


def _session_position_capture_rows(
    position: Mapping[str, Any], *, expires_at: str
) -> list[dict[str, Any]]:
    rows = []
    for leg_role, option_symbol in (
        ("short", position.get("short_symbol")),
        ("long", position.get("long_symbol")),
    ):
        rendered_symbol = _as_text(option_symbol)
        if rendered_symbol is None:
            continue
        rows.append(
            {
                "option_symbol": rendered_symbol,
                "underlying_symbol": _as_text(position.get("underlying_symbol")),
                "strategy": _as_text(position.get("strategy")),
                "leg_role": leg_role,
                "quote_enabled": True,
                "trade_enabled": True,
                "feed": "opra",
                "data_base_url": None,
                "expires_at": expires_at,
                "metadata": {
                    "label": position.get("label"),
                    "position_status": position.get("status"),
                },
            }
        )
    return rows


def refresh_execution_capture_targets(
    *,
    storage: Any,
) -> dict[str, Any]:
    recovery_store = storage.recovery
    execution_store = storage.execution
    if not recovery_store.schema_ready() or not execution_store.schema_ready():
        return {"status": "skipped", "reason": "schema_unavailable"}

    from spreads.services.execution import OPEN_STATUSES

    attempt_rows = [
        dict(row)
        for row in execution_store.list_attempts_by_status(
            statuses=sorted(OPEN_STATUSES),
            limit=500,
        )
    ]
    rolling_expiry = _future_utc(15)
    attempt_owner_keys: list[str] = []
    for attempt in attempt_rows:
        owner_key = str(attempt["execution_attempt_id"])
        attempt_owner_keys.append(owner_key)
        recovery_store.replace_capture_targets(
            owner_kind=CAPTURE_OWNER_EXECUTION_ATTEMPT,
            owner_key=owner_key,
            reason=CAPTURE_TARGET_REASON_PENDING_EXECUTION,
            session_id=_as_text(attempt.get("session_id")),
            session_date=_as_text(attempt.get("session_date")),
            label=_as_text(attempt.get("label")),
            rows=_execution_attempt_capture_rows(
                attempt,
                expires_at=rolling_expiry,
            ),
        )
    recovery_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_EXECUTION_ATTEMPT,
        active_owner_keys=attempt_owner_keys,
        reason=CAPTURE_TARGET_REASON_PENDING_EXECUTION,
    )

    if not execution_store.portfolio_schema_ready():
        return {
            "status": "ok",
            "pending_execution_target_count": sum(
                len(_execution_attempt_capture_rows(row, expires_at=rolling_expiry))
                for row in attempt_rows
            ),
            "open_position_target_count": 0,
        }

    position_rows = [
        enrich_position_row(dict(row))
        for row in execution_store.list_positions(
            statuses=OPEN_POSITION_CAPTURE_STATUSES,
            limit=500,
        )
    ]
    position_owner_keys: list[str] = []
    for position in position_rows:
        owner_key = str(position["position_id"])
        position_owner_keys.append(owner_key)
        recovery_store.replace_capture_targets(
            owner_kind=CAPTURE_OWNER_SESSION_POSITION,
            owner_key=owner_key,
            reason=CAPTURE_TARGET_REASON_OPEN_POSITION,
            session_id=_as_text(position.get("session_id")),
            session_date=_as_text(position.get("session_date")),
            label=_as_text(position.get("label")),
            rows=_session_position_capture_rows(
                position,
                expires_at=rolling_expiry,
            ),
        )
    recovery_store.delete_capture_targets_for_absent_owners(
        owner_kind=CAPTURE_OWNER_SESSION_POSITION,
        active_owner_keys=position_owner_keys,
        reason=CAPTURE_TARGET_REASON_OPEN_POSITION,
    )

    return {
        "status": "ok",
        "pending_execution_target_count": sum(
            len(_execution_attempt_capture_rows(row, expires_at=rolling_expiry))
            for row in attempt_rows
        ),
        "open_position_target_count": sum(
            len(_session_position_capture_rows(row, expires_at=rolling_expiry))
            for row in position_rows
        ),
    }


def refresh_recovery_session_capture_targets(
    *,
    storage: Any,
    session_id: str,
    slot_rows: Sequence[Mapping[str, Any]],
    slot_health: Mapping[str, Any],
) -> dict[str, Any]:
    recovery_store = storage.recovery
    if not recovery_store.schema_ready():
        return {"status": "skipped", "reason": "recovery_schema_unavailable"}
    if (
        str(slot_health.get("recovery_state") or RECOVERY_STATE_CLEAR)
        == RECOVERY_STATE_CLEAR
    ):
        recovery_store.delete_capture_targets(
            owner_kind=CAPTURE_OWNER_RECOVERY_SESSION,
            owner_key=session_id,
        )
        return {"status": "cleared", "target_count": 0}

    latest_success = None
    for row in sorted(
        slot_rows,
        key=lambda item: _slot_timestamp(item) or datetime.fromtimestamp(0, UTC),
        reverse=True,
    ):
        if str(row.get("status") or "") == LIVE_SLOT_STATUS_SUCCEEDED:
            latest_success = dict(row)
            break
    if latest_success is None:
        recovery_store.delete_capture_targets(
            owner_kind=CAPTURE_OWNER_RECOVERY_SESSION,
            owner_key=session_id,
        )
        return {"status": "cleared", "target_count": 0}

    details = _slot_details(latest_success)
    capture_targets = (
        details.get("capture_targets")
        if isinstance(details.get("capture_targets"), Mapping)
        else {}
    )
    total_target_count = 0
    for reason in (CAPTURE_TARGET_REASON_PROMOTABLE, CAPTURE_TARGET_REASON_MONITOR):
        reason_rows = [
            dict(row)
            for row in list(capture_targets.get(reason) or [])
            if isinstance(row, Mapping)
        ]
        recovery_store.replace_capture_targets(
            owner_kind=CAPTURE_OWNER_RECOVERY_SESSION,
            owner_key=session_id,
            reason=reason,
            session_id=session_id,
            session_date=_as_text(latest_success.get("session_date")),
            label=_as_text(latest_success.get("label")),
            profile=None,
            rows=reason_rows,
        )
        total_target_count += len(reason_rows)
    return {"status": "ok", "target_count": total_target_count}


def _coverage_summary_for_slot(
    *,
    history_store: Any,
    continuity_rows: list[dict[str, Any]],
    slot_at: str | datetime,
    interval_seconds: int,
) -> dict[str, Any]:
    slot_at_dt = parse_datetime(slot_at)
    if slot_at_dt is None:
        return {
            "coverage_sufficient": False,
            "quote_target_symbols": [],
            "trade_target_symbols": [],
            "covered_quote_symbols": [],
            "covered_trade_symbols": [],
            "missing_quote_symbols": [],
            "missing_trade_symbols": [],
        }
    captured_to = slot_at_dt + timedelta(seconds=max(int(interval_seconds), 1))
    quote_target_symbols = sorted(
        {
            str(row["option_symbol"])
            for row in continuity_rows
            if bool(row.get("quote_enabled")) and _as_text(row.get("option_symbol"))
        }
    )
    trade_target_symbols = sorted(
        {
            str(row["option_symbol"])
            for row in continuity_rows
            if bool(row.get("trade_enabled")) and _as_text(row.get("option_symbol"))
        }
    )
    quote_summary = history_store.summarize_option_quote_window(
        option_symbols=quote_target_symbols,
        captured_from=slot_at_dt,
        captured_to=captured_to,
    )
    trade_summary = history_store.summarize_option_trade_window(
        option_symbols=trade_target_symbols,
        captured_from=slot_at_dt,
        captured_to=captured_to,
    )
    covered_quote_symbols = sorted(quote_summary.keys())
    covered_trade_symbols = sorted(trade_summary.keys())
    missing_quote_symbols = sorted(
        symbol for symbol in quote_target_symbols if symbol not in quote_summary
    )
    missing_trade_symbols = sorted(
        symbol for symbol in trade_target_symbols if symbol not in trade_summary
    )
    coverage_sufficient = bool(quote_target_symbols) and not missing_quote_symbols
    return {
        "coverage_sufficient": coverage_sufficient,
        "quote_target_symbols": quote_target_symbols,
        "trade_target_symbols": trade_target_symbols,
        "covered_quote_symbols": covered_quote_symbols,
        "covered_trade_symbols": covered_trade_symbols,
        "missing_quote_symbols": missing_quote_symbols,
        "missing_trade_symbols": missing_trade_symbols,
        "quote_summary": quote_summary,
        "trade_summary": trade_summary,
    }


def _latest_success_capture_targets_before_slot(
    *,
    slot_rows: Sequence[Mapping[str, Any]],
    slot_at: str | datetime,
) -> list[dict[str, Any]]:
    slot_at_dt = parse_datetime(slot_at)
    if slot_at_dt is None:
        return []
    for row in sorted(
        [dict(item) for item in slot_rows],
        key=lambda item: _slot_timestamp(item) or datetime.fromtimestamp(0, UTC),
        reverse=True,
    ):
        row_slot_at = _slot_timestamp(row)
        if row_slot_at is None or row_slot_at >= slot_at_dt:
            continue
        if str(row.get("status") or "") != LIVE_SLOT_STATUS_SUCCEEDED:
            continue
        capture_targets = _slot_details(row).get("capture_targets")
        if not isinstance(capture_targets, Mapping):
            return []
        rows: list[dict[str, Any]] = []
        for reason in (CAPTURE_TARGET_REASON_PROMOTABLE, CAPTURE_TARGET_REASON_MONITOR):
            rows.extend(
                dict(target)
                for target in list(capture_targets.get(reason) or [])
                if isinstance(target, Mapping)
            )
        return rows
    return []


def _continuity_rows_for_slot(
    *,
    recovery_store: Any,
    session_id: str,
    slot_rows: Sequence[Mapping[str, Any]],
    slot_at: str | datetime,
) -> list[dict[str, Any]]:
    rows_by_symbol: dict[str, dict[str, Any]] = {}
    for row in _latest_success_capture_targets_before_slot(
        slot_rows=slot_rows,
        slot_at=slot_at,
    ):
        option_symbol = _as_text(row.get("option_symbol"))
        if option_symbol is None:
            continue
        rows_by_symbol[option_symbol] = {
            **dict(row),
            "quote_enabled": bool(row.get("quote_enabled", True)),
            "trade_enabled": bool(row.get("trade_enabled", False)),
        }
    for row in recovery_store.list_capture_targets(
        session_id=session_id,
        reasons=[
            CAPTURE_TARGET_REASON_PENDING_EXECUTION,
            CAPTURE_TARGET_REASON_OPEN_POSITION,
        ],
        active_only=True,
        limit=200,
    ):
        option_symbol = _as_text(row.get("option_symbol"))
        if option_symbol is None:
            continue
        existing = rows_by_symbol.get(option_symbol, {})
        rows_by_symbol[option_symbol] = {
            **existing,
            **dict(row),
            "quote_enabled": bool(
                existing.get("quote_enabled", False) or row.get("quote_enabled", False)
            ),
            "trade_enabled": bool(
                existing.get("trade_enabled", False) or row.get("trade_enabled", False)
            ),
        }
    return sorted(rows_by_symbol.values(), key=_option_sort_key)


def _slot_should_be_marked_missed(
    *,
    job_store: Any,
    slot_row: Mapping[str, Any],
    now: datetime,
    stale_after_seconds: int,
) -> bool:
    slot_at = _slot_timestamp(slot_row)
    if slot_at is None:
        return False
    if now <= slot_at + timedelta(seconds=max(stale_after_seconds, 1)):
        return False
    job_run_id = _as_text(slot_row.get("job_run_id"))
    if job_run_id is None:
        return True
    run_record = job_store.get_job_run(job_run_id)
    if run_record is None:
        return True
    run_status = str(run_record.get("status") or "")
    if run_status in {"succeeded"}:
        return False
    if run_status in {"queued", "running"}:
        last_seen = (
            parse_datetime(_as_text(run_record.get("heartbeat_at")))
            or parse_datetime(_as_text(run_record.get("started_at")))
            or parse_datetime(_as_text(run_record.get("scheduled_for")))
        )
        if last_seen is not None and now <= last_seen + timedelta(
            seconds=max(stale_after_seconds, 1)
        ):
            return False
    return True


def _set_recovery_control_mode(
    *,
    db_target: str,
    storage: Any,
    blocked_sessions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    control = get_control_state_snapshot(storage=storage)
    configured_mode = str(control.get("configured_mode") or "normal")
    reason_code = _as_text(control.get("reason_code"))
    configured_source_kind = _as_text(control.get("configured_source_kind"))
    if blocked_sessions:
        if configured_mode == "normal" or (
            reason_code == RECOVERY_CONTROL_REASON_CODE
            and configured_source_kind == "recovery_manager"
        ):
            note = f"{len(blocked_sessions)} live session(s) are blocked by collector gap recovery."
            return set_control_mode(
                db_target=db_target,
                mode="degraded",
                reason_code=RECOVERY_CONTROL_REASON_CODE,
                note=note,
                source_kind="recovery_manager",
                actor_id="collector_recovery",
                metadata={
                    "blocked_session_count": len(blocked_sessions),
                    "session_ids": sorted(blocked_sessions.keys())[:25],
                },
                storage=storage,
            )
        return {
            "action": "set_mode",
            "changed": False,
            "message": "Control mode already reflects a stricter operator state.",
            "control": control,
        }
    if (
        configured_mode == "degraded"
        and reason_code == RECOVERY_CONTROL_REASON_CODE
        and configured_source_kind == "recovery_manager"
    ):
        return set_control_mode(
            db_target=db_target,
            mode="normal",
            reason_code=RECOVERY_CONTROL_CLEAR_REASON_CODE,
            note="Collector gaps cleared after a successful post-gap live slot.",
            source_kind="recovery_manager",
            actor_id="collector_recovery",
            metadata={},
            storage=storage,
        )
    return {
        "action": "set_mode",
        "changed": False,
        "message": "Recovery control mode already clear.",
        "control": control,
    }


def run_collector_recovery(
    *,
    db_target: str,
    storage: Any,
) -> dict[str, Any]:
    recovery_store = storage.recovery
    job_store = storage.jobs
    history_store = storage.history
    if not recovery_store.schema_ready() or not job_store.schema_ready():
        return {
            "status": "skipped",
            "reason": "recovery_schema_unavailable",
        }

    from spreads.services.broker_sync import run_broker_sync
    from spreads.services.exit_manager import run_position_exit_manager

    broker_sync = run_broker_sync(db_target=db_target, storage=storage)
    exit_manager = run_position_exit_manager(db_target=db_target, storage=storage)
    execution_targets = refresh_execution_capture_targets(storage=storage)

    definitions = {
        str(row["job_key"]): dict(row)
        for row in job_store.list_job_definitions(
            enabled_only=True,
            job_type="live_collector",
        )
    }
    now = datetime.now(UTC)
    session_rows = recovery_store.list_live_session_slots(
        statuses=[
            LIVE_SLOT_STATUS_EXPECTED,
            LIVE_SLOT_STATUS_QUEUED,
            LIVE_SLOT_STATUS_RUNNING,
            LIVE_SLOT_STATUS_MISSED,
            LIVE_SLOT_STATUS_RECOVERED,
            LIVE_SLOT_STATUS_UNRECOVERABLE,
            LIVE_SLOT_STATUS_SUCCEEDED,
        ],
        session_date=datetime.now(NEW_YORK).date().isoformat(),
        limit=5000,
        ascending=True,
    )
    rows_by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in session_rows:
        session_id = _as_text(row.get("session_id"))
        if session_id is None:
            continue
        rows_by_session[session_id].append(dict(row))

    recovered_slot_count = 0
    unrecoverable_slot_count = 0
    newly_missed_slot_count = 0
    blocked_sessions: dict[str, dict[str, Any]] = {}
    session_summaries: dict[str, dict[str, Any]] = {}

    for session_id, slot_rows in rows_by_session.items():
        ordered_rows = sorted(
            slot_rows,
            key=lambda row: _slot_timestamp(row) or datetime.fromtimestamp(0, UTC),
        )
        if not ordered_rows:
            continue
        definition = definitions.get(str(ordered_rows[0].get("job_key") or ""))
        payload = definition.get("payload") if isinstance(definition, Mapping) else {}
        interval_seconds = max(_coerce_int(payload.get("interval_seconds")) or 300, 1)
        stale_after_seconds = resolve_live_slot_stale_after_seconds(interval_seconds)

        for slot_row in ordered_rows:
            slot_status = str(slot_row.get("status") or "")
            if slot_status not in {
                LIVE_SLOT_STATUS_EXPECTED,
                LIVE_SLOT_STATUS_QUEUED,
                LIVE_SLOT_STATUS_RUNNING,
            }:
                continue
            if not _slot_should_be_marked_missed(
                job_store=job_store,
                slot_row=slot_row,
                now=now,
                stale_after_seconds=stale_after_seconds,
            ):
                continue
            updated = recovery_store.upsert_live_session_slot(
                job_key=str(slot_row["job_key"]),
                session_id=session_id,
                session_date=str(slot_row["session_date"]),
                label=str(slot_row["label"]),
                slot_at=str(slot_row["slot_at"]),
                scheduled_for=_as_text(slot_row.get("scheduled_for")),
                status=LIVE_SLOT_STATUS_MISSED,
                job_run_id=_as_text(slot_row.get("job_run_id")),
                capture_status=_as_text(slot_row.get("capture_status")),
                recovery_note="Live slot aged past its freshness window before completing.",
                slot_details=_slot_details(slot_row),
                queued_at=_as_text(slot_row.get("queued_at")),
                started_at=_as_text(slot_row.get("started_at")),
                finished_at=_utc_now(),
                updated_at=_utc_now(),
            )
            slot_row.update(updated)
            newly_missed_slot_count += 1

        for slot_row in ordered_rows:
            if str(slot_row.get("status") or "") != LIVE_SLOT_STATUS_MISSED:
                continue
            continuity_rows = _continuity_rows_for_slot(
                recovery_store=recovery_store,
                session_id=session_id,
                slot_rows=ordered_rows,
                slot_at=str(slot_row["slot_at"]),
            )
            if not continuity_rows:
                updated = recovery_store.upsert_live_session_slot(
                    job_key=str(slot_row["job_key"]),
                    session_id=session_id,
                    session_date=str(slot_row["session_date"]),
                    label=str(slot_row["label"]),
                    slot_at=str(slot_row["slot_at"]),
                    scheduled_for=_as_text(slot_row.get("scheduled_for")),
                    status=LIVE_SLOT_STATUS_UNRECOVERABLE,
                    job_run_id=_as_text(slot_row.get("job_run_id")),
                    capture_status=_as_text(slot_row.get("capture_status")),
                    recovery_note="Gap is unrecoverable because no continuity capture targets existed before the missed slot.",
                    slot_details={
                        **_slot_details(slot_row),
                        "continuity_target_count": 0,
                    },
                    queued_at=_as_text(slot_row.get("queued_at")),
                    started_at=_as_text(slot_row.get("started_at")),
                    finished_at=_utc_now(),
                    updated_at=_utc_now(),
                )
                slot_row.update(updated)
                unrecoverable_slot_count += 1
                continue
            coverage = _coverage_summary_for_slot(
                history_store=history_store,
                continuity_rows=continuity_rows,
                slot_at=str(slot_row["slot_at"]),
                interval_seconds=interval_seconds,
            )
            if bool(coverage["coverage_sufficient"]):
                updated = recovery_store.upsert_live_session_slot(
                    job_key=str(slot_row["job_key"]),
                    session_id=session_id,
                    session_date=str(slot_row["session_date"]),
                    label=str(slot_row["label"]),
                    slot_at=str(slot_row["slot_at"]),
                    scheduled_for=_as_text(slot_row.get("scheduled_for")),
                    status=LIVE_SLOT_STATUS_RECOVERED,
                    job_run_id=_as_text(slot_row.get("job_run_id")),
                    capture_status=_as_text(slot_row.get("capture_status")),
                    recovery_note="Continuity quotes were present for all tracked symbols during the missed slot window.",
                    slot_details={
                        **_slot_details(slot_row),
                        "recovery_coverage": coverage,
                        "continuity_targets": continuity_rows,
                    },
                    queued_at=_as_text(slot_row.get("queued_at")),
                    started_at=_as_text(slot_row.get("started_at")),
                    finished_at=_utc_now(),
                    updated_at=_utc_now(),
                )
                slot_row.update(updated)
                recovered_slot_count += 1
            else:
                updated = recovery_store.upsert_live_session_slot(
                    job_key=str(slot_row["job_key"]),
                    session_id=session_id,
                    session_date=str(slot_row["session_date"]),
                    label=str(slot_row["label"]),
                    slot_at=str(slot_row["slot_at"]),
                    scheduled_for=_as_text(slot_row.get("scheduled_for")),
                    status=LIVE_SLOT_STATUS_UNRECOVERABLE,
                    job_run_id=_as_text(slot_row.get("job_run_id")),
                    capture_status=_as_text(slot_row.get("capture_status")),
                    recovery_note="Gap is unrecoverable because recorder coverage was incomplete for tracked continuity symbols.",
                    slot_details={
                        **_slot_details(slot_row),
                        "recovery_coverage": coverage,
                        "continuity_targets": continuity_rows,
                    },
                    queued_at=_as_text(slot_row.get("queued_at")),
                    started_at=_as_text(slot_row.get("started_at")),
                    finished_at=_utc_now(),
                    updated_at=_utc_now(),
                )
                slot_row.update(updated)
                unrecoverable_slot_count += 1

        refreshed_rows = recovery_store.list_live_session_slots(
            session_id=session_id,
            session_date=str(ordered_rows[0]["session_date"]),
            limit=500,
            ascending=True,
        )
        slot_health = summarize_session_slot_health(refreshed_rows)
        session_summaries[session_id] = slot_health
        refresh_recovery_session_capture_targets(
            storage=storage,
            session_id=session_id,
            slot_rows=refreshed_rows,
            slot_health=slot_health,
        )
        if (
            str(slot_health.get("recovery_state") or RECOVERY_STATE_CLEAR)
            != RECOVERY_STATE_CLEAR
        ):
            blocked_sessions[session_id] = slot_health

    control_action = _set_recovery_control_mode(
        db_target=db_target,
        storage=storage,
        blocked_sessions=blocked_sessions,
    )
    return {
        "status": "ok",
        "broker_sync": broker_sync,
        "exit_manager": exit_manager,
        "execution_targets": execution_targets,
        "newly_missed_slot_count": newly_missed_slot_count,
        "recovered_slot_count": recovered_slot_count,
        "unrecoverable_slot_count": unrecoverable_slot_count,
        "blocked_session_count": len(blocked_sessions),
        "sessions": session_summaries,
        "control_action": control_action,
    }
