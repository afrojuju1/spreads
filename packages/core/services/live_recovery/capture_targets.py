from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from core.jobs.orchestration import NEW_YORK, _market_schedule
from core.services.option_quote_records import build_quote_symbol_metadata
from core.services.option_trade_records import build_trade_symbol_metadata
from core.services.positions import enrich_position_row
from core.services.value_coercion import as_text as _as_text
from core.storage.serializers import parse_date

from .shared import (
    CAPTURE_OWNER_EXECUTION_ATTEMPT,
    CAPTURE_OWNER_LIVE_SESSION,
    CAPTURE_OWNER_RECOVERY_SESSION,
    CAPTURE_OWNER_SESSION_POSITION,
    CAPTURE_TARGET_REASON_MONITOR,
    CAPTURE_TARGET_REASON_OPEN_POSITION,
    CAPTURE_TARGET_REASON_PENDING_EXECUTION,
    CAPTURE_TARGET_REASON_PROMOTABLE,
    LIVE_SLOT_STATUS_SUCCEEDED,
    OPEN_POSITION_CAPTURE_STATUSES,
    RECOVERY_STATE_CLEAR,
    _option_sort_key,
    _slot_details,
    _slot_timestamp,
)


def _future_utc(minutes: int) -> str:
    return (
        (datetime.now(UTC) + timedelta(minutes=max(int(minutes), 1)))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


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
        return expiry.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    _, market_close = market_window
    expiry = market_close + timedelta(
        minutes=max(int(session_end_offset_minutes), 0) + max(int(grace_minutes), 0)
    )
    return expiry.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


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

    from core.services.execution import OPEN_STATUSES

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
