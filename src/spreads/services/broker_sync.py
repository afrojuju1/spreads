from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from spreads.db.decorators import with_storage
from spreads.events.bus import publish_global_event_sync
from spreads.services.account_state import fetch_account_overview_live
from spreads.services.alpaca import create_alpaca_client_from_env
from spreads.services.execution import OPEN_STATUSES, refresh_live_session_execution
from spreads.services.session_positions import sync_session_position_from_attempt
from spreads.storage.serializers import parse_datetime

BROKER_SYNC_KEY = "broker_sync:alpaca"
FILL_ACTIVITY_SYNC_KEY = "broker_sync:alpaca:activity:FILL"
OPEN_POSITION_STATUSES = ["open", "partial_close"]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _hydrate_attempt_payload(execution_store: Any, execution_attempt_id: str) -> dict[str, Any] | None:
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        return None
    payload = attempt.to_dict()
    payload["orders"] = [
        order.to_dict()
        for order in execution_store.list_orders(execution_attempt_id=execution_attempt_id)
    ]
    payload["fills"] = [
        fill.to_dict()
        for fill in execution_store.list_fills(execution_attempt_id=execution_attempt_id)
    ]
    return payload


def _parse_activity_timestamp(activity: dict[str, Any]) -> datetime | None:
    return parse_datetime(_as_text(activity.get("transaction_time")))


def _activity_dates(sync_state: dict[str, Any] | None, *, lookback_days: int) -> list[str]:
    today = datetime.now(UTC).date()
    start_date = today - timedelta(days=max(lookback_days, 0))
    cursor = {} if sync_state is None else dict(sync_state.get("cursor") or {})
    last_activity_date = cursor.get("last_activity_date")
    if isinstance(last_activity_date, str):
        try:
            cursor_date = date.fromisoformat(last_activity_date)
        except ValueError:
            cursor_date = None
        if cursor_date is not None:
            start_date = max(start_date, cursor_date - timedelta(days=1))
    days = (today - start_date).days
    return [(start_date + timedelta(days=offset)).isoformat() for offset in range(days + 1)]


def _fetch_fill_activities(*, activity_dates: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    client = create_alpaca_client_from_env()
    errors: list[str] = []
    activities_by_id: dict[str, dict[str, Any]] = {}
    for activity_date in activity_dates:
        try:
            payload = client.list_account_activities(
                activity_type="FILL",
                date=activity_date,
                page_size=100,
                direction="desc",
            )
        except Exception as exc:
            errors.append(f"{activity_date}: {exc}")
            continue
        for activity in payload:
            if not isinstance(activity, dict):
                continue
            activity_id = _as_text(activity.get("id"))
            if activity_id is None:
                continue
            activities_by_id[activity_id] = dict(activity)
    activities = list(activities_by_id.values())
    activities.sort(
        key=lambda activity: _parse_activity_timestamp(activity) or datetime.min.replace(tzinfo=UTC),
        reverse=False,
    )
    return activities, errors


def _sync_recent_fill_activities(
    *,
    broker_store: Any,
    execution_store: Any,
    lookback_days: int,
    synced_at: str,
) -> dict[str, Any]:
    fill_sync_state = broker_store.get_sync_state(FILL_ACTIVITY_SYNC_KEY)
    activity_dates = _activity_dates(
        None if fill_sync_state is None else fill_sync_state.to_dict(),
        lookback_days=lookback_days,
    )
    activities, errors = _fetch_fill_activities(activity_dates=activity_dates)
    broker_order_ids = sorted(
        {
            str(order_id)
            for activity in activities
            if (order_id := _as_text(activity.get("order_id"))) is not None
        }
    )
    persisted_orders = execution_store.list_orders_by_broker_order_ids(broker_order_ids)
    orders_by_broker_order_id = {
        str(order["broker_order_id"]): order.to_dict()
        for order in persisted_orders
    }

    matched_rows_by_attempt: dict[str, list[dict[str, Any]]] = {}
    unmatched_activity_count = 0
    for activity in activities:
        broker_order_id = _as_text(activity.get("order_id"))
        broker_fill_id = _as_text(activity.get("id"))
        filled_at = _as_text(activity.get("transaction_time"))
        symbol = _as_text(activity.get("symbol"))
        quantity = _coerce_float(activity.get("qty"))
        if (
            broker_order_id is None
            or broker_fill_id is None
            or filled_at is None
            or symbol is None
            or quantity is None
        ):
            unmatched_activity_count += 1
            continue
        order = orders_by_broker_order_id.get(broker_order_id)
        if order is None:
            unmatched_activity_count += 1
            continue
        matched_rows_by_attempt.setdefault(str(order["execution_attempt_id"]), []).append(
            {
                "execution_order_id": order.get("execution_order_id"),
                "broker": str(order.get("broker") or "alpaca"),
                "broker_fill_id": broker_fill_id,
                "broker_order_id": broker_order_id,
                "symbol": symbol,
                "side": _as_text(activity.get("side")),
                "fill_type": _as_text(activity.get("type")),
                "quantity": quantity,
                "cumulative_quantity": _coerce_float(activity.get("cum_qty")),
                "remaining_quantity": _coerce_float(activity.get("leaves_qty")),
                "price": _coerce_float(activity.get("price")),
                "filled_at": filled_at,
                "fill": dict(activity),
            }
        )

    affected_attempt_ids: list[str] = []
    for execution_attempt_id, rows in matched_rows_by_attempt.items():
        execution_store.upsert_fills(
            execution_attempt_id=execution_attempt_id,
            rows=rows,
        )
        attempt_payload = _hydrate_attempt_payload(execution_store, execution_attempt_id)
        if attempt_payload is not None:
            sync_session_position_from_attempt(
                execution_store=execution_store,
                attempt=attempt_payload,
            )
            affected_attempt_ids.append(execution_attempt_id)

    latest_activity_timestamp = None
    if activities:
        timestamps = [timestamp for timestamp in (_parse_activity_timestamp(activity) for activity in activities) if timestamp]
        if timestamps:
            latest_activity_timestamp = max(timestamps).isoformat(timespec="seconds").replace("+00:00", "Z")

    summary = {
        "activity_dates": activity_dates,
        "activity_count": len(activities),
        "matched_activity_count": sum(len(rows) for rows in matched_rows_by_attempt.values()),
        "unmatched_activity_count": unmatched_activity_count,
        "affected_attempt_count": len(affected_attempt_ids),
        "error_count": len(errors),
        "latest_activity_timestamp": latest_activity_timestamp,
    }
    broker_store.upsert_sync_state(
        sync_key=FILL_ACTIVITY_SYNC_KEY,
        broker="alpaca",
        status="degraded" if errors else "healthy",
        updated_at=synced_at,
        cursor={
            "last_activity_date": activity_dates[-1] if activity_dates else None,
            "latest_activity_timestamp": latest_activity_timestamp,
        },
        summary={
            **summary,
            "errors": errors[:25],
        },
        error_text=None if not errors else "; ".join(errors[:5]),
    )
    return {
        **summary,
        "errors": errors[:25],
    }


def _reconcile_position(
    *,
    execution_store: Any,
    position: dict[str, Any],
    broker_positions_by_symbol: dict[str, dict[str, Any]],
    reconciled_at: str,
) -> dict[str, Any]:
    remaining_quantity = _coerce_float(position.get("remaining_quantity")) or 0.0
    short_symbol = str(position["short_symbol"])
    long_symbol = str(position["long_symbol"])
    short_position = broker_positions_by_symbol.get(short_symbol)
    long_position = broker_positions_by_symbol.get(long_symbol)

    issues: list[str] = []
    short_qty = _coerce_float(None if short_position is None else short_position.get("qty"))
    long_qty = _coerce_float(None if long_position is None else long_position.get("qty"))
    short_side = _as_text(None if short_position is None else short_position.get("side"))
    long_side = _as_text(None if long_position is None else long_position.get("side"))

    if short_position is None:
        issues.append(f"missing broker short leg {short_symbol}")
    elif short_side != "short":
        issues.append(f"short leg side mismatch for {short_symbol}")
    elif short_qty is None or short_qty < remaining_quantity:
        issues.append(f"short leg quantity mismatch for {short_symbol}")

    if long_position is None:
        issues.append(f"missing broker long leg {long_symbol}")
    elif long_side != "long":
        issues.append(f"long leg side mismatch for {long_symbol}")
    elif long_qty is None or long_qty < remaining_quantity:
        issues.append(f"long leg quantity mismatch for {long_symbol}")

    reconciliation_status = "matched" if not issues else "mismatch"
    reconciliation_note = None if not issues else "; ".join(issues)
    updated = execution_store.update_session_position(
        session_position_id=str(position["session_position_id"]),
        last_reconciled_at=reconciled_at,
        reconciliation_status=reconciliation_status,
        reconciliation_note=reconciliation_note,
        updated_at=reconciled_at,
    )
    return updated.to_dict()


@with_storage()
def run_broker_sync(
    *,
    db_target: str,
    history_range: str = "1D",
    activity_lookback_days: int = 1,
    storage: Any | None = None,
) -> dict[str, Any]:
    now = _utc_now()
    broker_store = storage.broker
    execution_store = storage.execution
    try:
        if not broker_store.schema_ready() or not execution_store.schema_ready() or not execution_store.positions_schema_ready():
            return {
                "status": "skipped",
                "reason": "broker_sync_schema_unavailable",
            }

        overview = fetch_account_overview_live(history_range=history_range)
        snapshot = broker_store.create_account_snapshot(
            broker=str(overview["broker"]),
            environment=str(overview["environment"]),
            source="broker_sync",
            captured_at=str(overview["retrieved_at"]),
            account=dict(overview["account"]),
            pnl=dict(overview["pnl"]),
            positions=list(overview["positions"]),
            history=dict(overview["history"]),
        )

        activity_summary = _sync_recent_fill_activities(
            broker_store=broker_store,
            execution_store=execution_store,
            lookback_days=activity_lookback_days,
            synced_at=now,
        )

        refreshed_attempts = 0
        refresh_errors: list[dict[str, str]] = []
        active_attempts = execution_store.list_attempts_by_status(
            statuses=sorted(OPEN_STATUSES),
            limit=200,
        )
        for attempt in active_attempts:
            try:
                refresh_live_session_execution(
                    db_target=db_target,
                    session_id=str(attempt["session_id"]),
                    execution_attempt_id=str(attempt["execution_attempt_id"]),
                )
                refreshed_attempts += 1
            except Exception as exc:
                refresh_errors.append(
                    {
                        "execution_attempt_id": str(attempt["execution_attempt_id"]),
                        "error": str(exc),
                    }
                )

        broker_positions_by_symbol = {
            str(position["symbol"]): position
            for position in overview["positions"]
            if isinstance(position, dict) and position.get("symbol")
        }
        open_positions = [
            position.to_dict()
            for position in execution_store.list_session_positions(
                statuses=OPEN_POSITION_STATUSES,
                limit=200,
            )
        ]
        reconciled_positions = [
            _reconcile_position(
                execution_store=execution_store,
                position=position,
                broker_positions_by_symbol=broker_positions_by_symbol,
                reconciled_at=now,
            )
            for position in open_positions
        ]
        mismatch_positions = [
            position
            for position in reconciled_positions
            if str(position.get("reconciliation_status") or "") == "mismatch"
        ]
        tracked_symbols = {
            symbol
            for position in open_positions
            for symbol in (str(position["short_symbol"]), str(position["long_symbol"]))
        }
        orphan_broker_positions = [
            position
            for symbol, position in broker_positions_by_symbol.items()
            if symbol not in tracked_symbols and str(position.get("asset_class") or "").lower() == "option"
        ]

        summary = {
            "history_range": history_range,
            "activity_lookback_days": activity_lookback_days,
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_captured_at": snapshot["captured_at"],
            "refreshed_attempt_count": refreshed_attempts,
            "refresh_error_count": len(refresh_errors),
            "open_position_count": len(open_positions),
            "mismatch_position_count": len(mismatch_positions),
            "orphan_broker_position_count": len(orphan_broker_positions),
            "activity_count": activity_summary["activity_count"],
            "matched_activity_count": activity_summary["matched_activity_count"],
            "unmatched_activity_count": activity_summary["unmatched_activity_count"],
            "account_equity": overview["account"].get("equity"),
            "environment": overview["environment"],
            "last_activity_timestamp": activity_summary["latest_activity_timestamp"],
        }
        status = "healthy"
        if refresh_errors or activity_summary["error_count"] or mismatch_positions or orphan_broker_positions:
            status = "degraded"

        state = broker_store.upsert_sync_state(
            sync_key=BROKER_SYNC_KEY,
            broker="alpaca",
            status=status,
            updated_at=now,
            cursor={
                "last_snapshot_at": snapshot["captured_at"],
                "last_activity_timestamp": activity_summary["latest_activity_timestamp"],
            },
            summary={
                **summary,
                "refresh_errors": refresh_errors[:25],
                "activity_errors": activity_summary["errors"],
                "mismatch_positions": [
                    {
                        "session_position_id": position["session_position_id"],
                        "reconciliation_note": position.get("reconciliation_note"),
                    }
                    for position in mismatch_positions[:25]
                ],
                "orphan_broker_positions": orphan_broker_positions[:25],
            },
        )
        publish_global_event_sync(
            topic="broker.sync.updated",
            entity_type="broker_sync",
            entity_id=BROKER_SYNC_KEY,
            payload={
                "sync_key": state["sync_key"],
                "status": state["status"],
                "updated_at": state["updated_at"],
                "summary": state["summary"],
            },
            timestamp=state["updated_at"],
        )
        return {
            "status": status,
            "snapshot_id": snapshot["snapshot_id"],
            "summary": summary,
            "refresh_errors": refresh_errors[:25],
            "activity_errors": activity_summary["errors"],
        }
    except Exception as exc:
        if broker_store.schema_ready():
            broker_store.upsert_sync_state(
                sync_key=BROKER_SYNC_KEY,
                broker="alpaca",
                status="failed",
                updated_at=now,
                cursor={},
                summary={},
                error_text=str(exc),
            )
        raise
