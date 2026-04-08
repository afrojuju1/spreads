from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from spreads.common import env_or_die, load_local_env
from spreads.events.bus import publish_global_event_sync
from spreads.integrations.alpaca.client import (
    AlpacaClient,
    DEFAULT_DATA_BASE_URL,
    infer_trading_base_url,
)
from spreads.services.live_pipelines import build_live_session_id
from spreads.storage.factory import build_collector_repository, build_execution_repository

BROKER_NAME = "alpaca"
EXECUTION_SCHEMA_MESSAGE = "Execution tables are not available yet. Run the latest Alembic migrations."
PENDING_SUBMISSION_STATUS = "pending_submission"
TERMINAL_STATUSES = {
    "canceled",
    "done_for_day",
    "expired",
    "failed",
    "filled",
    "rejected",
}
OPEN_STATUSES = {
    PENDING_SUBMISSION_STATUS,
    "accepted",
    "accepted_for_bidding",
    "calculated",
    "held",
    "new",
    "partially_filled",
    "pending_cancel",
    "pending_new",
    "pending_replace",
    "replaced",
    "stopped",
    "suspended",
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _execution_attempt_id() -> str:
    return f"execution:{uuid4().hex}"


def _execution_client_order_id() -> str:
    return f"spr-exec-{uuid4().hex[:20]}"


def _is_terminal_status(status: str | None) -> bool:
    return str(status or "").lower() in TERMINAL_STATUSES


def _resolve_completed_at(order: dict[str, Any]) -> str | None:
    for key in ("filled_at", "canceled_at", "expired_at", "failed_at", "updated_at"):
        value = _as_text(order.get(key))
        if value:
            return value
    return None


def _create_alpaca_client() -> AlpacaClient:
    load_local_env()
    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    trading_base_url = infer_trading_base_url(key_id, os.environ.get("ALPACA_TRADING_BASE_URL"))
    data_base_url = os.environ.get("ALPACA_DATA_BASE_URL", DEFAULT_DATA_BASE_URL)
    return AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=trading_base_url,
        data_base_url=data_base_url,
    )


def _require_execution_schema(execution_store: Any) -> None:
    if not execution_store.schema_ready():
        raise RuntimeError(EXECUTION_SCHEMA_MESSAGE)


def normalize_execution_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    raw_policy = source.get("execution_policy")
    if isinstance(raw_policy, dict):
        enabled = bool(raw_policy.get("enabled"))
        mode = _as_text(raw_policy.get("mode")) or "disabled"
        quantity = _coerce_int(raw_policy.get("quantity")) or 1
    else:
        enabled = False
        mode = "disabled"
        quantity = 1
    if not enabled:
        return {
            "enabled": False,
            "mode": "disabled",
            "quantity": quantity,
        }
    if mode != "top_board":
        raise ValueError(f"Unsupported execution policy mode: {mode}")
    return {
        "enabled": True,
        "mode": "top_board",
        "quantity": max(quantity, 1),
    }


def _attach_attempt_details(
    *,
    execution_store: Any,
    attempts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not attempts:
        return []
    attempt_ids = [str(item["execution_attempt_id"]) for item in attempts]
    orders = execution_store.list_orders(execution_attempt_ids=attempt_ids)
    fills = execution_store.list_fills(execution_attempt_ids=attempt_ids)
    orders_by_attempt: dict[str, list[dict[str, Any]]] = {}
    fills_by_attempt: dict[str, list[dict[str, Any]]] = {}

    for order in orders:
        orders_by_attempt.setdefault(str(order["execution_attempt_id"]), []).append(order.to_dict())
    for fill in fills:
        fills_by_attempt.setdefault(str(fill["execution_attempt_id"]), []).append(fill.to_dict())

    return [
        {
            **attempt,
            "orders": orders_by_attempt.get(str(attempt["execution_attempt_id"]), []),
            "fills": fills_by_attempt.get(str(attempt["execution_attempt_id"]), []),
        }
        for attempt in attempts
    ]


def list_session_execution_attempts(
    *,
    db_target: str,
    session_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    execution_store = build_execution_repository(db_target)
    try:
        if not execution_store.schema_ready():
            return []
        attempts = [attempt.to_dict() for attempt in execution_store.list_attempts(session_id=session_id, limit=limit)]
        return _attach_attempt_details(execution_store=execution_store, attempts=attempts)
    finally:
        execution_store.close()


def _get_attempt_payload(execution_store: Any, execution_attempt_id: str) -> dict[str, Any]:
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    return _attach_attempt_details(execution_store=execution_store, attempts=[attempt.to_dict()])[0]


def _resolve_session_candidate(
    *,
    collector_store: Any,
    session_id: str,
    candidate_id: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate = collector_store.get_candidate(candidate_id)
    if candidate is None:
        raise ValueError(f"Unknown candidate_id: {candidate_id}")
    cycle = collector_store.get_cycle(str(candidate["cycle_id"]))
    if cycle is None:
        raise ValueError(f"Missing cycle for candidate_id: {candidate_id}")
    candidate_session_id = cycle.get("session_id") or build_live_session_id(cycle["label"], cycle["session_date"])
    if str(candidate_session_id) != session_id:
        raise ValueError(f"Candidate {candidate_id} does not belong to session {session_id}")
    return candidate.to_dict(), cycle.to_dict()


def _build_order_request(
    *,
    candidate: dict[str, Any],
    quantity: int | None,
    limit_price: float | None,
    client_order_id: str,
) -> tuple[dict[str, Any], int, float]:
    candidate_payload = dict(candidate.get("candidate") or {})
    order_payload = dict(candidate_payload.get("order_payload") or {})
    if not order_payload:
        raise ValueError("Selected live candidate does not include an executable order payload")
    resolved_quantity = quantity if quantity is not None else _coerce_int(order_payload.get("qty")) or 1
    if resolved_quantity <= 0:
        raise ValueError("Execution quantity must be positive")
    resolved_limit_price = (
        limit_price
        if limit_price is not None
        else _coerce_float(order_payload.get("limit_price"))
        or _coerce_float(candidate_payload.get("midpoint_credit"))
    )
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError("Execution limit price must be positive")

    request = dict(order_payload)
    request["qty"] = str(int(resolved_quantity))
    request["limit_price"] = f"{float(resolved_limit_price):.2f}"
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), round(float(resolved_limit_price), 2)


def _flatten_order_snapshot(
    order: dict[str, Any],
    *,
    parent_broker_order_id: str | None = None,
) -> list[dict[str, Any]]:
    broker_order_id = _as_text(order.get("id"))
    if broker_order_id is None:
        raise ValueError("Broker order payload is missing an id")
    updated_at = (
        _as_text(order.get("updated_at"))
        or _as_text(order.get("filled_at"))
        or _as_text(order.get("submitted_at"))
        or _utc_now()
    )
    symbol = _as_text(order.get("symbol"))
    side = _as_text(order.get("side"))
    rows = [
        {
            "broker": BROKER_NAME,
            "broker_order_id": broker_order_id,
            "parent_broker_order_id": parent_broker_order_id,
            "client_order_id": _as_text(order.get("client_order_id")),
            "order_status": str(order.get("status") or "unknown"),
            "order_type": _as_text(order.get("type")),
            "time_in_force": _as_text(order.get("time_in_force")),
            "order_class": _as_text(order.get("order_class")),
            "side": side,
            "symbol": symbol,
            "leg_symbol": symbol if parent_broker_order_id is not None else None,
            "leg_side": side if parent_broker_order_id is not None else None,
            "position_intent": _as_text(order.get("position_intent")),
            "quantity": _coerce_float(order.get("qty")),
            "limit_price": _coerce_float(order.get("limit_price")),
            "filled_qty": _coerce_float(order.get("filled_qty")),
            "filled_avg_price": _coerce_float(order.get("filled_avg_price")),
            "submitted_at": _as_text(order.get("submitted_at")),
            "updated_at": updated_at,
            "order": order,
        }
    ]
    for leg in order.get("legs") or []:
        if isinstance(leg, dict):
            rows.extend(_flatten_order_snapshot(leg, parent_broker_order_id=broker_order_id))
    return rows


def _sync_fill_rows(
    *,
    client: AlpacaClient,
    session_date: str,
    persisted_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    broker_order_ids = {str(order["broker_order_id"]) for order in persisted_orders}
    if not broker_order_ids:
        return []
    order_lookup = {str(order["broker_order_id"]): order for order in persisted_orders}
    activities = client.list_account_activities(activity_type="FILL", date=session_date)
    rows: list[dict[str, Any]] = []
    for activity in activities:
        broker_fill_id = _as_text(activity.get("id"))
        broker_order_id = _as_text(activity.get("order_id"))
        symbol = _as_text(activity.get("symbol"))
        filled_at = _as_text(activity.get("transaction_time"))
        quantity = _coerce_float(activity.get("qty"))
        if (
            broker_fill_id is None
            or broker_order_id is None
            or broker_order_id not in broker_order_ids
            or symbol is None
            or filled_at is None
            or quantity is None
        ):
            continue
        matching_order = order_lookup.get(broker_order_id)
        rows.append(
            {
                "execution_order_id": None if matching_order is None else matching_order.get("execution_order_id"),
                "broker": BROKER_NAME,
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
                "fill": activity,
            }
        )
    return rows


def _sync_attempt_state(
    *,
    execution_store: Any,
    attempt: dict[str, Any],
    client: AlpacaClient,
    order_snapshot: dict[str, Any],
) -> dict[str, Any]:
    order_rows = _flatten_order_snapshot(order_snapshot)
    persisted_orders = [
        row.to_dict()
        for row in execution_store.upsert_orders(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=order_rows,
        )
    ]
    try:
        fill_rows = _sync_fill_rows(
            client=client,
            session_date=str(attempt["session_date"]),
            persisted_orders=persisted_orders,
        )
    except Exception:
        fill_rows = []
    if fill_rows:
        execution_store.upsert_fills(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            rows=fill_rows,
        )

    status = str(order_snapshot.get("status") or attempt.get("status") or "unknown").lower()
    completed_at = _resolve_completed_at(order_snapshot) if _is_terminal_status(status) else None
    execution_store.update_attempt(
        execution_attempt_id=str(attempt["execution_attempt_id"]),
        status=status,
        broker_order_id=_as_text(order_snapshot.get("id")),
        client_order_id=_as_text(order_snapshot.get("client_order_id")),
        submitted_at=_as_text(order_snapshot.get("submitted_at")) or str(attempt["requested_at"]),
        completed_at=completed_at,
        error_text=None,
    )
    return _get_attempt_payload(execution_store, str(attempt["execution_attempt_id"]))


def _publish_execution_attempt_event(attempt: dict[str, Any], *, message: str) -> None:
    try:
        publish_global_event_sync(
            topic="execution.attempt.updated",
            entity_type="execution_attempt",
            entity_id=str(attempt["execution_attempt_id"]),
            payload={
                **attempt,
                "message": message,
            },
            timestamp=attempt.get("completed_at")
            or attempt.get("submitted_at")
            or attempt.get("requested_at")
            or _utc_now(),
        )
    except Exception:
        pass


def submit_live_session_execution(
    *,
    db_target: str,
    session_id: str,
    candidate_id: int,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    collector_store = build_collector_repository(db_target)
    execution_store = build_execution_repository(db_target)
    requested_at = _utc_now()
    client_order_id = _execution_client_order_id()
    attempt_id: str | None = None
    submitted_order: dict[str, Any] | None = None
    try:
        _require_execution_schema(execution_store)
        candidate, cycle = _resolve_session_candidate(
            collector_store=collector_store,
            session_id=session_id,
            candidate_id=candidate_id,
        )

        existing_attempts = execution_store.list_open_attempts_for_identity(
            session_id=session_id,
            strategy=str(candidate["strategy"]),
            short_symbol=str(candidate["short_symbol"]),
            long_symbol=str(candidate["long_symbol"]),
            statuses=sorted(OPEN_STATUSES),
        )
        if existing_attempts:
            payload = _get_attempt_payload(
                execution_store,
                str(existing_attempts[0]["execution_attempt_id"]),
            )
            return {
                "action": "submit",
                "changed": False,
                "message": (
                    f"An active execution already exists for "
                    f"{payload['short_symbol']} / {payload['long_symbol']} in this session."
                ),
                "attempt": payload,
            }

        order_request, resolved_quantity, resolved_limit_price = _build_order_request(
            candidate=candidate,
            quantity=quantity,
            limit_price=limit_price,
            client_order_id=client_order_id,
        )

        attempt_id = _execution_attempt_id()
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=session_id,
            session_date=str(cycle["session_date"]),
            label=str(cycle["label"]),
            cycle_id=_as_text(cycle.get("cycle_id")),
            candidate_id=_coerce_int(candidate.get("candidate_id")),
            bucket=_as_text(candidate.get("bucket")),
            candidate_generated_at=_as_text(candidate.get("generated_at")),
            run_id=_as_text(candidate.get("run_id")),
            job_run_id=_as_text(cycle.get("job_run_id")),
            underlying_symbol=str(candidate["underlying_symbol"]),
            strategy=str(candidate["strategy"]),
            expiration_date=str(candidate["expiration_date"]),
            short_symbol=str(candidate["short_symbol"]),
            long_symbol=str(candidate["long_symbol"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                "order": order_request,
                **({} if request_metadata is None else request_metadata),
            },
            candidate=dict(candidate.get("candidate") or {}),
        ).to_dict()

        client = _create_alpaca_client()
        submitted_order = client.submit_order(order_request)
        execution_store.update_attempt(
            execution_attempt_id=str(attempt["execution_attempt_id"]),
            status=str(submitted_order.get("status") or "submitted").lower(),
            broker_order_id=_as_text(submitted_order.get("id")),
            client_order_id=_as_text(submitted_order.get("client_order_id")) or client_order_id,
            submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
        )

        try:
            order_snapshot = client.get_order(str(submitted_order["id"]), nested=True)
        except Exception:
            order_snapshot = submitted_order

        payload = _sync_attempt_state(
            execution_store=execution_store,
            attempt=attempt,
            client=client,
            order_snapshot=order_snapshot,
        )
        message = (
            f"Submitted {payload['underlying_symbol']} {payload['strategy']} "
            f"{payload['short_symbol']} / {payload['long_symbol']}."
        )
        _publish_execution_attempt_event(payload, message=message)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": payload,
        }
    except Exception as exc:
        if submitted_order is None:
            if attempt_id is not None:
                execution_store.update_attempt(
                    execution_attempt_id=attempt_id,
                    status="failed",
                    client_order_id=client_order_id,
                    completed_at=requested_at,
                    error_text=str(exc),
                )
                payload = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    payload,
                    message=f"Execution failed before submission: {exc}",
                )
            raise
        broker_order_id = _as_text(submitted_order.get("id"))
        status = str(submitted_order.get("status") or "submitted").lower()
        if attempt_id is not None:
            execution_store.update_attempt(
                execution_attempt_id=attempt_id,
                status=status,
                broker_order_id=broker_order_id,
                client_order_id=_as_text(submitted_order.get("client_order_id")) or client_order_id,
                submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
                completed_at=_resolve_completed_at(submitted_order) if _is_terminal_status(status) else None,
                error_text=str(exc),
            )
            payload = _get_attempt_payload(execution_store, attempt_id)
            message = (
                f"Order {broker_order_id or payload['execution_attempt_id']} was submitted, "
                f"but local execution sync failed: {exc}"
            )
            _publish_execution_attempt_event(payload, message=message)
            return {
                "action": "submit",
                "changed": True,
                "message": message,
                "attempt": payload,
            }
        raise
    finally:
        execution_store.close()
        collector_store.close()


def refresh_live_session_execution(
    *,
    db_target: str,
    session_id: str,
    execution_attempt_id: str,
) -> dict[str, Any]:
    execution_store = build_execution_repository(db_target)
    try:
        _require_execution_schema(execution_store)
        attempt = execution_store.get_attempt(execution_attempt_id)
        if attempt is None:
            raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
        if str(attempt["session_id"]) != session_id:
            raise ValueError(f"Execution {execution_attempt_id} does not belong to session {session_id}")
        broker_order_id = _as_text(attempt.get("broker_order_id"))
        if broker_order_id is None:
            raise ValueError("Execution does not have a broker order id to refresh")

        client = _create_alpaca_client()
        order_snapshot = client.get_order(broker_order_id, nested=True)
        payload = _sync_attempt_state(
            execution_store=execution_store,
            attempt=attempt.to_dict(),
            client=client,
            order_snapshot=order_snapshot,
        )
        message = f"Refreshed execution {execution_attempt_id}: {payload['status']}."
        _publish_execution_attempt_event(payload, message=message)
        return {
            "action": "refresh",
            "changed": True,
            "message": message,
            "attempt": payload,
        }
    finally:
        execution_store.close()


def submit_auto_session_execution(
    *,
    db_target: str,
    session_id: str,
    cycle_id: str,
    policy: dict[str, Any] | None,
    job_run_id: str | None = None,
) -> dict[str, Any]:
    normalized_policy = normalize_execution_policy(policy)
    if not normalized_policy["enabled"]:
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": "execution_disabled",
            "message": "Automatic execution is disabled for this live collector.",
            "policy": normalized_policy,
        }

    collector_store = build_collector_repository(db_target)
    execution_store = build_execution_repository(db_target)
    try:
        _require_execution_schema(execution_store)
        existing_attempts = execution_store.list_attempts(session_id=session_id, limit=1)
        if existing_attempts:
            return {
                "action": "auto_submit",
                "changed": False,
                "reason": "session_already_has_execution",
                "message": "Automatic execution skipped because this session already has an execution attempt.",
                "policy": normalized_policy,
                "attempt": _get_attempt_payload(
                    execution_store,
                    str(existing_attempts[0]["execution_attempt_id"]),
                ),
            }

        board_candidates = collector_store.list_cycle_candidates(cycle_id, bucket="board")
        if not board_candidates:
            return {
                "action": "auto_submit",
                "changed": False,
                "reason": "no_board_candidate",
                "message": "Automatic execution skipped because the cycle does not have a board candidate.",
                "policy": normalized_policy,
            }

        top_candidate = min(board_candidates, key=lambda candidate: int(candidate["position"]))
        result = submit_live_session_execution(
            db_target=db_target,
            session_id=session_id,
            candidate_id=int(top_candidate["candidate_id"]),
            quantity=int(normalized_policy["quantity"]),
            request_metadata={
                "source": {
                    "kind": "auto_session_execution",
                    "mode": normalized_policy["mode"],
                    "cycle_id": cycle_id,
                    "job_run_id": job_run_id,
                    "candidate_id": int(top_candidate["candidate_id"]),
                },
                "policy": normalized_policy,
            },
        )
        return {
            **result,
            "action": "auto_submit",
            "reason": None,
            "policy": normalized_policy,
            "selected_candidate_id": int(top_candidate["candidate_id"]),
        }
    finally:
        execution_store.close()
        collector_store.close()
