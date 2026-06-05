from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from core.db.decorators import with_storage
from core.services.trading_strategy_runtime import resolve_management_runtimes
from core.services.trading_engine.portfolio_runtime import (
    PostgresPortfolioEngine,
    build_blocked_close_decision,
    build_portfolio_run_ref,
    build_position_snapshot,
    close_decision_lifecycle,
    close_decision_row_fields,
)
from core.services.trading_engine.risk_runtime import (
    position_close_block_reason,
    position_is_open,
)
from core.services.execution_portfolio import refresh_session_position_marks
from core.services.positions import enrich_position_row
from core.services.risk_manager import CLOSE_RECONCILIATION_MAX_AGE_SECONDS
from core.storage.serializers import parse_datetime

OPEN_CLOSE_ATTEMPT_STATUSES = [
    "accepted",
    "accepted_for_bidding",
    "calculated",
    "held",
    "new",
    "partially_filled",
    "pending_cancel",
    "pending_new",
    "pending_replace",
    "pending_submission",
    "replaced",
    "stopped",
    "suspended",
]
MANAGED_CLOSE_INTENT_TTL_MINUTES = 5
BROKER_SYNC_KEY = "broker_sync:alpaca"
BROKER_SYNC_IN_FLIGHT_STATUSES = {"queued", "running", "leased"}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _expires_in(minutes: int) -> str:
    return (datetime.now(UTC) + timedelta(minutes=max(minutes, 1))).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _latest_broker_sync_run(storage: Any) -> dict[str, Any] | None:
    job_store = getattr(storage, "jobs", None)
    if job_store is None:
        return None
    rows = job_store.list_job_runs(job_key=BROKER_SYNC_KEY, limit=1)
    return dict(rows[0]) if rows else None


def _broker_sync_snapshot(storage: Any, *, now: datetime) -> dict[str, Any]:
    latest_run = _latest_broker_sync_run(storage)
    latest_run_status = None if latest_run is None else str(latest_run.get("status") or "").lower()
    latest_run_started_at = None if latest_run is None else parse_datetime(latest_run.get("started_at"))
    latest_run_started_at_text = (
        None if latest_run_started_at is None else latest_run_started_at.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    )
    broker_sync_in_flight = latest_run_status in BROKER_SYNC_IN_FLIGHT_STATUSES
    snapshot: dict[str, Any] = {
        "sync_key": BROKER_SYNC_KEY,
        "status": "unknown",
        "reason": None,
        "updated_at": None,
        "age_seconds": None,
        "max_age_seconds": CLOSE_RECONCILIATION_MAX_AGE_SECONDS,
        "state_status": None,
        "job_run_id": None if latest_run is None else latest_run.get("job_run_id"),
        "job_status": latest_run_status,
        "job_started_at": latest_run_started_at_text,
        "state_covers_in_flight_run": False,
    }

    broker_store = getattr(storage, "broker", None)
    if broker_store is None or not broker_store.schema_ready():
        snapshot["status"] = "in_flight" if broker_sync_in_flight else "missing"
        snapshot["reason"] = "broker_sync_in_flight" if broker_sync_in_flight else "broker_sync_schema_unavailable"
        return snapshot
    state = broker_store.get_sync_state(BROKER_SYNC_KEY)
    if not isinstance(state, Mapping):
        snapshot["status"] = "in_flight" if broker_sync_in_flight else "missing"
        snapshot["reason"] = "broker_sync_in_flight" if broker_sync_in_flight else "broker_sync_missing"
        return snapshot

    updated_at = parse_datetime(_as_text(state.get("updated_at")))
    state_status = str(state.get("status") or "unknown").lower()
    age_seconds = None
    if updated_at is not None:
        age_seconds = max((now - updated_at.astimezone(UTC)).total_seconds(), 0.0)
    state_covers_in_flight_run = (
        broker_sync_in_flight
        and updated_at is not None
        and latest_run_started_at is not None
        and updated_at.astimezone(UTC) >= latest_run_started_at.astimezone(UTC)
    )
    snapshot.update(
        {
            "updated_at": None if updated_at is None else updated_at.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "age_seconds": None if age_seconds is None else round(age_seconds, 1),
            "state_status": state_status,
            "summary": dict(state.get("summary") or {}),
            "state_covers_in_flight_run": state_covers_in_flight_run,
        }
    )
    if broker_sync_in_flight and not state_covers_in_flight_run:
        snapshot["status"] = "in_flight"
        snapshot["reason"] = "broker_sync_in_flight"
        return snapshot
    if state_status != "healthy":
        snapshot["status"] = "unhealthy"
        snapshot["reason"] = "broker_sync_unhealthy"
        return snapshot
    if age_seconds is None:
        snapshot["status"] = "missing"
        snapshot["reason"] = "broker_sync_updated_at_missing"
        return snapshot
    if age_seconds > CLOSE_RECONCILIATION_MAX_AGE_SECONDS:
        snapshot["status"] = "stale"
        snapshot["reason"] = "broker_sync_stale"
        return snapshot
    snapshot["status"] = "current"
    return snapshot


def _round_money(value: Any) -> float | None:
    parsed = _coerce_float(value)
    if parsed is None:
        return None
    return round(parsed, 4)


def _close_source_payload(*, kind: str, decision: dict[str, Any]) -> dict[str, Any]:
    details = dict(decision.get("decision_details") or {}) if isinstance(decision.get("decision_details"), dict) else {}
    exit_context: dict[str, Any] = {}
    for key in (
        "mark",
        "effective_mark",
        "entry_value",
        "profit_target_mark",
        "stop_mark",
    ):
        rounded = _round_money(details.get(key))
        if rounded is not None:
            exit_context[key] = rounded
    for key in ("mark_state", "force_close_at"):
        text = _as_text(details.get(key))
        if text is not None:
            exit_context[key] = text

    payload: dict[str, Any] = {
        "kind": kind,
        "reason": _as_text(decision.get("reason")),
        "decision_source": _as_text(decision.get("decision_source")),
        "recipe_ref": _as_text(decision.get("recipe_ref")),
        "limit_price_source": _as_text(decision.get("limit_price_source")),
    }
    if exit_context:
        payload["exit_context"] = exit_context
    close_decision = decision.get("close_decision")
    if isinstance(close_decision, Mapping):
        payload["close_decision"] = dict(close_decision)
    return payload


def _has_open_close_attempt(execution_store: Any, position_id: str) -> bool:
    return bool(
        execution_store.list_open_attempts_for_position(
            position_id=position_id,
            statuses=sorted(OPEN_CLOSE_ATTEMPT_STATUSES),
        )
    )


def _close_intent_id(position_id: str, trading_strategy_id: str) -> str:
    return f"execution_intent:manage:{trading_strategy_id}:{position_id}"


def _close_slot_key(position_id: str) -> str:
    return f"manage:{position_id}:close"


def _has_active_close_intent(execution_store: Any, position_id: str) -> bool:
    if not execution_store.intent_schema_ready():
        return False
    from core.services.execution_intents.shared import ACTIVE_INTENT_STATES

    return bool(
        execution_store.list_execution_intents(
            slot_key=_close_slot_key(position_id),
            states=sorted(ACTIVE_INTENT_STATES),
            limit=1,
        )
    )


def _create_close_intent(
    execution_store: Any,
    *,
    position: dict[str, Any],
    runtime: Any,
    decision: dict[str, Any],
) -> dict[str, Any]:
    from core.services.execution_intents.shared import issue_pending_execution_intent

    position_id = str(position["position_id"])
    close_decision = close_decision_lifecycle(
        position=position,
        decision=decision,
        decision_source=_as_text(decision.get("decision_source")),
    )
    decision = {**decision, "close_decision": close_decision}
    return issue_pending_execution_intent(
        execution_store,
        execution_intent_id=_close_intent_id(position_id, runtime.trading_strategy_id),
        trading_strategy_id=runtime.trading_strategy_id,
        strategy_position_id=position_id,
        execution_attempt_id=None,
        action_type="close",
        slot_key=_close_slot_key(position_id),
        claim_token=None,
        policy_ref={
            "trading_strategy_id": runtime.trading_strategy_id,
            "trade_structure": runtime.trade_structure,
            "routine": "manage",
        },
        config_hash=runtime.config_hash,
        state="pending",
        expires_at=_expires_in(MANAGED_CLOSE_INTENT_TTL_MINUTES),
        superseded_by_id=None,
        payload={
            "position_id": position_id,
            "limit_price": decision.get("limit_price"),
            "limit_price_source": decision.get("limit_price_source"),
            "reason": decision.get("reason"),
            "recipe_ref": decision.get("recipe_ref"),
            "close_decision": close_decision,
            "decision_source": decision.get("decision_source"),
            "decision_details": dict(decision.get("decision_details") or {}),
            "source": _close_source_payload(
                kind="management_runtime_exit",
                decision=decision,
            ),
            "execution_mode": runtime.strategy.execution.mode,
            "approval_mode": runtime.strategy.execution.approval,
            "execution_runtime": runtime.strategy.execution.runtime,
        },
        created_event_payload={
            "position_id": position_id,
            "reason": decision.get("reason"),
            "recipe_ref": decision.get("recipe_ref"),
            "close_decision_id": close_decision.get("close_decision_id"),
            "close_decision_state": close_decision.get("decision_state"),
            "limit_price": decision.get("limit_price"),
            "execution_runtime": runtime.strategy.execution.runtime,
        },
    )


def _refresh_open_position_marks(*, db_target: str, session_ids: list[str], storage: Any | None = None) -> None:
    refresh_session_position_marks(
        db_target=db_target,
        session_ids=session_ids,
        storage=storage,
    )


@with_storage()
def run_position_exit_manager(
    *,
    db_target: str,
    trading_strategy_id: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    open_attempt_guard: dict[str, Any] = {
        "status": "skipped",
        "reason": "not_run",
    }
    if not execution_store.portfolio_schema_ready():
        return {
            "status": "skipped",
            "reason": "positions_schema_unavailable",
            "open_attempt_guard": open_attempt_guard,
        }

    now = datetime.now(UTC)
    broker_sync = _broker_sync_snapshot(storage, now=now)
    management_runtimes = tuple(resolve_management_runtimes())
    portfolio_engine = PostgresPortfolioEngine(
        execution_store=execution_store,
        now=now,
        management_runtimes=management_runtimes,
    )
    portfolio_run_ref = build_portfolio_run_ref(
        trading_strategy_id=trading_strategy_id,
        now=now,
    )
    open_position_snapshots = portfolio_engine.list_open_positions(
        trading_strategy_id=trading_strategy_id,
        limit=200,
    )
    open_positions = [dict(position.payload) for position in open_position_snapshots]
    if not open_positions:
        return {
            "status": "degraded" if open_attempt_guard.get("status") == "degraded" else "ok",
            "portfolio_engine": {
                "run_id": portfolio_run_ref.run_id,
            },
            "position_count": 0,
            "evaluated": 0,
            "created_intents": 0,
            "skipped": 0,
            "failure_count": 0,
            "open_attempt_guard": open_attempt_guard,
            "broker_sync": broker_sync,
        }
    if broker_sync.get("status") != "current":
        broker_reason = str(broker_sync.get("reason") or "broker_sync_not_current")
        broker_decisions: list[dict[str, Any]] = []
        decided_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
        for position in open_positions[:25]:
            close_decision = build_blocked_close_decision(
                position=position,
                reason=broker_reason,
                decision_source="broker_sync",
                decided_at=decided_at,
            )
            broker_decisions.append(
                {
                    "position_id": position.get("position_id"),
                    "reason": broker_reason,
                    "decision_source": "broker_sync",
                    "should_close": False,
                    "portfolio_run_id": portfolio_run_ref.run_id,
                    **close_decision_row_fields(close_decision),
                }
            )
        return {
            "status": "skipped",
            "reason": broker_reason,
            "portfolio_engine": {
                "run_id": portfolio_run_ref.run_id,
            },
            "position_count": len(open_positions),
            "evaluated": 0,
            "created_intents": 0,
            "skipped": len(open_positions),
            "failure_count": 0,
            "decisions": broker_decisions,
            "failures": [],
            "open_attempt_guard": open_attempt_guard,
            "broker_sync": broker_sync,
        }

    try:
        from core.services.execution import run_open_execution_guard

        open_attempt_guard = run_open_execution_guard(
            db_target=db_target,
            storage=storage,
        )
    except Exception as exc:
        open_attempt_guard = {
            "status": "degraded",
            "reason": "guard_error",
            "error": str(exc),
        }

    _refresh_open_position_marks(
        db_target=db_target,
        session_ids=sorted({str(position["session_id"]) for position in open_positions if position.get("session_id")}),
        storage=storage,
    )
    refreshed_position_snapshots = portfolio_engine.list_open_positions(
        trading_strategy_id=trading_strategy_id,
        limit=200,
    )
    refreshed_positions = [dict(position.payload) for position in refreshed_position_snapshots]

    evaluated = 0
    created_intents = 0
    skipped = 0
    failures: list[dict[str, str]] = []
    decisions: list[dict[str, Any]] = []
    now_iso = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    for position_snapshot in refreshed_position_snapshots:
        position = dict(position_snapshot.payload)
        position_id = str(position_snapshot.position_id)
        latest_position = execution_store.get_position(position_id)
        if latest_position is not None:
            position = enrich_position_row(dict(latest_position))
            position_snapshot = build_position_snapshot(position)

        if _has_open_close_attempt(execution_store, position_id):
            evaluated += 1
            skipped += 1
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason="close_already_open",
                updated_at=_utc_now(),
            )
            close_decision = build_blocked_close_decision(
                position=position,
                reason="close_already_open",
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions.append(
                {
                    "position_id": position_id,
                    "reason": "close_already_open",
                    "decision_source": "close_guard",
                    "should_close": False,
                    "portfolio_run_id": portfolio_run_ref.run_id,
                    **close_decision_row_fields(close_decision),
                }
            )
            continue

        close_block_reason = position_close_block_reason(position, now=now)
        if close_block_reason is not None:
            evaluated += 1
            skipped += 1
            if position_is_open(position):
                execution_store.update_position(
                    position_id=position_id,
                    last_exit_evaluated_at=_utc_now(),
                    last_exit_reason=close_block_reason,
                    updated_at=_utc_now(),
                )
            close_decision = build_blocked_close_decision(
                position=position,
                reason=close_block_reason,
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions.append(
                {
                    "position_id": position_id,
                    "reason": close_block_reason,
                    "decision_source": "close_guard",
                    "should_close": False,
                    "portfolio_run_id": portfolio_run_ref.run_id,
                    **close_decision_row_fields(close_decision),
                }
            )
            continue

        close_result = portfolio_engine.evaluate_close(
            run_ref=portfolio_run_ref,
            position=position_snapshot,
        )
        decision = dict(close_result.payload.get("decision") or {})
        decision_source = _as_text(close_result.payload.get("decision_source")) or "portfolio_engine"
        management_runtime = close_result.payload.get("management_runtime")
        close_decision = dict(close_result.payload.get("close_decision") or decision.get("close_decision") or {})
        evaluated += 1
        execution_store.update_position(
            position_id=position_id,
            last_exit_evaluated_at=_utc_now(),
            last_exit_reason=str(decision["reason"]),
            updated_at=_utc_now(),
        )
        decisions.append(
            {
                "position_id": position_id,
                "reason": decision["reason"],
                "decision_source": decision_source,
                "should_close": bool(decision["should_close"]),
                "portfolio_run_id": portfolio_run_ref.run_id,
                **close_decision_row_fields(close_decision),
            }
        )
        if not decision["should_close"]:
            skipped += 1
            continue

        latest_position = execution_store.get_position(position_id)
        if latest_position is not None:
            position = enrich_position_row(dict(latest_position))
            position_snapshot = build_position_snapshot(position)
        close_block_reason = position_close_block_reason(position, now=now)
        if close_block_reason is not None:
            skipped += 1
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason=close_block_reason,
                updated_at=_utc_now(),
            )
            close_decision = build_blocked_close_decision(
                position=position,
                reason=close_block_reason,
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions[-1]["reason"] = close_block_reason
            decisions[-1]["decision_source"] = "close_guard"
            decisions[-1]["should_close"] = False
            decisions[-1].update(close_decision_row_fields(close_decision))
            continue

        if management_runtime is None:
            skipped += 1
            reason = "management_runtime_required_for_close_intent"
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason=reason,
                updated_at=_utc_now(),
            )
            close_decision = build_blocked_close_decision(
                position=position,
                reason=reason,
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions[-1]["reason"] = reason
            decisions[-1]["decision_source"] = "close_guard"
            decisions[-1]["should_close"] = False
            decisions[-1].update(close_decision_row_fields(close_decision))
            continue

        if not execution_store.intent_schema_ready():
            skipped += 1
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason="execution_intent_schema_unavailable",
                updated_at=_utc_now(),
            )
            close_decision = build_blocked_close_decision(
                position=position,
                reason="execution_intent_schema_unavailable",
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions[-1]["reason"] = "execution_intent_schema_unavailable"
            decisions[-1]["decision_source"] = "close_guard"
            decisions[-1]["should_close"] = False
            decisions[-1].update(close_decision_row_fields(close_decision))
            continue

        if _has_active_close_intent(execution_store, position_id):
            skipped += 1
            execution_store.update_position(
                position_id=position_id,
                last_exit_evaluated_at=_utc_now(),
                last_exit_reason="close_intent_already_open",
                updated_at=_utc_now(),
            )
            close_decision = build_blocked_close_decision(
                position=position,
                reason="close_intent_already_open",
                decision_source="close_guard",
                decided_at=now_iso,
            )
            decisions[-1]["reason"] = "close_intent_already_open"
            decisions[-1]["decision_source"] = "close_guard"
            decisions[-1]["should_close"] = False
            decisions[-1].update(close_decision_row_fields(close_decision))
            continue

        try:
            _create_close_intent(
                execution_store,
                position=position,
                runtime=management_runtime,
                decision=decision,
            )
            created_intents += 1
        except Exception as exc:
            failures.append(
                {
                    "position_id": position_id,
                    "error": str(exc),
                }
            )

    return {
        "status": "degraded" if failures or open_attempt_guard.get("status") == "degraded" else "ok",
        "portfolio_engine": {
            "run_id": portfolio_run_ref.run_id,
        },
        "position_count": len(refreshed_positions),
        "evaluated": evaluated,
        "created_intents": created_intents,
        "skipped": skipped,
        "failure_count": len(failures),
        "decisions": decisions[:25],
        "failures": failures[:25],
        "open_attempt_guard": open_attempt_guard,
        "broker_sync": broker_sync,
    }
