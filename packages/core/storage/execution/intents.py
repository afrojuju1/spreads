from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload

from core.engine.events import EngineAggregateType, EngineEvent, EngineEventType
from core.services.trading_lifecycle import LifecycleObject, require_lifecycle_transition
from core.storage.engine_event_repository import append_engine_event_in_session
from core.storage.execution_models import ExecutionAttemptModel, ExecutionIntentModel
from core.storage.lifecycle_models import TradeAdmissionModel
from core.storage.records import ExecutionIntentRecord
from core.storage.serializers import parse_date, parse_datetime, render_value


class ExecutionIntentConcurrencyError(RuntimeError):
    """The caller attempted to transition a stale intent projection."""


def _required_datetime(value: Any, *, field: str) -> datetime:
    parsed = parse_datetime(value)
    if parsed is None:
        raise ValueError(f"{field} is required")
    return parsed


def _admission_values(admission: dict[str, Any]) -> dict[str, Any]:
    source_object_type = str(admission.get("source_object_type") or "").strip()
    source_object_id = str(admission.get("source_object_id") or "").strip()
    if not source_object_type or not source_object_id:
        raise ValueError("admission source_object_type and source_object_id are required")
    evidence = dict(admission.get("evidence") or {})
    close_decision_id = admission.get("close_decision_id")
    if close_decision_id is None and source_object_type == "close_decision":
        close_decision_id = source_object_id
    return {
        "admission_decision_id": str(admission["admission_decision_id"]),
        "source_object_type": source_object_type,
        "source_object_id": source_object_id,
        "trade_signal_id": admission.get("trade_signal_id"),
        "trade_decision_id": admission.get("trade_decision_id"),
        "close_decision_id": close_decision_id,
        "position_id": admission.get("position_id") or evidence.get("position_id"),
        "admission_kind": str(admission["admission_kind"]),
        "admission_state": str(admission["admission_state"]),
        "account_id": admission.get("account_id"),
        "session_date": parse_date(admission["session_date"]),
        "requested_quantity": admission.get("requested_quantity"),
        "requested_notional": admission.get("requested_notional"),
        "max_loss": admission.get("max_loss"),
        "policy_snapshot_json": render_value(dict(admission.get("policy_snapshot") or {})),
        "capability_snapshot_json": render_value(dict(admission.get("capability_snapshot") or {})),
        "metrics_json": render_value(dict(admission.get("metrics") or {})),
        "reason_codes_json": list(admission.get("reason_codes") or []),
        "blockers_json": list(admission.get("blockers") or []),
        "evidence_json": render_value(evidence),
        "note": admission.get("note") or admission.get("message"),
        "decided_at": _required_datetime(admission.get("decided_at"), field="admission decided_at"),
    }


def _intent_values(execution_intent: dict[str, Any]) -> dict[str, Any]:
    initial_state = require_lifecycle_transition(
        LifecycleObject.EXECUTION_INTENT,
        None,
        str(execution_intent["state"]),
    ).to_state
    return {
        "execution_intent_id": str(execution_intent["execution_intent_id"]),
        "trading_strategy_id": str(execution_intent["trading_strategy_id"]),
        "trade_signal_id": execution_intent.get("trade_signal_id"),
        "trade_decision_id": execution_intent.get("trade_decision_id"),
        "admission_decision_id": execution_intent.get("admission_decision_id"),
        "close_decision_id": execution_intent.get("close_decision_id"),
        "position_id": execution_intent.get("position_id"),
        "intent_kind": str(execution_intent["intent_kind"]),
        "slot_key": str(execution_intent["slot_key"]),
        "claim_token": execution_intent.get("claim_token"),
        "claimed_at": parse_datetime(execution_intent.get("claimed_at")),
        "workflow_id": execution_intent.get("workflow_id"),
        "workflow_run_id": execution_intent.get("workflow_run_id"),
        "policy_ref_json": render_value(dict(execution_intent.get("policy_ref") or {})),
        "config_hash": str(execution_intent.get("config_hash") or ""),
        "state": initial_state,
        "expires_at": parse_datetime(execution_intent.get("expires_at")),
        "supersedes_execution_intent_id": execution_intent.get("supersedes_execution_intent_id"),
        "state_version": int(execution_intent.get("state_version") or 1),
        "payload_json": render_value(dict(execution_intent.get("payload") or {})),
        "created_at": _required_datetime(execution_intent.get("created_at"), field="execution intent created_at"),
        "updated_at": _required_datetime(execution_intent.get("updated_at"), field="execution intent updated_at"),
    }


def _assert_immutable_match(row: Any, values: dict[str, Any], *, fields: tuple[str, ...], object_name: str) -> None:
    mismatches = [field for field in fields if render_value(getattr(row, field)) != render_value(values[field])]
    if mismatches:
        raise ValueError(f"{object_name} already exists with conflicting immutable fields: {', '.join(mismatches)}")


class ExecutionIntentRepositoryMixin:
    def get_execution_intent(self, execution_intent_id: str) -> ExecutionIntentRecord | None:
        with self.session_factory() as session:
            row = session.get(ExecutionIntentModel, execution_intent_id)
            return None if row is None else self.row(row)

    def get_execution_attempt_for_intent(self, execution_intent_id: str) -> dict[str, Any] | None:
        with self.session_factory() as session:
            row = session.scalar(
                select(ExecutionAttemptModel).where(ExecutionAttemptModel.execution_intent_id == execution_intent_id)
            )
            return None if row is None else self._attempt_row(row)

    def get_successor_execution_intent(self, execution_intent_id: str) -> ExecutionIntentRecord | None:
        with self.session_factory() as session:
            row = session.scalar(
                select(ExecutionIntentModel).where(
                    ExecutionIntentModel.supersedes_execution_intent_id == execution_intent_id
                )
            )
            return None if row is None else self.row(row)

    def persist_admission_intent_handoff(
        self,
        *,
        admission: dict[str, Any],
        execution_intent: dict[str, Any] | None = None,
        created_event_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        admission_values = _admission_values(admission)
        admission_id = str(admission_values["admission_decision_id"])
        admission_state = str(admission_values["admission_state"])
        if admission_state == "approved" and execution_intent is None:
            raise ValueError("Approved admission requires an execution intent in the same transaction")
        if admission_state != "approved" and execution_intent is not None:
            raise ValueError("Blocked or unknown admission cannot create an execution intent")

        intent_values = None if execution_intent is None else _intent_values(execution_intent)
        if intent_values is not None and intent_values["admission_decision_id"] != admission_id:
            raise ValueError("execution intent admission_decision_id must match the admission")
        with self.session_scope() as session:
            admission_inserted = session.execute(
                pg_insert(TradeAdmissionModel)
                .values(**admission_values)
                .on_conflict_do_nothing(index_elements=["admission_decision_id"])
                .returning(TradeAdmissionModel.admission_decision_id)
            ).scalar_one_or_none()
            admission_row = session.get(TradeAdmissionModel, admission_id)
            if admission_row is None:
                raise RuntimeError(f"Admission {admission_id!r} disappeared during handoff")
            if admission_inserted is None:
                _assert_immutable_match(
                    admission_row,
                    admission_values,
                    fields=(
                        "source_object_type",
                        "source_object_id",
                        "trade_signal_id",
                        "trade_decision_id",
                        "close_decision_id",
                        "position_id",
                        "admission_kind",
                        "admission_state",
                        "session_date",
                    ),
                    object_name=f"admission {admission_id}",
                )

            intent_row = None
            if intent_values is not None:
                intent_id = str(intent_values["execution_intent_id"])
                intent_inserted = session.execute(
                    pg_insert(ExecutionIntentModel)
                    .values(**intent_values)
                    .on_conflict_do_nothing(index_elements=["execution_intent_id"])
                    .returning(ExecutionIntentModel.execution_intent_id)
                ).scalar_one_or_none()
                intent_row = session.get(ExecutionIntentModel, intent_id)
                if intent_row is None:
                    raise RuntimeError(f"Execution intent {intent_id!r} disappeared during handoff")
                if intent_inserted is None:
                    _assert_immutable_match(
                        intent_row,
                        intent_values,
                        fields=(
                            "trading_strategy_id",
                            "trade_signal_id",
                            "trade_decision_id",
                            "admission_decision_id",
                            "close_decision_id",
                            "position_id",
                            "intent_kind",
                            "slot_key",
                            "policy_ref_json",
                            "config_hash",
                            "supersedes_execution_intent_id",
                            "payload_json",
                        ),
                        object_name=f"execution intent {intent_id}",
                    )
                else:
                    append_engine_event_in_session(
                        session,
                        EngineEvent(
                            event_type=EngineEventType.COMMAND_ACCEPTED,
                            aggregate_type=EngineAggregateType.EXECUTION_INTENT,
                            aggregate_id=intent_id,
                            aggregate_version=1,
                            lifecycle_object=LifecycleObject.EXECUTION_INTENT.value,
                            to_state=str(intent_values["state"]),
                            trading_strategy_id=str(intent_values["trading_strategy_id"]),
                            trade_signal_id=intent_values["trade_signal_id"],
                            trade_decision_id=intent_values["trade_decision_id"],
                            execution_intent_id=intent_id,
                            position_id=intent_values["position_id"],
                            correlation_id=intent_id,
                            idempotency_key=f"execution_intent_created:{intent_id}:1",
                            payload={
                                "intent_event": "created",
                                "admission_decision_id": admission_id,
                                **dict(created_event_payload or {}),
                            },
                            occurred_at=intent_values["created_at"],
                        ),
                    )
            return {
                "admission": self.row(admission_row),
                "execution_intent": None if intent_row is None else self.row(intent_row),
            }

    def transition_execution_intent(
        self,
        *,
        execution_intent_id: str,
        expected_state: str,
        expected_version: int,
        to_state: str,
        transition_reason: str,
        event_payload: dict[str, Any] | None = None,
        engine_event_type: str = EngineEventType.STATE_TRANSITIONED,
        claim_token: str | None = None,
        claimed_at: str | None = None,
        workflow_id: str | None = None,
        workflow_run_id: str | None = None,
        execution_attempt_id: str | None = None,
        occurred_at: str | None = None,
    ) -> ExecutionIntentRecord:
        normalized_to = require_lifecycle_transition(
            LifecycleObject.EXECUTION_INTENT,
            expected_state,
            to_state,
        ).to_state
        event_at = parse_datetime(occurred_at) or datetime.now(UTC)
        new_version = int(expected_version) + 1
        values: dict[str, Any] = {
            "state": normalized_to,
            "state_version": new_version,
            "updated_at": event_at,
        }
        if claim_token is not None:
            values["claim_token"] = claim_token
        if claimed_at is not None:
            values["claimed_at"] = parse_datetime(claimed_at)
        if workflow_id is not None:
            values["workflow_id"] = workflow_id
        if workflow_run_id is not None:
            values["workflow_run_id"] = workflow_run_id

        with self.session_scope() as session:
            updated_id = session.execute(
                update(ExecutionIntentModel)
                .where(
                    ExecutionIntentModel.execution_intent_id == execution_intent_id,
                    ExecutionIntentModel.state == expected_state,
                    ExecutionIntentModel.state_version == int(expected_version),
                )
                .values(**values)
                .returning(ExecutionIntentModel.execution_intent_id)
            ).scalar_one_or_none()
            row = session.get(ExecutionIntentModel, execution_intent_id)
            if row is None:
                raise ValueError(f"Unknown execution_intent_id: {execution_intent_id}")
            if updated_id is None:
                raise ExecutionIntentConcurrencyError(
                    f"Execution intent {execution_intent_id} expected {expected_state}@{expected_version}, "
                    f"found {row.state}@{row.state_version}"
                )
            append_engine_event_in_session(
                session,
                EngineEvent(
                    event_type=engine_event_type,
                    aggregate_type=EngineAggregateType.EXECUTION_INTENT,
                    aggregate_id=execution_intent_id,
                    aggregate_version=new_version,
                    lifecycle_object=LifecycleObject.EXECUTION_INTENT.value,
                    from_state=expected_state,
                    to_state=normalized_to,
                    trading_strategy_id=row.trading_strategy_id,
                    trade_signal_id=row.trade_signal_id,
                    trade_decision_id=row.trade_decision_id,
                    execution_intent_id=execution_intent_id,
                    execution_attempt_id=execution_attempt_id,
                    position_id=row.position_id,
                    workflow_id=workflow_id or row.workflow_id,
                    workflow_run_id=workflow_run_id or row.workflow_run_id,
                    correlation_id=execution_intent_id,
                    idempotency_key=f"execution_intent_transition:{execution_intent_id}:{new_version}",
                    payload={"transition_reason": transition_reason, **dict(event_payload or {})},
                    occurred_at=event_at,
                ),
            )
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_execution_intents(
        self,
        *,
        trading_strategy_id: str | None = None,
        trade_signal_id: str | None = None,
        trade_decision_id: str | None = None,
        position_id: str | None = None,
        slot_key: str | None = None,
        states: list[str] | None = None,
        execution_attempt_id: str | None = None,
        limit: int = 200,
    ) -> list[ExecutionIntentRecord]:
        statement = select(ExecutionIntentModel)
        if execution_attempt_id:
            statement = statement.join(
                ExecutionAttemptModel,
                ExecutionAttemptModel.execution_intent_id == ExecutionIntentModel.execution_intent_id,
            ).where(ExecutionAttemptModel.execution_attempt_id == execution_attempt_id)
        if trading_strategy_id:
            statement = statement.where(ExecutionIntentModel.trading_strategy_id == trading_strategy_id)
        if trade_signal_id:
            statement = statement.where(ExecutionIntentModel.trade_signal_id == trade_signal_id)
        if trade_decision_id:
            statement = statement.where(ExecutionIntentModel.trade_decision_id == trade_decision_id)
        if position_id:
            statement = statement.where(ExecutionIntentModel.position_id == position_id)
        if slot_key:
            statement = statement.where(ExecutionIntentModel.slot_key == slot_key)
        if states:
            statement = statement.where(ExecutionIntentModel.state.in_(states))
        statement = statement.order_by(
            ExecutionIntentModel.created_at.desc(),
            ExecutionIntentModel.execution_intent_id.asc(),
        ).limit(limit)
        with self.session_factory() as session:
            return self.rows(list(session.scalars(statement).all()))

    def list_trade_decision_lifecycle_states(
        self,
        *,
        trade_decision_ids: list[str],
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        normalized_ids = sorted({str(value).strip() for value in trade_decision_ids if str(value).strip()})
        if not normalized_ids:
            return []
        statement = (
            select(TradeAdmissionModel, ExecutionIntentModel)
            .outerjoin(
                ExecutionIntentModel,
                ExecutionIntentModel.admission_decision_id == TradeAdmissionModel.admission_decision_id,
            )
            .where(TradeAdmissionModel.trade_decision_id.in_(normalized_ids))
            .order_by(
                TradeAdmissionModel.decided_at.desc(),
                TradeAdmissionModel.admission_decision_id.asc(),
            )
            .limit(max(int(limit), 1))
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        lifecycle_by_decision: dict[str, dict[str, Any]] = {}
        for admission, intent in rows:
            decision_id = str(admission.trade_decision_id or "").strip()
            if not decision_id or decision_id in lifecycle_by_decision:
                continue
            lifecycle_by_decision[decision_id] = {
                "trade_decision_id": decision_id,
                "admission": self.row(admission),
                "intent": None if intent is None else self.row(intent),
            }
        return list(lifecycle_by_decision.values())

    def list_approved_admissions_missing_execution_intents(
        self,
        *,
        session_date: str | date | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        statement = (
            select(TradeAdmissionModel)
            .options(selectinload(TradeAdmissionModel.decision))
            .outerjoin(
                ExecutionIntentModel,
                ExecutionIntentModel.admission_decision_id == TradeAdmissionModel.admission_decision_id,
            )
            .where(TradeAdmissionModel.admission_state == "approved")
            .where(ExecutionIntentModel.execution_intent_id.is_(None))
            .order_by(TradeAdmissionModel.decided_at, TradeAdmissionModel.admission_decision_id)
            .limit(max(int(limit), 1))
        )
        if session_date is not None:
            statement = statement.where(TradeAdmissionModel.session_date == parse_date(session_date))
        with self.session_factory() as session:
            rows = list(session.scalars(statement).all())
            return [
                {
                    "admission": self.row(admission),
                    "decision": None if admission.decision is None else self.row(admission.decision),
                }
                for admission in rows
            ]
