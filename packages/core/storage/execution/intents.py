from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload

from core.storage.execution_models import (
    ExecutionIntentEventModel,
    ExecutionIntentModel,
)
from core.storage.lifecycle_models import (
    TradeAdmissionModel,
    TradeExecutionIntentModel,
)
from core.storage.records import ExecutionIntentEventRecord, ExecutionIntentRecord
from core.storage.serializers import parse_date, parse_datetime, render_value


class ExecutionIntentRepositoryMixin:
    def get_execution_intent(self, execution_intent_id: str) -> ExecutionIntentRecord | None:
        with self.session_factory() as session:
            row = session.get(ExecutionIntentModel, execution_intent_id)
        if row is None:
            return None
        return self.row(row)

    def delete_execution_intent(self, execution_intent_id: str) -> bool:
        with self.session_scope() as session:
            row = session.get(ExecutionIntentModel, execution_intent_id)
            if row is None:
                return False
            session.delete(row)
            return True

    def upsert_admission_intent_handoff(
        self,
        *,
        trade_intent: dict[str, Any],
        admission: dict[str, Any],
        execution_intent: dict[str, Any] | None = None,
        created_event: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        trade_intent_created_at = parse_datetime(trade_intent.get("created_at"))
        trade_intent_updated_at = parse_datetime(trade_intent.get("updated_at"))
        trade_intent_claimed_at = parse_datetime(trade_intent.get("claimed_at"))
        trade_intent_expires_at = parse_datetime(trade_intent.get("expires_at"))
        admission_decided_at = parse_datetime(admission.get("decided_at"))
        if trade_intent_created_at is None or trade_intent_updated_at is None:
            raise ValueError("trade intent created_at and updated_at are required")
        if admission_decided_at is None:
            raise ValueError("admission decided_at is required")

        execution_created_at = None if execution_intent is None else parse_datetime(execution_intent.get("created_at"))
        execution_updated_at = None if execution_intent is None else parse_datetime(execution_intent.get("updated_at"))
        execution_expires_at = None if execution_intent is None else parse_datetime(execution_intent.get("expires_at"))
        event_at = None if created_event is None else parse_datetime(created_event.get("event_at"))
        if execution_intent is not None and (execution_created_at is None or execution_updated_at is None):
            raise ValueError("execution intent created_at and updated_at are required")
        if created_event is not None and execution_intent is None:
            raise ValueError("created event requires execution_intent")
        if created_event is not None and event_at is None:
            raise ValueError("created event event_at is required")
        if created_event is not None:
            created_event_intent_id = str(created_event.get("execution_intent_id") or "").strip()
            execution_intent_id = str(execution_intent.get("execution_intent_id") or "").strip()
            if not created_event_intent_id:
                raise ValueError("created event execution_intent_id is required")
            if not execution_intent_id:
                raise ValueError("execution intent execution_intent_id is required")
            if created_event_intent_id != execution_intent_id:
                raise ValueError("created event execution_intent_id must match execution_intent")
            if str(created_event.get("event_type") or "") != "created":
                raise ValueError("created event event_type must be created")

        with self.session_scope() as session:
            trade_intent_row = session.get(TradeExecutionIntentModel, str(trade_intent["execution_intent_id"]))
            if trade_intent_row is None:
                trade_intent_row = TradeExecutionIntentModel(
                    execution_intent_id=str(trade_intent["execution_intent_id"]),
                    created_at=trade_intent_created_at,
                    intent_kind=str(trade_intent["intent_kind"]),
                    source_object_type=str(trade_intent["source_object_type"]),
                    source_object_id=str(trade_intent["source_object_id"]),
                    trade_signal_id=trade_intent.get("trade_signal_id"),
                    trade_decision_id=trade_intent.get("trade_decision_id"),
                    position_id=trade_intent.get("position_id"),
                    trading_strategy_id=trade_intent.get("trading_strategy_id"),
                    trade_structure=trade_intent.get("trade_structure"),
                    routine=trade_intent.get("routine"),
                    account_id=trade_intent.get("account_id"),
                    slot_key=str(trade_intent["slot_key"]),
                    idempotency_key=str(trade_intent["idempotency_key"]),
                    intent_state=str(trade_intent["intent_state"]),
                    claim_token=trade_intent.get("claim_token"),
                    claimed_at=trade_intent_claimed_at,
                    expires_at=trade_intent_expires_at,
                    supersedes_intent_id=trade_intent.get("supersedes_intent_id"),
                    superseded_by_intent_id=trade_intent.get("superseded_by_intent_id"),
                    payload_json=render_value(dict(trade_intent.get("payload") or {})),
                    policy_snapshot_json=render_value(dict(trade_intent.get("policy_snapshot") or {})),
                    config_hash=trade_intent.get("config_hash"),
                    updated_at=trade_intent_updated_at,
                )
                session.add(trade_intent_row)
            else:
                trade_intent_row.intent_kind = str(trade_intent["intent_kind"])
                trade_intent_row.source_object_type = str(trade_intent["source_object_type"])
                trade_intent_row.source_object_id = str(trade_intent["source_object_id"])
                trade_intent_row.trade_signal_id = trade_intent.get("trade_signal_id")
                trade_intent_row.trade_decision_id = trade_intent.get("trade_decision_id")
                trade_intent_row.position_id = trade_intent.get("position_id")
                trade_intent_row.trading_strategy_id = trade_intent.get("trading_strategy_id")
                trade_intent_row.trade_structure = trade_intent.get("trade_structure")
                trade_intent_row.routine = trade_intent.get("routine")
                trade_intent_row.account_id = trade_intent.get("account_id")
                trade_intent_row.slot_key = str(trade_intent["slot_key"])
                trade_intent_row.idempotency_key = str(trade_intent["idempotency_key"])
                trade_intent_row.intent_state = str(trade_intent["intent_state"])
                trade_intent_row.claim_token = trade_intent.get("claim_token")
                trade_intent_row.claimed_at = trade_intent_claimed_at
                trade_intent_row.expires_at = trade_intent_expires_at
                trade_intent_row.supersedes_intent_id = trade_intent.get("supersedes_intent_id")
                trade_intent_row.superseded_by_intent_id = trade_intent.get("superseded_by_intent_id")
                trade_intent_row.payload_json = render_value(dict(trade_intent.get("payload") or {}))
                trade_intent_row.policy_snapshot_json = render_value(dict(trade_intent.get("policy_snapshot") or {}))
                trade_intent_row.config_hash = trade_intent.get("config_hash")
                trade_intent_row.updated_at = trade_intent_updated_at

            session.flush()

            admission_row = session.get(TradeAdmissionModel, str(admission["admission_decision_id"]))
            if admission_row is None:
                admission_row = TradeAdmissionModel(
                    admission_decision_id=str(admission["admission_decision_id"]),
                    execution_intent_id=str(admission["execution_intent_id"]),
                    trade_signal_id=admission.get("trade_signal_id"),
                    trade_decision_id=admission.get("trade_decision_id"),
                    position_id=admission.get("position_id"),
                    admission_kind=str(admission["admission_kind"]),
                    admission_state=str(admission["admission_state"]),
                    account_id=admission.get("account_id"),
                    session_date=parse_date(admission["session_date"]),
                    requested_quantity=admission.get("requested_quantity"),
                    requested_notional=admission.get("requested_notional"),
                    max_loss=admission.get("max_loss"),
                    policy_snapshot_json=render_value(dict(admission.get("policy_snapshot") or {})),
                    capability_snapshot_json=render_value(dict(admission.get("capability_snapshot") or {})),
                    metrics_json=render_value(dict(admission.get("metrics") or {})),
                    reason_codes_json=list(admission.get("reason_codes") or []),
                    blockers_json=list(admission.get("blockers") or []),
                    evidence_json=render_value(dict(admission.get("evidence") or {})),
                    note=admission.get("note"),
                    execution_attempt_id=admission.get("execution_attempt_id"),
                    decided_at=admission_decided_at,
                )
                session.add(admission_row)
            else:
                admission_row.execution_intent_id = str(admission["execution_intent_id"])
                admission_row.trade_signal_id = admission.get("trade_signal_id")
                admission_row.trade_decision_id = admission.get("trade_decision_id")
                admission_row.position_id = admission.get("position_id")
                admission_row.admission_kind = str(admission["admission_kind"])
                admission_row.admission_state = str(admission["admission_state"])
                admission_row.account_id = admission.get("account_id")
                admission_row.session_date = parse_date(admission["session_date"])
                admission_row.requested_quantity = admission.get("requested_quantity")
                admission_row.requested_notional = admission.get("requested_notional")
                admission_row.max_loss = admission.get("max_loss")
                admission_row.policy_snapshot_json = render_value(dict(admission.get("policy_snapshot") or {}))
                admission_row.capability_snapshot_json = render_value(dict(admission.get("capability_snapshot") or {}))
                admission_row.metrics_json = render_value(dict(admission.get("metrics") or {}))
                admission_row.reason_codes_json = list(admission.get("reason_codes") or [])
                admission_row.blockers_json = list(admission.get("blockers") or [])
                admission_row.evidence_json = render_value(dict(admission.get("evidence") or {}))
                admission_row.note = admission.get("note")
                admission_row.execution_attempt_id = admission.get("execution_attempt_id")
                admission_row.decided_at = admission_decided_at

            execution_row = None
            event_row = None
            if execution_intent is not None:
                execution_row = self._upsert_execution_intent_model(
                    session,
                    execution_intent=execution_intent,
                    created_at=execution_created_at,
                    updated_at=execution_updated_at,
                    expires_at=execution_expires_at,
                )
                if created_event is not None:
                    session.flush()
                    event_row = session.execute(
                        select(ExecutionIntentEventModel)
                        .where(
                            ExecutionIntentEventModel.execution_intent_id == str(created_event["execution_intent_id"]),
                            ExecutionIntentEventModel.event_type == "created",
                        )
                        .order_by(ExecutionIntentEventModel.execution_intent_event_id.asc())
                        .limit(1)
                    ).scalar_one_or_none()
                    if event_row is None:
                        event_row = ExecutionIntentEventModel(
                            execution_intent_id=str(created_event["execution_intent_id"]),
                            event_type=str(created_event["event_type"]),
                            event_at=event_at,
                            payload_json=render_value(dict(created_event.get("payload") or {})),
                        )
                        session.add(event_row)
            session.flush()
            for row in (trade_intent_row, admission_row, execution_row, event_row):
                if row is not None:
                    session.refresh(row)
            return {
                "trade_intent": self.row(trade_intent_row),
                "admission": self.row(admission_row),
                "execution_intent": None if execution_row is None else self.row(execution_row),
                "created_event": None if event_row is None else self.row(event_row),
            }

    def create_terminal_repair_execution_intent_if_missing(
        self,
        *,
        execution_intent: dict[str, Any],
        created_event: dict[str, Any],
        terminal_state: str,
        terminal_payload_updates: dict[str, Any],
        terminal_event: dict[str, Any],
        terminal_updated_at: str,
    ) -> dict[str, Any]:
        created_at = parse_datetime(execution_intent.get("created_at"))
        updated_at = parse_datetime(execution_intent.get("updated_at"))
        expires_at = parse_datetime(execution_intent.get("expires_at"))
        created_event_at = parse_datetime(created_event.get("event_at"))
        terminal_event_at = parse_datetime(terminal_event.get("event_at"))
        terminal_updated_at_dt = parse_datetime(terminal_updated_at)
        if created_at is None or updated_at is None or terminal_updated_at_dt is None:
            raise ValueError("execution intent repair timestamps are required")
        if created_event_at is None or terminal_event_at is None:
            raise ValueError("execution intent repair event_at values are required")

        values = {
            "execution_intent_id": str(execution_intent["execution_intent_id"]),
            "trading_strategy_id": str(execution_intent["trading_strategy_id"]),
            "trade_signal_id": execution_intent.get("trade_signal_id"),
            "trade_decision_id": execution_intent.get("trade_decision_id"),
            "strategy_position_id": execution_intent.get("strategy_position_id"),
            "execution_attempt_id": execution_intent.get("execution_attempt_id"),
            "action_type": str(execution_intent["action_type"]),
            "slot_key": str(execution_intent["slot_key"]),
            "claim_token": execution_intent.get("claim_token"),
            "policy_ref_json": render_value(dict(execution_intent.get("policy_ref") or {})),
            "config_hash": str(execution_intent.get("config_hash") or ""),
            "state": str(execution_intent["state"]),
            "expires_at": expires_at,
            "superseded_by_id": execution_intent.get("superseded_by_id"),
            "payload_json": render_value(dict(execution_intent.get("payload") or {})),
            "created_at": created_at,
            "updated_at": updated_at,
        }
        with self.session_scope() as session:
            statement = (
                pg_insert(ExecutionIntentModel)
                .values(**values)
                .on_conflict_do_nothing(index_elements=["execution_intent_id"])
                .returning(ExecutionIntentModel.execution_intent_id)
            )
            inserted_id = session.execute(statement).scalar_one_or_none()
            if inserted_id is None:
                existing_row = session.get(ExecutionIntentModel, str(execution_intent["execution_intent_id"]))
                return {
                    "created": False,
                    "execution_intent": None if existing_row is None else self.row(existing_row),
                    "terminal_event": None,
                }

            created_event_row = ExecutionIntentEventModel(
                execution_intent_id=str(created_event["execution_intent_id"]),
                event_type=str(created_event["event_type"]),
                event_at=created_event_at,
                payload_json=render_value(dict(created_event.get("payload") or {})),
            )
            session.add(created_event_row)
            execution_row = session.get(ExecutionIntentModel, str(execution_intent["execution_intent_id"]))
            if execution_row is None:
                raise ValueError(f"Execution intent repair insert vanished: {execution_intent['execution_intent_id']}")
            payload = dict(execution_row.payload_json or {})
            payload.update(terminal_payload_updates)
            execution_row.state = terminal_state
            execution_row.payload_json = render_value(payload)
            execution_row.updated_at = terminal_updated_at_dt
            terminal_event_row = ExecutionIntentEventModel(
                execution_intent_id=str(terminal_event["execution_intent_id"]),
                event_type=str(terminal_event["event_type"]),
                event_at=terminal_event_at,
                payload_json=render_value(dict(terminal_event.get("payload") or {})),
            )
            session.add(terminal_event_row)
            session.flush()
            session.refresh(execution_row)
            session.refresh(created_event_row)
            session.refresh(terminal_event_row)
            return {
                "created": True,
                "execution_intent": self.row(execution_row),
                "created_event": self.row(created_event_row),
                "terminal_event": self.row(terminal_event_row),
            }

    def _upsert_execution_intent_model(
        self,
        session: Any,
        *,
        execution_intent: dict[str, Any],
        created_at: Any,
        updated_at: Any,
        expires_at: Any,
    ) -> ExecutionIntentModel:
        row = session.get(ExecutionIntentModel, str(execution_intent["execution_intent_id"]))
        if row is None:
            row = ExecutionIntentModel(
                execution_intent_id=str(execution_intent["execution_intent_id"]),
                created_at=created_at,
                trading_strategy_id=str(execution_intent["trading_strategy_id"]),
                trade_signal_id=execution_intent.get("trade_signal_id"),
                trade_decision_id=execution_intent.get("trade_decision_id"),
                strategy_position_id=execution_intent.get("strategy_position_id"),
                execution_attempt_id=execution_intent.get("execution_attempt_id"),
                action_type=str(execution_intent["action_type"]),
                slot_key=str(execution_intent["slot_key"]),
                claim_token=execution_intent.get("claim_token"),
                policy_ref_json=render_value(dict(execution_intent.get("policy_ref") or {})),
                config_hash=str(execution_intent.get("config_hash") or ""),
                state=str(execution_intent["state"]),
                expires_at=expires_at,
                superseded_by_id=execution_intent.get("superseded_by_id"),
                payload_json=render_value(dict(execution_intent.get("payload") or {})),
                updated_at=updated_at,
            )
            session.add(row)
        else:
            row.trading_strategy_id = str(execution_intent["trading_strategy_id"])
            row.trade_signal_id = execution_intent.get("trade_signal_id")
            row.trade_decision_id = execution_intent.get("trade_decision_id")
            row.strategy_position_id = execution_intent.get("strategy_position_id")
            row.execution_attempt_id = execution_intent.get("execution_attempt_id")
            row.action_type = str(execution_intent["action_type"])
            row.slot_key = str(execution_intent["slot_key"])
            row.claim_token = execution_intent.get("claim_token")
            row.policy_ref_json = render_value(dict(execution_intent.get("policy_ref") or {}))
            row.config_hash = str(execution_intent.get("config_hash") or "")
            row.state = str(execution_intent["state"])
            row.expires_at = expires_at
            row.superseded_by_id = execution_intent.get("superseded_by_id")
            row.payload_json = render_value(dict(execution_intent.get("payload") or {}))
            row.updated_at = updated_at
        return row

    def upsert_execution_intent(
        self,
        *,
        execution_intent_id: str,
        trading_strategy_id: str,
        strategy_position_id: str | None,
        execution_attempt_id: str | None,
        action_type: str,
        slot_key: str,
        claim_token: str | None,
        policy_ref: dict[str, Any],
        config_hash: str,
        state: str,
        expires_at: str | None,
        superseded_by_id: str | None,
        payload: dict[str, Any] | None,
        created_at: str,
        updated_at: str,
        trade_signal_id: str | None = None,
        trade_decision_id: str | None = None,
    ) -> ExecutionIntentRecord:
        created_at_dt = parse_datetime(created_at)
        updated_at_dt = parse_datetime(updated_at)
        expires_at_dt = parse_datetime(expires_at)
        if created_at_dt is None or updated_at_dt is None:
            raise ValueError("created_at and updated_at are required")
        with self.session_scope() as session:
            row = self._upsert_execution_intent_model(
                session,
                execution_intent={
                    "execution_intent_id": execution_intent_id,
                    "trading_strategy_id": trading_strategy_id,
                    "trade_signal_id": trade_signal_id,
                    "trade_decision_id": trade_decision_id,
                    "strategy_position_id": strategy_position_id,
                    "execution_attempt_id": execution_attempt_id,
                    "action_type": action_type,
                    "slot_key": slot_key,
                    "claim_token": claim_token,
                    "policy_ref": policy_ref,
                    "config_hash": config_hash,
                    "state": state,
                    "superseded_by_id": superseded_by_id,
                    "payload": payload,
                },
                created_at=created_at_dt,
                updated_at=updated_at_dt,
                expires_at=expires_at_dt,
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
        strategy_position_id: str | None = None,
        slot_key: str | None = None,
        states: list[str] | None = None,
        execution_attempt_id: str | None = None,
        limit: int = 200,
    ) -> list[ExecutionIntentRecord]:
        statement = select(ExecutionIntentModel)
        if trading_strategy_id:
            statement = statement.where(ExecutionIntentModel.trading_strategy_id == trading_strategy_id)
        if trade_signal_id:
            statement = statement.where(ExecutionIntentModel.trade_signal_id == trade_signal_id)
        if trade_decision_id:
            statement = statement.where(ExecutionIntentModel.trade_decision_id == trade_decision_id)
        if strategy_position_id:
            statement = statement.where(ExecutionIntentModel.strategy_position_id == strategy_position_id)
        if slot_key:
            statement = statement.where(ExecutionIntentModel.slot_key == slot_key)
        if states:
            statement = statement.where(ExecutionIntentModel.state.in_(states))
        if execution_attempt_id:
            statement = statement.where(ExecutionIntentModel.execution_attempt_id == execution_attempt_id)
        statement = statement.order_by(
            ExecutionIntentModel.created_at.desc(),
            ExecutionIntentModel.execution_intent_id.asc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_approved_admissions_missing_execution_intents(
        self,
        *,
        session_date: str | date | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        statement = (
            select(TradeAdmissionModel)
            .options(
                selectinload(TradeAdmissionModel.decision),
                selectinload(TradeAdmissionModel.trade_intent),
            )
            .outerjoin(
                ExecutionIntentModel,
                ExecutionIntentModel.execution_intent_id == TradeAdmissionModel.execution_intent_id,
            )
            .where(TradeAdmissionModel.admission_state == "approved")
            .where(ExecutionIntentModel.execution_intent_id.is_(None))
            .order_by(
                TradeAdmissionModel.decided_at.asc(),
                TradeAdmissionModel.admission_decision_id.asc(),
            )
            .limit(max(int(limit), 1))
        )
        if session_date is not None:
            statement = statement.where(TradeAdmissionModel.session_date == parse_date(session_date))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [
            {
                "admission": self.row(admission),
                "decision": None if admission.decision is None else self.row(admission.decision),
                "trade_intent": None if admission.trade_intent is None else self.row(admission.trade_intent),
            }
            for admission in rows
        ]

    def append_execution_intent_event(
        self,
        *,
        execution_intent_id: str,
        event_type: str,
        event_at: str,
        payload: dict[str, Any] | None = None,
    ) -> ExecutionIntentEventRecord:
        event_at_dt = parse_datetime(event_at)
        if event_at_dt is None:
            raise ValueError("event_at is required")
        with self.session_scope() as session:
            row = ExecutionIntentEventModel(
                execution_intent_id=execution_intent_id,
                event_type=event_type,
                event_at=event_at_dt,
                payload_json=dict(payload or {}),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)
