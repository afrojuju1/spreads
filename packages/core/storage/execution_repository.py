from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.services.option_structures import (
    common_expiration_date,
    legs_identity_key,
    normalize_legs,
    normalize_strategy_family,
    primary_short_long_symbols,
    structure_symbol_path,
)
from core.storage.base import RepositoryBase
from core.storage.execution_models import (
    ExecutionAttemptModel,
    ExecutionFillModel,
    ExecutionIntentEventModel,
    ExecutionIntentModel,
    ExecutionOrderModel,
    PortfolioPositionModel,
    PositionCloseModel,
)
from core.storage.lifecycle_models import (
    TradeAdmissionModel,
    TradeDecisionModel,
    TradeExecutionIntentModel,
)
from core.storage.records import (
    ExecutionAttemptRecord,
    ExecutionFillRecord,
    ExecutionIntentEventRecord,
    ExecutionIntentRecord,
    ExecutionOrderRecord,
    PortfolioPositionRecord,
    PositionCloseRecord,
)
from core.storage.read_models import ExecutionAttemptActivityRead
from core.storage.serializers import parse_date, parse_datetime, render_value


def _optional_date(value: str | None) -> Any:
    if value in (None, ""):
        return None
    return parse_date(value)


class ExecutionRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("execution_attempts", "execution_orders", "execution_fills")

    def intent_schema_ready(self) -> bool:
        return self.schema_has_tables("execution_intents", "execution_intent_events", "execution_attempts")

    def positions_schema_ready(self) -> bool:
        return self.portfolio_schema_ready()

    def portfolio_schema_ready(self) -> bool:
        return self.schema_has_tables("execution_attempts", "portfolio_positions", "position_closes")

    def _attempt_extra(self, row: ExecutionAttemptModel | None) -> dict[str, Any]:
        if row is None:
            return {
                "short_symbol": None,
                "long_symbol": None,
                "symbol_path": None,
            }
        legs = list(row.legs_json or [])
        short_symbol, long_symbol = primary_short_long_symbols(legs)
        return {
            "short_symbol": short_symbol,
            "long_symbol": long_symbol,
            "symbol_path": structure_symbol_path(legs),
        }

    def _attempt_row(self, row: ExecutionAttemptModel) -> ExecutionAttemptRecord:
        return self.row(row, extra=self._attempt_extra(row))

    def _attempt_rows(
        self,
        rows: list[ExecutionAttemptModel],
    ) -> list[ExecutionAttemptRecord]:
        return [self._attempt_row(row) for row in rows]

    def create_attempt(
        self,
        *,
        execution_attempt_id: str,
        session_id: str,
        session_date: str,
        label: str,
        trading_strategy_id: str | None = None,
        market_date: str | None = None,
        cycle_id: str | None,
        attempt_context: str | None,
        candidate_generated_at: str | None,
        run_id: str | None,
        job_run_id: str | None,
        underlying_symbol: str,
        strategy: str,
        expiration_date: str | None,
        structure_identity: str | None = None,
        legs: list[dict[str, Any]] | None = None,
        order_payload: dict[str, Any] | None = None,
        economics: dict[str, Any] | None = None,
        trade_intent: str,
        position_id: str | None = None,
        root_symbol: str | None = None,
        strategy_family: str | None = None,
        style_profile: str | None = None,
        horizon_intent: str | None = None,
        product_class: str | None = None,
        quantity: int,
        limit_price: float,
        requested_at: str,
        status: str,
        broker: str,
        request: dict[str, Any],
        candidate: dict[str, Any],
        broker_order_id: str | None = None,
        client_order_id: str | None = None,
        submitted_at: str | None = None,
        completed_at: str | None = None,
        error_text: str | None = None,
        source_object_type: str | None = None,
        source_object_id: str | None = None,
        trade_signal_id: str | None = None,
        trade_decision_id: str | None = None,
        admission_decision_id: str | None = None,
    ) -> ExecutionAttemptRecord:
        resolved_legs = normalize_legs(legs, expiration_date=expiration_date)
        if not resolved_legs:
            raise ValueError("Execution attempt requires canonical legs")
        resolved_expiration_date = common_expiration_date(resolved_legs) or expiration_date
        resolved_strategy_family = normalize_strategy_family(strategy_family or strategy)
        resolved_structure_identity = structure_identity
        if resolved_structure_identity is None and resolved_legs:
            resolved_structure_identity = legs_identity_key(
                strategy=resolved_strategy_family,
                legs=resolved_legs,
            )
        if resolved_structure_identity is None:
            raise ValueError("Execution attempt requires structure identity")
        with self.session_scope() as session:
            row = ExecutionAttemptModel(
                execution_attempt_id=execution_attempt_id,
                session_id=session_id,
                session_date=parse_date(session_date),
                label=label,
                trading_strategy_id=trading_strategy_id,
                market_date=parse_date(market_date or session_date),
                cycle_id=cycle_id,
                source_object_type=source_object_type,
                source_object_id=source_object_id,
                trade_signal_id=trade_signal_id,
                trade_decision_id=trade_decision_id,
                admission_decision_id=admission_decision_id,
                attempt_context=attempt_context,
                candidate_generated_at=parse_datetime(candidate_generated_at),
                run_id=run_id,
                job_run_id=job_run_id,
                underlying_symbol=underlying_symbol,
                strategy=strategy,
                expiration_date=_optional_date(resolved_expiration_date),
                structure_identity=resolved_structure_identity,
                trade_intent=trade_intent,
                position_id=position_id,
                root_symbol=root_symbol or underlying_symbol,
                strategy_family=resolved_strategy_family,
                style_profile=style_profile,
                horizon_intent=horizon_intent,
                product_class=product_class,
                requested_quantity=int(quantity),
                requested_limit_price=float(limit_price),
                quantity=int(quantity),
                limit_price=float(limit_price),
                requested_at=parse_datetime(requested_at),
                submitted_at=parse_datetime(submitted_at),
                completed_at=parse_datetime(completed_at),
                status=status,
                broker=broker,
                broker_order_id=broker_order_id,
                client_order_id=client_order_id,
                request_json=request,
                candidate_json=candidate,
                legs_json=list(resolved_legs),
                order_payload_json=dict(order_payload or {}),
                economics_json=dict(economics or {}),
                error_text=error_text,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self._attempt_row(row)

    def get_attempt(self, execution_attempt_id: str) -> ExecutionAttemptRecord | None:
        with self.session_factory() as session:
            row = session.get(ExecutionAttemptModel, execution_attempt_id)
        if row is None:
            return None
        return self._attempt_row(row)

    def get_attempt_activity(self, execution_attempt_id: str) -> ExecutionAttemptActivityRead | None:
        with self.session_factory() as session:
            row = session.get(ExecutionAttemptModel, execution_attempt_id)
            if row is None:
                return None
            orders = session.scalars(
                select(ExecutionOrderModel)
                .where(ExecutionOrderModel.execution_attempt_id == execution_attempt_id)
                .order_by(
                    ExecutionOrderModel.updated_at.desc(),
                    ExecutionOrderModel.execution_order_id.desc(),
                )
            ).all()
            fills = session.scalars(
                select(ExecutionFillModel)
                .where(ExecutionFillModel.execution_attempt_id == execution_attempt_id)
                .order_by(
                    ExecutionFillModel.filled_at.desc(),
                    ExecutionFillModel.execution_fill_id.desc(),
                )
            ).all()
            return ExecutionAttemptActivityRead.from_rows(
                attempt=self._attempt_row(row),
                orders=self.rows(orders),
                fills=self.rows(fills),
            )

    def list_attempts(
        self,
        *,
        session_id: str,
        limit: int = 50,
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.session_id == session_id)
            .order_by(
                ExecutionAttemptModel.requested_at.desc(),
                ExecutionAttemptModel.execution_attempt_id.desc(),
            )
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self._attempt_rows(rows)

    def list_attempts_for_market_date(
        self,
        *,
        market_date: str,
        limit: int = 500,
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.market_date == parse_date(market_date))
            .order_by(
                ExecutionAttemptModel.requested_at.desc(),
                ExecutionAttemptModel.execution_attempt_id.desc(),
            )
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self._attempt_rows(rows)

    def list_session_attempts_by_status(
        self,
        *,
        session_id: str,
        statuses: list[str],
        trade_intent: str | None = None,
        limit: int = 200,
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel).where(ExecutionAttemptModel.session_id == session_id).where(ExecutionAttemptModel.status.in_(statuses))
        )
        if trade_intent is not None:
            statement = statement.where(ExecutionAttemptModel.trade_intent == trade_intent)
        statement = statement.order_by(
            ExecutionAttemptModel.requested_at.desc(),
            ExecutionAttemptModel.execution_attempt_id.desc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self._attempt_rows(rows)

    def list_attempts_by_status(
        self,
        *,
        statuses: list[str],
        trade_intent: str | None = None,
        limit: int = 200,
    ) -> list[ExecutionAttemptRecord]:
        statement = select(ExecutionAttemptModel).where(ExecutionAttemptModel.status.in_(statuses))
        if trade_intent is not None:
            statement = statement.where(ExecutionAttemptModel.trade_intent == trade_intent)
        statement = statement.order_by(
            ExecutionAttemptModel.requested_at.desc(),
            ExecutionAttemptModel.execution_attempt_id.desc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self._attempt_rows(rows)

    def list_open_attempts_for_identity(
        self,
        *,
        session_id: str,
        strategy: str,
        structure_identity: str,
        statuses: list[str],
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.session_id == session_id)
            .where(ExecutionAttemptModel.strategy == strategy)
            .where(ExecutionAttemptModel.trade_intent == "open")
            .where(ExecutionAttemptModel.status.in_(statuses))
            .where(ExecutionAttemptModel.structure_identity == structure_identity)
            .order_by(ExecutionAttemptModel.requested_at.desc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self._attempt_rows(rows)

    def list_open_attempts_for_position(
        self,
        *,
        position_id: str,
        statuses: list[str],
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.position_id == position_id)
            .where(ExecutionAttemptModel.trade_intent == "close")
            .where(ExecutionAttemptModel.status.in_(statuses))
            .order_by(ExecutionAttemptModel.requested_at.desc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self._attempt_rows(rows)

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
        legacy_intent: dict[str, Any],
        admission: dict[str, Any],
        execution_intent: dict[str, Any] | None = None,
        created_event: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        legacy_created_at = parse_datetime(legacy_intent.get("created_at"))
        legacy_updated_at = parse_datetime(legacy_intent.get("updated_at"))
        legacy_claimed_at = parse_datetime(legacy_intent.get("claimed_at"))
        legacy_expires_at = parse_datetime(legacy_intent.get("expires_at"))
        admission_decided_at = parse_datetime(admission.get("decided_at"))
        if legacy_created_at is None or legacy_updated_at is None:
            raise ValueError("legacy intent created_at and updated_at are required")
        if admission_decided_at is None:
            raise ValueError("admission decided_at is required")

        execution_created_at = None if execution_intent is None else parse_datetime(execution_intent.get("created_at"))
        execution_updated_at = None if execution_intent is None else parse_datetime(execution_intent.get("updated_at"))
        execution_expires_at = None if execution_intent is None else parse_datetime(execution_intent.get("expires_at"))
        event_at = None if created_event is None else parse_datetime(created_event.get("event_at"))
        if execution_intent is not None and (execution_created_at is None or execution_updated_at is None):
            raise ValueError("execution intent created_at and updated_at are required")
        if created_event is not None and event_at is None:
            raise ValueError("created event event_at is required")

        with self.session_scope() as session:
            legacy_row = session.get(TradeExecutionIntentModel, str(legacy_intent["execution_intent_id"]))
            if legacy_row is None:
                legacy_row = TradeExecutionIntentModel(
                    execution_intent_id=str(legacy_intent["execution_intent_id"]),
                    created_at=legacy_created_at,
                    intent_kind=str(legacy_intent["intent_kind"]),
                    source_object_type=str(legacy_intent["source_object_type"]),
                    source_object_id=str(legacy_intent["source_object_id"]),
                    trade_signal_id=legacy_intent.get("trade_signal_id"),
                    trade_decision_id=legacy_intent.get("trade_decision_id"),
                    position_id=legacy_intent.get("position_id"),
                    trading_strategy_id=legacy_intent.get("trading_strategy_id"),
                    trade_structure=legacy_intent.get("trade_structure"),
                    routine=legacy_intent.get("routine"),
                    account_id=legacy_intent.get("account_id"),
                    slot_key=str(legacy_intent["slot_key"]),
                    idempotency_key=str(legacy_intent["idempotency_key"]),
                    intent_state=str(legacy_intent["intent_state"]),
                    claim_token=legacy_intent.get("claim_token"),
                    claimed_at=legacy_claimed_at,
                    expires_at=legacy_expires_at,
                    supersedes_intent_id=legacy_intent.get("supersedes_intent_id"),
                    superseded_by_intent_id=legacy_intent.get("superseded_by_intent_id"),
                    payload_json=render_value(dict(legacy_intent.get("payload") or {})),
                    policy_snapshot_json=render_value(dict(legacy_intent.get("policy_snapshot") or {})),
                    config_hash=legacy_intent.get("config_hash"),
                    updated_at=legacy_updated_at,
                )
                session.add(legacy_row)
            else:
                legacy_row.intent_kind = str(legacy_intent["intent_kind"])
                legacy_row.source_object_type = str(legacy_intent["source_object_type"])
                legacy_row.source_object_id = str(legacy_intent["source_object_id"])
                legacy_row.trade_signal_id = legacy_intent.get("trade_signal_id")
                legacy_row.trade_decision_id = legacy_intent.get("trade_decision_id")
                legacy_row.position_id = legacy_intent.get("position_id")
                legacy_row.trading_strategy_id = legacy_intent.get("trading_strategy_id")
                legacy_row.trade_structure = legacy_intent.get("trade_structure")
                legacy_row.routine = legacy_intent.get("routine")
                legacy_row.account_id = legacy_intent.get("account_id")
                legacy_row.slot_key = str(legacy_intent["slot_key"])
                legacy_row.idempotency_key = str(legacy_intent["idempotency_key"])
                legacy_row.intent_state = str(legacy_intent["intent_state"])
                legacy_row.claim_token = legacy_intent.get("claim_token")
                legacy_row.claimed_at = legacy_claimed_at
                legacy_row.expires_at = legacy_expires_at
                legacy_row.supersedes_intent_id = legacy_intent.get("supersedes_intent_id")
                legacy_row.superseded_by_intent_id = legacy_intent.get("superseded_by_intent_id")
                legacy_row.payload_json = render_value(dict(legacy_intent.get("payload") or {}))
                legacy_row.policy_snapshot_json = render_value(dict(legacy_intent.get("policy_snapshot") or {}))
                legacy_row.config_hash = legacy_intent.get("config_hash")
                legacy_row.updated_at = legacy_updated_at

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
                    event_row = ExecutionIntentEventModel(
                        execution_intent_id=str(created_event["execution_intent_id"]),
                        event_type=str(created_event["event_type"]),
                        event_at=event_at,
                        payload_json=render_value(dict(created_event.get("payload") or {})),
                    )
                    session.add(event_row)
            session.flush()
            for row in (legacy_row, admission_row, execution_row, event_row):
                if row is not None:
                    session.refresh(row)
            return {
                "legacy_intent": self.row(legacy_row),
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
            select(TradeAdmissionModel, TradeDecisionModel, TradeExecutionIntentModel)
            .outerjoin(
                TradeDecisionModel,
                TradeDecisionModel.trade_decision_id == TradeAdmissionModel.trade_decision_id,
            )
            .outerjoin(
                TradeExecutionIntentModel,
                TradeExecutionIntentModel.execution_intent_id == TradeAdmissionModel.execution_intent_id,
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
            rows = session.execute(statement).all()
        return [
            {
                "admission": self.row(admission),
                "decision": None if decision is None else self.row(decision),
                "legacy_intent": None if legacy_intent is None else self.row(legacy_intent),
            }
            for admission, decision, legacy_intent in rows
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

    def update_attempt(
        self,
        *,
        execution_attempt_id: str,
        status: str | None = None,
        broker_order_id: str | None = None,
        client_order_id: str | None = None,
        submitted_at: str | None = None,
        completed_at: str | None = None,
        error_text: str | None = None,
        position_id: str | None = None,
    ) -> ExecutionAttemptRecord:
        with self.session_scope() as session:
            row = session.get(ExecutionAttemptModel, execution_attempt_id)
            if row is None:
                raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
            if status is not None:
                row.status = status
            if broker_order_id is not None:
                row.broker_order_id = broker_order_id
            if client_order_id is not None:
                row.client_order_id = client_order_id
            if submitted_at is not None:
                row.submitted_at = parse_datetime(submitted_at)
            if completed_at is not None:
                row.completed_at = parse_datetime(completed_at)
            if position_id is not None:
                row.position_id = position_id
            if error_text is not None or (status == "failed"):
                row.error_text = error_text
            elif status is not None and status != "failed":
                row.error_text = None
            session.flush()
            session.refresh(row)
            return self._attempt_row(row)

    def list_orders(
        self,
        *,
        execution_attempt_ids: list[str] | None = None,
        execution_attempt_id: str | None = None,
    ) -> list[ExecutionOrderRecord]:
        statement = select(ExecutionOrderModel)
        if execution_attempt_id is not None:
            statement = statement.where(ExecutionOrderModel.execution_attempt_id == execution_attempt_id)
        elif execution_attempt_ids:
            statement = statement.where(ExecutionOrderModel.execution_attempt_id.in_(execution_attempt_ids))
        statement = statement.order_by(
            ExecutionOrderModel.updated_at.desc(),
            ExecutionOrderModel.execution_order_id.desc(),
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_orders_by_broker_order_ids(
        self,
        broker_order_ids: list[str],
    ) -> list[ExecutionOrderRecord]:
        if not broker_order_ids:
            return []
        statement = (
            select(ExecutionOrderModel)
            .where(ExecutionOrderModel.broker_order_id.in_(broker_order_ids))
            .order_by(
                ExecutionOrderModel.updated_at.desc(),
                ExecutionOrderModel.execution_order_id.desc(),
            )
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def upsert_orders(
        self,
        *,
        execution_attempt_id: str,
        rows: list[dict[str, Any]],
    ) -> list[ExecutionOrderRecord]:
        if not rows:
            return []
        broker_order_ids = [str(row["broker_order_id"]) for row in rows]
        with self.session_scope() as session:
            existing_rows = session.scalars(select(ExecutionOrderModel).where(ExecutionOrderModel.broker_order_id.in_(broker_order_ids))).all()
            existing_by_order_id = {row.broker_order_id: row for row in existing_rows}
            persisted: list[ExecutionOrderModel] = []
            for payload in rows:
                broker_order_id = str(payload["broker_order_id"])
                row = existing_by_order_id.get(broker_order_id)
                if row is None:
                    row = ExecutionOrderModel(
                        execution_attempt_id=execution_attempt_id,
                        broker_order_id=broker_order_id,
                    )
                    session.add(row)
                row.execution_attempt_id = execution_attempt_id
                row.broker = str(payload.get("broker") or "alpaca")
                row.parent_broker_order_id = payload.get("parent_broker_order_id")
                row.client_order_id = payload.get("client_order_id")
                row.order_status = str(payload["order_status"])
                row.order_type = payload.get("order_type")
                row.time_in_force = payload.get("time_in_force")
                row.order_class = payload.get("order_class")
                row.side = payload.get("side")
                row.symbol = payload.get("symbol")
                row.leg_symbol = payload.get("leg_symbol")
                row.leg_side = payload.get("leg_side")
                row.position_intent = payload.get("position_intent")
                row.quantity = payload.get("quantity")
                row.limit_price = payload.get("limit_price")
                row.filled_qty = payload.get("filled_qty")
                row.filled_avg_price = payload.get("filled_avg_price")
                row.submitted_at = parse_datetime(payload.get("submitted_at"))
                row.updated_at = parse_datetime(payload.get("updated_at"))
                row.order_json = dict(payload.get("order") or {})
                persisted.append(row)
            session.flush()
            for row in persisted:
                session.refresh(row)
            return self.rows(persisted)

    def list_fills(
        self,
        *,
        execution_attempt_ids: list[str] | None = None,
        execution_attempt_id: str | None = None,
    ) -> list[ExecutionFillRecord]:
        statement = select(ExecutionFillModel)
        if execution_attempt_id is not None:
            statement = statement.where(ExecutionFillModel.execution_attempt_id == execution_attempt_id)
        elif execution_attempt_ids:
            statement = statement.where(ExecutionFillModel.execution_attempt_id.in_(execution_attempt_ids))
        statement = statement.order_by(
            ExecutionFillModel.filled_at.desc(),
            ExecutionFillModel.execution_fill_id.desc(),
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def upsert_fills(
        self,
        *,
        execution_attempt_id: str,
        rows: list[dict[str, Any]],
    ) -> list[ExecutionFillRecord]:
        if not rows:
            return []
        broker_fill_ids = [str(row["broker_fill_id"]) for row in rows]
        with self.session_scope() as session:
            existing_rows = session.scalars(select(ExecutionFillModel).where(ExecutionFillModel.broker_fill_id.in_(broker_fill_ids))).all()
            existing_by_fill_id = {row.broker_fill_id: row for row in existing_rows}
            persisted: list[ExecutionFillModel] = []
            for payload in rows:
                broker_fill_id = str(payload["broker_fill_id"])
                row = existing_by_fill_id.get(broker_fill_id)
                if row is None:
                    row = ExecutionFillModel(
                        execution_attempt_id=execution_attempt_id,
                        broker_fill_id=broker_fill_id,
                    )
                    session.add(row)
                row.execution_attempt_id = execution_attempt_id
                row.execution_order_id = payload.get("execution_order_id")
                row.broker = str(payload.get("broker") or "alpaca")
                row.broker_order_id = str(payload["broker_order_id"])
                row.symbol = str(payload["symbol"])
                row.side = payload.get("side")
                row.fill_type = payload.get("fill_type")
                row.quantity = float(payload["quantity"])
                row.cumulative_quantity = payload.get("cumulative_quantity")
                row.remaining_quantity = payload.get("remaining_quantity")
                row.price = payload.get("price")
                row.filled_at = parse_datetime(payload["filled_at"])
                row.fill_json = dict(payload.get("fill") or {})
                persisted.append(row)
            session.flush()
            for row in persisted:
                session.refresh(row)
            return self.rows(persisted)

    def get_position(self, position_id: str) -> PortfolioPositionRecord | None:
        with self.session_factory() as session:
            row = session.get(PortfolioPositionModel, position_id)
        if row is None:
            return None
        return self.row(row)

    def get_position_by_open_attempt(self, open_execution_attempt_id: str) -> PortfolioPositionRecord | None:
        statement = select(PortfolioPositionModel).where(PortfolioPositionModel.open_execution_attempt_id == open_execution_attempt_id)
        with self.session_factory() as session:
            row = session.scalars(statement).first()
        if row is None:
            return None
        return self.row(row)

    def list_positions(
        self,
        *,
        market_date: str | None = None,
        trading_strategy_id: str | None = None,
        statuses: list[str] | None = None,
        limit: int | None = None,
    ) -> list[PortfolioPositionRecord]:
        statement = select(PortfolioPositionModel)
        if market_date is not None:
            market_date_value = parse_date(market_date)
            statement = statement.where(PortfolioPositionModel.market_date_opened == market_date_value)
        if trading_strategy_id is not None:
            statement = statement.where(PortfolioPositionModel.trading_strategy_id == trading_strategy_id)
        if statuses:
            statement = statement.where(PortfolioPositionModel.status.in_(statuses))
        statement = statement.order_by(
            PortfolioPositionModel.updated_at.desc(),
            PortfolioPositionModel.position_id.desc(),
        )
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def create_position(
        self,
        *,
        position_id: str,
        trading_strategy_id: str | None,
        source_object_type: str | None,
        source_object_id: str | None,
        trade_signal_id: str | None,
        trade_decision_id: str | None,
        admission_decision_id: str | None,
        opening_execution_intent_id: str | None,
        open_execution_attempt_id: str,
        root_symbol: str,
        strategy_family: str,
        style_profile: str | None,
        horizon_intent: str | None,
        product_class: str | None,
        market_date_opened: str,
        market_date_closed: str | None,
        status: str,
        legs: list[dict[str, Any]],
        economics: dict[str, Any],
        strategy_metrics: dict[str, Any],
        requested_quantity: int,
        opened_quantity: float,
        remaining_quantity: float,
        entry_value: float | None,
        realized_pnl: float,
        unrealized_pnl: float | None,
        close_mark: float | None,
        close_mark_source: str | None,
        close_marked_at: str | None,
        last_broker_status: str | None,
        exit_policy: dict[str, Any],
        risk_policy: dict[str, Any],
        config_hash: str | None,
        source_job_type: str | None,
        source_job_key: str | None,
        source_job_run_id: str | None,
        last_exit_evaluated_at: str | None,
        last_exit_reason: str | None,
        last_reconciled_at: str | None,
        reconciliation_status: str | None,
        reconciliation_note: str | None,
        opened_at: str | None,
        closed_at: str | None,
        created_at: str,
        updated_at: str,
    ) -> PortfolioPositionRecord:
        with self.session_scope() as session:
            row = PortfolioPositionModel(
                position_id=position_id,
                trading_strategy_id=trading_strategy_id,
                source_object_type=source_object_type,
                source_object_id=source_object_id,
                trade_signal_id=trade_signal_id,
                trade_decision_id=trade_decision_id,
                admission_decision_id=admission_decision_id,
                opening_execution_intent_id=opening_execution_intent_id,
                open_execution_attempt_id=open_execution_attempt_id,
                root_symbol=root_symbol,
                strategy_family=strategy_family,
                style_profile=style_profile,
                horizon_intent=horizon_intent,
                product_class=product_class,
                market_date_opened=parse_date(market_date_opened),
                market_date_closed=_optional_date(market_date_closed),
                status=status,
                legs_json=list(legs),
                economics_json=dict(economics),
                strategy_metrics_json=dict(strategy_metrics),
                requested_quantity=int(requested_quantity),
                opened_quantity=float(opened_quantity),
                remaining_quantity=float(remaining_quantity),
                entry_value=entry_value,
                realized_pnl=float(realized_pnl),
                unrealized_pnl=unrealized_pnl,
                close_mark=close_mark,
                close_mark_source=close_mark_source,
                close_marked_at=parse_datetime(close_marked_at),
                last_broker_status=last_broker_status,
                exit_policy_json=dict(exit_policy),
                risk_policy_json=dict(risk_policy),
                config_hash=config_hash,
                source_job_type=source_job_type,
                source_job_key=source_job_key,
                source_job_run_id=source_job_run_id,
                last_exit_evaluated_at=parse_datetime(last_exit_evaluated_at),
                last_exit_reason=last_exit_reason,
                last_reconciled_at=parse_datetime(last_reconciled_at),
                reconciliation_status=reconciliation_status,
                reconciliation_note=reconciliation_note,
                opened_at=parse_datetime(opened_at),
                closed_at=parse_datetime(closed_at),
                created_at=parse_datetime(created_at),
                updated_at=parse_datetime(updated_at),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def update_position(
        self,
        *,
        position_id: str,
        trading_strategy_id: str | None = None,
        source_object_type: str | None = None,
        source_object_id: str | None = None,
        trade_signal_id: str | None = None,
        trade_decision_id: str | None = None,
        admission_decision_id: str | None = None,
        opening_execution_intent_id: str | None = None,
        root_symbol: str | None = None,
        strategy_family: str | None = None,
        style_profile: str | None = None,
        horizon_intent: str | None = None,
        product_class: str | None = None,
        market_date_opened: str | None = None,
        market_date_closed: str | None = None,
        status: str | None = None,
        legs: list[dict[str, Any]] | None = None,
        economics: dict[str, Any] | None = None,
        strategy_metrics: dict[str, Any] | None = None,
        requested_quantity: int | None = None,
        opened_quantity: float | None = None,
        remaining_quantity: float | None = None,
        entry_value: float | None = None,
        realized_pnl: float | None = None,
        unrealized_pnl: float | None = None,
        close_mark: float | None = None,
        close_mark_source: str | None = None,
        close_marked_at: str | None = None,
        last_broker_status: str | None = None,
        exit_policy: dict[str, Any] | None = None,
        risk_policy: dict[str, Any] | None = None,
        config_hash: str | None = None,
        source_job_type: str | None = None,
        source_job_key: str | None = None,
        source_job_run_id: str | None = None,
        last_exit_evaluated_at: str | None = None,
        last_exit_reason: str | None = None,
        last_reconciled_at: str | None = None,
        reconciliation_status: str | None = None,
        reconciliation_note: str | None = None,
        opened_at: str | None = None,
        closed_at: str | None = None,
        updated_at: str | None = None,
    ) -> PortfolioPositionRecord:
        with self.session_scope() as session:
            row = session.get(PortfolioPositionModel, position_id)
            if row is None:
                raise ValueError(f"Unknown position_id: {position_id}")
            if trading_strategy_id is not None:
                row.trading_strategy_id = trading_strategy_id
            if source_object_type is not None:
                row.source_object_type = source_object_type
            if source_object_id is not None:
                row.source_object_id = source_object_id
            if trade_signal_id is not None:
                row.trade_signal_id = trade_signal_id
            if trade_decision_id is not None:
                row.trade_decision_id = trade_decision_id
            if admission_decision_id is not None:
                row.admission_decision_id = admission_decision_id
            if opening_execution_intent_id is not None:
                row.opening_execution_intent_id = opening_execution_intent_id
            if root_symbol is not None:
                row.root_symbol = root_symbol
            if strategy_family is not None:
                row.strategy_family = strategy_family
            if style_profile is not None:
                row.style_profile = style_profile
            if horizon_intent is not None:
                row.horizon_intent = horizon_intent
            if product_class is not None:
                row.product_class = product_class
            if market_date_opened is not None:
                row.market_date_opened = parse_date(market_date_opened)
            if market_date_closed is not None:
                row.market_date_closed = _optional_date(market_date_closed)
            if status is not None:
                row.status = status
            if legs is not None:
                row.legs_json = list(legs)
            if economics is not None:
                row.economics_json = dict(economics)
            if strategy_metrics is not None:
                row.strategy_metrics_json = dict(strategy_metrics)
            if requested_quantity is not None:
                row.requested_quantity = int(requested_quantity)
            if opened_quantity is not None:
                row.opened_quantity = float(opened_quantity)
            if remaining_quantity is not None:
                row.remaining_quantity = float(remaining_quantity)
            if entry_value is not None:
                row.entry_value = entry_value
            if realized_pnl is not None:
                row.realized_pnl = float(realized_pnl)
            if unrealized_pnl is not None or close_mark is not None or close_mark_source is not None or close_marked_at is not None:
                row.unrealized_pnl = unrealized_pnl
            if close_mark is not None:
                row.close_mark = close_mark
            if close_mark_source is not None:
                row.close_mark_source = close_mark_source
            if close_marked_at is not None:
                row.close_marked_at = parse_datetime(close_marked_at)
            if last_broker_status is not None:
                row.last_broker_status = last_broker_status
            if exit_policy is not None:
                row.exit_policy_json = dict(exit_policy)
            if risk_policy is not None:
                row.risk_policy_json = dict(risk_policy)
            if config_hash is not None:
                row.config_hash = config_hash
            if source_job_type is not None:
                row.source_job_type = source_job_type
            if source_job_key is not None:
                row.source_job_key = source_job_key
            if source_job_run_id is not None:
                row.source_job_run_id = source_job_run_id
            if last_exit_evaluated_at is not None:
                row.last_exit_evaluated_at = parse_datetime(last_exit_evaluated_at)
            if last_exit_reason is not None:
                row.last_exit_reason = last_exit_reason
            if last_reconciled_at is not None:
                row.last_reconciled_at = parse_datetime(last_reconciled_at)
            if reconciliation_status is not None:
                row.reconciliation_status = reconciliation_status
                row.reconciliation_note = reconciliation_note
            elif reconciliation_note is not None:
                row.reconciliation_note = reconciliation_note
            if opened_at is not None:
                row.opened_at = parse_datetime(opened_at)
            if closed_at is not None:
                row.closed_at = parse_datetime(closed_at)
            row.updated_at = parse_datetime(updated_at) if updated_at is not None else row.updated_at
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_position_closes(
        self,
        *,
        position_ids: list[str] | None = None,
        position_id: str | None = None,
    ) -> list[PositionCloseRecord]:
        statement = select(PositionCloseModel)
        if position_id is not None:
            statement = statement.where(PositionCloseModel.position_id == position_id)
        elif position_ids:
            statement = statement.where(PositionCloseModel.position_id.in_(position_ids))
        statement = statement.order_by(
            PositionCloseModel.closed_at.desc(),
            PositionCloseModel.position_close_id.desc(),
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def upsert_position_close(
        self,
        *,
        position_id: str,
        execution_attempt_id: str,
        closed_quantity: float,
        exit_value: float | None,
        realized_pnl: float,
        broker_order_id: str | None,
        closed_at: str | None,
        created_at: str,
        updated_at: str,
    ) -> PositionCloseRecord:
        with self.session_scope() as session:
            statement = select(PositionCloseModel).where(PositionCloseModel.execution_attempt_id == execution_attempt_id)
            row = session.scalars(statement).first()
            if row is None:
                row = PositionCloseModel(
                    position_id=position_id,
                    execution_attempt_id=execution_attempt_id,
                    created_at=parse_datetime(created_at),
                )
                session.add(row)
            row.position_id = position_id
            row.closed_quantity = float(closed_quantity)
            row.exit_value = exit_value
            row.realized_pnl = float(realized_pnl)
            row.broker_order_id = broker_order_id
            row.closed_at = parse_datetime(closed_at)
            row.updated_at = parse_datetime(updated_at)
            session.flush()
            session.refresh(row)
            return self.row(row)
