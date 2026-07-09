from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from core.services.option_structures import (
    common_expiration_date,
    legs_identity_key,
    normalize_legs,
    normalize_strategy_family,
)
from core.storage.execution.shared import _optional_date
from core.storage.execution_models import ExecutionAttemptModel
from core.storage.records import ExecutionAttemptRecord
from core.storage.read_models import ExecutionAttemptActivityRead
from core.storage.serializers import parse_date, parse_datetime


class ExecutionAttemptRepositoryMixin:
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
            row = session.scalars(
                select(ExecutionAttemptModel)
                .options(
                    selectinload(ExecutionAttemptModel.orders),
                    selectinload(ExecutionAttemptModel.fills),
                )
                .where(ExecutionAttemptModel.execution_attempt_id == execution_attempt_id)
            ).first()
            if row is None:
                return None
            return ExecutionAttemptActivityRead.from_rows(
                attempt=self._attempt_row(row),
                orders=self.rows(list(row.orders)),
                fills=self.rows(list(row.fills)),
            )

    def list_attempt_activities(
        self,
        *,
        session_id: str | None = None,
        execution_attempt_ids: list[str] | None = None,
        limit: int = 50,
    ) -> list[ExecutionAttemptActivityRead]:
        if session_id is None and not execution_attempt_ids:
            return []
        statement = select(ExecutionAttemptModel).options(
            selectinload(ExecutionAttemptModel.orders),
            selectinload(ExecutionAttemptModel.fills),
        )
        if session_id is not None:
            statement = statement.where(ExecutionAttemptModel.session_id == session_id)
        if execution_attempt_ids:
            statement = statement.where(ExecutionAttemptModel.execution_attempt_id.in_(execution_attempt_ids))
        statement = statement.order_by(
            ExecutionAttemptModel.requested_at.desc(),
            ExecutionAttemptModel.execution_attempt_id.desc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
            return [
                ExecutionAttemptActivityRead.from_rows(
                    attempt=self._attempt_row(row),
                    orders=self.rows(list(row.orders)),
                    fills=self.rows(list(row.fills)),
                )
                for row in rows
            ]

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
