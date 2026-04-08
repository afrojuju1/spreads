from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import inspect, select
from sqlalchemy.orm import Session

from spreads.storage.db import build_session_factory
from spreads.storage.execution_models import (
    ExecutionAttemptModel,
    ExecutionFillModel,
    ExecutionOrderModel,
)
from spreads.storage.records import (
    ExecutionAttemptRecord,
    ExecutionFillRecord,
    ExecutionOrderRecord,
)
from spreads.storage.serializers import (
    parse_date,
    parse_datetime,
    to_execution_attempt_record,
    to_execution_fill_record,
    to_execution_order_record,
)


class ExecutionRepository:
    def __init__(self, database_url: str) -> None:
        self.path = database_url
        self.engine, self.session_factory = build_session_factory(database_url)
        with self.session_factory() as session:
            session.execute(select(1))

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def schema_ready(self) -> bool:
        tables = set(inspect(self.engine).get_table_names(schema="public"))
        required = {"execution_attempts", "execution_orders", "execution_fills"}
        return required.issubset(tables)

    def create_attempt(
        self,
        *,
        execution_attempt_id: str,
        session_id: str,
        session_date: str,
        label: str,
        cycle_id: str | None,
        candidate_id: int | None,
        bucket: str | None,
        candidate_generated_at: str | None,
        run_id: str | None,
        job_run_id: str | None,
        underlying_symbol: str,
        strategy: str,
        expiration_date: str,
        short_symbol: str,
        long_symbol: str,
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
    ) -> ExecutionAttemptRecord:
        with self.session_scope() as session:
            row = ExecutionAttemptModel(
                execution_attempt_id=execution_attempt_id,
                session_id=session_id,
                session_date=parse_date(session_date),
                label=label,
                cycle_id=cycle_id,
                candidate_id=candidate_id,
                bucket=bucket,
                candidate_generated_at=parse_datetime(candidate_generated_at),
                run_id=run_id,
                job_run_id=job_run_id,
                underlying_symbol=underlying_symbol,
                strategy=strategy,
                expiration_date=parse_date(expiration_date),
                short_symbol=short_symbol,
                long_symbol=long_symbol,
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
                error_text=error_text,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return to_execution_attempt_record(row)

    def get_attempt(self, execution_attempt_id: str) -> ExecutionAttemptRecord | None:
        with self.session_factory() as session:
            row = session.get(ExecutionAttemptModel, execution_attempt_id)
        if row is None:
            return None
        return to_execution_attempt_record(row)

    def list_attempts(
        self,
        *,
        session_id: str,
        limit: int = 50,
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.session_id == session_id)
            .order_by(ExecutionAttemptModel.requested_at.desc(), ExecutionAttemptModel.execution_attempt_id.desc())
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_execution_attempt_record(row) for row in rows]

    def list_open_attempts_for_identity(
        self,
        *,
        session_id: str,
        strategy: str,
        short_symbol: str,
        long_symbol: str,
        statuses: list[str],
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.session_id == session_id)
            .where(ExecutionAttemptModel.strategy == strategy)
            .where(ExecutionAttemptModel.short_symbol == short_symbol)
            .where(ExecutionAttemptModel.long_symbol == long_symbol)
            .where(ExecutionAttemptModel.status.in_(statuses))
            .order_by(ExecutionAttemptModel.requested_at.desc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_execution_attempt_record(row) for row in rows]

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
            if error_text is not None or (status == "failed"):
                row.error_text = error_text
            elif status is not None and status != "failed":
                row.error_text = None
            session.flush()
            session.refresh(row)
            return to_execution_attempt_record(row)

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
        return [to_execution_order_record(row) for row in rows]

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
            existing_rows = session.scalars(
                select(ExecutionOrderModel).where(ExecutionOrderModel.broker_order_id.in_(broker_order_ids))
            ).all()
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
            return [to_execution_order_record(row) for row in persisted]

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
        return [to_execution_fill_record(row) for row in rows]

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
            existing_rows = session.scalars(
                select(ExecutionFillModel).where(ExecutionFillModel.broker_fill_id.in_(broker_fill_ids))
            ).all()
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
            return [to_execution_fill_record(row) for row in persisted]

    def close(self) -> None:
        self.engine.dispose()
