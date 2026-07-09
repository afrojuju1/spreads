from __future__ import annotations

from typing import Any

from sqlalchemy import select

from core.storage.execution_models import ExecutionFillModel, ExecutionOrderModel
from core.storage.records import ExecutionFillRecord, ExecutionOrderRecord
from core.storage.serializers import parse_datetime


class ExecutionOrderRepositoryMixin:
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
