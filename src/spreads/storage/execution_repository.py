from __future__ import annotations

from typing import Any

from sqlalchemy import select

from spreads.storage.base import RepositoryBase
from spreads.storage.execution_models import (
    ExecutionAttemptModel,
    ExecutionFillModel,
    ExecutionOrderModel,
    PortfolioPositionModel,
    PositionCloseModel,
    SessionPositionCloseModel,
    SessionPositionModel,
)
from spreads.storage.records import (
    ExecutionAttemptRecord,
    ExecutionFillRecord,
    ExecutionOrderRecord,
    PortfolioPositionRecord,
    PositionCloseRecord,
    SessionPositionCloseRecord,
    SessionPositionRecord,
)
from spreads.storage.serializers import parse_date, parse_datetime


def _optional_date(value: str | None) -> Any:
    if value in (None, ""):
        return None
    return parse_date(value)


class ExecutionRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("execution_attempts", "execution_orders", "execution_fills")

    def positions_schema_ready(self) -> bool:
        return self.schema_has_tables("execution_attempts", "session_positions", "session_position_closes")

    def portfolio_schema_ready(self) -> bool:
        return self.schema_has_tables("execution_attempts", "portfolio_positions", "position_closes")

    def create_attempt(
        self,
        *,
        execution_attempt_id: str,
        session_id: str,
        session_date: str,
        label: str,
        pipeline_id: str | None = None,
        market_date: str | None = None,
        cycle_id: str | None,
        opportunity_id: str | None,
        risk_decision_id: str | None,
        candidate_id: int | None,
        attempt_context: str | None,
        candidate_generated_at: str | None,
        run_id: str | None,
        job_run_id: str | None,
        underlying_symbol: str,
        strategy: str,
        expiration_date: str,
        short_symbol: str,
        long_symbol: str,
        trade_intent: str,
        session_position_id: str | None,
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
    ) -> ExecutionAttemptRecord:
        with self.session_scope() as session:
            row = ExecutionAttemptModel(
                execution_attempt_id=execution_attempt_id,
                session_id=session_id,
                session_date=parse_date(session_date),
                label=label,
                pipeline_id=pipeline_id,
                market_date=parse_date(market_date or session_date),
                cycle_id=cycle_id,
                opportunity_id=opportunity_id,
                risk_decision_id=risk_decision_id,
                candidate_id=candidate_id,
                attempt_context=attempt_context,
                candidate_generated_at=parse_datetime(candidate_generated_at),
                run_id=run_id,
                job_run_id=job_run_id,
                underlying_symbol=underlying_symbol,
                strategy=strategy,
                expiration_date=parse_date(expiration_date),
                short_symbol=short_symbol,
                long_symbol=long_symbol,
                trade_intent=trade_intent,
                session_position_id=session_position_id,
                position_id=position_id,
                root_symbol=root_symbol or underlying_symbol,
                strategy_family=strategy_family or strategy,
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
                error_text=error_text,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_attempt(self, execution_attempt_id: str) -> ExecutionAttemptRecord | None:
        with self.session_factory() as session:
            row = session.get(ExecutionAttemptModel, execution_attempt_id)
        if row is None:
            return None
        return self.row(row)

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
        return self.rows(rows)

    def list_pipeline_attempts(
        self,
        *,
        pipeline_id: str,
        market_date: str | None = None,
        limit: int = 50,
    ) -> list[ExecutionAttemptRecord]:
        statement = select(ExecutionAttemptModel).where(ExecutionAttemptModel.pipeline_id == pipeline_id)
        if market_date is not None:
            statement = statement.where(ExecutionAttemptModel.market_date == parse_date(market_date))
        statement = statement.order_by(
            ExecutionAttemptModel.requested_at.desc(),
            ExecutionAttemptModel.execution_attempt_id.desc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_session_attempts_by_status(
        self,
        *,
        session_id: str,
        statuses: list[str],
        trade_intent: str | None = None,
        limit: int = 200,
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.session_id == session_id)
            .where(ExecutionAttemptModel.status.in_(statuses))
        )
        if trade_intent is not None:
            statement = statement.where(ExecutionAttemptModel.trade_intent == trade_intent)
        statement = statement.order_by(
            ExecutionAttemptModel.requested_at.desc(),
            ExecutionAttemptModel.execution_attempt_id.desc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

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
        return self.rows(rows)

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
            .where(ExecutionAttemptModel.trade_intent == "open")
            .where(ExecutionAttemptModel.status.in_(statuses))
            .order_by(ExecutionAttemptModel.requested_at.desc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_open_attempts_for_session_position(
        self,
        *,
        session_position_id: str,
        statuses: list[str],
    ) -> list[ExecutionAttemptRecord]:
        statement = (
            select(ExecutionAttemptModel)
            .where(ExecutionAttemptModel.session_position_id == session_position_id)
            .where(ExecutionAttemptModel.trade_intent == "close")
            .where(ExecutionAttemptModel.status.in_(statuses))
            .order_by(ExecutionAttemptModel.requested_at.desc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

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
        return self.rows(rows)

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
        session_position_id: str | None = None,
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
            if session_position_id is not None:
                row.session_position_id = session_position_id
            if position_id is not None:
                row.position_id = position_id
            if error_text is not None or (status == "failed"):
                row.error_text = error_text
            elif status is not None and status != "failed":
                row.error_text = None
            session.flush()
            session.refresh(row)
            return self.row(row)

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
            .order_by(ExecutionOrderModel.updated_at.desc(), ExecutionOrderModel.execution_order_id.desc())
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
            return self.rows(persisted)

    def get_session_position(self, session_position_id: str) -> SessionPositionRecord | None:
        with self.session_factory() as session:
            row = session.get(SessionPositionModel, session_position_id)
        if row is None:
            return None
        return self.row(row)

    def get_session_position_by_open_attempt(self, open_execution_attempt_id: str) -> SessionPositionRecord | None:
        statement = select(SessionPositionModel).where(
            SessionPositionModel.open_execution_attempt_id == open_execution_attempt_id
        )
        with self.session_factory() as session:
            row = session.scalars(statement).first()
        if row is None:
            return None
        return self.row(row)

    def list_session_positions(
        self,
        *,
        session_id: str | None = None,
        statuses: list[str] | None = None,
        limit: int | None = None,
    ) -> list[SessionPositionRecord]:
        statement = select(SessionPositionModel)
        if session_id is not None:
            statement = statement.where(SessionPositionModel.session_id == session_id)
        if statuses:
            statement = statement.where(SessionPositionModel.status.in_(statuses))
        statement = statement.order_by(SessionPositionModel.updated_at.desc(), SessionPositionModel.session_position_id.desc())
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def create_session_position(
        self,
        *,
        session_position_id: str,
        session_id: str,
        session_date: str,
        label: str,
        candidate_id: int | None,
        open_execution_attempt_id: str,
        underlying_symbol: str,
        strategy: str,
        expiration_date: str,
        short_symbol: str,
        long_symbol: str,
        requested_quantity: int,
        opened_quantity: float,
        remaining_quantity: float,
        entry_credit: float | None,
        entry_notional: float | None,
        width: float | None,
        max_profit: float | None,
        max_loss: float | None,
        opened_at: str | None,
        closed_at: str | None,
        status: str,
        realized_pnl: float,
        unrealized_pnl: float | None,
        close_mark: float | None,
        close_mark_source: str | None,
        close_marked_at: str | None,
        last_broker_status: str | None,
        exit_policy: dict[str, Any],
        risk_policy: dict[str, Any],
        source_job_type: str | None,
        source_job_key: str | None,
        source_job_run_id: str | None,
        last_exit_evaluated_at: str | None,
        last_exit_reason: str | None,
        last_reconciled_at: str | None,
        reconciliation_status: str | None,
        reconciliation_note: str | None,
        created_at: str,
        updated_at: str,
    ) -> SessionPositionRecord:
        with self.session_scope() as session:
            row = SessionPositionModel(
                session_position_id=session_position_id,
                session_id=session_id,
                session_date=parse_date(session_date),
                label=label,
                candidate_id=candidate_id,
                open_execution_attempt_id=open_execution_attempt_id,
                underlying_symbol=underlying_symbol,
                strategy=strategy,
                expiration_date=parse_date(expiration_date),
                short_symbol=short_symbol,
                long_symbol=long_symbol,
                requested_quantity=int(requested_quantity),
                opened_quantity=float(opened_quantity),
                remaining_quantity=float(remaining_quantity),
                entry_credit=entry_credit,
                entry_notional=entry_notional,
                width=width,
                max_profit=max_profit,
                max_loss=max_loss,
                opened_at=parse_datetime(opened_at),
                closed_at=parse_datetime(closed_at),
                status=status,
                realized_pnl=float(realized_pnl),
                unrealized_pnl=unrealized_pnl,
                close_mark=close_mark,
                close_mark_source=close_mark_source,
                close_marked_at=parse_datetime(close_marked_at),
                last_broker_status=last_broker_status,
                exit_policy_json=exit_policy,
                risk_policy_json=risk_policy,
                source_job_type=source_job_type,
                source_job_key=source_job_key,
                source_job_run_id=source_job_run_id,
                last_exit_evaluated_at=parse_datetime(last_exit_evaluated_at),
                last_exit_reason=last_exit_reason,
                last_reconciled_at=parse_datetime(last_reconciled_at),
                reconciliation_status=reconciliation_status,
                reconciliation_note=reconciliation_note,
                created_at=parse_datetime(created_at),
                updated_at=parse_datetime(updated_at),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def update_session_position(
        self,
        *,
        session_position_id: str,
        opened_quantity: float | None = None,
        remaining_quantity: float | None = None,
        entry_credit: float | None = None,
        entry_notional: float | None = None,
        width: float | None = None,
        max_profit: float | None = None,
        max_loss: float | None = None,
        opened_at: str | None = None,
        closed_at: str | None = None,
        status: str | None = None,
        realized_pnl: float | None = None,
        unrealized_pnl: float | None = None,
        close_mark: float | None = None,
        close_mark_source: str | None = None,
        close_marked_at: str | None = None,
        last_broker_status: str | None = None,
        exit_policy: dict[str, Any] | None = None,
        risk_policy: dict[str, Any] | None = None,
        source_job_type: str | None = None,
        source_job_key: str | None = None,
        source_job_run_id: str | None = None,
        last_exit_evaluated_at: str | None = None,
        last_exit_reason: str | None = None,
        last_reconciled_at: str | None = None,
        reconciliation_status: str | None = None,
        reconciliation_note: str | None = None,
        updated_at: str | None = None,
    ) -> SessionPositionRecord:
        with self.session_scope() as session:
            row = session.get(SessionPositionModel, session_position_id)
            if row is None:
                raise ValueError(f"Unknown session_position_id: {session_position_id}")
            if opened_quantity is not None:
                row.opened_quantity = float(opened_quantity)
            if remaining_quantity is not None:
                row.remaining_quantity = float(remaining_quantity)
            if entry_credit is not None:
                row.entry_credit = entry_credit
            if entry_notional is not None:
                row.entry_notional = entry_notional
            if width is not None:
                row.width = width
            if max_profit is not None:
                row.max_profit = max_profit
            if max_loss is not None:
                row.max_loss = max_loss
            if opened_at is not None:
                row.opened_at = parse_datetime(opened_at)
            if closed_at is not None:
                row.closed_at = parse_datetime(closed_at)
            if status is not None:
                row.status = status
            if realized_pnl is not None:
                row.realized_pnl = float(realized_pnl)
            if unrealized_pnl is not None or (close_mark is not None) or (close_mark_source is not None) or (close_marked_at is not None):
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
                row.exit_policy_json = exit_policy
            if risk_policy is not None:
                row.risk_policy_json = risk_policy
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
            row.updated_at = parse_datetime(updated_at) if updated_at is not None else row.updated_at
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_session_position_closes(
        self,
        *,
        session_position_ids: list[str] | None = None,
        session_position_id: str | None = None,
    ) -> list[SessionPositionCloseRecord]:
        statement = select(SessionPositionCloseModel)
        if session_position_id is not None:
            statement = statement.where(SessionPositionCloseModel.session_position_id == session_position_id)
        elif session_position_ids:
            statement = statement.where(SessionPositionCloseModel.session_position_id.in_(session_position_ids))
        statement = statement.order_by(
            SessionPositionCloseModel.closed_at.desc(),
            SessionPositionCloseModel.session_position_close_id.desc(),
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def upsert_session_position_close(
        self,
        *,
        session_position_id: str,
        execution_attempt_id: str,
        closed_quantity: float,
        exit_debit: float | None,
        realized_pnl: float,
        broker_order_id: str | None,
        closed_at: str | None,
        created_at: str,
        updated_at: str,
    ) -> SessionPositionCloseRecord:
        with self.session_scope() as session:
            statement = select(SessionPositionCloseModel).where(
                SessionPositionCloseModel.execution_attempt_id == execution_attempt_id
            )
            row = session.scalars(statement).first()
            if row is None:
                row = SessionPositionCloseModel(
                    session_position_id=session_position_id,
                    execution_attempt_id=execution_attempt_id,
                    created_at=parse_datetime(created_at),
                )
                session.add(row)
            row.session_position_id = session_position_id
            row.closed_quantity = float(closed_quantity)
            row.exit_debit = exit_debit
            row.realized_pnl = float(realized_pnl)
            row.broker_order_id = broker_order_id
            row.closed_at = parse_datetime(closed_at)
            row.updated_at = parse_datetime(updated_at)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_position(self, position_id: str) -> PortfolioPositionRecord | None:
        with self.session_factory() as session:
            row = session.get(PortfolioPositionModel, position_id)
        if row is None:
            return None
        return self.row(row)

    def get_position_by_open_attempt(self, open_execution_attempt_id: str) -> PortfolioPositionRecord | None:
        statement = select(PortfolioPositionModel).where(
            PortfolioPositionModel.open_execution_attempt_id == open_execution_attempt_id
        )
        with self.session_factory() as session:
            row = session.scalars(statement).first()
        if row is None:
            return None
        return self.row(row)

    def list_positions(
        self,
        *,
        pipeline_id: str | None = None,
        market_date: str | None = None,
        statuses: list[str] | None = None,
        limit: int | None = None,
    ) -> list[PortfolioPositionRecord]:
        statement = select(PortfolioPositionModel)
        if pipeline_id is not None:
            statement = statement.where(PortfolioPositionModel.pipeline_id == pipeline_id)
        if market_date is not None:
            market_date_value = parse_date(market_date)
            statement = statement.where(PortfolioPositionModel.market_date_opened == market_date_value)
        if statuses:
            statement = statement.where(PortfolioPositionModel.status.in_(statuses))
        statement = statement.order_by(PortfolioPositionModel.updated_at.desc(), PortfolioPositionModel.position_id.desc())
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def create_position(
        self,
        *,
        position_id: str,
        pipeline_id: str,
        source_opportunity_id: str | None,
        legacy_session_position_id: str | None,
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
                pipeline_id=pipeline_id,
                source_opportunity_id=source_opportunity_id,
                legacy_session_position_id=legacy_session_position_id,
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
        pipeline_id: str | None = None,
        source_opportunity_id: str | None = None,
        legacy_session_position_id: str | None = None,
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
            if pipeline_id is not None:
                row.pipeline_id = pipeline_id
            if source_opportunity_id is not None:
                row.source_opportunity_id = source_opportunity_id
            if legacy_session_position_id is not None:
                row.legacy_session_position_id = legacy_session_position_id
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
        statement = statement.order_by(PositionCloseModel.closed_at.desc(), PositionCloseModel.position_close_id.desc())
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def upsert_position_close(
        self,
        *,
        position_id: str,
        execution_attempt_id: str,
        legacy_session_position_id: str | None,
        closed_quantity: float,
        exit_value: float | None,
        realized_pnl: float,
        broker_order_id: str | None,
        closed_at: str | None,
        created_at: str,
        updated_at: str,
    ) -> PositionCloseRecord:
        with self.session_scope() as session:
            statement = select(PositionCloseModel).where(
                PositionCloseModel.execution_attempt_id == execution_attempt_id
            )
            row = session.scalars(statement).first()
            if row is None:
                row = PositionCloseModel(
                    position_id=position_id,
                    execution_attempt_id=execution_attempt_id,
                    created_at=parse_datetime(created_at),
                )
                session.add(row)
            row.position_id = position_id
            row.legacy_session_position_id = legacy_session_position_id
            row.closed_quantity = float(closed_quantity)
            row.exit_value = exit_value
            row.realized_pnl = float(realized_pnl)
            row.broker_order_id = broker_order_id
            row.closed_at = parse_datetime(closed_at)
            row.updated_at = parse_datetime(updated_at)
            session.flush()
            session.refresh(row)
            return self.row(row)
