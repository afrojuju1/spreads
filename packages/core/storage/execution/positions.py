from __future__ import annotations

from typing import Any

from sqlalchemy import select

from core.storage.execution.shared import _optional_date
from core.storage.execution_models import PortfolioPositionModel, PositionCloseModel
from core.storage.records import PortfolioPositionRecord, PositionCloseRecord
from core.storage.serializers import parse_date, parse_datetime


class ExecutionPositionRepositoryMixin:
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
