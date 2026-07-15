from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from core.storage.lifecycle_models import (
    TradeCloseDecisionModel,
    TradePositionModel,
)
from core.storage.records import StorageRow
from core.storage.serializers import parse_date, parse_datetime, render_value
from core.value_coercion import as_text, coerce_float

if TYPE_CHECKING:
    pass


class EngineFactLifecycleMixin:
    def upsert_trade_position_from_portfolio_position(
        self,
        *,
        position: Mapping[str, Any],
    ) -> StorageRow:
        position_id = as_text(position.get("position_id"))
        session_date = as_text(position.get("session_date") or position.get("market_date") or position.get("market_date_opened"))
        updated_at = parse_datetime(as_text(position.get("updated_at")) or datetime.now(UTC).isoformat())
        if position_id is None:
            raise ValueError("position_id is required")
        if session_date is None:
            raise ValueError("position session_date is required")
        if updated_at is None:
            raise ValueError("position updated_at is required")
        position_state = as_text(position.get("position_status") or position.get("status")) or "unknown"
        underlying_symbol = as_text(position.get("underlying_symbol") or position.get("root_symbol")) or "UNKNOWN"
        opened_quantity = coerce_float(position.get("opened_quantity")) or 0.0
        remaining_quantity = coerce_float(position.get("remaining_quantity")) or 0.0
        with self.session_scope() as session:
            row = session.get(TradePositionModel, position_id)
            if row is None:
                row = TradePositionModel(
                    position_id=position_id,
                    session_date=parse_date(session_date),
                    market_session=as_text(position.get("market_session")) or "regular",
                    position_state=position_state,
                    underlying_symbol=underlying_symbol,
                    canonical_legs_json=render_value(position.get("legs") or []),
                    opened_quantity=opened_quantity,
                    remaining_quantity=remaining_quantity,
                    realized_pnl=coerce_float(position.get("realized_pnl")) or 0.0,
                    risk_policy_snapshot_json={},
                    exit_policy_snapshot_json={},
                    updated_at=updated_at,
                )
                session.add(row)
            row.account_id = as_text(position.get("account_id"))
            row.session_date = parse_date(session_date)
            row.market_session = as_text(position.get("market_session")) or "regular"
            row.source_trade_signal_id = as_text(position.get("trade_signal_id"))
            row.opening_trade_decision_id = as_text(position.get("trade_decision_id"))
            row.opening_execution_intent_id = as_text(position.get("opening_execution_intent_id"))
            row.opening_execution_attempt_id = as_text(position.get("open_execution_attempt_id"))
            row.position_state = position_state
            row.underlying_symbol = underlying_symbol
            row.root_symbol = as_text(position.get("root_symbol"))
            row.trading_strategy_id = as_text(position.get("trading_strategy_id"))
            row.trade_structure = as_text(position.get("strategy_family") or position.get("strategy"))
            row.routine = as_text(position.get("routine"))
            row.config_hash = as_text(position.get("config_hash"))
            row.product_class = as_text(position.get("product_class"))
            row.canonical_legs_json = render_value(position.get("legs") or [])
            row.opened_quantity = opened_quantity
            row.remaining_quantity = remaining_quantity
            row.entry_value = coerce_float(position.get("entry_value") or position.get("entry_credit"))
            row.realized_pnl = coerce_float(position.get("realized_pnl")) or 0.0
            row.unrealized_pnl = coerce_float(position.get("unrealized_pnl"))
            row.mark = coerce_float(position.get("close_mark"))
            row.mark_source = as_text(position.get("close_mark_source"))
            row.marked_at = parse_datetime(as_text(position.get("close_marked_at")))
            row.risk_policy_snapshot_json = render_value(position.get("risk_policy") if isinstance(position.get("risk_policy"), Mapping) else {})
            row.exit_policy_snapshot_json = render_value(position.get("exit_policy") if isinstance(position.get("exit_policy"), Mapping) else {})
            row.reconciliation_state = as_text(position.get("reconciliation_status") or position.get("reconciliation_state"))
            row.last_reconciled_at = parse_datetime(as_text(position.get("last_reconciled_at")))
            row.reconciliation_note = as_text(position.get("reconciliation_note"))
            row.opened_at = parse_datetime(as_text(position.get("opened_at")))
            row.closed_at = parse_datetime(as_text(position.get("closed_at")))
            row.updated_at = updated_at
            session.flush()
            session.refresh(row)
            return self.row(row)

    def upsert_trade_close_decision(
        self,
        *,
        close_decision_id: str,
        position_id: str,
        decision_state: str,
        reason: str,
        quantity_to_close: float | None,
        limit_source: str | None,
        limit_price: float | None,
        mark_source: str | None,
        policy_snapshot: dict[str, Any],
        reason_codes: list[str],
        blockers: list[str],
        evidence: dict[str, Any],
        metrics: dict[str, Any],
        decided_at: str,
        execution_intent_id: str | None = None,
    ) -> StorageRow:
        decided_at_dt = parse_datetime(decided_at)
        if decided_at_dt is None:
            raise ValueError("decided_at is required")
        with self.session_scope() as session:
            row = session.get(TradeCloseDecisionModel, close_decision_id)
            if row is None:
                row = TradeCloseDecisionModel(
                    close_decision_id=close_decision_id,
                    position_id=position_id,
                    decision_state=decision_state,
                    reason=reason,
                    decided_at=decided_at_dt,
                )
                session.add(row)
            row.position_id = position_id
            row.decision_state = decision_state
            row.reason = reason
            row.quantity_to_close = quantity_to_close
            row.limit_source = limit_source
            row.limit_price = limit_price
            row.mark_source = mark_source
            row.policy_snapshot_json = render_value(policy_snapshot)
            row.reason_codes_json = list(reason_codes)
            row.blockers_json = list(blockers)
            row.evidence_json = render_value(evidence)
            row.metrics_json = render_value(metrics)
            row.decided_at = decided_at_dt
            row.execution_intent_id = execution_intent_id
            session.flush()
            session.refresh(row)
            return self.row(row)

    def attach_trade_close_decision_intent(
        self,
        *,
        close_decision_id: str,
        execution_intent_id: str,
    ) -> StorageRow | None:
        with self.session_scope() as session:
            row = session.get(TradeCloseDecisionModel, close_decision_id)
            if row is None:
                return None
            row.execution_intent_id = execution_intent_id
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_trade_close_decisions(
        self,
        *,
        trading_strategy_id: str | None = None,
        session_date: str | date | None = None,
        position_id: str | None = None,
        limit: int = 200,
    ) -> list[StorageRow]:
        statement = select(TradeCloseDecisionModel)
        if trading_strategy_id is not None or session_date is not None:
            statement = statement.join(TradePositionModel, TradeCloseDecisionModel.position_id == TradePositionModel.position_id)
        if trading_strategy_id is not None:
            statement = statement.where(TradePositionModel.trading_strategy_id == trading_strategy_id)
        if session_date is not None:
            statement = statement.where(TradePositionModel.session_date == parse_date(session_date))
        if position_id is not None:
            statement = statement.where(TradeCloseDecisionModel.position_id == position_id)
        statement = statement.order_by(
            TradeCloseDecisionModel.decided_at.desc(),
            TradeCloseDecisionModel.close_decision_id.asc(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)


__all__ = ["EngineFactLifecycleMixin"]
