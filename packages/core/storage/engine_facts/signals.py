from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

from sqlalchemy import or_, select

from core.storage.lifecycle_models import (
    TradeDecisionModel,
    TradeSignalModel,
)
from core.storage.read_models import TradeDecisionSignalRead
from core.storage.records import StorageRow
from core.storage.serializers import parse_date, parse_datetime, render_value

if TYPE_CHECKING:
    pass


from core.storage.engine_facts.contracts import (
    TradeDecisionSignalQuery,
)

class EngineFactSignalMixin:
    def upsert_trade_signal(
        self,
        *,
        trade_signal_id: str,
        idempotency_key: str,
        trade_candidate_id: str | None,
        source_kind: str,
        source_id: str,
        trading_strategy_id: str,
        trade_structure: str,
        routine: str,
        config_hash: str,
        session_date: str | date,
        market_session: str,
        observed_at: str,
        expires_at: str | None,
        underlying_symbol: str,
        root_symbol: str | None,
        asset_class: str | None,
        product_class: str | None,
        horizon: str | None,
        style_profile: str | None,
        signal_state: str,
        rank: int | None,
        score: float | None,
        confidence: float | None,
        legs: list[dict[str, Any]],
        execution_shape: dict[str, Any],
        economics: dict[str, Any],
        reason_codes: list[str],
        blockers: list[str],
        evidence: dict[str, Any],
        metrics: dict[str, Any],
        updated_at: str,
    ) -> StorageRow:
        observed_at_dt = parse_datetime(observed_at)
        expires_at_dt = parse_datetime(expires_at)
        updated_at_dt = parse_datetime(updated_at)
        if observed_at_dt is None or updated_at_dt is None:
            raise ValueError("observed_at and updated_at are required")
        with self.session_scope() as session:
            row = session.get(TradeSignalModel, trade_signal_id)
            if row is None:
                row = TradeSignalModel(
                    trade_signal_id=trade_signal_id,
                    idempotency_key=idempotency_key,
                    trade_candidate_id=trade_candidate_id,
                    source_kind=source_kind,
                    source_id=source_id,
                    trading_strategy_id=trading_strategy_id,
                    routine=routine,
                    config_hash=config_hash,
                    account_id=None,
                    session_date=parse_date(session_date),
                    market_session=market_session,
                    observed_at=observed_at_dt,
                    expires_at=expires_at_dt,
                    underlying_symbol=underlying_symbol.upper(),
                    root_symbol=root_symbol,
                    asset_class=asset_class,
                    trade_structure=trade_structure,
                    product_class=product_class,
                    horizon=horizon,
                    style_profile=style_profile,
                    signal_state=signal_state,
                    rank=rank,
                    score=score,
                    confidence=confidence,
                    legs_json=render_value(legs),
                    execution_shape_json=render_value(execution_shape),
                    economics_json=render_value(economics),
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    evidence_json=render_value(evidence),
                    metrics_json=render_value(metrics),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
            else:
                row.idempotency_key = idempotency_key
                row.trade_candidate_id = trade_candidate_id
                row.source_kind = source_kind
                row.source_id = source_id
                row.trading_strategy_id = trading_strategy_id
                row.routine = routine
                row.config_hash = config_hash
                row.session_date = parse_date(session_date)
                row.market_session = market_session
                row.observed_at = observed_at_dt
                row.expires_at = expires_at_dt
                row.underlying_symbol = underlying_symbol.upper()
                row.root_symbol = root_symbol
                row.asset_class = asset_class
                row.trade_structure = trade_structure
                row.product_class = product_class
                row.horizon = horizon
                row.style_profile = style_profile
                row.signal_state = signal_state
                row.rank = rank
                row.score = score
                row.confidence = confidence
                row.legs_json = render_value(legs)
                row.execution_shape_json = render_value(execution_shape)
                row.economics_json = render_value(economics)
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.evidence_json = render_value(evidence)
                row.metrics_json = render_value(metrics)
                row.updated_at = updated_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def upsert_trade_decision(
        self,
        *,
        trade_decision_id: str,
        trade_signal_id: str,
        trading_strategy_id: str,
        trade_structure: str,
        routine: str,
        config_hash: str,
        run_key: str,
        scope_key: str,
        decision_state: str,
        rank: int | None,
        score: float | None,
        selected_quantity: int | None,
        selected_execution_shape: dict[str, Any],
        reason_codes: list[str],
        blockers: list[str],
        evidence: dict[str, Any],
        metrics: dict[str, Any],
        supersedes_decision_id: str | None,
        superseded_by_decision_id: str | None,
        decided_at: str,
    ) -> StorageRow:
        decided_at_dt = parse_datetime(decided_at)
        if decided_at_dt is None:
            raise ValueError("decided_at is required")
        with self.session_scope() as session:
            row = session.get(TradeDecisionModel, trade_decision_id)
            if row is None:
                row = TradeDecisionModel(
                    trade_decision_id=trade_decision_id,
                    trade_signal_id=trade_signal_id,
                    trading_strategy_id=trading_strategy_id,
                    trade_structure=trade_structure,
                    routine=routine,
                    config_hash=config_hash,
                    run_key=run_key,
                    scope_key=scope_key,
                    decision_state=decision_state,
                    rank=rank,
                    score=score,
                    selected_quantity=selected_quantity,
                    selected_execution_shape_json=render_value(selected_execution_shape),
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    evidence_json=render_value(evidence),
                    metrics_json=render_value(metrics),
                    supersedes_decision_id=supersedes_decision_id,
                    superseded_by_decision_id=superseded_by_decision_id,
                    decided_at=decided_at_dt,
                )
                session.add(row)
            else:
                row.trade_signal_id = trade_signal_id
                row.trading_strategy_id = trading_strategy_id
                row.trade_structure = trade_structure
                row.routine = routine
                row.config_hash = config_hash
                row.run_key = run_key
                row.scope_key = scope_key
                row.decision_state = decision_state
                row.rank = rank
                row.score = score
                row.selected_quantity = selected_quantity
                row.selected_execution_shape_json = render_value(selected_execution_shape)
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.evidence_json = render_value(evidence)
                row.metrics_json = render_value(metrics)
                row.supersedes_decision_id = supersedes_decision_id
                row.superseded_by_decision_id = superseded_by_decision_id
                row.decided_at = decided_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_trade_signal(self, trade_signal_id: str) -> StorageRow | None:
        with self.session_factory() as session:
            row = session.get(TradeSignalModel, trade_signal_id)
        if row is None:
            return None
        return self.row(row)

    def get_trade_decision(self, trade_decision_id: str) -> StorageRow | None:
        with self.session_factory() as session:
            row = session.get(TradeDecisionModel, trade_decision_id)
        if row is None:
            return None
        return self.row(row)

    def get_trade_decision_with_signal(self, trade_decision_id: str) -> TradeDecisionSignalRead | None:
        statement = (
            select(TradeDecisionModel, TradeSignalModel)
            .join(TradeSignalModel, TradeDecisionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
            .where(TradeDecisionModel.trade_decision_id == trade_decision_id)
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.execute(statement).first()
        if row is None:
            return None
        decision, signal = row
        return TradeDecisionSignalRead.from_rows(
            decision=self.row(decision),
            signal=self.row(signal),
        )

    def list_trade_signals(
        self,
        *,
        signal_states: list[str] | None = None,
        routine: str | None = None,
        as_of: str | None = None,
        limit: int = 100,
    ) -> list[StorageRow]:
        as_of_dt = parse_datetime(as_of)
        statement = select(TradeSignalModel)
        if signal_states:
            statement = statement.where(TradeSignalModel.signal_state.in_(signal_states))
        if routine is not None:
            statement = statement.where(TradeSignalModel.routine == routine)
        if as_of_dt is not None:
            statement = statement.where(or_(TradeSignalModel.expires_at.is_(None), TradeSignalModel.expires_at > as_of_dt))
        statement = statement.order_by(
            TradeSignalModel.score.desc().nullslast(),
            TradeSignalModel.rank.asc().nullslast(),
            TradeSignalModel.updated_at.desc(),
            TradeSignalModel.trade_signal_id.asc(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_trade_decisions_with_signals(
        self,
        *,
        decision_states: list[str] | None = None,
        trading_strategy_ids: list[str] | None = None,
        routine: str | None = None,
        session_date: str | date | None = None,
        as_of: str | None = None,
        limit: int = 100,
    ) -> list[StorageRow]:
        query = TradeDecisionSignalQuery.model_validate(
            {
                "decision_states": decision_states,
                "trading_strategy_ids": trading_strategy_ids,
                "routine": routine,
                "session_date": session_date,
                "as_of": as_of,
                "limit": limit,
            }
        )
        statement = select(TradeDecisionModel, TradeSignalModel).join(
            TradeSignalModel, TradeDecisionModel.trade_signal_id == TradeSignalModel.trade_signal_id
        )
        if query.decision_states:
            statement = statement.where(TradeDecisionModel.decision_state.in_(query.decision_states))
        if query.trading_strategy_ids:
            statement = statement.where(TradeDecisionModel.trading_strategy_id.in_(query.trading_strategy_ids))
        if query.routine is not None:
            statement = statement.where(TradeDecisionModel.routine == query.routine)
        if query.session_date is not None:
            statement = statement.where(TradeSignalModel.session_date == query.session_date)
        if query.as_of is not None:
            statement = statement.where(or_(TradeSignalModel.expires_at.is_(None), TradeSignalModel.expires_at > query.as_of))
        statement = statement.order_by(
            TradeDecisionModel.score.desc().nullslast(),
            TradeDecisionModel.rank.asc().nullslast(),
            TradeDecisionModel.decided_at.desc(),
            TradeDecisionModel.trade_decision_id.asc(),
        ).limit(query.limit)
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            {
                "trade_decision": self.row(decision),
                "trade_signal": self.row(signal),
            }
            for decision, signal in rows
        ]

    def list_trade_decisions(
        self,
        *,
        trading_strategy_id: str | None = None,
        decision_states: list[str] | None = None,
        routine: str | None = None,
        limit: int = 200,
    ) -> list[StorageRow]:
        statement = select(TradeDecisionModel)
        if trading_strategy_id is not None:
            statement = statement.where(TradeDecisionModel.trading_strategy_id == trading_strategy_id)
        if decision_states:
            statement = statement.where(TradeDecisionModel.decision_state.in_(decision_states))
        if routine is not None:
            statement = statement.where(TradeDecisionModel.routine == routine)
        statement = statement.order_by(
            TradeDecisionModel.decided_at.desc(),
            TradeDecisionModel.trade_decision_id.asc(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)


__all__ = ["EngineFactSignalMixin"]
