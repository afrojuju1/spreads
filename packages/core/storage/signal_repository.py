from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select

from core.storage.base import RepositoryBase
from core.storage.records import StrategyRunRecord
from core.storage.serializers import parse_date, parse_datetime
from core.storage.signal_models import StrategyRunModel


class SignalRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("strategy_runs")

    def decision_schema_ready(self) -> bool:
        return self.schema_has_tables("strategy_runs")

    def strategy_runtime_schema_ready(self) -> bool:
        return self.schema_has_tables("strategy_runs")

    def upsert_strategy_run(
        self,
        *,
        strategy_run_id: str,
        trading_strategy_id: str,
        trigger_type: str,
        job_run_id: str | None,
        cycle_id: str | None,
        label: str | None,
        session_date: str | date,
        started_at: str,
        completed_at: str | None,
        status: str,
        result: dict[str, Any] | None,
        config_hash: str,
    ) -> StrategyRunRecord:
        started_at_dt = parse_datetime(started_at)
        completed_at_dt = parse_datetime(completed_at)
        session_date_value = parse_date(session_date)
        if started_at_dt is None:
            raise ValueError("started_at is required")
        with self.session_scope() as session:
            row = session.get(StrategyRunModel, strategy_run_id)
            if row is None:
                row = StrategyRunModel(
                    strategy_run_id=strategy_run_id,
                    trading_strategy_id=trading_strategy_id,
                    trigger_type=trigger_type,
                    job_run_id=job_run_id,
                    cycle_id=cycle_id,
                    label=label,
                    session_date=session_date_value,
                    started_at=started_at_dt,
                    completed_at=completed_at_dt,
                    status=status,
                    result_json=dict(result or {}),
                    config_hash=config_hash,
                )
                session.add(row)
            else:
                row.trading_strategy_id = trading_strategy_id
                row.trigger_type = trigger_type
                row.job_run_id = job_run_id
                row.cycle_id = cycle_id
                row.label = label
                row.session_date = session_date_value
                row.started_at = started_at_dt
                row.completed_at = completed_at_dt
                row.status = status
                row.result_json = dict(result or {})
                row.config_hash = config_hash
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_strategy_runs(
        self,
        *,
        trading_strategy_id: str | None = None,
        session_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        cycle_id: str | None = None,
        limit: int = 200,
    ) -> list[StrategyRunRecord]:
        statement = select(StrategyRunModel)
        if trading_strategy_id:
            statement = statement.where(StrategyRunModel.trading_strategy_id == trading_strategy_id)
        if session_date:
            statement = statement.where(StrategyRunModel.session_date == date.fromisoformat(session_date))
        if start_date:
            statement = statement.where(StrategyRunModel.session_date >= date.fromisoformat(start_date))
        if end_date:
            statement = statement.where(StrategyRunModel.session_date <= date.fromisoformat(end_date))
        if cycle_id:
            statement = statement.where(StrategyRunModel.cycle_id == cycle_id)
        statement = statement.order_by(
            StrategyRunModel.started_at.desc(),
            StrategyRunModel.strategy_run_id.asc(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)
