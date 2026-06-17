from __future__ import annotations

from typing import Any

from sqlalchemy import select

from core.storage.backtest_models import BacktestArtifactModel, BacktestRunModel, BacktestVariantResultModel
from core.storage.base import RepositoryBase
from core.storage.records import BacktestArtifactRecord, BacktestRunRecord, BacktestVariantResultRecord
from core.storage.serializers import parse_date, parse_datetime


class BacktestRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("backtest_runs", "backtest_artifacts", "backtest_variant_results")

    def create_run(
        self,
        *,
        backtest_run_id: str,
        mode: str,
        state: str,
        requested_by: str | None,
        strategy_ids: list[str],
        start_date: str,
        end_date: str,
        config_snapshot: dict[str, Any],
        request: dict[str, Any],
        artifact_root: str | None,
        created_at: str,
        started_at: str | None = None,
        summary: dict[str, Any] | None = None,
        fidelity: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> BacktestRunRecord:
        created_at_dt = parse_datetime(created_at)
        started_at_dt = parse_datetime(started_at)
        if created_at_dt is None:
            raise ValueError("created_at is required")
        with self.session_scope() as session:
            row = BacktestRunModel(
                backtest_run_id=backtest_run_id,
                mode=mode,
                state=state,
                requested_by=requested_by,
                strategy_ids_json=list(strategy_ids),
                start_date=parse_date(start_date),
                end_date=parse_date(end_date),
                config_snapshot_json=dict(config_snapshot),
                request_json=dict(request),
                summary_json=dict(summary or {}),
                fidelity_json=dict(fidelity or {}),
                artifact_root=artifact_root,
                error_text=error_text,
                created_at=created_at_dt,
                started_at=started_at_dt,
                completed_at=None,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def complete_run(
        self,
        *,
        backtest_run_id: str,
        summary: dict[str, Any],
        fidelity: dict[str, Any],
        completed_at: str,
    ) -> BacktestRunRecord:
        completed_at_dt = parse_datetime(completed_at)
        if completed_at_dt is None:
            raise ValueError("completed_at is required")
        with self.session_scope() as session:
            row = session.get(BacktestRunModel, backtest_run_id)
            if row is None:
                raise ValueError(f"Unknown backtest_run_id: {backtest_run_id}")
            row.state = "completed"
            row.summary_json = dict(summary)
            row.fidelity_json = dict(fidelity)
            row.completed_at = completed_at_dt
            row.error_text = None
            session.flush()
            session.refresh(row)
            return self.row(row)

    def fail_run(
        self,
        *,
        backtest_run_id: str,
        error_text: str,
        completed_at: str,
    ) -> BacktestRunRecord:
        completed_at_dt = parse_datetime(completed_at)
        if completed_at_dt is None:
            raise ValueError("completed_at is required")
        with self.session_scope() as session:
            row = session.get(BacktestRunModel, backtest_run_id)
            if row is None:
                raise ValueError(f"Unknown backtest_run_id: {backtest_run_id}")
            row.state = "failed"
            row.completed_at = completed_at_dt
            row.error_text = error_text
            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_run(self, backtest_run_id: str) -> BacktestRunRecord | None:
        with self.session_factory() as session:
            row = session.get(BacktestRunModel, backtest_run_id)
        if row is None:
            return None
        return self.row(row)

    def list_runs(
        self,
        *,
        mode: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ) -> list[BacktestRunRecord]:
        statement = select(BacktestRunModel)
        if mode is not None:
            statement = statement.where(BacktestRunModel.mode == mode)
        if state is not None:
            statement = statement.where(BacktestRunModel.state == state)
        statement = statement.order_by(BacktestRunModel.created_at.desc(), BacktestRunModel.backtest_run_id.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def record_artifact(
        self,
        *,
        backtest_artifact_id: str,
        backtest_run_id: str,
        artifact_kind: str,
        storage_kind: str,
        uri: str,
        content_type: str | None,
        row_count: int | None,
        byte_count: int | None,
        schema: dict[str, Any],
        metadata: dict[str, Any],
        created_at: str,
    ) -> BacktestArtifactRecord:
        created_at_dt = parse_datetime(created_at)
        if created_at_dt is None:
            raise ValueError("created_at is required")
        with self.session_scope() as session:
            row = BacktestArtifactModel(
                backtest_artifact_id=backtest_artifact_id,
                backtest_run_id=backtest_run_id,
                artifact_kind=artifact_kind,
                storage_kind=storage_kind,
                uri=uri,
                content_type=content_type,
                row_count=row_count,
                byte_count=byte_count,
                schema_json=dict(schema),
                metadata_json=dict(metadata),
                created_at=created_at_dt,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_variant_results(
        self,
        backtest_run_id: str,
        *,
        limit: int = 500,
    ) -> list[BacktestVariantResultRecord]:
        statement = (
            select(BacktestVariantResultModel)
            .where(BacktestVariantResultModel.backtest_run_id == backtest_run_id)
            .order_by(BacktestVariantResultModel.rank.asc().nullslast(), BacktestVariantResultModel.backtest_variant_id.asc())
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_artifacts(self, backtest_run_id: str) -> list[BacktestArtifactRecord]:
        statement = (
            select(BacktestArtifactModel)
            .where(BacktestArtifactModel.backtest_run_id == backtest_run_id)
            .order_by(BacktestArtifactModel.created_at.asc(), BacktestArtifactModel.backtest_artifact_id.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def record_variant_result(
        self,
        *,
        backtest_variant_id: str,
        backtest_run_id: str,
        trading_strategy_id: str,
        config_hash: str,
        variant_hash: str,
        parameters: dict[str, Any],
        summary: dict[str, Any],
        metrics: dict[str, Any],
        fidelity: dict[str, Any],
        rank: int | None,
        created_at: str,
    ) -> BacktestVariantResultRecord:
        created_at_dt = parse_datetime(created_at)
        if created_at_dt is None:
            raise ValueError("created_at is required")
        with self.session_scope() as session:
            row = BacktestVariantResultModel(
                backtest_variant_id=backtest_variant_id,
                backtest_run_id=backtest_run_id,
                trading_strategy_id=trading_strategy_id,
                config_hash=config_hash,
                variant_hash=variant_hash,
                parameters_json=dict(parameters),
                summary_json=dict(summary),
                metrics_json=dict(metrics),
                fidelity_json=dict(fidelity),
                rank=rank,
                created_at=created_at_dt,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)


__all__ = ["BacktestRepository"]
