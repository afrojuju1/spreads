from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Iterator

from sqlalchemy import delete, inspect, select
from sqlalchemy.orm import Session

from spreads.storage.db import build_session_factory
from spreads.storage.post_market_models import PostMarketAnalysisRunModel
from spreads.storage.records import PostMarketAnalysisRunRecord
from spreads.storage.serializers import (
    parse_date,
    parse_datetime,
    to_post_market_analysis_run_record,
)


class PostMarketAnalysisRepository:
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
        return "post_market_analysis_runs" in tables

    def begin_run(
        self,
        *,
        analysis_run_id: str,
        session_date: str | date,
        label: str,
        created_at: str | datetime,
        job_run_id: str | None = None,
        status: str = "running",
    ) -> PostMarketAnalysisRunRecord:
        created_at_dt = parse_datetime(created_at)
        if created_at_dt is None:
            raise ValueError("created_at is required")
        with self.session_scope() as session:
            row = session.get(PostMarketAnalysisRunModel, analysis_run_id)
            if row is None:
                row = PostMarketAnalysisRunModel(
                    analysis_run_id=analysis_run_id,
                    job_run_id=job_run_id,
                    session_date=parse_date(session_date),
                    label=label,
                    created_at=created_at_dt,
                    status=status,
                )
                session.add(row)
            else:
                row.job_run_id = job_run_id
                row.session_date = parse_date(session_date)
                row.label = label
                row.created_at = created_at_dt
                row.status = status
                row.completed_at = None
                row.summary_json = None
                row.diagnostics_json = None
                row.recommendations_json = None
                row.report_markdown = None
                row.error_text = None
            session.flush()
            session.refresh(row)
            return to_post_market_analysis_run_record(row)

    def complete_run(
        self,
        *,
        analysis_run_id: str,
        completed_at: str | datetime,
        summary: dict[str, Any],
        diagnostics: dict[str, Any],
        recommendations: list[dict[str, Any]],
        report_markdown: str,
    ) -> PostMarketAnalysisRunRecord:
        completed_at_dt = parse_datetime(completed_at)
        if completed_at_dt is None:
            raise ValueError("completed_at is required")
        with self.session_scope() as session:
            row = session.get(PostMarketAnalysisRunModel, analysis_run_id)
            if row is None:
                raise ValueError(f"Unknown analysis_run_id: {analysis_run_id}")
            row.status = "succeeded"
            row.completed_at = completed_at_dt
            row.summary_json = summary
            row.diagnostics_json = diagnostics
            row.recommendations_json = recommendations
            row.report_markdown = report_markdown
            row.error_text = None
            session.flush()
            session.refresh(row)
            return to_post_market_analysis_run_record(row)

    def fail_run(
        self,
        *,
        analysis_run_id: str,
        completed_at: str | datetime,
        error_text: str,
    ) -> PostMarketAnalysisRunRecord:
        completed_at_dt = parse_datetime(completed_at)
        if completed_at_dt is None:
            raise ValueError("completed_at is required")
        with self.session_scope() as session:
            row = session.get(PostMarketAnalysisRunModel, analysis_run_id)
            if row is None:
                raise ValueError(f"Unknown analysis_run_id: {analysis_run_id}")
            row.status = "failed"
            row.completed_at = completed_at_dt
            row.error_text = error_text
            session.flush()
            session.refresh(row)
            return to_post_market_analysis_run_record(row)

    def skip_run(
        self,
        *,
        analysis_run_id: str,
        completed_at: str | datetime,
        error_text: str,
    ) -> PostMarketAnalysisRunRecord:
        completed_at_dt = parse_datetime(completed_at)
        if completed_at_dt is None:
            raise ValueError("completed_at is required")
        with self.session_scope() as session:
            row = session.get(PostMarketAnalysisRunModel, analysis_run_id)
            if row is None:
                raise ValueError(f"Unknown analysis_run_id: {analysis_run_id}")
            row.status = "skipped"
            row.completed_at = completed_at_dt
            row.summary_json = None
            row.diagnostics_json = None
            row.recommendations_json = None
            row.report_markdown = None
            row.error_text = error_text
            session.flush()
            session.refresh(row)
            return to_post_market_analysis_run_record(row)

    def get_run(self, analysis_run_id: str) -> PostMarketAnalysisRunRecord | None:
        with self.session_factory() as session:
            row = session.get(PostMarketAnalysisRunModel, analysis_run_id)
        if row is None:
            return None
        return to_post_market_analysis_run_record(row)

    def get_latest_run(
        self,
        *,
        label: str,
        session_date: str | None = None,
        succeeded_only: bool = False,
    ) -> PostMarketAnalysisRunRecord | None:
        statement = select(PostMarketAnalysisRunModel).where(PostMarketAnalysisRunModel.label == label)
        if session_date:
            statement = statement.where(PostMarketAnalysisRunModel.session_date == date.fromisoformat(session_date))
        if succeeded_only:
            statement = statement.where(PostMarketAnalysisRunModel.status == "succeeded")
        statement = statement.order_by(
            PostMarketAnalysisRunModel.completed_at.desc().nullslast(),
            PostMarketAnalysisRunModel.created_at.desc(),
            PostMarketAnalysisRunModel.analysis_run_id.desc(),
        ).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return to_post_market_analysis_run_record(row)

    def list_runs(
        self,
        *,
        session_date: str | None = None,
        label: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[PostMarketAnalysisRunRecord]:
        statement = select(PostMarketAnalysisRunModel)
        if session_date:
            statement = statement.where(PostMarketAnalysisRunModel.session_date == date.fromisoformat(session_date))
        if label:
            statement = statement.where(PostMarketAnalysisRunModel.label == label)
        if status:
            statement = statement.where(PostMarketAnalysisRunModel.status == status)
        statement = statement.order_by(
            PostMarketAnalysisRunModel.completed_at.desc().nullslast(),
            PostMarketAnalysisRunModel.created_at.desc(),
            PostMarketAnalysisRunModel.analysis_run_id.desc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_post_market_analysis_run_record(row) for row in rows]

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(PostMarketAnalysisRunModel))

    def close(self) -> None:
        self.engine.dispose()
