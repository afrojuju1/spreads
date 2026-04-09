from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import delete, select

from spreads.storage.base import RepositoryBase
from spreads.storage.generator_job_models import GeneratorJobModel
from spreads.storage.records import GeneratorJobRecord
from spreads.storage.serializers import parse_datetime


class GeneratorJobRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("generator_jobs")

    def create_job(
        self,
        *,
        generator_job_id: str,
        arq_job_id: str | None,
        symbol: str,
        created_at: str | datetime,
        request: dict[str, Any],
        status: str = "queued",
    ) -> GeneratorJobRecord:
        created_at_dt = parse_datetime(created_at)
        if created_at_dt is None:
            raise ValueError("created_at is required")
        with self.session_scope() as session:
            row = GeneratorJobModel(
                generator_job_id=generator_job_id,
                arq_job_id=arq_job_id,
                symbol=symbol,
                status=status,
                created_at=created_at_dt,
                request_json=request,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def start_job(
        self,
        *,
        generator_job_id: str,
        started_at: str | datetime,
        status: str = "running",
    ) -> GeneratorJobRecord:
        started_at_dt = parse_datetime(started_at)
        if started_at_dt is None:
            raise ValueError("started_at is required")
        with self.session_scope() as session:
            row = session.get(GeneratorJobModel, generator_job_id)
            if row is None:
                raise ValueError(f"Unknown generator_job_id: {generator_job_id}")
            row.status = status
            row.started_at = started_at_dt
            row.finished_at = None
            row.result_json = None
            row.error_text = None
            session.flush()
            session.refresh(row)
            return self.row(row)

    def complete_job(
        self,
        *,
        generator_job_id: str,
        finished_at: str | datetime,
        status: str,
        result: dict[str, Any],
    ) -> GeneratorJobRecord:
        finished_at_dt = parse_datetime(finished_at)
        if finished_at_dt is None:
            raise ValueError("finished_at is required")
        with self.session_scope() as session:
            row = session.get(GeneratorJobModel, generator_job_id)
            if row is None:
                raise ValueError(f"Unknown generator_job_id: {generator_job_id}")
            row.status = status
            row.finished_at = finished_at_dt
            row.result_json = result
            row.error_text = None
            session.flush()
            session.refresh(row)
            return self.row(row)

    def fail_job(
        self,
        *,
        generator_job_id: str,
        finished_at: str | datetime,
        error_text: str,
    ) -> GeneratorJobRecord:
        finished_at_dt = parse_datetime(finished_at)
        if finished_at_dt is None:
            raise ValueError("finished_at is required")
        with self.session_scope() as session:
            row = session.get(GeneratorJobModel, generator_job_id)
            if row is None:
                raise ValueError(f"Unknown generator_job_id: {generator_job_id}")
            row.status = "failed"
            row.finished_at = finished_at_dt
            row.error_text = error_text
            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_job(self, generator_job_id: str) -> GeneratorJobRecord | None:
        with self.session_factory() as session:
            row = session.get(GeneratorJobModel, generator_job_id)
        if row is None:
            return None
        return self.row(row)

    def list_jobs(
        self,
        *,
        symbol: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[GeneratorJobRecord]:
        statement = select(GeneratorJobModel)
        if symbol:
            statement = statement.where(GeneratorJobModel.symbol == symbol.upper())
        if status:
            statement = statement.where(GeneratorJobModel.status == status)
        statement = statement.order_by(GeneratorJobModel.created_at.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(GeneratorJobModel))
