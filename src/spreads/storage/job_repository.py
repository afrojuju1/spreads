from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Iterator

from sqlalchemy import delete, inspect, select
from sqlalchemy.orm import Session

from spreads.storage.db import build_session_factory
from spreads.storage.job_models import JobDefinitionModel, JobLeaseModel, JobRunModel
from spreads.storage.records import JobDefinitionRecord, JobLeaseRecord, JobRunRecord
from spreads.storage.serializers import (
    parse_datetime,
    to_job_definition_record,
    to_job_lease_record,
    to_job_run_record,
)


class JobRepository:
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
        required = {"job_definitions", "job_runs", "job_leases"}
        return required.issubset(tables)

    def upsert_job_definition(
        self,
        *,
        job_key: str,
        job_type: str,
        enabled: bool,
        schedule_type: str,
        schedule: dict[str, Any],
        payload: dict[str, Any],
        market_calendar: str = "NYSE",
        singleton_scope: str | None = None,
    ) -> JobDefinitionRecord:
        now = datetime.now(UTC)
        with self.session_scope() as session:
            row = session.get(JobDefinitionModel, job_key)
            if row is None:
                row = JobDefinitionModel(
                    job_key=job_key,
                    created_at=now,
                )
                session.add(row)
            row.job_type = job_type
            row.enabled = enabled
            row.schedule_type = schedule_type
            row.schedule_json = schedule
            row.payload_json = payload
            row.market_calendar = market_calendar
            row.singleton_scope = singleton_scope
            row.updated_at = now
            session.flush()
            session.refresh(row)
            return to_job_definition_record(row)

    def get_job_definition(self, job_key: str) -> JobDefinitionRecord | None:
        with self.session_factory() as session:
            row = session.get(JobDefinitionModel, job_key)
        if row is None:
            return None
        return to_job_definition_record(row)

    def list_job_definitions(
        self,
        *,
        enabled_only: bool | None = None,
        job_type: str | None = None,
    ) -> list[JobDefinitionRecord]:
        statement = select(JobDefinitionModel)
        if enabled_only is True:
            statement = statement.where(JobDefinitionModel.enabled.is_(True))
        elif enabled_only is False:
            statement = statement.where(JobDefinitionModel.enabled.is_(False))
        if job_type:
            statement = statement.where(JobDefinitionModel.job_type == job_type)
        statement = statement.order_by(JobDefinitionModel.job_key.asc())
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_job_definition_record(row) for row in rows]

    def create_job_run(
        self,
        *,
        job_run_id: str,
        job_key: str,
        arq_job_id: str | None,
        job_type: str,
        status: str,
        scheduled_for: str | datetime,
        session_id: str | None = None,
        slot_at: str | datetime | None = None,
        retry_count: int = 0,
        payload: dict[str, Any],
        worker_name: str | None = None,
        result: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> tuple[JobRunRecord, bool]:
        scheduled_for_dt = parse_datetime(scheduled_for)
        if scheduled_for_dt is None:
            raise ValueError("scheduled_for is required")
        slot_at_dt = parse_datetime(slot_at)
        with self.session_scope() as session:
            row = None
            if session_id and slot_at_dt is not None:
                statement = (
                    select(JobRunModel)
                    .where(JobRunModel.job_key == job_key)
                    .where(JobRunModel.session_id == session_id)
                    .where(JobRunModel.slot_at == slot_at_dt)
                )
                row = session.scalar(statement)
            if row is None:
                row = session.get(JobRunModel, job_run_id)
            if row is not None:
                return to_job_run_record(row), False
            row = JobRunModel(
                job_run_id=job_run_id,
                job_key=job_key,
                arq_job_id=arq_job_id,
                scheduled_for=scheduled_for_dt,
                session_id=session_id,
                slot_at=slot_at_dt,
                retry_count=retry_count,
                payload_json=payload,
                job_type=job_type,
                status=status,
                worker_name=worker_name,
                result_json=result,
                error_text=error_text,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return to_job_run_record(row), True

    def get_job_run(self, job_run_id: str) -> JobRunRecord | None:
        with self.session_factory() as session:
            row = session.get(JobRunModel, job_run_id)
        if row is None:
            return None
        return to_job_run_record(row)

    def get_job_run_for_slot(
        self,
        *,
        job_key: str,
        session_id: str,
        slot_at: str | datetime,
    ) -> JobRunRecord | None:
        slot_at_dt = parse_datetime(slot_at)
        if slot_at_dt is None:
            raise ValueError("slot_at is required")
        statement = (
            select(JobRunModel)
            .where(JobRunModel.job_key == job_key)
            .where(JobRunModel.session_id == session_id)
            .where(JobRunModel.slot_at == slot_at_dt)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return to_job_run_record(row)

    def list_job_runs(
        self,
        *,
        job_key: str | None = None,
        job_type: str | None = None,
        status: str | None = None,
        session_id: str | None = None,
        scheduled_from: str | datetime | None = None,
        scheduled_to: str | datetime | None = None,
        limit: int = 100,
    ) -> list[JobRunRecord]:
        statement = select(JobRunModel)
        if job_key:
            statement = statement.where(JobRunModel.job_key == job_key)
        if job_type:
            statement = statement.where(JobRunModel.job_type == job_type)
        if status:
            statement = statement.where(JobRunModel.status == status)
        if session_id:
            statement = statement.where(JobRunModel.session_id == session_id)
        scheduled_from_dt = parse_datetime(scheduled_from)
        if scheduled_from_dt is not None:
            statement = statement.where(JobRunModel.scheduled_for >= scheduled_from_dt)
        scheduled_to_dt = parse_datetime(scheduled_to)
        if scheduled_to_dt is not None:
            statement = statement.where(JobRunModel.scheduled_for <= scheduled_to_dt)
        statement = statement.order_by(JobRunModel.scheduled_for.desc(), JobRunModel.job_run_id.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_job_run_record(row) for row in rows]

    def update_job_run_status(
        self,
        *,
        job_run_id: str,
        status: str,
        expected_arq_job_id: str | None = None,
        worker_name: str | None = None,
        started_at: str | datetime | None = None,
        finished_at: str | datetime | None = None,
        heartbeat_at: str | datetime | None = None,
        result: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> JobRunRecord | None:
        with self.session_scope() as session:
            row = session.get(JobRunModel, job_run_id)
            if row is None:
                raise ValueError(f"Unknown job_run_id: {job_run_id}")
            if expected_arq_job_id is not None and row.arq_job_id != expected_arq_job_id:
                return None
            row.status = status
            if worker_name is not None:
                row.worker_name = worker_name
            if started_at is not None:
                row.started_at = parse_datetime(started_at)
            if finished_at is not None:
                row.finished_at = parse_datetime(finished_at)
            if heartbeat_at is not None:
                row.heartbeat_at = parse_datetime(heartbeat_at)
            if result is not None:
                row.result_json = result
            if status == "queued":
                row.started_at = None
                row.finished_at = None
                row.heartbeat_at = None
                row.worker_name = None
                row.result_json = None
            if error_text is not None or status == "failed":
                row.error_text = error_text
            elif status in {"queued", "running", "succeeded", "skipped"}:
                row.error_text = None
            session.flush()
            session.refresh(row)
            return to_job_run_record(row)

    def heartbeat_job_run(
        self,
        *,
        job_run_id: str,
        expected_arq_job_id: str | None = None,
        heartbeat_at: str | datetime | None = None,
        worker_name: str | None = None,
    ) -> JobRunRecord | None:
        return self.update_job_run_status(
            job_run_id=job_run_id,
            status="running",
            expected_arq_job_id=expected_arq_job_id,
            heartbeat_at=heartbeat_at or datetime.now(UTC),
            worker_name=worker_name,
        )

    def requeue_job_run(
        self,
        *,
        job_run_id: str,
        arq_job_id: str,
        payload: dict[str, Any] | None = None,
    ) -> JobRunRecord:
        with self.session_scope() as session:
            row = session.get(JobRunModel, job_run_id)
            if row is None:
                raise ValueError(f"Unknown job_run_id: {job_run_id}")
            row.arq_job_id = arq_job_id
            row.status = "queued"
            row.retry_count = int(row.retry_count) + 1
            row.started_at = None
            row.finished_at = None
            row.heartbeat_at = None
            row.worker_name = None
            row.result_json = None
            row.error_text = None
            if payload is not None:
                row.payload_json = payload
            session.flush()
            session.refresh(row)
            return to_job_run_record(row)

    def acquire_lease(
        self,
        *,
        lease_key: str,
        owner: str,
        expires_in_seconds: int,
        job_run_id: str | None = None,
        state: dict[str, Any] | None = None,
    ) -> bool:
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=max(expires_in_seconds, 1))
        with self.session_scope() as session:
            statement = select(JobLeaseModel).where(JobLeaseModel.lease_key == lease_key).with_for_update()
            row = session.scalar(statement)
            if row is not None and row.expires_at > now and row.owner != owner:
                return False
            if row is None:
                row = JobLeaseModel(lease_key=lease_key)
                session.add(row)
            row.job_run_id = job_run_id
            row.owner = owner
            row.acquired_at = now
            row.expires_at = expires_at
            row.lease_state_json = state or {}
            return True

    def renew_lease(
        self,
        *,
        lease_key: str,
        owner: str,
        expires_in_seconds: int,
        state: dict[str, Any] | None = None,
    ) -> JobLeaseRecord | None:
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=max(expires_in_seconds, 1))
        with self.session_scope() as session:
            statement = select(JobLeaseModel).where(JobLeaseModel.lease_key == lease_key).with_for_update()
            row = session.scalar(statement)
            if row is None or row.owner != owner:
                return None
            row.expires_at = expires_at
            if state is not None:
                row.lease_state_json = state
            session.flush()
            session.refresh(row)
            return to_job_lease_record(row)

    def release_lease(self, lease_key: str, *, owner: str | None = None) -> None:
        with self.session_scope() as session:
            row = session.get(JobLeaseModel, lease_key)
            if row is None:
                return
            if owner is not None and row.owner != owner:
                return
            session.delete(row)

    def get_lease(self, lease_key: str) -> JobLeaseRecord | None:
        with self.session_factory() as session:
            row = session.get(JobLeaseModel, lease_key)
        if row is None:
            return None
        return to_job_lease_record(row)

    def list_active_leases(self, *, prefix: str | None = None) -> list[JobLeaseRecord]:
        now = datetime.now(UTC)
        statement = select(JobLeaseModel).where(JobLeaseModel.expires_at > now)
        if prefix:
            statement = statement.where(JobLeaseModel.lease_key.like(f"{prefix}%"))
        statement = statement.order_by(JobLeaseModel.expires_at.desc())
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_job_lease_record(row) for row in rows]

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(JobLeaseModel))
            session.execute(delete(JobRunModel))
            session.execute(delete(JobDefinitionModel))

    def close(self) -> None:
        self.engine.dispose()
