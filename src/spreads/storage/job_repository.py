from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from spreads.storage.base import RepositoryBase
from spreads.storage.job_models import JobDefinitionModel, JobLeaseModel, JobRunModel
from spreads.storage.records import JobDefinitionRecord, JobLeaseRecord, JobRunRecord
from spreads.storage.serializers import parse_datetime


class JobRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("job_definitions", "job_runs", "job_leases")

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
            return self.row(row)

    def get_job_definition(self, job_key: str) -> JobDefinitionRecord | None:
        with self.session_factory() as session:
            row = session.get(JobDefinitionModel, job_key)
        if row is None:
            return None
        return self.row(row)

    def delete_job_definition(self, job_key: str) -> bool:
        with self.session_scope() as session:
            row = session.get(JobDefinitionModel, job_key)
            if row is None:
                return False
            session.delete(row)
            return True

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
        return self.rows(rows)

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
                return self.row(row), False
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
            return self.row(row), True

    def get_job_run(self, job_run_id: str) -> JobRunRecord | None:
        with self.session_factory() as session:
            row = session.get(JobRunModel, job_run_id)
        if row is None:
            return None
        return self.row(row)

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
        return self.row(row)

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
        return self.rows(rows)

    def get_latest_live_collector_run(
        self,
        *,
        label: str | None = None,
        status: str | None = "succeeded",
    ) -> JobRunRecord | None:
        statement = select(JobRunModel).where(JobRunModel.job_type == "live_collector")
        if label:
            statement = statement.where(JobRunModel.payload_json["label"].astext == label)
        if status:
            statement = statement.where(JobRunModel.status == status)
        statement = statement.order_by(JobRunModel.scheduled_for.desc(), JobRunModel.job_run_id.desc()).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return self.row(row)

    def get_live_collector_run_by_cycle_id(
        self,
        *,
        cycle_id: str,
        label: str | None = None,
        status: str | None = "succeeded",
    ) -> JobRunRecord | None:
        statement = select(JobRunModel).where(JobRunModel.job_type == "live_collector")
        if label:
            statement = statement.where(JobRunModel.payload_json["label"].astext == label)
        if status:
            statement = statement.where(JobRunModel.status == status)
        statement = statement.order_by(JobRunModel.scheduled_for.desc(), JobRunModel.job_run_id.desc()).limit(500)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        for row in rows:
            payload = self.row(row)
            result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
            result_cycle_id = str(result.get("cycle_id") or "").strip()
            if result_cycle_id == cycle_id:
                return payload
            result_cycle_ids = result.get("cycle_ids")
            if isinstance(result_cycle_ids, list) and cycle_id in [str(item or "").strip() for item in result_cycle_ids]:
                return payload
        return None

    def list_latest_runs_by_session_ids(
        self,
        *,
        session_ids: list[str],
        job_type: str | None = None,
        statuses: list[str] | None = None,
    ) -> list[JobRunRecord]:
        if not session_ids:
            return []
        ranked_runs = (
            select(
                JobRunModel.job_run_id.label("job_run_id"),
                func.row_number()
                .over(
                    partition_by=JobRunModel.session_id,
                    order_by=(
                        JobRunModel.scheduled_for.desc(),
                        JobRunModel.job_run_id.desc(),
                    ),
                )
                .label("run_rank"),
            )
            .where(JobRunModel.session_id.in_(session_ids))
        )
        if job_type:
            ranked_runs = ranked_runs.where(JobRunModel.job_type == job_type)
        if statuses:
            ranked_runs = ranked_runs.where(JobRunModel.status.in_(statuses))
        ranked_runs = ranked_runs.subquery()
        statement = (
            select(JobRunModel)
            .join(ranked_runs, JobRunModel.job_run_id == ranked_runs.c.job_run_id)
            .where(ranked_runs.c.run_rank == 1)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_latest_runs_by_job_keys(
        self,
        *,
        job_keys: list[str],
        statuses: list[str] | None = None,
    ) -> list[JobRunRecord]:
        if not job_keys:
            return []
        ranked_runs = (
            select(
                JobRunModel.job_run_id.label("job_run_id"),
                func.row_number()
                .over(
                    partition_by=JobRunModel.job_key,
                    order_by=(
                        JobRunModel.scheduled_for.desc(),
                        JobRunModel.job_run_id.desc(),
                    ),
                )
                .label("run_rank"),
            )
            .where(JobRunModel.job_key.in_(job_keys))
        )
        if statuses:
            ranked_runs = ranked_runs.where(JobRunModel.status.in_(statuses))
        ranked_runs = ranked_runs.subquery()
        statement = (
            select(JobRunModel)
            .join(ranked_runs, JobRunModel.job_run_id == ranked_runs.c.job_run_id)
            .where(ranked_runs.c.run_rank == 1)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_session_ids(
        self,
        *,
        job_type: str | None = None,
        limit: int = 100,
    ) -> list[str]:
        activity_at = func.max(
            func.coalesce(
                JobRunModel.finished_at,
                JobRunModel.heartbeat_at,
                JobRunModel.started_at,
                JobRunModel.slot_at,
                JobRunModel.scheduled_for,
            )
        )
        statement = select(JobRunModel.session_id, activity_at.label("activity_at")).where(
            JobRunModel.session_id.is_not(None)
        )
        if job_type:
            statement = statement.where(JobRunModel.job_type == job_type)
        statement = (
            statement.group_by(JobRunModel.session_id)
            .order_by(activity_at.desc(), JobRunModel.session_id.desc())
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [str(session_id) for session_id, _ in rows if session_id]

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
            return self.row(row)

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
            return self.row(row)

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
        try:
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
                session.flush()
                return True
        except IntegrityError:
            return False

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
            return self.row(row)

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
        return self.row(row)

    def list_active_leases(self, *, prefix: str | None = None) -> list[JobLeaseRecord]:
        now = datetime.now(UTC)
        statement = select(JobLeaseModel).where(JobLeaseModel.expires_at > now)
        if prefix:
            statement = statement.where(JobLeaseModel.lease_key.like(f"{prefix}%"))
        statement = statement.order_by(JobLeaseModel.expires_at.desc())
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(JobLeaseModel))
            session.execute(delete(JobRunModel))
            session.execute(delete(JobDefinitionModel))
