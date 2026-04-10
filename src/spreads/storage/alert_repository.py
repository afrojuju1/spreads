from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import delete, false, func, or_, select, tuple_

from spreads.storage.alert_models import AlertEventModel
from spreads.storage.base import RepositoryBase
from spreads.storage.records import AlertEventRecord, AlertStateRecord
from spreads.storage.serializers import parse_date, parse_datetime

ALERT_RECORD_KIND_DELIVERY = "delivery"
ALERT_RECORD_KIND_SCORE_ANCHOR = "score_anchor"
ALERT_DELIVERY_STATUSES = frozenset(
    {
        "pending",
        "dispatching",
        "retry_wait",
        "delivered",
        "suppressed",
        "dead_letter",
    }
)
ALERT_FINAL_STATUSES = frozenset({"delivered", "suppressed", "dead_letter"})


def _advisory_lock_id(key: str) -> int:
    digest = hashlib.sha1(key.encode("utf-8")).digest()[:8]
    return int.from_bytes(digest, byteorder="big", signed=True)


def _utc_now() -> datetime:
    return datetime.now(UTC)


class AlertRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("alert_events")

    def get_alert_state(self, dedupe_key: str) -> AlertStateRecord | None:
        statement = (
            select(AlertEventModel)
            .where(AlertEventModel.dedupe_key == dedupe_key)
            .order_by(AlertEventModel.updated_at.desc(), AlertEventModel.alert_id.desc())
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return self.row(row, aliases={"state_json": "state"})

    def get_alert_event(self, alert_id: int) -> AlertEventRecord | None:
        with self.session_factory() as session:
            row = session.get(AlertEventModel, alert_id)
        if row is None:
            return None
        return self.row(row)

    def get_delivery_event(self, alert_id: int) -> AlertEventRecord | None:
        statement = (
            select(AlertEventModel)
            .where(AlertEventModel.alert_id == alert_id)
            .where(AlertEventModel.record_kind == ALERT_RECORD_KIND_DELIVERY)
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return self.row(row)

    def plan_delivery_event(
        self,
        *,
        created_at: str | datetime,
        session_date: str | date,
        label: str,
        session_id: str | None,
        cycle_id: str,
        symbol: str,
        alert_type: str,
        dedupe_key: str,
        delivery_target: str,
        status: str,
        payload: dict[str, Any],
        state: dict[str, Any] | None,
        planner_job_run_id: str | None = None,
        response: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> tuple[AlertEventRecord, bool]:
        parsed_created_at = parse_datetime(created_at)
        if parsed_created_at is None:
            raise ValueError("created_at is required")
        if status not in ALERT_DELIVERY_STATUSES:
            raise ValueError(f"Unsupported delivery status: {status}")
        with self.session_scope() as session:
            session.execute(
                select(func.pg_advisory_xact_lock(_advisory_lock_id(f"{delivery_target}|{dedupe_key}")))
            )
            statement = (
                select(AlertEventModel)
                .where(AlertEventModel.record_kind == ALERT_RECORD_KIND_DELIVERY)
                .where(AlertEventModel.dedupe_key == dedupe_key)
                .where(AlertEventModel.delivery_target == delivery_target)
                .order_by(AlertEventModel.updated_at.desc(), AlertEventModel.alert_id.desc())
                .limit(1)
            )
            existing = session.scalar(statement)
            if existing is not None:
                return self.row(existing), False
            row = AlertEventModel(
                record_kind=ALERT_RECORD_KIND_DELIVERY,
                created_at=parsed_created_at,
                updated_at=parsed_created_at,
                session_date=parse_date(session_date),
                label=label,
                session_id=session_id,
                cycle_id=cycle_id,
                symbol=symbol,
                alert_type=alert_type,
                dedupe_key=dedupe_key,
                delivery_target=delivery_target,
                status=status,
                attempt_count=0,
                planner_job_run_id=planner_job_run_id,
                payload_json=dict(payload),
                state_json=None if state is None else dict(state),
                response_json=None if response is None else dict(response),
                error_text=error_text,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row), True

    def upsert_score_anchor(
        self,
        *,
        created_at: str | datetime,
        session_date: str | date,
        label: str,
        session_id: str | None,
        cycle_id: str,
        symbol: str,
        dedupe_key: str,
        state: dict[str, Any],
        planner_job_run_id: str | None = None,
    ) -> AlertEventRecord:
        parsed_created_at = parse_datetime(created_at)
        if parsed_created_at is None:
            raise ValueError("created_at is required")
        with self.session_scope() as session:
            session.execute(
                select(func.pg_advisory_xact_lock(_advisory_lock_id(f"score_anchor|{dedupe_key}")))
            )
            statement = (
                select(AlertEventModel)
                .where(AlertEventModel.record_kind == ALERT_RECORD_KIND_SCORE_ANCHOR)
                .where(AlertEventModel.dedupe_key == dedupe_key)
                .order_by(AlertEventModel.updated_at.desc(), AlertEventModel.alert_id.desc())
                .limit(1)
            )
            row = session.scalar(statement)
            if row is None:
                row = AlertEventModel(
                    record_kind=ALERT_RECORD_KIND_SCORE_ANCHOR,
                    created_at=parsed_created_at,
                    updated_at=parsed_created_at,
                    session_date=parse_date(session_date),
                    label=label,
                    session_id=session_id,
                    cycle_id=cycle_id,
                    symbol=symbol,
                    alert_type=ALERT_RECORD_KIND_SCORE_ANCHOR,
                    dedupe_key=dedupe_key,
                    status="anchor",
                    attempt_count=0,
                    planner_job_run_id=planner_job_run_id,
                    state_json=dict(state),
                )
                session.add(row)
            else:
                row.updated_at = parsed_created_at
                row.session_date = parse_date(session_date)
                row.label = label
                row.session_id = session_id
                row.cycle_id = cycle_id
                row.symbol = symbol
                row.planner_job_run_id = planner_job_run_id
                row.state_json = dict(state)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def mark_delivery_job_queued(
        self,
        *,
        alert_id: int,
        delivery_job_run_id: str,
        queued_at: str | datetime | None = None,
    ) -> AlertEventRecord:
        queued_at_dt = parse_datetime(queued_at) or _utc_now()
        with self.session_scope() as session:
            row = session.get(AlertEventModel, alert_id)
            if row is None:
                raise ValueError(f"Unknown alert_id: {alert_id}")
            row.delivery_job_run_id = delivery_job_run_id
            row.updated_at = queued_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def claim_delivery_event(
        self,
        *,
        alert_id: int,
        delivery_job_run_id: str,
        worker_name: str,
        claimed_at: str | datetime | None = None,
    ) -> AlertEventRecord | None:
        claimed_at_dt = parse_datetime(claimed_at) or _utc_now()
        with self.session_scope() as session:
            row = session.get(AlertEventModel, alert_id, with_for_update=True)
            if row is None:
                return None
            if row.record_kind != ALERT_RECORD_KIND_DELIVERY:
                return None
            if row.status == "pending":
                eligible = True
            elif row.status == "retry_wait":
                next_attempt_at = row.next_attempt_at
                eligible = next_attempt_at is None or next_attempt_at <= claimed_at_dt
            else:
                eligible = False
            if not eligible:
                return None
            row.status = "dispatching"
            row.updated_at = claimed_at_dt
            row.claimed_at = claimed_at_dt
            row.last_attempt_at = claimed_at_dt
            row.next_attempt_at = None
            row.delivery_job_run_id = delivery_job_run_id
            row.worker_name = worker_name
            row.attempt_count = int(row.attempt_count or 0) + 1
            row.error_text = None
            session.flush()
            session.refresh(row)
            return self.row(row)

    def finish_delivery_event(
        self,
        *,
        alert_id: int,
        status: str,
        finished_at: str | datetime | None = None,
        response: dict[str, Any] | None = None,
        error_text: str | None = None,
        next_attempt_at: str | datetime | None = None,
        delivered_at: str | datetime | None = None,
        worker_name: str | None = None,
    ) -> AlertEventRecord:
        if status not in ALERT_DELIVERY_STATUSES:
            raise ValueError(f"Unsupported delivery status: {status}")
        finished_at_dt = parse_datetime(finished_at) or _utc_now()
        delivered_at_dt = parse_datetime(delivered_at)
        next_attempt_at_dt = parse_datetime(next_attempt_at)
        with self.session_scope() as session:
            row = session.get(AlertEventModel, alert_id)
            if row is None:
                raise ValueError(f"Unknown alert_id: {alert_id}")
            row.status = status
            row.updated_at = finished_at_dt
            row.claimed_at = None
            row.next_attempt_at = next_attempt_at_dt
            row.response_json = None if response is None else dict(response)
            row.error_text = error_text
            if delivered_at_dt is not None:
                row.delivered_at = delivered_at_dt
            elif status != "delivered":
                row.delivered_at = None
            if worker_name is not None:
                row.worker_name = worker_name
            session.flush()
            session.refresh(row)
            return self.row(row)

    def reset_stale_dispatching_event(
        self,
        *,
        alert_id: int,
        reset_at: str | datetime | None = None,
    ) -> AlertEventRecord | None:
        reset_at_dt = parse_datetime(reset_at) or _utc_now()
        with self.session_scope() as session:
            row = session.get(AlertEventModel, alert_id, with_for_update=True)
            if row is None or row.record_kind != ALERT_RECORD_KIND_DELIVERY or row.status != "dispatching":
                return None
            row.status = "pending"
            row.updated_at = reset_at_dt
            row.claimed_at = None
            row.worker_name = None
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_due_delivery_events(
        self,
        *,
        now: str | datetime | None = None,
        stale_dispatching_before: str | datetime | None = None,
        limit: int = 200,
    ) -> list[AlertEventRecord]:
        current = parse_datetime(now) or _utc_now()
        stale_before = parse_datetime(stale_dispatching_before)
        statement = (
            select(AlertEventModel)
            .where(AlertEventModel.record_kind == ALERT_RECORD_KIND_DELIVERY)
            .where(
                or_(
                    AlertEventModel.status == "pending",
                    (AlertEventModel.status == "retry_wait")
                    & (
                        AlertEventModel.next_attempt_at.is_(None)
                        | (AlertEventModel.next_attempt_at <= current)
                    ),
                    (
                        (AlertEventModel.status == "dispatching")
                        & (AlertEventModel.claimed_at.is_not(None))
                        & (AlertEventModel.claimed_at <= stale_before)
                    )
                    if stale_before is not None
                    else false(),
                )
            )
            .order_by(AlertEventModel.created_at.asc(), AlertEventModel.alert_id.asc())
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_alert_events(
        self,
        *,
        session_date: str | None = None,
        label: str | None = None,
        symbol: str | None = None,
        record_kind: str | None = ALERT_RECORD_KIND_DELIVERY,
        limit: int = 100,
    ) -> list[AlertEventRecord]:
        statement = select(AlertEventModel)
        if record_kind is not None:
            statement = statement.where(AlertEventModel.record_kind == record_kind)
        if session_date:
            statement = statement.where(AlertEventModel.session_date == date.fromisoformat(session_date))
        if label:
            statement = statement.where(AlertEventModel.label == label)
        if symbol:
            statement = statement.where(AlertEventModel.symbol == symbol.upper())
        statement = statement.order_by(AlertEventModel.created_at.desc(), AlertEventModel.alert_id.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def count_alert_events(
        self,
        *,
        session_date: str | None = None,
        label: str | None = None,
        record_kind: str | None = ALERT_RECORD_KIND_DELIVERY,
    ) -> int:
        statement = select(func.count()).select_from(AlertEventModel)
        if record_kind is not None:
            statement = statement.where(AlertEventModel.record_kind == record_kind)
        if session_date:
            statement = statement.where(AlertEventModel.session_date == date.fromisoformat(session_date))
        if label:
            statement = statement.where(AlertEventModel.label == label)
        with self.session_factory() as session:
            count = session.scalar(statement)
        return int(count or 0)

    def count_alert_events_by_session_keys(
        self,
        session_keys: list[tuple[str, str]],
        *,
        record_kind: str | None = ALERT_RECORD_KIND_DELIVERY,
    ) -> dict[tuple[str, str], int]:
        if not session_keys:
            return {}
        normalized_keys = [
            (parse_date(session_date), label)
            for session_date, label in session_keys
        ]
        statement = (
            select(
                AlertEventModel.session_date,
                AlertEventModel.label,
                func.count().label("alert_count"),
            )
            .where(tuple_(AlertEventModel.session_date, AlertEventModel.label).in_(normalized_keys))
            .group_by(AlertEventModel.session_date, AlertEventModel.label)
        )
        if record_kind is not None:
            statement = statement.where(AlertEventModel.record_kind == record_kind)
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        counts: dict[tuple[str, str], int] = {}
        for session_date, label, alert_count in rows:
            counts[(str(session_date), str(label))] = int(alert_count or 0)
        return counts

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(AlertEventModel))
