from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Iterator
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, inspect, select
from sqlalchemy.orm import Session

from spreads.storage.collector_models import (
    CollectorCycleCandidateModel,
    CollectorCycleEventModel,
    CollectorCycleModel,
)
from spreads.storage.db import build_session_factory
from spreads.storage.records import (
    CollectorCycleCandidateRecord,
    CollectorCycleEventRecord,
    CollectorCycleRecord,
)
from spreads.storage.serializers import (
    parse_date,
    parse_datetime,
    to_collector_cycle_candidate_record,
    to_collector_cycle_event_record,
    to_collector_cycle_record,
)

NEW_YORK = ZoneInfo("America/New_York")


class CollectorRepository:
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
        required = {"collector_cycles", "collector_cycle_candidates", "collector_cycle_events"}
        return required.issubset(tables)

    def save_cycle(
        self,
        *,
        cycle_id: str,
        label: str,
        generated_at: str,
        universe_label: str,
        strategy: str,
        profile: str,
        greeks_source: str,
        symbols: list[str],
        failures: list[dict[str, Any]],
        selection_state: dict[str, Any],
        board_candidates: list[dict[str, Any]],
        watchlist_candidates: list[dict[str, Any]],
        events: list[dict[str, Any]],
    ) -> None:
        generated_at_dt = parse_datetime(generated_at)
        if generated_at_dt is None:
            raise ValueError("generated_at is required")
        session_date = generated_at_dt.astimezone(NEW_YORK).date()

        def build_candidate_models(
            bucket: str,
            payloads: list[dict[str, Any]],
        ) -> list[CollectorCycleCandidateModel]:
            models: list[CollectorCycleCandidateModel] = []
            for position, payload in enumerate(payloads, start=1):
                run_id = payload.get("run_id")
                if not run_id:
                    raise ValueError(f"Persisted {bucket} candidate is missing run_id")
                models.append(
                    CollectorCycleCandidateModel(
                        cycle_id=cycle_id,
                        bucket=bucket,
                        position=position,
                        run_id=str(run_id),
                        underlying_symbol=str(payload["underlying_symbol"]),
                        strategy=str(payload["strategy"]),
                        expiration_date=parse_date(payload["expiration_date"]),
                        short_symbol=str(payload["short_symbol"]),
                        long_symbol=str(payload["long_symbol"]),
                        quality_score=float(payload["quality_score"]),
                        midpoint_credit=float(payload["midpoint_credit"]),
                        candidate_json=payload,
                    )
                )
            return models

        cycle = CollectorCycleModel(
            cycle_id=cycle_id,
            label=label,
            session_date=session_date,
            generated_at=generated_at_dt,
            universe_label=universe_label,
            strategy=strategy,
            profile=profile,
            greeks_source=greeks_source,
            symbols_json=symbols,
            failures_json=failures,
            selection_state_json=selection_state,
            candidates=[
                *build_candidate_models("board", board_candidates),
                *build_candidate_models("watchlist", watchlist_candidates),
            ],
            events=[
                CollectorCycleEventModel(
                    cycle_id=cycle_id,
                    label=label,
                    session_date=session_date,
                    generated_at=generated_at_dt,
                    symbol=str(event["symbol"]),
                    event_type=str(event["event_type"]),
                    message=str(event["message"]),
                    previous_candidate_json=event.get("previous"),
                    current_candidate_json=event.get("current"),
                )
                for event in events
            ],
        )

        with self.session_scope() as session:
            session.merge(cycle)

    def get_cycle(self, cycle_id: str) -> CollectorCycleRecord | None:
        with self.session_factory() as session:
            cycle = session.get(CollectorCycleModel, cycle_id)
        if cycle is None:
            return None
        return to_collector_cycle_record(cycle)

    def get_latest_cycle(self, label: str) -> CollectorCycleRecord | None:
        statement = (
            select(CollectorCycleModel)
            .where(CollectorCycleModel.label == label)
            .order_by(CollectorCycleModel.generated_at.desc(), CollectorCycleModel.cycle_id.desc())
            .limit(1)
        )
        with self.session_factory() as session:
            cycle = session.scalar(statement)
        if cycle is None:
            return None
        return to_collector_cycle_record(cycle)

    def list_cycles(
        self,
        label: str,
        session_date: str | None = None,
        limit: int = 100,
    ) -> list[CollectorCycleRecord]:
        statement = select(CollectorCycleModel).where(CollectorCycleModel.label == label)
        if session_date:
            statement = statement.where(CollectorCycleModel.session_date == date.fromisoformat(session_date))
        statement = statement.order_by(CollectorCycleModel.generated_at.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_collector_cycle_record(row) for row in rows]

    def list_cycle_candidates(
        self,
        cycle_id: str,
        bucket: str | None = None,
    ) -> list[CollectorCycleCandidateRecord]:
        statement = (
            select(CollectorCycleCandidateModel, CollectorCycleModel)
            .join(CollectorCycleModel, CollectorCycleCandidateModel.cycle_id == CollectorCycleModel.cycle_id)
            .where(CollectorCycleCandidateModel.cycle_id == cycle_id)
        )
        if bucket:
            statement = statement.where(CollectorCycleCandidateModel.bucket == bucket)
        statement = statement.order_by(
            CollectorCycleCandidateModel.bucket.asc(),
            CollectorCycleCandidateModel.position.asc(),
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            to_collector_cycle_candidate_record(
                candidate,
                label=cycle.label,
                session_date=cycle.session_date,
                generated_at=cycle.generated_at,
            )
            for candidate, cycle in rows
        ]

    def list_session_candidates(
        self,
        *,
        label: str,
        session_date: str,
        bucket: str | None = None,
    ) -> list[CollectorCycleCandidateRecord]:
        session_date_value = date.fromisoformat(session_date)
        statement = (
            select(CollectorCycleCandidateModel, CollectorCycleModel)
            .join(CollectorCycleModel, CollectorCycleCandidateModel.cycle_id == CollectorCycleModel.cycle_id)
            .where(
                and_(
                    CollectorCycleModel.label == label,
                    CollectorCycleModel.session_date == session_date_value,
                )
            )
        )
        if bucket:
            statement = statement.where(CollectorCycleCandidateModel.bucket == bucket)
        statement = statement.order_by(
            CollectorCycleModel.generated_at.asc(),
            CollectorCycleCandidateModel.position.asc(),
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            to_collector_cycle_candidate_record(
                candidate,
                label=cycle.label,
                session_date=cycle.session_date,
                generated_at=cycle.generated_at,
            )
            for candidate, cycle in rows
        ]

    def list_events(
        self,
        label: str,
        session_date: str,
        limit: int = 500,
        *,
        ascending: bool = False,
    ) -> list[CollectorCycleEventRecord]:
        order_column = CollectorCycleEventModel.generated_at.asc() if ascending else CollectorCycleEventModel.generated_at.desc()
        statement = (
            select(CollectorCycleEventModel)
            .where(CollectorCycleEventModel.label == label)
            .where(CollectorCycleEventModel.session_date == date.fromisoformat(session_date))
            .order_by(order_column, CollectorCycleEventModel.event_id.asc())
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_collector_cycle_event_record(row) for row in rows]

    def list_cycle_events(self, cycle_id: str) -> list[CollectorCycleEventRecord]:
        statement = (
            select(CollectorCycleEventModel)
            .where(CollectorCycleEventModel.cycle_id == cycle_id)
            .order_by(CollectorCycleEventModel.generated_at.asc(), CollectorCycleEventModel.event_id.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [to_collector_cycle_event_record(row) for row in rows]

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(CollectorCycleEventModel))
            session.execute(delete(CollectorCycleCandidateModel))
            session.execute(delete(CollectorCycleModel))

    def close(self) -> None:
        self.engine.dispose()
