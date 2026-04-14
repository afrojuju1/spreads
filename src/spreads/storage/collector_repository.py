from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, func, select

from spreads.storage.base import RepositoryBase
from spreads.storage.collector_models import (
    CollectorCycleCandidateModel,
    CollectorCycleEventModel,
    CollectorCycleModel,
    PipelineCycleModel,
    PipelineModel,
)
from spreads.services.runtime_identity import (
    build_pipeline_id,
    resolve_pipeline_policy_fields,
)
from spreads.storage.records import (
    CollectorCycleCandidateRecord,
    CollectorCycleEventRecord,
    CollectorCycleRecord,
    PipelineCycleRecord,
    PipelineRecord,
)
from spreads.storage.serializers import parse_date, parse_datetime

NEW_YORK = ZoneInfo("America/New_York")


class CollectorRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables(
            "collector_cycles",
            "collector_cycle_candidates",
            "collector_cycle_events",
        )

    def pipeline_schema_ready(self) -> bool:
        return self.schema_has_tables("pipelines", "pipeline_cycles")

    def _cycle_candidate_row(
        self,
        model: CollectorCycleCandidateModel,
        *,
        label: str,
        session_date: date,
        generated_at: datetime,
    ) -> CollectorCycleCandidateRecord:
        return self.row(
            model,
            extra={
                "label": label,
                "session_date": session_date,
                "generated_at": generated_at,
            },
        )

    def save_cycle(
        self,
        *,
        cycle_id: str,
        label: str,
        generated_at: str,
        job_run_id: str | None = None,
        session_id: str | None = None,
        universe_label: str,
        strategy: str,
        profile: str,
        greeks_source: str,
        symbols: list[str],
        failures: list[dict[str, Any]],
        selection_memory: dict[str, Any],
        opportunities: list[dict[str, Any]],
        events: list[dict[str, Any]],
    ) -> None:
        generated_at_dt = parse_datetime(generated_at)
        if generated_at_dt is None:
            raise ValueError("generated_at is required")
        session_date = generated_at_dt.astimezone(NEW_YORK).date()
        pipeline_id = build_pipeline_id(label)
        policy_fields = resolve_pipeline_policy_fields(
            profile=profile,
            universe_label=universe_label,
        )

        def build_candidate_models(
            payloads: list[dict[str, Any]],
        ) -> list[CollectorCycleCandidateModel]:
            models: list[CollectorCycleCandidateModel] = []
            for payload in payloads:
                run_id = payload.get("run_id")
                if not run_id:
                    raise ValueError("Persisted opportunity is missing run_id")
                candidate_payload = payload.get("candidate")
                if isinstance(candidate_payload, dict):
                    stored_candidate = dict(candidate_payload)
                else:
                    stored_candidate = {
                        key: value
                        for key, value in payload.items()
                        if key
                        not in {
                            "selection_state",
                            "selection_rank",
                            "state_reason",
                            "origin",
                            "eligibility",
                            "candidate",
                        }
                    }
                models.append(
                    CollectorCycleCandidateModel(
                        cycle_id=cycle_id,
                        selection_state=str(payload["selection_state"]),
                        selection_rank=int(payload["selection_rank"]),
                        state_reason=str(payload["state_reason"]),
                        origin=str(payload["origin"]),
                        eligibility=str(payload["eligibility"]),
                        run_id=str(run_id),
                        underlying_symbol=str(payload["underlying_symbol"]),
                        strategy=str(payload["strategy"]),
                        expiration_date=parse_date(payload["expiration_date"]),
                        short_symbol=str(payload["short_symbol"]),
                        long_symbol=str(payload["long_symbol"]),
                        quality_score=float(payload["quality_score"]),
                        midpoint_credit=float(payload["midpoint_credit"]),
                        candidate_json=stored_candidate,
                    )
                )
            return models

        cycle = CollectorCycleModel(
            cycle_id=cycle_id,
            label=label,
            session_date=session_date,
            generated_at=generated_at_dt,
            job_run_id=job_run_id,
            session_id=session_id,
            universe_label=universe_label,
            strategy=strategy,
            profile=profile,
            greeks_source=greeks_source,
            symbols_json=symbols,
            failures_json=failures,
            selection_memory_json=selection_memory,
            candidates=build_candidate_models(opportunities),
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
        pipeline = PipelineModel(
            pipeline_id=pipeline_id,
            label=label,
            name=label,
            enabled=True,
            universe_label=universe_label,
            style_profile=str(policy_fields["style_profile"]),
            default_horizon_intent=str(policy_fields["horizon_intent"]),
            strategy_families_json=[strategy],
            product_scope_json={
                "product_class": str(policy_fields["product_class"]),
                "legacy_labels": [label],
            },
            policy_json={
                "legacy_profile": profile,
                "strategy_mode": strategy,
                "greeks_source": greeks_source,
            },
            created_at=generated_at_dt,
            updated_at=generated_at_dt,
        )
        pipeline_cycle = PipelineCycleModel(
            cycle_id=cycle_id,
            pipeline_id=pipeline_id,
            label=label,
            market_date=session_date,
            generated_at=generated_at_dt,
            job_run_id=job_run_id,
            universe_label=universe_label,
            strategy_mode=strategy,
            legacy_profile=profile,
            greeks_source=greeks_source,
            symbols_json=symbols,
            failures_json=failures,
            selection_memory_json=selection_memory,
            summary_json={
                "candidate_count": len(opportunities),
                "promotable_count": sum(
                    1
                    for payload in opportunities
                    if str(payload.get("selection_state") or "") == "promotable"
                ),
                "monitor_count": sum(
                    1
                    for payload in opportunities
                    if str(payload.get("selection_state") or "") == "monitor"
                ),
                "failure_count": len(failures),
                "event_count": len(events),
            },
        )

        with self.session_scope() as session:
            session.merge(pipeline)
            session.merge(cycle)
            session.merge(pipeline_cycle)

    def get_cycle(self, cycle_id: str) -> CollectorCycleRecord | None:
        with self.session_factory() as session:
            cycle = session.get(CollectorCycleModel, cycle_id)
        if cycle is None:
            return None
        return self.row(cycle)

    def get_latest_cycle(self, label: str) -> CollectorCycleRecord | None:
        statement = (
            select(CollectorCycleModel)
            .where(CollectorCycleModel.label == label)
            .order_by(
                CollectorCycleModel.generated_at.desc(),
                CollectorCycleModel.cycle_id.desc(),
            )
            .limit(1)
        )
        with self.session_factory() as session:
            cycle = session.scalar(statement)
        if cycle is None:
            return None
        return self.row(cycle)

    def list_cycles(
        self,
        label: str,
        session_date: str | None = None,
        limit: int = 100,
    ) -> list[CollectorCycleRecord]:
        statement = select(CollectorCycleModel).where(
            CollectorCycleModel.label == label
        )
        if session_date:
            statement = statement.where(
                CollectorCycleModel.session_date == date.fromisoformat(session_date)
            )
        statement = statement.order_by(CollectorCycleModel.generated_at.desc()).limit(
            limit
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_session_labels(
        self,
        *,
        session_date: str | None = None,
        limit: int | None = None,
    ) -> list[str]:
        statement = select(CollectorCycleModel.label).distinct()
        if session_date:
            statement = statement.where(
                CollectorCycleModel.session_date == date.fromisoformat(session_date)
            )
        statement = statement.order_by(CollectorCycleModel.label.asc())
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [str(row) for row in rows]

    def list_session_ids(
        self,
        *,
        session_date: str | None = None,
        limit: int | None = None,
    ) -> list[str]:
        statement = (
            select(CollectorCycleModel.session_id)
            .distinct()
            .where(CollectorCycleModel.session_id.is_not(None))
        )
        if session_date:
            statement = statement.where(
                CollectorCycleModel.session_date == date.fromisoformat(session_date)
            )
        statement = statement.order_by(CollectorCycleModel.session_id.desc())
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [str(row) for row in rows if row]

    def get_latest_session_cycle(self, session_id: str) -> CollectorCycleRecord | None:
        statement = (
            select(CollectorCycleModel)
            .where(CollectorCycleModel.session_id == session_id)
            .order_by(
                CollectorCycleModel.generated_at.desc(),
                CollectorCycleModel.cycle_id.desc(),
            )
            .limit(1)
        )
        with self.session_factory() as session:
            cycle = session.scalar(statement)
        if cycle is None:
            return None
        return self.row(cycle)

    def get_pipeline(self, pipeline_id: str) -> PipelineRecord | None:
        with self.session_factory() as session:
            row = session.get(PipelineModel, pipeline_id)
        if row is None:
            return None
        return self.row(row)

    def list_pipelines(
        self,
        *,
        limit: int = 100,
        enabled_only: bool = False,
    ) -> list[PipelineRecord]:
        statement = select(PipelineModel)
        if enabled_only:
            statement = statement.where(PipelineModel.enabled.is_(True))
        statement = statement.order_by(PipelineModel.updated_at.desc(), PipelineModel.pipeline_id.asc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def get_latest_pipeline_cycle(
        self,
        pipeline_id: str,
        *,
        market_date: str | None = None,
    ) -> PipelineCycleRecord | None:
        statement = select(PipelineCycleModel).where(PipelineCycleModel.pipeline_id == pipeline_id)
        if market_date:
            statement = statement.where(PipelineCycleModel.market_date == date.fromisoformat(market_date))
        statement = statement.order_by(
            PipelineCycleModel.generated_at.desc(),
            PipelineCycleModel.cycle_id.desc(),
        ).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return self.row(row)

    def list_pipeline_cycles(
        self,
        *,
        pipeline_id: str,
        market_date: str | None = None,
        limit: int = 100,
    ) -> list[PipelineCycleRecord]:
        statement = select(PipelineCycleModel).where(PipelineCycleModel.pipeline_id == pipeline_id)
        if market_date:
            statement = statement.where(PipelineCycleModel.market_date == date.fromisoformat(market_date))
        statement = statement.order_by(PipelineCycleModel.generated_at.desc(), PipelineCycleModel.cycle_id.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_latest_cycles_by_pipeline_ids(
        self,
        pipeline_ids: list[str],
    ) -> list[PipelineCycleRecord]:
        if not pipeline_ids:
            return []
        ranked_cycles = (
            select(
                PipelineCycleModel.cycle_id.label("cycle_id"),
                func.row_number()
                .over(
                    partition_by=PipelineCycleModel.pipeline_id,
                    order_by=(
                        PipelineCycleModel.generated_at.desc(),
                        PipelineCycleModel.cycle_id.desc(),
                    ),
                )
                .label("cycle_rank"),
            )
            .where(PipelineCycleModel.pipeline_id.in_(pipeline_ids))
            .subquery()
        )
        statement = (
            select(PipelineCycleModel)
            .join(ranked_cycles, PipelineCycleModel.cycle_id == ranked_cycles.c.cycle_id)
            .where(ranked_cycles.c.cycle_rank == 1)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_latest_cycles_by_session_ids(
        self,
        session_ids: list[str],
    ) -> list[CollectorCycleRecord]:
        if not session_ids:
            return []
        ranked_cycles = (
            select(
                CollectorCycleModel.cycle_id.label("cycle_id"),
                func.row_number()
                .over(
                    partition_by=CollectorCycleModel.session_id,
                    order_by=(
                        CollectorCycleModel.generated_at.desc(),
                        CollectorCycleModel.cycle_id.desc(),
                    ),
                )
                .label("cycle_rank"),
            )
            .where(CollectorCycleModel.session_id.in_(session_ids))
            .subquery()
        )
        statement = (
            select(CollectorCycleModel)
            .join(
                ranked_cycles, CollectorCycleModel.cycle_id == ranked_cycles.c.cycle_id
            )
            .where(ranked_cycles.c.cycle_rank == 1)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def count_cycle_candidates_by_cycle_ids(
        self,
        cycle_ids: list[str],
    ) -> dict[str, dict[str, int]]:
        if not cycle_ids:
            return {}
        statement = (
            select(
                CollectorCycleCandidateModel.cycle_id,
                CollectorCycleCandidateModel.selection_state,
                func.count().label("candidate_count"),
            )
            .where(CollectorCycleCandidateModel.cycle_id.in_(cycle_ids))
            .where(CollectorCycleCandidateModel.eligibility == "live")
            .group_by(
                CollectorCycleCandidateModel.cycle_id,
                CollectorCycleCandidateModel.selection_state,
            )
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        counts: dict[str, dict[str, int]] = {}
        for cycle_id, selection_state, candidate_count in rows:
            counts.setdefault(str(cycle_id), {})[str(selection_state)] = int(
                candidate_count or 0
            )
        return counts

    def list_cycle_candidates(
        self,
        cycle_id: str,
        selection_state: str | None = None,
        *,
        eligibility: str | None = None,
    ) -> list[CollectorCycleCandidateRecord]:
        statement = (
            select(CollectorCycleCandidateModel, CollectorCycleModel)
            .join(
                CollectorCycleModel,
                CollectorCycleCandidateModel.cycle_id == CollectorCycleModel.cycle_id,
            )
            .where(CollectorCycleCandidateModel.cycle_id == cycle_id)
        )
        if selection_state:
            statement = statement.where(
                CollectorCycleCandidateModel.selection_state == selection_state
            )
        if eligibility:
            statement = statement.where(
                CollectorCycleCandidateModel.eligibility == eligibility
            )
        statement = statement.order_by(
            CollectorCycleCandidateModel.selection_rank.asc(),
            CollectorCycleCandidateModel.candidate_id.asc(),
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            self._cycle_candidate_row(
                candidate,
                label=cycle.label,
                session_date=cycle.session_date,
                generated_at=cycle.generated_at,
            )
            for candidate, cycle in rows
        ]

    def get_candidate(self, candidate_id: int) -> CollectorCycleCandidateRecord | None:
        statement = (
            select(CollectorCycleCandidateModel, CollectorCycleModel)
            .join(
                CollectorCycleModel,
                CollectorCycleCandidateModel.cycle_id == CollectorCycleModel.cycle_id,
            )
            .where(CollectorCycleCandidateModel.candidate_id == candidate_id)
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.execute(statement).first()
        if row is None:
            return None
        candidate, cycle = row
        return self._cycle_candidate_row(
            candidate,
            label=cycle.label,
            session_date=cycle.session_date,
            generated_at=cycle.generated_at,
        )

    def list_session_candidates(
        self,
        *,
        label: str,
        session_date: str,
        selection_state: str | None = None,
        eligibility: str | None = None,
    ) -> list[CollectorCycleCandidateRecord]:
        session_date_value = date.fromisoformat(session_date)
        statement = (
            select(CollectorCycleCandidateModel, CollectorCycleModel)
            .join(
                CollectorCycleModel,
                CollectorCycleCandidateModel.cycle_id == CollectorCycleModel.cycle_id,
            )
            .where(
                and_(
                    CollectorCycleModel.label == label,
                    CollectorCycleModel.session_date == session_date_value,
                )
            )
        )
        if selection_state:
            statement = statement.where(
                CollectorCycleCandidateModel.selection_state == selection_state
            )
        if eligibility:
            statement = statement.where(
                CollectorCycleCandidateModel.eligibility == eligibility
            )
        statement = statement.order_by(
            CollectorCycleModel.generated_at.asc(),
            CollectorCycleCandidateModel.selection_rank.asc(),
            CollectorCycleCandidateModel.candidate_id.asc(),
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            self._cycle_candidate_row(
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
        order_column = (
            CollectorCycleEventModel.generated_at.asc()
            if ascending
            else CollectorCycleEventModel.generated_at.desc()
        )
        statement = (
            select(CollectorCycleEventModel)
            .where(CollectorCycleEventModel.label == label)
            .where(
                CollectorCycleEventModel.session_date
                == date.fromisoformat(session_date)
            )
            .order_by(order_column, CollectorCycleEventModel.event_id.asc())
            .limit(limit)
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_cycle_events(self, cycle_id: str) -> list[CollectorCycleEventRecord]:
        statement = (
            select(CollectorCycleEventModel)
            .where(CollectorCycleEventModel.cycle_id == cycle_id)
            .order_by(
                CollectorCycleEventModel.generated_at.asc(),
                CollectorCycleEventModel.event_id.asc(),
            )
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(CollectorCycleEventModel))
            session.execute(delete(CollectorCycleCandidateModel))
            session.execute(delete(CollectorCycleModel))
