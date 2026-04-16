from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import Integer, and_, delete, func, select

from core.storage.base import RepositoryBase
from core.storage.collector_models import (
    CollectorCycleCandidateModel,
    CollectorCycleEventModel,
    CollectorCycleModel,
)
from core.services.runtime_identity import (
    build_pipeline_id,
    parse_pipeline_id,
    resolve_pipeline_policy_fields,
)
from core.storage.records import (
    CollectorCycleCandidateRecord,
    CollectorCycleEventRecord,
    CollectorCycleRecord,
    PipelineCycleRecord,
    PipelineRecord,
)
from core.storage.serializers import parse_date, parse_datetime

NEW_YORK = ZoneInfo("America/New_York")


class CollectorRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables(
            "collector_cycles",
            "collector_cycle_candidates",
            "collector_cycle_events",
        )

    def pipeline_schema_ready(self) -> bool:
        return self.schema_ready()

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
    ) -> list[CollectorCycleCandidateRecord]:
        generated_at_dt = parse_datetime(generated_at)
        if generated_at_dt is None:
            raise ValueError("generated_at is required")
        session_date = generated_at_dt.astimezone(NEW_YORK).date()

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

        with self.session_scope() as session:
            session.merge(cycle)
        return self.list_cycle_candidates(cycle_id)

    def _build_pipeline_row(
        self,
        *,
        label: str,
        universe_label: str,
        strategy: str,
        profile: str,
        greeks_source: str,
        created_at: datetime,
        updated_at: datetime,
    ) -> PipelineRecord:
        policy_fields = resolve_pipeline_policy_fields(
            profile=profile,
            universe_label=universe_label,
        )
        return {
            "pipeline_id": build_pipeline_id(label),
            "label": label,
            "name": label,
            "source_job_key": None,
            "enabled": True,
            "universe_label": universe_label,
            "style_profile": str(policy_fields["style_profile"]),
            "default_horizon_intent": str(policy_fields["horizon_intent"]),
            "strategy_families": [strategy],
            "strategy_families_json": [strategy],
            "product_scope": {
                "product_class": str(policy_fields["product_class"]),
                "legacy_labels": [label],
            },
            "product_scope_json": {
                "product_class": str(policy_fields["product_class"]),
                "legacy_labels": [label],
            },
            "policy": {
                "legacy_profile": profile,
                "strategy_mode": strategy,
                "greeks_source": greeks_source,
            },
            "policy_json": {
                "legacy_profile": profile,
                "strategy_mode": strategy,
                "greeks_source": greeks_source,
            },
            "created_at": created_at,
            "updated_at": updated_at,
        }

    def _build_pipeline_cycle_row(
        self,
        cycle: CollectorCycleModel,
        *,
        candidate_count: int = 0,
        promotable_count: int = 0,
        monitor_count: int = 0,
        event_count: int = 0,
    ) -> PipelineCycleRecord:
        return {
            "cycle_id": cycle.cycle_id,
            "pipeline_id": build_pipeline_id(str(cycle.label)),
            "label": cycle.label,
            "market_date": cycle.session_date,
            "generated_at": cycle.generated_at,
            "job_run_id": cycle.job_run_id,
            "universe_label": cycle.universe_label,
            "strategy_mode": cycle.strategy,
            "legacy_profile": cycle.profile,
            "greeks_source": cycle.greeks_source,
            "symbols": list(cycle.symbols_json or []),
            "failures": list(cycle.failures_json or []),
            "selection_memory": dict(cycle.selection_memory_json or {}),
            "summary": {
                "candidate_count": candidate_count,
                "promotable_count": promotable_count,
                "monitor_count": monitor_count,
                "failure_count": len(cycle.failures_json or []),
                "event_count": event_count,
            },
        }

    def _resolve_pipeline_label(self, pipeline_id: str) -> str | None:
        parsed = parse_pipeline_id(pipeline_id)
        if parsed is None:
            return None
        return str(parsed["label"])

    def _latest_pipeline_cycles_by_label(
        self,
        labels: list[str] | None = None,
        *,
        limit: int | None = None,
    ) -> list[CollectorCycleModel]:
        ranked_cycles = (
            select(
                CollectorCycleModel.cycle_id.label("cycle_id"),
                func.row_number()
                .over(
                    partition_by=CollectorCycleModel.label,
                    order_by=(
                        CollectorCycleModel.generated_at.desc(),
                        CollectorCycleModel.cycle_id.desc(),
                    ),
                )
                .label("cycle_rank"),
            )
            .subquery()
        )
        statement = (
            select(CollectorCycleModel)
            .join(
                ranked_cycles,
                CollectorCycleModel.cycle_id == ranked_cycles.c.cycle_id,
            )
            .where(ranked_cycles.c.cycle_rank == 1)
        )
        if labels:
            statement = statement.where(CollectorCycleModel.label.in_(labels))
        statement = statement.order_by(
            CollectorCycleModel.generated_at.desc(),
            CollectorCycleModel.label.asc(),
        )
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            return session.scalars(statement).all()

    def _cycle_summary_by_cycle_ids(
        self,
        cycle_ids: list[str],
    ) -> dict[str, dict[str, int]]:
        if not cycle_ids:
            return {}
        candidate_rows = (
            select(
                CollectorCycleCandidateModel.cycle_id,
                func.count().label("candidate_count"),
                func.sum(
                    func.cast(
                        CollectorCycleCandidateModel.selection_state == "promotable",
                        Integer,
                    )
                ).label("promotable_count"),
                func.sum(
                    func.cast(
                        CollectorCycleCandidateModel.selection_state == "monitor",
                        Integer,
                    )
                ).label("monitor_count"),
            )
            .where(CollectorCycleCandidateModel.cycle_id.in_(cycle_ids))
            .group_by(CollectorCycleCandidateModel.cycle_id)
        )
        event_rows = (
            select(
                CollectorCycleEventModel.cycle_id,
                func.count().label("event_count"),
            )
            .where(CollectorCycleEventModel.cycle_id.in_(cycle_ids))
            .group_by(CollectorCycleEventModel.cycle_id)
        )
        summary: dict[str, dict[str, int]] = {}
        with self.session_factory() as session:
            for cycle_id, candidate_count, promotable_count, monitor_count in session.execute(
                candidate_rows
            ):
                summary[str(cycle_id)] = {
                    "candidate_count": int(candidate_count or 0),
                    "promotable_count": int(promotable_count or 0),
                    "monitor_count": int(monitor_count or 0),
                    "event_count": 0,
                }
            for cycle_id, event_count in session.execute(event_rows):
                summary.setdefault(str(cycle_id), {}).update(
                    {"event_count": int(event_count or 0)}
                )
        return summary

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
        label = self._resolve_pipeline_label(pipeline_id)
        if label is None:
            return None
        latest_cycle = self.get_latest_cycle(label)
        if latest_cycle is None:
            return None
        generated_at = parse_datetime(str(latest_cycle["generated_at"]))
        if generated_at is None:
            return None
        return self._build_pipeline_row(
            label=str(latest_cycle["label"]),
            universe_label=str(latest_cycle["universe_label"]),
            strategy=str(latest_cycle["strategy"]),
            profile=str(latest_cycle["profile"]),
            greeks_source=str(latest_cycle["greeks_source"]),
            created_at=generated_at,
            updated_at=generated_at,
        )

    def list_pipelines(
        self,
        *,
        limit: int = 100,
        enabled_only: bool = False,
    ) -> list[PipelineRecord]:
        del enabled_only
        rows = self._latest_pipeline_cycles_by_label(limit=limit)
        output: list[PipelineRecord] = []
        for cycle in rows:
            output.append(
                self._build_pipeline_row(
                    label=str(cycle.label),
                    universe_label=str(cycle.universe_label),
                    strategy=str(cycle.strategy),
                    profile=str(cycle.profile),
                    greeks_source=str(cycle.greeks_source),
                    created_at=cycle.generated_at,
                    updated_at=cycle.generated_at,
                )
            )
        return output

    def get_latest_pipeline_cycle(
        self,
        pipeline_id: str,
        *,
        market_date: str | None = None,
    ) -> PipelineCycleRecord | None:
        label = self._resolve_pipeline_label(pipeline_id)
        if label is None:
            return None
        statement = select(CollectorCycleModel).where(CollectorCycleModel.label == label)
        if market_date:
            statement = statement.where(
                CollectorCycleModel.session_date == date.fromisoformat(market_date)
            )
        statement = statement.order_by(
            CollectorCycleModel.generated_at.desc(),
            CollectorCycleModel.cycle_id.desc(),
        ).limit(1)
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        summary = self._cycle_summary_by_cycle_ids([str(row.cycle_id)]).get(
            str(row.cycle_id),
            {},
        )
        return self._build_pipeline_cycle_row(
            row,
            candidate_count=int(summary.get("candidate_count") or 0),
            promotable_count=int(summary.get("promotable_count") or 0),
            monitor_count=int(summary.get("monitor_count") or 0),
            event_count=int(summary.get("event_count") or 0),
        )

    def list_pipeline_cycles(
        self,
        *,
        pipeline_id: str,
        market_date: str | None = None,
        limit: int = 100,
    ) -> list[PipelineCycleRecord]:
        label = self._resolve_pipeline_label(pipeline_id)
        if label is None:
            return []
        statement = select(CollectorCycleModel).where(CollectorCycleModel.label == label)
        if market_date:
            statement = statement.where(
                CollectorCycleModel.session_date == date.fromisoformat(market_date)
            )
        statement = statement.order_by(
            CollectorCycleModel.generated_at.desc(),
            CollectorCycleModel.cycle_id.desc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        cycle_ids = [str(row.cycle_id) for row in rows]
        summary_by_cycle = self._cycle_summary_by_cycle_ids(cycle_ids)
        return [
            self._build_pipeline_cycle_row(
                row,
                candidate_count=int(
                    summary_by_cycle.get(str(row.cycle_id), {}).get("candidate_count")
                    or 0
                ),
                promotable_count=int(
                    summary_by_cycle.get(str(row.cycle_id), {}).get(
                        "promotable_count"
                    )
                    or 0
                ),
                monitor_count=int(
                    summary_by_cycle.get(str(row.cycle_id), {}).get("monitor_count")
                    or 0
                ),
                event_count=int(
                    summary_by_cycle.get(str(row.cycle_id), {}).get("event_count")
                    or 0
                ),
            )
            for row in rows
        ]

    def list_latest_cycles_by_pipeline_ids(
        self,
        pipeline_ids: list[str],
    ) -> list[PipelineCycleRecord]:
        if not pipeline_ids:
            return []
        labels = [
            resolved
            for pipeline_id in pipeline_ids
            if (resolved := self._resolve_pipeline_label(pipeline_id)) is not None
        ]
        if not labels:
            return []
        rows = self._latest_pipeline_cycles_by_label(labels)
        cycle_ids = [str(row.cycle_id) for row in rows]
        summary_by_cycle = self._cycle_summary_by_cycle_ids(cycle_ids)
        return [
            self._build_pipeline_cycle_row(
                row,
                candidate_count=int(
                    summary_by_cycle.get(str(row.cycle_id), {}).get("candidate_count")
                    or 0
                ),
                promotable_count=int(
                    summary_by_cycle.get(str(row.cycle_id), {}).get(
                        "promotable_count"
                    )
                    or 0
                ),
                monitor_count=int(
                    summary_by_cycle.get(str(row.cycle_id), {}).get("monitor_count")
                    or 0
                ),
                event_count=int(
                    summary_by_cycle.get(str(row.cycle_id), {}).get("event_count")
                    or 0
                ),
            )
            for row in rows
        ]

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
