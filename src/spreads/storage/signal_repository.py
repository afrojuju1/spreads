from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select

from spreads.storage.base import RepositoryBase
from spreads.storage.records import OpportunityRecord, SignalStateRecord, SignalStateTransitionRecord
from spreads.storage.serializers import parse_date, parse_datetime
from spreads.storage.signal_models import OpportunityModel, SignalStateModel, SignalStateTransitionModel


def _state_snapshot(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload["label"],
        payload["strategy_family"],
        payload["profile"],
        payload["entity_type"],
        payload["entity_key"],
        payload["underlying_symbol"],
        payload["state"],
        payload.get("confidence"),
        tuple(payload.get("reason_codes") or ()),
        tuple(payload.get("blockers") or ()),
        dict(payload.get("evidence") or {}),
        payload.get("active_cycle_id"),
        payload.get("active_candidate_id"),
        payload.get("active_selection_state"),
        payload.get("opportunity_id"),
        str(payload["session_date"]),
        payload["market_session"],
        payload.get("expires_at"),
    )


def _state_model_snapshot(row: SignalStateModel) -> tuple[Any, ...]:
    return (
        row.label,
        row.strategy_family,
        row.profile,
        row.entity_type,
        row.entity_key,
        row.underlying_symbol,
        row.state,
        row.confidence,
        tuple(row.reason_codes_json or ()),
        tuple(row.blockers_json or ()),
        dict(row.evidence_json or {}),
        row.active_cycle_id,
        row.active_candidate_id,
        row.active_selection_state,
        row.opportunity_id,
        str(row.session_date),
        row.market_session,
        None if row.expires_at is None else row.expires_at.isoformat(),
    )


def _opportunity_snapshot(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload["label"],
        str(payload["session_date"]),
        payload["strategy_family"],
        payload["profile"],
        payload["entity_type"],
        payload["entity_key"],
        payload["underlying_symbol"],
        payload.get("side"),
        payload["selection_state"],
        payload.get("selection_rank"),
        payload.get("state_reason"),
        payload.get("origin"),
        payload.get("eligibility"),
        payload.get("confidence"),
        payload.get("signal_state_ref"),
        payload["lifecycle_state"],
        payload.get("expires_at"),
        tuple(payload.get("reason_codes") or ()),
        tuple(payload.get("blockers") or ()),
        dict(payload.get("execution_shape") or {}),
        dict(payload.get("risk_hints") or {}),
        payload.get("source_cycle_id"),
        payload.get("source_candidate_id"),
        payload.get("source_selection_state"),
        payload.get("candidate_identity"),
        dict(payload.get("candidate") or {}),
        payload.get("consumed_by_execution_attempt_id"),
    )


def _opportunity_model_snapshot(row: OpportunityModel) -> tuple[Any, ...]:
    return (
        row.label,
        str(row.session_date),
        row.strategy_family,
        row.profile,
        row.entity_type,
        row.entity_key,
        row.underlying_symbol,
        row.side,
        row.selection_state,
        row.selection_rank,
        row.state_reason,
        row.origin,
        row.eligibility,
        row.confidence,
        row.signal_state_ref,
        row.lifecycle_state,
        None if row.expires_at is None else row.expires_at.isoformat(),
        tuple(row.reason_codes_json or ()),
        tuple(row.blockers_json or ()),
        dict(row.execution_shape_json or {}),
        dict(row.risk_hints_json or {}),
        row.source_cycle_id,
        row.source_candidate_id,
        row.source_selection_state,
        row.candidate_identity,
        dict(row.candidate_json or {}),
        row.consumed_by_execution_attempt_id,
    )


class SignalRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables(
            "signal_states",
            "signal_state_transitions",
            "opportunities",
        )

    def get_signal_state(self, signal_state_id: str) -> SignalStateRecord | None:
        with self.session_factory() as session:
            row = session.get(SignalStateModel, signal_state_id)
        if row is None:
            return None
        return self.row(row)

    def upsert_signal_state(
        self,
        *,
        signal_state_id: str,
        label: str,
        strategy_family: str,
        profile: str,
        entity_type: str,
        entity_key: str,
        underlying_symbol: str,
        state: str,
        confidence: float | None,
        reason_codes: list[str],
        blockers: list[str],
        evidence: dict[str, Any],
        active_cycle_id: str | None,
        active_candidate_id: int | None,
        active_selection_state: str | None,
        opportunity_id: str | None,
        session_date: str | date,
        market_session: str,
        observed_at: str,
        expires_at: str | None = None,
    ) -> tuple[SignalStateRecord, SignalStateTransitionRecord | None, bool]:
        observed_at_dt = parse_datetime(observed_at)
        if observed_at_dt is None:
            raise ValueError("observed_at is required")
        expires_at_dt = parse_datetime(expires_at)
        session_date_value = parse_date(session_date)
        with self.session_scope() as session:
            row = session.get(SignalStateModel, signal_state_id)
            previous_state = None if row is None else row.state
            semantic_changed = True
            if row is None:
                row = SignalStateModel(
                    signal_state_id=signal_state_id,
                    first_observed_at=observed_at_dt,
                    last_observed_at=observed_at_dt,
                    updated_at=observed_at_dt,
                    label=label,
                    strategy_family=strategy_family,
                    profile=profile,
                    entity_type=entity_type,
                    entity_key=entity_key,
                    underlying_symbol=underlying_symbol,
                    state=state,
                    confidence=confidence,
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    evidence_json=dict(evidence),
                    active_cycle_id=active_cycle_id,
                    active_candidate_id=active_candidate_id,
                    active_selection_state=active_selection_state,
                    opportunity_id=opportunity_id,
                    session_date=session_date_value,
                    market_session=market_session,
                    expires_at=expires_at_dt,
                )
                session.add(row)
            else:
                semantic_changed = _state_model_snapshot(row) != _state_snapshot(
                    {
                        "label": label,
                        "strategy_family": strategy_family,
                        "profile": profile,
                        "entity_type": entity_type,
                        "entity_key": entity_key,
                        "underlying_symbol": underlying_symbol,
                        "state": state,
                        "confidence": confidence,
                        "reason_codes": reason_codes,
                        "blockers": blockers,
                        "evidence": evidence,
                        "active_cycle_id": active_cycle_id,
                        "active_candidate_id": active_candidate_id,
                        "active_selection_state": active_selection_state,
                        "opportunity_id": opportunity_id,
                        "session_date": session_date_value,
                        "market_session": market_session,
                        "expires_at": None if expires_at_dt is None else expires_at_dt.isoformat(),
                    }
                )
                row.label = label
                row.strategy_family = strategy_family
                row.profile = profile
                row.entity_type = entity_type
                row.entity_key = entity_key
                row.underlying_symbol = underlying_symbol
                row.state = state
                row.confidence = confidence
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.evidence_json = dict(evidence)
                row.active_cycle_id = active_cycle_id
                row.active_candidate_id = active_candidate_id
                row.active_selection_state = active_selection_state
                row.opportunity_id = opportunity_id
                row.session_date = session_date_value
                row.market_session = market_session
                row.last_observed_at = observed_at_dt
                row.updated_at = observed_at_dt
                row.expires_at = expires_at_dt

            transition_row: SignalStateTransitionModel | None = None
            if previous_state != state:
                transition_row = SignalStateTransitionModel(
                    signal_state_id=signal_state_id,
                    label=label,
                    strategy_family=strategy_family,
                    profile=profile,
                    entity_type=entity_type,
                    entity_key=entity_key,
                    underlying_symbol=underlying_symbol,
                    from_state=previous_state,
                    to_state=state,
                    confidence=confidence,
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    evidence_json=dict(evidence),
                    active_cycle_id=active_cycle_id,
                    active_candidate_id=active_candidate_id,
                    active_selection_state=active_selection_state,
                    opportunity_id=opportunity_id,
                    session_date=session_date_value,
                    market_session=market_session,
                    occurred_at=observed_at_dt,
                )
                session.add(transition_row)

            session.flush()
            session.refresh(row)
            if transition_row is not None:
                session.refresh(transition_row)
            return (
                self.row(row),
                None if transition_row is None else self.row(transition_row),
                semantic_changed,
            )

    def list_signal_states(
        self,
        *,
        label: str | None = None,
        session_date: str | None = None,
        state: str | None = None,
        underlying_symbol: str | None = None,
        limit: int = 200,
    ) -> list[SignalStateRecord]:
        statement = select(SignalStateModel)
        if label:
            statement = statement.where(SignalStateModel.label == label)
        if session_date:
            statement = statement.where(SignalStateModel.session_date == date.fromisoformat(session_date))
        if state:
            statement = statement.where(SignalStateModel.state == state)
        if underlying_symbol:
            statement = statement.where(SignalStateModel.underlying_symbol == underlying_symbol.upper())
        statement = statement.order_by(SignalStateModel.updated_at.desc(), SignalStateModel.signal_state_id.asc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_signal_transitions(
        self,
        *,
        label: str | None = None,
        session_date: str | None = None,
        signal_state_id: str | None = None,
        underlying_symbol: str | None = None,
        limit: int = 200,
    ) -> list[SignalStateTransitionRecord]:
        statement = select(SignalStateTransitionModel)
        if label:
            statement = statement.where(SignalStateTransitionModel.label == label)
        if session_date:
            statement = statement.where(
                SignalStateTransitionModel.session_date == date.fromisoformat(session_date)
            )
        if signal_state_id:
            statement = statement.where(SignalStateTransitionModel.signal_state_id == signal_state_id)
        if underlying_symbol:
            statement = statement.where(
                SignalStateTransitionModel.underlying_symbol == underlying_symbol.upper()
            )
        statement = statement.order_by(
            SignalStateTransitionModel.occurred_at.desc(),
            SignalStateTransitionModel.transition_id.desc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def get_opportunity(self, opportunity_id: str) -> OpportunityRecord | None:
        with self.session_factory() as session:
            row = session.get(OpportunityModel, opportunity_id)
        if row is None:
            return None
        return self.row(row)

    def upsert_opportunity(
        self,
        *,
        opportunity_id: str,
        label: str,
        session_date: str | date,
        strategy_family: str,
        profile: str,
        entity_type: str,
        entity_key: str,
        underlying_symbol: str,
        side: str | None,
        selection_state: str,
        selection_rank: int | None,
        state_reason: str,
        origin: str,
        eligibility: str,
        confidence: float | None,
        signal_state_ref: str | None,
        lifecycle_state: str,
        created_at: str,
        updated_at: str,
        expires_at: str | None,
        reason_codes: list[str],
        blockers: list[str],
        execution_shape: dict[str, Any],
        risk_hints: dict[str, Any],
        source_cycle_id: str | None,
        source_candidate_id: int | None,
        source_selection_state: str | None,
        candidate_identity: str | None,
        candidate: dict[str, Any],
        consumed_by_execution_attempt_id: str | None = None,
    ) -> tuple[OpportunityRecord, bool]:
        created_at_dt = parse_datetime(created_at)
        updated_at_dt = parse_datetime(updated_at)
        expires_at_dt = parse_datetime(expires_at)
        if created_at_dt is None or updated_at_dt is None:
            raise ValueError("created_at and updated_at are required")
        session_date_value = parse_date(session_date)
        with self.session_scope() as session:
            row = session.get(OpportunityModel, opportunity_id)
            semantic_changed = True
            if row is None:
                row = OpportunityModel(
                    opportunity_id=opportunity_id,
                    label=label,
                    session_date=session_date_value,
                    strategy_family=strategy_family,
                    profile=profile,
                    entity_type=entity_type,
                    entity_key=entity_key,
                    underlying_symbol=underlying_symbol,
                    side=side,
                    selection_state=selection_state,
                    selection_rank=selection_rank,
                    state_reason=state_reason,
                    origin=origin,
                    eligibility=eligibility,
                    confidence=confidence,
                    signal_state_ref=signal_state_ref,
                    lifecycle_state=lifecycle_state,
                    created_at=created_at_dt,
                    updated_at=updated_at_dt,
                    expires_at=expires_at_dt,
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    execution_shape_json=dict(execution_shape),
                    risk_hints_json=dict(risk_hints),
                    source_cycle_id=source_cycle_id,
                    source_candidate_id=source_candidate_id,
                    source_selection_state=source_selection_state,
                    candidate_identity=candidate_identity,
                    candidate_json=dict(candidate),
                    consumed_by_execution_attempt_id=consumed_by_execution_attempt_id,
                )
                session.add(row)
            else:
                semantic_changed = _opportunity_model_snapshot(row) != _opportunity_snapshot(
                    {
                        "label": label,
                        "session_date": session_date_value,
                        "strategy_family": strategy_family,
                        "profile": profile,
                        "entity_type": entity_type,
                        "entity_key": entity_key,
                        "underlying_symbol": underlying_symbol,
                        "side": side,
                        "selection_state": selection_state,
                        "selection_rank": selection_rank,
                        "state_reason": state_reason,
                        "origin": origin,
                        "eligibility": eligibility,
                        "confidence": confidence,
                        "signal_state_ref": signal_state_ref,
                        "lifecycle_state": lifecycle_state,
                        "expires_at": None if expires_at_dt is None else expires_at_dt.isoformat(),
                        "reason_codes": reason_codes,
                        "blockers": blockers,
                        "execution_shape": execution_shape,
                        "risk_hints": risk_hints,
                        "source_cycle_id": source_cycle_id,
                        "source_candidate_id": source_candidate_id,
                        "source_selection_state": source_selection_state,
                        "candidate_identity": candidate_identity,
                        "candidate": candidate,
                        "consumed_by_execution_attempt_id": consumed_by_execution_attempt_id,
                    }
                )
                row.label = label
                row.session_date = session_date_value
                row.strategy_family = strategy_family
                row.profile = profile
                row.entity_type = entity_type
                row.entity_key = entity_key
                row.underlying_symbol = underlying_symbol
                row.side = side
                row.selection_state = selection_state
                row.selection_rank = selection_rank
                row.state_reason = state_reason
                row.origin = origin
                row.eligibility = eligibility
                row.confidence = confidence
                row.signal_state_ref = signal_state_ref
                row.lifecycle_state = lifecycle_state
                row.updated_at = updated_at_dt
                row.expires_at = expires_at_dt
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.execution_shape_json = dict(execution_shape)
                row.risk_hints_json = dict(risk_hints)
                row.source_cycle_id = source_cycle_id
                row.source_candidate_id = source_candidate_id
                row.source_selection_state = source_selection_state
                row.candidate_identity = candidate_identity
                row.candidate_json = dict(candidate)
                row.consumed_by_execution_attempt_id = consumed_by_execution_attempt_id

            session.flush()
            session.refresh(row)
            return self.row(row), semantic_changed

    def list_opportunities(
        self,
        *,
        label: str | None = None,
        session_date: str | None = None,
        lifecycle_state: str | None = None,
        underlying_symbol: str | None = None,
        strategy_family: str | None = None,
        limit: int = 200,
    ) -> list[OpportunityRecord]:
        statement = select(OpportunityModel)
        if label:
            statement = statement.where(OpportunityModel.label == label)
        if session_date:
            statement = statement.where(OpportunityModel.session_date == date.fromisoformat(session_date))
        if lifecycle_state:
            statement = statement.where(OpportunityModel.lifecycle_state == lifecycle_state)
        if underlying_symbol:
            statement = statement.where(OpportunityModel.underlying_symbol == underlying_symbol.upper())
        if strategy_family:
            statement = statement.where(OpportunityModel.strategy_family == strategy_family)
        statement = statement.order_by(OpportunityModel.updated_at.desc(), OpportunityModel.opportunity_id.asc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def expire_absent_opportunities(
        self,
        *,
        label: str,
        session_date: str | date,
        active_opportunity_ids: list[str],
        expired_at: str,
    ) -> list[OpportunityRecord]:
        session_date_value = parse_date(session_date)
        expired_at_dt = parse_datetime(expired_at)
        if expired_at_dt is None:
            raise ValueError("expired_at is required")
        statement = select(OpportunityModel).where(
            OpportunityModel.label == label,
            OpportunityModel.session_date == session_date_value,
            OpportunityModel.lifecycle_state.in_(("candidate", "ready", "blocked")),
        )
        if active_opportunity_ids:
            statement = statement.where(OpportunityModel.opportunity_id.not_in(active_opportunity_ids))
        with self.session_scope() as session:
            rows = session.scalars(statement).all()
            expired_rows: list[OpportunityModel] = []
            for row in rows:
                reason_codes = [str(value) for value in row.reason_codes_json or []]
                if "expired_cycle_absence" not in reason_codes:
                    reason_codes.append("expired_cycle_absence")
                row.lifecycle_state = "expired"
                row.reason_codes_json = reason_codes
                row.updated_at = expired_at_dt
                row.expires_at = expired_at_dt
                expired_rows.append(row)
            session.flush()
            for row in expired_rows:
                session.refresh(row)
            return self.rows(expired_rows)

    def find_active_opportunity_by_candidate_id(self, candidate_id: int) -> OpportunityRecord | None:
        statement = (
            select(OpportunityModel)
            .where(OpportunityModel.source_candidate_id == candidate_id)
            .where(OpportunityModel.lifecycle_state.in_(("candidate", "ready", "blocked")))
            .order_by(OpportunityModel.updated_at.desc(), OpportunityModel.opportunity_id.asc())
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return self.row(row)

    def mark_opportunity_consumed(
        self,
        *,
        opportunity_id: str,
        execution_attempt_id: str,
        consumed_at: str,
    ) -> tuple[OpportunityRecord | None, bool]:
        consumed_at_dt = parse_datetime(consumed_at)
        if consumed_at_dt is None:
            raise ValueError("consumed_at is required")
        with self.session_scope() as session:
            row = session.get(OpportunityModel, opportunity_id)
            if row is None:
                return None, False
            changed = (
                row.lifecycle_state != "consumed"
                or row.consumed_by_execution_attempt_id != execution_attempt_id
            )
            row.lifecycle_state = "consumed"
            row.consumed_by_execution_attempt_id = execution_attempt_id
            row.updated_at = consumed_at_dt
            if row.expires_at is None or row.expires_at > consumed_at_dt:
                row.expires_at = consumed_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row), changed
