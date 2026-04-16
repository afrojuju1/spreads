from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import func, select

from core.services.runtime_identity import build_pipeline_id
from core.storage.base import RepositoryBase
from core.storage.records import (
    AutomationRunRecord,
    OpportunityRecord,
    OpportunityDecisionRecord,
    SignalStateRecord,
    SignalStateTransitionRecord,
)
from core.storage.serializers import parse_date, parse_datetime
from core.storage.signal_models import (
    AutomationRunModel,
    OpportunityDecisionModel,
    OpportunityModel,
    SignalStateModel,
    SignalStateTransitionModel,
)


def _optional_date(value: str | date | None) -> date | None:
    if value in (None, ""):
        return None
    return parse_date(value)


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
        payload.get("pipeline_id"),
        payload["label"],
        str(payload.get("market_date") or payload["session_date"]),
        str(payload["session_date"]),
        payload.get("cycle_id"),
        payload.get("root_symbol"),
        payload.get("bot_id"),
        payload.get("automation_id"),
        payload.get("automation_run_id"),
        payload.get("strategy_config_id"),
        payload.get("strategy_id"),
        payload.get("config_hash"),
        dict(payload.get("policy_ref") or {}),
        payload["strategy_family"],
        payload["profile"],
        payload.get("style_profile"),
        payload.get("horizon_intent"),
        payload.get("product_class"),
        payload.get("expiration_date"),
        payload["entity_type"],
        payload["entity_key"],
        payload["underlying_symbol"],
        payload.get("side"),
        payload.get("side_bias"),
        payload["selection_state"],
        payload.get("selection_rank"),
        payload.get("state_reason"),
        payload.get("origin"),
        payload.get("eligibility"),
        payload.get("eligibility_state"),
        payload.get("promotion_score"),
        payload.get("execution_score"),
        payload.get("confidence"),
        payload.get("signal_state_ref"),
        payload["lifecycle_state"],
        payload.get("expires_at"),
        tuple(payload.get("reason_codes") or ()),
        tuple(payload.get("blockers") or ()),
        tuple(payload.get("legs") or ()),
        dict(payload.get("economics") or {}),
        dict(payload.get("strategy_metrics") or {}),
        dict(payload.get("order_payload") or {}),
        dict(payload.get("evidence") or {}),
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
        row.pipeline_id,
        row.label,
        str(row.market_date or row.session_date),
        str(row.session_date),
        row.cycle_id,
        row.root_symbol,
        row.bot_id,
        row.automation_id,
        row.automation_run_id,
        row.strategy_config_id,
        row.strategy_id,
        row.config_hash,
        dict(row.policy_ref_json or {}),
        row.strategy_family,
        row.profile,
        row.style_profile,
        row.horizon_intent,
        row.product_class,
        None if row.expiration_date is None else row.expiration_date.isoformat(),
        row.entity_type,
        row.entity_key,
        row.underlying_symbol,
        row.side,
        row.side_bias,
        row.selection_state,
        row.selection_rank,
        row.state_reason,
        row.origin,
        row.eligibility,
        row.eligibility_state,
        row.promotion_score,
        row.execution_score,
        row.confidence,
        row.signal_state_ref,
        row.lifecycle_state,
        None if row.expires_at is None else row.expires_at.isoformat(),
        tuple(row.reason_codes_json or ()),
        tuple(row.blockers_json or ()),
        tuple(row.legs_json or ()),
        dict(row.economics_json or {}),
        dict(row.strategy_metrics_json or {}),
        dict(row.order_payload_json or {}),
        dict(row.evidence_json or {}),
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

    def decision_schema_ready(self) -> bool:
        return self.schema_has_tables("opportunities", "opportunity_decisions")

    def automation_runtime_schema_ready(self) -> bool:
        return self.schema_has_tables("opportunities", "automation_runs")

    def upsert_automation_run(
        self,
        *,
        automation_run_id: str,
        bot_id: str,
        automation_id: str,
        strategy_config_id: str,
        trigger_type: str,
        job_run_id: str | None,
        cycle_id: str | None,
        label: str | None,
        session_date: str | date,
        started_at: str,
        completed_at: str | None,
        status: str,
        result: dict[str, Any] | None,
        config_hash: str,
    ) -> AutomationRunRecord:
        started_at_dt = parse_datetime(started_at)
        completed_at_dt = parse_datetime(completed_at)
        session_date_value = parse_date(session_date)
        if started_at_dt is None:
            raise ValueError("started_at is required")
        with self.session_scope() as session:
            row = session.get(AutomationRunModel, automation_run_id)
            if row is None:
                row = AutomationRunModel(
                    automation_run_id=automation_run_id,
                    bot_id=bot_id,
                    automation_id=automation_id,
                    strategy_config_id=strategy_config_id,
                    trigger_type=trigger_type,
                    job_run_id=job_run_id,
                    cycle_id=cycle_id,
                    label=label,
                    session_date=session_date_value,
                    started_at=started_at_dt,
                    completed_at=completed_at_dt,
                    status=status,
                    result_json=dict(result or {}),
                    config_hash=config_hash,
                )
                session.add(row)
            else:
                row.bot_id = bot_id
                row.automation_id = automation_id
                row.strategy_config_id = strategy_config_id
                row.trigger_type = trigger_type
                row.job_run_id = job_run_id
                row.cycle_id = cycle_id
                row.label = label
                row.session_date = session_date_value
                row.started_at = started_at_dt
                row.completed_at = completed_at_dt
                row.status = status
                row.result_json = dict(result or {})
                row.config_hash = config_hash
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_automation_runs(
        self,
        *,
        bot_id: str | None = None,
        automation_id: str | None = None,
        session_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        cycle_id: str | None = None,
        limit: int = 200,
    ) -> list[AutomationRunRecord]:
        statement = select(AutomationRunModel)
        if bot_id:
            statement = statement.where(AutomationRunModel.bot_id == bot_id)
        if automation_id:
            statement = statement.where(
                AutomationRunModel.automation_id == automation_id
            )
        if session_date:
            statement = statement.where(
                AutomationRunModel.session_date == date.fromisoformat(session_date)
            )
        if start_date:
            statement = statement.where(
                AutomationRunModel.session_date >= date.fromisoformat(start_date)
            )
        if end_date:
            statement = statement.where(
                AutomationRunModel.session_date <= date.fromisoformat(end_date)
            )
        if cycle_id:
            statement = statement.where(AutomationRunModel.cycle_id == cycle_id)
        statement = statement.order_by(
            AutomationRunModel.started_at.desc(),
            AutomationRunModel.automation_run_id.asc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

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
                        "expires_at": None
                        if expires_at_dt is None
                        else expires_at_dt.isoformat(),
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
            statement = statement.where(
                SignalStateModel.session_date == date.fromisoformat(session_date)
            )
        if state:
            statement = statement.where(SignalStateModel.state == state)
        if underlying_symbol:
            statement = statement.where(
                SignalStateModel.underlying_symbol == underlying_symbol.upper()
            )
        statement = statement.order_by(
            SignalStateModel.updated_at.desc(), SignalStateModel.signal_state_id.asc()
        ).limit(limit)
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
                SignalStateTransitionModel.session_date
                == date.fromisoformat(session_date)
            )
        if signal_state_id:
            statement = statement.where(
                SignalStateTransitionModel.signal_state_id == signal_state_id
            )
        if underlying_symbol:
            statement = statement.where(
                SignalStateTransitionModel.underlying_symbol
                == underlying_symbol.upper()
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

    def delete_opportunity(self, opportunity_id: str) -> bool:
        with self.session_scope() as session:
            row = session.get(OpportunityModel, opportunity_id)
            if row is None:
                return False
            session.delete(row)
            return True

    def upsert_opportunity(
        self,
        *,
        opportunity_id: str,
        pipeline_id: str | None = None,
        label: str,
        market_date: str | date | None = None,
        session_date: str | date,
        cycle_id: str | None = None,
        root_symbol: str | None = None,
        bot_id: str | None = None,
        automation_id: str | None = None,
        automation_run_id: str | None = None,
        strategy_config_id: str | None = None,
        strategy_id: str | None = None,
        config_hash: str | None = None,
        policy_ref: dict[str, Any] | None = None,
        strategy_family: str,
        profile: str,
        style_profile: str | None = None,
        horizon_intent: str | None = None,
        product_class: str | None = None,
        expiration_date: str | date | None = None,
        entity_type: str,
        entity_key: str,
        underlying_symbol: str,
        side: str | None,
        side_bias: str | None = None,
        selection_state: str,
        selection_rank: int | None,
        state_reason: str,
        origin: str,
        eligibility: str,
        eligibility_state: str | None = None,
        promotion_score: float | None = None,
        execution_score: float | None = None,
        confidence: float | None,
        signal_state_ref: str | None,
        lifecycle_state: str,
        created_at: str,
        updated_at: str,
        expires_at: str | None,
        reason_codes: list[str],
        blockers: list[str],
        legs: list[dict[str, Any]] | None = None,
        economics: dict[str, Any] | None = None,
        strategy_metrics: dict[str, Any] | None = None,
        order_payload: dict[str, Any] | None = None,
        evidence: dict[str, Any] | None = None,
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
        market_date_value = parse_date(market_date or session_date_value)
        expiration_date_value = _optional_date(expiration_date)
        with self.session_scope() as session:
            row = session.get(OpportunityModel, opportunity_id)
            semantic_changed = True
            if row is None:
                row = OpportunityModel(
                    opportunity_id=opportunity_id,
                    pipeline_id=pipeline_id or build_pipeline_id(label),
                    label=label,
                    market_date=market_date_value,
                    session_date=session_date_value,
                    cycle_id=cycle_id or source_cycle_id,
                    root_symbol=root_symbol or underlying_symbol,
                    bot_id=bot_id,
                    automation_id=automation_id,
                    automation_run_id=automation_run_id,
                    strategy_config_id=strategy_config_id,
                    strategy_id=strategy_id,
                    config_hash=config_hash,
                    policy_ref_json=dict(policy_ref or {}),
                    strategy_family=strategy_family,
                    profile=profile,
                    style_profile=style_profile,
                    horizon_intent=horizon_intent,
                    product_class=product_class,
                    expiration_date=expiration_date_value,
                    entity_type=entity_type,
                    entity_key=entity_key,
                    underlying_symbol=underlying_symbol,
                    side=side,
                    side_bias=side_bias or side,
                    selection_state=selection_state,
                    selection_rank=selection_rank,
                    state_reason=state_reason,
                    origin=origin,
                    eligibility=eligibility,
                    eligibility_state=eligibility_state or eligibility,
                    promotion_score=promotion_score,
                    execution_score=execution_score,
                    confidence=confidence,
                    signal_state_ref=signal_state_ref,
                    lifecycle_state=lifecycle_state,
                    created_at=created_at_dt,
                    updated_at=updated_at_dt,
                    expires_at=expires_at_dt,
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    legs_json=list(legs or []),
                    economics_json=dict(economics or {}),
                    strategy_metrics_json=dict(strategy_metrics or {}),
                    order_payload_json=dict(order_payload or {}),
                    evidence_json=dict(evidence or {}),
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
                semantic_changed = _opportunity_model_snapshot(
                    row
                ) != _opportunity_snapshot(
                    {
                        "pipeline_id": pipeline_id or build_pipeline_id(label),
                        "label": label,
                        "market_date": market_date_value,
                        "session_date": session_date_value,
                        "cycle_id": cycle_id or source_cycle_id,
                        "root_symbol": root_symbol or underlying_symbol,
                        "bot_id": bot_id,
                        "automation_id": automation_id,
                        "automation_run_id": automation_run_id,
                        "strategy_config_id": strategy_config_id,
                        "strategy_id": strategy_id,
                        "config_hash": config_hash,
                        "policy_ref": dict(policy_ref or {}),
                        "strategy_family": strategy_family,
                        "profile": profile,
                        "style_profile": style_profile,
                        "horizon_intent": horizon_intent,
                        "product_class": product_class,
                        "expiration_date": None
                        if expiration_date_value is None
                        else expiration_date_value.isoformat(),
                        "entity_type": entity_type,
                        "entity_key": entity_key,
                        "underlying_symbol": underlying_symbol,
                        "side": side,
                        "side_bias": side_bias or side,
                        "selection_state": selection_state,
                        "selection_rank": selection_rank,
                        "state_reason": state_reason,
                        "origin": origin,
                        "eligibility": eligibility,
                        "eligibility_state": eligibility_state or eligibility,
                        "promotion_score": promotion_score,
                        "execution_score": execution_score,
                        "confidence": confidence,
                        "signal_state_ref": signal_state_ref,
                        "lifecycle_state": lifecycle_state,
                        "expires_at": None
                        if expires_at_dt is None
                        else expires_at_dt.isoformat(),
                        "reason_codes": reason_codes,
                        "blockers": blockers,
                        "legs": list(legs or []),
                        "economics": dict(economics or {}),
                        "strategy_metrics": dict(strategy_metrics or {}),
                        "order_payload": dict(order_payload or {}),
                        "evidence": dict(evidence or {}),
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
                row.pipeline_id = pipeline_id or build_pipeline_id(label)
                row.label = label
                row.market_date = market_date_value
                row.session_date = session_date_value
                row.cycle_id = cycle_id or source_cycle_id
                row.root_symbol = root_symbol or underlying_symbol
                row.bot_id = bot_id
                row.automation_id = automation_id
                row.automation_run_id = automation_run_id
                row.strategy_config_id = strategy_config_id
                row.strategy_id = strategy_id
                row.config_hash = config_hash
                row.policy_ref_json = dict(policy_ref or {})
                row.strategy_family = strategy_family
                row.profile = profile
                row.style_profile = style_profile
                row.horizon_intent = horizon_intent
                row.product_class = product_class
                row.expiration_date = expiration_date_value
                row.entity_type = entity_type
                row.entity_key = entity_key
                row.underlying_symbol = underlying_symbol
                row.side = side
                row.side_bias = side_bias or side
                row.selection_state = selection_state
                row.selection_rank = selection_rank
                row.state_reason = state_reason
                row.origin = origin
                row.eligibility = eligibility
                row.eligibility_state = eligibility_state or eligibility
                row.promotion_score = promotion_score
                row.execution_score = execution_score
                row.confidence = confidence
                row.signal_state_ref = signal_state_ref
                row.lifecycle_state = lifecycle_state
                row.updated_at = updated_at_dt
                row.expires_at = expires_at_dt
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.legs_json = list(legs or [])
                row.economics_json = dict(economics or {})
                row.strategy_metrics_json = dict(strategy_metrics or {})
                row.order_payload_json = dict(order_payload or {})
                row.evidence_json = dict(evidence or {})
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
        pipeline_id: str | None = None,
        label: str | None = None,
        market_date: str | None = None,
        session_date: str | None = None,
        lifecycle_state: str | None = None,
        eligibility_state: str | None = None,
        underlying_symbol: str | None = None,
        strategy_family: str | None = None,
        bot_id: str | None = None,
        automation_id: str | None = None,
        strategy_config_id: str | None = None,
        automation_run_id: str | None = None,
        runtime_owned: bool | None = False,
        limit: int = 200,
    ) -> list[OpportunityRecord]:
        statement = select(OpportunityModel)
        if runtime_owned is False and not any(
            [bot_id, automation_id, strategy_config_id, automation_run_id]
        ):
            statement = statement.where(OpportunityModel.bot_id.is_(None))
        elif runtime_owned is True or any(
            [bot_id, automation_id, strategy_config_id, automation_run_id]
        ):
            statement = statement.where(OpportunityModel.bot_id.is_not(None))
        if pipeline_id:
            statement = statement.where(OpportunityModel.pipeline_id == pipeline_id)
        if label:
            statement = statement.where(OpportunityModel.label == label)
        if market_date:
            statement = statement.where(
                OpportunityModel.market_date == date.fromisoformat(market_date)
            )
        if session_date:
            statement = statement.where(
                OpportunityModel.session_date == date.fromisoformat(session_date)
            )
        if lifecycle_state:
            statement = statement.where(
                OpportunityModel.lifecycle_state == lifecycle_state
            )
        if eligibility_state:
            statement = statement.where(
                OpportunityModel.eligibility_state == eligibility_state
            )
        if underlying_symbol:
            statement = statement.where(
                OpportunityModel.underlying_symbol == underlying_symbol.upper()
            )
        if strategy_family:
            statement = statement.where(
                OpportunityModel.strategy_family == strategy_family
            )
        if bot_id:
            statement = statement.where(OpportunityModel.bot_id == bot_id)
        if automation_id:
            statement = statement.where(OpportunityModel.automation_id == automation_id)
        if strategy_config_id:
            statement = statement.where(
                OpportunityModel.strategy_config_id == strategy_config_id
            )
        if automation_run_id:
            statement = statement.where(
                OpportunityModel.automation_run_id == automation_run_id
            )
        statement = statement.order_by(
            OpportunityModel.updated_at.desc(), OpportunityModel.opportunity_id.asc()
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_active_cycle_opportunities(
        self,
        cycle_id: str,
        *,
        eligibility_state: str | None = None,
        exclude_consumed: bool = True,
        bot_id: str | None = None,
        automation_id: str | None = None,
        strategy_config_id: str | None = None,
        runtime_owned: bool | None = False,
        limit: int = 200,
    ) -> list[OpportunityRecord]:
        statement = select(OpportunityModel).where(
            OpportunityModel.cycle_id == cycle_id
        )
        if runtime_owned is False and not any(
            [bot_id, automation_id, strategy_config_id]
        ):
            statement = statement.where(OpportunityModel.bot_id.is_(None))
        elif runtime_owned is True or any([bot_id, automation_id, strategy_config_id]):
            statement = statement.where(OpportunityModel.bot_id.is_not(None))
        statement = statement.where(
            OpportunityModel.lifecycle_state.in_(("candidate", "ready", "blocked"))
        )
        if eligibility_state:
            statement = statement.where(
                OpportunityModel.eligibility_state == eligibility_state
            )
        if exclude_consumed:
            statement = statement.where(
                OpportunityModel.consumed_by_execution_attempt_id.is_(None)
            )
        if bot_id:
            statement = statement.where(OpportunityModel.bot_id == bot_id)
        if automation_id:
            statement = statement.where(OpportunityModel.automation_id == automation_id)
        if strategy_config_id:
            statement = statement.where(
                OpportunityModel.strategy_config_id == strategy_config_id
            )
        statement = statement.order_by(
            OpportunityModel.updated_at.desc(), OpportunityModel.opportunity_id.asc()
        ).limit(limit)
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
        bot_id: str | None = None,
        automation_id: str | None = None,
        strategy_config_id: str | None = None,
        runtime_owned: bool | None = False,
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
        if runtime_owned is False and not any(
            [bot_id, automation_id, strategy_config_id]
        ):
            statement = statement.where(OpportunityModel.bot_id.is_(None))
        elif runtime_owned is True or any([bot_id, automation_id, strategy_config_id]):
            statement = statement.where(OpportunityModel.bot_id.is_not(None))
        if bot_id:
            statement = statement.where(OpportunityModel.bot_id == bot_id)
        if automation_id:
            statement = statement.where(OpportunityModel.automation_id == automation_id)
        if strategy_config_id:
            statement = statement.where(
                OpportunityModel.strategy_config_id == strategy_config_id
            )
        if active_opportunity_ids:
            statement = statement.where(
                OpportunityModel.opportunity_id.not_in(active_opportunity_ids)
            )
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

    def find_active_opportunity_by_candidate_id(
        self,
        candidate_id: int,
        *,
        runtime_owned: bool | None = False,
    ) -> OpportunityRecord | None:
        statement = (
            select(OpportunityModel)
            .where(OpportunityModel.source_candidate_id == candidate_id)
            .where(
                OpportunityModel.lifecycle_state.in_(("candidate", "ready", "blocked"))
            )
            .where(
                OpportunityModel.bot_id.is_not(None)
                if runtime_owned
                else OpportunityModel.bot_id.is_(None)
            )
            .order_by(
                OpportunityModel.updated_at.desc(),
                OpportunityModel.opportunity_id.asc(),
            )
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return self.row(row)

    def count_active_cycle_opportunities_by_cycle_ids(
        self,
        cycle_ids: list[str],
        *,
        exclude_consumed: bool = True,
        runtime_owned: bool | None = False,
    ) -> dict[str, dict[str, int]]:
        if not cycle_ids:
            return {}
        statement = (
            select(
                OpportunityModel.cycle_id,
                OpportunityModel.selection_state,
                func.count().label("row_count"),
            )
            .where(OpportunityModel.cycle_id.in_(cycle_ids))
            .where(
                OpportunityModel.lifecycle_state.in_(("candidate", "ready", "blocked"))
            )
        )
        statement = statement.where(
            OpportunityModel.bot_id.is_not(None)
            if runtime_owned
            else OpportunityModel.bot_id.is_(None)
        )
        if exclude_consumed:
            statement = statement.where(
                OpportunityModel.consumed_by_execution_attempt_id.is_(None)
            )
        statement = statement.group_by(
            OpportunityModel.cycle_id,
            OpportunityModel.selection_state,
        )
        counts: dict[str, dict[str, int]] = {}
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        for cycle_id, selection_state, row_count in rows:
            cycle_counts = counts.setdefault(
                str(cycle_id),
                {
                    "candidate_count": 0,
                    "promotable": 0,
                    "monitor": 0,
                },
            )
            count = int(row_count or 0)
            cycle_counts["candidate_count"] += count
            normalized_state = str(selection_state or "")
            if normalized_state in {"promotable", "monitor"}:
                cycle_counts[normalized_state] += count
        return counts

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

    def get_opportunity_decision(
        self, opportunity_decision_id: str
    ) -> OpportunityDecisionRecord | None:
        with self.session_factory() as session:
            row = session.get(OpportunityDecisionModel, opportunity_decision_id)
        if row is None:
            return None
        return self.row(row)

    def upsert_opportunity_decision(
        self,
        *,
        opportunity_decision_id: str,
        opportunity_id: str,
        bot_id: str,
        automation_id: str,
        run_key: str,
        scope_key: str,
        policy_ref: dict[str, Any],
        config_hash: str,
        state: str,
        score: float | None,
        rank: int | None,
        reason_codes: list[str],
        superseded_by_id: str | None,
        decided_at: str,
        payload: dict[str, Any] | None = None,
    ) -> OpportunityDecisionRecord:
        decided_at_dt = parse_datetime(decided_at)
        if decided_at_dt is None:
            raise ValueError("decided_at is required")
        with self.session_scope() as session:
            row = session.get(OpportunityDecisionModel, opportunity_decision_id)
            if row is None:
                row = OpportunityDecisionModel(
                    opportunity_decision_id=opportunity_decision_id,
                    opportunity_id=opportunity_id,
                    bot_id=bot_id,
                    automation_id=automation_id,
                    run_key=run_key,
                    scope_key=scope_key,
                    policy_ref_json=dict(policy_ref),
                    config_hash=config_hash,
                    state=state,
                    score=score,
                    rank=rank,
                    reason_codes_json=list(reason_codes),
                    superseded_by_id=superseded_by_id,
                    decided_at=decided_at_dt,
                    payload_json=dict(payload or {}),
                )
                session.add(row)
            else:
                row.opportunity_id = opportunity_id
                row.bot_id = bot_id
                row.automation_id = automation_id
                row.run_key = run_key
                row.scope_key = scope_key
                row.policy_ref_json = dict(policy_ref)
                row.config_hash = config_hash
                row.state = state
                row.score = score
                row.rank = rank
                row.reason_codes_json = list(reason_codes)
                row.superseded_by_id = superseded_by_id
                row.decided_at = decided_at_dt
                row.payload_json = dict(payload or {})
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_opportunity_decisions(
        self,
        *,
        bot_id: str | None = None,
        automation_id: str | None = None,
        opportunity_id: str | None = None,
        run_key: str | None = None,
        scope_key: str | None = None,
        states: list[str] | None = None,
        limit: int = 200,
    ) -> list[OpportunityDecisionRecord]:
        statement = select(OpportunityDecisionModel)
        if bot_id:
            statement = statement.where(OpportunityDecisionModel.bot_id == bot_id)
        if automation_id:
            statement = statement.where(
                OpportunityDecisionModel.automation_id == automation_id
            )
        if opportunity_id:
            statement = statement.where(
                OpportunityDecisionModel.opportunity_id == opportunity_id
            )
        if run_key:
            statement = statement.where(OpportunityDecisionModel.run_key == run_key)
        if scope_key:
            statement = statement.where(OpportunityDecisionModel.scope_key == scope_key)
        if states:
            statement = statement.where(OpportunityDecisionModel.state.in_(states))
        statement = statement.order_by(
            OpportunityDecisionModel.decided_at.desc(),
            OpportunityDecisionModel.opportunity_decision_id.asc(),
        ).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)
