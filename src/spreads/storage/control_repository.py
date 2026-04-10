from __future__ import annotations

from typing import Any

from sqlalchemy import select

from spreads.storage.base import RepositoryBase
from spreads.storage.control_models import ControlStateModel, OperatorActionModel, PolicyRolloutModel
from spreads.storage.records import ControlStateRecord, OperatorActionRecord, PolicyRolloutRecord
from spreads.storage.serializers import parse_datetime


class ControlRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("control_state", "operator_actions", "policy_rollouts")

    def get_control_state(self, control_state_id: str) -> ControlStateRecord | None:
        with self.session_factory() as session:
            row = session.get(ControlStateModel, control_state_id)
        if row is None:
            return None
        return self.row(row)

    def upsert_control_state(
        self,
        *,
        control_state_id: str,
        mode: str,
        reason_code: str | None,
        note: str | None,
        source_kind: str,
        triggered_by_action_id: str | None,
        effective_at: str,
        updated_at: str,
        metadata: dict[str, Any],
    ) -> ControlStateRecord:
        effective_at_dt = parse_datetime(effective_at)
        updated_at_dt = parse_datetime(updated_at)
        if effective_at_dt is None:
            raise ValueError("effective_at is required")
        if updated_at_dt is None:
            raise ValueError("updated_at is required")
        with self.session_scope() as session:
            row = session.get(ControlStateModel, control_state_id)
            if row is None:
                row = ControlStateModel(control_state_id=control_state_id)
                session.add(row)
            row.mode = mode
            row.reason_code = reason_code
            row.note = note
            row.source_kind = source_kind
            row.triggered_by_action_id = triggered_by_action_id
            row.effective_at = effective_at_dt
            row.updated_at = updated_at_dt
            row.metadata_json = dict(metadata)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def append_operator_action(
        self,
        *,
        operator_action_id: str,
        action_kind: str,
        source_kind: str,
        actor_id: str | None,
        target_scope: str,
        requested_payload: dict[str, Any],
        resulting_state: dict[str, Any],
        note: str | None,
        correlation_id: str | None,
        causation_id: str | None,
        occurred_at: str,
    ) -> OperatorActionRecord:
        occurred_at_dt = parse_datetime(occurred_at)
        if occurred_at_dt is None:
            raise ValueError("occurred_at is required")
        with self.session_scope() as session:
            row = OperatorActionModel(
                operator_action_id=operator_action_id,
                action_kind=action_kind,
                source_kind=source_kind,
                actor_id=actor_id,
                target_scope=target_scope,
                requested_payload_json=dict(requested_payload),
                resulting_state_json=dict(resulting_state),
                note=note,
                correlation_id=correlation_id,
                causation_id=causation_id,
                occurred_at=occurred_at_dt,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_operator_actions(
        self,
        *,
        action_kind: str | None = None,
        limit: int = 100,
    ) -> list[OperatorActionRecord]:
        statement = select(OperatorActionModel)
        if action_kind:
            statement = statement.where(OperatorActionModel.action_kind == action_kind)
        statement = statement.order_by(OperatorActionModel.occurred_at.desc(), OperatorActionModel.operator_action_id.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def create_policy_rollout(
        self,
        *,
        policy_rollout_id: str,
        family: str,
        scope_kind: str,
        scope_key: str | None,
        status: str,
        version_token: str,
        policy: dict[str, Any],
        note: str | None,
        source_kind: str,
        operator_action_id: str | None,
        effective_at: str,
        ended_at: str | None,
        metadata: dict[str, Any],
        supersede_active: bool = True,
    ) -> PolicyRolloutRecord:
        effective_at_dt = parse_datetime(effective_at)
        ended_at_dt = parse_datetime(ended_at)
        if effective_at_dt is None:
            raise ValueError("effective_at is required")
        with self.session_scope() as session:
            if supersede_active and status == "active":
                active_rows = session.scalars(
                    select(PolicyRolloutModel)
                    .where(PolicyRolloutModel.family == family)
                    .where(PolicyRolloutModel.scope_kind == scope_kind)
                    .where(PolicyRolloutModel.scope_key == scope_key)
                    .where(PolicyRolloutModel.status == "active")
                ).all()
                for active_row in active_rows:
                    active_row.status = "superseded"
                    active_row.ended_at = effective_at_dt
            row = PolicyRolloutModel(
                policy_rollout_id=policy_rollout_id,
                family=family,
                scope_kind=scope_kind,
                scope_key=scope_key,
                status=status,
                version_token=version_token,
                policy_json=dict(policy),
                note=note,
                source_kind=source_kind,
                operator_action_id=operator_action_id,
                effective_at=effective_at_dt,
                ended_at=ended_at_dt,
                metadata_json=dict(metadata),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)

    def list_policy_rollouts(
        self,
        *,
        family: str | None = None,
        status: str | None = None,
        scope_kind: str | None = None,
        scope_key: str | None = None,
        limit: int = 100,
    ) -> list[PolicyRolloutRecord]:
        statement = select(PolicyRolloutModel)
        if family:
            statement = statement.where(PolicyRolloutModel.family == family)
        if status:
            statement = statement.where(PolicyRolloutModel.status == status)
        if scope_kind:
            statement = statement.where(PolicyRolloutModel.scope_kind == scope_kind)
        if scope_key is not None:
            statement = statement.where(PolicyRolloutModel.scope_key == scope_key)
        statement = statement.order_by(PolicyRolloutModel.effective_at.desc(), PolicyRolloutModel.policy_rollout_id.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_active_policy_rollouts(self) -> list[PolicyRolloutRecord]:
        return self.list_policy_rollouts(status="active", limit=20)
