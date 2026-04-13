from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, select

from spreads.storage.base import RepositoryBase
from spreads.storage.recovery_models import LiveSessionSlotModel, MarketRecorderTargetModel
from spreads.storage.records import LiveSessionSlotRecord, MarketRecorderTargetRecord
from spreads.storage.serializers import parse_date, parse_datetime


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _slot_row_id(session_id: str, slot_at: str | datetime) -> str:
    slot_at_dt = parse_datetime(slot_at)
    if slot_at_dt is None:
        raise ValueError("slot_at is required")
    slot_token = slot_at_dt.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{session_id}:{slot_token}"


def _capture_target_id(
    *,
    owner_kind: str,
    owner_key: str,
    reason: str,
    option_symbol: str,
) -> str:
    material = f"{owner_kind}|{owner_key}|{reason}|{option_symbol}".encode("utf-8")
    return f"capture_target:{hashlib.sha1(material).hexdigest()[:24]}"


class RecoveryRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("live_session_slots", "market_recorder_targets")

    def get_live_session_slot(
        self,
        *,
        session_id: str,
        slot_at: str | datetime,
    ) -> LiveSessionSlotRecord | None:
        slot_at_dt = parse_datetime(slot_at)
        if slot_at_dt is None:
            raise ValueError("slot_at is required")
        statement = (
            select(LiveSessionSlotModel)
            .where(LiveSessionSlotModel.session_id == session_id)
            .where(LiveSessionSlotModel.slot_at == slot_at_dt)
        )
        with self.session_factory() as session:
            row = session.scalar(statement)
        if row is None:
            return None
        return self.row(row)

    def upsert_live_session_slot(
        self,
        *,
        job_key: str,
        session_id: str,
        session_date: str,
        label: str,
        slot_at: str | datetime,
        status: str,
        scheduled_for: str | datetime | None = None,
        job_run_id: str | None = None,
        capture_status: str | None = None,
        recovery_note: str | None = None,
        slot_details: dict[str, Any] | None = None,
        queued_at: str | datetime | None = None,
        started_at: str | datetime | None = None,
        finished_at: str | datetime | None = None,
        created_at: str | datetime | None = None,
        updated_at: str | datetime | None = None,
    ) -> LiveSessionSlotRecord:
        slot_at_dt = parse_datetime(slot_at)
        if slot_at_dt is None:
            raise ValueError("slot_at is required")
        now = _utc_now()
        created_at_dt = parse_datetime(created_at) or now
        updated_at_dt = parse_datetime(updated_at) or now
        with self.session_scope() as session:
            statement = (
                select(LiveSessionSlotModel)
                .where(LiveSessionSlotModel.session_id == session_id)
                .where(LiveSessionSlotModel.slot_at == slot_at_dt)
            )
            row = session.scalar(statement)
            if row is None:
                row = LiveSessionSlotModel(
                    session_slot_id=_slot_row_id(session_id, slot_at_dt),
                    created_at=created_at_dt,
                    slot_details_json={},
                )
                session.add(row)
            row.job_key = job_key
            row.session_id = session_id
            row.session_date = parse_date(session_date)
            row.label = label
            row.slot_at = slot_at_dt
            row.status = status
            row.scheduled_for = parse_datetime(scheduled_for)
            row.job_run_id = job_run_id
            row.capture_status = capture_status
            row.recovery_note = recovery_note
            if slot_details is not None:
                row.slot_details_json = dict(slot_details)
            row.queued_at = parse_datetime(queued_at)
            row.started_at = parse_datetime(started_at)
            row.finished_at = parse_datetime(finished_at)
            row.updated_at = updated_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def ensure_live_session_slots(
        self,
        *,
        job_key: str,
        session_id: str,
        session_date: str,
        label: str,
        slots: list[dict[str, Any]],
    ) -> list[LiveSessionSlotRecord]:
        if not slots:
            return []
        now = _utc_now()
        slot_times = [
            parse_datetime(slot.get("slot_at") or slot.get("scheduled_for"))
            for slot in slots
        ]
        slot_times = [slot_time for slot_time in slot_times if slot_time is not None]
        if not slot_times:
            return []
        with self.session_scope() as session:
            existing_rows = session.scalars(
                select(LiveSessionSlotModel)
                .where(LiveSessionSlotModel.session_id == session_id)
                .where(LiveSessionSlotModel.slot_at.in_(slot_times))
            ).all()
            existing_by_slot = {row.slot_at: row for row in existing_rows}
            persisted: list[LiveSessionSlotModel] = []
            for slot_payload in slots:
                slot_at_dt = parse_datetime(slot_payload.get("slot_at") or slot_payload.get("scheduled_for"))
                if slot_at_dt is None:
                    continue
                row = existing_by_slot.get(slot_at_dt)
                if row is None:
                    row = LiveSessionSlotModel(
                        session_slot_id=_slot_row_id(session_id, slot_at_dt),
                        created_at=now,
                        slot_details_json={},
                    )
                    session.add(row)
                row.job_key = job_key
                row.session_id = session_id
                row.session_date = parse_date(session_date)
                row.label = label
                row.slot_at = slot_at_dt
                row.scheduled_for = parse_datetime(slot_payload.get("scheduled_for"))
                if row.status in {None, ""}:  # type: ignore[comparison-overlap]
                    row.status = "expected"
                else:
                    row.status = row.status or "expected"
                row.updated_at = now
                persisted.append(row)
            session.flush()
            for row in persisted:
                session.refresh(row)
            return self.rows(persisted)

    def list_live_session_slots(
        self,
        *,
        session_id: str | None = None,
        session_ids: list[str] | None = None,
        statuses: list[str] | None = None,
        session_date: str | None = None,
        slot_from: str | datetime | None = None,
        slot_to: str | datetime | None = None,
        limit: int | None = None,
        ascending: bool = False,
    ) -> list[LiveSessionSlotRecord]:
        statement = select(LiveSessionSlotModel)
        if session_id is not None:
            statement = statement.where(LiveSessionSlotModel.session_id == session_id)
        elif session_ids:
            statement = statement.where(LiveSessionSlotModel.session_id.in_(session_ids))
        if statuses:
            statement = statement.where(LiveSessionSlotModel.status.in_(statuses))
        if session_date is not None:
            statement = statement.where(LiveSessionSlotModel.session_date == parse_date(session_date))
        slot_from_dt = parse_datetime(slot_from)
        if slot_from_dt is not None:
            statement = statement.where(LiveSessionSlotModel.slot_at >= slot_from_dt)
        slot_to_dt = parse_datetime(slot_to)
        if slot_to_dt is not None:
            statement = statement.where(LiveSessionSlotModel.slot_at <= slot_to_dt)
        order_column = LiveSessionSlotModel.slot_at.asc() if ascending else LiveSessionSlotModel.slot_at.desc()
        statement = statement.order_by(order_column, LiveSessionSlotModel.session_slot_id.desc())
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def replace_capture_targets(
        self,
        *,
        owner_kind: str,
        owner_key: str,
        reason: str,
        session_id: str | None = None,
        session_date: str | None = None,
        label: str | None = None,
        profile: str | None = None,
        rows: list[dict[str, Any]],
    ) -> list[MarketRecorderTargetRecord]:
        now = _utc_now()
        with self.session_scope() as session:
            existing_rows = session.scalars(
                select(MarketRecorderTargetModel)
                .where(MarketRecorderTargetModel.owner_kind == owner_kind)
                .where(MarketRecorderTargetModel.owner_key == owner_key)
                .where(MarketRecorderTargetModel.reason == reason)
            ).all()
            existing_by_symbol = {
                str(row.option_symbol): row
                for row in existing_rows
            }
            persisted: list[MarketRecorderTargetModel] = []
            desired_symbols: set[str] = set()
            for payload in rows:
                option_symbol = str(payload.get("option_symbol") or "").strip()
                if not option_symbol:
                    continue
                desired_symbols.add(option_symbol)
                row = existing_by_symbol.get(option_symbol)
                if row is None:
                    row = MarketRecorderTargetModel(
                        capture_target_id=_capture_target_id(
                            owner_kind=owner_kind,
                            owner_key=owner_key,
                            reason=reason,
                            option_symbol=option_symbol,
                        ),
                        created_at=now,
                        metadata_json={},
                    )
                    session.add(row)
                row.owner_kind = owner_kind
                row.owner_key = owner_key
                row.reason = reason
                row.session_id = session_id
                row.session_date = None if session_date is None else parse_date(session_date)
                row.label = label
                row.profile = profile
                row.underlying_symbol = payload.get("underlying_symbol")
                row.strategy = payload.get("strategy")
                row.leg_role = payload.get("leg_role")
                row.option_symbol = option_symbol
                row.quote_enabled = bool(payload.get("quote_enabled", True))
                row.trade_enabled = bool(payload.get("trade_enabled", False))
                row.feed = str(payload.get("feed") or "opra")
                row.data_base_url = payload.get("data_base_url")
                row.expires_at = parse_datetime(payload.get("expires_at"))
                row.metadata_json = dict(payload.get("metadata") or {})
                row.updated_at = now
                persisted.append(row)
            for existing in existing_rows:
                if str(existing.option_symbol) not in desired_symbols:
                    session.delete(existing)
            session.flush()
            for row in persisted:
                session.refresh(row)
            return self.rows(persisted)

    def delete_capture_targets(
        self,
        *,
        owner_kind: str | None = None,
        owner_key: str | None = None,
        reason: str | None = None,
    ) -> int:
        statement = delete(MarketRecorderTargetModel)
        if owner_kind is not None:
            statement = statement.where(MarketRecorderTargetModel.owner_kind == owner_kind)
        if owner_key is not None:
            statement = statement.where(MarketRecorderTargetModel.owner_key == owner_key)
        if reason is not None:
            statement = statement.where(MarketRecorderTargetModel.reason == reason)
        with self.session_scope() as session:
            result = session.execute(statement)
        return int(result.rowcount or 0)

    def delete_capture_targets_for_absent_owners(
        self,
        *,
        owner_kind: str,
        active_owner_keys: list[str],
        reason: str | None = None,
    ) -> int:
        statement = delete(MarketRecorderTargetModel).where(
            MarketRecorderTargetModel.owner_kind == owner_kind
        )
        if reason is not None:
            statement = statement.where(MarketRecorderTargetModel.reason == reason)
        if active_owner_keys:
            statement = statement.where(
                ~MarketRecorderTargetModel.owner_key.in_(active_owner_keys)
            )
        with self.session_scope() as session:
            result = session.execute(statement)
        return int(result.rowcount or 0)

    def list_capture_targets(
        self,
        *,
        owner_kind: str | None = None,
        owner_key: str | None = None,
        session_id: str | None = None,
        reasons: list[str] | None = None,
        active_only: bool = False,
        as_of: str | datetime | None = None,
        limit: int | None = None,
    ) -> list[MarketRecorderTargetRecord]:
        statement = select(MarketRecorderTargetModel)
        if owner_kind is not None:
            statement = statement.where(MarketRecorderTargetModel.owner_kind == owner_kind)
        if owner_key is not None:
            statement = statement.where(MarketRecorderTargetModel.owner_key == owner_key)
        if session_id is not None:
            statement = statement.where(MarketRecorderTargetModel.session_id == session_id)
        if reasons:
            statement = statement.where(MarketRecorderTargetModel.reason.in_(reasons))
        if active_only:
            as_of_dt = parse_datetime(as_of) or _utc_now()
            statement = statement.where(
                (MarketRecorderTargetModel.expires_at.is_(None))
                | (MarketRecorderTargetModel.expires_at > as_of_dt)
            )
        statement = statement.order_by(
            MarketRecorderTargetModel.owner_kind.asc(),
            MarketRecorderTargetModel.owner_key.asc(),
            MarketRecorderTargetModel.reason.asc(),
            MarketRecorderTargetModel.option_symbol.asc(),
        )
        if limit is not None:
            statement = statement.limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_active_capture_targets(
        self,
        *,
        as_of: str | datetime | None = None,
        limit: int | None = None,
    ) -> list[MarketRecorderTargetRecord]:
        return self.list_capture_targets(
            active_only=True,
            as_of=as_of,
            limit=limit,
        )
