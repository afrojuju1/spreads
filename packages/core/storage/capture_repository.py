from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select

from core.storage.base import RepositoryBase
from core.storage.capture_models import CaptureSummaryModel, CaptureTargetModel
from core.storage.records import CaptureSummaryRecord, CaptureTargetRecord
from core.storage.serializers import parse_date, parse_datetime, render_value


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _capture_target_id(
    *,
    owner_kind: str,
    owner_key: str,
    reason: str,
    option_symbol: str,
) -> str:
    material = f"{owner_kind}|{owner_key}|{reason}|{option_symbol}".encode("utf-8")
    return f"capture_target:{hashlib.sha1(material).hexdigest()[:24]}"


def _capture_summary_id(source: str, captured_at: datetime) -> str:
    token = captured_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"capture_summary:{source}:{token}:{uuid4().hex[:8]}"


class CaptureRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("capture_targets", "capture_summaries")

    def target_schema_ready(self) -> bool:
        return self.schema_has_tables("capture_targets")

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
        priority: int = 100,
        rows: list[dict[str, Any]],
    ) -> list[CaptureTargetRecord]:
        now = _utc_now()
        with self.session_scope() as session:
            existing_rows = session.scalars(
                select(CaptureTargetModel)
                .where(CaptureTargetModel.owner_kind == owner_kind)
                .where(CaptureTargetModel.owner_key == owner_key)
                .where(CaptureTargetModel.reason == reason)
            ).all()
            existing_by_symbol = {str(row.option_symbol): row for row in existing_rows}
            persisted: list[CaptureTargetModel] = []
            desired_symbols: set[str] = set()
            for payload in rows:
                option_symbol = str(payload.get("option_symbol") or "").strip()
                if not option_symbol:
                    continue
                desired_symbols.add(option_symbol)
                row = existing_by_symbol.get(option_symbol)
                if row is None:
                    row = CaptureTargetModel(
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
                row.priority = int(payload.get("priority") or priority)
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
                row.metadata_json = render_value(dict(payload.get("metadata") or {}))
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
        statement = delete(CaptureTargetModel)
        if owner_kind is not None:
            statement = statement.where(CaptureTargetModel.owner_kind == owner_kind)
        if owner_key is not None:
            statement = statement.where(CaptureTargetModel.owner_key == owner_key)
        if reason is not None:
            statement = statement.where(CaptureTargetModel.reason == reason)
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
        statement = delete(CaptureTargetModel).where(CaptureTargetModel.owner_kind == owner_kind)
        if reason is not None:
            statement = statement.where(CaptureTargetModel.reason == reason)
        if active_owner_keys:
            statement = statement.where(~CaptureTargetModel.owner_key.in_(active_owner_keys))
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
    ) -> list[CaptureTargetRecord]:
        statement = select(CaptureTargetModel)
        if owner_kind is not None:
            statement = statement.where(CaptureTargetModel.owner_kind == owner_kind)
        if owner_key is not None:
            statement = statement.where(CaptureTargetModel.owner_key == owner_key)
        if session_id is not None:
            statement = statement.where(CaptureTargetModel.session_id == session_id)
        if reasons:
            statement = statement.where(CaptureTargetModel.reason.in_(reasons))
        if active_only:
            as_of_dt = parse_datetime(as_of) or _utc_now()
            statement = statement.where((CaptureTargetModel.expires_at.is_(None)) | (CaptureTargetModel.expires_at > as_of_dt))
        statement = statement.order_by(
            CaptureTargetModel.priority.asc(),
            CaptureTargetModel.updated_at.desc(),
            CaptureTargetModel.owner_kind.asc(),
            CaptureTargetModel.owner_key.asc(),
            CaptureTargetModel.reason.asc(),
            CaptureTargetModel.option_symbol.asc(),
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
    ) -> list[CaptureTargetRecord]:
        return self.list_capture_targets(active_only=True, as_of=as_of, limit=limit)

    def save_capture_summary(
        self,
        *,
        source: str,
        status: str,
        active_target_count: int,
        selected_target_count: int,
        capture_group_count: int,
        quote_rows_saved: int,
        trade_rows_saved: int,
        target_limit: int | None,
        target_counts: dict[str, Any],
        group_summary: list[dict[str, Any]],
        errors: dict[str, Any],
        metadata: dict[str, Any],
        captured_at: str | datetime | None = None,
    ) -> CaptureSummaryRecord:
        now = _utc_now()
        captured_at_dt = parse_datetime(captured_at) or now
        row = CaptureSummaryModel(
            capture_summary_id=_capture_summary_id(source, captured_at_dt),
            source=source,
            status=status,
            active_target_count=int(active_target_count),
            selected_target_count=int(selected_target_count),
            capture_group_count=int(capture_group_count),
            quote_rows_saved=int(quote_rows_saved),
            trade_rows_saved=int(trade_rows_saved),
            target_limit=None if target_limit is None else int(target_limit),
            target_counts_json=render_value(target_counts),
            group_summary_json=render_value(group_summary),
            error_json=render_value(errors),
            metadata_json=render_value(metadata),
            captured_at=captured_at_dt,
            created_at=now,
        )
        with self.session_scope() as session:
            session.add(row)
            session.flush()
            session.refresh(row)
            return self.row(row)
