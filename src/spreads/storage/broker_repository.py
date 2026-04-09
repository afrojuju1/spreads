from __future__ import annotations

from typing import Any

from sqlalchemy import select

from spreads.storage.base import RepositoryBase
from spreads.storage.broker_models import AccountSnapshotModel, BrokerSyncStateModel
from spreads.storage.records import AccountSnapshotRecord, BrokerSyncStateRecord
from spreads.storage.serializers import (
    parse_datetime,
    to_account_snapshot_record,
    to_broker_sync_state_record,
)


class BrokerRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("account_snapshots", "broker_sync_state")

    def create_account_snapshot(
        self,
        *,
        broker: str,
        environment: str,
        source: str,
        captured_at: str,
        account: dict[str, Any],
        pnl: dict[str, Any],
        positions: list[dict[str, Any]],
        history: dict[str, Any],
    ) -> AccountSnapshotRecord:
        with self.session_scope() as session:
            row = AccountSnapshotModel(
                broker=broker,
                environment=environment,
                source=source,
                captured_at=parse_datetime(captured_at),
                account_json=account,
                pnl_json=pnl,
                positions_json=positions,
                history_json=history,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return to_account_snapshot_record(row)

    def get_latest_account_snapshot(self, *, broker: str = "alpaca") -> AccountSnapshotRecord | None:
        statement = (
            select(AccountSnapshotModel)
            .where(AccountSnapshotModel.broker == broker)
            .order_by(AccountSnapshotModel.captured_at.desc(), AccountSnapshotModel.snapshot_id.desc())
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.scalars(statement).first()
        if row is None:
            return None
        return to_account_snapshot_record(row)

    def upsert_sync_state(
        self,
        *,
        sync_key: str,
        broker: str,
        status: str,
        updated_at: str,
        cursor: dict[str, Any],
        summary: dict[str, Any],
        error_text: str | None = None,
    ) -> BrokerSyncStateRecord:
        with self.session_scope() as session:
            row = session.get(BrokerSyncStateModel, sync_key)
            if row is None:
                row = BrokerSyncStateModel(
                    sync_key=sync_key,
                    broker=broker,
                )
                session.add(row)
            row.broker = broker
            row.status = status
            row.updated_at = parse_datetime(updated_at)
            row.cursor_json = cursor
            row.summary_json = summary
            row.error_text = error_text
            session.flush()
            session.refresh(row)
            return to_broker_sync_state_record(row)

    def get_sync_state(self, sync_key: str) -> BrokerSyncStateRecord | None:
        with self.session_factory() as session:
            row = session.get(BrokerSyncStateModel, sync_key)
        if row is None:
            return None
        return to_broker_sync_state_record(row)
