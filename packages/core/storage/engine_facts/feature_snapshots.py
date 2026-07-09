from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select

from core.storage.engine_models import (
    TradingFeatureSnapshotModel,
)
from core.storage.records import StorageRow
from core.storage.serializers import parse_date, parse_datetime, render_value
from core.value_coercion import as_text

if TYPE_CHECKING:
    pass


class EngineFactFeatureSnapshotMixin:
    def replace_trading_feature_snapshots(
        self,
        *,
        candidate_run_id: str,
        snapshots: list[dict[str, Any]],
        updated_at: str,
    ) -> list[StorageRow]:
        if not self.feature_store_schema_ready():
            return []
        updated_at_dt = parse_datetime(updated_at)
        if updated_at_dt is None:
            raise ValueError("updated_at is required")
        rows: list[StorageRow] = []
        with self.session_scope() as session:
            session.execute(delete(TradingFeatureSnapshotModel).where(TradingFeatureSnapshotModel.candidate_run_id == candidate_run_id))
            for snapshot in snapshots:
                observed_at = parse_datetime(snapshot.get("observed_at"))
                if observed_at is None:
                    raise ValueError("feature snapshot observed_at is required")
                row = TradingFeatureSnapshotModel(
                    trading_feature_snapshot_id=str(snapshot["trading_feature_snapshot_id"]),
                    feature_version=str(snapshot["feature_version"]),
                    candidate_run_id=str(snapshot["candidate_run_id"]),
                    trade_candidate_id=as_text(snapshot.get("trade_candidate_id")),
                    ticker_source_run_id=as_text(snapshot.get("ticker_source_run_id")),
                    ticker_source_kind=str(snapshot["ticker_source_kind"]),
                    ticker_source_id=str(snapshot["ticker_source_id"]),
                    trading_strategy_id=str(snapshot["trading_strategy_id"]),
                    trade_structure=str(snapshot["trade_structure"]),
                    routine=str(snapshot["routine"]),
                    config_hash=str(snapshot["config_hash"]),
                    session_date=parse_date(snapshot["session_date"]),
                    observed_at=observed_at,
                    underlying_symbol=str(snapshot["underlying_symbol"]).upper(),
                    candidate_identity=as_text(snapshot.get("candidate_identity")),
                    feature_scope=str(snapshot["feature_scope"]),
                    quality_profile_id=str(snapshot["quality_profile_id"]),
                    quality_status=str(snapshot["quality_status"]),
                    market_data_quality_state=str(snapshot["market_data_quality_state"]),
                    market_data_quality_reason=str(snapshot["market_data_quality_reason"]),
                    source_json=render_value(snapshot.get("source") or {}),
                    underlying_json=render_value(snapshot.get("underlying") or {}),
                    chain_json=render_value(snapshot.get("chain") or {}),
                    premium_json=render_value(snapshot.get("premium") or {}),
                    candidate_json=render_value(snapshot.get("candidate") or {}),
                    metadata_json=render_value(snapshot.get("metadata") or {}),
                    quality_json=render_value(snapshot.get("quality") or {}),
                    market_data_quality_json=render_value(snapshot.get("market_data_quality") or {}),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
                session.flush()
                rows.append(self.row(row))
        return rows

    def list_trading_feature_snapshots(
        self,
        *,
        trading_strategy_id: str | None = None,
        session_date: str | date | None = None,
        candidate_run_id: str | None = None,
        limit: int = 500,
    ) -> list[StorageRow]:
        if not self.feature_store_schema_ready():
            return []
        statement = select(TradingFeatureSnapshotModel)
        if trading_strategy_id is not None:
            statement = statement.where(TradingFeatureSnapshotModel.trading_strategy_id == trading_strategy_id)
        if session_date is not None:
            statement = statement.where(TradingFeatureSnapshotModel.session_date == parse_date(session_date))
        if candidate_run_id is not None:
            statement = statement.where(TradingFeatureSnapshotModel.candidate_run_id == candidate_run_id)
        statement = statement.order_by(
            TradingFeatureSnapshotModel.observed_at.desc(),
            TradingFeatureSnapshotModel.underlying_symbol.asc(),
            TradingFeatureSnapshotModel.candidate_identity.asc().nullslast(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)


__all__ = ["EngineFactFeatureSnapshotMixin"]
