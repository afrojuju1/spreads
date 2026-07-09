from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from core.storage.engine_models import (
    MarketContextSnapshotModel,
)
from core.storage.records import StorageRow
from core.storage.serializers import parse_datetime, render_value

if TYPE_CHECKING:
    from core.services.trading_engine.market_context import MarketContextSnapshot


from core.storage.engine_facts.contracts import (
    _market_context_snapshot_id,
)

class EngineFactMarketContextMixin:
    def upsert_market_context_snapshot(
        self,
        snapshot: MarketContextSnapshot,
        *,
        updated_at: str | datetime | None = None,
    ) -> StorageRow:
        if not self.market_context_schema_ready():
            raise RuntimeError("market_context_snapshots schema is not ready")
        updated_at_dt = parse_datetime(updated_at) if updated_at is not None else datetime.now(UTC)
        if updated_at_dt is None:
            raise ValueError("updated_at is invalid")
        payload = snapshot.to_payload()
        snapshot_id = _market_context_snapshot_id(snapshot)
        payload["snapshot_id"] = snapshot_id
        regime_payload = dict(payload.get("regime") or {})
        data_quality_payload = dict(payload.get("data_quality") or {})
        with self.session_scope() as session:
            row = session.get(MarketContextSnapshotModel, snapshot_id)
            if row is None:
                row = MarketContextSnapshotModel(
                    market_context_snapshot_id=snapshot_id,
                    scope=snapshot.scope,
                    observed_at=snapshot.observed_at,
                    expires_at=snapshot.expires_at,
                    generated_at=snapshot.generated_at,
                    context_version=snapshot.context_version,
                    config_hash=snapshot.config_hash,
                    regime_label=str(regime_payload.get("regime_label") or "unknown"),
                    risk_posture=str(regime_payload.get("risk_posture") or "unknown"),
                    trend_strength=str(regime_payload.get("trend_strength") or "unknown"),
                    volatility_state=str(regime_payload.get("volatility_state") or "unknown"),
                    confidence=float(regime_payload.get("confidence") or 0.0),
                    data_quality_state=str(data_quality_payload.get("state") or "unknown"),
                    freshness_state=str(data_quality_payload.get("freshness") or "unknown"),
                    fidelity_json=list(payload.get("fidelity") or []),
                    payload_json=render_value(payload),
                    regime_json=render_value(regime_payload),
                    benchmark_evidence_json=render_value(payload.get("benchmark_evidence") or []),
                    source_evidence_json=render_value(payload.get("source_evidence") or {}),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
            else:
                row.scope = snapshot.scope
                row.observed_at = snapshot.observed_at
                row.expires_at = snapshot.expires_at
                row.generated_at = snapshot.generated_at
                row.context_version = snapshot.context_version
                row.config_hash = snapshot.config_hash
                row.regime_label = str(regime_payload.get("regime_label") or "unknown")
                row.risk_posture = str(regime_payload.get("risk_posture") or "unknown")
                row.trend_strength = str(regime_payload.get("trend_strength") or "unknown")
                row.volatility_state = str(regime_payload.get("volatility_state") or "unknown")
                row.confidence = float(regime_payload.get("confidence") or 0.0)
                row.data_quality_state = str(data_quality_payload.get("state") or "unknown")
                row.freshness_state = str(data_quality_payload.get("freshness") or "unknown")
                row.fidelity_json = list(payload.get("fidelity") or [])
                row.payload_json = render_value(payload)
                row.regime_json = render_value(regime_payload)
                row.benchmark_evidence_json = render_value(payload.get("benchmark_evidence") or [])
                row.source_evidence_json = render_value(payload.get("source_evidence") or {})
                row.updated_at = updated_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_market_context_snapshot(self, market_context_snapshot_id: str) -> StorageRow | None:
        if not self.market_context_schema_ready():
            return None
        with self.session_factory() as session:
            row = session.get(MarketContextSnapshotModel, market_context_snapshot_id)
        return None if row is None else self.row(row)

    def latest_market_context_snapshot(
        self,
        *,
        scope: str = "global_market",
        as_of: str | datetime | None = None,
        include_expired: bool = True,
    ) -> StorageRow | None:
        if not self.market_context_schema_ready():
            return None
        as_of_dt = parse_datetime(as_of) if as_of is not None else None
        statement = select(MarketContextSnapshotModel).where(MarketContextSnapshotModel.scope == scope)
        if as_of_dt is not None:
            statement = statement.where(MarketContextSnapshotModel.observed_at <= as_of_dt)
            if not include_expired:
                statement = statement.where(MarketContextSnapshotModel.expires_at > as_of_dt)
        statement = statement.order_by(MarketContextSnapshotModel.observed_at.desc()).limit(1)
        with self.session_factory() as session:
            row = session.scalars(statement).first()
        return None if row is None else self.row(row)


__all__ = ["EngineFactMarketContextMixin"]
