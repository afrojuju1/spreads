from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.positions import enrich_position_row
from core.value_coercion import as_text

from .portfolio import PositionSnapshot
from .risk_runtime import OPEN_POSITION_STATUSES


def build_position_snapshot(position: Mapping[str, Any]) -> PositionSnapshot:
    payload = enrich_position_row(dict(position))
    position_id = str(payload["position_id"])
    return PositionSnapshot(
        position_id=position_id,
        trading_strategy_id=as_text(payload.get("trading_strategy_id")) or "",
        underlying_symbol=str(payload.get("underlying_symbol") or payload.get("root_symbol") or ""),
        state=str(payload.get("position_status") or payload.get("status") or "unknown"),
        payload=payload,
    )


class PortfolioEngine:
    def __init__(
        self,
        *,
        execution_store: Any,
    ) -> None:
        self.execution_store = execution_store

    def list_open_positions(
        self,
        *,
        trading_strategy_id: str | None = None,
        limit: int = 200,
    ) -> tuple[PositionSnapshot, ...]:
        positions = self.execution_store.list_positions(
            trading_strategy_id=trading_strategy_id,
            statuses=list(OPEN_POSITION_STATUSES),
            limit=limit,
        )
        return tuple(build_position_snapshot(position) for position in positions)


__all__ = [
    "OPEN_POSITION_STATUSES",
    "PortfolioEngine",
    "build_position_snapshot",
]
