from __future__ import annotations

from dataclasses import dataclass, field

from .kernel import EnginePayload


@dataclass(frozen=True)
class PositionSnapshot:
    position_id: str
    trading_strategy_id: str
    underlying_symbol: str
    state: str
    payload: EnginePayload = field(default_factory=dict)
