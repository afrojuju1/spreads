from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from .kernel import EngineRunRef


@dataclass(frozen=True)
class PositionSnapshot:
    position_id: str
    trading_strategy_id: str
    underlying_symbol: str
    state: str
    payload: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class CloseDecisionResult:
    run_ref: EngineRunRef
    close_decision_id: str
    position_id: str
    state: str
    reason_codes: tuple[str, ...] = ()
    payload: Mapping[str, object] = field(default_factory=dict)


class PortfolioEngine(Protocol):
    def list_open_positions(
        self,
        *,
        trading_strategy_id: str | None = None,
    ) -> tuple[PositionSnapshot, ...]:
        ...

    def evaluate_close(
        self,
        *,
        run_ref: EngineRunRef,
        position: PositionSnapshot,
    ) -> CloseDecisionResult:
        ...
