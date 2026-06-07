from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Protocol

from .kernel import EnginePolicy, EngineRunRef, EngineSummary


@dataclass(frozen=True)
class StrategyEntryRequest:
    run_ref: EngineRunRef
    trading_strategy_id: str
    market_date: date
    policy: EnginePolicy = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyEntryResult:
    run_ref: EngineRunRef
    strategy_run_id: str
    trade_signal_ids: tuple[str, ...]
    trade_decision_ids: tuple[str, ...]
    selected_decision_ids: tuple[str, ...]
    status: str
    reason: str | None = None
    summary: EngineSummary = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyManageRequest:
    run_ref: EngineRunRef
    trading_strategy_id: str
    market_date: date
    policy: EnginePolicy = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyManageResult:
    run_ref: EngineRunRef
    close_decision_ids: tuple[str, ...]
    selected_close_decision_ids: tuple[str, ...]
    status: str
    reason: str | None = None
    summary: EngineSummary = field(default_factory=dict)


class StrategyEngine(Protocol):
    def run_entry(self, request: StrategyEntryRequest) -> StrategyEntryResult: ...

    def run_manage(self, request: StrategyManageRequest) -> StrategyManageResult: ...
