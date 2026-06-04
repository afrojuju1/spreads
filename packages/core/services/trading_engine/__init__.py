"""Nautilus-shaped trading engine boundaries for Spreads.

This package names the engine roles first; runtime wiring moves here bead by bead.
"""

from .data import DataEngine
from .capture_targets import refresh_engine_capture_targets
from .data_runtime import PostgresDataEngine
from .execution import ExecutionEngine
from .kernel import EngineComponentRole, EngineContext, EngineRunRef
from .portfolio import PortfolioEngine
from .portfolio_runtime import PostgresPortfolioEngine
from .risk import RiskEngine
from .strategy import StrategyEngine

__all__ = [
    "DataEngine",
    "EngineComponentRole",
    "EngineContext",
    "EngineRunRef",
    "ExecutionEngine",
    "PortfolioEngine",
    "PostgresDataEngine",
    "PostgresPortfolioEngine",
    "RiskEngine",
    "StrategyEngine",
    "refresh_engine_capture_targets",
]
