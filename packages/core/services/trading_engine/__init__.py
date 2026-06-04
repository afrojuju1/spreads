"""Nautilus-shaped trading engine boundaries for Spreads.

This package names the engine roles first; runtime wiring moves here bead by bead.
"""

from .data import DataEngine
from .data_runtime import PostgresDataEngine
from .execution import ExecutionEngine
from .kernel import EngineComponentRole, EngineContext, EngineRunRef
from .portfolio import PortfolioEngine
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
    "RiskEngine",
    "StrategyEngine",
]
