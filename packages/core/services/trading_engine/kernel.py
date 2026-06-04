from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class EngineComponentRole(StrEnum):
    DATA = "data"
    STRATEGY = "strategy"
    RISK = "risk"
    EXECUTION = "execution"
    PORTFOLIO = "portfolio"
    OPS = "ops"


@dataclass(frozen=True)
class EngineContext:
    db_target: str
    storage: Any
    job_run_id: str | None = None
    config_root: str | None = None
    now: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EngineRunRef:
    role: EngineComponentRole
    run_id: str
    trading_strategy_id: str | None = None
    job_run_id: str | None = None
    source_id: str | None = None
    parent_run_id: str | None = None
    config_hash: str | None = None
