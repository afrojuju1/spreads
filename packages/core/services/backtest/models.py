from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator

from core.model_contracts import DomainModel


class BacktestMode(StrEnum):
    STORED_FACTS = "stored_facts"


class BacktestRunState(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class BacktestArtifactKind(StrEnum):
    RESULT = "result"


class BacktestStorageKind(StrEnum):
    FILE = "file"


class BacktestRequest(DomainModel):
    start_date: str
    end_date: str | None = None
    strategy_ids: tuple[str, ...] | None = None
    mode: BacktestMode = BacktestMode.STORED_FACTS
    max_days: int = Field(default=31, ge=1)
    market_data_symbol_limit: int = Field(default=250, ge=1)
    requested_by: str | None = None
    artifact_root: str | None = None

    @field_validator("strategy_ids", mode="before")
    @classmethod
    def _normalize_strategy_ids(cls, value: Any) -> tuple[str, ...] | None:
        if value in (None, "", ()):
            return None
        values = (value,) if isinstance(value, str) else tuple(value)
        normalized = tuple(dict.fromkeys(str(item).strip() for item in values if str(item or "").strip()))
        return normalized or None

    @field_validator("requested_by", "artifact_root", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        rendered = str(value or "").strip()
        return rendered or None


__all__ = [
    "BacktestArtifactKind",
    "BacktestMode",
    "BacktestRequest",
    "BacktestRunState",
    "BacktestStorageKind",
]
