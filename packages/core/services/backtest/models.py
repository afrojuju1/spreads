from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator

from core.model_contracts import DomainModel


class BacktestMode(StrEnum):
    STORED_FACTS = "stored_facts"


class BacktestRequest(DomainModel):
    start_date: str
    end_date: str | None = None
    strategy_ids: tuple[str, ...] | None = None
    mode: BacktestMode = BacktestMode.STORED_FACTS
    max_days: int = Field(default=31, ge=1)
    market_data_symbol_limit: int = Field(default=250, ge=1)

    @field_validator("strategy_ids", mode="before")
    @classmethod
    def _normalize_strategy_ids(cls, value: Any) -> tuple[str, ...] | None:
        if value in (None, "", ()):
            return None
        values = (value,) if isinstance(value, str) else tuple(value)
        normalized = tuple(dict.fromkeys(str(item).strip() for item in values if str(item or "").strip()))
        return normalized or None


__all__ = ["BacktestMode", "BacktestRequest"]
