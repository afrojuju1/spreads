from __future__ import annotations

from typing import Literal

from pydantic import Field

from core.model_contracts import DomainModel


class EquityOrderRequest(DomainModel):
    symbol: str = Field(min_length=1, max_length=12)
    side: Literal["buy", "sell"]
    quantity: int = Field(ge=1, le=100_000)
    limit_price: float = Field(gt=0)
    time_in_force: Literal["day", "gtc"] = "day"
    label: str = Field(default="manual_equity", min_length=1, max_length=80)
    market_date: str | None = None
    execution_runtime: str | None = None
