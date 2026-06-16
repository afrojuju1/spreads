from __future__ import annotations

from pydantic import Field

from core.model_contracts import DomainModel


class PositionCloseRequest(DomainModel):
    quantity: int | None = Field(default=None, ge=1, le=25)
    limit_price: float | None = Field(default=None, gt=0)
    execution_runtime: str | None = None
