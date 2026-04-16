from __future__ import annotations

from pydantic import BaseModel, Field


class PositionCloseRequest(BaseModel):
    quantity: int | None = Field(default=None, ge=1, le=25)
    limit_price: float | None = Field(default=None, gt=0)
