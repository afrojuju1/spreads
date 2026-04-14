from __future__ import annotations

from pydantic import BaseModel, Field


class SessionExecutionRequest(BaseModel):
    candidate_id: int = Field(..., gt=0)
    quantity: int | None = Field(default=None, ge=1, le=25)
    limit_price: float | None = Field(default=None, gt=0)


class SessionPositionCloseRequest(BaseModel):
    quantity: int | None = Field(default=None, ge=1, le=25)
    limit_price: float | None = Field(default=None, gt=0)
