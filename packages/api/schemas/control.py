from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ControlModeRequest(BaseModel):
    mode: Literal["normal", "degraded", "halted"]
    reason_code: str = Field(..., min_length=1)
    note: str | None = Field(default=None, min_length=1)
