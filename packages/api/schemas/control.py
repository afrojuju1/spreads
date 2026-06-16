from __future__ import annotations

from typing import Literal

from pydantic import Field

from core.model_contracts import DomainModel


class ControlModeRequest(DomainModel):
    mode: Literal["normal", "degraded", "halted"]
    reason_code: str = Field(..., min_length=1)
    note: str | None = Field(default=None, min_length=1)
