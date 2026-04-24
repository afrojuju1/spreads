from __future__ import annotations

from pydantic import BaseModel


class PipelineRunRequest(BaseModel):
    symbol: str
    strategy_mode: str = "auto"
    strategy_family: str | None = None
