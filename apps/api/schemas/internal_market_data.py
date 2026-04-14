from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class OptionMarketDataCaptureRequest(BaseModel):
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    feed: Literal["opra", "indicative"] = "opra"
    quote_duration_seconds: float = Field(default=20.0, ge=0, le=60.0)
    trade_duration_seconds: float = Field(default=20.0, ge=0, le=60.0)
    data_base_url: str | None = None
