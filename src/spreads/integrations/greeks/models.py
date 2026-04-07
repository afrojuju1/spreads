from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class GreeksRequest:
    symbol: str
    option_symbol: str
    option_type: str
    spot_price: float
    strike_price: float
    bid: float
    ask: float
    expiration: datetime
    as_of: datetime
    risk_free_rate: float
    dividend_yield: float = 0.0

    @property
    def midpoint(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(frozen=True)
class GreeksResult:
    status: str
    source: str
    implied_volatility: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    reason: str | None = None
