from __future__ import annotations

import math
from dataclasses import dataclass

from py_vollib.black_scholes_merton.greeks.analytical import delta, gamma, theta, vega
from py_vollib.black_scholes_merton.implied_volatility import implied_volatility

from .models import GreeksRequest, GreeksResult


SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0


@dataclass(frozen=True)
class LocalGreeksProvider:
    risk_free_rate: float = 0.04
    default_dividend_yield: float = 0.0
    max_relative_spread: float = 0.75
    min_time_to_expiry_seconds: int = 60
    min_option_price: float = 0.01
    min_implied_volatility: float = 1e-4
    max_implied_volatility: float = 8.0
    source_name: str = "local_bsm"

    def compute(self, request: GreeksRequest) -> GreeksResult:
        option_flag = self._resolve_option_flag(request.option_type)
        if option_flag is None:
            return GreeksResult(status="invalid", source=self.source_name, reason="unsupported_option_type")
        if request.spot_price <= 0 or request.strike_price <= 0:
            return GreeksResult(status="invalid", source=self.source_name, reason="invalid_spot_or_strike")
        if request.bid <= 0 or request.ask <= 0 or request.ask < request.bid:
            return GreeksResult(status="invalid", source=self.source_name, reason="invalid_option_quote")

        midpoint = request.midpoint
        relative_spread = (request.ask - request.bid) / midpoint if midpoint > 0 else math.inf
        if midpoint < self.min_option_price:
            return GreeksResult(status="invalid", source=self.source_name, reason="option_price_too_small")
        if relative_spread > self.max_relative_spread:
            return GreeksResult(status="invalid", source=self.source_name, reason="quote_spread_too_wide")

        seconds_to_expiry = (request.expiration - request.as_of).total_seconds()
        if seconds_to_expiry <= self.min_time_to_expiry_seconds:
            return GreeksResult(status="invalid", source=self.source_name, reason="too_close_to_expiry")

        intrinsic_value = self._intrinsic_value(
            option_type=option_flag,
            spot_price=request.spot_price,
            strike_price=request.strike_price,
        )
        if midpoint < intrinsic_value - 0.01:
            return GreeksResult(status="invalid", source=self.source_name, reason="midpoint_below_intrinsic")

        years_to_expiry = seconds_to_expiry / SECONDS_PER_YEAR
        rate = request.risk_free_rate
        dividend_yield = request.dividend_yield

        try:
            implied_vol = implied_volatility(
                midpoint,
                request.spot_price,
                request.strike_price,
                years_to_expiry,
                rate,
                dividend_yield,
                option_flag,
            )
        except Exception as exc:  # py_vollib raises library-specific pricing exceptions
            return GreeksResult(status="invalid", source=self.source_name, reason=f"iv_solve_failed:{exc}")

        if not math.isfinite(implied_vol) or not (self.min_implied_volatility <= implied_vol <= self.max_implied_volatility):
            return GreeksResult(status="invalid", source=self.source_name, reason="implied_vol_out_of_range")

        try:
            option_delta = delta(
                option_flag,
                request.spot_price,
                request.strike_price,
                years_to_expiry,
                rate,
                implied_vol,
                dividend_yield,
            )
            option_gamma = gamma(
                option_flag,
                request.spot_price,
                request.strike_price,
                years_to_expiry,
                rate,
                implied_vol,
                dividend_yield,
            )
            option_theta = theta(
                option_flag,
                request.spot_price,
                request.strike_price,
                years_to_expiry,
                rate,
                implied_vol,
                dividend_yield,
            )
            option_vega = vega(
                option_flag,
                request.spot_price,
                request.strike_price,
                years_to_expiry,
                rate,
                implied_vol,
                dividend_yield,
            )
        except Exception as exc:
            return GreeksResult(status="invalid", source=self.source_name, reason=f"greeks_failed:{exc}")

        greeks = (option_delta, option_gamma, option_theta, option_vega)
        if not all(math.isfinite(value) for value in greeks):
            return GreeksResult(status="invalid", source=self.source_name, reason="non_finite_greeks")

        return GreeksResult(
            status="ok",
            source=self.source_name,
            implied_volatility=float(implied_vol),
            delta=float(option_delta),
            gamma=float(option_gamma),
            theta=float(option_theta),
            vega=float(option_vega),
        )

    def build_request(
        self,
        *,
        symbol: str,
        option_symbol: str,
        option_type: str,
        spot_price: float,
        strike_price: float,
        bid: float,
        ask: float,
        expiration,
        as_of,
    ) -> GreeksRequest:
        return GreeksRequest(
            symbol=symbol,
            option_symbol=option_symbol,
            option_type=option_type,
            spot_price=spot_price,
            strike_price=strike_price,
            bid=bid,
            ask=ask,
            expiration=expiration,
            as_of=as_of,
            risk_free_rate=self.risk_free_rate,
            dividend_yield=self.default_dividend_yield,
        )

    @staticmethod
    def _resolve_option_flag(option_type: str) -> str | None:
        normalized = option_type.lower()
        if normalized in {"call", "c"}:
            return "c"
        if normalized in {"put", "p"}:
            return "p"
        return None

    @staticmethod
    def _intrinsic_value(*, option_type: str, spot_price: float, strike_price: float) -> float:
        if option_type == "c":
            return max(spot_price - strike_price, 0.0)
        return max(strike_price - spot_price, 0.0)
