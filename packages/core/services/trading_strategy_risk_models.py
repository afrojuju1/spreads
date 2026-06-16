from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator
from whenever import Time

PROTECTION_RULE_KEYS = frozenset(
    {
        "account_emergency_stop",
        "daily_drawdown_halt",
        "rolling_drawdown_halt",
        "loss_streak_cooldown",
        "strategy_family_cooldown",
        "event_calendar_block",
        "duplicate_underlying_theme_cap",
        "options_exposure_scenario_cap",
    }
)


class RiskConfigModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)


class StrategyRiskDefaults(RiskConfigModel):
    min_return_on_risk: float | None = None
    position_size_pct_of_available_balance: float | None = None
    max_risk_per_trade: float | None = None
    max_credit_slippage_pct: float | None = None

    @model_validator(mode="after")
    def _validate_ranges(self) -> StrategyRiskDefaults:
        if self.min_return_on_risk is not None and self.min_return_on_risk <= 0:
            raise ValueError("risk.min_return_on_risk must be > 0")
        if self.position_size_pct_of_available_balance is not None and (
            self.position_size_pct_of_available_balance <= 0 or self.position_size_pct_of_available_balance > 1
        ):
            raise ValueError("risk.position_size_pct_of_available_balance must be > 0 and <= 1")
        if self.max_risk_per_trade is not None and self.max_risk_per_trade <= 0:
            raise ValueError("risk.max_risk_per_trade must be > 0")
        if self.max_credit_slippage_pct is not None and self.max_credit_slippage_pct < 0:
            raise ValueError("risk.max_credit_slippage_pct must be >= 0")
        return self


class StrategyPortfolioAdmissionLimits(RiskConfigModel):
    max_strategy_open_positions: int | None = None
    max_family_open_positions: int | None = None
    max_symbol_family_open_positions: int | None = None
    max_daily_new_entries: int | None = None
    max_total_strategy_risk: float | None = None
    max_correlated_group_open_positions: int | None = None
    configured: bool = False

    @model_validator(mode="before")
    @classmethod
    def _mark_configured(cls, value: Any) -> Any:
        if value is None:
            return {"configured": False}
        if isinstance(value, Mapping):
            payload = dict(value)
            payload.setdefault("configured", bool(payload))
            return payload
        return value

    @model_validator(mode="after")
    def _validate_ranges(self) -> StrategyPortfolioAdmissionLimits:
        for field_name, value in (
            (
                "risk.limits.portfolio_admission.max_strategy_open_positions",
                self.max_strategy_open_positions,
            ),
            (
                "risk.limits.portfolio_admission.max_family_open_positions",
                self.max_family_open_positions,
            ),
            (
                "risk.limits.portfolio_admission.max_symbol_family_open_positions",
                self.max_symbol_family_open_positions,
            ),
            (
                "risk.limits.portfolio_admission.max_daily_new_entries",
                self.max_daily_new_entries,
            ),
            (
                "risk.limits.portfolio_admission.max_correlated_group_open_positions",
                self.max_correlated_group_open_positions,
            ),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        if self.max_total_strategy_risk is not None and self.max_total_strategy_risk < 0:
            raise ValueError("risk.limits.portfolio_admission.max_total_strategy_risk must be >= 0")
        return self

    def as_policy(
        self,
        *,
        trading_strategy_id: str,
        strategy_family: str,
    ) -> dict[str, Any]:
        return {
            "trading_strategy_id": trading_strategy_id,
            "strategy_family": strategy_family,
            "policy_source": "strategy_config",
            **self.model_dump(exclude_none=True, exclude={"configured"}),
        }


class StrategyRiskLimits(RiskConfigModel):
    max_open_positions: int = 0
    max_daily_actions: int = 0
    max_new_entries_per_day: int | None = None
    daily_loss_limit: float | None = None
    portfolio_admission: StrategyPortfolioAdmissionLimits = Field(default_factory=StrategyPortfolioAdmissionLimits)

    @model_validator(mode="after")
    def _validate_ranges(self) -> StrategyRiskLimits:
        if self.max_open_positions < 0:
            raise ValueError("risk.limits.max_open_positions must be >= 0")
        if self.max_daily_actions < 0:
            raise ValueError("risk.limits.max_daily_actions must be >= 0")
        if self.max_new_entries_per_day is not None and self.max_new_entries_per_day < 0:
            raise ValueError("risk.limits.max_new_entries_per_day must be >= 0")
        if self.daily_loss_limit is not None and self.daily_loss_limit < 0:
            raise ValueError("risk.limits.daily_loss_limit must be >= 0")
        return self

    def dump_config(self) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True, exclude={"portfolio_admission"})
        if self.portfolio_admission.configured:
            payload["portfolio_admission"] = self.portfolio_admission.model_dump(exclude_none=True, exclude={"configured"})
        return payload


class StrategyProtectionPolicy(RiskConfigModel):
    profile_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("protection_model_id", "profile_id"),
        serialization_alias="protection_model_id",
    )
    rules: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @field_validator("profile_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        rendered = str(value or "").strip()
        return rendered or None

    @model_validator(mode="after")
    def _validate_rules(self) -> StrategyProtectionPolicy:
        unknown = sorted(set(self.rules) - PROTECTION_RULE_KEYS)
        if unknown:
            joined = ", ".join(unknown)
            raise ValueError(f"protection.rules contains unsupported rule(s): {joined}")
        return self

    @property
    def configured(self) -> bool:
        return bool(self.rules)


class StrategyRuntimeControls(RiskConfigModel):
    cancel_pending_entries_after_et: str | None = None
    flatten_positions_at_et: str | None = None
    paused: bool = False

    @field_validator("cancel_pending_entries_after_et", "flatten_positions_at_et", mode="before")
    @classmethod
    def _normalize_hhmm(cls, value: Any) -> str | None:
        rendered = str(value or "").strip()
        if not rendered:
            return None
        try:
            parsed = Time.parse_iso(rendered)
        except ValueError as exc:
            raise ValueError("must be HH:MM") from exc
        if parsed.second or parsed.nanosecond:
            raise ValueError("must be HH:MM")
        return f"{parsed.hour:02d}:{parsed.minute:02d}"


__all__ = [
    "PROTECTION_RULE_KEYS",
    "StrategyPortfolioAdmissionLimits",
    "StrategyProtectionPolicy",
    "StrategyRiskDefaults",
    "StrategyRiskLimits",
    "StrategyRuntimeControls",
]
