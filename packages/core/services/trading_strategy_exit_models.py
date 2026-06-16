from __future__ import annotations

from typing import Any

from pydantic import Field, field_validator, model_validator

from core.model_contracts import DomainModel
from core.services.payload_validation import normalize_optional_text, normalize_required_text, normalize_text_tuple
from core.value_coercion import coerce_bool, coerce_float, coerce_int

DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE = 10
DEFAULT_PROFIT_TARGET_PCT = 0.5
DEFAULT_STOP_MULTIPLE = 2.0
DEBIT_PROFIT_TARGET_PCT = 0.4
DEBIT_MAX_LOSS_40PCT_STOP_MULTIPLE = 1.0 / 0.6


class UnderlyingInvalidationConfig(DomainModel):
    enabled: bool = True
    min_intraday_bars: int | None = None
    recent_lookback_bars: int | None = None
    confirmation_bars: int | None = None
    breakdown_tolerance_bps: float | None = None
    max_spread_pct: float | None = None
    max_quote_age_seconds: int | None = None

    @field_validator("enabled", mode="before")
    @classmethod
    def _normalize_enabled(cls, value: Any) -> bool:
        return coerce_bool(value, default=True)

    @field_validator("min_intraday_bars", "recent_lookback_bars", "confirmation_bars", "max_quote_age_seconds", mode="before")
    @classmethod
    def _normalize_positive_int(cls, value: Any) -> int | None:
        parsed = coerce_int(value)
        if parsed is not None and parsed <= 0:
            raise ValueError("underlying_invalidation integer thresholds must be positive")
        return parsed

    @field_validator("breakdown_tolerance_bps", "max_spread_pct", mode="before")
    @classmethod
    def _normalize_non_negative_float(cls, value: Any) -> float | None:
        parsed = coerce_float(value)
        if parsed is not None and parsed < 0:
            raise ValueError("underlying_invalidation numeric thresholds must be non-negative")
        return parsed


class ExitControllerPolicy(DomainModel):
    enabled: bool = True
    profit_target_pct: float = DEFAULT_PROFIT_TARGET_PCT
    stop_multiple: float | None = DEFAULT_STOP_MULTIPLE
    stop_loss_pct: float | None = None
    force_close_at: str | None = None
    force_close_minutes_before_close: int | None = DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE
    max_spread_pct: float | None = None
    max_quote_age_seconds: int | None = None
    underlying_invalidation: UnderlyingInvalidationConfig | None = None

    @field_validator("enabled", mode="before")
    @classmethod
    def _normalize_enabled(cls, value: Any) -> bool:
        return coerce_bool(value, default=True)

    @field_validator("profit_target_pct", "stop_multiple", "stop_loss_pct", "max_spread_pct", mode="before")
    @classmethod
    def _normalize_non_negative_float(cls, value: Any) -> float | None:
        parsed = coerce_float(value)
        if parsed is not None and parsed < 0:
            raise ValueError("exit policy numeric thresholds must be non-negative")
        return parsed

    @field_validator("force_close_minutes_before_close", "max_quote_age_seconds", mode="before")
    @classmethod
    def _normalize_positive_int(cls, value: Any) -> int | None:
        parsed = coerce_int(value)
        if parsed is not None and parsed <= 0:
            raise ValueError("exit policy integer thresholds must be positive")
        return parsed

    @field_validator("force_close_at", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return normalize_optional_text(value)

    @model_validator(mode="before")
    @classmethod
    def _unwrap_exit_policy(cls, value: Any) -> Any:
        if isinstance(value, dict) and isinstance(value.get("exit_policy"), dict):
            return value["exit_policy"]
        return value

    @model_validator(mode="after")
    def _validate_policy(self) -> ExitControllerPolicy:
        if self.stop_multiple is not None and self.stop_multiple <= 0:
            raise ValueError("exit policy stop_multiple must be positive")
        if self.profit_target_pct < 0:
            raise ValueError("exit policy profit_target_pct must be non-negative")
        return self

    @classmethod
    def from_recipe_refs(
        cls,
        recipe_refs: tuple[str, ...] = (),
        *,
        existing_policy: dict[str, Any] | None = None,
    ) -> ExitControllerPolicy:
        payload = cls.model_validate(existing_policy or {}).to_exit_policy_payload()
        for recipe_ref in recipe_refs:
            normalized = normalize_required_text(recipe_ref).lower()
            if normalized == "take_profit_50pct":
                payload["profit_target_pct"] = 0.5
            elif normalized == "take_profit_40pct":
                payload["profit_target_pct"] = DEBIT_PROFIT_TARGET_PCT
            elif normalized == "max_loss_2x_credit":
                payload["stop_multiple"] = DEFAULT_STOP_MULTIPLE
                payload.pop("stop_loss_pct", None)
            elif normalized == "max_loss_40pct_debit":
                payload["stop_multiple"] = DEBIT_MAX_LOSS_40PCT_STOP_MULTIPLE
                payload.pop("stop_loss_pct", None)
            elif normalized == "expiry_day_exit":
                payload.setdefault("force_close_minutes_before_close", DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE)
            else:
                raise ValueError(f"unknown management recipe: {normalized}")
        return cls.model_validate(payload)

    def to_exit_policy_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class ExitControllerProfile(DomainModel):
    routine_profile: str
    recipes: tuple[str, ...] = Field(default_factory=tuple)
    policy: ExitControllerPolicy = Field(default_factory=ExitControllerPolicy)

    @field_validator("routine_profile", mode="before")
    @classmethod
    def _normalize_routine_profile(cls, value: Any) -> str:
        return normalize_required_text(value)

    @field_validator("recipes", mode="before")
    @classmethod
    def _normalize_recipes(cls, value: Any) -> tuple[str, ...]:
        return normalize_text_tuple(value)


__all__ = [
    "DEFAULT_FORCE_CLOSE_MINUTES_BEFORE_CLOSE",
    "DEFAULT_PROFIT_TARGET_PCT",
    "DEFAULT_STOP_MULTIPLE",
    "DEBIT_MAX_LOSS_40PCT_STOP_MULTIPLE",
    "DEBIT_PROFIT_TARGET_PCT",
    "ExitControllerPolicy",
    "ExitControllerProfile",
    "UnderlyingInvalidationConfig",
]
