from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, Field, field_validator, model_validator

from core.model_contracts import DomainModel


class StrategyOrderStylePolicy(DomainModel):
    order_type: Literal["limit"] = Field(default="limit", validation_alias=AliasChoices("order_type", "type"))
    time_in_force: Literal["day", "gtc"] = "day"
    pricing_mode: Literal["midpoint", "adaptive_credit", "adaptive_debit", "adaptive"] = "adaptive_credit"
    min_credit_retention_pct: float = 0.95

    @field_validator("order_type", "time_in_force", "pricing_mode", mode="before")
    @classmethod
    def _normalize_text(cls, value: Any) -> str:
        return str(value or "").strip().lower()

    @model_validator(mode="after")
    def _validate_range(self) -> StrategyOrderStylePolicy:
        if self.min_credit_retention_pct < 0.5 or self.min_credit_retention_pct > 1.0:
            raise ValueError("execution.order_style.min_credit_retention_pct must be between 0.5 and 1.0")
        return self


class StrategyQuoteFreshnessPolicy(DomainModel):
    max_age_seconds: int = 180

    @model_validator(mode="after")
    def _validate_range(self) -> StrategyQuoteFreshnessPolicy:
        if self.max_age_seconds <= 0:
            raise ValueError("execution.quote_freshness.max_age_seconds must be positive")
        return self


class StrategyOrderRepricingPolicy(DomainModel):
    enabled: bool = True
    stale_after_seconds: int = Field(default=75, validation_alias=AliasChoices("stale_after_seconds", "ttl_seconds"))
    max_reprices: int = Field(default=3, validation_alias=AliasChoices("max_reprices", "max_reprice_count"))
    price_step: float = Field(default=0.01, validation_alias=AliasChoices("price_step", "step"))
    max_concession: float = Field(default=0.03, validation_alias=AliasChoices("max_concession", "max_credit_concession"))

    @model_validator(mode="after")
    def _validate_ranges(self) -> StrategyOrderRepricingPolicy:
        if self.stale_after_seconds <= 0:
            raise ValueError("execution lifecycle repricing.stale_after_seconds must be positive")
        if self.max_reprices < 0:
            raise ValueError("execution lifecycle repricing.max_reprices must be non-negative")
        if self.price_step <= 0:
            raise ValueError("execution lifecycle repricing.price_step must be positive")
        if self.max_concession < 0:
            raise ValueError("execution lifecycle repricing.max_concession must be non-negative")
        return self


class StrategyOrderLifecyclePolicy(DomainModel):
    submit_ttl_minutes: int = Field(default=5, validation_alias=AliasChoices("submit_ttl_minutes", "ttl_minutes"))
    stale_order_action: Literal["cancel_and_reprice", "leave_working", "fail_closed"] = "cancel_and_reprice"
    repricing: StrategyOrderRepricingPolicy = Field(default_factory=StrategyOrderRepricingPolicy)

    @field_validator("stale_order_action", mode="before")
    @classmethod
    def _normalize_action(cls, value: Any) -> str:
        return str(value or "").strip().lower()

    @model_validator(mode="after")
    def _validate_range(self) -> StrategyOrderLifecyclePolicy:
        if self.submit_ttl_minutes <= 0:
            raise ValueError("execution lifecycle submit_ttl_minutes must be positive")
        return self


class StrategyExecutionPolicy(DomainModel):
    approval: Literal["auto", "manual"] = Field(validation_alias=AliasChoices("approval", "approval_mode"))
    mode: Literal["paper", "live", "shadow"]
    runtime: Literal["alpaca_direct"] = "alpaca_direct"
    executor_profile_id: str | None = Field(default=None, validation_alias=AliasChoices("executor_profile_id", "profile_id"))
    order_style: StrategyOrderStylePolicy = Field(default_factory=StrategyOrderStylePolicy)
    quote_freshness: StrategyQuoteFreshnessPolicy = Field(default_factory=StrategyQuoteFreshnessPolicy)
    open_lifecycle: StrategyOrderLifecyclePolicy = Field(default_factory=StrategyOrderLifecyclePolicy)
    close_lifecycle: StrategyOrderLifecyclePolicy = Field(default_factory=StrategyOrderLifecyclePolicy)
    unsupported_structure_behavior: Literal["fail_closed"] = "fail_closed"

    @field_validator("approval", "mode", "runtime", "unsupported_structure_behavior", mode="before")
    @classmethod
    def _normalize_text(cls, value: Any) -> str:
        return str(value or "").strip().lower()

    @field_validator("executor_profile_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        rendered = str(value or "").strip()
        return rendered or None

    def lifecycle_for_action(self, action_type: str) -> StrategyOrderLifecyclePolicy:
        return self.close_lifecycle if str(action_type or "").strip().lower() == "close" else self.open_lifecycle

    def execution_policy_for_action(self, action_type: str, *, quantity: int | None = None) -> dict[str, Any]:
        lifecycle = self.lifecycle_for_action(action_type)
        payload = {
            "enabled": self.approval == "auto" and self.mode in {"paper", "live"},
            "mode": "top_promotable",
            "pricing_mode": self.order_style.pricing_mode,
            "min_credit_retention_pct": self.order_style.min_credit_retention_pct,
            "max_credit_concession": lifecycle.repricing.max_concession,
            "order_type": self.order_style.order_type,
            "time_in_force": self.order_style.time_in_force,
            "max_quote_age_seconds": self.quote_freshness.max_age_seconds,
            "submit_ttl_minutes": lifecycle.submit_ttl_minutes,
            "stale_order_action": lifecycle.stale_order_action,
            "unsupported_structure_behavior": self.unsupported_structure_behavior,
            "repricing_policy": lifecycle.repricing.model_dump(),
        }
        if quantity is not None:
            payload["quantity"] = quantity
        return payload

    def executor_profile_snapshot(self, action_type: str) -> dict[str, Any]:
        lifecycle = self.lifecycle_for_action(action_type)
        return {
            "executor_profile_id": self.executor_profile_id,
            "approval": self.approval,
            "mode": self.mode,
            "runtime": self.runtime,
            "order_style": self.order_style.model_dump(),
            "quote_freshness": self.quote_freshness.model_dump(),
            "lifecycle": lifecycle.model_dump(),
            "unsupported_structure_behavior": self.unsupported_structure_behavior,
        }


__all__ = [
    "StrategyExecutionPolicy",
    "StrategyOrderLifecyclePolicy",
    "StrategyOrderRepricingPolicy",
    "StrategyOrderStylePolicy",
    "StrategyQuoteFreshnessPolicy",
]
