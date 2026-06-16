from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic import TypeAdapter

from core.services.payload_validation import normalize_mapping, normalize_optional_text, normalize_required_text

BOOLEAN_CONFIG_VALUE = TypeAdapter(bool)


@dataclass(frozen=True)
class StrategyOrderStylePolicy:
    order_type: str = "limit"
    time_in_force: str = "day"
    pricing_mode: str = "adaptive_credit"
    min_credit_retention_pct: float = 0.95

    def __post_init__(self) -> None:
        order_type = self.order_type.lower()
        time_in_force = self.time_in_force.lower()
        pricing_mode = self.pricing_mode.lower()
        if order_type != "limit":
            raise ValueError("execution.order_style.order_type must be limit")
        if time_in_force not in {"day", "gtc"}:
            raise ValueError("execution.order_style.time_in_force must be day or gtc")
        if pricing_mode not in {"midpoint", "adaptive_credit", "adaptive_debit", "adaptive"}:
            raise ValueError("execution.order_style.pricing_mode is unsupported")
        if self.min_credit_retention_pct < 0.5 or self.min_credit_retention_pct > 1.0:
            raise ValueError("execution.order_style.min_credit_retention_pct must be between 0.5 and 1.0")
        object.__setattr__(self, "order_type", order_type)
        object.__setattr__(self, "time_in_force", time_in_force)
        object.__setattr__(self, "pricing_mode", pricing_mode)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> StrategyOrderStylePolicy:
        mapping = normalize_mapping(payload, field_name="execution.order_style")
        return cls(
            order_type=normalize_required_text(
                mapping.get("order_type") or mapping.get("type") or "limit",
                field_name="execution.order_style.order_type",
            ),
            time_in_force=normalize_required_text(
                mapping.get("time_in_force") or "day",
                field_name="execution.order_style.time_in_force",
            ),
            pricing_mode=normalize_required_text(
                mapping.get("pricing_mode") or "adaptive_credit",
                field_name="execution.order_style.pricing_mode",
            ),
            min_credit_retention_pct=(
                float(mapping["min_credit_retention_pct"]) if mapping.get("min_credit_retention_pct") not in (None, "") else 0.95
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "pricing_mode": self.pricing_mode,
            "min_credit_retention_pct": self.min_credit_retention_pct,
        }


@dataclass(frozen=True)
class StrategyQuoteFreshnessPolicy:
    max_age_seconds: int = 180

    def __post_init__(self) -> None:
        if self.max_age_seconds <= 0:
            raise ValueError("execution.quote_freshness.max_age_seconds must be positive")

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> StrategyQuoteFreshnessPolicy:
        mapping = normalize_mapping(payload, field_name="execution.quote_freshness")
        return cls(max_age_seconds=int(mapping["max_age_seconds"]) if mapping.get("max_age_seconds") not in (None, "") else 180)

    def as_dict(self) -> dict[str, Any]:
        return {"max_age_seconds": self.max_age_seconds}


@dataclass(frozen=True)
class StrategyOrderRepricingPolicy:
    enabled: bool = True
    stale_after_seconds: int = 75
    max_reprices: int = 3
    price_step: float = 0.01
    max_concession: float = 0.03

    def __post_init__(self) -> None:
        if self.stale_after_seconds <= 0:
            raise ValueError("execution lifecycle repricing.stale_after_seconds must be positive")
        if self.max_reprices < 0:
            raise ValueError("execution lifecycle repricing.max_reprices must be non-negative")
        if self.price_step <= 0:
            raise ValueError("execution lifecycle repricing.price_step must be positive")
        if self.max_concession < 0:
            raise ValueError("execution lifecycle repricing.max_concession must be non-negative")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
        *,
        default_stale_after_seconds: int = 75,
        default_max_concession: float = 0.03,
    ) -> StrategyOrderRepricingPolicy:
        mapping = normalize_mapping(payload, field_name="execution lifecycle repricing")
        stale_after_value = mapping.get("stale_after_seconds")
        if stale_after_value in (None, ""):
            stale_after_value = mapping.get("ttl_seconds")
        max_reprices_value = mapping.get("max_reprices")
        if max_reprices_value in (None, ""):
            max_reprices_value = mapping.get("max_reprice_count")
        max_concession_value = mapping.get("max_concession")
        if max_concession_value in (None, ""):
            max_concession_value = mapping.get("max_credit_concession")
        return cls(
            enabled=BOOLEAN_CONFIG_VALUE.validate_python(mapping["enabled"]) if mapping.get("enabled") not in (None, "") else True,
            stale_after_seconds=int(stale_after_value) if stale_after_value not in (None, "") else default_stale_after_seconds,
            max_reprices=int(max_reprices_value) if max_reprices_value not in (None, "") else 3,
            price_step=float(mapping.get("price_step") or mapping.get("step")) if mapping.get("price_step") or mapping.get("step") else 0.01,
            max_concession=(
                float(max_concession_value) if max_concession_value not in (None, "") else default_max_concession
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "stale_after_seconds": self.stale_after_seconds,
            "max_reprices": self.max_reprices,
            "price_step": self.price_step,
            "max_concession": self.max_concession,
        }


@dataclass(frozen=True)
class StrategyOrderLifecyclePolicy:
    submit_ttl_minutes: int = 5
    stale_order_action: str = "cancel_and_reprice"
    repricing: StrategyOrderRepricingPolicy = field(default_factory=StrategyOrderRepricingPolicy)

    def __post_init__(self) -> None:
        stale_order_action = self.stale_order_action.lower()
        if self.submit_ttl_minutes <= 0:
            raise ValueError("execution lifecycle submit_ttl_minutes must be positive")
        if stale_order_action not in {"cancel_and_reprice", "leave_working", "fail_closed"}:
            raise ValueError("execution lifecycle stale_order_action is unsupported")
        object.__setattr__(self, "stale_order_action", stale_order_action)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
        *,
        default_stale_after_seconds: int = 75,
        default_max_concession: float = 0.03,
    ) -> StrategyOrderLifecyclePolicy:
        mapping = normalize_mapping(payload, field_name="execution lifecycle")
        repricing_payload = mapping.get("repricing")
        submit_ttl_value = mapping.get("submit_ttl_minutes")
        if submit_ttl_value in (None, ""):
            submit_ttl_value = mapping.get("ttl_minutes")
        return cls(
            submit_ttl_minutes=int(submit_ttl_value) if submit_ttl_value not in (None, "") else 5,
            stale_order_action=normalize_required_text(
                mapping.get("stale_order_action") or "cancel_and_reprice",
                field_name="execution lifecycle stale_order_action",
            ),
            repricing=StrategyOrderRepricingPolicy.from_payload(
                repricing_payload if isinstance(repricing_payload, Mapping) else None,
                default_stale_after_seconds=default_stale_after_seconds,
                default_max_concession=default_max_concession,
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "submit_ttl_minutes": self.submit_ttl_minutes,
            "stale_order_action": self.stale_order_action,
            "repricing": self.repricing.as_dict(),
        }


@dataclass(frozen=True)
class StrategyExecutionPolicy:
    approval: str
    mode: str
    runtime: str = "alpaca_direct"
    executor_profile_id: str | None = None
    order_style: StrategyOrderStylePolicy = field(default_factory=StrategyOrderStylePolicy)
    quote_freshness: StrategyQuoteFreshnessPolicy = field(default_factory=StrategyQuoteFreshnessPolicy)
    open_lifecycle: StrategyOrderLifecyclePolicy = field(default_factory=StrategyOrderLifecyclePolicy)
    close_lifecycle: StrategyOrderLifecyclePolicy = field(default_factory=StrategyOrderLifecyclePolicy)
    unsupported_structure_behavior: str = "fail_closed"

    def __post_init__(self) -> None:
        approval = self.approval.lower()
        mode = self.mode.lower()
        runtime = self.runtime.lower()
        unsupported_structure_behavior = self.unsupported_structure_behavior.lower()
        if approval not in {"auto", "manual"}:
            raise ValueError("execution.approval must be auto or manual")
        if mode not in {"paper", "live", "shadow"}:
            raise ValueError("execution.mode must be paper, live, or shadow")
        if runtime != "alpaca_direct":
            raise ValueError("execution.runtime must be alpaca_direct")
        if unsupported_structure_behavior not in {"fail_closed"}:
            raise ValueError("execution.unsupported_structure_behavior must be fail_closed")
        object.__setattr__(self, "approval", approval)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "runtime", runtime)
        object.__setattr__(self, "unsupported_structure_behavior", unsupported_structure_behavior)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyExecutionPolicy:
        mapping = normalize_mapping(payload, field_name="execution")
        return cls(
            approval=normalize_required_text(
                mapping.get("approval") or mapping.get("approval_mode"),
                field_name="execution.approval",
            ),
            mode=normalize_required_text(mapping.get("mode"), field_name="execution.mode"),
            runtime=str(mapping.get("runtime") or "alpaca_direct"),
            executor_profile_id=normalize_optional_text(mapping.get("executor_profile_id") or mapping.get("profile_id")),
            order_style=StrategyOrderStylePolicy.from_payload(mapping.get("order_style") if isinstance(mapping.get("order_style"), Mapping) else None),
            quote_freshness=StrategyQuoteFreshnessPolicy.from_payload(
                mapping.get("quote_freshness") if isinstance(mapping.get("quote_freshness"), Mapping) else None
            ),
            open_lifecycle=StrategyOrderLifecyclePolicy.from_payload(
                mapping.get("open_lifecycle") if isinstance(mapping.get("open_lifecycle"), Mapping) else None,
                default_stale_after_seconds=75,
                default_max_concession=0.03,
            ),
            close_lifecycle=StrategyOrderLifecyclePolicy.from_payload(
                mapping.get("close_lifecycle") if isinstance(mapping.get("close_lifecycle"), Mapping) else None,
                default_stale_after_seconds=75,
                default_max_concession=0.03,
            ),
            unsupported_structure_behavior=normalize_required_text(
                mapping.get("unsupported_structure_behavior") or "fail_closed",
                field_name="execution.unsupported_structure_behavior",
            ),
        )

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
            "repricing_policy": lifecycle.repricing.as_dict(),
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
            "order_style": self.order_style.as_dict(),
            "quote_freshness": self.quote_freshness.as_dict(),
            "lifecycle": lifecycle.as_dict(),
            "unsupported_structure_behavior": self.unsupported_structure_behavior,
        }

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "approval": self.approval,
            "mode": self.mode,
            "runtime": self.runtime,
            "order_style": self.order_style.as_dict(),
            "quote_freshness": self.quote_freshness.as_dict(),
            "open_lifecycle": self.open_lifecycle.as_dict(),
            "close_lifecycle": self.close_lifecycle.as_dict(),
            "unsupported_structure_behavior": self.unsupported_structure_behavior,
        }
        if self.executor_profile_id is not None:
            payload["executor_profile_id"] = self.executor_profile_id
        return payload


__all__ = [
    "StrategyExecutionPolicy",
    "StrategyOrderLifecyclePolicy",
    "StrategyOrderRepricingPolicy",
    "StrategyOrderStylePolicy",
    "StrategyQuoteFreshnessPolicy",
]
