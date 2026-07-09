from __future__ import annotations

from functools import lru_cache
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError, field_validator

from core.model_contracts import DomainModel
from core.services.config_inheritance import load_yaml_mapping
from core.services.payload_validation import format_validation_error
from core.services.trading_strategies import default_config_root
from core.value_coercion import (
    coerce_float,
    coerce_int,
)

OPEN_POSITION_STATUSES = ["open", "partial_close"]
ACTIVE_PORTFOLIO_INTENT_STATES = [
    "pending",
    "claimed",
    "submitted",
    "partially_filled",
]
CLOSE_RECONCILIATION_MAX_AGE_SECONDS = 180
POSITION_SIZING_STRATEGIES = {
    "short_call",
    "short_put",
    "call_credit_spread",
    "put_credit_spread",
    "call_debit_spread",
    "put_debit_spread",
    "iron_condor",
    "long_call",
    "long_put",
    "long_straddle",
    "long_strangle",
}
BASELINE_RISK_POLICY_NAME = "baseline"
RISK_POLICY_DERIVED_FLAGS = {
    "max_contracts_per_position_configured": False,
}
ACCOUNT_CAPACITY_REQUEST_TIMEOUT_SECONDS = 5.0
ENTRY_CAPACITY_ADMISSION_BOUNDARY = "entry_capacity_precheck"
ALLOCATION_PLAN_BOUNDARY = "allocation_plan"
PROTECTION_ADMISSION_BOUNDARY = "protection_admission"
PORTFOLIO_ADMISSION_BOUNDARY = "portfolio_admission"
DEFERRED_EXECUTION_READINESS_REASON = "deferred_to_broker_activity"
ALLOCATION_DECISION_LIMIT = 200
MARKET_CONTEXT_FILTER_ID = "market_context_regime_fit"
BROAD_INDEX_CORRELATION_SYMBOLS = {"SPY", "QQQ", "DIA", "IWM"}
PROTECTION_ADMISSIBLE_STATUSES = {"admissible", "approved", "ok", "pass", "passed"}
TERMINAL_ENTRY_ATTEMPT_STATUSES = {
    "blocked",
    "canceled",
    "cancelled",
    "expired",
    "failed",
    "rejected",
}


class BaselineRiskPolicyYamlPayload(DomainModel):
    enabled: bool
    allow_live: bool
    max_open_positions_per_session: int = Field(gt=0)
    max_open_positions_per_underlying: int = Field(gt=0)
    max_open_positions_per_underlying_strategy: int = Field(gt=0)
    max_contracts_per_position: int = Field(gt=0)
    max_contracts_per_session: int = Field(gt=0)
    max_position_notional: float | None = Field(ge=0)
    max_session_notional: float | None = Field(ge=0)
    max_position_max_loss: float | None = Field(ge=0)
    max_session_max_loss: float | None = Field(ge=0)
    stale_quote_after_seconds: float = Field(gt=0)


class RuntimeRiskPolicyOverride(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    enabled: bool | None = None
    allow_live: bool | None = None
    max_open_positions_per_session: int | None = Field(default=None, gt=0)
    max_open_positions_per_underlying: int | None = Field(default=None, gt=0)
    max_open_positions_per_underlying_strategy: int | None = Field(
        default=None,
        gt=0,
        validation_alias=AliasChoices("max_open_positions_per_underlying_strategy", "duplicate_underlying_strategy_limit"),
    )
    max_contracts_per_position: int | None = Field(default=None, gt=0)
    max_contracts_per_session: int | None = Field(default=None, gt=0)
    max_position_notional: float | None = Field(default=None, ge=0)
    max_session_notional: float | None = Field(default=None, ge=0)
    max_position_max_loss: float | None = Field(default=None, ge=0)
    max_session_max_loss: float | None = Field(default=None, ge=0)
    stale_quote_after_seconds: float | None = Field(
        default=None,
        gt=0,
        validation_alias=AliasChoices("stale_quote_after_seconds", "max_candidate_age_seconds"),
    )

    @field_validator("*", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Any) -> Any:
        return None if value == "" else value


class RuntimeRiskPolicy(BaselineRiskPolicyYamlPayload):
    max_contracts_per_position_configured: bool = False


class ProtectionRuleConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")

    enabled: bool = False

    @field_validator("*", mode="before")
    @classmethod
    def _blank_to_none(cls, value: Any) -> Any:
        return None if value == "" else value

    @property
    def configured(self) -> bool:
        return self.enabled or bool(self.__pydantic_extra__ or {})

    def get(self, key: str, default: Any = None) -> Any:
        return (self.__pydantic_extra__ or {}).get(key, default)

    def positive_int(self, key: str) -> int | None:
        value = coerce_int(self.get(key))
        if value is None or value <= 0:
            return None
        return value

    def positive_float(self, *keys: str) -> float | None:
        for key in keys:
            value = coerce_float(self.get(key))
            if value is not None and value > 0:
                return value
        return None


@lru_cache(maxsize=1)
def _baseline_risk_policy() -> dict[str, Any]:
    path = default_config_root() / "policies" / "risk" / f"{BASELINE_RISK_POLICY_NAME}.yaml"
    try:
        payload = BaselineRiskPolicyYamlPayload.model_validate(load_yaml_mapping(path))
    except ValidationError as exc:
        raise ValueError(f"Invalid baseline risk policy config in {path}: {format_validation_error(exc)}") from exc
    return payload.model_dump()

def normalize_risk_policy(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    raw_policy = source.get("risk_policy") if isinstance(source.get("risk_policy"), dict) else source

    overrides = RuntimeRiskPolicyOverride.model_validate(raw_policy).model_dump(exclude_unset=True)
    baseline = _baseline_risk_policy()
    policy = {
        **baseline,
        **RISK_POLICY_DERIVED_FLAGS,
        **overrides,
        "max_contracts_per_position_configured": "max_contracts_per_position" in baseline or "max_contracts_per_position" in overrides,
    }
    return RuntimeRiskPolicy.model_validate(policy).model_dump()
