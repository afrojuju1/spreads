from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic import TypeAdapter
from whenever import Time

from core.services.payload_validation import (
    normalize_mapping,
    normalize_optional_text,
    normalize_required_text,
)

BOOLEAN_CONFIG_VALUE = TypeAdapter(bool)
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


@dataclass(frozen=True)
class StrategyRiskDefaults:
    min_return_on_risk: float | None = None
    position_size_pct_of_available_balance: float | None = None
    max_risk_per_trade: float | None = None
    max_credit_slippage_pct: float | None = None

    def __post_init__(self) -> None:
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

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyRiskDefaults:
        mapping = normalize_mapping(payload, field_name="risk")
        return cls(
            min_return_on_risk=float(mapping["min_return_on_risk"]) if mapping.get("min_return_on_risk") not in (None, "") else None,
            position_size_pct_of_available_balance=(
                float(mapping["position_size_pct_of_available_balance"])
                if mapping.get("position_size_pct_of_available_balance") not in (None, "")
                else None
            ),
            max_risk_per_trade=float(mapping["max_risk_per_trade"]) if mapping.get("max_risk_per_trade") not in (None, "") else None,
            max_credit_slippage_pct=(
                float(mapping["max_credit_slippage_pct"]) if mapping.get("max_credit_slippage_pct") not in (None, "") else None
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.min_return_on_risk is not None:
            payload["min_return_on_risk"] = self.min_return_on_risk
        if self.position_size_pct_of_available_balance is not None:
            payload["position_size_pct_of_available_balance"] = self.position_size_pct_of_available_balance
        if self.max_risk_per_trade is not None:
            payload["max_risk_per_trade"] = self.max_risk_per_trade
        if self.max_credit_slippage_pct is not None:
            payload["max_credit_slippage_pct"] = self.max_credit_slippage_pct
        return payload


@dataclass(frozen=True)
class StrategyPortfolioAdmissionLimits:
    max_strategy_open_positions: int | None = None
    max_family_open_positions: int | None = None
    max_symbol_family_open_positions: int | None = None
    max_daily_new_entries: int | None = None
    max_total_strategy_risk: float | None = None
    max_correlated_group_open_positions: int | None = None
    configured: bool = False

    def __post_init__(self) -> None:
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

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyPortfolioAdmissionLimits:
        mapping = normalize_mapping(payload, field_name="risk.limits.portfolio_admission")
        return cls(
            max_strategy_open_positions=(
                int(mapping["max_strategy_open_positions"]) if mapping.get("max_strategy_open_positions") not in (None, "") else None
            ),
            max_family_open_positions=(
                int(mapping["max_family_open_positions"]) if mapping.get("max_family_open_positions") not in (None, "") else None
            ),
            max_symbol_family_open_positions=(
                int(mapping["max_symbol_family_open_positions"]) if mapping.get("max_symbol_family_open_positions") not in (None, "") else None
            ),
            max_daily_new_entries=int(mapping["max_daily_new_entries"]) if mapping.get("max_daily_new_entries") not in (None, "") else None,
            max_total_strategy_risk=(
                float(mapping["max_total_strategy_risk"]) if mapping.get("max_total_strategy_risk") not in (None, "") else None
            ),
            max_correlated_group_open_positions=(
                int(mapping["max_correlated_group_open_positions"])
                if mapping.get("max_correlated_group_open_positions") not in (None, "")
                else None
            ),
            configured=bool(mapping),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key, value in (
            ("max_strategy_open_positions", self.max_strategy_open_positions),
            ("max_family_open_positions", self.max_family_open_positions),
            ("max_symbol_family_open_positions", self.max_symbol_family_open_positions),
            ("max_daily_new_entries", self.max_daily_new_entries),
            ("max_total_strategy_risk", self.max_total_strategy_risk),
            ("max_correlated_group_open_positions", self.max_correlated_group_open_positions),
        ):
            if value is not None:
                payload[key] = value
        return payload

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
            **self.as_dict(),
        }


@dataclass(frozen=True)
class StrategyRiskLimits:
    max_open_positions: int
    max_daily_actions: int
    max_new_entries_per_day: int | None = None
    daily_loss_limit: float | None = None
    portfolio_admission: StrategyPortfolioAdmissionLimits = field(default_factory=StrategyPortfolioAdmissionLimits)

    def __post_init__(self) -> None:
        if self.max_open_positions < 0:
            raise ValueError("risk.limits.max_open_positions must be >= 0")
        if self.max_daily_actions < 0:
            raise ValueError("risk.limits.max_daily_actions must be >= 0")
        if self.max_new_entries_per_day is not None and self.max_new_entries_per_day < 0:
            raise ValueError("risk.limits.max_new_entries_per_day must be >= 0")
        if self.daily_loss_limit is not None and self.daily_loss_limit < 0:
            raise ValueError("risk.limits.daily_loss_limit must be >= 0")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyRiskLimits:
        mapping = normalize_mapping(payload, field_name="risk.limits")
        return cls(
            max_open_positions=int(mapping.get("max_open_positions") or 0),
            max_daily_actions=int(mapping.get("max_daily_actions") or 0),
            max_new_entries_per_day=(
                int(mapping["max_new_entries_per_day"]) if mapping.get("max_new_entries_per_day") not in (None, "") else None
            ),
            daily_loss_limit=float(mapping["daily_loss_limit"]) if mapping.get("daily_loss_limit") not in (None, "") else None,
            portfolio_admission=StrategyPortfolioAdmissionLimits.from_payload(
                mapping.get("portfolio_admission"),
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "max_open_positions": self.max_open_positions,
            "max_daily_actions": self.max_daily_actions,
        }
        if self.max_new_entries_per_day is not None:
            payload["max_new_entries_per_day"] = self.max_new_entries_per_day
        if self.daily_loss_limit is not None:
            payload["daily_loss_limit"] = self.daily_loss_limit
        if self.portfolio_admission.configured:
            payload["portfolio_admission"] = self.portfolio_admission.as_dict()
        return payload


@dataclass(frozen=True)
class StrategyProtectionPolicy:
    profile_id: str | None = None
    rules: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        unknown = sorted(set(self.rules) - PROTECTION_RULE_KEYS)
        if unknown:
            joined = ", ".join(unknown)
            raise ValueError(f"protection.rules contains unsupported rule(s): {joined}")
        for rule_name, rule_payload in self.rules.items():
            if not isinstance(rule_payload, Mapping):
                raise ValueError(f"protection.rules.{rule_name} must be a mapping")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyProtectionPolicy:
        mapping = normalize_mapping(payload, field_name="protection")
        raw_rules = normalize_mapping(mapping.get("rules"), field_name="protection.rules")
        rules: dict[str, Mapping[str, Any]] = {}
        for raw_rule_name, raw_rule_payload in raw_rules.items():
            rule_name = normalize_required_text(raw_rule_name, field_name="protection.rules key")
            rules[rule_name] = dict(
                normalize_mapping(
                    raw_rule_payload,
                    field_name=f"protection.rules.{rule_name}",
                )
            )
        return cls(
            profile_id=normalize_optional_text(mapping.get("protection_model_id") or mapping.get("profile_id")),
            rules=rules,
        )

    @property
    def configured(self) -> bool:
        return bool(self.rules)

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"rules": {key: dict(value) for key, value in self.rules.items()}}
        if self.profile_id is not None:
            payload["protection_model_id"] = self.profile_id
        return payload


@dataclass(frozen=True)
class StrategyRuntimeControls:
    cancel_pending_entries_after_et: str | None = None
    flatten_positions_at_et: str | None = None
    paused: bool = False

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyRuntimeControls:
        mapping = normalize_mapping(payload, field_name="runtime")
        cancel_pending_entries_after_et = normalize_optional_text(mapping.get("cancel_pending_entries_after_et"))
        flatten_positions_at_et = normalize_optional_text(mapping.get("flatten_positions_at_et"))
        if cancel_pending_entries_after_et is not None:
            try:
                cancel_time = Time.parse_iso(cancel_pending_entries_after_et)
            except ValueError as exc:
                raise ValueError("runtime.cancel_pending_entries_after_et must be HH:MM") from exc
            if cancel_time.second or cancel_time.nanosecond:
                raise ValueError("runtime.cancel_pending_entries_after_et must be HH:MM")
            cancel_pending_entries_after_et = f"{cancel_time.hour:02d}:{cancel_time.minute:02d}"
        if flatten_positions_at_et is not None:
            try:
                flatten_time = Time.parse_iso(flatten_positions_at_et)
            except ValueError as exc:
                raise ValueError("runtime.flatten_positions_at_et must be HH:MM") from exc
            if flatten_time.second or flatten_time.nanosecond:
                raise ValueError("runtime.flatten_positions_at_et must be HH:MM")
            flatten_positions_at_et = f"{flatten_time.hour:02d}:{flatten_time.minute:02d}"
        return cls(
            cancel_pending_entries_after_et=cancel_pending_entries_after_et,
            flatten_positions_at_et=flatten_positions_at_et,
            paused=BOOLEAN_CONFIG_VALUE.validate_python(mapping["paused"]) if mapping.get("paused") not in (None, "") else False,
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "paused": self.paused,
        }
        if self.cancel_pending_entries_after_et is not None:
            payload["cancel_pending_entries_after_et"] = self.cancel_pending_entries_after_et
        if self.flatten_positions_at_et is not None:
            payload["flatten_positions_at_et"] = self.flatten_positions_at_et
        return payload


__all__ = [
    "PROTECTION_RULE_KEYS",
    "StrategyPortfolioAdmissionLimits",
    "StrategyProtectionPolicy",
    "StrategyRiskDefaults",
    "StrategyRiskLimits",
    "StrategyRuntimeControls",
]
