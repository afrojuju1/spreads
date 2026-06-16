from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import AliasChoices, Field, field_validator, model_validator

from core.model_contracts import DomainModel
from core.services.trading_strategy_build_models import (
    EntrySelectionPolicy,
    RoutineSchedule,
    StrategyBuildConfig,
    StrategyEntryQualityPolicy,
    StrategyLiquidityRules,
)
from core.services.trading_strategy_execution_models import StrategyExecutionPolicy
from core.services.trading_strategy_risk_models import (
    StrategyProtectionPolicy,
    StrategyRiskDefaults,
    StrategyRiskLimits,
    StrategyRuntimeControls,
)

if TYPE_CHECKING:
    from core.services.trade_structure_specs import TradeStructureSpec


class StrategySource(DomainModel):
    kind: str = Field(validation_alias=AliasChoices("kind", "type"), serialization_alias="type")
    ref: str
    max_age_seconds: int | None = None
    max_symbols: int | None = None
    fallback_universe_ref: str | None = None

    @field_validator("kind", mode="before")
    @classmethod
    def _normalize_kind(cls, value: Any) -> str:
        rendered = str(value or "").strip().lower()
        if rendered not in {"static", "dynamic"}:
            raise ValueError("source type must be static or dynamic")
        return rendered

    @field_validator("ref", "fallback_universe_ref", mode="before")
    @classmethod
    def _normalize_text(cls, value: Any) -> str | None:
        rendered = str(value or "").strip()
        return rendered or None

    @model_validator(mode="after")
    def _validate_ranges(self) -> StrategySource:
        if self.max_age_seconds is not None and self.max_age_seconds <= 0:
            raise ValueError("source.max_age_seconds must be positive")
        if self.max_symbols is not None and self.max_symbols <= 0:
            raise ValueError("source.max_symbols must be positive")
        return self

    @property
    def is_static(self) -> bool:
        return self.kind == "static"

    @property
    def is_dynamic(self) -> bool:
        return self.kind == "dynamic"


class StrategyRoutine(DomainModel):
    routine: str
    schedule: RoutineSchedule
    enabled: bool
    selection: EntrySelectionPolicy
    quality: StrategyEntryQualityPolicy
    recipes: tuple[str, ...]
    policy: dict[str, Any]

    @model_validator(mode="before")
    @classmethod
    def _normalize_payload(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        payload = dict(value)
        if "quality" not in payload:
            payload["quality"] = {
                "quality_profile": payload.pop("quality_profile", None),
                "quality_overrides": payload.pop("quality_overrides", {}),
            }
        return payload

    @property
    def trigger_policy(self) -> dict[str, Any]:
        return self.selection.model_dump(exclude_none=True)

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "schedule": self.schedule.as_dict(),
            "selection": self.selection.model_dump(exclude_none=True),
            **self.quality.model_dump(exclude_none=True, exclude_defaults=True, by_alias=True),
            "recipes": list(self.recipes),
            "policy": dict(self.policy),
        }


@dataclass(frozen=True)
class TradingStrategyConfig:
    trading_strategy_id: str
    name: str
    trade_structure: str
    trade_structure_spec: TradeStructureSpec
    source: StrategySource
    build: StrategyBuildConfig
    entry: StrategyRoutine | None
    management: StrategyRoutine | None
    liquidity: StrategyLiquidityRules
    position_sizing: StrategyRiskDefaults
    risk_limits: StrategyRiskLimits
    protection: StrategyProtectionPolicy
    runtime: StrategyRuntimeControls
    execution: StrategyExecutionPolicy
    enabled: bool
    config_path: Path
    config_hash: str
    symbols: tuple[str, ...]

    @property
    def strategy_id(self) -> str:
        return self.trading_strategy_id

    @property
    def strategy_family(self) -> str:
        return self.trade_structure

    @property
    def candidate_builder_key(self) -> str:
        return self.trade_structure_spec.candidate_builder_key

    @property
    def build_profile(self) -> str:
        dte_max = int(self.build.dte.maximum)
        if dte_max <= 3:
            return "micro"
        if dte_max <= 10:
            return "weekly"
        if dte_max <= 21:
            return "swing"
        return "core"

    @property
    def builder_params(self) -> dict[str, Any]:
        return dict(self.build.as_builder_params())

    @property
    def liquidity_rules(self) -> dict[str, Any]:
        return self.liquidity.model_dump(exclude_none=True)

    @property
    def risk_defaults(self) -> dict[str, Any]:
        return self.position_sizing.model_dump(exclude_none=True)

    @property
    def entry_recipe_refs(self) -> tuple[str, ...]:
        return () if self.entry is None else self.entry.recipes

    @property
    def management_recipe_refs(self) -> tuple[str, ...]:
        return () if self.management is None else self.management.recipes

    @property
    def paused(self) -> bool:
        return self.runtime.paused

    @property
    def max_open_positions(self) -> int:
        return self.risk_limits.max_open_positions

    @property
    def max_daily_actions(self) -> int:
        return self.risk_limits.max_daily_actions

    @property
    def max_new_entries_per_day(self) -> int | None:
        return self.risk_limits.max_new_entries_per_day

    @property
    def daily_loss_limit(self) -> float | None:
        return self.risk_limits.daily_loss_limit


@dataclass(frozen=True)
class StrategyBuildSettings:
    trading_strategy_id: str
    trade_structure: str
    trade_structure_spec: TradeStructureSpec
    build: StrategyBuildConfig
    liquidity: StrategyLiquidityRules
    risk: StrategyRiskDefaults
    candidate_builder_key: str
    build_profile: str
    dte_min: int | None
    dte_max: int | None
    short_delta_min: float | None
    short_delta_max: float | None
    short_delta_target: float | None
    width_points: tuple[float, ...]
    min_open_interest: int | None
    max_leg_spread_pct_mid: float | None
    min_return_on_risk: float | None
    min_fill_ratio: float | None
    min_short_vs_expected_move_ratio: float | None
    min_breakeven_vs_expected_move_ratio: float | None
    max_quote_age_seconds: int | None
    ranking_policy: dict[str, Any]
    builder_params: dict[str, Any]
    liquidity_rules: dict[str, Any]
    risk_defaults: dict[str, Any]


@dataclass(frozen=True)
class EntryRuntime:
    strategy: TradingStrategyConfig
    build_settings: StrategyBuildSettings
    config_hash: str

    @property
    def trading_strategy_id(self) -> str:
        return self.strategy.trading_strategy_id

    @property
    def trade_structure(self) -> str:
        return self.strategy.trade_structure

    @property
    def symbols(self) -> tuple[str, ...]:
        return self.strategy.symbols

    @property
    def source_ref(self) -> str:
        return self.strategy.source.ref

    @property
    def entry_recipe_refs(self) -> tuple[str, ...]:
        return self.strategy.entry_recipe_refs

    @property
    def trigger_policy(self) -> dict[str, Any]:
        if self.strategy.entry is None:
            return {}
        return dict(self.strategy.entry.trigger_policy)

    @property
    def quality_profile_id(self) -> str | None:
        configured = None if self.strategy.entry is None else self.strategy.entry.quality.profile_id
        if configured is None:
            return None
        from core.services.trading_engine.entry_quality import resolve_entry_quality_profile

        resolve_entry_quality_profile(configured)
        return configured

    @property
    def quality_overrides(self) -> dict[str, Any]:
        if self.strategy.entry is None:
            return {}
        return dict(self.strategy.entry.quality.overrides)


@dataclass(frozen=True)
class ManagementRuntime:
    strategy: TradingStrategyConfig
    config_hash: str

    @property
    def trading_strategy_id(self) -> str:
        return self.strategy.trading_strategy_id

    @property
    def trade_structure(self) -> str:
        return self.strategy.trade_structure

    @property
    def symbols(self) -> tuple[str, ...]:
        return self.strategy.symbols

    @property
    def source_ref(self) -> str:
        return self.strategy.source.ref

    @property
    def management_recipe_refs(self) -> tuple[str, ...]:
        return self.strategy.management_recipe_refs

    @property
    def trigger_policy(self) -> dict[str, Any]:
        if self.strategy.management is None:
            return {}
        return dict(self.strategy.management.trigger_policy)


__all__ = [
    "EntryRuntime",
    "ManagementRuntime",
    "StrategyBuildSettings",
    "StrategyRoutine",
    "StrategySource",
    "TradingStrategyConfig",
]
