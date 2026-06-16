from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

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


@dataclass(frozen=True)
class StrategySource:
    kind: str
    ref: str
    max_age_seconds: int | None = None
    max_symbols: int | None = None
    fallback_universe_ref: str | None = None

    @property
    def is_static(self) -> bool:
        return self.kind == "static"

    @property
    def is_dynamic(self) -> bool:
        return self.kind == "dynamic"

    @classmethod
    def from_payload(cls, payload: Any) -> StrategySource:
        return cls(
            kind=payload.kind,
            ref=payload.ref,
            max_age_seconds=payload.max_age_seconds,
            max_symbols=payload.max_symbols,
            fallback_universe_ref=payload.fallback_universe_ref,
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": self.kind,
            "ref": self.ref,
        }
        if self.max_age_seconds is not None:
            payload["max_age_seconds"] = self.max_age_seconds
        if self.max_symbols is not None:
            payload["max_symbols"] = self.max_symbols
        if self.fallback_universe_ref is not None:
            payload["fallback_universe_ref"] = self.fallback_universe_ref
        return payload


@dataclass(frozen=True)
class StrategyRoutine:
    routine: str
    schedule: RoutineSchedule
    enabled: bool
    selection: EntrySelectionPolicy
    quality: StrategyEntryQualityPolicy
    recipes: tuple[str, ...]
    policy: dict[str, Any]

    @classmethod
    def from_payload(cls, routine: str, payload: Any | None) -> StrategyRoutine | None:
        if payload is None:
            return None
        return cls(
            routine=routine,
            schedule=RoutineSchedule.from_payload(payload.schedule),
            enabled=payload.enabled,
            selection=EntrySelectionPolicy.from_payload(payload.selection),
            quality=StrategyEntryQualityPolicy.from_payload(
                quality_profile=payload.quality_profile,
                quality_overrides=payload.quality_overrides,
            ),
            recipes=payload.recipes,
            policy=payload.policy,
        )

    @property
    def trigger_policy(self) -> dict[str, Any]:
        return self.selection.as_dict()

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "schedule": self.schedule.as_dict(),
            "selection": self.selection.as_dict(),
            **self.quality.as_dict(),
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
        return self.liquidity.as_dict()

    @property
    def risk_defaults(self) -> dict[str, Any]:
        return self.position_sizing.as_dict()

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
