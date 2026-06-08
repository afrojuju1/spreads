from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator
import yaml

from core.services.config_inheritance import resolve_policy_mapping
from core.services.payload_validation import (
    normalize_mapping,
    normalize_optional_text,
    normalize_required_text,
    normalize_text_tuple,
    validate_payload_model,
)
from core.services.strategy_specs import StrategySpec, resolve_strategy_spec
from core.services.trading_strategy_models import (
    EntrySelectionPolicy,
    RoutineSchedule,
    StrategyBuildConfig,
    StrategyExecutionPolicy,
    StrategyEntryQualityPolicy,
    StrategyLiquidityRules,
    StrategyRiskDefaults,
    StrategyRiskLimits,
    StrategyRuntimeControls,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TRADING_CONFIG_ROOT = REPO_ROOT / "packages" / "config"
NEW_YORK = ZoneInfo("America/New_York")

STATIC_SOURCE = "static"
DYNAMIC_SOURCE = "dynamic"


def _canonical_hash(payload: dict[str, Any]) -> str:
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(rendered.encode("utf-8")).hexdigest()


def _load_yaml_file(path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected mapping payload in {path}")
    return raw


def _yaml_directory_signature(root: Path) -> tuple[tuple[str, int, int], ...]:
    if not root.exists():
        return ()
    signature: list[tuple[str, int, int]] = []
    for path in sorted(root.glob("*.yaml")):
        stat = path.stat()
        signature.append((path.name, stat.st_mtime_ns, stat.st_size))
    return tuple(signature)


def _yaml_file_signature(path: Path) -> tuple[str, int, int] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return (path.name, stat.st_mtime_ns, stat.st_size)


class UniverseYamlPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbols: tuple[str, ...]

    @field_validator("symbols", mode="before")
    @classmethod
    def _normalize_symbols(cls, value: Any) -> tuple[str, ...]:
        return normalize_text_tuple(value, uppercase=True, require_non_empty=True)


class StrategySourceYamlPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    kind: Literal["static", "dynamic"] = Field(alias="type")
    ref: str
    max_age_seconds: int | None = None
    max_symbols: int | None = None
    fallback_universe_ref: str | None = None

    @field_validator("kind", mode="before")
    @classmethod
    def _normalize_kind(cls, value: Any) -> str:
        rendered = str(value or "").strip().lower()
        if rendered not in {STATIC_SOURCE, DYNAMIC_SOURCE}:
            raise ValueError(f"must be one of: {STATIC_SOURCE}, {DYNAMIC_SOURCE}")
        return rendered

    @field_validator("ref", mode="before")
    @classmethod
    def _normalize_ref(cls, value: Any) -> str:
        return normalize_required_text(value)

    @field_validator("fallback_universe_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return normalize_optional_text(value)

    @field_validator("max_age_seconds", "max_symbols", mode="before")
    @classmethod
    def _normalize_optional_int(cls, value: Any) -> Any:
        if value in (None, ""):
            return None
        return value

    @field_validator("max_age_seconds", "max_symbols")
    @classmethod
    def _require_positive_int(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("must be positive")
        return value


class StrategyRoutineYamlPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    schedule: dict[str, Any] = Field(default_factory=dict)
    selection: dict[str, Any] = Field(default_factory=dict)
    quality_profile: str | None = None
    quality_overrides: dict[str, Any] = Field(default_factory=dict)
    recipes: tuple[str, ...] = Field(default_factory=tuple)
    policy: dict[str, Any] = Field(default_factory=dict)

    @field_validator("schedule", "selection", "quality_overrides", "policy", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: Any) -> dict[str, Any]:
        return normalize_mapping(value)

    @field_validator("quality_profile", mode="before")
    @classmethod
    def _normalize_quality_profile(cls, value: Any) -> str | None:
        return normalize_optional_text(value)

    @field_validator("recipes", mode="before")
    @classmethod
    def _normalize_recipes(cls, value: Any) -> tuple[str, ...]:
        return normalize_text_tuple(value)


class TradingStrategyYamlPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trading_strategy_id: str
    name: str
    enabled: bool = True
    source: StrategySourceYamlPayload
    trade_structure: str
    build: dict[str, Any] = Field(default_factory=dict)
    entry: StrategyRoutineYamlPayload | None = None
    management: StrategyRoutineYamlPayload | None = None
    liquidity: dict[str, Any] = Field(default_factory=dict)
    risk: dict[str, Any] = Field(default_factory=dict)
    runtime: dict[str, Any] = Field(default_factory=dict)
    execution: dict[str, Any] = Field(default_factory=dict)

    @field_validator("trading_strategy_id", "name", "trade_structure", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: Any) -> str:
        return normalize_required_text(value)

    @field_validator("build", "liquidity", "risk", "runtime", "execution", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: Any) -> dict[str, Any]:
        return normalize_mapping(value)


def default_config_root(config_root: str | Path | None = None) -> Path:
    if config_root is None:
        return DEFAULT_TRADING_CONFIG_ROOT
    return Path(config_root).resolve()


@dataclass(frozen=True)
class StrategySource:
    kind: str
    ref: str
    max_age_seconds: int | None = None
    max_symbols: int | None = None
    fallback_universe_ref: str | None = None

    @property
    def is_static(self) -> bool:
        return self.kind == STATIC_SOURCE

    @property
    def is_dynamic(self) -> bool:
        return self.kind == DYNAMIC_SOURCE

    @classmethod
    def from_payload(cls, payload: StrategySourceYamlPayload) -> StrategySource:
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
    def from_payload(cls, routine: str, payload: StrategyRoutineYamlPayload | None) -> StrategyRoutine | None:
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
        payload: dict[str, Any] = {
            "enabled": self.enabled,
            "schedule": self.schedule.as_dict(),
            "selection": self.selection.as_dict(),
            **self.quality.as_dict(),
            "recipes": list(self.recipes),
            "policy": dict(self.policy),
        }
        return payload


@dataclass(frozen=True)
class TradingStrategyConfig:
    trading_strategy_id: str
    name: str
    trade_structure: str
    strategy_spec: StrategySpec
    source: StrategySource
    build: StrategyBuildConfig
    entry: StrategyRoutine | None
    management: StrategyRoutine | None
    liquidity: StrategyLiquidityRules
    position_sizing: StrategyRiskDefaults
    risk_limits: StrategyRiskLimits
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
        return self.strategy_spec.candidate_builder_key

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
    def live_enabled(self) -> bool:
        return self.runtime.live_enabled

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


def _strategy_payload(strategy: TradingStrategyConfig) -> dict[str, Any]:
    return {
        "trading_strategy_id": strategy.trading_strategy_id,
        "name": strategy.name,
        "enabled": strategy.enabled,
        "source": strategy.source.as_dict(),
        "trade_structure": strategy.trade_structure,
        "build": strategy.build.as_builder_params(),
        "entry": None if strategy.entry is None else strategy.entry.as_dict(),
        "management": None if strategy.management is None else strategy.management.as_dict(),
        "liquidity": strategy.liquidity.as_dict(),
        "position_sizing": strategy.position_sizing.as_dict(),
        "risk_limits": strategy.risk_limits.as_dict(),
        "runtime": strategy.runtime.as_dict(),
        "execution": strategy.execution.as_dict(),
    }


def load_universe_symbols(
    universe_ref: str | None,
    *,
    config_root: str | Path | None = None,
) -> tuple[str, ...]:
    if universe_ref is None:
        return ()
    path = default_config_root(config_root) / "universes" / f"{universe_ref}.yaml"
    signature = _yaml_file_signature(path)
    if signature is None:
        raise ValueError(f"Unknown universe_ref: {universe_ref}")
    return _load_universe_symbols_cached(str(path), signature, universe_ref)


@lru_cache(maxsize=64)
def _load_universe_symbols_cached(
    path_key: str,
    signature: tuple[str, int, int],
    universe_ref: str,
) -> tuple[str, ...]:
    del signature
    path = Path(path_key)
    payload = validate_payload_model(UniverseYamlPayload, _load_yaml_file(path), path=path, label=f"universe {universe_ref}")
    return payload.symbols


def _source_symbols(
    source: StrategySource,
    *,
    config_root: str | Path | None,
) -> tuple[str, ...]:
    if source.is_static:
        return load_universe_symbols(source.ref, config_root=config_root)
    if source.fallback_universe_ref:
        return load_universe_symbols(source.fallback_universe_ref, config_root=config_root)
    return ()


@lru_cache(maxsize=8)
def _load_trading_strategies_cached(
    root_key: str,
    signature: tuple[tuple[str, int, int], ...],
    limits_policy_signature: tuple[tuple[str, int, int], ...],
    runtime_policy_signature: tuple[tuple[str, int, int], ...],
    universe_signature: tuple[tuple[str, int, int], ...],
) -> tuple[TradingStrategyConfig, ...]:
    del signature, limits_policy_signature, runtime_policy_signature, universe_signature
    root = Path(root_key)
    config_root = root.parent
    strategies: dict[str, TradingStrategyConfig] = {}
    for path in sorted(root.glob("*.yaml")):
        payload = validate_payload_model(TradingStrategyYamlPayload, _load_yaml_file(path), path=path, label="trading strategy")
        source = StrategySource.from_payload(payload.source)
        trade_structure = payload.trade_structure
        strategy_spec = resolve_strategy_spec(trade_structure)
        risk_limits_payload = resolve_policy_mapping(
            payload.risk.get("limits"),
            field_name="risk.limits",
            policy_kind="strategy_limits",
            config_root=config_root,
            config_path=path,
        )
        runtime_payload = resolve_policy_mapping(
            payload.runtime,
            field_name="runtime",
            policy_kind="strategy_runtime",
            config_root=config_root,
            config_path=path,
        )
        strategy = TradingStrategyConfig(
            trading_strategy_id=payload.trading_strategy_id,
            name=payload.name,
            trade_structure=trade_structure,
            strategy_spec=strategy_spec,
            source=source,
            build=strategy_spec.validate_build(payload.build),
            entry=StrategyRoutine.from_payload("entry", payload.entry),
            management=StrategyRoutine.from_payload("management", payload.management),
            liquidity=StrategyLiquidityRules.from_payload(payload.liquidity),
            position_sizing=StrategyRiskDefaults.from_payload(payload.risk.get("sizing")),
            risk_limits=StrategyRiskLimits.from_payload(risk_limits_payload),
            runtime=StrategyRuntimeControls.from_payload(runtime_payload),
            execution=StrategyExecutionPolicy.from_payload(payload.execution),
            enabled=payload.enabled,
            config_path=path,
            config_hash="",
            symbols=_source_symbols(source, config_root=config_root),
        )
        strategy = TradingStrategyConfig(
            **{
                **strategy.__dict__,
                "config_hash": _canonical_hash(_strategy_payload(strategy)),
            }
        )
        if strategy.trading_strategy_id in strategies:
            raise ValueError(f"Duplicate trading_strategy_id {strategy.trading_strategy_id}")
        strategies[strategy.trading_strategy_id] = strategy
    return tuple(strategies.values())


def load_trading_strategies(
    config_root: str | Path | None = None,
) -> dict[str, TradingStrategyConfig]:
    config_root_path = default_config_root(config_root)
    root = config_root_path / "trading_strategies"
    if not root.exists():
        return {}
    return {
        strategy.trading_strategy_id: strategy
        for strategy in _load_trading_strategies_cached(
            str(root),
            _yaml_directory_signature(root),
            _yaml_directory_signature(config_root_path / "policies" / "strategy_limits"),
            _yaml_directory_signature(config_root_path / "policies" / "strategy_runtime"),
            _yaml_directory_signature(config_root_path / "universes"),
        )
    }


def load_active_trading_strategies(
    config_root: str | Path | None = None,
) -> dict[str, TradingStrategyConfig]:
    return {
        strategy_id: strategy for strategy_id, strategy in load_trading_strategies(config_root).items() if strategy.enabled and not strategy.paused
    }


def resolve_trading_strategy(
    trading_strategy_id: str,
    *,
    config_root: str | Path | None = None,
) -> TradingStrategyConfig:
    strategies = load_trading_strategies(config_root)
    strategy = strategies.get(trading_strategy_id)
    if strategy is None:
        raise ValueError(f"Unknown trading_strategy_id: {trading_strategy_id}")
    return strategy


def resolve_active_trading_strategy(
    trading_strategy_id: str,
    *,
    config_root: str | Path | None = None,
) -> TradingStrategyConfig:
    strategies = load_active_trading_strategies(config_root)
    strategy = strategies.get(trading_strategy_id)
    if strategy is None:
        raise ValueError(f"Unknown or paused trading_strategy_id: {trading_strategy_id}")
    return strategy


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour_text, separator, minute_text = str(value).partition(":")
    if separator != ":":
        raise ValueError(f"Invalid HH:MM time: {value}")
    return int(hour_text), int(minute_text)


def routine_should_run_now(
    routine: StrategyRoutine,
    *,
    now: datetime | None = None,
) -> bool:
    current = (now or datetime.now(NEW_YORK)).astimezone(NEW_YORK)
    if current.weekday() >= 5:
        return False
    schedule = routine.schedule.as_dict()
    if bool(schedule.get("market_hours_only", False)) and not ((9, 30) <= (current.hour, current.minute) <= (16, 0)):
        return False
    start_time = schedule.get("start_time_et")
    if start_time:
        start_hour, start_minute = _parse_hhmm(str(start_time))
        if (current.hour, current.minute) < (start_hour, start_minute):
            return False
    end_time = schedule.get("end_time_et")
    if end_time:
        end_hour, end_minute = _parse_hhmm(str(end_time))
        if (current.hour, current.minute) > (end_hour, end_minute):
            return False
    return True


def cadence_minutes(schedule: dict[str, Any] | RoutineSchedule) -> int:
    if isinstance(schedule, RoutineSchedule):
        return max(int(schedule.cadence_minutes), 1)
    cadence = str(schedule.get("cadence") or "").strip().lower()
    if cadence.endswith("m"):
        return max(int(cadence[:-1]), 1)
    raise ValueError(f"Unsupported routine cadence: {cadence}")


def active_entry_strategies(
    config_root: str | Path | None = None,
    *,
    candidate_builder_key: str | None = None,
    build_profile: str | None = None,
) -> list[TradingStrategyConfig]:
    rows: list[TradingStrategyConfig] = []
    for strategy in load_active_trading_strategies(config_root).values():
        if strategy.entry is None or not strategy.entry.enabled:
            continue
        if candidate_builder_key is not None and strategy.candidate_builder_key != candidate_builder_key:
            continue
        if build_profile is not None and strategy.build_profile != build_profile:
            continue
        rows.append(strategy)
    return rows


def build_entry_strategy_symbols(
    config_root: str | Path | None = None,
    *,
    candidate_builder_key: str | None = None,
    build_profile: str | None = None,
) -> tuple[str, ...]:
    strategies = active_entry_strategies(
        config_root,
        candidate_builder_key=candidate_builder_key,
        build_profile=build_profile,
    )
    return tuple(sorted({symbol for strategy in strategies for symbol in strategy.symbols}))


__all__ = [
    "DYNAMIC_SOURCE",
    "STATIC_SOURCE",
    "StrategyRoutine",
    "StrategySource",
    "TradingStrategyConfig",
    "active_entry_strategies",
    "build_entry_strategy_symbols",
    "cadence_minutes",
    "default_config_root",
    "load_active_trading_strategies",
    "load_trading_strategies",
    "load_universe_symbols",
    "resolve_active_trading_strategy",
    "resolve_trading_strategy",
    "routine_should_run_now",
]
