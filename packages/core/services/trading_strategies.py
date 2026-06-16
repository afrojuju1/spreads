from __future__ import annotations

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
from core.services.trade_structure_specs import resolve_trade_structure_spec
from core.services.trading_strategy_build_models import RoutineSchedule, StrategyLiquidityRules
from core.services.trading_strategy_execution_models import StrategyExecutionPolicy
from core.services.trading_strategy_risk_models import (
    StrategyProtectionPolicy,
    StrategyRiskDefaults,
    StrategyRiskLimits,
    StrategyRuntimeControls,
)
from core.services.trading_strategy_runtime_models import (
    StrategyRoutine,
    StrategySource,
    TradingStrategyConfig,
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


class StrategyRoutinePayload(BaseModel):
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


class TradingStrategyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trading_strategy_id: str
    name: str
    enabled: bool = True
    source: StrategySourceYamlPayload
    trade_structure: str
    build: dict[str, Any] = Field(default_factory=dict)
    entry: StrategyRoutinePayload | None = None
    management: StrategyRoutinePayload | None = None
    liquidity: dict[str, Any] = Field(default_factory=dict)
    risk: dict[str, Any] = Field(default_factory=dict)
    protection: dict[str, Any] = Field(default_factory=dict)
    runtime: dict[str, Any] = Field(default_factory=dict)
    execution: dict[str, Any] = Field(default_factory=dict)

    @field_validator("trading_strategy_id", "name", "trade_structure", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: Any) -> str:
        return normalize_required_text(value)

    @field_validator("build", "liquidity", "risk", "protection", "runtime", "execution", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: Any) -> dict[str, Any]:
        return normalize_mapping(value)


class StrategyActivationPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    state: Literal["active", "disabled"] = "active"
    paused: bool = False

    @field_validator("state", mode="before")
    @classmethod
    def _normalize_state(cls, value: Any) -> str:
        rendered = normalize_required_text(value or "active").lower()
        if rendered not in {"active", "disabled"}:
            raise ValueError("activation.state must be active or disabled")
        return rendered


class StrategyCatalogExecutionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["shadow", "paper", "live"]
    approval: str | None = None
    runtime: str | None = None
    executor_profile: str | None = None

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value: Any) -> str:
        rendered = normalize_required_text(value).lower()
        if rendered not in {"shadow", "paper", "live"}:
            raise ValueError("execution.mode must be shadow, paper, or live")
        return rendered

    @field_validator("approval", "runtime", "executor_profile", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return normalize_optional_text(value)


class StrategyCatalogRoutinePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    routine_profile: str | None = None
    exit_controller: str | None = None
    enabled: bool = True
    selection: dict[str, Any] = Field(default_factory=dict)
    quality_overrides: dict[str, Any] = Field(default_factory=dict)
    recipes: tuple[str, ...] = Field(default_factory=tuple)
    policy: dict[str, Any] = Field(default_factory=dict)

    @field_validator("routine_profile", "exit_controller", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return normalize_optional_text(value)

    @field_validator("selection", "quality_overrides", "policy", mode="before")
    @classmethod
    def _normalize_mapping(cls, value: Any) -> dict[str, Any]:
        return normalize_mapping(value)

    @field_validator("recipes", mode="before")
    @classmethod
    def _normalize_recipes(cls, value: Any) -> tuple[str, ...]:
        return normalize_text_tuple(value)


class StrategyCatalogEntryPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trading_strategy_id: str
    name: str
    activation: StrategyActivationPayload = Field(default_factory=StrategyActivationPayload)
    execution: StrategyCatalogExecutionPayload
    archetype: str
    trade_structure: str
    structure_model: str | None = None
    source_model: str | None = None
    liquidity_profile: str | None = None
    portfolio_model: str | None = None
    protection_model: str | None = None
    executor_profile: str | None = None
    thesis: str | None = None
    entry: StrategyCatalogRoutinePayload | None = None
    management: StrategyCatalogRoutinePayload | None = None

    @field_validator(
        "trading_strategy_id",
        "name",
        "archetype",
        "trade_structure",
        mode="before",
    )
    @classmethod
    def _normalize_required_text(cls, value: Any) -> str:
        return normalize_required_text(value)

    @field_validator(
        "structure_model",
        "source_model",
        "liquidity_profile",
        "portfolio_model",
        "protection_model",
        "executor_profile",
        "thesis",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        return normalize_optional_text(value)


class StrategyCatalogPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = 1
    profiles: str = "profiles.yaml"
    strategies: tuple[StrategyCatalogEntryPayload, ...]

    @field_validator("profiles", mode="before")
    @classmethod
    def _normalize_profiles(cls, value: Any) -> str:
        return normalize_required_text(value or "profiles.yaml")

    @field_validator("strategies", mode="before")
    @classmethod
    def _normalize_strategies(cls, value: Any) -> Any:
        if not isinstance(value, list) or not value:
            raise ValueError("strategies must be a non-empty list")
        return value


def default_config_root(config_root: str | Path | None = None) -> Path:
    if config_root is None:
        return DEFAULT_TRADING_CONFIG_ROOT
    return Path(config_root).resolve()


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
        "protection": strategy.protection.as_dict(),
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


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _required_mapping(payload: dict[str, Any], key: str, *, label: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{label}.{key} must be a mapping")
    return value


def _resolve_profile(
    profiles: dict[str, Any],
    section_name: str,
    profile_id: str,
    *,
    stack: tuple[str, ...] = (),
) -> dict[str, Any]:
    section = _required_mapping(profiles, section_name, label="profiles")
    raw = section.get(profile_id)
    if not isinstance(raw, dict):
        raise ValueError(f"Unknown {section_name} profile: {profile_id}")
    if profile_id in stack:
        cycle = " -> ".join((*stack, profile_id))
        raise ValueError(f"Profile inheritance cycle in {section_name}: {cycle}")
    payload = dict(raw)
    parent = normalize_optional_text(payload.pop("extends", None))
    if parent is None:
        return payload
    base = _resolve_profile(profiles, section_name, parent, stack=(*stack, profile_id))
    return _deep_merge(base, payload)


def _profile_ref(
    *,
    entry_ref: str | None,
    archetype: dict[str, Any],
    archetype_key: str,
    required: bool = True,
) -> str | None:
    ref = entry_ref or normalize_optional_text(archetype.get(archetype_key))
    if required and ref is None:
        raise ValueError(f"archetype.{archetype_key} is required")
    return ref


def _compose_entry_routine(
    *,
    profiles: dict[str, Any],
    archetype: dict[str, Any],
    structure_model: dict[str, Any],
    entry: StrategyCatalogRoutinePayload | None,
) -> dict[str, Any]:
    routine = entry or StrategyCatalogRoutinePayload()
    routine_profile_ref = _profile_ref(
        entry_ref=routine.routine_profile,
        archetype=archetype,
        archetype_key="entry_routine_profile",
    )
    if routine_profile_ref is None:
        raise ValueError("entry routine profile is required")
    payload: dict[str, Any] = {
        "enabled": routine.enabled,
        "schedule": _resolve_profile(profiles, "routine_profiles", routine_profile_ref),
        "selection": dict(routine.selection),
        "quality_profile": normalize_required_text(structure_model.get("quality_profile")),
        "quality_overrides": dict(routine.quality_overrides),
        "recipes": list(routine.recipes),
        "policy": dict(routine.policy),
    }
    return payload


def _compose_management_routine(
    *,
    profiles: dict[str, Any],
    archetype: dict[str, Any],
    management: StrategyCatalogRoutinePayload | None,
) -> dict[str, Any]:
    routine = management or StrategyCatalogRoutinePayload()
    exit_controller_ref = _profile_ref(
        entry_ref=routine.exit_controller,
        archetype=archetype,
        archetype_key="exit_controller",
    )
    if exit_controller_ref is None:
        raise ValueError("management exit controller is required")
    exit_controller = _resolve_profile(profiles, "exit_controllers", exit_controller_ref)
    routine_profile_ref = routine.routine_profile or normalize_optional_text(exit_controller.get("routine_profile"))
    if routine_profile_ref is None:
        raise ValueError(f"exit_controller {exit_controller_ref} must declare routine_profile")
    default_recipes = normalize_text_tuple(exit_controller.get("recipes"))
    payload: dict[str, Any] = {
        "enabled": routine.enabled,
        "schedule": _resolve_profile(profiles, "routine_profiles", routine_profile_ref),
        "selection": dict(routine.selection),
        "quality_overrides": dict(routine.quality_overrides),
        "recipes": list(routine.recipes or default_recipes),
        "policy": _deep_merge(normalize_mapping(exit_controller.get("policy")), dict(routine.policy)),
    }
    return payload


def _validate_trade_structure(
    *,
    strategy: StrategyCatalogEntryPayload,
    archetype: dict[str, Any],
    structure_model: dict[str, Any],
) -> None:
    configured = strategy.trade_structure
    structure_family = normalize_optional_text(archetype.get("structure_family"))
    allowed = normalize_text_tuple(archetype.get("allowed_trade_structures"))
    if structure_family is not None and configured != structure_family:
        raise ValueError(
            f"{strategy.trading_strategy_id} trade_structure {configured!r} does not match archetype structure_family {structure_family!r}"
        )
    if allowed and configured not in allowed:
        raise ValueError(f"{strategy.trading_strategy_id} trade_structure {configured!r} is not allowed by archetype {strategy.archetype}")
    resolve_trade_structure_spec(configured).validate_build(normalize_mapping(structure_model.get("build")))


def _compose_strategy_payload(
    *,
    strategy: StrategyCatalogEntryPayload,
    profiles: dict[str, Any],
) -> dict[str, Any]:
    archetype = _resolve_profile(profiles, "archetypes", strategy.archetype)
    source_model_ref = _profile_ref(
        entry_ref=strategy.source_model,
        archetype=archetype,
        archetype_key="universe_model",
    )
    structure_model_ref = _profile_ref(
        entry_ref=strategy.structure_model,
        archetype=archetype,
        archetype_key="default_structure_model",
    )
    liquidity_profile_ref = _profile_ref(
        entry_ref=strategy.liquidity_profile,
        archetype=archetype,
        archetype_key="liquidity_profile",
    )
    portfolio_model_ref = _profile_ref(
        entry_ref=strategy.portfolio_model,
        archetype=archetype,
        archetype_key="portfolio_model",
    )
    protection_model_ref = _profile_ref(
        entry_ref=strategy.protection_model,
        archetype=archetype,
        archetype_key="protection_model",
    )
    executor_profile_ref = (
        strategy.execution.executor_profile
        or strategy.executor_profile
        or normalize_required_text(archetype.get("executor_profile"))
    )
    if source_model_ref is None or structure_model_ref is None or liquidity_profile_ref is None or portfolio_model_ref is None or protection_model_ref is None:
        raise ValueError(f"{strategy.trading_strategy_id} has incomplete profile references")

    source_model = _resolve_profile(profiles, "source_models", source_model_ref)
    structure_model = _resolve_profile(profiles, "structure_models", structure_model_ref)
    liquidity_profile = _resolve_profile(profiles, "liquidity_profiles", liquidity_profile_ref)
    portfolio_model = _resolve_profile(profiles, "portfolio_models", portfolio_model_ref)
    protection_model = _resolve_profile(profiles, "protection_models", protection_model_ref)
    executor_profile = _resolve_profile(profiles, "executor_profiles", executor_profile_ref)
    _validate_trade_structure(strategy=strategy, archetype=archetype, structure_model=structure_model)

    runtime_payload = normalize_mapping(protection_model.get("runtime"))
    if strategy.activation.paused:
        runtime_payload = _deep_merge(runtime_payload, {"paused": True})
    execution_payload = _deep_merge(
        normalize_mapping(executor_profile),
        {
            "executor_profile_id": executor_profile_ref,
            "mode": strategy.execution.mode,
            **({} if strategy.execution.approval is None else {"approval": strategy.execution.approval}),
            **({} if strategy.execution.runtime is None else {"runtime": strategy.execution.runtime}),
        },
    )
    return {
        "trading_strategy_id": strategy.trading_strategy_id,
        "name": strategy.name,
        "enabled": strategy.activation.state == "active",
        "source": normalize_mapping(source_model.get("source")),
        "trade_structure": strategy.trade_structure,
        "build": normalize_mapping(structure_model.get("build")),
        "entry": _compose_entry_routine(
            profiles=profiles,
            archetype=archetype,
            structure_model=structure_model,
            entry=strategy.entry,
        ),
        "management": _compose_management_routine(
            profiles=profiles,
            archetype=archetype,
            management=strategy.management,
        ),
        "liquidity": liquidity_profile,
        "risk": {
            "limits": normalize_mapping(portfolio_model.get("limits")),
            "sizing": normalize_mapping(portfolio_model.get("sizing")),
        },
        "protection": {
            "protection_model_id": protection_model_ref,
            "rules": normalize_mapping(protection_model.get("rules")),
        },
        "runtime": runtime_payload,
        "execution": execution_payload,
    }


def _load_strategy_catalog(path: Path) -> StrategyCatalogPayload:
    payload = validate_payload_model(StrategyCatalogPayload, _load_yaml_file(path), path=path, label="strategy catalog")
    if payload.version != 1:
        raise ValueError(f"Unsupported strategy catalog version: {payload.version}")
    if payload.profiles != "profiles.yaml":
        raise ValueError("strategy catalog must use profiles: profiles.yaml")
    return payload


def _load_profiles(path: Path) -> dict[str, Any]:
    payload = _load_yaml_file(path)
    version = int(payload.get("version") or 0)
    if version != 1:
        raise ValueError(f"Unsupported strategy profiles version: {version}")
    return payload


@lru_cache(maxsize=8)
def _load_trading_strategies_cached(
    catalog_key: str,
    profiles_key: str,
    catalog_signature: tuple[str, int, int] | None,
    profiles_signature: tuple[str, int, int] | None,
    limits_policy_signature: tuple[tuple[str, int, int], ...],
    runtime_policy_signature: tuple[tuple[str, int, int], ...],
    protection_policy_signature: tuple[tuple[str, int, int], ...],
    universe_signature: tuple[tuple[str, int, int], ...],
) -> tuple[TradingStrategyConfig, ...]:
    del catalog_signature, profiles_signature, limits_policy_signature, runtime_policy_signature, protection_policy_signature, universe_signature
    catalog_path = Path(catalog_key)
    profiles_path = Path(profiles_key)
    config_root = catalog_path.parents[1]
    catalog = _load_strategy_catalog(catalog_path)
    profiles = _load_profiles(profiles_path)
    strategies: dict[str, TradingStrategyConfig] = {}
    for catalog_entry in catalog.strategies:
        payload = validate_payload_model(
            TradingStrategyPayload,
            _compose_strategy_payload(strategy=catalog_entry, profiles=profiles),
            path=catalog_path,
            label="trading strategy",
        )
        source = StrategySource.from_payload(payload.source)
        trade_structure = payload.trade_structure
        trade_structure_spec = resolve_trade_structure_spec(trade_structure)
        risk_limits_payload = resolve_policy_mapping(
            payload.risk.get("limits"),
            field_name="risk.limits",
            policy_kind="strategy_limits",
            config_root=config_root,
            config_path=catalog_path,
        )
        runtime_payload = resolve_policy_mapping(
            payload.runtime,
            field_name="runtime",
            policy_kind="strategy_runtime",
            config_root=config_root,
            config_path=catalog_path,
        )
        protection_payload = resolve_policy_mapping(
            payload.protection,
            field_name="protection",
            policy_kind="strategy_protection",
            config_root=config_root,
            config_path=catalog_path,
        )
        strategy = TradingStrategyConfig(
            trading_strategy_id=payload.trading_strategy_id,
            name=payload.name,
            trade_structure=trade_structure,
            trade_structure_spec=trade_structure_spec,
            source=source,
            build=trade_structure_spec.validate_build(payload.build),
            entry=StrategyRoutine.from_payload("entry", payload.entry),
            management=StrategyRoutine.from_payload("management", payload.management),
            liquidity=StrategyLiquidityRules.from_payload(payload.liquidity),
            position_sizing=StrategyRiskDefaults.from_payload(payload.risk.get("sizing")),
            risk_limits=StrategyRiskLimits.from_payload(risk_limits_payload),
            protection=StrategyProtectionPolicy.from_payload(protection_payload),
            runtime=StrategyRuntimeControls.from_payload(runtime_payload),
            execution=StrategyExecutionPolicy.from_payload(payload.execution),
            enabled=payload.enabled,
            config_path=catalog_path,
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
    root = config_root_path / "strategies"
    catalog_path = root / "catalog.yaml"
    profiles_path = root / "profiles.yaml"
    if not catalog_path.exists() and not profiles_path.exists():
        return {}
    if not catalog_path.exists():
        raise ValueError(f"Missing strategy catalog: {catalog_path}")
    if not profiles_path.exists():
        raise ValueError(f"Missing strategy profiles: {profiles_path}")
    return {
        strategy.trading_strategy_id: strategy
        for strategy in _load_trading_strategies_cached(
            str(catalog_path),
            str(profiles_path),
            _yaml_file_signature(catalog_path),
            _yaml_file_signature(profiles_path),
            _yaml_directory_signature(config_root_path / "policies" / "strategy_limits"),
            _yaml_directory_signature(config_root_path / "policies" / "strategy_runtime"),
            _yaml_directory_signature(config_root_path / "policies" / "strategy_protection"),
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
