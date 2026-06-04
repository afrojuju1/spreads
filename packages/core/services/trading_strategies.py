from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import yaml

from core.services.candidate_policy import resolve_strategy_min_return_on_risk
from core.services.config_inheritance import resolve_policy_mapping
from core.services.strategy_specs import StrategySpec, resolve_strategy_spec
from core.services.trading_strategy_models import (
    EntrySelectionPolicy,
    RoutineSchedule,
    StrategyBuildConfig,
    StrategyExecutionPolicy,
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
RANKING_POLICY_ARG_KEYS = (
    "ranking_min_probability_of_profit",
    "ranking_min_expected_value_dollars",
    "ranking_min_slippage_adjusted_expected_value_dollars",
    "ranking_max_entry_slippage_dollars",
    "ranking_min_model_implied_volatility",
    "ranking_max_model_implied_volatility",
    "ranking_weight_probability_of_profit",
    "ranking_weight_expected_value_dollars",
    "ranking_weight_slippage_adjusted_expected_value_dollars",
    "ranking_weight_entry_slippage_dollars",
    "ranking_weight_model_implied_volatility",
)


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


def _as_text(value: Any, *, field_name: str) -> str:
    rendered = str(value or "").strip()
    if not rendered:
        raise ValueError(f"{field_name} is required")
    return rendered


def _as_optional_text(value: Any) -> str | None:
    rendered = str(value or "").strip()
    return rendered or None


def _as_list(value: Any, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return tuple(str(item).strip() for item in value if str(item or "").strip())


def _as_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping")
    return dict(value)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


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
    def from_payload(cls, payload: Mapping[str, Any] | None) -> StrategySource:
        mapping = _as_mapping(payload, field_name="source")
        source_type = _as_text(mapping.get("type"), field_name="source.type").strip().lower()
        if source_type not in {STATIC_SOURCE, DYNAMIC_SOURCE}:
            raise ValueError(f"Unsupported source.type: {source_type}")
        return cls(
            kind=source_type,
            ref=_as_text(mapping.get("ref"), field_name="source.ref"),
            max_age_seconds=_optional_int(mapping.get("max_age_seconds")),
            max_symbols=_optional_int(mapping.get("max_symbols")),
            fallback_universe_ref=_as_optional_text(mapping.get("fallback_universe_ref")),
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
    recipes: tuple[str, ...]
    policy: dict[str, Any]

    @classmethod
    def from_payload(cls, routine: str, payload: Mapping[str, Any] | None) -> StrategyRoutine | None:
        if payload is None:
            return None
        mapping = _as_mapping(payload, field_name=routine)
        return cls(
            routine=routine,
            schedule=RoutineSchedule.from_payload(mapping.get("schedule")),
            enabled=bool(mapping.get("enabled", True)),
            selection=EntrySelectionPolicy.from_payload(mapping.get("selection")),
            recipes=_as_list(mapping.get("recipes"), field_name=f"{routine}.recipes"),
            policy=_as_mapping(mapping.get("policy"), field_name=f"{routine}.policy"),
        )

    @property
    def trigger_policy(self) -> dict[str, Any]:
        return self.selection.as_dict()

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "enabled": self.enabled,
            "schedule": self.schedule.as_dict(),
            "selection": self.selection.as_dict(),
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
    def scanner_strategy(self) -> str:
        return self.strategy_spec.scanner_strategy

    @property
    def scanner_profile(self) -> str:
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
    payload = _load_yaml_file(Path(path_key))
    symbols = _as_list(payload.get("symbols"), field_name=f"{universe_ref}.symbols")
    return tuple(str(symbol).upper() for symbol in symbols)


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
        payload = _load_yaml_file(path)
        source = StrategySource.from_payload(payload.get("source"))
        trade_structure = _as_text(payload.get("trade_structure"), field_name="trade_structure")
        strategy_spec = resolve_strategy_spec(trade_structure)
        risk_payload = _as_mapping(payload.get("risk"), field_name="risk")
        risk_limits_payload = resolve_policy_mapping(
            risk_payload.get("limits"),
            field_name="risk.limits",
            policy_kind="strategy_limits",
            config_root=config_root,
            config_path=path,
        )
        runtime_payload = resolve_policy_mapping(
            payload.get("runtime"),
            field_name="runtime",
            policy_kind="strategy_runtime",
            config_root=config_root,
            config_path=path,
        )
        strategy = TradingStrategyConfig(
            trading_strategy_id=_as_text(payload.get("trading_strategy_id"), field_name="trading_strategy_id"),
            name=_as_text(payload.get("name"), field_name="name"),
            trade_structure=trade_structure,
            strategy_spec=strategy_spec,
            source=source,
            build=strategy_spec.validate_build(payload.get("build")),
            entry=StrategyRoutine.from_payload("entry", payload.get("entry")),
            management=StrategyRoutine.from_payload("management", payload.get("management")),
            liquidity=StrategyLiquidityRules.from_payload(payload.get("liquidity")),
            position_sizing=StrategyRiskDefaults.from_payload(risk_payload.get("sizing")),
            risk_limits=StrategyRiskLimits.from_payload(risk_limits_payload),
            runtime=StrategyRuntimeControls.from_payload(runtime_payload),
            execution=StrategyExecutionPolicy.from_payload(payload.get("execution")),
            enabled=bool(payload.get("enabled", True)),
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
    scanner_strategy: str | None = None,
    scanner_profile: str | None = None,
) -> list[TradingStrategyConfig]:
    rows: list[TradingStrategyConfig] = []
    for strategy in load_active_trading_strategies(config_root).values():
        if strategy.entry is None or not strategy.entry.enabled:
            continue
        if scanner_strategy is not None and strategy.scanner_strategy != scanner_strategy:
            continue
        if scanner_profile is not None and strategy.scanner_profile != scanner_profile:
            continue
        rows.append(strategy)
    return rows


def build_entry_strategy_symbols(
    config_root: str | Path | None = None,
    *,
    scanner_strategy: str | None = None,
    scanner_profile: str | None = None,
) -> tuple[str, ...]:
    strategies = active_entry_strategies(
        config_root,
        scanner_strategy=scanner_strategy,
        scanner_profile=scanner_profile,
    )
    return tuple(sorted({symbol for strategy in strategies for symbol in strategy.symbols}))


def _aggregate_scope_ranking_policy(strategies: list[TradingStrategyConfig]) -> dict[str, float]:
    values_by_key: dict[str, list[float]] = {key: [] for key in RANKING_POLICY_ARG_KEYS}
    for strategy in strategies:
        builder_params = strategy.builder_params
        for key in RANKING_POLICY_ARG_KEYS:
            value = builder_params.get(key)
            if value is None:
                continue
            values_by_key[key].append(float(value))
    payload: dict[str, float] = {}
    for key, values in values_by_key.items():
        if not values:
            continue
        if key.startswith("ranking_weight_"):
            payload[key] = sum(values) / len(values)
        elif key.startswith("ranking_max_"):
            payload[key] = max(values)
        else:
            payload[key] = min(values)
    return payload


def build_discovery_run_scope(
    config_root: str | Path | None = None,
    *,
    scanner_strategy: str | None = None,
    scanner_profile: str | None = None,
) -> dict[str, Any]:
    strategies = active_entry_strategies(
        config_root,
        scanner_strategy=scanner_strategy,
        scanner_profile=scanner_profile,
    )
    static_strategies = [strategy for strategy in strategies if strategy.source.is_static]
    if not static_strategies:
        return {
            "enabled": False,
            "symbols": (),
            "scanner_strategy": None,
            "scanner_profile": None,
            "entry_strategies": [],
        }
    symbols = sorted({symbol for strategy in static_strategies for symbol in strategy.symbols})
    scanner_strategies = {strategy.scanner_strategy for strategy in static_strategies}
    scanner_profiles = {strategy.scanner_profile for strategy in static_strategies}
    universe_refs = {strategy.source.ref for strategy in static_strategies if strategy.source.ref}
    dte_mins = [
        int(strategy.builder_params.get("dte_min") or 0) for strategy in static_strategies if strategy.builder_params.get("dte_min") is not None
    ]
    dte_maxs = [
        int(strategy.builder_params.get("dte_max") or 0) for strategy in static_strategies if strategy.builder_params.get("dte_max") is not None
    ]
    short_delta_mins = [
        float(strategy.builder_params.get("short_delta_min") or 0.0)
        for strategy in static_strategies
        if strategy.builder_params.get("short_delta_min") is not None
    ]
    short_delta_maxs = [
        float(strategy.builder_params.get("short_delta_max") or 0.0)
        for strategy in static_strategies
        if strategy.builder_params.get("short_delta_max") is not None
    ]
    short_delta_targets = [
        float(strategy.builder_params.get("short_delta_target") or 0.0)
        for strategy in static_strategies
        if strategy.builder_params.get("short_delta_target") is not None
    ]
    short_delta_target = None
    if short_delta_targets:
        short_delta_target = sum(short_delta_targets) / len(short_delta_targets)
    elif short_delta_mins and short_delta_maxs:
        short_delta_target = (min(short_delta_mins) + max(short_delta_maxs)) / 2.0
    widths = [float(width) for strategy in static_strategies for width in list(strategy.builder_params.get("width_points") or [])]
    open_interest_values = [
        int(strategy.liquidity_rules.get("min_open_interest") or 0)
        for strategy in static_strategies
        if strategy.liquidity_rules.get("min_open_interest") is not None
    ]
    relative_spread_values = [
        float(strategy.liquidity_rules.get("max_leg_spread_pct_mid") or 0.0)
        for strategy in static_strategies
        if strategy.liquidity_rules.get("max_leg_spread_pct_mid") is not None
    ]
    return_on_risk_values = [
        float(minimum_return_on_risk)
        for strategy in static_strategies
        if (
            minimum_return_on_risk := resolve_strategy_min_return_on_risk(
                strategy.scanner_profile,
                risk_defaults=strategy.risk_defaults,
            )
        )
        is not None
    ]
    return {
        "enabled": True,
        "symbols": tuple(symbols),
        "scanner_strategy": None if len(scanner_strategies) != 1 else next(iter(scanner_strategies)),
        "scanner_profile": None if len(scanner_profiles) != 1 else next(iter(scanner_profiles)),
        "universe_ref": None if len(universe_refs) != 1 else next(iter(universe_refs)),
        "scanner_args": {
            **({} if not dte_mins else {"min_dte": min(dte_mins)}),
            **({} if not dte_maxs else {"max_dte": max(dte_maxs)}),
            **({} if not short_delta_mins else {"short_delta_min": min(short_delta_mins)}),
            **({} if not short_delta_maxs else {"short_delta_max": max(short_delta_maxs)}),
            **({} if short_delta_target is None else {"short_delta_target": short_delta_target}),
            **({} if not widths else {"min_width": min(widths), "max_width": max(widths)}),
            **({} if not open_interest_values else {"min_open_interest": min(open_interest_values)}),
            **({} if not relative_spread_values else {"max_relative_spread": max(relative_spread_values)}),
            **({} if not return_on_risk_values else {"min_return_on_risk": min(return_on_risk_values)}),
            **_aggregate_scope_ranking_policy(static_strategies),
        },
        "entry_strategies": static_strategies,
    }


def build_discovery_run_scopes(
    config_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[TradingStrategyConfig]] = {}
    for strategy in active_entry_strategies(config_root):
        if not strategy.source.is_static:
            continue
        key = (strategy.scanner_strategy, strategy.scanner_profile)
        groups.setdefault(key, []).append(strategy)

    scopes: list[dict[str, Any]] = []
    for scanner_strategy, scanner_profile in sorted(groups):
        scope = build_discovery_run_scope(
            config_root,
            scanner_strategy=scanner_strategy,
            scanner_profile=scanner_profile,
        )
        if scope.get("enabled"):
            scopes.append(scope)
    return scopes


__all__ = [
    "DYNAMIC_SOURCE",
    "STATIC_SOURCE",
    "StrategyRoutine",
    "StrategySource",
    "TradingStrategyConfig",
    "active_entry_strategies",
    "build_discovery_run_scope",
    "build_discovery_run_scopes",
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
