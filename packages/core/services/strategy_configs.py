from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from core.services.options_automation_models import (
    StrategyBuildConfig,
    StrategyLiquidityRules,
    StrategyRecipes,
    StrategyRiskDefaults,
)
from core.services.option_structures import normalize_strategy_family
from core.services.strategy_specs import StrategySpec, resolve_strategy_spec

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OPTIONS_AUTOMATION_CONFIG_ROOT = REPO_ROOT / "packages" / "config"


def _canonical_hash(payload: dict[str, Any]) -> str:
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(rendered.encode("utf-8")).hexdigest()


def _load_yaml_file(path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected mapping payload in {path}")
    return raw


def _as_text(value: Any, *, field_name: str) -> str:
    rendered = str(value or "").strip()
    if not rendered:
        raise ValueError(f"{field_name} is required")
    return rendered


def _as_list(value: Any, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return tuple(str(item).strip() for item in value if str(item or "").strip())


def default_config_root(config_root: str | Path | None = None) -> Path:
    if config_root is None:
        return DEFAULT_OPTIONS_AUTOMATION_CONFIG_ROOT
    return Path(config_root).resolve()


def _strategy_config_payload(strategy_config: StrategyConfig) -> dict[str, Any]:
    return {
        "strategy_config_id": strategy_config.strategy_config_id,
        "strategy": {
            "family": strategy_config.strategy_family,
        },
        "enabled": strategy_config.enabled,
        "build": strategy_config.build_payload,
        "recipes": strategy_config.recipes_payload,
        "liquidity": strategy_config.liquidity.as_dict(),
        "risk": strategy_config.risk.as_dict(),
    }


@dataclass(frozen=True)
class StrategyConfig:
    strategy_config_id: str
    strategy_family: str
    strategy_spec: StrategySpec
    build: StrategyBuildConfig
    recipes: StrategyRecipes
    liquidity: StrategyLiquidityRules
    risk: StrategyRiskDefaults
    enabled: bool
    config_path: Path
    config_hash: str

    @property
    def strategy_id(self) -> str:
        return self.strategy_family

    @property
    def builder_params(self) -> dict[str, Any]:
        return dict(self.build.as_builder_params())

    @property
    def build_payload(self) -> dict[str, Any]:
        build_payload = {
            "dte": {
                "min": self.build.dte.minimum,
                "max": self.build.dte.maximum,
            },
            "min_fill_ratio": self.build.min_fill_ratio,
            "expected_move": {
                "min_short_vs_expected_move_ratio": self.build.expected_move.min_short_vs_expected_move_ratio,
                "min_breakeven_vs_expected_move_ratio": self.build.expected_move.min_breakeven_vs_expected_move_ratio,
            },
        }
        if hasattr(self.build, "short_delta"):
            build_payload["short_delta"] = {
                "min": self.build.short_delta.minimum,
                "max": self.build.short_delta.maximum,
                "target": self.build.short_delta.target,
            }
        if hasattr(self.build, "widths"):
            build_payload["widths"] = list(self.build.widths)
        if hasattr(self.build, "entry_delta"):
            build_payload["entry_delta"] = {
                "min": self.build.entry_delta.minimum,
                "max": self.build.entry_delta.maximum,
                "target": self.build.entry_delta.target,
            }
        if hasattr(self.build, "symmetric_wings_only"):
            build_payload["symmetric_wings_only"] = self.build.symmetric_wings_only
        return build_payload

    @property
    def recipes_payload(self) -> dict[str, Any]:
        return {
            "entry": list(self.recipes.entry),
            "management": list(self.recipes.management),
        }

    @property
    def entry_recipe_refs(self) -> tuple[str, ...]:
        return self.recipes.entry

    @property
    def management_recipe_refs(self) -> tuple[str, ...]:
        return self.recipes.management

    @property
    def liquidity_rules(self) -> dict[str, Any]:
        return self.liquidity.as_dict()

    @property
    def risk_defaults(self) -> dict[str, Any]:
        return self.risk.as_dict()

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


def load_strategy_configs(
    config_root: str | Path | None = None,
) -> dict[str, StrategyConfig]:
    root = default_config_root(config_root) / "strategies"
    if not root.exists():
        return {}
    configs: dict[str, StrategyConfig] = {}
    for path in sorted(root.glob("*.yaml")):
        payload = _load_yaml_file(path)
        strategy_payload = payload.get("strategy")
        if not isinstance(strategy_payload, dict):
            raise ValueError(f"strategy must be a mapping in {path}")
        strategy_family = normalize_strategy_family(
            _as_text(strategy_payload.get("family"), field_name="strategy.family")
        )
        strategy_spec = resolve_strategy_spec(strategy_family)
        strategy_config = StrategyConfig(
            strategy_config_id=_as_text(
                payload.get("strategy_config_id"),
                field_name="strategy_config_id",
            ),
            strategy_family=strategy_family,
            strategy_spec=strategy_spec,
            build=strategy_spec.validate_build(payload.get("build")),
            recipes=StrategyRecipes.from_payload(payload.get("recipes")),
            liquidity=StrategyLiquidityRules.from_payload(payload.get("liquidity")),
            risk=StrategyRiskDefaults.from_payload(payload.get("risk")),
            enabled=bool(payload.get("enabled", True)),
            config_path=path,
            config_hash="",
        )
        strategy_config = StrategyConfig(
            **{
                **strategy_config.__dict__,
                "config_hash": _canonical_hash(
                    _strategy_config_payload(strategy_config)
                ),
            }
        )
        if strategy_config.strategy_family == "unknown":
            raise ValueError(f"Unsupported strategy family in {path}")
        if strategy_config.strategy_config_id in configs:
            raise ValueError(
                f"Duplicate strategy_config_id {strategy_config.strategy_config_id}"
            )
        configs[strategy_config.strategy_config_id] = strategy_config
    return configs


__all__ = [
    "DEFAULT_OPTIONS_AUTOMATION_CONFIG_ROOT",
    "StrategyConfig",
    "_as_list",
    "_as_text",
    "_canonical_hash",
    "_load_yaml_file",
    "default_config_root",
    "load_strategy_configs",
]
