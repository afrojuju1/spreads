from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from core.services.option_structures import normalize_strategy_family

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


@dataclass(frozen=True)
class StrategyConfig:
    strategy_config_id: str
    strategy_id: str
    builder_params: dict[str, Any]
    entry_recipe_refs: tuple[str, ...]
    management_recipe_refs: tuple[str, ...]
    liquidity_rules: dict[str, Any]
    risk_defaults: dict[str, Any]
    enabled: bool
    config_path: Path
    config_hash: str

    @property
    def strategy_family(self) -> str:
        return normalize_strategy_family(self.strategy_id)

    @property
    def scanner_strategy(self) -> str:
        family = self.strategy_family
        return {
            "put_credit_spread": "put_credit",
            "call_credit_spread": "call_credit",
            "put_debit_spread": "put_debit",
            "call_debit_spread": "call_debit",
        }.get(family, family)

    @property
    def scanner_profile(self) -> str:
        dte_min = int(self.builder_params.get("dte_min", 0) or 0)
        dte_max = int(self.builder_params.get("dte_max", 0) or 0)
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
        strategy_config = StrategyConfig(
            strategy_config_id=_as_text(
                payload.get("strategy_config_id"), field_name="strategy_config_id"
            ),
            strategy_id=_as_text(payload.get("strategy_id"), field_name="strategy_id"),
            builder_params=dict(payload.get("builder_params") or {}),
            entry_recipe_refs=_as_list(
                payload.get("entry_recipe_refs"), field_name="entry_recipe_refs"
            ),
            management_recipe_refs=_as_list(
                payload.get("management_recipe_refs"),
                field_name="management_recipe_refs",
            ),
            liquidity_rules=dict(payload.get("liquidity_rules") or {}),
            risk_defaults=dict(payload.get("risk_defaults") or {}),
            enabled=bool(payload.get("enabled", True)),
            config_path=path,
            config_hash=_canonical_hash(payload),
        )
        if strategy_config.strategy_family == "unknown":
            raise ValueError(
                f"Unsupported strategy_id in {path}: {strategy_config.strategy_id}"
            )
        configs[strategy_config.strategy_config_id] = strategy_config
    return configs


__all__ = [
    "DEFAULT_OPTIONS_AUTOMATION_CONFIG_ROOT",
    "StrategyConfig",
    "default_config_root",
    "load_strategy_configs",
]
