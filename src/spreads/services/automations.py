from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from spreads.services.strategy_configs import (
    StrategyConfig,
    _as_list,
    _as_text,
    _canonical_hash,
    _load_yaml_file,
    default_config_root,
    load_strategy_configs,
)

NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class AutomationConfig:
    automation_id: str
    strategy_config_id: str
    automation_type: str
    schedule: dict[str, Any]
    universe_ref: str | None
    trigger_policy: dict[str, Any]
    approval_mode: str
    execution_mode: str
    enabled: bool
    config_path: Path
    config_hash: str

    @property
    def is_entry(self) -> bool:
        return self.automation_type == "entry"

    @property
    def is_management(self) -> bool:
        return self.automation_type == "management"


@dataclass(frozen=True)
class ResolvedAutomation:
    automation: AutomationConfig
    strategy_config: StrategyConfig
    symbols: tuple[str, ...]


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour_text, _, minute_text = str(value).partition(":")
    if not _:
        raise ValueError(f"Invalid HH:MM time: {value}")
    return int(hour_text), int(minute_text)


def cadence_minutes(schedule: dict[str, Any]) -> int:
    cadence = str(schedule.get("cadence") or "").strip().lower()
    if cadence.endswith("m"):
        return max(int(cadence[:-1]), 1)
    raise ValueError(f"Unsupported automation cadence: {cadence}")


def automation_should_run_now(
    automation: AutomationConfig,
    *,
    now: datetime | None = None,
) -> bool:
    current = (now or datetime.now(NEW_YORK)).astimezone(NEW_YORK)
    if current.weekday() >= 5:
        return False
    schedule = automation.schedule
    if bool(schedule.get("market_hours_only", False)) and not (
        (9, 30) <= (current.hour, current.minute) <= (16, 0)
    ):
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


def load_universe_symbols(
    universe_ref: str | None,
    *,
    config_root: str | Path | None = None,
) -> tuple[str, ...]:
    if universe_ref is None:
        return ()
    path = default_config_root(config_root) / "universes" / f"{universe_ref}.yaml"
    if not path.exists():
        raise ValueError(f"Unknown universe_ref: {universe_ref}")
    payload = _load_yaml_file(path)
    symbols = _as_list(payload.get("symbols"), field_name=f"{universe_ref}.symbols")
    return tuple(str(symbol).upper() for symbol in symbols)


def load_automations(
    config_root: str | Path | None = None,
) -> dict[str, AutomationConfig]:
    root = default_config_root(config_root) / "automations"
    if not root.exists():
        return {}
    automations: dict[str, AutomationConfig] = {}
    for path in sorted(root.glob("*.yaml")):
        payload = _load_yaml_file(path)
        automation = AutomationConfig(
            automation_id=_as_text(
                payload.get("automation_id"), field_name="automation_id"
            ),
            strategy_config_id=_as_text(
                payload.get("strategy_config_id"), field_name="strategy_config_id"
            ),
            automation_type=_as_text(
                payload.get("automation_type"), field_name="automation_type"
            ).lower(),
            schedule=dict(payload.get("schedule") or {}),
            universe_ref=(
                None
                if payload.get("universe_ref") in (None, "")
                else str(payload.get("universe_ref")).strip()
            ),
            trigger_policy=dict(payload.get("trigger_policy") or {}),
            approval_mode=_as_text(
                payload.get("approval_mode"), field_name="approval_mode"
            ).lower(),
            execution_mode=_as_text(
                payload.get("execution_mode"), field_name="execution_mode"
            ).lower(),
            enabled=bool(payload.get("enabled", True)),
            config_path=path,
            config_hash=_canonical_hash(payload),
        )
        if automation.automation_type not in {"entry", "management"}:
            raise ValueError(
                f"Unsupported automation_type in {path}: {automation.automation_type}"
            )
        automations[automation.automation_id] = automation
    return automations


def resolve_automation(
    automation_id: str,
    *,
    config_root: str | Path | None = None,
) -> ResolvedAutomation:
    strategies = load_strategy_configs(config_root)
    automations = load_automations(config_root)
    automation = automations.get(automation_id)
    if automation is None:
        raise ValueError(f"Unknown automation_id: {automation_id}")
    strategy_config = strategies.get(automation.strategy_config_id)
    if strategy_config is None:
        raise ValueError(
            f"Automation {automation_id} references unknown strategy_config_id "
            f"{automation.strategy_config_id}"
        )
    return ResolvedAutomation(
        automation=automation,
        strategy_config=strategy_config,
        symbols=load_universe_symbols(
            automation.universe_ref,
            config_root=config_root,
        ),
    )


__all__ = [
    "AutomationConfig",
    "ResolvedAutomation",
    "automation_should_run_now",
    "cadence_minutes",
    "load_automations",
    "load_universe_symbols",
    "resolve_automation",
]
