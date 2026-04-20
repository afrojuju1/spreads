from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.services.options_automation_models import (
    AutomationExecution,
    AutomationSchedule,
    AutomationTriggers,
)
from core.services.strategy_configs import (
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
    strategy_config_ref: str
    kind: str
    schedule_config: AutomationSchedule
    universe: str | None
    triggers: AutomationTriggers
    execution: AutomationExecution
    enabled: bool
    config_path: Path
    config_hash: str

    @property
    def strategy_config_id(self) -> str:
        return self.strategy_config_ref

    @property
    def automation_type(self) -> str:
        return self.kind

    @property
    def schedule(self) -> dict[str, Any]:
        return self.schedule_config.as_dict()

    @property
    def universe_ref(self) -> str | None:
        return self.universe

    @property
    def trigger_policy(self) -> dict[str, Any]:
        return self.triggers.as_dict()

    @property
    def approval_mode(self) -> str:
        return self.execution.approval_mode

    @property
    def execution_mode(self) -> str:
        return self.execution.mode

    @property
    def is_entry(self) -> bool:
        return self.kind == "entry"

    @property
    def is_management(self) -> bool:
        return self.kind == "management"


@dataclass(frozen=True)
class ResolvedAutomation:
    automation: AutomationConfig
    strategy_config: StrategyConfig
    symbols: tuple[str, ...]


def _automation_payload(automation: AutomationConfig) -> dict[str, Any]:
    return {
        "automation_id": automation.automation_id,
        "strategy_config": automation.strategy_config_ref,
        "kind": automation.kind,
        "schedule": {
            "cadence_minutes": automation.schedule_config.cadence_minutes,
            "market_hours_only": automation.schedule_config.market_hours_only,
            "window": {
                "start_et": automation.schedule_config.window.start_et,
                "end_et": automation.schedule_config.window.end_et,
            },
        },
        "universe": automation.universe,
        "triggers": automation.triggers.as_dict(),
        "execution": {
            "approval_mode": automation.execution.approval_mode,
            "mode": automation.execution.mode,
        },
        "enabled": automation.enabled,
    }


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour_text, _, minute_text = str(value).partition(":")
    if not _:
        raise ValueError(f"Invalid HH:MM time: {value}")
    return int(hour_text), int(minute_text)


def cadence_minutes(schedule: dict[str, Any] | AutomationSchedule) -> int:
    if isinstance(schedule, AutomationSchedule):
        return max(int(schedule.cadence_minutes), 1)
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
                payload.get("automation_id"),
                field_name="automation_id",
            ),
            strategy_config_ref=_as_text(
                payload.get("strategy_config"),
                field_name="strategy_config",
            ),
            kind=_as_text(payload.get("kind"), field_name="kind").lower(),
            schedule_config=AutomationSchedule.from_payload(payload.get("schedule")),
            universe=(
                None
                if payload.get("universe") in (None, "")
                else str(payload.get("universe")).strip()
            ),
            triggers=AutomationTriggers.from_payload(payload.get("triggers")),
            execution=AutomationExecution.from_payload(payload.get("execution")),
            enabled=bool(payload.get("enabled", True)),
            config_path=path,
            config_hash="",
        )
        if automation.kind not in {"entry", "management"}:
            raise ValueError(f"Unsupported kind in {path}: {automation.kind}")
        automation = AutomationConfig(
            **{
                **automation.__dict__,
                "config_hash": _canonical_hash(_automation_payload(automation)),
            }
        )
        if automation.automation_id in automations:
            raise ValueError(f"Duplicate automation_id {automation.automation_id}")
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
    strategy_config = strategies.get(automation.strategy_config_ref)
    if strategy_config is None:
        raise ValueError(
            f"Automation {automation_id} references unknown strategy_config "
            f"{automation.strategy_config_ref}"
        )
    return ResolvedAutomation(
        automation=automation,
        strategy_config=strategy_config,
        symbols=load_universe_symbols(
            automation.universe,
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
