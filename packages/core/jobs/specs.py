from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from core.services.bots import build_discovery_run_scope, load_active_bots
from core.services.config_inheritance import (
    as_mapping as _as_mapping,
    as_required_text as _as_text,
    load_yaml_mapping as _load_yaml_mapping,
    resolve_policy_mapping as _resolve_policy_mapping,
)
from core.services.strategy_configs import default_config_root


VALID_SCHEDULE_TYPES = {
    "interval_minutes",
    "market_open_plus_minutes",
    "market_close_plus_minutes",
    "manual",
}


def _canonical_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _schedule_payload(value: Any, *, field_name: str) -> tuple[str, dict[str, Any]]:
    mapping = _as_mapping(value, field_name=field_name)
    schedule_type = _as_text(mapping.get("type"), field_name=f"{field_name}.type")
    if schedule_type not in VALID_SCHEDULE_TYPES:
        raise ValueError(
            f"Unsupported schedule type {schedule_type!r} in {field_name}"
        )
    return schedule_type, {key: mapping[key] for key in mapping if key != "type"}


@dataclass(frozen=True)
class DeclaredJobSpec:
    job_key: str
    job_type: str
    enabled: bool
    schedule_type: str
    schedule: dict[str, Any]
    payload: dict[str, Any]
    market_calendar: str
    singleton_scope: str | None
    config_path: Path | None
    config_hash: str

    def as_row(self) -> dict[str, Any]:
        return {
            "job_key": self.job_key,
            "job_type": self.job_type,
            "enabled": self.enabled,
            "schedule_type": self.schedule_type,
            "schedule": dict(self.schedule),
            "payload": dict(self.payload),
            "market_calendar": self.market_calendar,
            "singleton_scope": self.singleton_scope,
            "updated_at": None,
            "config_hash": self.config_hash,
            "config_path": None if self.config_path is None else str(self.config_path),
        }


@dataclass(frozen=True)
class DiscoveryRunConfig:
    discovery_run_id: str
    job_key: str
    label: str
    scanner_strategy: str
    scanner_profile: str
    enabled: bool
    schedule_type: str
    schedule: dict[str, Any]
    market_calendar: str
    greeks_source: str
    top: int
    per_symbol_top: int
    interval_seconds: int
    backfill_missed_slots: bool
    max_slot_retries: int
    quote_capture_seconds: int
    trade_capture_seconds: int
    allow_off_hours: bool
    session_start_offset_minutes: int
    session_end_offset_minutes: int
    execution_policy: dict[str, Any]
    risk_policy: dict[str, Any]
    exit_policy: dict[str, Any]
    scanner_args: dict[str, Any]
    singleton_scope: str | None
    config_path: Path
    config_hash: str


@dataclass(frozen=True)
class DiscoveryRunSpec:
    config: DiscoveryRunConfig
    scope: dict[str, Any]

    @property
    def enabled(self) -> bool:
        return self.config.enabled and bool(self.scope.get("enabled"))

    @property
    def job_key(self) -> str:
        return self.config.job_key

    @property
    def job_type(self) -> str:
        return "discovery_run"

    @property
    def singleton_scope(self) -> str | None:
        return self.config.singleton_scope

    @property
    def market_calendar(self) -> str:
        return self.config.market_calendar

    @property
    def options_automation_scope(self) -> dict[str, Any]:
        return dict(self.scope)

    def payload(self) -> dict[str, Any]:
        symbols = [str(symbol).upper() for symbol in list(self.scope.get("symbols") or [])]
        universe_ref = str(self.scope.get("universe_ref") or "").strip()
        payload = {
            "job_key": self.config.job_key,
            "label": self.config.label,
            "symbols": ",".join(symbols),
            "strategy": self.config.scanner_strategy,
            "profile": self.config.scanner_profile,
            "greeks_source": self.config.greeks_source,
            "top": self.config.top,
            "per_symbol_top": self.config.per_symbol_top,
            "interval_seconds": self.config.interval_seconds,
            "backfill_missed_slots": self.config.backfill_missed_slots,
            "max_slot_retries": self.config.max_slot_retries,
            "quote_capture_seconds": self.config.quote_capture_seconds,
            "trade_capture_seconds": self.config.trade_capture_seconds,
            "allow_off_hours": self.config.allow_off_hours,
            "session_start_offset_minutes": self.config.session_start_offset_minutes,
            "session_end_offset_minutes": self.config.session_end_offset_minutes,
            "options_automation_enabled": True,
            "execution_policy": dict(self.config.execution_policy),
            "risk_policy": dict(self.config.risk_policy),
            "exit_policy": dict(self.config.exit_policy),
            "declared_config_hash": self.config.config_hash,
            **dict(self.scope.get("scanner_args") or {}),
            **dict(self.config.scanner_args),
        }
        if universe_ref:
            payload["universe"] = universe_ref
        elif not symbols:
            payload["universe"] = "0dte_core"
        return payload

    def as_job_spec(self) -> DeclaredJobSpec:
        return DeclaredJobSpec(
            job_key=self.config.job_key,
            job_type="discovery_run",
            enabled=self.enabled,
            schedule_type=self.config.schedule_type,
            schedule=dict(self.config.schedule),
            payload=self.payload(),
            market_calendar=self.config.market_calendar,
            singleton_scope=self.config.singleton_scope,
            config_path=self.config.config_path,
            config_hash=self.config.config_hash,
        )


def _load_job_specs(config_root: str | Path | None = None) -> list[DeclaredJobSpec]:
    root = default_config_root(config_root) / "jobs"
    if not root.exists():
        return []
    specs: list[DeclaredJobSpec] = []
    for path in sorted(root.glob("*.yaml")):
        raw = _load_yaml_mapping(path)
        schedule_type, schedule = _schedule_payload(raw.get("schedule"), field_name="schedule")
        payload = dict(raw.get("payload") or {})
        spec = DeclaredJobSpec(
            job_key=_as_text(raw.get("job_key"), field_name="job_key"),
            job_type=_as_text(raw.get("job_type"), field_name="job_type"),
            enabled=bool(raw.get("enabled", True)),
            schedule_type=schedule_type,
            schedule=schedule,
            payload=payload,
            market_calendar=str(raw.get("market_calendar") or "NYSE"),
            singleton_scope=(
                None
                if raw.get("singleton_scope") in (None, "")
                else str(raw.get("singleton_scope")).strip()
            ),
            config_path=path,
            config_hash=_canonical_hash(
                {
                    "job_key": raw.get("job_key"),
                    "job_type": raw.get("job_type"),
                    "enabled": bool(raw.get("enabled", True)),
                    "schedule_type": schedule_type,
                    "schedule": schedule,
                    "payload": payload,
                    "market_calendar": str(raw.get("market_calendar") or "NYSE"),
                    "singleton_scope": raw.get("singleton_scope"),
                }
            ),
        )
        specs.append(spec)
    return specs


def _load_discovery_run_configs(
    config_root: str | Path | None = None,
) -> list[DiscoveryRunConfig]:
    config_root_path = default_config_root(config_root)
    root = config_root_path / "discovery_runs"
    if not root.exists():
        return []
    configs: list[DiscoveryRunConfig] = []
    for path in sorted(root.glob("*.yaml")):
        raw = _resolve_policy_mapping(
            _load_yaml_mapping(path),
            field_name="discovery_run",
            policy_kind="discovery_run",
            config_root=config_root_path,
            config_path=path,
        )
        schedule_type, schedule = _schedule_payload(raw.get("schedule"), field_name="schedule")
        execution_policy = _resolve_policy_mapping(
            raw.get("execution_policy"),
            field_name="execution_policy",
            policy_kind="execution",
            config_root=config_root_path,
            config_path=path,
        )
        risk_policy = _resolve_policy_mapping(
            raw.get("risk_policy"),
            field_name="risk_policy",
            policy_kind="risk",
            config_root=config_root_path,
            config_path=path,
        )
        exit_policy = _resolve_policy_mapping(
            raw.get("exit_policy"),
            field_name="exit_policy",
            policy_kind="exit",
            config_root=config_root_path,
            config_path=path,
        )
        scanner_args = (
            {}
            if raw.get("scanner_args") is None
            else _as_mapping(raw.get("scanner_args"), field_name="scanner_args")
        )
        config = DiscoveryRunConfig(
            discovery_run_id=_as_text(raw.get("discovery_run_id"), field_name="discovery_run_id"),
            job_key=_as_text(raw.get("job_key"), field_name="job_key"),
            label=_as_text(raw.get("label"), field_name="label"),
            scanner_strategy=_as_text(
                raw.get("scanner_strategy"), field_name="scanner_strategy"
            ),
            scanner_profile=_as_text(
                raw.get("scanner_profile"), field_name="scanner_profile"
            ),
            enabled=bool(raw.get("enabled", True)),
            schedule_type=schedule_type,
            schedule=schedule,
            market_calendar=str(raw.get("market_calendar") or "NYSE"),
            greeks_source=str(raw.get("greeks_source") or "auto"),
            top=max(int(raw.get("top", 10)), 1),
            per_symbol_top=max(int(raw.get("per_symbol_top", 1)), 1),
            interval_seconds=max(int(raw.get("interval_seconds", 300)), 1),
            backfill_missed_slots=bool(raw.get("backfill_missed_slots", False)),
            max_slot_retries=max(int(raw.get("max_slot_retries", 3)), 0),
            quote_capture_seconds=max(int(raw.get("quote_capture_seconds", 20)), 0),
            trade_capture_seconds=max(int(raw.get("trade_capture_seconds", 10)), 0),
            allow_off_hours=bool(raw.get("allow_off_hours", False)),
            session_start_offset_minutes=int(
                raw.get("session_start_offset_minutes", 0)
            ),
            session_end_offset_minutes=int(raw.get("session_end_offset_minutes", 0)),
            execution_policy=execution_policy,
            risk_policy=risk_policy,
            exit_policy=exit_policy,
            scanner_args=scanner_args,
            singleton_scope=(
                None
                if raw.get("singleton_scope") in (None, "")
                else str(raw.get("singleton_scope")).strip()
            ),
            config_path=path,
            config_hash=_canonical_hash(
                {
                    "discovery_run_id": raw.get("discovery_run_id"),
                    "job_key": raw.get("job_key"),
                    "label": raw.get("label"),
                    "scanner_strategy": raw.get("scanner_strategy"),
                    "scanner_profile": raw.get("scanner_profile"),
                    "enabled": bool(raw.get("enabled", True)),
                    "schedule_type": schedule_type,
                    "schedule": schedule,
                    "market_calendar": str(raw.get("market_calendar") or "NYSE"),
                    "greeks_source": str(raw.get("greeks_source") or "auto"),
                    "top": max(int(raw.get("top", 10)), 1),
                    "per_symbol_top": max(int(raw.get("per_symbol_top", 1)), 1),
                    "interval_seconds": max(int(raw.get("interval_seconds", 300)), 1),
                    "backfill_missed_slots": bool(
                        raw.get("backfill_missed_slots", False)
                    ),
                    "max_slot_retries": max(int(raw.get("max_slot_retries", 3)), 0),
                    "quote_capture_seconds": max(
                        int(raw.get("quote_capture_seconds", 20)), 0
                    ),
                    "trade_capture_seconds": max(
                        int(raw.get("trade_capture_seconds", 10)), 0
                    ),
                    "allow_off_hours": bool(raw.get("allow_off_hours", False)),
                    "session_start_offset_minutes": int(
                        raw.get("session_start_offset_minutes", 0)
                    ),
                    "session_end_offset_minutes": int(
                        raw.get("session_end_offset_minutes", 0)
                    ),
                    "execution_policy": execution_policy,
                    "risk_policy": risk_policy,
                    "exit_policy": exit_policy,
                    "scanner_args": scanner_args,
                    "singleton_scope": raw.get("singleton_scope"),
                }
            ),
        )
        configs.append(config)
    return configs


def load_declared_discovery_run_specs(
    config_root: str | Path | None = None,
) -> list[DiscoveryRunSpec]:
    specs: list[DiscoveryRunSpec] = []
    for config in _load_discovery_run_configs(config_root):
        scope = build_discovery_run_scope(
            config_root=config_root,
            scanner_strategy=config.scanner_strategy,
            scanner_profile=config.scanner_profile,
        )
        specs.append(DiscoveryRunSpec(config=config, scope=scope))
    return specs


def _automation_job_specs(
    config_root: str | Path | None = None,
) -> list[DeclaredJobSpec]:
    specs: list[DeclaredJobSpec] = []
    for bot in load_active_bots(config_root).values():
        for automation in bot.automations:
            cadence = max(
                int(automation.automation.schedule_config.cadence_minutes),
                1,
            )
            payload = {
                "bot_id": bot.bot.bot_id,
                "automation_id": automation.automation.automation_id,
                "allow_off_hours": not bool(
                    automation.automation.schedule.get("market_hours_only", False)
                ),
                "declared_config_hash": bot.config_hash,
            }
            if automation.automation.is_entry:
                job_key = (
                    f"options_automation_entry:{bot.bot.bot_id}:"
                    f"{automation.automation.automation_id}"
                )
                specs.append(
                    DeclaredJobSpec(
                        job_key=job_key,
                        job_type="options_automation_entry",
                        enabled=True,
                        schedule_type="interval_minutes",
                        schedule={"minutes": cadence},
                        payload=payload,
                        market_calendar="NYSE",
                        singleton_scope=f"{bot.bot.bot_id}:{automation.automation.automation_id}",
                        config_path=automation.automation.config_path,
                        config_hash=bot.config_hash,
                    )
                )
    return specs


def load_declared_job_specs(
    config_root: str | Path | None = None,
) -> list[DeclaredJobSpec]:
    # The declared job surface is assembled from static job YAML plus
    # config-compiled discovery and automation definitions.
    specs = list(_load_job_specs(config_root))
    specs.extend(spec.as_job_spec() for spec in load_declared_discovery_run_specs(config_root))
    specs.extend(_automation_job_specs(config_root))
    specs.sort(key=lambda item: item.job_key)
    return specs


def list_declared_job_rows(
    *,
    config_root: str | Path | None = None,
    enabled_only: bool | None = None,
    job_type: str | None = None,
) -> list[dict[str, Any]]:
    rows = [spec.as_row() for spec in load_declared_job_specs(config_root)]
    if enabled_only is True:
        rows = [row for row in rows if bool(row.get("enabled"))]
    elif enabled_only is False:
        rows = [row for row in rows if not bool(row.get("enabled"))]
    if job_type:
        rows = [row for row in rows if str(row.get("job_type") or "") == job_type]
    return sorted(rows, key=lambda row: str(row.get("job_key") or ""))


def get_declared_job_row(
    job_key: str,
    *,
    config_root: str | Path | None = None,
) -> dict[str, Any] | None:
    normalized = str(job_key or "").strip()
    if not normalized:
        return None
    return next(
        (
            row
            for row in list_declared_job_rows(config_root=config_root)
            if str(row.get("job_key") or "") == normalized
        ),
        None,
    )


def get_declared_discovery_run_spec(
    job_key: str,
    *,
    config_root: str | Path | None = None,
) -> DiscoveryRunSpec | None:
    normalized = str(job_key or "").strip()
    if not normalized:
        return None
    return next(
        (
            spec
            for spec in load_declared_discovery_run_specs(config_root)
            if spec.job_key == normalized
        ),
        None,
    )


__all__ = [
    "DiscoveryRunConfig",
    "DiscoveryRunSpec",
    "DeclaredJobSpec",
    "get_declared_discovery_run_spec",
    "get_declared_job_row",
    "list_declared_job_rows",
    "load_declared_discovery_run_specs",
    "load_declared_job_specs",
]
