from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.services.config_inheritance import load_yaml_mapping as _load_yaml_mapping
from core.services.payload_validation import (
    normalize_mapping,
    normalize_optional_text,
    normalize_required_text,
    validate_payload_model,
)
from core.services.ticker_sources import VALID_TICKER_SOURCE_RECIPES
from core.services.trading_strategies import (
    default_config_root,
    load_active_trading_strategies,
)

VALID_SCHEDULE_TYPES = {
    "interval_minutes",
    "market_open_plus_minutes",
    "market_close_plus_minutes",
    "manual",
}
ScheduleType = Literal["interval_minutes", "market_open_plus_minutes", "market_close_plus_minutes", "manual"]


def excluded_declared_job_types() -> set[str]:
    raw = os.environ.get("SPREADS_EXCLUDED_JOB_TYPES", "")
    return {part.strip() for part in raw.split(",") if part is not None and str(part).strip()}


def _canonical_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


class ScheduleYamlPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    schedule_type: ScheduleType = Field(alias="type")

    @field_validator("schedule_type", mode="before")
    @classmethod
    def _normalize_schedule_type(cls, value: Any) -> str:
        schedule_type = normalize_required_text(value).lower()
        if schedule_type not in VALID_SCHEDULE_TYPES:
            raise ValueError(f"must be one of: {', '.join(sorted(VALID_SCHEDULE_TYPES))}")
        return schedule_type

    def schedule_args(self) -> dict[str, Any]:
        payload = self.model_dump(by_alias=True)
        payload.pop("type", None)
        return payload


class DeclaredJobYamlPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_key: str
    job_type: str
    enabled: bool = True
    schedule: ScheduleYamlPayload
    payload: dict[str, Any] = Field(default_factory=dict)
    market_calendar: str = "NYSE"
    singleton_scope: str | None = None

    @field_validator("job_key", "job_type", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: Any) -> str:
        return normalize_required_text(value)

    @field_validator("payload", mode="before")
    @classmethod
    def _normalize_payload(cls, value: Any) -> dict[str, Any]:
        return normalize_mapping(value)

    @field_validator("market_calendar", mode="before")
    @classmethod
    def _normalize_market_calendar(cls, value: Any) -> str:
        return normalize_required_text(value or "NYSE")

    @field_validator("singleton_scope", mode="before")
    @classmethod
    def _normalize_singleton_scope(cls, value: Any) -> str | None:
        return normalize_optional_text(value)


class TickerSourceYamlPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker_source_id: str
    job_key: str
    enabled: bool = True
    schedule: ScheduleYamlPayload
    market_calendar: str = "NYSE"
    allow_off_hours: bool = False
    recipe: str
    recipe_args: dict[str, Any] = Field(default_factory=dict)
    singleton_scope: str | None = None

    @field_validator("ticker_source_id", "job_key", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: Any) -> str:
        return normalize_required_text(value)

    @field_validator("recipe", mode="before")
    @classmethod
    def _normalize_recipe(cls, value: Any) -> str:
        return normalize_required_text(value).lower()

    @field_validator("recipe")
    @classmethod
    def _validate_recipe(cls, value: str) -> str:
        if value not in VALID_TICKER_SOURCE_RECIPES:
            raise ValueError(f"must be one of: {', '.join(sorted(VALID_TICKER_SOURCE_RECIPES))}")
        return value

    @field_validator("recipe_args", mode="before")
    @classmethod
    def _normalize_recipe_args(cls, value: Any) -> dict[str, Any]:
        return normalize_mapping(value)

    @field_validator("market_calendar", mode="before")
    @classmethod
    def _normalize_market_calendar(cls, value: Any) -> str:
        return normalize_required_text(value or "NYSE")

    @field_validator("singleton_scope", mode="before")
    @classmethod
    def _normalize_singleton_scope(cls, value: Any) -> str | None:
        return normalize_optional_text(value)


def _schedule_payload(payload: ScheduleYamlPayload) -> tuple[str, dict[str, Any]]:
    return payload.schedule_type, payload.schedule_args()


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
class TickerSourceConfig:
    ticker_source_id: str
    job_key: str
    recipe: str
    enabled: bool
    schedule_type: str
    schedule: dict[str, Any]
    market_calendar: str
    allow_off_hours: bool
    recipe_args: dict[str, Any]
    singleton_scope: str | None
    config_path: Path
    config_hash: str


@dataclass(frozen=True)
class TickerSourceSpec:
    config: TickerSourceConfig

    @property
    def job_key(self) -> str:
        return self.config.job_key

    @property
    def job_type(self) -> str:
        return "ticker_source"

    def payload(self) -> dict[str, Any]:
        return {
            "source_id": self.config.ticker_source_id,
            "recipe": self.config.recipe,
            "recipe_args": dict(self.config.recipe_args),
            "allow_off_hours": self.config.allow_off_hours,
            "declared_config_hash": self.config.config_hash,
        }

    def as_job_spec(self) -> DeclaredJobSpec:
        return DeclaredJobSpec(
            job_key=self.config.job_key,
            job_type="ticker_source",
            enabled=self.config.enabled,
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
        raw = validate_payload_model(DeclaredJobYamlPayload, _load_yaml_mapping(path), path=path, label="declared job")
        schedule_type, schedule = _schedule_payload(raw.schedule)
        payload = dict(raw.payload)
        spec = DeclaredJobSpec(
            job_key=raw.job_key,
            job_type=raw.job_type,
            enabled=raw.enabled,
            schedule_type=schedule_type,
            schedule=schedule,
            payload=payload,
            market_calendar=raw.market_calendar,
            singleton_scope=raw.singleton_scope,
            config_path=path,
            config_hash=_canonical_hash(
                {
                    "job_key": raw.job_key,
                    "job_type": raw.job_type,
                    "enabled": raw.enabled,
                    "schedule_type": schedule_type,
                    "schedule": schedule,
                    "payload": payload,
                    "market_calendar": raw.market_calendar,
                    "singleton_scope": raw.singleton_scope,
                }
            ),
        )
        specs.append(spec)
    return specs


def _load_ticker_source_configs(
    config_root: str | Path | None = None,
) -> list[TickerSourceConfig]:
    config_root_path = default_config_root(config_root)
    root = config_root_path / "ticker_sources"
    if not root.exists():
        return []
    configs: list[TickerSourceConfig] = []
    for path in sorted(root.glob("*.yaml")):
        raw = validate_payload_model(TickerSourceYamlPayload, _load_yaml_mapping(path), path=path, label="ticker source")
        schedule_type, schedule = _schedule_payload(raw.schedule)
        recipe_args = dict(raw.recipe_args)
        configs.append(
            TickerSourceConfig(
                ticker_source_id=raw.ticker_source_id,
                job_key=raw.job_key,
                recipe=raw.recipe,
                enabled=raw.enabled,
                schedule_type=schedule_type,
                schedule=schedule,
                market_calendar=raw.market_calendar,
                allow_off_hours=raw.allow_off_hours,
                recipe_args=recipe_args,
                singleton_scope=raw.singleton_scope,
                config_path=path,
                config_hash=_canonical_hash(
                    {
                        "ticker_source_id": raw.ticker_source_id,
                        "job_key": raw.job_key,
                        "recipe": raw.recipe,
                        "enabled": raw.enabled,
                        "schedule_type": schedule_type,
                        "schedule": schedule,
                        "market_calendar": raw.market_calendar,
                        "allow_off_hours": raw.allow_off_hours,
                        "recipe_args": recipe_args,
                        "singleton_scope": raw.singleton_scope,
                    }
                ),
            )
        )
    return configs


def load_declared_ticker_source_specs(
    config_root: str | Path | None = None,
) -> list[TickerSourceSpec]:
    return [TickerSourceSpec(config=config) for config in _load_ticker_source_configs(config_root)]


def get_declared_ticker_source_spec(
    source_id: str,
    *,
    config_root: str | Path | None = None,
) -> TickerSourceSpec | None:
    normalized = str(source_id or "").strip()
    if not normalized:
        return None
    return next(
        (spec for spec in load_declared_ticker_source_specs(config_root) if spec.config.ticker_source_id == normalized),
        None,
    )


def _trading_strategy_job_specs(
    config_root: str | Path | None = None,
) -> list[DeclaredJobSpec]:
    specs: list[DeclaredJobSpec] = []
    for strategy in load_active_trading_strategies(config_root).values():
        for routine_name, routine in (
            ("entry", strategy.entry),
            ("manage", strategy.management),
        ):
            if routine is None or not routine.enabled:
                continue
            job_key = f"trading_strategy:{strategy.trading_strategy_id}:{routine_name}"
            specs.append(
                DeclaredJobSpec(
                    job_key=job_key,
                    job_type=f"trading_strategy_{routine_name}",
                    enabled=True,
                    schedule_type="interval_minutes",
                    schedule={"minutes": max(int(routine.schedule.cadence_minutes), 1)},
                    payload={
                        "trading_strategy_id": strategy.trading_strategy_id,
                        "routine": routine_name,
                        "allow_off_hours": not bool(routine.schedule.market_hours_only),
                        "declared_config_hash": strategy.config_hash,
                    },
                    market_calendar="NYSE",
                    singleton_scope=f"{strategy.trading_strategy_id}:{routine_name}",
                    config_path=strategy.config_path,
                    config_hash=strategy.config_hash,
                )
            )
    return specs


def load_declared_job_specs(
    config_root: str | Path | None = None,
) -> list[DeclaredJobSpec]:
    # The declared job surface is assembled from static job YAML plus
    # config-compiled ticker-source and trading-strategy definitions.
    specs = list(_load_job_specs(config_root))
    specs.extend(spec.as_job_spec() for spec in load_declared_ticker_source_specs(config_root))
    specs.extend(_trading_strategy_job_specs(config_root))
    excluded_job_types = excluded_declared_job_types()
    if excluded_job_types:
        specs = [spec for spec in specs if str(spec.job_type or "").strip() not in excluded_job_types]
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
        (row for row in list_declared_job_rows(config_root=config_root) if str(row.get("job_key") or "") == normalized),
        None,
    )


__all__ = [
    "DeclaredJobSpec",
    "TickerSourceConfig",
    "TickerSourceSpec",
    "excluded_declared_job_types",
    "get_declared_job_row",
    "get_declared_ticker_source_spec",
    "list_declared_job_rows",
    "load_declared_job_specs",
    "load_declared_ticker_source_specs",
]
