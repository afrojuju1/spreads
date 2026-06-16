from __future__ import annotations

from typing import Any, TypeAlias

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator
from whenever import Time

from core.domain.profiles import RankingPolicyConfig

RANKING_POLICY_CONFIG = TypeAdapter(RankingPolicyConfig)


class BuildConfigModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )


class DteRange(BuildConfigModel):
    minimum: int = Field(validation_alias=AliasChoices("minimum", "min"))
    maximum: int = Field(validation_alias=AliasChoices("maximum", "max"))

    @model_validator(mode="after")
    def _validate_range(self) -> DteRange:
        if self.minimum < 0 or self.maximum < self.minimum:
            raise ValueError("build.dte requires 0 <= min <= max")
        return self


class DeltaRange(BuildConfigModel):
    minimum: float = Field(validation_alias=AliasChoices("minimum", "min"))
    maximum: float = Field(validation_alias=AliasChoices("maximum", "max"))
    target: float | None = None

    @model_validator(mode="after")
    def _validate_range(self) -> DeltaRange:
        if self.minimum < 0 or self.maximum > 1 or self.minimum > self.maximum:
            raise ValueError("build.short_delta requires 0 <= min <= max <= 1")
        if self.target is not None and not (self.minimum <= self.target <= self.maximum):
            raise ValueError("build.short_delta.target must fall within the band")
        return self


class ExpectedMoveGuard(BuildConfigModel):
    min_short_vs_expected_move_ratio: float | None = None
    min_breakeven_vs_expected_move_ratio: float | None = None

    @model_validator(mode="after")
    def _validate_ranges(self) -> ExpectedMoveGuard:
        for field_name, value in (
            (
                "build.expected_move.min_short_vs_expected_move_ratio",
                self.min_short_vs_expected_move_ratio,
            ),
            (
                "build.expected_move.min_breakeven_vs_expected_move_ratio",
                self.min_breakeven_vs_expected_move_ratio,
            ),
        ):
            if value is not None and (value < -1 or value > 1):
                raise ValueError(f"{field_name} must be between -1 and 1")
        return self

    def as_builder_params(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True)


class VerticalSpreadBuildConfig(BuildConfigModel):
    dte: DteRange
    short_delta: DeltaRange
    widths: tuple[float, ...]
    min_fill_ratio: float | None = None
    expected_move: ExpectedMoveGuard = Field(default_factory=ExpectedMoveGuard)
    ranking: RankingPolicyConfig = Field(default_factory=RankingPolicyConfig)

    @field_validator("widths", mode="before")
    @classmethod
    def _normalize_widths(cls, value: Any) -> tuple[float, ...]:
        if not isinstance(value, list | tuple):
            raise ValueError("build.widths must be a list")
        return tuple(sorted({round(float(item), 4) for item in value if item not in (None, "")}))

    @field_validator("ranking", mode="before")
    @classmethod
    def _normalize_ranking(cls, value: Any) -> RankingPolicyConfig:
        return RANKING_POLICY_CONFIG.validate_python(value or {})

    @model_validator(mode="after")
    def _validate_ranges(self) -> VerticalSpreadBuildConfig:
        if self.min_fill_ratio is not None and (self.min_fill_ratio <= 0 or self.min_fill_ratio > 1.25):
            raise ValueError("build.min_fill_ratio must be in (0, 1.25]")
        if not self.widths:
            raise ValueError("build.widths must not be empty")
        if any(width <= 0 for width in self.widths):
            raise ValueError("build.widths values must be positive")
        return self

    def as_builder_params(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "dte_min": self.dte.minimum,
            "dte_max": self.dte.maximum,
            "short_delta_min": self.short_delta.minimum,
            "short_delta_max": self.short_delta.maximum,
            "width_points": list(self.widths),
        }
        if self.short_delta.target is not None:
            payload["short_delta_target"] = self.short_delta.target
        if self.min_fill_ratio is not None:
            payload["min_fill_ratio"] = self.min_fill_ratio
        payload.update(self.expected_move.as_builder_params())
        payload.update(self.ranking.as_builder_params())
        return payload


class IronCondorBuildConfig(VerticalSpreadBuildConfig):
    symmetric_wings_only: bool = False

    def as_builder_params(self) -> dict[str, Any]:
        payload = super().as_builder_params()
        payload["symmetric_wings_only"] = self.symmetric_wings_only
        return payload


class LongVolBuildConfig(BuildConfigModel):
    dte: DteRange
    entry_delta: DeltaRange = Field(validation_alias=AliasChoices("entry_delta", "short_delta"))
    min_fill_ratio: float | None = None
    expected_move: ExpectedMoveGuard = Field(default_factory=ExpectedMoveGuard)
    ranking: RankingPolicyConfig = Field(default_factory=RankingPolicyConfig)

    @field_validator("ranking", mode="before")
    @classmethod
    def _normalize_ranking(cls, value: Any) -> RankingPolicyConfig:
        return RANKING_POLICY_CONFIG.validate_python(value or {})

    @model_validator(mode="after")
    def _validate_range(self) -> LongVolBuildConfig:
        if self.min_fill_ratio is not None and (self.min_fill_ratio <= 0 or self.min_fill_ratio > 1.25):
            raise ValueError("build.min_fill_ratio must be in (0, 1.25]")
        return self

    def as_builder_params(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "dte_min": self.dte.minimum,
            "dte_max": self.dte.maximum,
            "short_delta_min": self.entry_delta.minimum,
            "short_delta_max": self.entry_delta.maximum,
        }
        if self.entry_delta.target is not None:
            payload["short_delta_target"] = self.entry_delta.target
        if self.min_fill_ratio is not None:
            payload["min_fill_ratio"] = self.min_fill_ratio
        payload.update(self.expected_move.as_builder_params())
        payload.update(self.ranking.as_builder_params())
        return payload


StrategyBuildConfig: TypeAlias = VerticalSpreadBuildConfig | IronCondorBuildConfig | LongVolBuildConfig


class StrategyRecipes(BuildConfigModel):
    entry: tuple[str, ...] = ()
    management: tuple[str, ...] = ()

    @field_validator("entry", "management", mode="before")
    @classmethod
    def _normalize_text_tuple(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if not isinstance(value, list | tuple):
            raise ValueError("must be a list")
        return tuple(str(item).strip() for item in value if str(item or "").strip())


class StrategyLiquidityRules(BuildConfigModel):
    min_open_interest: int | None = None
    max_leg_spread_pct_mid: float | None = None
    max_quote_age_seconds: int | None = None

    @model_validator(mode="after")
    def _validate_ranges(self) -> StrategyLiquidityRules:
        if self.min_open_interest is not None and self.min_open_interest < 0:
            raise ValueError("liquidity.min_open_interest must be >= 0")
        if self.max_leg_spread_pct_mid is not None and self.max_leg_spread_pct_mid <= 0:
            raise ValueError("liquidity.max_leg_spread_pct_mid must be > 0")
        if self.max_quote_age_seconds is not None and self.max_quote_age_seconds <= 0:
            raise ValueError("liquidity.max_quote_age_seconds must be > 0")
        return self


class RoutineScheduleWindow(BuildConfigModel):
    start_et: str | None = None
    end_et: str | None = None

    @field_validator("start_et", "end_et", mode="before")
    @classmethod
    def _normalize_hhmm(cls, value: Any) -> str | None:
        rendered = str(value or "").strip()
        if not rendered:
            return None
        try:
            parsed = Time.parse_iso(rendered)
        except ValueError as exc:
            raise ValueError("must be HH:MM") from exc
        if parsed.second or parsed.nanosecond:
            raise ValueError("must be HH:MM")
        return f"{parsed.hour:02d}:{parsed.minute:02d}"


class RoutineSchedule(BuildConfigModel):
    cadence_minutes: int
    market_hours_only: bool = False
    offset_seconds: int = 0
    window: RoutineScheduleWindow = Field(default_factory=RoutineScheduleWindow)

    @model_validator(mode="after")
    def _validate_ranges(self) -> RoutineSchedule:
        if self.cadence_minutes <= 0:
            raise ValueError("routine.schedule.cadence_minutes must be > 0")
        if self.offset_seconds < 0:
            raise ValueError("routine.schedule.offset_seconds must be >= 0")
        return self

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "cadence": f"{self.cadence_minutes}m",
            "market_hours_only": self.market_hours_only,
        }
        if self.offset_seconds:
            payload["offset_seconds"] = self.offset_seconds
        if self.window.start_et is not None:
            payload["start_time_et"] = self.window.start_et
        if self.window.end_et is not None:
            payload["end_time_et"] = self.window.end_et
        return payload


class EntrySelectionPolicy(BuildConfigModel):
    min_signal_score: float | None = None


class StrategyEntryQualityPolicy(BuildConfigModel):
    profile_id: str | None = Field(default=None, validation_alias=AliasChoices("profile_id", "quality_profile"), serialization_alias="quality_profile")
    overrides: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("overrides", "quality_overrides"),
        serialization_alias="quality_overrides",
    )

    @field_validator("profile_id", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        rendered = str(value or "").strip()
        return rendered or None


__all__ = [
    "DeltaRange",
    "DteRange",
    "EntrySelectionPolicy",
    "ExpectedMoveGuard",
    "IronCondorBuildConfig",
    "LongVolBuildConfig",
    "RoutineSchedule",
    "RoutineScheduleWindow",
    "StrategyBuildConfig",
    "StrategyEntryQualityPolicy",
    "StrategyLiquidityRules",
    "StrategyRecipes",
    "VerticalSpreadBuildConfig",
]
