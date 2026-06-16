from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from whenever import Time

from core.domain.profiles import RankingPolicyConfig, RankingWeightsConfig
from core.services.payload_validation import normalize_mapping, normalize_optional_text, normalize_text_tuple


@dataclass(frozen=True)
class DteRange:
    minimum: int
    maximum: int

    def __post_init__(self) -> None:
        if self.minimum < 0 or self.maximum < self.minimum:
            raise ValueError("build.dte requires 0 <= min <= max")


@dataclass(frozen=True)
class DeltaRange:
    minimum: float
    maximum: float
    target: float | None = None

    def __post_init__(self) -> None:
        if self.minimum < 0 or self.maximum > 1 or self.minimum > self.maximum:
            raise ValueError("build.short_delta requires 0 <= min <= max <= 1")
        if self.target is not None and not (self.minimum <= self.target <= self.maximum):
            raise ValueError("build.short_delta.target must fall within the band")


@dataclass(frozen=True)
class ExpectedMoveGuard:
    min_short_vs_expected_move_ratio: float | None = None
    min_breakeven_vs_expected_move_ratio: float | None = None

    def __post_init__(self) -> None:
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
            if value is None:
                continue
            if value < -1 or value > 1:
                raise ValueError(f"{field_name} must be between -1 and 1")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> ExpectedMoveGuard:
        mapping = normalize_mapping(payload, field_name="build.expected_move")
        return cls(
            min_short_vs_expected_move_ratio=(
                float(mapping["min_short_vs_expected_move_ratio"])
                if mapping.get("min_short_vs_expected_move_ratio") not in (None, "")
                else None
            ),
            min_breakeven_vs_expected_move_ratio=(
                float(mapping["min_breakeven_vs_expected_move_ratio"])
                if mapping.get("min_breakeven_vs_expected_move_ratio") not in (None, "")
                else None
            ),
        )

    def as_builder_params(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.min_short_vs_expected_move_ratio is not None:
            payload["min_short_vs_expected_move_ratio"] = self.min_short_vs_expected_move_ratio
        if self.min_breakeven_vs_expected_move_ratio is not None:
            payload["min_breakeven_vs_expected_move_ratio"] = self.min_breakeven_vs_expected_move_ratio
        return payload


def _ranking_weights_from_payload(
    payload: Mapping[str, Any] | None,
) -> RankingWeightsConfig:
    mapping = normalize_mapping(payload, field_name="build.ranking.weights")
    return RankingWeightsConfig(
        probability_of_profit=float(mapping["probability_of_profit"]) if mapping.get("probability_of_profit") not in (None, "") else None,
        expected_value_dollars=float(mapping["expected_value_dollars"]) if mapping.get("expected_value_dollars") not in (None, "") else None,
        slippage_adjusted_expected_value_dollars=(
            float(mapping["slippage_adjusted_expected_value_dollars"])
            if mapping.get("slippage_adjusted_expected_value_dollars") not in (None, "")
            else None
        ),
        entry_slippage_dollars=float(mapping["entry_slippage_dollars"]) if mapping.get("entry_slippage_dollars") not in (None, "") else None,
        model_implied_volatility=(
            float(mapping["model_implied_volatility"]) if mapping.get("model_implied_volatility") not in (None, "") else None
        ),
    )


def _ranking_policy_from_payload(
    payload: Mapping[str, Any] | None,
) -> RankingPolicyConfig:
    mapping = normalize_mapping(payload, field_name="build.ranking")
    return RankingPolicyConfig(
        min_probability_of_profit=(
            float(mapping["min_probability_of_profit"]) if mapping.get("min_probability_of_profit") not in (None, "") else None
        ),
        min_expected_value_dollars=(
            float(mapping["min_expected_value_dollars"]) if mapping.get("min_expected_value_dollars") not in (None, "") else None
        ),
        min_slippage_adjusted_expected_value_dollars=(
            float(mapping["min_slippage_adjusted_expected_value_dollars"])
            if mapping.get("min_slippage_adjusted_expected_value_dollars") not in (None, "")
            else None
        ),
        max_entry_slippage_dollars=(
            float(mapping["max_entry_slippage_dollars"]) if mapping.get("max_entry_slippage_dollars") not in (None, "") else None
        ),
        min_model_implied_volatility=(
            float(mapping["min_model_implied_volatility"]) if mapping.get("min_model_implied_volatility") not in (None, "") else None
        ),
        max_model_implied_volatility=(
            float(mapping["max_model_implied_volatility"]) if mapping.get("max_model_implied_volatility") not in (None, "") else None
        ),
        weights=_ranking_weights_from_payload(mapping.get("weights")),
    )


@dataclass(frozen=True)
class VerticalSpreadBuildConfig:
    dte: DteRange
    short_delta: DeltaRange
    widths: tuple[float, ...]
    min_fill_ratio: float | None = None
    expected_move: ExpectedMoveGuard = field(default_factory=ExpectedMoveGuard)
    ranking: RankingPolicyConfig = field(default_factory=RankingPolicyConfig)

    def __post_init__(self) -> None:
        if self.min_fill_ratio is not None and (self.min_fill_ratio <= 0 or self.min_fill_ratio > 1.25):
            raise ValueError("build.min_fill_ratio must be in (0, 1.25]")
        if not self.widths:
            raise ValueError("build.widths must not be empty")
        if any(width <= 0 for width in self.widths):
            raise ValueError("build.widths values must be positive")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> VerticalSpreadBuildConfig:
        mapping = normalize_mapping(payload, field_name="build")
        dte_payload = normalize_mapping(mapping.get("dte"), field_name="build.dte")
        short_delta_payload = normalize_mapping(
            mapping.get("short_delta"),
            field_name="build.short_delta",
        )
        raw_widths = mapping.get("widths")
        if raw_widths is None or not isinstance(raw_widths, list):
            raise ValueError("build.widths must be a list")
        widths = tuple(sorted({round(float(item), 4) for item in raw_widths if item not in (None, "")}))
        return cls(
            dte=DteRange(
                minimum=int(dte_payload.get("min") or 0),
                maximum=int(dte_payload.get("max") or 0),
            ),
            short_delta=DeltaRange(
                minimum=float(short_delta_payload.get("min") or 0.0),
                maximum=float(short_delta_payload.get("max") or 0.0),
                target=float(short_delta_payload["target"]) if short_delta_payload.get("target") not in (None, "") else None,
            ),
            widths=widths,
            min_fill_ratio=float(mapping["min_fill_ratio"]) if mapping.get("min_fill_ratio") not in (None, "") else None,
            expected_move=ExpectedMoveGuard.from_payload(mapping.get("expected_move")),
            ranking=_ranking_policy_from_payload(mapping.get("ranking")),
        )

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


@dataclass(frozen=True)
class IronCondorBuildConfig(VerticalSpreadBuildConfig):
    symmetric_wings_only: bool = False

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> IronCondorBuildConfig:
        base = VerticalSpreadBuildConfig.from_payload(payload)
        mapping = normalize_mapping(payload, field_name="build")
        return cls(
            dte=base.dte,
            short_delta=base.short_delta,
            widths=base.widths,
            min_fill_ratio=base.min_fill_ratio,
            expected_move=base.expected_move,
            ranking=base.ranking,
            symmetric_wings_only=bool(mapping.get("symmetric_wings_only", False)),
        )

    def as_builder_params(self) -> dict[str, Any]:
        payload = super().as_builder_params()
        payload["symmetric_wings_only"] = self.symmetric_wings_only
        return payload


@dataclass(frozen=True)
class LongVolBuildConfig:
    dte: DteRange
    entry_delta: DeltaRange
    min_fill_ratio: float | None = None
    expected_move: ExpectedMoveGuard = field(default_factory=ExpectedMoveGuard)
    ranking: RankingPolicyConfig = field(default_factory=RankingPolicyConfig)

    def __post_init__(self) -> None:
        if self.min_fill_ratio is not None and (self.min_fill_ratio <= 0 or self.min_fill_ratio > 1.25):
            raise ValueError("build.min_fill_ratio must be in (0, 1.25]")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> LongVolBuildConfig:
        mapping = normalize_mapping(payload, field_name="build")
        dte_payload = normalize_mapping(mapping.get("dte"), field_name="build.dte")
        delta_payload = normalize_mapping(
            mapping.get("entry_delta") or mapping.get("short_delta"),
            field_name="build.entry_delta",
        )
        return cls(
            dte=DteRange(
                minimum=int(dte_payload.get("min") or 0),
                maximum=int(dte_payload.get("max") or 0),
            ),
            entry_delta=DeltaRange(
                minimum=float(delta_payload.get("min") or 0.0),
                maximum=float(delta_payload.get("max") or 0.0),
                target=float(delta_payload["target"]) if delta_payload.get("target") not in (None, "") else None,
            ),
            min_fill_ratio=float(mapping["min_fill_ratio"]) if mapping.get("min_fill_ratio") not in (None, "") else None,
            expected_move=ExpectedMoveGuard.from_payload(mapping.get("expected_move")),
            ranking=_ranking_policy_from_payload(mapping.get("ranking")),
        )

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


@dataclass(frozen=True)
class StrategyRecipes:
    entry: tuple[str, ...]
    management: tuple[str, ...]

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyRecipes:
        mapping = normalize_mapping(payload, field_name="recipes")
        return cls(
            entry=normalize_text_tuple(mapping.get("entry"), field_name="recipes.entry"),
            management=normalize_text_tuple(
                mapping.get("management"),
                field_name="recipes.management",
            ),
        )


@dataclass(frozen=True)
class StrategyLiquidityRules:
    min_open_interest: int | None = None
    max_leg_spread_pct_mid: float | None = None
    max_quote_age_seconds: int | None = None

    def __post_init__(self) -> None:
        if self.min_open_interest is not None and self.min_open_interest < 0:
            raise ValueError("liquidity.min_open_interest must be >= 0")
        if self.max_leg_spread_pct_mid is not None and self.max_leg_spread_pct_mid <= 0:
            raise ValueError("liquidity.max_leg_spread_pct_mid must be > 0")
        if self.max_quote_age_seconds is not None and self.max_quote_age_seconds <= 0:
            raise ValueError("liquidity.max_quote_age_seconds must be > 0")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyLiquidityRules:
        mapping = normalize_mapping(payload, field_name="liquidity")
        return cls(
            min_open_interest=int(mapping["min_open_interest"]) if mapping.get("min_open_interest") not in (None, "") else None,
            max_leg_spread_pct_mid=(
                float(mapping["max_leg_spread_pct_mid"]) if mapping.get("max_leg_spread_pct_mid") not in (None, "") else None
            ),
            max_quote_age_seconds=int(mapping["max_quote_age_seconds"]) if mapping.get("max_quote_age_seconds") not in (None, "") else None,
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.min_open_interest is not None:
            payload["min_open_interest"] = self.min_open_interest
        if self.max_leg_spread_pct_mid is not None:
            payload["max_leg_spread_pct_mid"] = self.max_leg_spread_pct_mid
        if self.max_quote_age_seconds is not None:
            payload["max_quote_age_seconds"] = self.max_quote_age_seconds
        return payload


@dataclass(frozen=True)
class RoutineScheduleWindow:
    start_et: str | None = None
    end_et: str | None = None

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> RoutineScheduleWindow:
        mapping = normalize_mapping(payload, field_name="routine.schedule.window")
        start_et = normalize_optional_text(mapping.get("start_et"))
        end_et = normalize_optional_text(mapping.get("end_et"))
        if start_et is not None:
            try:
                start_time = Time.parse_iso(start_et)
            except ValueError as exc:
                raise ValueError("routine.schedule.window.start_et must be HH:MM") from exc
            if start_time.second or start_time.nanosecond:
                raise ValueError("routine.schedule.window.start_et must be HH:MM")
            start_et = f"{start_time.hour:02d}:{start_time.minute:02d}"
        if end_et is not None:
            try:
                end_time = Time.parse_iso(end_et)
            except ValueError as exc:
                raise ValueError("routine.schedule.window.end_et must be HH:MM") from exc
            if end_time.second or end_time.nanosecond:
                raise ValueError("routine.schedule.window.end_et must be HH:MM")
            end_et = f"{end_time.hour:02d}:{end_time.minute:02d}"
        return cls(
            start_et=start_et,
            end_et=end_et,
        )


@dataclass(frozen=True)
class RoutineSchedule:
    cadence_minutes: int
    market_hours_only: bool = False
    offset_seconds: int = 0
    window: RoutineScheduleWindow = field(default_factory=RoutineScheduleWindow)

    def __post_init__(self) -> None:
        if self.cadence_minutes <= 0:
            raise ValueError("routine.schedule.cadence_minutes must be > 0")
        if self.offset_seconds < 0:
            raise ValueError("routine.schedule.offset_seconds must be >= 0")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> RoutineSchedule:
        mapping = normalize_mapping(payload, field_name="schedule")
        cadence_minutes = int(mapping["cadence_minutes"]) if mapping.get("cadence_minutes") not in (None, "") else None
        if cadence_minutes is None:
            raise ValueError("routine.schedule.cadence_minutes is required")
        return cls(
            cadence_minutes=cadence_minutes,
            market_hours_only=bool(mapping.get("market_hours_only", False)),
            offset_seconds=int(mapping["offset_seconds"]) if mapping.get("offset_seconds") not in (None, "") else 0,
            window=RoutineScheduleWindow.from_payload(mapping.get("window")),
        )

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


@dataclass(frozen=True)
class EntrySelectionPolicy:
    min_signal_score: float | None = None

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> EntrySelectionPolicy:
        mapping = normalize_mapping(payload, field_name="triggers")
        return cls(
            min_signal_score=float(mapping["min_signal_score"]) if mapping.get("min_signal_score") not in (None, "") else None,
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.min_signal_score is not None:
            payload["min_signal_score"] = self.min_signal_score
        return payload


@dataclass(frozen=True)
class StrategyEntryQualityPolicy:
    profile_id: str | None = None
    overrides: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(
        cls,
        *,
        quality_profile: Any = None,
        quality_overrides: Mapping[str, Any] | None = None,
    ) -> StrategyEntryQualityPolicy:
        return cls(
            profile_id=normalize_optional_text(quality_profile),
            overrides=normalize_mapping(quality_overrides, field_name="entry.quality_overrides"),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.profile_id is not None:
            payload["quality_profile"] = self.profile_id
        if self.overrides:
            payload["quality_overrides"] = dict(self.overrides)
        return payload


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
