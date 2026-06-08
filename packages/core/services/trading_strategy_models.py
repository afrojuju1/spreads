from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from core.domain.profiles import RankingPolicyConfig, RankingWeightsConfig


def _require_mapping(value: Any, *, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    rendered = str(value).strip()
    return rendered or None


def _required_text(value: Any, *, field_name: str) -> str:
    rendered = _optional_text(value)
    if rendered is None:
        raise ValueError(f"{field_name} is required")
    return rendered


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _tuple_of_floats(value: Any, *, field_name: str) -> tuple[float, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    rendered = sorted({round(float(item), 4) for item in value if item not in (None, "")})
    if not rendered:
        raise ValueError(f"{field_name} must not be empty")
    return tuple(rendered)


def _tuple_of_texts(value: Any, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    rendered = [str(item).strip() for item in value if str(item or "").strip()]
    return tuple(rendered)


def _validate_hhmm(value: Any, *, field_name: str) -> str | None:
    rendered = _optional_text(value)
    if rendered is None:
        return None
    hour_text, separator, minute_text = rendered.partition(":")
    if separator != ":":
        raise ValueError(f"{field_name} must be HH:MM")
    hour = int(hour_text)
    minute = int(minute_text)
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"{field_name} must be HH:MM")
    return f"{hour:02d}:{minute:02d}"


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
        mapping = _require_mapping(payload, field_name="build.expected_move")
        return cls(
            min_short_vs_expected_move_ratio=_optional_float(mapping.get("min_short_vs_expected_move_ratio")),
            min_breakeven_vs_expected_move_ratio=_optional_float(mapping.get("min_breakeven_vs_expected_move_ratio")),
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
    mapping = _require_mapping(payload, field_name="build.ranking.weights")
    return RankingWeightsConfig(
        probability_of_profit=_optional_float(mapping.get("probability_of_profit")),
        expected_value_dollars=_optional_float(mapping.get("expected_value_dollars")),
        slippage_adjusted_expected_value_dollars=_optional_float(mapping.get("slippage_adjusted_expected_value_dollars")),
        entry_slippage_dollars=_optional_float(mapping.get("entry_slippage_dollars")),
        model_implied_volatility=_optional_float(mapping.get("model_implied_volatility")),
    )


def _ranking_policy_from_payload(
    payload: Mapping[str, Any] | None,
) -> RankingPolicyConfig:
    mapping = _require_mapping(payload, field_name="build.ranking")
    return RankingPolicyConfig(
        min_probability_of_profit=_optional_float(mapping.get("min_probability_of_profit")),
        min_expected_value_dollars=_optional_float(mapping.get("min_expected_value_dollars")),
        min_slippage_adjusted_expected_value_dollars=_optional_float(mapping.get("min_slippage_adjusted_expected_value_dollars")),
        max_entry_slippage_dollars=_optional_float(mapping.get("max_entry_slippage_dollars")),
        min_model_implied_volatility=_optional_float(mapping.get("min_model_implied_volatility")),
        max_model_implied_volatility=_optional_float(mapping.get("max_model_implied_volatility")),
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
        mapping = _require_mapping(payload, field_name="build")
        dte_payload = _require_mapping(mapping.get("dte"), field_name="build.dte")
        short_delta_payload = _require_mapping(
            mapping.get("short_delta"),
            field_name="build.short_delta",
        )
        return cls(
            dte=DteRange(
                minimum=int(dte_payload.get("min") or 0),
                maximum=int(dte_payload.get("max") or 0),
            ),
            short_delta=DeltaRange(
                minimum=float(short_delta_payload.get("min") or 0.0),
                maximum=float(short_delta_payload.get("max") or 0.0),
                target=_optional_float(short_delta_payload.get("target")),
            ),
            widths=_tuple_of_floats(mapping.get("widths"), field_name="build.widths"),
            min_fill_ratio=_optional_float(mapping.get("min_fill_ratio")),
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
        mapping = _require_mapping(payload, field_name="build")
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
        mapping = _require_mapping(payload, field_name="build")
        dte_payload = _require_mapping(mapping.get("dte"), field_name="build.dte")
        delta_payload = _require_mapping(
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
                target=_optional_float(delta_payload.get("target")),
            ),
            min_fill_ratio=_optional_float(mapping.get("min_fill_ratio")),
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
        mapping = _require_mapping(payload, field_name="recipes")
        return cls(
            entry=_tuple_of_texts(mapping.get("entry"), field_name="recipes.entry"),
            management=_tuple_of_texts(
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
        mapping = _require_mapping(payload, field_name="liquidity")
        return cls(
            min_open_interest=_optional_int(mapping.get("min_open_interest")),
            max_leg_spread_pct_mid=_optional_float(mapping.get("max_leg_spread_pct_mid")),
            max_quote_age_seconds=_optional_int(mapping.get("max_quote_age_seconds")),
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
class StrategyRiskDefaults:
    min_return_on_risk: float | None = None
    position_size_pct_of_available_balance: float | None = None
    max_risk_per_trade: float | None = None
    max_credit_slippage_pct: float | None = None

    def __post_init__(self) -> None:
        if self.min_return_on_risk is not None and self.min_return_on_risk <= 0:
            raise ValueError("risk.min_return_on_risk must be > 0")
        if self.position_size_pct_of_available_balance is not None and (
            self.position_size_pct_of_available_balance <= 0 or self.position_size_pct_of_available_balance > 1
        ):
            raise ValueError("risk.position_size_pct_of_available_balance must be > 0 and <= 1")
        if self.max_risk_per_trade is not None and self.max_risk_per_trade <= 0:
            raise ValueError("risk.max_risk_per_trade must be > 0")
        if self.max_credit_slippage_pct is not None and self.max_credit_slippage_pct < 0:
            raise ValueError("risk.max_credit_slippage_pct must be >= 0")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyRiskDefaults:
        mapping = _require_mapping(payload, field_name="risk")
        return cls(
            min_return_on_risk=_optional_float(mapping.get("min_return_on_risk")),
            position_size_pct_of_available_balance=_optional_float(mapping.get("position_size_pct_of_available_balance")),
            max_risk_per_trade=_optional_float(mapping.get("max_risk_per_trade")),
            max_credit_slippage_pct=_optional_float(mapping.get("max_credit_slippage_pct")),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.min_return_on_risk is not None:
            payload["min_return_on_risk"] = self.min_return_on_risk
        if self.position_size_pct_of_available_balance is not None:
            payload["position_size_pct_of_available_balance"] = self.position_size_pct_of_available_balance
        if self.max_risk_per_trade is not None:
            payload["max_risk_per_trade"] = self.max_risk_per_trade
        if self.max_credit_slippage_pct is not None:
            payload["max_credit_slippage_pct"] = self.max_credit_slippage_pct
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
        mapping = _require_mapping(payload, field_name="routine.schedule.window")
        return cls(
            start_et=_validate_hhmm(mapping.get("start_et"), field_name="routine.schedule.window.start_et"),
            end_et=_validate_hhmm(mapping.get("end_et"), field_name="routine.schedule.window.end_et"),
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
        mapping = _require_mapping(payload, field_name="schedule")
        cadence_minutes = _optional_int(mapping.get("cadence_minutes"))
        if cadence_minutes is None:
            raise ValueError("routine.schedule.cadence_minutes is required")
        return cls(
            cadence_minutes=cadence_minutes,
            market_hours_only=bool(mapping.get("market_hours_only", False)),
            offset_seconds=_optional_int(mapping.get("offset_seconds")) or 0,
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
        mapping = _require_mapping(payload, field_name="triggers")
        return cls(
            min_signal_score=_optional_float(mapping.get("min_signal_score")),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.min_signal_score is not None:
            payload["min_signal_score"] = self.min_signal_score
        return payload


@dataclass(frozen=True)
class StrategyExecutionPolicy:
    approval: str
    mode: str
    runtime: str = "alpaca_direct"

    def __post_init__(self) -> None:
        approval = self.approval.lower()
        mode = self.mode.lower()
        runtime = self.runtime.lower()
        if approval not in {"auto", "manual"}:
            raise ValueError("execution.approval must be auto or manual")
        if mode not in {"paper", "live", "shadow"}:
            raise ValueError("execution.mode must be paper, live, or shadow")
        if runtime != "alpaca_direct":
            raise ValueError("execution.runtime must be alpaca_direct")
        object.__setattr__(self, "approval", approval)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "runtime", runtime)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyExecutionPolicy:
        mapping = _require_mapping(payload, field_name="execution")
        return cls(
            approval=_required_text(
                mapping.get("approval") or mapping.get("approval_mode"),
                field_name="execution.approval",
            ),
            mode=_required_text(mapping.get("mode"), field_name="execution.mode"),
            runtime=str(mapping.get("runtime") or "alpaca_direct"),
        )

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "approval": self.approval,
            "mode": self.mode,
        }
        if self.runtime != "alpaca_direct":
            payload["runtime"] = self.runtime
        return payload


@dataclass(frozen=True)
class StrategyRiskLimits:
    max_open_positions: int
    max_daily_actions: int
    max_new_entries_per_day: int | None = None
    daily_loss_limit: float | None = None

    def __post_init__(self) -> None:
        if self.max_open_positions < 0:
            raise ValueError("risk.limits.max_open_positions must be >= 0")
        if self.max_daily_actions < 0:
            raise ValueError("risk.limits.max_daily_actions must be >= 0")
        if self.max_new_entries_per_day is not None and self.max_new_entries_per_day < 0:
            raise ValueError("risk.limits.max_new_entries_per_day must be >= 0")
        if self.daily_loss_limit is not None and self.daily_loss_limit < 0:
            raise ValueError("risk.limits.daily_loss_limit must be >= 0")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyRiskLimits:
        mapping = _require_mapping(payload, field_name="risk.limits")
        return cls(
            max_open_positions=int(mapping.get("max_open_positions") or 0),
            max_daily_actions=int(mapping.get("max_daily_actions") or 0),
            max_new_entries_per_day=_optional_int(mapping.get("max_new_entries_per_day")),
            daily_loss_limit=_optional_float(mapping.get("daily_loss_limit")),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "max_open_positions": self.max_open_positions,
            "max_daily_actions": self.max_daily_actions,
        }
        if self.max_new_entries_per_day is not None:
            payload["max_new_entries_per_day"] = self.max_new_entries_per_day
        if self.daily_loss_limit is not None:
            payload["daily_loss_limit"] = self.daily_loss_limit
        return payload


@dataclass(frozen=True)
class StrategyRuntimeControls:
    live_enabled: bool = False
    cancel_pending_entries_after_et: str | None = None
    flatten_positions_at_et: str | None = None
    paused: bool = False

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyRuntimeControls:
        mapping = _require_mapping(payload, field_name="runtime")
        return cls(
            live_enabled=bool(mapping.get("live_enabled", False)),
            cancel_pending_entries_after_et=_validate_hhmm(
                mapping.get("cancel_pending_entries_after_et"),
                field_name="runtime.cancel_pending_entries_after_et",
            ),
            flatten_positions_at_et=_validate_hhmm(
                mapping.get("flatten_positions_at_et"),
                field_name="runtime.flatten_positions_at_et",
            ),
            paused=bool(mapping.get("paused", False)),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "live_enabled": self.live_enabled,
            "paused": self.paused,
        }
        if self.cancel_pending_entries_after_et is not None:
            payload["cancel_pending_entries_after_et"] = self.cancel_pending_entries_after_et
        if self.flatten_positions_at_et is not None:
            payload["flatten_positions_at_et"] = self.flatten_positions_at_et
        return payload


__all__ = [
    "StrategyExecutionPolicy",
    "RoutineSchedule",
    "RoutineScheduleWindow",
    "EntrySelectionPolicy",
    "StrategyRiskLimits",
    "StrategyRuntimeControls",
    "DeltaRange",
    "DteRange",
    "ExpectedMoveGuard",
    "IronCondorBuildConfig",
    "LongVolBuildConfig",
    "StrategyBuildConfig",
    "StrategyLiquidityRules",
    "StrategyRecipes",
    "StrategyRiskDefaults",
    "VerticalSpreadBuildConfig",
]
