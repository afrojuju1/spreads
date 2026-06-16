from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from core.domain.profiles import RankingPolicyConfig, RankingWeightsConfig

PROTECTION_RULE_KEYS = frozenset(
    {
        "account_emergency_stop",
        "daily_drawdown_halt",
        "rolling_drawdown_halt",
        "loss_streak_cooldown",
        "strategy_family_cooldown",
        "event_calendar_block",
        "duplicate_underlying_theme_cap",
        "options_exposure_scenario_cap",
    }
)


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


def _optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    rendered = str(value).strip().lower()
    if rendered in {"1", "true", "yes", "y", "on"}:
        return True
    if rendered in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Expected boolean value, got {value!r}")


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
class StrategyOrderStylePolicy:
    order_type: str = "limit"
    time_in_force: str = "day"
    pricing_mode: str = "adaptive_credit"
    min_credit_retention_pct: float = 0.95

    def __post_init__(self) -> None:
        order_type = self.order_type.lower()
        time_in_force = self.time_in_force.lower()
        pricing_mode = self.pricing_mode.lower()
        if order_type != "limit":
            raise ValueError("execution.order_style.order_type must be limit")
        if time_in_force not in {"day", "gtc"}:
            raise ValueError("execution.order_style.time_in_force must be day or gtc")
        if pricing_mode not in {"midpoint", "adaptive_credit", "adaptive_debit", "adaptive"}:
            raise ValueError("execution.order_style.pricing_mode is unsupported")
        if self.min_credit_retention_pct < 0.5 or self.min_credit_retention_pct > 1.0:
            raise ValueError("execution.order_style.min_credit_retention_pct must be between 0.5 and 1.0")
        object.__setattr__(self, "order_type", order_type)
        object.__setattr__(self, "time_in_force", time_in_force)
        object.__setattr__(self, "pricing_mode", pricing_mode)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> StrategyOrderStylePolicy:
        mapping = _require_mapping(payload, field_name="execution.order_style")
        return cls(
            order_type=_required_text(
                mapping.get("order_type") or mapping.get("type") or "limit",
                field_name="execution.order_style.order_type",
            ),
            time_in_force=_required_text(
                mapping.get("time_in_force") or "day",
                field_name="execution.order_style.time_in_force",
            ),
            pricing_mode=_required_text(
                mapping.get("pricing_mode") or "adaptive_credit",
                field_name="execution.order_style.pricing_mode",
            ),
            min_credit_retention_pct=_optional_float(mapping.get("min_credit_retention_pct")) or 0.95,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "pricing_mode": self.pricing_mode,
            "min_credit_retention_pct": self.min_credit_retention_pct,
        }


@dataclass(frozen=True)
class StrategyQuoteFreshnessPolicy:
    max_age_seconds: int = 180

    def __post_init__(self) -> None:
        if self.max_age_seconds <= 0:
            raise ValueError("execution.quote_freshness.max_age_seconds must be positive")

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | None) -> StrategyQuoteFreshnessPolicy:
        mapping = _require_mapping(payload, field_name="execution.quote_freshness")
        return cls(max_age_seconds=_optional_int(mapping.get("max_age_seconds")) or 180)

    def as_dict(self) -> dict[str, Any]:
        return {"max_age_seconds": self.max_age_seconds}


@dataclass(frozen=True)
class StrategyOrderRepricingPolicy:
    enabled: bool = True
    stale_after_seconds: int = 75
    max_reprices: int = 3
    price_step: float = 0.01
    max_concession: float = 0.03

    def __post_init__(self) -> None:
        if self.stale_after_seconds <= 0:
            raise ValueError("execution lifecycle repricing.stale_after_seconds must be positive")
        if self.max_reprices < 0:
            raise ValueError("execution lifecycle repricing.max_reprices must be non-negative")
        if self.price_step <= 0:
            raise ValueError("execution lifecycle repricing.price_step must be positive")
        if self.max_concession < 0:
            raise ValueError("execution lifecycle repricing.max_concession must be non-negative")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
        *,
        default_stale_after_seconds: int = 75,
        default_max_concession: float = 0.03,
    ) -> StrategyOrderRepricingPolicy:
        mapping = _require_mapping(payload, field_name="execution lifecycle repricing")
        stale_after_value = mapping.get("stale_after_seconds")
        if stale_after_value in (None, ""):
            stale_after_value = mapping.get("ttl_seconds")
        max_reprices_value = mapping.get("max_reprices")
        if max_reprices_value in (None, ""):
            max_reprices_value = mapping.get("max_reprice_count")
        max_concession_value = mapping.get("max_concession")
        if max_concession_value in (None, ""):
            max_concession_value = mapping.get("max_credit_concession")
        return cls(
            enabled=_optional_bool(mapping.get("enabled")) if mapping.get("enabled") is not None else True,
            stale_after_seconds=_optional_int(stale_after_value) or default_stale_after_seconds,
            max_reprices=_optional_int(max_reprices_value) if max_reprices_value not in (None, "") else 3,
            price_step=_optional_float(mapping.get("price_step") or mapping.get("step")) or 0.01,
            max_concession=(
                _optional_float(max_concession_value) if max_concession_value not in (None, "") else default_max_concession
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "stale_after_seconds": self.stale_after_seconds,
            "max_reprices": self.max_reprices,
            "price_step": self.price_step,
            "max_concession": self.max_concession,
        }


@dataclass(frozen=True)
class StrategyOrderLifecyclePolicy:
    submit_ttl_minutes: int = 5
    stale_order_action: str = "cancel_and_reprice"
    repricing: StrategyOrderRepricingPolicy = field(default_factory=StrategyOrderRepricingPolicy)

    def __post_init__(self) -> None:
        stale_order_action = self.stale_order_action.lower()
        if self.submit_ttl_minutes <= 0:
            raise ValueError("execution lifecycle submit_ttl_minutes must be positive")
        if stale_order_action not in {"cancel_and_reprice", "leave_working", "fail_closed"}:
            raise ValueError("execution lifecycle stale_order_action is unsupported")
        object.__setattr__(self, "stale_order_action", stale_order_action)

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
        *,
        default_stale_after_seconds: int = 75,
        default_max_concession: float = 0.03,
    ) -> StrategyOrderLifecyclePolicy:
        mapping = _require_mapping(payload, field_name="execution lifecycle")
        repricing_payload = mapping.get("repricing")
        submit_ttl_value = mapping.get("submit_ttl_minutes")
        if submit_ttl_value in (None, ""):
            submit_ttl_value = mapping.get("ttl_minutes")
        return cls(
            submit_ttl_minutes=_optional_int(submit_ttl_value) or 5,
            stale_order_action=_required_text(
                mapping.get("stale_order_action") or "cancel_and_reprice",
                field_name="execution lifecycle stale_order_action",
            ),
            repricing=StrategyOrderRepricingPolicy.from_payload(
                repricing_payload if isinstance(repricing_payload, Mapping) else None,
                default_stale_after_seconds=default_stale_after_seconds,
                default_max_concession=default_max_concession,
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "submit_ttl_minutes": self.submit_ttl_minutes,
            "stale_order_action": self.stale_order_action,
            "repricing": self.repricing.as_dict(),
        }


@dataclass(frozen=True)
class StrategyExecutionPolicy:
    approval: str
    mode: str
    runtime: str = "alpaca_direct"
    executor_profile_id: str | None = None
    order_style: StrategyOrderStylePolicy = field(default_factory=StrategyOrderStylePolicy)
    quote_freshness: StrategyQuoteFreshnessPolicy = field(default_factory=StrategyQuoteFreshnessPolicy)
    open_lifecycle: StrategyOrderLifecyclePolicy = field(default_factory=StrategyOrderLifecyclePolicy)
    close_lifecycle: StrategyOrderLifecyclePolicy = field(default_factory=StrategyOrderLifecyclePolicy)
    unsupported_structure_behavior: str = "fail_closed"

    def __post_init__(self) -> None:
        approval = self.approval.lower()
        mode = self.mode.lower()
        runtime = self.runtime.lower()
        unsupported_structure_behavior = self.unsupported_structure_behavior.lower()
        if approval not in {"auto", "manual"}:
            raise ValueError("execution.approval must be auto or manual")
        if mode not in {"paper", "live", "shadow"}:
            raise ValueError("execution.mode must be paper, live, or shadow")
        if runtime != "alpaca_direct":
            raise ValueError("execution.runtime must be alpaca_direct")
        if unsupported_structure_behavior not in {"fail_closed"}:
            raise ValueError("execution.unsupported_structure_behavior must be fail_closed")
        object.__setattr__(self, "approval", approval)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "runtime", runtime)
        object.__setattr__(self, "unsupported_structure_behavior", unsupported_structure_behavior)

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
            executor_profile_id=_optional_text(mapping.get("executor_profile_id") or mapping.get("profile_id")),
            order_style=StrategyOrderStylePolicy.from_payload(mapping.get("order_style") if isinstance(mapping.get("order_style"), Mapping) else None),
            quote_freshness=StrategyQuoteFreshnessPolicy.from_payload(
                mapping.get("quote_freshness") if isinstance(mapping.get("quote_freshness"), Mapping) else None
            ),
            open_lifecycle=StrategyOrderLifecyclePolicy.from_payload(
                mapping.get("open_lifecycle") if isinstance(mapping.get("open_lifecycle"), Mapping) else None,
                default_stale_after_seconds=75,
                default_max_concession=0.03,
            ),
            close_lifecycle=StrategyOrderLifecyclePolicy.from_payload(
                mapping.get("close_lifecycle") if isinstance(mapping.get("close_lifecycle"), Mapping) else None,
                default_stale_after_seconds=75,
                default_max_concession=0.03,
            ),
            unsupported_structure_behavior=_required_text(
                mapping.get("unsupported_structure_behavior") or "fail_closed",
                field_name="execution.unsupported_structure_behavior",
            ),
        )

    def lifecycle_for_action(self, action_type: str) -> StrategyOrderLifecyclePolicy:
        return self.close_lifecycle if str(action_type or "").strip().lower() == "close" else self.open_lifecycle

    def execution_policy_for_action(self, action_type: str, *, quantity: int | None = None) -> dict[str, Any]:
        lifecycle = self.lifecycle_for_action(action_type)
        payload = {
            "enabled": self.approval == "auto" and self.mode in {"paper", "live"},
            "mode": "top_promotable",
            "pricing_mode": self.order_style.pricing_mode,
            "min_credit_retention_pct": self.order_style.min_credit_retention_pct,
            "max_credit_concession": lifecycle.repricing.max_concession,
            "order_type": self.order_style.order_type,
            "time_in_force": self.order_style.time_in_force,
            "max_quote_age_seconds": self.quote_freshness.max_age_seconds,
            "submit_ttl_minutes": lifecycle.submit_ttl_minutes,
            "stale_order_action": lifecycle.stale_order_action,
            "unsupported_structure_behavior": self.unsupported_structure_behavior,
            "repricing_policy": lifecycle.repricing.as_dict(),
        }
        if quantity is not None:
            payload["quantity"] = quantity
        return payload

    def executor_profile_snapshot(self, action_type: str) -> dict[str, Any]:
        lifecycle = self.lifecycle_for_action(action_type)
        return {
            "executor_profile_id": self.executor_profile_id,
            "approval": self.approval,
            "mode": self.mode,
            "runtime": self.runtime,
            "order_style": self.order_style.as_dict(),
            "quote_freshness": self.quote_freshness.as_dict(),
            "lifecycle": lifecycle.as_dict(),
            "unsupported_structure_behavior": self.unsupported_structure_behavior,
        }

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "approval": self.approval,
            "mode": self.mode,
            "runtime": self.runtime,
            "order_style": self.order_style.as_dict(),
            "quote_freshness": self.quote_freshness.as_dict(),
            "open_lifecycle": self.open_lifecycle.as_dict(),
            "close_lifecycle": self.close_lifecycle.as_dict(),
            "unsupported_structure_behavior": self.unsupported_structure_behavior,
        }
        if self.executor_profile_id is not None:
            payload["executor_profile_id"] = self.executor_profile_id
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
            profile_id=_optional_text(quality_profile),
            overrides=dict(_require_mapping(quality_overrides, field_name="entry.quality_overrides")),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.profile_id is not None:
            payload["quality_profile"] = self.profile_id
        if self.overrides:
            payload["quality_overrides"] = dict(self.overrides)
        return payload


@dataclass(frozen=True)
class StrategyPortfolioAdmissionLimits:
    max_strategy_open_positions: int | None = None
    max_family_open_positions: int | None = None
    max_symbol_family_open_positions: int | None = None
    max_daily_new_entries: int | None = None
    max_total_strategy_risk: float | None = None
    max_correlated_group_open_positions: int | None = None
    configured: bool = False

    def __post_init__(self) -> None:
        for field_name, value in (
            (
                "risk.limits.portfolio_admission.max_strategy_open_positions",
                self.max_strategy_open_positions,
            ),
            (
                "risk.limits.portfolio_admission.max_family_open_positions",
                self.max_family_open_positions,
            ),
            (
                "risk.limits.portfolio_admission.max_symbol_family_open_positions",
                self.max_symbol_family_open_positions,
            ),
            (
                "risk.limits.portfolio_admission.max_daily_new_entries",
                self.max_daily_new_entries,
            ),
            (
                "risk.limits.portfolio_admission.max_correlated_group_open_positions",
                self.max_correlated_group_open_positions,
            ),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        if self.max_total_strategy_risk is not None and self.max_total_strategy_risk < 0:
            raise ValueError("risk.limits.portfolio_admission.max_total_strategy_risk must be >= 0")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyPortfolioAdmissionLimits:
        mapping = _require_mapping(payload, field_name="risk.limits.portfolio_admission")
        return cls(
            max_strategy_open_positions=_optional_int(mapping.get("max_strategy_open_positions")),
            max_family_open_positions=_optional_int(mapping.get("max_family_open_positions")),
            max_symbol_family_open_positions=_optional_int(mapping.get("max_symbol_family_open_positions")),
            max_daily_new_entries=_optional_int(mapping.get("max_daily_new_entries")),
            max_total_strategy_risk=_optional_float(mapping.get("max_total_strategy_risk")),
            max_correlated_group_open_positions=_optional_int(mapping.get("max_correlated_group_open_positions")),
            configured=bool(mapping),
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key, value in (
            ("max_strategy_open_positions", self.max_strategy_open_positions),
            ("max_family_open_positions", self.max_family_open_positions),
            ("max_symbol_family_open_positions", self.max_symbol_family_open_positions),
            ("max_daily_new_entries", self.max_daily_new_entries),
            ("max_total_strategy_risk", self.max_total_strategy_risk),
            ("max_correlated_group_open_positions", self.max_correlated_group_open_positions),
        ):
            if value is not None:
                payload[key] = value
        return payload

    def as_policy(
        self,
        *,
        trading_strategy_id: str,
        strategy_family: str,
    ) -> dict[str, Any]:
        return {
            "trading_strategy_id": trading_strategy_id,
            "strategy_family": strategy_family,
            "policy_source": "strategy_config",
            **self.as_dict(),
        }


@dataclass(frozen=True)
class StrategyRiskLimits:
    max_open_positions: int
    max_daily_actions: int
    max_new_entries_per_day: int | None = None
    daily_loss_limit: float | None = None
    portfolio_admission: StrategyPortfolioAdmissionLimits = field(default_factory=StrategyPortfolioAdmissionLimits)

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
            portfolio_admission=StrategyPortfolioAdmissionLimits.from_payload(
                mapping.get("portfolio_admission"),
            ),
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
        if self.portfolio_admission.configured:
            payload["portfolio_admission"] = self.portfolio_admission.as_dict()
        return payload


@dataclass(frozen=True)
class StrategyProtectionPolicy:
    profile_id: str | None = None
    rules: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        unknown = sorted(set(self.rules) - PROTECTION_RULE_KEYS)
        if unknown:
            joined = ", ".join(unknown)
            raise ValueError(f"protection.rules contains unsupported rule(s): {joined}")
        for rule_name, rule_payload in self.rules.items():
            if not isinstance(rule_payload, Mapping):
                raise ValueError(f"protection.rules.{rule_name} must be a mapping")

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any] | None,
    ) -> StrategyProtectionPolicy:
        mapping = _require_mapping(payload, field_name="protection")
        raw_rules = _require_mapping(mapping.get("rules"), field_name="protection.rules")
        rules: dict[str, Mapping[str, Any]] = {}
        for raw_rule_name, raw_rule_payload in raw_rules.items():
            rule_name = _required_text(raw_rule_name, field_name="protection.rules key")
            rules[rule_name] = dict(
                _require_mapping(
                    raw_rule_payload,
                    field_name=f"protection.rules.{rule_name}",
                )
            )
        return cls(
            profile_id=_optional_text(mapping.get("protection_model_id") or mapping.get("profile_id")),
            rules=rules,
        )

    @property
    def configured(self) -> bool:
        return bool(self.rules)

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"rules": {key: dict(value) for key, value in self.rules.items()}}
        if self.profile_id is not None:
            payload["protection_model_id"] = self.profile_id
        return payload


@dataclass(frozen=True)
class StrategyRuntimeControls:
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
    "StrategyPortfolioAdmissionLimits",
    "StrategyProtectionPolicy",
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
