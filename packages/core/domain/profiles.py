from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, time

from core.services.market_dates import NEW_YORK

DEFAULT_BOARD_UNIVERSE = "etf_core"
ZERO_DTE_CORE_SYMBOLS = ("SPY", "QQQ", "IWM")
ZERO_DTE_ALLOWED_SYMBOLS = (
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "XLF",
    "XLE",
    "XLI",
    "XLV",
    "GLD",
    "TLT",
)
UNIVERSE_PRESETS: dict[str, tuple[str, ...]] = {
    "0dte_core": ZERO_DTE_CORE_SYMBOLS,
    "explore_10": ZERO_DTE_ALLOWED_SYMBOLS,
    "etf_core": ("SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLE", "SMH"),
    "liquid_mixed": (
        "SPY",
        "QQQ",
        "IWM",
        "SMH",
        "XLK",
        "XLF",
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "AMD",
    ),
}
SHORT_PREMIUM_STRATEGIES = frozenset(
    {"call_credit_spread", "put_credit_spread", "iron_condor", "short_call", "short_put"}
)
DEBIT_SPREAD_STRATEGIES = frozenset({"call_debit_spread", "put_debit_spread"})
DIRECTIONAL_LONG_STRATEGIES = frozenset({"long_call", "long_put"})
LONG_VOL_STRATEGIES = frozenset({"long_straddle", "long_strangle"})

_RANKING_POLICY_STRATEGY_ALIASES = {
    "call_credit": "call_credit_spread",
    "put_credit": "put_credit_spread",
    "call_debit": "call_debit_spread",
    "put_debit": "put_debit_spread",
}


@dataclass(frozen=True)
class RankingWeightsConfig:
    probability_of_profit: float | None = None
    expected_value_dollars: float | None = None
    slippage_adjusted_expected_value_dollars: float | None = None
    entry_slippage_dollars: float | None = None
    model_implied_volatility: float | None = None

    def __post_init__(self) -> None:
        for field_name, value in (
            ("probability_of_profit", self.probability_of_profit),
            ("expected_value_dollars", self.expected_value_dollars),
            (
                "slippage_adjusted_expected_value_dollars",
                self.slippage_adjusted_expected_value_dollars,
            ),
            ("entry_slippage_dollars", self.entry_slippage_dollars),
            ("model_implied_volatility", self.model_implied_volatility),
        ):
            if value is not None and value < 0:
                raise ValueError(f"ranking weight {field_name} must be >= 0")

    def as_builder_params(self) -> dict[str, float]:
        payload: dict[str, float] = {}
        if self.probability_of_profit is not None:
            payload["ranking_weight_probability_of_profit"] = (
                self.probability_of_profit
            )
        if self.expected_value_dollars is not None:
            payload["ranking_weight_expected_value_dollars"] = (
                self.expected_value_dollars
            )
        if self.slippage_adjusted_expected_value_dollars is not None:
            payload["ranking_weight_slippage_adjusted_expected_value_dollars"] = (
                self.slippage_adjusted_expected_value_dollars
            )
        if self.entry_slippage_dollars is not None:
            payload["ranking_weight_entry_slippage_dollars"] = (
                self.entry_slippage_dollars
            )
        if self.model_implied_volatility is not None:
            payload["ranking_weight_model_implied_volatility"] = (
                self.model_implied_volatility
            )
        return payload

    def as_payload(self) -> dict[str, float]:
        payload: dict[str, float] = {}
        if self.probability_of_profit is not None:
            payload["probability_of_profit"] = self.probability_of_profit
        if self.expected_value_dollars is not None:
            payload["expected_value_dollars"] = self.expected_value_dollars
        if self.slippage_adjusted_expected_value_dollars is not None:
            payload["slippage_adjusted_expected_value_dollars"] = (
                self.slippage_adjusted_expected_value_dollars
            )
        if self.entry_slippage_dollars is not None:
            payload["entry_slippage_dollars"] = self.entry_slippage_dollars
        if self.model_implied_volatility is not None:
            payload["model_implied_volatility"] = self.model_implied_volatility
        return payload


@dataclass(frozen=True)
class RankingPolicyConfig:
    min_probability_of_profit: float | None = None
    min_expected_value_dollars: float | None = None
    min_slippage_adjusted_expected_value_dollars: float | None = None
    max_entry_slippage_dollars: float | None = None
    min_model_implied_volatility: float | None = None
    max_model_implied_volatility: float | None = None
    weights: RankingWeightsConfig = field(default_factory=RankingWeightsConfig)

    def __post_init__(self) -> None:
        if self.min_probability_of_profit is not None and not (
            0.0 <= self.min_probability_of_profit <= 1.0
        ):
            raise ValueError("min_probability_of_profit must be between 0 and 1")
        for field_name, value in (
            ("min_expected_value_dollars", self.min_expected_value_dollars),
            (
                "min_slippage_adjusted_expected_value_dollars",
                self.min_slippage_adjusted_expected_value_dollars,
            ),
            ("max_entry_slippage_dollars", self.max_entry_slippage_dollars),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        for field_name, value in (
            ("min_model_implied_volatility", self.min_model_implied_volatility),
            ("max_model_implied_volatility", self.max_model_implied_volatility),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        if (
            self.min_model_implied_volatility is not None
            and self.max_model_implied_volatility is not None
            and self.max_model_implied_volatility < self.min_model_implied_volatility
        ):
            raise ValueError("max_model_implied_volatility must be >= min")

    def as_builder_params(self) -> dict[str, float]:
        payload: dict[str, float] = {}
        if self.min_probability_of_profit is not None:
            payload["ranking_min_probability_of_profit"] = (
                self.min_probability_of_profit
            )
        if self.min_expected_value_dollars is not None:
            payload["ranking_min_expected_value_dollars"] = (
                self.min_expected_value_dollars
            )
        if self.min_slippage_adjusted_expected_value_dollars is not None:
            payload["ranking_min_slippage_adjusted_expected_value_dollars"] = (
                self.min_slippage_adjusted_expected_value_dollars
            )
        if self.max_entry_slippage_dollars is not None:
            payload["ranking_max_entry_slippage_dollars"] = (
                self.max_entry_slippage_dollars
            )
        if self.min_model_implied_volatility is not None:
            payload["ranking_min_model_implied_volatility"] = (
                self.min_model_implied_volatility
            )
        if self.max_model_implied_volatility is not None:
            payload["ranking_max_model_implied_volatility"] = (
                self.max_model_implied_volatility
            )
        payload.update(self.weights.as_builder_params())
        return payload

    def as_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {}
        if self.min_probability_of_profit is not None:
            payload["min_probability_of_profit"] = self.min_probability_of_profit
        if self.min_expected_value_dollars is not None:
            payload["min_expected_value_dollars"] = self.min_expected_value_dollars
        if self.min_slippage_adjusted_expected_value_dollars is not None:
            payload["min_slippage_adjusted_expected_value_dollars"] = (
                self.min_slippage_adjusted_expected_value_dollars
            )
        if self.max_entry_slippage_dollars is not None:
            payload["max_entry_slippage_dollars"] = self.max_entry_slippage_dollars
        if self.min_model_implied_volatility is not None:
            payload["min_model_implied_volatility"] = (
                self.min_model_implied_volatility
            )
        if self.max_model_implied_volatility is not None:
            payload["max_model_implied_volatility"] = (
                self.max_model_implied_volatility
            )
        weights_payload = self.weights.as_payload()
        if weights_payload:
            payload["weights"] = weights_payload
        return payload


@dataclass(frozen=True)
class ProfileConfig:
    name: str
    min_dte: int
    max_dte: int
    short_delta_min: float
    short_delta_max: float
    short_delta_target: float
    min_width: float
    max_width_by_underlying: dict[str, float]
    min_credit: float
    min_open_interest_by_underlying: dict[str, int]
    max_relative_spread_by_underlying: dict[str, float]
    min_return_on_risk: float
    min_fill_ratio: float
    min_short_vs_expected_move_ratio: float
    min_breakeven_vs_expected_move_ratio: float


@dataclass(frozen=True)
class StrategyProfileOverride:
    short_delta_min: float | None = None
    short_delta_max: float | None = None
    short_delta_target: float | None = None
    min_short_vs_expected_move_ratio: float | None = None
    min_breakeven_vs_expected_move_ratio: float | None = None


_PROFILE_RANKING_POLICY_BASES: dict[str, RankingPolicyConfig] = {
    "0dte": RankingPolicyConfig(
        min_probability_of_profit=0.62,
        min_expected_value_dollars=8.0,
        min_slippage_adjusted_expected_value_dollars=4.0,
        max_entry_slippage_dollars=8.0,
        weights=RankingWeightsConfig(
            probability_of_profit=0.42,
            expected_value_dollars=0.16,
            slippage_adjusted_expected_value_dollars=0.24,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.06,
        ),
    ),
    "micro": RankingPolicyConfig(
        min_probability_of_profit=0.60,
        min_expected_value_dollars=9.0,
        min_slippage_adjusted_expected_value_dollars=5.0,
        max_entry_slippage_dollars=10.0,
        weights=RankingWeightsConfig(
            probability_of_profit=0.40,
            expected_value_dollars=0.16,
            slippage_adjusted_expected_value_dollars=0.25,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.07,
        ),
    ),
    "weekly": RankingPolicyConfig(
        min_probability_of_profit=0.58,
        min_expected_value_dollars=10.0,
        min_slippage_adjusted_expected_value_dollars=6.0,
        max_entry_slippage_dollars=12.0,
        weights=RankingWeightsConfig(
            probability_of_profit=0.38,
            expected_value_dollars=0.17,
            slippage_adjusted_expected_value_dollars=0.25,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.08,
        ),
    ),
    "swing": RankingPolicyConfig(
        min_probability_of_profit=0.54,
        min_expected_value_dollars=12.0,
        min_slippage_adjusted_expected_value_dollars=7.0,
        max_entry_slippage_dollars=15.0,
        weights=RankingWeightsConfig(
            probability_of_profit=0.34,
            expected_value_dollars=0.19,
            slippage_adjusted_expected_value_dollars=0.25,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.10,
        ),
    ),
    "core": RankingPolicyConfig(
        min_probability_of_profit=0.50,
        min_expected_value_dollars=15.0,
        min_slippage_adjusted_expected_value_dollars=9.0,
        max_entry_slippage_dollars=18.0,
        weights=RankingWeightsConfig(
            probability_of_profit=0.30,
            expected_value_dollars=0.22,
            slippage_adjusted_expected_value_dollars=0.25,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.11,
        ),
    ),
}

_STRATEGY_RANKING_POLICY_OVERRIDES: dict[str, RankingPolicyConfig] = {
    "call_debit_spread": RankingPolicyConfig(
        min_probability_of_profit=0.40,
        max_model_implied_volatility=0.42,
        weights=RankingWeightsConfig(
            probability_of_profit=0.28,
            expected_value_dollars=0.26,
            slippage_adjusted_expected_value_dollars=0.28,
            entry_slippage_dollars=0.10,
            model_implied_volatility=0.08,
        ),
    ),
    "put_debit_spread": RankingPolicyConfig(
        min_probability_of_profit=0.40,
        max_model_implied_volatility=0.42,
        weights=RankingWeightsConfig(
            probability_of_profit=0.28,
            expected_value_dollars=0.26,
            slippage_adjusted_expected_value_dollars=0.28,
            entry_slippage_dollars=0.10,
            model_implied_volatility=0.08,
        ),
    ),
    "long_call": RankingPolicyConfig(
        min_probability_of_profit=0.34,
        max_model_implied_volatility=0.40,
        weights=RankingWeightsConfig(
            probability_of_profit=0.22,
            expected_value_dollars=0.28,
            slippage_adjusted_expected_value_dollars=0.30,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.08,
        ),
    ),
    "long_put": RankingPolicyConfig(
        min_probability_of_profit=0.34,
        max_model_implied_volatility=0.40,
        weights=RankingWeightsConfig(
            probability_of_profit=0.22,
            expected_value_dollars=0.28,
            slippage_adjusted_expected_value_dollars=0.30,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.08,
        ),
    ),
    "long_straddle": RankingPolicyConfig(
        min_probability_of_profit=0.28,
        max_model_implied_volatility=0.34,
        weights=RankingWeightsConfig(
            probability_of_profit=0.18,
            expected_value_dollars=0.28,
            slippage_adjusted_expected_value_dollars=0.34,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.08,
        ),
    ),
    "long_strangle": RankingPolicyConfig(
        min_probability_of_profit=0.28,
        max_model_implied_volatility=0.34,
        weights=RankingWeightsConfig(
            probability_of_profit=0.18,
            expected_value_dollars=0.28,
            slippage_adjusted_expected_value_dollars=0.34,
            entry_slippage_dollars=0.12,
            model_implied_volatility=0.08,
        ),
    ),
}


PROFILE_CONFIGS: dict[str, ProfileConfig] = {
    "0dte": ProfileConfig(
        name="0dte",
        min_dte=0,
        max_dte=0,
        short_delta_min=0.03,
        short_delta_max=0.18,
        short_delta_target=0.10,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 2.0, "single_name_equity": 2.0},
        min_credit=0.08,
        min_open_interest_by_underlying={
            "etf_index_proxy": 750,
            "single_name_equity": 750,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.08,
            "single_name_equity": 0.08,
        },
        min_return_on_risk=0.05,
        min_fill_ratio=0.80,
        min_short_vs_expected_move_ratio=0.08,
        min_breakeven_vs_expected_move_ratio=0.03,
    ),
    "micro": ProfileConfig(
        name="micro",
        min_dte=1,
        max_dte=3,
        short_delta_min=0.05,
        short_delta_max=0.12,
        short_delta_target=0.08,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 2.0, "single_name_equity": 2.0},
        min_credit=0.10,
        min_open_interest_by_underlying={
            "etf_index_proxy": 1500,
            "single_name_equity": 1500,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.10,
            "single_name_equity": 0.10,
        },
        min_return_on_risk=0.08,
        min_fill_ratio=0.75,
        min_short_vs_expected_move_ratio=0.05,
        min_breakeven_vs_expected_move_ratio=0.00,
    ),
    "weekly": ProfileConfig(
        name="weekly",
        min_dte=4,
        max_dte=10,
        short_delta_min=0.08,
        short_delta_max=0.16,
        short_delta_target=0.12,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 3.0, "single_name_equity": 5.0},
        min_credit=0.18,
        min_open_interest_by_underlying={
            "etf_index_proxy": 500,
            "single_name_equity": 400,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.12,
            "single_name_equity": 0.15,
        },
        min_return_on_risk=0.10,
        min_fill_ratio=0.72,
        min_short_vs_expected_move_ratio=-0.05,
        min_breakeven_vs_expected_move_ratio=-0.02,
    ),
    "swing": ProfileConfig(
        name="swing",
        min_dte=11,
        max_dte=21,
        short_delta_min=0.12,
        short_delta_max=0.20,
        short_delta_target=0.16,
        min_width=1.0,
        max_width_by_underlying={"etf_index_proxy": 5.0, "single_name_equity": 10.0},
        min_credit=0.25,
        min_open_interest_by_underlying={
            "etf_index_proxy": 500,
            "single_name_equity": 250,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.18,
            "single_name_equity": 0.18,
        },
        min_return_on_risk=0.10,
        min_fill_ratio=0.70,
        min_short_vs_expected_move_ratio=-0.08,
        min_breakeven_vs_expected_move_ratio=-0.04,
    ),
    "core": ProfileConfig(
        name="core",
        min_dte=22,
        max_dte=35,
        short_delta_min=0.15,
        short_delta_max=0.22,
        short_delta_target=0.18,
        min_width=2.0,
        max_width_by_underlying={"etf_index_proxy": 10.0, "single_name_equity": 10.0},
        min_credit=0.35,
        min_open_interest_by_underlying={
            "etf_index_proxy": 300,
            "single_name_equity": 200,
        },
        max_relative_spread_by_underlying={
            "etf_index_proxy": 0.20,
            "single_name_equity": 0.20,
        },
        min_return_on_risk=0.12,
        min_fill_ratio=0.68,
        min_short_vs_expected_move_ratio=-0.10,
        min_breakeven_vs_expected_move_ratio=-0.05,
    ),
}

_EMPTY_STRATEGY_PROFILE_OVERRIDE = StrategyProfileOverride()

_STRATEGY_PROFILE_OVERRIDES: dict[tuple[str, str], StrategyProfileOverride] = {
    (
        "weekly",
        "short_call",
    ): StrategyProfileOverride(
        short_delta_min=0.14,
        short_delta_max=0.22,
        short_delta_target=0.19,
        min_short_vs_expected_move_ratio=-0.10,
        min_breakeven_vs_expected_move_ratio=-0.05,
    ),
}


def _normalize_ranking_strategy(strategy: str) -> str:
    normalized = str(strategy or "").strip().lower()
    return _RANKING_POLICY_STRATEGY_ALIASES.get(normalized, normalized)


def _merge_ranking_policies(
    base: RankingPolicyConfig,
    override: RankingPolicyConfig | None,
) -> RankingPolicyConfig:
    if override is None:
        return base
    merged_weights = replace(
        base.weights,
        **{
            field_name: value
            for field_name, value in override.weights.__dict__.items()
            if value is not None
        },
    )
    override_payload = {
        field_name: value
        for field_name, value in override.__dict__.items()
        if field_name != "weights" and value is not None
    }
    return replace(base, weights=merged_weights, **override_payload)


def resolve_ranking_policy(profile_name: str, strategy: str) -> RankingPolicyConfig:
    base = _PROFILE_RANKING_POLICY_BASES[profile_name]
    return _merge_ranking_policies(
        base,
        _STRATEGY_RANKING_POLICY_OVERRIDES.get(_normalize_ranking_strategy(strategy)),
    )


def resolve_strategy_profile_override(
    profile_name: str,
    strategy: str,
) -> StrategyProfileOverride:
    return _STRATEGY_PROFILE_OVERRIDES.get(
        (str(profile_name), _normalize_ranking_strategy(strategy)),
        _EMPTY_STRATEGY_PROFILE_OVERRIDE,
    )


def zero_dte_session_bucket(now: datetime | None = None) -> str:
    current = datetime.now(NEW_YORK) if now is None else now.astimezone(NEW_YORK)
    current_time = current.time()
    if current_time < time(9, 30) or current_time >= time(16, 0):
        return "off_hours"
    if current_time < time(10, 30):
        return "open"
    if current_time < time(13, 30):
        return "midday"
    return "late"


def format_session_bucket(bucket: str) -> str:
    return bucket.replace("_", "-")


def zero_dte_delta_target(session_bucket: str) -> float:
    return {
        "open": 0.08,
        "midday": 0.10,
        "late": 0.12,
        "off_hours": 0.10,
    }[session_bucket]


__all__ = [
    "DEFAULT_BOARD_UNIVERSE",
    "DEBIT_SPREAD_STRATEGIES",
    "DIRECTIONAL_LONG_STRATEGIES",
    "LONG_VOL_STRATEGIES",
    "PROFILE_CONFIGS",
    "RankingPolicyConfig",
    "RankingWeightsConfig",
    "SHORT_PREMIUM_STRATEGIES",
    "ProfileConfig",
    "StrategyProfileOverride",
    "UNIVERSE_PRESETS",
    "ZERO_DTE_ALLOWED_SYMBOLS",
    "ZERO_DTE_CORE_SYMBOLS",
    "format_session_bucket",
    "resolve_ranking_policy",
    "resolve_strategy_profile_override",
    "zero_dte_delta_target",
    "zero_dte_session_bucket",
]
