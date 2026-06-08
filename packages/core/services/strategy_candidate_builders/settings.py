from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from typing import Any

from core.domain.profiles import (
    PROFILE_CONFIGS,
    ZERO_DTE_ALLOWED_SYMBOLS,
    resolve_ranking_policy,
    resolve_strategy_profile_override,
)
from core.integrations.alpaca.client import DEFAULT_DATA_BASE_URL
from core.integrations.calendar_events import classify_underlying_type
from core.services.option_structures import normalize_strategy_family
from core.services.strategy_candidate_builders.runtime_context import candidate_session_bucket
from core.services.trading_strategies import default_config_root, load_trading_strategies

DIRECTIONAL_LONG_DELTA_DEFAULTS: dict[str, tuple[float, float, float]] = {
    "0dte": (0.18, 0.35, 0.25),
    "micro": (0.18, 0.35, 0.25),
    "weekly": (0.20, 0.40, 0.30),
    "swing": (0.25, 0.45, 0.35),
    "core": (0.30, 0.50, 0.40),
}

RANKING_POLICY_ARG_KEYS = (
    "ranking_min_probability_of_profit",
    "ranking_min_expected_value_dollars",
    "ranking_min_slippage_adjusted_expected_value_dollars",
    "ranking_max_entry_slippage_dollars",
    "ranking_min_model_implied_volatility",
    "ranking_max_model_implied_volatility",
    "ranking_weight_probability_of_profit",
    "ranking_weight_expected_value_dollars",
    "ranking_weight_slippage_adjusted_expected_value_dollars",
    "ranking_weight_entry_slippage_dollars",
    "ranking_weight_model_implied_volatility",
)

PROFILE_FALLBACK_RANKING_STRATEGY_FAMILIES = frozenset(
    {
        "combined",
        "call_debit_spread",
        "put_debit_spread",
        "long_call",
        "long_put",
        "short_call",
        "short_put",
        "long_straddle",
        "long_strangle",
    }
)
CALENDAR_CONFIDENCE_POLICIES = ("strict", "consensus", "off")


def _default_trading_base_url() -> str | None:
    return os.environ.get("ALPACA_TRADING_BASE_URL")


def _default_data_base_url() -> str:
    return os.environ.get("ALPACA_DATA_BASE_URL", DEFAULT_DATA_BASE_URL)


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    rendered = str(value).strip()
    return rendered or None


def _symbol_tuple(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, str):
        raw_symbols = value.split(",")
    else:
        raw_symbols = list(value or [])
    return tuple(dict.fromkeys(str(symbol).upper().strip() for symbol in raw_symbols if str(symbol or "").strip()))


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _ranking_value_payload(context: Any) -> dict[str, float | None]:
    return {key: _optional_float(getattr(context, key, None)) for key in RANKING_POLICY_ARG_KEYS}


@dataclass(frozen=True)
class CandidateBuildParameters:
    symbol: str | None = None
    symbols: tuple[str, ...] = ()
    candidate_builder_key: str = "call_credit"
    build_profile: str = "core"
    min_dte: int | None = None
    max_dte: int | None = None
    short_delta_min: float | None = None
    short_delta_max: float | None = None
    short_delta_target: float | None = None
    min_width: float | None = None
    max_width: float | None = None
    min_credit: float | None = None
    min_open_interest: int | None = None
    max_relative_spread: float | None = None
    min_return_on_risk: float | None = None
    min_fill_ratio: float | None = None
    min_short_vs_expected_move_ratio: float | None = None
    min_breakeven_vs_expected_move_ratio: float | None = None
    max_quote_age_seconds: int | None = None
    feed: str = "opra"
    stock_feed: str = "sip"
    greeks_source: str = "auto"
    calendar_policy: str = "strict"
    refresh_calendar_events: bool = False
    expand_duplicates: bool = False
    setup_filter: str = "on"
    data_policy: str = "strict"
    calendar_confidence_policy: str = "strict"
    top: int = 10
    per_symbol_top: int = 1
    trading_base_url: str | None = field(default_factory=_default_trading_base_url)
    data_base_url: str = field(default_factory=_default_data_base_url)
    history_db: str | None = None
    session_label: str | None = None
    evaluation_timestamp: str | None = None
    evaluation_date: str | None = None
    session_bucket_override: str | None = None
    config_root: str | None = None
    ranking_min_probability_of_profit: float | None = None
    ranking_min_expected_value_dollars: float | None = None
    ranking_min_slippage_adjusted_expected_value_dollars: float | None = None
    ranking_max_entry_slippage_dollars: float | None = None
    ranking_min_model_implied_volatility: float | None = None
    ranking_max_model_implied_volatility: float | None = None
    ranking_weight_probability_of_profit: float | None = None
    ranking_weight_expected_value_dollars: float | None = None
    ranking_weight_slippage_adjusted_expected_value_dollars: float | None = None
    ranking_weight_entry_slippage_dollars: float | None = None
    ranking_weight_model_implied_volatility: float | None = None

    @classmethod
    def from_context(cls, context: Any) -> CandidateBuildParameters:
        defaults = cls()
        return cls(
            symbol=_optional_text(getattr(context, "symbol", defaults.symbol)),
            symbols=_symbol_tuple(getattr(context, "symbols", defaults.symbols)),
            candidate_builder_key=str(getattr(context, "candidate_builder_key", defaults.candidate_builder_key) or defaults.candidate_builder_key),
            build_profile=str(getattr(context, "build_profile", defaults.build_profile) or defaults.build_profile),
            min_dte=_optional_int(getattr(context, "min_dte", defaults.min_dte)),
            max_dte=_optional_int(getattr(context, "max_dte", defaults.max_dte)),
            short_delta_min=_optional_float(getattr(context, "short_delta_min", defaults.short_delta_min)),
            short_delta_max=_optional_float(getattr(context, "short_delta_max", defaults.short_delta_max)),
            short_delta_target=_optional_float(getattr(context, "short_delta_target", defaults.short_delta_target)),
            min_width=_optional_float(getattr(context, "min_width", defaults.min_width)),
            max_width=_optional_float(getattr(context, "max_width", defaults.max_width)),
            min_credit=_optional_float(getattr(context, "min_credit", defaults.min_credit)),
            min_open_interest=_optional_int(getattr(context, "min_open_interest", defaults.min_open_interest)),
            max_relative_spread=_optional_float(getattr(context, "max_relative_spread", defaults.max_relative_spread)),
            min_return_on_risk=_optional_float(getattr(context, "min_return_on_risk", defaults.min_return_on_risk)),
            min_fill_ratio=_optional_float(getattr(context, "min_fill_ratio", defaults.min_fill_ratio)),
            min_short_vs_expected_move_ratio=_optional_float(
                getattr(context, "min_short_vs_expected_move_ratio", defaults.min_short_vs_expected_move_ratio)
            ),
            min_breakeven_vs_expected_move_ratio=_optional_float(
                getattr(context, "min_breakeven_vs_expected_move_ratio", defaults.min_breakeven_vs_expected_move_ratio)
            ),
            max_quote_age_seconds=_optional_int(getattr(context, "max_quote_age_seconds", defaults.max_quote_age_seconds)),
            feed=str(getattr(context, "feed", defaults.feed) or defaults.feed),
            stock_feed=str(getattr(context, "stock_feed", defaults.stock_feed) or defaults.stock_feed),
            greeks_source=str(getattr(context, "greeks_source", defaults.greeks_source) or defaults.greeks_source),
            calendar_policy=str(getattr(context, "calendar_policy", defaults.calendar_policy) or defaults.calendar_policy),
            refresh_calendar_events=bool(getattr(context, "refresh_calendar_events", defaults.refresh_calendar_events)),
            expand_duplicates=bool(getattr(context, "expand_duplicates", defaults.expand_duplicates)),
            setup_filter=str(getattr(context, "setup_filter", defaults.setup_filter) or defaults.setup_filter),
            data_policy=str(getattr(context, "data_policy", defaults.data_policy) or defaults.data_policy),
            calendar_confidence_policy=str(
                getattr(context, "calendar_confidence_policy", defaults.calendar_confidence_policy) or defaults.calendar_confidence_policy
            ),
            top=int(getattr(context, "top", defaults.top) or defaults.top),
            per_symbol_top=int(getattr(context, "per_symbol_top", defaults.per_symbol_top) or defaults.per_symbol_top),
            trading_base_url=_optional_text(getattr(context, "trading_base_url", defaults.trading_base_url)),
            data_base_url=str(getattr(context, "data_base_url", defaults.data_base_url) or defaults.data_base_url),
            history_db=_optional_text(getattr(context, "history_db", defaults.history_db)),
            session_label=_optional_text(getattr(context, "session_label", defaults.session_label)),
            evaluation_timestamp=_optional_text(getattr(context, "evaluation_timestamp", defaults.evaluation_timestamp)),
            evaluation_date=_optional_text(getattr(context, "evaluation_date", defaults.evaluation_date)),
            session_bucket_override=_optional_text(getattr(context, "session_bucket_override", defaults.session_bucket_override)),
            config_root=_optional_text(getattr(context, "config_root", defaults.config_root)),
            **_ranking_value_payload(context),
        )

    def apply_to_context(self, context: Any) -> Any:
        for key, value in self.as_context_payload().items():
            setattr(context, key, value)
        return context

    def as_context_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "candidate_builder_key": self.candidate_builder_key,
            "build_profile": self.build_profile,
            "min_dte": self.min_dte,
            "max_dte": self.max_dte,
            "short_delta_min": self.short_delta_min,
            "short_delta_max": self.short_delta_max,
            "short_delta_target": self.short_delta_target,
            "min_width": self.min_width,
            "max_width": self.max_width,
            "min_credit": self.min_credit,
            "min_open_interest": self.min_open_interest,
            "max_relative_spread": self.max_relative_spread,
            "min_return_on_risk": self.min_return_on_risk,
            "min_fill_ratio": self.min_fill_ratio,
            "min_short_vs_expected_move_ratio": self.min_short_vs_expected_move_ratio,
            "min_breakeven_vs_expected_move_ratio": self.min_breakeven_vs_expected_move_ratio,
            "max_quote_age_seconds": self.max_quote_age_seconds,
            "feed": self.feed,
            "stock_feed": self.stock_feed,
            "greeks_source": self.greeks_source,
            "calendar_policy": self.calendar_policy,
            "refresh_calendar_events": self.refresh_calendar_events,
            "expand_duplicates": self.expand_duplicates,
            "setup_filter": self.setup_filter,
            "data_policy": self.data_policy,
            "calendar_confidence_policy": self.calendar_confidence_policy,
            "top": self.top,
            "per_symbol_top": self.per_symbol_top,
            "trading_base_url": self.trading_base_url,
            "data_base_url": self.data_base_url,
            "history_db": self.history_db,
            "session_label": self.session_label,
            "evaluation_timestamp": self.evaluation_timestamp,
            "evaluation_date": self.evaluation_date,
            "session_bucket_override": self.session_bucket_override,
            "config_root": self.config_root,
            **{key: getattr(self, key) for key in RANKING_POLICY_ARG_KEYS},
        }


@lru_cache(maxsize=4)
def _cached_strategy_configs(config_root: str) -> tuple[Any, ...]:
    return tuple(load_trading_strategies(config_root).values())


def _normalized_strategy_config_root(config_root: str | Path | None = None) -> str:
    return str(default_config_root(config_root))


def _aggregate_ranking_builder_params(
    configs: tuple[Any, ...],
) -> dict[str, float]:
    values_by_key: dict[str, list[float]] = {key: [] for key in RANKING_POLICY_ARG_KEYS}
    for strategy_config in configs:
        for key, value in strategy_config.build.ranking.as_builder_params().items():
            if value is None:
                continue
            values_by_key[key].append(float(value))

    payload: dict[str, float] = {}
    for key, values in values_by_key.items():
        if not values:
            continue
        if key.startswith("ranking_weight_"):
            payload[key] = sum(values) / len(values)
        elif key.startswith("ranking_max_"):
            payload[key] = max(values)
        else:
            payload[key] = min(values)
    return payload


def _config_backed_ranking_builder_params(
    *,
    profile_name: str,
    strategy_family: str,
    config_root: str | Path | None = None,
) -> dict[str, float]:
    configs = tuple(
        strategy_config
        for strategy_config in _cached_strategy_configs(_normalized_strategy_config_root(config_root))
        if strategy_config.enabled and strategy_config.build_profile == profile_name and strategy_config.strategy_family == strategy_family
    )
    return _aggregate_ranking_builder_params(configs)


def resolve_ranking_builder_params(
    *,
    profile_name: str,
    strategy_family: str,
    config_root: str | Path | None = None,
) -> tuple[str, dict[str, float]]:
    normalized_strategy_family = normalize_strategy_family(strategy_family)
    config_backed_params = _config_backed_ranking_builder_params(
        profile_name=profile_name,
        strategy_family=normalized_strategy_family,
        config_root=config_root,
    )
    if config_backed_params:
        return "trading_strategy", config_backed_params
    if normalized_strategy_family not in PROFILE_FALLBACK_RANKING_STRATEGY_FAMILIES:
        raise ValueError(f"No config-backed ranking defaults exist for {normalized_strategy_family} on build profile {profile_name}.")
    return (
        "profile_fallback",
        resolve_ranking_policy(
            profile_name,
            normalized_strategy_family,
        ).as_builder_params(),
    )


def infer_underlying_key(underlying_type: str) -> str:
    return "etf_index_proxy" if underlying_type == "etf_index_proxy" else "single_name_equity"


def resolve_profile_value(override: Any, preset: Any) -> Any:
    return preset if override is None else override


def normalize_calendar_confidence_policy(value: str | None) -> str:
    normalized = str(value or "strict").strip().lower()
    if normalized not in CALENDAR_CONFIDENCE_POLICIES:
        raise ValueError(f"Unsupported calendar confidence policy: {value}")
    return normalized


def apply_candidate_profile_defaults(
    parameters: CandidateBuildParameters,
    underlying_type: str,
    *,
    config_root: str | Path | None = None,
) -> CandidateBuildParameters:
    profile = PROFILE_CONFIGS[parameters.build_profile]
    underlying_key = infer_underlying_key(underlying_type)
    normalized_strategy = normalize_strategy_family(parameters.candidate_builder_key)
    effective_config_root = config_root if config_root is not None else parameters.config_root
    resolved_config_root = (
        _normalized_strategy_config_root(effective_config_root) if effective_config_root not in (None, "") else parameters.config_root
    )
    _ranking_source, ranking_builder_params = resolve_ranking_builder_params(
        profile_name=parameters.build_profile,
        strategy_family=normalized_strategy,
        config_root=resolved_config_root,
    )
    strategy_profile_override = resolve_strategy_profile_override(
        profile_name=parameters.build_profile,
        strategy=normalized_strategy,
    )
    directional_long_defaults = (
        DIRECTIONAL_LONG_DELTA_DEFAULTS.get(parameters.build_profile) if normalized_strategy in {"long_call", "long_put"} else None
    )

    updates: dict[str, Any] = {
        "config_root": resolved_config_root,
        "min_dte": resolve_profile_value(parameters.min_dte, profile.min_dte),
        "max_dte": resolve_profile_value(parameters.max_dte, profile.max_dte),
        "short_delta_min": resolve_profile_value(
            parameters.short_delta_min,
            (
                directional_long_defaults[0]
                if directional_long_defaults is not None
                else (profile.short_delta_min if strategy_profile_override.short_delta_min is None else strategy_profile_override.short_delta_min)
            ),
        ),
        "short_delta_max": resolve_profile_value(
            parameters.short_delta_max,
            (
                directional_long_defaults[1]
                if directional_long_defaults is not None
                else (profile.short_delta_max if strategy_profile_override.short_delta_max is None else strategy_profile_override.short_delta_max)
            ),
        ),
        "short_delta_target": resolve_profile_value(
            parameters.short_delta_target,
            (
                directional_long_defaults[2]
                if directional_long_defaults is not None
                else (
                    profile.short_delta_target
                    if strategy_profile_override.short_delta_target is None
                    else strategy_profile_override.short_delta_target
                )
            ),
        ),
        "min_width": resolve_profile_value(
            parameters.min_width,
            0.0 if normalized_strategy in {"long_call", "long_put", "short_call", "short_put"} else profile.min_width,
        ),
        "max_width": resolve_profile_value(
            parameters.max_width,
            0.0 if normalized_strategy in {"long_call", "long_put", "short_call", "short_put"} else profile.max_width_by_underlying[underlying_key],
        ),
        "min_credit": resolve_profile_value(parameters.min_credit, profile.min_credit),
        "min_open_interest": resolve_profile_value(
            parameters.min_open_interest,
            profile.min_open_interest_by_underlying[underlying_key],
        ),
        "max_relative_spread": resolve_profile_value(
            parameters.max_relative_spread,
            profile.max_relative_spread_by_underlying[underlying_key],
        ),
        "min_return_on_risk": resolve_profile_value(parameters.min_return_on_risk, profile.min_return_on_risk),
        "min_fill_ratio": resolve_profile_value(parameters.min_fill_ratio, profile.min_fill_ratio),
        "min_short_vs_expected_move_ratio": resolve_profile_value(
            parameters.min_short_vs_expected_move_ratio,
            (
                profile.min_short_vs_expected_move_ratio
                if strategy_profile_override.min_short_vs_expected_move_ratio is None
                else strategy_profile_override.min_short_vs_expected_move_ratio
            ),
        ),
        "min_breakeven_vs_expected_move_ratio": resolve_profile_value(
            parameters.min_breakeven_vs_expected_move_ratio,
            (
                profile.min_breakeven_vs_expected_move_ratio
                if strategy_profile_override.min_breakeven_vs_expected_move_ratio is None
                else strategy_profile_override.min_breakeven_vs_expected_move_ratio
            ),
        ),
    }
    for key, value in ranking_builder_params.items():
        updates[key] = resolve_profile_value(getattr(parameters, key), value)
    return replace(parameters, **updates)


def apply_strategy_build_settings(
    parameters: CandidateBuildParameters,
    settings: Any,
) -> CandidateBuildParameters:
    short_delta_target = resolve_profile_value(
        settings.short_delta_target,
        parameters.short_delta_target,
    )
    if short_delta_target is None and settings.short_delta_min is not None and settings.short_delta_max is not None:
        if settings.short_delta_min <= settings.short_delta_max:
            short_delta_target = (float(settings.short_delta_min) + float(settings.short_delta_max)) / 2.0

    updates: dict[str, Any] = {
        "candidate_builder_key": settings.candidate_builder_key,
        "build_profile": settings.build_profile,
        "min_dte": settings.dte_min,
        "max_dte": settings.dte_max,
        "short_delta_min": settings.short_delta_min,
        "short_delta_max": settings.short_delta_max,
        "short_delta_target": short_delta_target,
        "min_open_interest": resolve_profile_value(settings.min_open_interest, parameters.min_open_interest),
        "max_relative_spread": resolve_profile_value(settings.max_leg_spread_pct_mid, parameters.max_relative_spread),
        "min_return_on_risk": resolve_profile_value(settings.min_return_on_risk, parameters.min_return_on_risk),
        "min_fill_ratio": resolve_profile_value(settings.min_fill_ratio, parameters.min_fill_ratio),
        "min_short_vs_expected_move_ratio": resolve_profile_value(
            settings.min_short_vs_expected_move_ratio,
            parameters.min_short_vs_expected_move_ratio,
        ),
        "min_breakeven_vs_expected_move_ratio": resolve_profile_value(
            settings.min_breakeven_vs_expected_move_ratio,
            parameters.min_breakeven_vs_expected_move_ratio,
        ),
        "max_quote_age_seconds": resolve_profile_value(settings.max_quote_age_seconds, parameters.max_quote_age_seconds),
    }
    if settings.width_points:
        updates["min_width"] = min(settings.width_points)
        updates["max_width"] = max(settings.width_points)
    for key in RANKING_POLICY_ARG_KEYS:
        updates[key] = resolve_profile_value(
            settings.ranking_policy.get(key),
            getattr(parameters, key),
        )
    return replace(parameters, **updates)


def validate_candidate_profile_scope(symbol: str, parameters: CandidateBuildParameters, underlying_type: str) -> None:
    if parameters.build_profile != "0dte":
        return
    if underlying_type != "etf_index_proxy":
        raise ValueError("0dte profile is currently limited to ETF/index proxies")
    if symbol.upper() not in ZERO_DTE_ALLOWED_SYMBOLS:
        allowed = ", ".join(ZERO_DTE_ALLOWED_SYMBOLS)
        raise ValueError(f"0dte profile is currently limited to: {allowed}")


def validate_candidate_build_parameters(parameters: CandidateBuildParameters) -> None:
    normalized_strategy = normalize_strategy_family(parameters.candidate_builder_key)
    if parameters.min_dte is None or parameters.max_dte is None or parameters.min_dte < 0 or parameters.max_dte < parameters.min_dte:
        raise ValueError("Expected 0 <= min-dte <= max-dte")
    if (
        parameters.short_delta_min is None
        or parameters.short_delta_max is None
        or parameters.short_delta_min < 0
        or parameters.short_delta_max > 1
        or parameters.short_delta_min > parameters.short_delta_max
    ):
        raise ValueError("Expected 0 <= short-delta-min <= short-delta-max <= 1")
    if (
        parameters.short_delta_target is None
        or parameters.short_delta_target < parameters.short_delta_min
        or parameters.short_delta_target > parameters.short_delta_max
    ):
        raise ValueError("Expected short-delta-target to fall inside the selected delta band")
    if parameters.min_width is None or parameters.max_width is None:
        raise ValueError("Expected min-width and max-width to be resolved")
    if normalized_strategy in {"long_call", "long_put", "short_call", "short_put"}:
        if parameters.min_width < 0:
            raise ValueError("Expected min-width >= 0")
    elif parameters.min_width <= 0:
        raise ValueError("Expected min-width > 0")
    if parameters.max_width < parameters.min_width:
        raise ValueError("Expected max-width >= min-width")
    if parameters.min_credit is None or parameters.min_credit <= 0:
        raise ValueError("Expected min-credit > 0")
    if parameters.min_open_interest is None or parameters.min_open_interest < 0:
        raise ValueError("Expected min-open-interest >= 0")
    if parameters.max_relative_spread is None or parameters.max_relative_spread <= 0:
        raise ValueError("Expected max-relative-spread > 0")
    if parameters.per_symbol_top <= 0:
        raise ValueError("Expected per-symbol-top > 0")
    if parameters.min_fill_ratio is None or parameters.min_fill_ratio <= 0 or parameters.min_fill_ratio > 1.25:
        raise ValueError("Expected min-fill-ratio to be in (0, 1.25]")
    if parameters.min_short_vs_expected_move_ratio is None or not (-1 <= parameters.min_short_vs_expected_move_ratio <= 1):
        raise ValueError("Expected min-short-vs-expected-move-ratio to be between -1 and 1")
    if parameters.min_breakeven_vs_expected_move_ratio is None or not (-1 <= parameters.min_breakeven_vs_expected_move_ratio <= 1):
        raise ValueError("Expected min-breakeven-vs-expected-move-ratio to be between -1 and 1")
    if parameters.ranking_min_probability_of_profit is not None and not (0 <= parameters.ranking_min_probability_of_profit <= 1):
        raise ValueError("Expected ranking-min-probability-of-profit in [0, 1]")
    for key in (
        "ranking_min_expected_value_dollars",
        "ranking_min_slippage_adjusted_expected_value_dollars",
        "ranking_max_entry_slippage_dollars",
        "ranking_min_model_implied_volatility",
        "ranking_max_model_implied_volatility",
        "ranking_weight_probability_of_profit",
        "ranking_weight_expected_value_dollars",
        "ranking_weight_slippage_adjusted_expected_value_dollars",
        "ranking_weight_entry_slippage_dollars",
        "ranking_weight_model_implied_volatility",
    ):
        value = getattr(parameters, key)
        if value is not None and value < 0:
            raise ValueError(f"Expected {key.replace('_', '-')} >= 0")
    if (
        parameters.ranking_min_model_implied_volatility is not None
        and parameters.ranking_max_model_implied_volatility is not None
        and parameters.ranking_max_model_implied_volatility < parameters.ranking_min_model_implied_volatility
    ):
        raise ValueError("Expected ranking-max-model-implied-volatility >= ranking-min-model-implied-volatility")


def resolve_symbol_candidate_build_parameters(
    *,
    symbol: str,
    base_parameters: CandidateBuildParameters,
    settings: Any,
    config_root: str | Path | None = None,
) -> tuple[CandidateBuildParameters, str]:
    normalized_symbol = symbol.upper()
    underlying_type = classify_underlying_type(normalized_symbol)
    parameters = replace(
        base_parameters,
        symbol=normalized_symbol,
        symbols=(normalized_symbol,),
        per_symbol_top=max(int(base_parameters.per_symbol_top or 1), 1),
        top=max(int(base_parameters.top or 10), int(base_parameters.per_symbol_top or 1)),
    )
    parameters = apply_strategy_build_settings(parameters, settings)
    parameters = apply_candidate_profile_defaults(
        parameters,
        underlying_type,
        config_root=config_root,
    )
    validate_candidate_build_parameters(parameters)
    validate_candidate_profile_scope(normalized_symbol, parameters, underlying_type)
    return parameters, underlying_type


def build_market_slice_parameters(
    *,
    symbol: str,
    base_parameters: CandidateBuildParameters,
    runtime_parameters: list[CandidateBuildParameters],
) -> CandidateBuildParameters:
    normalized_symbol = symbol.upper()
    dte_mins = [int(parameters.min_dte) for parameters in runtime_parameters if parameters.min_dte is not None]
    dte_maxes = [int(parameters.max_dte) for parameters in runtime_parameters if parameters.max_dte is not None]
    return replace(
        base_parameters,
        symbol=normalized_symbol,
        symbols=(normalized_symbol,),
        min_dte=min(dte_mins) if dte_mins else base_parameters.min_dte,
        max_dte=max(dte_maxes) if dte_maxes else base_parameters.max_dte,
    )


def build_candidate_filter_payload(parameters: CandidateBuildParameters) -> dict[str, Any]:
    return {
        "candidate_builder": parameters.candidate_builder_key,
        "build_profile": parameters.build_profile,
        "session_label": parameters.session_label,
        "greeks_source": parameters.greeks_source,
        "session_bucket": (candidate_session_bucket(parameters) if parameters.build_profile == "0dte" else None),
        "evaluation_date": parameters.evaluation_date,
        "evaluation_timestamp": parameters.evaluation_timestamp,
        "min_dte": parameters.min_dte,
        "max_dte": parameters.max_dte,
        "short_delta_min": parameters.short_delta_min,
        "short_delta_max": parameters.short_delta_max,
        "short_delta_target": parameters.short_delta_target,
        "min_width": parameters.min_width,
        "max_width": parameters.max_width,
        "min_credit": parameters.min_credit,
        "min_open_interest": parameters.min_open_interest,
        "max_relative_spread": parameters.max_relative_spread,
        "min_return_on_risk": parameters.min_return_on_risk,
        "feed": parameters.feed,
        "stock_feed": parameters.stock_feed,
        "calendar_policy": parameters.calendar_policy,
        "setup_filter": parameters.setup_filter,
        "expand_duplicates": parameters.expand_duplicates,
        "data_policy": parameters.data_policy,
        "calendar_confidence_policy": parameters.calendar_confidence_policy,
        "min_fill_ratio": parameters.min_fill_ratio,
        "min_short_vs_expected_move_ratio": parameters.min_short_vs_expected_move_ratio,
        "min_breakeven_vs_expected_move_ratio": parameters.min_breakeven_vs_expected_move_ratio,
        **{key: getattr(parameters, key) for key in RANKING_POLICY_ARG_KEYS},
    }


__all__ = [
    "CandidateBuildParameters",
    "CALENDAR_CONFIDENCE_POLICIES",
    "DIRECTIONAL_LONG_DELTA_DEFAULTS",
    "PROFILE_FALLBACK_RANKING_STRATEGY_FAMILIES",
    "RANKING_POLICY_ARG_KEYS",
    "apply_candidate_profile_defaults",
    "apply_strategy_build_settings",
    "build_candidate_filter_payload",
    "build_market_slice_parameters",
    "infer_underlying_key",
    "normalize_calendar_confidence_policy",
    "resolve_profile_value",
    "resolve_ranking_builder_params",
    "resolve_symbol_candidate_build_parameters",
    "validate_candidate_build_parameters",
    "validate_candidate_profile_scope",
]
