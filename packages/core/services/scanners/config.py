from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from functools import lru_cache
import os
from pathlib import Path
from typing import Any

from core.domain.profiles import (
    DEFAULT_BOARD_UNIVERSE,
    PROFILE_CONFIGS,
    UNIVERSE_PRESETS,
    ZERO_DTE_ALLOWED_SYMBOLS,
    resolve_ranking_policy,
    resolve_strategy_profile_override,
    zero_dte_session_bucket,
)
from core.integrations.alpaca.client import DEFAULT_DATA_BASE_URL
from core.integrations.calendar_events import classify_underlying_type
from core.runtime.config import default_database_url
from core.services.automations import load_universe_symbols
from core.services.market_dates import NEW_YORK
from core.services.option_structures import normalize_strategy_family
from core.services.strategy_specs import (
    concrete_strategies,
    strategy_direction,
    strategy_display_label,
    strategy_option_type,
)
from core.services.strategy_configs import default_config_root, load_strategy_configs

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


@lru_cache(maxsize=4)
def _cached_strategy_configs(config_root: str) -> tuple[Any, ...]:
    return tuple(load_strategy_configs(config_root).values())


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
) -> dict[str, float]:
    configs = tuple(
        strategy_config
        for strategy_config in _cached_strategy_configs(str(default_config_root()))
        if strategy_config.enabled
        and strategy_config.scanner_profile == profile_name
        and strategy_config.strategy_family == strategy_family
    )
    return _aggregate_ranking_builder_params(configs)


def resolve_ranking_builder_params(
    *,
    profile_name: str,
    strategy_family: str,
) -> tuple[str, dict[str, float]]:
    normalized_strategy_family = normalize_strategy_family(strategy_family)
    config_backed_params = _config_backed_ranking_builder_params(
        profile_name=profile_name,
        strategy_family=normalized_strategy_family,
    )
    if config_backed_params:
        return "strategy_config", config_backed_params
    if normalized_strategy_family not in PROFILE_FALLBACK_RANKING_STRATEGY_FAMILIES:
        raise ValueError(
            "No config-backed ranking defaults exist for "
            f"{normalized_strategy_family} on profile {profile_name}."
        )
    return (
        "profile_fallback",
        resolve_ranking_policy(
            profile_name,
            normalized_strategy_family,
        ).as_builder_params(),
    )


def available_universe_labels() -> tuple[str, ...]:
    labels = set(UNIVERSE_PRESETS)
    root = default_config_root() / "universes"
    if root.exists():
        labels.update(path.stem for path in root.glob("*.yaml"))
    return tuple(sorted(labels))


def resolve_universe_symbols(universe_label: str) -> tuple[str, ...]:
    normalized = str(universe_label or "").strip()
    if not normalized:
        return ()
    preset = UNIVERSE_PRESETS.get(normalized)
    if preset is not None:
        return preset
    return load_universe_symbols(normalized)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find vertical spread candidates for one symbol or a ranked multi-symbol universe using Alpaca."
    )
    parser.add_argument("--symbol", help="Scan a single underlying.")
    parser.add_argument(
        "--symbols",
        help="Comma-separated list of underlyings to scan as a ranked universe.",
    )
    parser.add_argument(
        "--symbols-file",
        help="Optional file containing one symbol per line for universe scanning.",
    )
    parser.add_argument(
        "--universe",
        choices=available_universe_labels(),
        help="Use a curated symbol preset for multi-symbol scanning.",
    )
    parser.add_argument(
        "--strategy",
        default="call_credit",
        choices=(
            "auto",
            "call_credit",
            "put_credit",
            "call_debit",
            "put_debit",
            "long_call",
            "long_put",
            "short_call",
            "short_put",
            "long_straddle",
            "long_strangle",
            "iron_condor",
            "combined",
        ),
        help=(
            "Options structure strategy. Use auto to evaluate the supported manual-scan families, "
            "or combined to evaluate both call and put credit spreads. "
            "Default: call_credit"
        ),
    )
    parser.add_argument(
        "--profile",
        default="core",
        choices=("0dte", "micro", "weekly", "swing", "core"),
        help="Scanner profile preset. Default: core",
    )
    parser.add_argument(
        "--min-dte",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--short-delta-min",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--short-delta-max",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--short-delta-target",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-width",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--max-width",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-credit",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-open-interest",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--max-relative-spread",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-return-on-risk",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--feed",
        default="opra",
        choices=("opra", "indicative"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--stock-feed",
        default="sip",
        choices=("sip", "iex", "delayed_sip", "boats", "overnight"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of candidates to print. Default: 10",
    )
    parser.add_argument(
        "--per-symbol-top",
        type=int,
        default=1,
        help="Maximum number of ranked spreads to keep per symbol in universe mode. Default: 1",
    )
    parser.add_argument(
        "--trading-base-url",
        default=os.environ.get("ALPACA_TRADING_BASE_URL"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--data-base-url",
        default=os.environ.get("ALPACA_DATA_BASE_URL", DEFAULT_DATA_BASE_URL),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--output",
        help="Output file path. Default: strategy-specific outputs directory",
    )
    parser.add_argument(
        "--output-format",
        default="csv",
        choices=("csv", "json"),
        help="Output file format. Default: csv",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of a human-readable summary.",
    )
    parser.add_argument(
        "--show-order-json",
        action="store_true",
        help="Print a sample Alpaca multi-leg order payload for each result.",
    )
    parser.add_argument(
        "--greeks-source",
        default="auto",
        choices=("alpaca", "local", "auto"),
        help="Greeks source mode. Default: auto",
    )
    parser.add_argument(
        "--stream-live-quotes",
        action="store_true",
        help="After printing results, stream fresh Alpaca option quotes for the displayed legs.",
    )
    parser.add_argument(
        "--calendar-policy",
        default="strict",
        choices=("strict", "warn", "off"),
        help="Calendar event mode. Default: strict",
    )
    parser.add_argument(
        "--refresh-calendar-events",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--expand-duplicates",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--setup-filter",
        default="on",
        choices=("on", "off"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--data-policy",
        default="strict",
        choices=("strict", "warn", "off"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-fill-ratio",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-short-vs-expected-move-ratio",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--min-breakeven-vs-expected-move-ratio",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-min-probability-of-profit",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-min-expected-value-dollars",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-min-slippage-adjusted-expected-value-dollars",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-max-entry-slippage-dollars",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-min-model-implied-volatility",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-max-model-implied-volatility",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-weight-probability-of-profit",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-weight-expected-value-dollars",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-weight-slippage-adjusted-expected-value-dollars",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-weight-entry-slippage-dollars",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ranking-weight-model-implied-volatility",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--history-db",
        default=default_database_url(),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--session-label",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--backtest-latest",
        action="store_true",
        help="Backtest the most recent stored run for the selected symbol instead of scanning live.",
    )
    parser.add_argument(
        "--backtest-run-id",
        help="Backtest a specific stored run id instead of scanning live.",
    )
    parser.add_argument(
        "--backtest-profit-target",
        type=float,
        default=0.50,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--backtest-stop-multiple",
        type=float,
        default=2.0,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--stream-seconds",
        type=float,
        default=8.0,
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def load_symbols_file(path: str) -> list[str]:
    symbols: list[str] = []
    for raw_line in Path(path).read_text().splitlines():
        line = raw_line.strip().upper()
        if not line or line.startswith("#"):
            continue
        symbols.append(line)
    return symbols


def resolve_symbols(args: argparse.Namespace) -> tuple[list[str], str]:
    symbols: list[str] = []
    default_label = "0dte_core" if args.profile == "0dte" else DEFAULT_BOARD_UNIVERSE
    label = args.symbol.upper() if args.symbol else default_label
    universe_symbols: tuple[str, ...] = ()

    if args.universe:
        universe_symbols = resolve_universe_symbols(str(args.universe))
        symbols.extend(universe_symbols)
        label = args.universe
    if args.symbols:
        explicit_symbols = [
            token.strip().upper() for token in args.symbols.split(",") if token.strip()
        ]
        symbols.extend(explicit_symbols)
        if explicit_symbols and (
            not args.universe
            or {symbol.upper() for symbol in explicit_symbols}
            != {symbol.upper() for symbol in universe_symbols}
        ):
            label = "custom_symbols"
    if args.symbols_file:
        file_symbols = load_symbols_file(args.symbols_file)
        symbols.extend(file_symbols)
        if not args.universe or {symbol.upper() for symbol in file_symbols} != {
            symbol.upper() for symbol in universe_symbols
        }:
            label = Path(args.symbols_file).stem.lower()

    if args.symbol:
        symbols.append(args.symbol.upper())
        label = args.symbol.upper()

    if not symbols:
        return list(UNIVERSE_PRESETS[default_label]), default_label

    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(symbol)
    return deduped, label

def infer_underlying_key(underlying_type: str) -> str:
    return (
        "etf_index_proxy"
        if underlying_type == "etf_index_proxy"
        else "single_name_equity"
    )


def resolve_profile_value(override: Any, preset: Any) -> Any:
    return preset if override is None else override


def _coerce_evaluation_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def resolve_scan_reference_datetime(args: argparse.Namespace) -> datetime | None:
    return _coerce_evaluation_datetime(getattr(args, "evaluation_timestamp", None))


def resolve_scan_reference_date(args: argparse.Namespace) -> date:
    reference_datetime = resolve_scan_reference_datetime(args)
    if reference_datetime is not None:
        return reference_datetime.astimezone(NEW_YORK).date()
    raw_date = getattr(args, "evaluation_date", None)
    if raw_date not in (None, ""):
        if isinstance(raw_date, date):
            return raw_date
        return date.fromisoformat(str(raw_date))
    return date.today()


def resolve_scan_session_bucket(args: argparse.Namespace) -> str | None:
    override = getattr(args, "session_bucket_override", None)
    if override not in (None, ""):
        return str(override)
    reference_datetime = resolve_scan_reference_datetime(args)
    if reference_datetime is not None:
        return zero_dte_session_bucket(reference_datetime)
    return zero_dte_session_bucket()


def apply_scan_evaluation_context(
    args: argparse.Namespace,
    *,
    evaluation_timestamp: datetime | str | None = None,
    evaluation_date: date | str | None = None,
    session_bucket: str | None = None,
) -> argparse.Namespace:
    if evaluation_timestamp is not None:
        resolved_timestamp = _coerce_evaluation_datetime(evaluation_timestamp)
        if resolved_timestamp is not None:
            args.evaluation_timestamp = resolved_timestamp.isoformat()
    if evaluation_date is not None:
        args.evaluation_date = (
            evaluation_date.isoformat()
            if isinstance(evaluation_date, date)
            else str(evaluation_date)
        )
    if session_bucket is not None:
        args.session_bucket_override = str(session_bucket)
    return args


def apply_profile_defaults(args: argparse.Namespace, underlying_type: str) -> None:
    profile = PROFILE_CONFIGS[args.profile]
    underlying_key = infer_underlying_key(underlying_type)
    normalized_strategy = normalize_strategy_family(args.strategy)
    _ranking_source, ranking_builder_params = resolve_ranking_builder_params(
        profile_name=args.profile,
        strategy_family=normalized_strategy,
    )
    strategy_profile_override = resolve_strategy_profile_override(
        profile_name=args.profile,
        strategy=normalized_strategy,
    )
    directional_long_defaults = (
        DIRECTIONAL_LONG_DELTA_DEFAULTS.get(args.profile)
        if normalized_strategy in {"long_call", "long_put"}
        else None
    )

    args.min_dte = resolve_profile_value(args.min_dte, profile.min_dte)
    args.max_dte = resolve_profile_value(args.max_dte, profile.max_dte)
    args.short_delta_min = resolve_profile_value(
        args.short_delta_min,
        (
            directional_long_defaults[0]
            if directional_long_defaults is not None
            else (
                profile.short_delta_min
                if strategy_profile_override.short_delta_min is None
                else strategy_profile_override.short_delta_min
            )
        ),
    )
    args.short_delta_max = resolve_profile_value(
        args.short_delta_max,
        (
            directional_long_defaults[1]
            if directional_long_defaults is not None
            else (
                profile.short_delta_max
                if strategy_profile_override.short_delta_max is None
                else strategy_profile_override.short_delta_max
            )
        ),
    )
    args.short_delta_target = resolve_profile_value(
        args.short_delta_target,
        (
            directional_long_defaults[2]
            if directional_long_defaults is not None
            else (
                profile.short_delta_target
                if strategy_profile_override.short_delta_target is None
                else strategy_profile_override.short_delta_target
            )
        ),
    )
    args.min_width = resolve_profile_value(
        args.min_width,
        0.0
        if normalized_strategy in {"long_call", "long_put", "short_call", "short_put"}
        else profile.min_width,
    )
    args.max_width = resolve_profile_value(
        args.max_width,
        0.0
        if normalized_strategy in {"long_call", "long_put", "short_call", "short_put"}
        else profile.max_width_by_underlying[underlying_key],
    )
    args.min_credit = resolve_profile_value(args.min_credit, profile.min_credit)
    args.min_open_interest = resolve_profile_value(
        args.min_open_interest,
        profile.min_open_interest_by_underlying[underlying_key],
    )
    args.max_relative_spread = resolve_profile_value(
        args.max_relative_spread,
        profile.max_relative_spread_by_underlying[underlying_key],
    )
    args.min_return_on_risk = resolve_profile_value(
        args.min_return_on_risk, profile.min_return_on_risk
    )
    args.min_fill_ratio = resolve_profile_value(
        args.min_fill_ratio, profile.min_fill_ratio
    )
    args.min_short_vs_expected_move_ratio = resolve_profile_value(
        args.min_short_vs_expected_move_ratio,
        (
            profile.min_short_vs_expected_move_ratio
            if strategy_profile_override.min_short_vs_expected_move_ratio is None
            else strategy_profile_override.min_short_vs_expected_move_ratio
        ),
    )
    args.min_breakeven_vs_expected_move_ratio = resolve_profile_value(
        args.min_breakeven_vs_expected_move_ratio,
        (
            profile.min_breakeven_vs_expected_move_ratio
            if strategy_profile_override.min_breakeven_vs_expected_move_ratio is None
            else strategy_profile_override.min_breakeven_vs_expected_move_ratio
        ),
    )
    for key, value in ranking_builder_params.items():
        setattr(
            args,
            key,
            resolve_profile_value(getattr(args, key, None), value),
        )


def validate_profile_scope(
    symbol: str, args: argparse.Namespace, underlying_type: str
) -> None:
    if args.profile != "0dte":
        return
    if underlying_type != "etf_index_proxy":
        raise SystemExit("0dte profile is currently limited to ETF/index proxies")
    if symbol.upper() not in ZERO_DTE_ALLOWED_SYMBOLS:
        allowed = ", ".join(ZERO_DTE_ALLOWED_SYMBOLS)
        raise SystemExit(f"0dte profile is currently limited to: {allowed}")


def build_filter_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "strategy": args.strategy,
        "profile": args.profile,
        "session_label": getattr(args, "session_label", None),
        "greeks_source": args.greeks_source,
        "session_bucket": (
            resolve_scan_session_bucket(args) if args.profile == "0dte" else None
        ),
        "evaluation_date": getattr(args, "evaluation_date", None),
        "evaluation_timestamp": getattr(args, "evaluation_timestamp", None),
        "min_dte": args.min_dte,
        "max_dte": args.max_dte,
        "short_delta_min": args.short_delta_min,
        "short_delta_max": args.short_delta_max,
        "short_delta_target": args.short_delta_target,
        "min_width": args.min_width,
        "max_width": args.max_width,
        "min_credit": args.min_credit,
        "min_open_interest": args.min_open_interest,
        "max_relative_spread": args.max_relative_spread,
        "min_return_on_risk": args.min_return_on_risk,
        "feed": args.feed,
        "stock_feed": args.stock_feed,
        "calendar_policy": args.calendar_policy,
        "setup_filter": args.setup_filter,
        "expand_duplicates": args.expand_duplicates,
        "data_policy": args.data_policy,
        "min_fill_ratio": args.min_fill_ratio,
        "min_short_vs_expected_move_ratio": args.min_short_vs_expected_move_ratio,
        "min_breakeven_vs_expected_move_ratio": args.min_breakeven_vs_expected_move_ratio,
        "ranking_min_probability_of_profit": args.ranking_min_probability_of_profit,
        "ranking_min_expected_value_dollars": args.ranking_min_expected_value_dollars,
        "ranking_min_slippage_adjusted_expected_value_dollars": (
            args.ranking_min_slippage_adjusted_expected_value_dollars
        ),
        "ranking_max_entry_slippage_dollars": args.ranking_max_entry_slippage_dollars,
        "ranking_min_model_implied_volatility": (
            args.ranking_min_model_implied_volatility
        ),
        "ranking_max_model_implied_volatility": (
            args.ranking_max_model_implied_volatility
        ),
        "ranking_weight_probability_of_profit": (
            args.ranking_weight_probability_of_profit
        ),
        "ranking_weight_expected_value_dollars": (
            args.ranking_weight_expected_value_dollars
        ),
        "ranking_weight_slippage_adjusted_expected_value_dollars": (
            args.ranking_weight_slippage_adjusted_expected_value_dollars
        ),
        "ranking_weight_entry_slippage_dollars": (
            args.ranking_weight_entry_slippage_dollars
        ),
        "ranking_weight_model_implied_volatility": (
            args.ranking_weight_model_implied_volatility
        ),
    }


def clone_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(**vars(args))


def validate_resolved_args(args: argparse.Namespace) -> None:
    normalized_strategy = normalize_strategy_family(args.strategy)
    if args.min_dte < 0 or args.max_dte < args.min_dte:
        raise SystemExit("Expected 0 <= min-dte <= max-dte")
    if (
        args.short_delta_min < 0
        or args.short_delta_max > 1
        or args.short_delta_min > args.short_delta_max
    ):
        raise SystemExit("Expected 0 <= short-delta-min <= short-delta-max <= 1")
    if (
        args.short_delta_target < args.short_delta_min
        or args.short_delta_target > args.short_delta_max
    ):
        raise SystemExit(
            "Expected short-delta-target to fall inside the selected delta band"
        )
    if normalized_strategy in {"long_call", "long_put", "short_call", "short_put"}:
        if args.min_width < 0:
            raise SystemExit("Expected min-width >= 0")
    elif args.min_width <= 0:
        raise SystemExit("Expected min-width > 0")
    if args.max_width < args.min_width:
        raise SystemExit("Expected max-width >= min-width")
    if args.min_credit <= 0:
        raise SystemExit("Expected min-credit > 0")
    if args.min_open_interest < 0:
        raise SystemExit("Expected min-open-interest >= 0")
    if args.max_relative_spread <= 0:
        raise SystemExit("Expected max-relative-spread > 0")
    if args.per_symbol_top <= 0:
        raise SystemExit("Expected per-symbol-top > 0")
    if args.min_fill_ratio <= 0 or args.min_fill_ratio > 1.25:
        raise SystemExit("Expected min-fill-ratio to be in (0, 1.25]")
    if (
        args.min_short_vs_expected_move_ratio < -1
        or args.min_short_vs_expected_move_ratio > 1
    ):
        raise SystemExit(
            "Expected min-short-vs-expected-move-ratio to be between -1 and 1"
        )
    if (
        args.min_breakeven_vs_expected_move_ratio < -1
        or args.min_breakeven_vs_expected_move_ratio > 1
    ):
        raise SystemExit(
            "Expected min-breakeven-vs-expected-move-ratio to be between -1 and 1"
        )
    if (
        args.ranking_min_probability_of_profit is not None
        and (
            args.ranking_min_probability_of_profit < 0
            or args.ranking_min_probability_of_profit > 1
        )
    ):
        raise SystemExit("Expected ranking-min-probability-of-profit in [0, 1]")
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
        value = getattr(args, key, None)
        if value is not None and value < 0:
            raise SystemExit(f"Expected {key.replace('_', '-')} >= 0")
    if (
        args.ranking_min_model_implied_volatility is not None
        and args.ranking_max_model_implied_volatility is not None
        and args.ranking_max_model_implied_volatility
        < args.ranking_min_model_implied_volatility
    ):
        raise SystemExit(
            "Expected ranking-max-model-implied-volatility >= ranking-min-model-implied-volatility"
        )


def resolve_symbol_scan_args(
    *, symbol: str, base_args: argparse.Namespace
) -> tuple[argparse.Namespace, str]:
    normalized_symbol = symbol.upper()
    underlying_type = classify_underlying_type(normalized_symbol)
    symbol_args = clone_args(base_args)
    symbol_args.symbol = normalized_symbol
    apply_profile_defaults(symbol_args, underlying_type)
    validate_resolved_args(symbol_args)
    validate_profile_scope(normalized_symbol, symbol_args, underlying_type)
    return symbol_args, underlying_type


__all__ = [
    "PROFILE_FALLBACK_RANKING_STRATEGY_FAMILIES",
    "apply_profile_defaults",
    "apply_scan_evaluation_context",
    "build_filter_payload",
    "clone_args",
    "concrete_strategies",
    "infer_underlying_key",
    "load_symbols_file",
    "parse_args",
    "RANKING_POLICY_ARG_KEYS",
    "resolve_profile_value",
    "resolve_ranking_builder_params",
    "resolve_scan_reference_date",
    "resolve_scan_reference_datetime",
    "resolve_scan_session_bucket",
    "resolve_symbol_scan_args",
    "resolve_symbols",
    "strategy_direction",
    "strategy_display_label",
    "strategy_option_type",
    "validate_profile_scope",
    "validate_resolved_args",
]
