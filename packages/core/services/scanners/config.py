from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from core.domain.profiles import (
    DEFAULT_BOARD_UNIVERSE,
    LONG_VOL_STRATEGIES,
    PROFILE_CONFIGS,
    UNIVERSE_PRESETS,
    ZERO_DTE_ALLOWED_SYMBOLS,
    zero_dte_session_bucket,
)
from core.integrations.alpaca.client import DEFAULT_DATA_BASE_URL
from core.integrations.calendar_events import classify_underlying_type
from core.runtime.config import default_database_url
from core.services.option_structures import normalize_strategy_family


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
        choices=tuple(sorted(UNIVERSE_PRESETS)),
        help="Use a curated symbol preset for multi-symbol scanning.",
    )
    parser.add_argument(
        "--strategy",
        default="call_credit",
        choices=(
            "call_credit",
            "put_credit",
            "call_debit",
            "put_debit",
            "long_straddle",
            "long_strangle",
            "iron_condor",
            "combined",
        ),
        help=(
            "Options structure strategy. Use combined to evaluate both call and put credit spreads. "
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

    if args.universe:
        symbols.extend(UNIVERSE_PRESETS[args.universe])
        label = args.universe
    if args.symbols:
        symbols.extend(
            [
                token.strip().upper()
                for token in args.symbols.split(",")
                if token.strip()
            ]
        )
        if args.symbols.strip():
            label = "custom_symbols"
    if args.symbols_file:
        symbols.extend(load_symbols_file(args.symbols_file))
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


def concrete_strategies(strategy: str) -> tuple[str, ...]:
    if strategy == "combined":
        return ("call_credit", "put_credit")
    return (strategy,)


def strategy_display_label(strategy: str) -> str:
    return {
        "call_credit": "Call Credit",
        "put_credit": "Put Credit",
        "call_debit": "Call Debit",
        "put_debit": "Put Debit",
        "long_straddle": "Long Straddle",
        "long_strangle": "Long Strangle",
        "iron_condor": "Iron Condor",
        "combined": "Combined",
    }.get(strategy, strategy)


def strategy_option_type(strategy: str) -> str:
    normalized = normalize_strategy_family(strategy)
    if normalized in {"call_credit_spread", "call_debit_spread", "long_call"}:
        return "call"
    if normalized in {"put_credit_spread", "put_debit_spread", "long_put"}:
        return "put"
    return "call"


def strategy_direction(strategy: str) -> str:
    normalized = normalize_strategy_family(strategy)
    if normalized in {"put_credit_spread", "call_debit_spread", "long_call"}:
        return "bullish"
    if normalized in {"call_credit_spread", "put_debit_spread", "long_put"}:
        return "bearish"
    if normalized in LONG_VOL_STRATEGIES or normalized == "iron_condor":
        return "neutral"
    return "unknown"


def infer_underlying_key(underlying_type: str) -> str:
    return (
        "etf_index_proxy"
        if underlying_type == "etf_index_proxy"
        else "single_name_equity"
    )


def resolve_profile_value(override: Any, preset: Any) -> Any:
    return preset if override is None else override


def apply_profile_defaults(args: argparse.Namespace, underlying_type: str) -> None:
    profile = PROFILE_CONFIGS[args.profile]
    underlying_key = infer_underlying_key(underlying_type)

    args.min_dte = resolve_profile_value(args.min_dte, profile.min_dte)
    args.max_dte = resolve_profile_value(args.max_dte, profile.max_dte)
    args.short_delta_min = resolve_profile_value(
        args.short_delta_min, profile.short_delta_min
    )
    args.short_delta_max = resolve_profile_value(
        args.short_delta_max, profile.short_delta_max
    )
    args.short_delta_target = resolve_profile_value(
        args.short_delta_target, profile.short_delta_target
    )
    args.min_width = resolve_profile_value(args.min_width, profile.min_width)
    args.max_width = resolve_profile_value(
        args.max_width, profile.max_width_by_underlying[underlying_key]
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
        profile.min_short_vs_expected_move_ratio,
    )
    args.min_breakeven_vs_expected_move_ratio = resolve_profile_value(
        args.min_breakeven_vs_expected_move_ratio,
        profile.min_breakeven_vs_expected_move_ratio,
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
        "session_bucket": zero_dte_session_bucket() if args.profile == "0dte" else None,
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
    }


def clone_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(**vars(args))


def validate_resolved_args(args: argparse.Namespace) -> None:
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
    if args.min_width <= 0:
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
    "apply_profile_defaults",
    "build_filter_payload",
    "clone_args",
    "concrete_strategies",
    "infer_underlying_key",
    "load_symbols_file",
    "parse_args",
    "resolve_profile_value",
    "resolve_symbol_scan_args",
    "resolve_symbols",
    "strategy_direction",
    "strategy_display_label",
    "strategy_option_type",
    "validate_profile_scope",
    "validate_resolved_args",
]
