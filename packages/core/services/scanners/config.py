from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from core.domain.profiles import (
    DEFAULT_BOARD_UNIVERSE,
    UNIVERSE_PRESETS,
)
from core.integrations.alpaca.client import DEFAULT_DATA_BASE_URL
from core.integrations.calendar_events import classify_underlying_type
from core.runtime.config import default_database_url
from core.services.strategy_specs import (
    concrete_strategies,
    strategy_direction,
    strategy_display_label,
    strategy_option_type,
)
from core.services.strategy_candidate_builders.settings import (
    PROFILE_FALLBACK_RANKING_STRATEGY_FAMILIES,
    RANKING_POLICY_ARG_KEYS,
    CandidateBuildParameters,
    apply_candidate_profile_defaults,
    build_candidate_filter_payload,
    infer_underlying_key,
    resolve_profile_value,
    resolve_ranking_builder_params,
    validate_candidate_build_parameters,
    validate_candidate_profile_scope,
)
from core.services.trading_strategies import default_config_root, load_universe_symbols

CALENDAR_CONFIDENCE_POLICIES = ("strict", "consensus", "off")


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


def normalize_calendar_confidence_policy(value: str | None) -> str:
    normalized = str(value or "strict").strip().lower()
    if normalized not in CALENDAR_CONFIDENCE_POLICIES:
        raise ValueError(f"Unsupported calendar confidence policy: {value}")
    return normalized


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find vertical spread candidates for one symbol or a ranked multi-symbol universe using Alpaca.")
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
        "--calendar-confidence-policy",
        default="strict",
        choices=CALENDAR_CONFIDENCE_POLICIES,
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
        explicit_symbols = [token.strip().upper() for token in args.symbols.split(",") if token.strip()]
        symbols.extend(explicit_symbols)
        if explicit_symbols and (
            not args.universe or {symbol.upper() for symbol in explicit_symbols} != {symbol.upper() for symbol in universe_symbols}
        ):
            label = "custom_symbols"
    if args.symbols_file:
        file_symbols = load_symbols_file(args.symbols_file)
        symbols.extend(file_symbols)
        if not args.universe or {symbol.upper() for symbol in file_symbols} != {symbol.upper() for symbol in universe_symbols}:
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


def apply_profile_defaults(
    args: argparse.Namespace,
    underlying_type: str,
    *,
    config_root: str | Path | None = None,
) -> None:
    parameters = apply_candidate_profile_defaults(
        CandidateBuildParameters.from_context(args),
        underlying_type,
        config_root=config_root,
    )
    parameters.apply_to_context(args)


def validate_profile_scope(symbol: str, args: argparse.Namespace, underlying_type: str) -> None:
    try:
        validate_candidate_profile_scope(
            symbol,
            CandidateBuildParameters.from_context(args),
            underlying_type,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def build_filter_payload(args: argparse.Namespace) -> dict[str, Any]:
    return build_candidate_filter_payload(CandidateBuildParameters.from_context(args))


def clone_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(**vars(args))


def validate_resolved_args(args: argparse.Namespace) -> None:
    try:
        validate_candidate_build_parameters(CandidateBuildParameters.from_context(args))
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def resolve_symbol_scan_args(
    *,
    symbol: str,
    base_args: argparse.Namespace,
    config_root: str | Path | None = None,
) -> tuple[argparse.Namespace, str]:
    normalized_symbol = symbol.upper()
    underlying_type = classify_underlying_type(normalized_symbol)
    symbol_args = clone_args(base_args)
    symbol_args.symbol = normalized_symbol
    apply_profile_defaults(
        symbol_args,
        underlying_type,
        config_root=config_root,
    )
    validate_resolved_args(symbol_args)
    validate_profile_scope(normalized_symbol, symbol_args, underlying_type)
    return symbol_args, underlying_type


__all__ = [
    "PROFILE_FALLBACK_RANKING_STRATEGY_FAMILIES",
    "apply_profile_defaults",
    "build_filter_payload",
    "clone_args",
    "concrete_strategies",
    "infer_underlying_key",
    "load_symbols_file",
    "parse_args",
    "RANKING_POLICY_ARG_KEYS",
    "resolve_profile_value",
    "resolve_ranking_builder_params",
    "resolve_symbol_scan_args",
    "resolve_symbols",
    "strategy_direction",
    "strategy_display_label",
    "strategy_option_type",
    "validate_profile_scope",
    "validate_resolved_args",
]
