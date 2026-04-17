from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Any

from core.runtime.config import default_database_url
from core.services.bots import build_collector_scope
from core.services.live_pipelines import build_live_snapshot_label
from core.services.market_dates import NEW_YORK
from core.services.option_structures import normalize_strategy_family
from core.services.scanners.config import parse_args as parse_scanner_args


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll the options scanner intraday and persist live opportunity cycles, selection state, and quote events to Postgres."
    )
    parser.add_argument(
        "--universe",
        default="0dte_core",
        help="Universe preset to scan. Default: 0dte_core",
    )
    parser.add_argument("--symbols", help="Optional comma-separated symbol list.")
    parser.add_argument(
        "--symbols-file", help="Optional file containing one symbol per line."
    )
    parser.add_argument(
        "--strategy",
        default="combined",
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
        help="Strategy mode. Default: combined",
    )
    parser.add_argument(
        "--profile",
        default="0dte",
        choices=("0dte", "micro", "weekly", "swing", "core"),
        help="Scanner profile. Default: 0dte",
    )
    parser.add_argument(
        "--greeks-source",
        default="auto",
        choices=("alpaca", "local", "auto"),
        help="Greeks source mode. Default: auto",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Maximum promotable opportunities to keep per cycle. Default: 10",
    )
    parser.add_argument(
        "--per-symbol-top",
        type=int,
        default=1,
        help="Maximum spreads to keep per symbol before live ranking. Default: 1",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=300,
        help="Polling interval in seconds. Default: 300",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of polling cycles to run. Default: 1",
    )
    parser.add_argument(
        "--quote-capture-seconds",
        type=int,
        default=20,
        help="Seconds of live option quote capture per cycle. Default: 20",
    )
    parser.add_argument(
        "--trade-capture-seconds",
        type=int,
        default=10,
        help="Seconds of live option trade capture per cycle. Default: 10",
    )
    parser.add_argument(
        "--allow-off-hours",
        action="store_true",
        help="Run even outside regular market hours.",
    )
    parser.add_argument(
        "--session-start-offset-minutes",
        type=int,
        default=0,
        help="Minutes relative to the 9:30 ET open when collection can begin. Default: 0",
    )
    parser.add_argument(
        "--session-end-offset-minutes",
        type=int,
        default=0,
        help="Minutes relative to the 4:00 ET close when collection should stop. Default: 0",
    )
    parser.add_argument(
        "--history-db",
        default=default_database_url(),
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def build_collection_args(
    overrides: dict[str, Any] | None = None,
) -> argparse.Namespace:
    args = parse_args([])
    for key, value in (overrides or {}).items():
        setattr(args, key, value)
    return args


def _options_automation_scope(args: argparse.Namespace) -> dict[str, Any]:
    try:
        strategy = str(getattr(args, "strategy", "") or "").strip() or None
        profile = str(getattr(args, "profile", "") or "").strip() or None
        return build_collector_scope(
            scanner_strategy=strategy,
            scanner_profile=profile,
        )
    except Exception as exc:
        print(f"Options automation config unavailable: {exc}")
        return {
            "enabled": False,
            "symbols": (),
            "scanner_strategy": None,
            "scanner_profile": None,
            "entry_runtimes": [],
        }


def _apply_options_automation_overrides(args: argparse.Namespace) -> argparse.Namespace:
    if not bool(getattr(args, "options_automation_enabled", False)):
        setattr(args, "options_automation_scope", {"enabled": False})
        return args
    if not str(getattr(args, "label", "") or "").strip():
        args.label = build_live_snapshot_label(
            universe_label=str(getattr(args, "universe", "0dte_core") or "0dte_core"),
            strategy=str(getattr(args, "strategy", "combined") or "combined"),
            profile=str(getattr(args, "profile", "0dte") or "0dte"),
            greeks_source=str(getattr(args, "greeks_source", "auto") or "auto"),
        )
    scope = _options_automation_scope(args)
    setattr(args, "options_automation_scope", scope)
    if not bool(scope.get("enabled")):
        return args
    symbols = list(scope.get("symbols") or [])
    if symbols:
        args.symbols = ",".join(symbols)
    scanner_strategy = scope.get("scanner_strategy")
    if isinstance(scanner_strategy, str) and scanner_strategy:
        args.strategy = scanner_strategy
    scanner_profile = scope.get("scanner_profile")
    if isinstance(scanner_profile, str) and scanner_profile:
        args.profile = scanner_profile
    return args


def _allowed_scope_symbols(scope: dict[str, Any]) -> set[str]:
    return {str(symbol).upper() for symbol in list(scope.get("symbols") or [])}


def _allowed_scope_families(scope: dict[str, Any]) -> set[str]:
    families: set[str] = set()
    for _bot, automation in list(scope.get("entry_runtimes") or []):
        families.add(str(automation.strategy_config.strategy_family))
    return families


def _filter_scope_candidates(
    symbol_strategy_candidates: dict[str, list[dict[str, Any]]],
    *,
    scope: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    if not bool(scope.get("enabled")):
        return symbol_strategy_candidates
    allowed_symbols = _allowed_scope_symbols(scope)
    allowed_families = _allowed_scope_families(scope)
    filtered: dict[str, list[dict[str, Any]]] = {}
    for symbol, candidates in symbol_strategy_candidates.items():
        if allowed_symbols and str(symbol).upper() not in allowed_symbols:
            continue
        matching = [
            dict(candidate)
            for candidate in candidates
            if normalize_strategy_family(
                candidate.get("strategy_family") or candidate.get("strategy")
            )
            in allowed_families
        ]
        if matching:
            filtered[str(symbol)] = matching
    return filtered


def _filter_scope_rows(
    rows: list[dict[str, Any]],
    *,
    scope: dict[str, Any],
) -> list[dict[str, Any]]:
    if not bool(scope.get("enabled")):
        return rows
    allowed_symbols = _allowed_scope_symbols(scope)
    allowed_families = _allowed_scope_families(scope)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(
            row.get("underlying_symbol")
            or row.get("symbol")
            or row.get("root_symbol")
            or ""
        ).upper()
        if allowed_symbols and symbol not in allowed_symbols:
            continue
        family = normalize_strategy_family(
            row.get("strategy_family") or row.get("strategy")
        )
        if family not in allowed_families:
            continue
        filtered.append(dict(row))
    return filtered


def _merge_runtime_candidate_rows(
    runtime_candidate_rows_by_owner: dict[
        tuple[str, str], dict[str, list[dict[str, Any]]]
    ],
) -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = {}
    for owner_rows in runtime_candidate_rows_by_owner.values():
        for symbol, rows in owner_rows.items():
            merged.setdefault(str(symbol), []).extend(dict(row) for row in rows)
    for symbol, rows in merged.items():
        rows.sort(
            key=lambda row: (
                float(row.get("quality_score") or 0.0),
                float(row.get("return_on_risk") or 0.0),
                str(row.get("underlying_symbol") or symbol),
            ),
            reverse=True,
        )
    return merged


def collection_window_is_open(
    *,
    now: datetime | None = None,
    session_start_offset_minutes: int = 0,
    session_end_offset_minutes: int = 0,
) -> bool:
    current = datetime.now(NEW_YORK) if now is None else now.astimezone(NEW_YORK)
    if current.weekday() >= 5:
        return False
    session_start = current.replace(
        hour=9, minute=30, second=0, microsecond=0
    ) + timedelta(minutes=session_start_offset_minutes)
    session_end = current.replace(
        hour=16, minute=0, second=0, microsecond=0
    ) + timedelta(minutes=session_end_offset_minutes)
    return session_start <= current <= session_end


def build_scanner_args(args: argparse.Namespace) -> argparse.Namespace:
    scanner_args = parse_scanner_args([])
    scanner_args.symbol = None
    scanner_args.symbols = args.symbols
    scanner_args.symbols_file = args.symbols_file
    scanner_args.universe = args.universe
    scanner_args.strategy = args.strategy
    scanner_args.profile = args.profile
    scanner_args.greeks_source = args.greeks_source
    scanner_args.top = args.top
    scanner_args.per_symbol_top = args.per_symbol_top
    scanner_args.output = None
    scanner_args.json = False
    scanner_args.show_order_json = False
    scanner_args.stream_live_quotes = False
    return scanner_args
