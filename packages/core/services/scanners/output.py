from __future__ import annotations

import argparse
import csv
import json
import shutil
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from core.domain.models import (
    LiveOptionQuote,
    SpreadCandidate,
    SymbolScanResult,
    UnderlyingSetupContext,
    UniverseScanFailure,
)
from core.domain.profiles import format_session_bucket, zero_dte_session_bucket
from core.integrations.alpaca.client import AlpacaClient
from core.integrations.alpaca.streaming import AlpacaOptionQuoteStreamer
from core.services.option_structures import candidate_legs, structure_quote_snapshot
from core.services.scanners.config import build_filter_payload, strategy_display_label
from core.services.scanners.setup import serialize_setup_context


def build_setup_summaries(results: list[SymbolScanResult]) -> tuple[str, ...]:
    summaries: list[str] = []
    for result in results:
        if result.setup is None:
            continue
        summaries.append(
            f"{result.args.strategy} {result.setup.status} ({result.setup.score:.1f})"
        )
    return tuple(summaries)


def default_output_path(symbol: str, strategy: str, output_format: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    directory = {
        "call_credit": "call_credit_spreads",
        "put_credit": "put_credit_spreads",
        "call_debit": "call_debit_spreads",
        "put_debit": "put_debit_spreads",
        "long_straddle": "long_straddles",
        "long_strangle": "long_strangles",
        "iron_condor": "iron_condors",
        "combined": "combined_credit_spreads",
    }.get(strategy, "call_credit_spreads")
    return str(
        Path("outputs") / directory / f"{symbol.lower()}_{timestamp}.{output_format}"
    )


def default_universe_output_path(label: str, strategy: str, output_format: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_label = label.lower().replace(" ", "_")
    return str(
        Path("outputs")
        / "universe_boards"
        / f"{safe_label}_{strategy}_{timestamp}.{output_format}"
    )


def write_latest_copy(output_path: str, latest_name: str) -> str:
    latest_path = str(Path(output_path).with_name(latest_name))
    shutil.copyfile(output_path, latest_path)
    return latest_path


def format_dte_label(days_to_expiration: int) -> str:
    return "0D" if days_to_expiration == 0 else str(days_to_expiration)


def build_table_rows(
    candidates: list[SpreadCandidate], *, include_strategy: bool = False
) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        row = []
        if include_strategy:
            row.append(strategy_display_label(candidate.strategy))
        row.extend(
            [
                candidate.expiration_date,
                format_dte_label(candidate.days_to_expiration),
                f"{candidate.short_strike:.2f}",
                f"{candidate.long_strike:.2f}",
                f"{candidate.width:.2f}",
                f"{candidate.midpoint_credit:.2f}",
                f"{candidate.return_on_risk * 100:.1f}",
                f"{candidate.quality_score:.1f}",
                "n/a"
                if candidate.short_delta is None
                else f"{candidate.short_delta:.2f}",
                f"{candidate.short_otm_pct * 100:.1f}",
                f"{candidate.breakeven_cushion_pct * 100:.1f}",
                "n/a"
                if candidate.short_vs_expected_move is None
                else f"{candidate.short_vs_expected_move:.2f}",
                f"{min(candidate.short_open_interest, candidate.long_open_interest)}",
                candidate.calendar_status,
                candidate.data_status,
                "n/a"
                if candidate.calendar_days_to_nearest_event is None
                else str(candidate.calendar_days_to_nearest_event),
            ]
        )
        rows.append(row)
    return rows


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt_row(row: list[str]) -> str:
        return " | ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(row))

    separator = "-+-".join("-" * width for width in widths)
    rendered = [fmt_row(headers), separator]
    rendered.extend(fmt_row(row) for row in rows)
    return "\n".join(rendered)


def print_human_readable(
    symbol: str,
    spot_price: float,
    candidates: list[SpreadCandidate],
    show_order_json: bool,
    setup: UnderlyingSetupContext | None,
    *,
    strategy: str,
    profile: str,
    greeks_source: str,
    setup_summaries: tuple[str, ...] = (),
) -> None:
    print(f"{symbol.upper()} spot: {spot_price:.2f}")
    print(f"Strategy: {strategy}")
    print(f"Profile: {profile}")
    print(f"Greeks: {greeks_source}")
    if profile == "0dte":
        print(f"0DTE session: {format_session_bucket(zero_dte_session_bucket())}")
    if setup is not None:
        print(f"Setup: {setup.status} ({setup.score:.1f})")
        if setup.reasons:
            print(f"Setup notes: {'; '.join(setup.reasons)}")
    elif setup_summaries:
        print(f"Setups: {'; '.join(setup_summaries)}")
    print(f"Candidates found: {len(candidates)}")
    print()

    if not candidates:
        print("No option structures matched the current filters and calendar policy.")
        return

    include_strategy = (
        strategy == "combined"
        or len({candidate.strategy for candidate in candidates}) > 1
    )
    headers = [
        "Expiry",
        "DTE",
        "Short",
        "Long",
        "Width",
        "Entry",
        "ROR%",
        "Score",
        "Δ",
        "OTM%",
        "BE%",
        "S-EM",
        "MinOI",
        "Cal",
        "DQ",
        "EvtD",
    ]
    if include_strategy:
        headers = ["Side", *headers]
    rows = build_table_rows(candidates, include_strategy=include_strategy)
    print(format_table(headers, rows))
    print()

    for index, candidate in enumerate(candidates, start=1):
        print(
            f"{index}. [{strategy_display_label(candidate.strategy)}] {candidate.short_symbol} -> {candidate.long_symbol} | "
            f"score {candidate.quality_score:.1f} | "
            f"breakeven {candidate.breakeven:.2f} | "
            f"calendar {candidate.calendar_status}"
        )
        if candidate.greeks_source != "alpaca":
            print(f"   greeks: {candidate.greeks_source}")
        if candidate.expected_move is not None:
            print(
                "   expected move: "
                f"{candidate.expected_move:.2f} ({candidate.expected_move_pct * 100:.2f}% of spot) "
                f"from {candidate.expected_move_source_strike:.2f} strike"
            )
        if candidate.calendar_reasons:
            print(f"   reasons: {'; '.join(candidate.calendar_reasons)}")
        if candidate.data_reasons:
            print(f"   data: {'; '.join(candidate.data_reasons)}")
        if candidate.calendar_sources:
            source_line = ", ".join(candidate.calendar_sources)
            print(
                f"   sources: {source_line} | confidence {candidate.calendar_confidence}"
            )
        if candidate.macro_regime:
            print(f"   macro regime: {candidate.macro_regime}")
        if candidate.setup_score is not None:
            print(f"   setup: {candidate.setup_status} ({candidate.setup_score:.1f})")
        if show_order_json:
            print("   order payload:")
            print(json.dumps(candidate.order_payload, indent=2))
        print()


def write_csv(path: str, candidates: list[SpreadCandidate]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "underlying_symbol",
        "strategy",
        "profile",
        "expiration_date",
        "days_to_expiration",
        "underlying_price",
        "short_symbol",
        "long_symbol",
        "short_strike",
        "long_strike",
        "width",
        "short_delta",
        "long_delta",
        "greeks_source",
        "short_midpoint",
        "long_midpoint",
        "short_bid",
        "short_ask",
        "long_bid",
        "long_ask",
        "midpoint_credit",
        "natural_credit",
        "max_profit",
        "max_loss",
        "return_on_risk",
        "breakeven",
        "breakeven_cushion_pct",
        "short_otm_pct",
        "short_open_interest",
        "long_open_interest",
        "short_relative_spread",
        "long_relative_spread",
        "fill_ratio",
        "min_quote_size",
        "expected_move",
        "expected_move_pct",
        "expected_move_source_strike",
        "debit_width_ratio",
        "modeled_move_vs_implied_move",
        "modeled_move_vs_break_even_move",
        "short_vs_expected_move",
        "breakeven_vs_expected_move",
        "quality_score",
        "calendar_status",
        "calendar_reasons",
        "calendar_confidence",
        "calendar_sources",
        "calendar_last_updated",
        "calendar_days_to_nearest_event",
        "macro_regime",
        "earnings_phase",
        "earnings_event_date",
        "earnings_session_timing",
        "earnings_cohort_key",
        "earnings_days_to_event",
        "earnings_days_since_event",
        "earnings_timing_confidence",
        "earnings_horizon_crosses_report",
        "earnings_primary_source",
        "earnings_supporting_sources",
        "earnings_consensus_status",
        "setup_status",
        "setup_score",
        "setup_reasons",
        "setup_daily_score",
        "setup_intraday_score",
        "setup_intraday_minutes",
        "setup_has_intraday_context",
        "setup_spot_vs_vwap_pct",
        "setup_intraday_return_pct",
        "setup_distance_to_session_extreme_pct",
        "setup_opening_range_break_pct",
        "setup_latest_close",
        "setup_vwap",
        "setup_opening_range_high",
        "setup_opening_range_low",
        "data_status",
        "data_reasons",
        "selection_notes",
        "order_payload",
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for candidate in candidates:
            row = asdict(candidate)
            row["calendar_reasons"] = "; ".join(candidate.calendar_reasons)
            row["calendar_sources"] = ", ".join(candidate.calendar_sources)
            row["setup_reasons"] = "; ".join(candidate.setup_reasons)
            row["data_reasons"] = "; ".join(candidate.data_reasons)
            row["selection_notes"] = ", ".join(candidate.selection_notes)
            row["order_payload"] = json.dumps(
                candidate.order_payload, separators=(",", ":")
            )
            writer.writerow(row)


def write_json(
    path: str,
    symbol: str,
    spot_price: float,
    args: argparse.Namespace,
    candidates: list[SpreadCandidate],
    *,
    run_id: str | None = None,
    setup: UnderlyingSetupContext | None = None,
) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": symbol,
        "spot_price": spot_price,
        "generated_at": datetime.now(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "run_id": run_id,
        "filters": build_filter_payload(args),
        "setup": serialize_setup_context(setup),
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def build_ranked_candidate_rows(
    candidates: list[SpreadCandidate], *, include_strategy: bool = False
) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        row = [candidate.underlying_symbol]
        if include_strategy:
            row.append(strategy_display_label(candidate.strategy))
        row.extend(
            [
                candidate.expiration_date,
                format_dte_label(candidate.days_to_expiration),
                f"{candidate.underlying_price:.2f}",
                f"{candidate.short_strike:.2f}",
                f"{candidate.long_strike:.2f}",
                f"{candidate.midpoint_credit:.2f}",
                f"{candidate.quality_score:.1f}",
                "n/a"
                if candidate.short_delta is None
                else f"{candidate.short_delta:.2f}",
                f"{candidate.breakeven_cushion_pct * 100:.1f}",
                "n/a"
                if candidate.short_vs_expected_move is None
                else f"{candidate.short_vs_expected_move:.2f}",
                candidate.calendar_status,
                candidate.data_status,
                candidate.setup_status,
                ",".join(candidate.selection_notes),
            ]
        )
        rows.append(row)
    return rows


def print_ranked_candidates(
    *,
    label: str,
    strategy: str,
    profile: str,
    greeks_source: str,
    symbols: list[str],
    ranked_candidates: list[SpreadCandidate],
    failures: list[UniverseScanFailure],
) -> None:
    print(f"Universe: {label}")
    print(f"Strategy: {strategy}")
    print(f"Greeks: {greeks_source}")
    if profile == "0dte" or (
        ranked_candidates
        and any(candidate.profile == "0dte" for candidate in ranked_candidates)
    ):
        print(f"0DTE session: {format_session_bucket(zero_dte_session_bucket())}")
    print(f"Symbols requested: {len(symbols)}")
    print(f"Top candidates: {len(ranked_candidates)}")
    if failures:
        print(f"Failures: {len(failures)}")
    print()

    if ranked_candidates:
        include_strategy = (
            strategy == "combined"
            or len({candidate.strategy for candidate in ranked_candidates}) > 1
        )
        headers = [
            "Symbol",
            "Expiry",
            "DTE",
            "Spot",
            "Short",
            "Long",
            "MidCr",
            "Score",
            "Δ",
            "BE%",
            "S-EM",
            "Cal",
            "DQ",
            "Setup",
            "Why",
        ]
        if include_strategy:
            headers = ["Symbol", "Side", *headers[1:]]
        print(
            format_table(
                headers,
                build_ranked_candidate_rows(
                    ranked_candidates, include_strategy=include_strategy
                ),
            )
        )
        print()
    else:
        print("No universe candidates matched the current filters.")
        print()

    for index, candidate in enumerate(ranked_candidates, start=1):
        print(
            f"{index}. {candidate.underlying_symbol} [{strategy_display_label(candidate.strategy)}] "
            f"{candidate.short_symbol} -> {candidate.long_symbol} | "
            f"score {candidate.quality_score:.1f} | breakeven {candidate.breakeven:.2f}"
        )
        if candidate.selection_notes:
            print(f"   why: {', '.join(candidate.selection_notes)}")
        if candidate.calendar_reasons:
            print(f"   calendar: {'; '.join(candidate.calendar_reasons)}")
        if candidate.data_reasons:
            print(f"   data: {'; '.join(candidate.data_reasons)}")
        if candidate.setup_reasons:
            print(f"   setup: {'; '.join(candidate.setup_reasons)}")
        print()

    if failures:
        print("Failures:")
        for failure in failures:
            print(f"- {failure.symbol}: {failure.error}")


def write_universe_csv(path: str, candidates: list[SpreadCandidate]) -> None:
    write_csv(path, candidates)


def write_universe_json(
    path: str,
    *,
    label: str,
    strategy: str,
    symbols: list[str],
    candidates: list[SpreadCandidate],
    failures: list[UniverseScanFailure],
) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": "universe",
        "label": label,
        "strategy": strategy,
        "symbols": symbols,
        "generated_at": datetime.now(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "candidate_count": len(candidates),
        "failures": [asdict(failure) for failure in failures],
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def build_stream_symbols(
    candidates: list[SpreadCandidate], *, max_symbols: int = 16
) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        for leg in candidate_legs(asdict(candidate)):
            symbol = str(leg.get("symbol") or "").strip()
            if symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
            if len(symbols) >= max_symbols:
                return symbols
    return symbols


def build_live_spread_rows(
    candidates: list[SpreadCandidate],
    live_quotes: dict[str, LiveOptionQuote],
) -> list[list[str]]:
    rows: list[list[str]] = []
    for candidate in candidates:
        payload = asdict(candidate)
        live_snapshot = structure_quote_snapshot(
            legs=candidate_legs(payload),
            strategy_family=payload.get("strategy"),
            quotes_by_symbol=live_quotes,
        )
        if live_snapshot is None:
            continue
        primary_label = f"{candidate.short_strike:.2f}/{candidate.long_strike:.2f}"
        if (
            candidate.secondary_short_strike is not None
            and candidate.secondary_long_strike is not None
        ):
            primary_label = (
                f"{candidate.long_strike:.2f}-{candidate.short_strike:.2f}"
                f" / {candidate.secondary_short_strike:.2f}-{candidate.secondary_long_strike:.2f}"
            )
        rows.append(
            [
                strategy_display_label(candidate.strategy),
                candidate.expiration_date,
                primary_label,
                f"{candidate.width:.2f}",
                f"{float(live_snapshot['midpoint_value']):.2f}",
                f"{float(live_snapshot['natural_value']):.2f}",
                str(len(live_snapshot.get("legs") or [])),
                "n/a"
                if live_snapshot.get("captured_at") is None
                else str(live_snapshot["captured_at"]),
            ]
        )
    return rows


def maybe_stream_live_quotes(
    *,
    args: argparse.Namespace,
    client: AlpacaClient,
    candidates: list[SpreadCandidate],
) -> None:
    if not args.stream_live_quotes or args.json or not candidates:
        return

    stream_symbols = build_stream_symbols(candidates[: args.top])
    if not stream_symbols:
        return

    print()
    print(
        f"Streaming live option quotes for {len(stream_symbols)} legs via Alpaca websocket..."
    )
    try:
        streamer = AlpacaOptionQuoteStreamer(
            key_id=client.headers["APCA-API-KEY-ID"],
            secret_key=client.headers["APCA-API-SECRET-KEY"],
            data_base_url=client.data_base_url,
            feed=args.feed,
        )
        live_quotes = streamer.stream_quotes(
            stream_symbols, duration_seconds=args.stream_seconds
        )
    except Exception as exc:
        print(f"Live quote stream unavailable: {exc}")
        return

    if not live_quotes:
        print("Live quote stream returned no quote updates.")
        return

    rows = build_live_spread_rows(candidates[: args.top], live_quotes)
    if not rows:
        print("Live quote stream did not return both legs for the displayed spreads.")
        return

    headers = [
        "Side",
        "Expiry",
        "Strikes",
        "Width",
        "LiveMid",
        "LiveNat",
        "Legs",
        "Time",
    ]
    print(format_table(headers, rows))
    print()


__all__ = [
    "build_setup_summaries",
    "default_output_path",
    "default_universe_output_path",
    "format_table",
    "maybe_stream_live_quotes",
    "print_human_readable",
    "print_ranked_candidates",
    "write_csv",
    "write_json",
    "write_latest_copy",
    "write_universe_csv",
    "write_universe_json",
]
