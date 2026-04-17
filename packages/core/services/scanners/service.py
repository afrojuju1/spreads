#!/usr/bin/env python3
"""Scan Alpaca option chains for vertical spread candidates.

Usage:
    uv run spreads scan --symbol SPY

Required environment variables:
    APCA_API_KEY_ID
    APCA_API_SECRET_KEY

Notes:
    - Uses Alpaca's Trading API for option contract metadata.
    - Uses Alpaca's Market Data API for underlying price and option chain snapshots.
    - Supports call/put credit and debit vertical spreads with shared ranking/replay logic.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime

from core.common import env_or_die, load_local_env
from core.domain.models import SpreadCandidate, SymbolScanResult, UniverseScanFailure
from core.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
from core.integrations.calendar_events import build_calendar_event_resolver
from core.integrations.greeks import build_local_greeks_provider
from core.services.scanners.builders.ranking import sort_candidates_for_display
from core.services.scanners.config import build_filter_payload, parse_args, resolve_symbols
from core.services.scanners.output import (
    build_setup_summaries,
    default_output_path,
    default_universe_output_path,
    maybe_stream_live_quotes,
    print_human_readable,
    print_ranked_candidates,
    write_csv,
    write_json,
    write_latest_copy,
    write_universe_csv,
    write_universe_json,
)
from core.services.scanners.replay import run_replay
from core.services.scanners.runtime import (
    merge_strategy_candidates,
    scan_symbol_across_strategies,
)
from core.storage.factory import build_history_store


def main(argv: list[str] | None = None) -> int:
    load_local_env()
    args = parse_args(argv)

    key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
    secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    symbols, universe_label = resolve_symbols(args)

    client = AlpacaClient(
        key_id=key_id,
        secret_key=secret_key,
        trading_base_url=infer_trading_base_url(key_id, args.trading_base_url),
        data_base_url=args.data_base_url,
    )
    history_store = build_history_store(args.history_db)
    calendar_resolver = build_calendar_event_resolver(
        key_id=key_id,
        secret_key=secret_key,
        data_base_url=args.data_base_url,
        database_url=args.history_db,
    )
    greeks_provider = build_local_greeks_provider()

    try:
        if args.replay_latest or args.replay_run_id:
            return run_replay(args=args, client=client, history_store=history_store)

        if len(symbols) == 1:
            strategy_results, failures = scan_symbol_across_strategies(
                symbol=symbols[0],
                base_args=args,
                client=client,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                history_store=history_store,
            )
            if failures and not strategy_results:
                raise SystemExit(failures[0].error)

            if args.strategy == "combined":
                combined_candidates = merge_strategy_candidates(strategy_results)
                primary_result = strategy_results[0]
                output_path = args.output or default_output_path(
                    primary_result.symbol, args.strategy, args.output_format
                )
                if args.output_format == "csv":
                    write_csv(output_path, combined_candidates)
                else:
                    write_json(
                        output_path,
                        primary_result.symbol,
                        primary_result.spot_price,
                        args,
                        combined_candidates,
                    )
                latest_copy = write_latest_copy(
                    output_path,
                    f"latest_{primary_result.symbol.lower()}_{args.strategy}.{args.output_format}",
                )
                candidates = combined_candidates[: args.top]
                setup_summaries = build_setup_summaries(strategy_results)
                if args.json:
                    print(
                        json.dumps(
                            {
                                "symbol": primary_result.symbol,
                                "strategy": args.strategy,
                                "spot_price": primary_result.spot_price,
                                "generated_at": datetime.now(UTC)
                                .isoformat(timespec="seconds")
                                .replace("+00:00", "Z"),
                                "filters": build_filter_payload(args),
                                "strategy_runs": [
                                    {
                                        "strategy": result.args.strategy,
                                        "run_id": result.run_id,
                                        "setup": None
                                        if result.setup is None
                                        else {
                                            "status": result.setup.status,
                                            "score": result.setup.score,
                                            "reasons": list(result.setup.reasons),
                                        },
                                    }
                                    for result in strategy_results
                                ],
                                "failures": [asdict(failure) for failure in failures],
                                "candidates": [
                                    asdict(candidate) for candidate in candidates
                                ],
                                "output_file": output_path,
                            },
                            indent=2,
                        )
                    )
                else:
                    print_human_readable(
                        primary_result.symbol,
                        primary_result.spot_price,
                        candidates,
                        args.show_order_json,
                        None,
                        strategy=args.strategy,
                        profile=args.profile,
                        greeks_source=args.greeks_source,
                        setup_summaries=setup_summaries,
                    )
                    for strategy_result in strategy_results:
                        if strategy_result.args.profile == "0dte":
                            print(
                                f"0DTE coverage [{strategy_result.args.strategy}]: Alpaca returned quotes for "
                                f"{strategy_result.quoted_contract_count} contracts, Alpaca delta for "
                                f"{strategy_result.alpaca_delta_contract_count}, final usable delta for "
                                f"{strategy_result.delta_contract_count}, local Greeks for "
                                f"{strategy_result.local_delta_contract_count}."
                            )
                    maybe_stream_live_quotes(
                        args=args, client=client, candidates=candidates
                    )
                    if failures:
                        print("Failures:")
                        for failure in failures:
                            print(f"- {failure.symbol}: {failure.error}")
                    print(f"Saved {len(combined_candidates)} candidates to {output_path}")
                    print(f"Latest copy: {latest_copy}")
                    print("Run ids:")
                    for result in strategy_results:
                        print(f"- {result.args.strategy}: {result.run_id}")
            else:
                result = strategy_results[0]
                output_path = args.output or default_output_path(
                    result.symbol, result.args.strategy, args.output_format
                )

                if args.output_format == "csv":
                    write_csv(output_path, result.candidates)
                else:
                    write_json(
                        output_path,
                        result.symbol,
                        result.spot_price,
                        result.args,
                        result.candidates,
                        run_id=result.run_id,
                        setup=result.setup,
                    )
                latest_copy = write_latest_copy(
                    output_path,
                    f"latest_{result.symbol.lower()}_{result.args.strategy}.{args.output_format}",
                )

                candidates = result.candidates[: result.args.top]
                if args.json:
                    print(
                        json.dumps(
                            {
                                "symbol": result.symbol,
                                "strategy": result.args.strategy,
                                "spot_price": result.spot_price,
                                "generated_at": datetime.now(UTC)
                                .isoformat(timespec="seconds")
                                .replace("+00:00", "Z"),
                                "run_id": result.run_id,
                                "filters": build_filter_payload(result.args),
                                "setup": None
                                if result.setup is None
                                else {
                                    "status": result.setup.status,
                                    "score": result.setup.score,
                                    "reasons": list(result.setup.reasons),
                                },
                                "candidates": [
                                    asdict(candidate) for candidate in candidates
                                ],
                                "output_file": output_path,
                            },
                            indent=2,
                        )
                    )
                else:
                    print_human_readable(
                        result.symbol,
                        result.spot_price,
                        candidates,
                        result.args.show_order_json,
                        result.setup,
                        strategy=result.args.strategy,
                        profile=result.args.profile,
                        greeks_source=result.args.greeks_source,
                    )
                    if result.args.profile == "0dte":
                        print(
                            f"0DTE coverage: Alpaca returned quotes for {result.quoted_contract_count} "
                            f"contracts, Alpaca delta for {result.alpaca_delta_contract_count}, final usable delta for "
                            f"{result.delta_contract_count}, local Greeks for "
                            f"{result.local_delta_contract_count}."
                        )
                    maybe_stream_live_quotes(
                        args=result.args, client=client, candidates=candidates
                    )
                    print(f"Saved {len(result.candidates)} candidates to {output_path}")
                    print(f"Latest copy: {latest_copy}")
                    print(f"Run id: {result.run_id}")
        else:
            scan_results: list[SymbolScanResult] = []
            failures: list[UniverseScanFailure] = []
            ranked_candidates: list[SpreadCandidate] = []

            for symbol in symbols:
                strategy_results, symbol_failures = scan_symbol_across_strategies(
                    symbol=symbol,
                    base_args=args,
                    client=client,
                    calendar_resolver=calendar_resolver,
                    greeks_provider=greeks_provider,
                    history_store=history_store,
                )
                failures.extend(symbol_failures)
                if not strategy_results:
                    continue
                scan_results.extend(strategy_results)
                symbol_ranked_candidates = merge_strategy_candidates(
                    strategy_results,
                    per_strategy_top=args.per_symbol_top,
                )[: args.per_symbol_top]
                ranked_candidates.extend(symbol_ranked_candidates)

            ranked_candidates = sort_candidates_for_display(ranked_candidates)
            ranked_candidates = ranked_candidates[: args.top]
            output_path = args.output or default_universe_output_path(
                universe_label, args.strategy, args.output_format
            )

            if args.output_format == "csv":
                write_universe_csv(output_path, ranked_candidates)
            else:
                write_universe_json(
                    output_path,
                    label=universe_label,
                    strategy=args.strategy,
                    symbols=symbols,
                    candidates=ranked_candidates,
                    failures=failures,
                )
            latest_copy = write_latest_copy(
                output_path,
                f"latest_{universe_label.lower().replace(' ', '_')}_{args.strategy}.{args.output_format}",
            )

            if args.json:
                print(
                    json.dumps(
                        {
                            "mode": "universe",
                            "label": universe_label,
                            "strategy": args.strategy,
                            "symbols": symbols,
                            "candidate_count": len(ranked_candidates),
                            "failures": [asdict(failure) for failure in failures],
                            "candidates": [
                                asdict(candidate) for candidate in ranked_candidates
                            ],
                            "output_file": output_path,
                        },
                        indent=2,
                    )
                )
            else:
                print_ranked_candidates(
                    label=universe_label,
                    strategy=args.strategy,
                    profile=args.profile,
                    greeks_source=args.greeks_source,
                    symbols=symbols,
                    ranked_candidates=ranked_candidates,
                    failures=failures,
                )
                maybe_stream_live_quotes(
                    args=args, client=client, candidates=ranked_candidates
                )
                if scan_results:
                    print(f"Stored per-symbol runs: {len(scan_results)}")
                print(f"Saved {len(ranked_candidates)} ranked candidates to {output_path}")
                print(f"Latest copy: {latest_copy}")

        return 0
    finally:
        history_store.close()
        calendar_resolver.store.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
