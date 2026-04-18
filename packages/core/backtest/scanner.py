from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import Any

from core.backtest.market_data import (
    bars_through_date,
    latest_bar_on_or_before,
    mark_structure_on_date as mark_spread_on_date,
    merge_option_bars_with_trades,
    simulate_exit_path as simulate_exit_until_date,
    summarize_market_outcomes,
)
from core.integrations.alpaca.client import AlpacaClient
from core.services.market_dates import NEW_YORK
from core.storage.run_history_repository import RunHistoryRepository


def _format_table(headers: list[str], rows: list[list[str]]) -> str:
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


def print_scanner_backtest_summary(
    run_payload: dict[str, Any],
    summaries: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    strategy = run_payload.get("strategy") or run_payload["filters"].get(
        "strategy", "call_credit"
    )
    print(
        f"Backtest run: {run_payload['run_id']} | {run_payload['symbol']} | "
        f"strategy {strategy} | profile {run_payload['profile']} | generated {run_payload['generated_at']}"
    )
    print(f"Stored candidates: {run_payload['candidate_count']}")
    print()

    table_headers = [
        "Horizon",
        "Avail",
        "Pending",
        "Touch%",
        "PastShort%",
        "PastBE%",
        "PT%",
        "Stop%",
        "Conf%",
        "AvgPnL$",
    ]
    table_rows = []
    for summary in summaries:
        table_rows.append(
            [
                summary["horizon"],
                str(summary["available"]),
                str(summary["pending"]),
                "n/a"
                if summary["touch_pct"] is None
                else f"{summary['touch_pct']:.1f}",
                "n/a"
                if summary["close_past_short_pct"] is None
                else f"{summary['close_past_short_pct']:.1f}",
                "n/a"
                if summary["close_past_breakeven_pct"] is None
                else f"{summary['close_past_breakeven_pct']:.1f}",
                "n/a"
                if summary["profit_target_hit_pct"] is None
                else f"{summary['profit_target_hit_pct']:.1f}",
                "n/a"
                if summary["stop_hit_pct"] is None
                else f"{summary['stop_hit_pct']:.1f}",
                "n/a"
                if summary["conflict_pct"] is None
                else f"{summary['conflict_pct']:.1f}",
                "n/a" if summary["avg_pnl"] is None else f"{summary['avg_pnl']:.0f}",
            ]
        )
    print(_format_table(table_headers, table_rows))
    print()

    available_rows = [row for row in rows if row["status"] == "available"][:10]
    if available_rows:
        detail_headers = [
            "Horizon",
            "Short",
            "Long",
            "Expiry",
            "Spot",
            "Sprd",
            "PnL$",
            "Touch",
            "PastShort",
            "PastBE",
            "Exit",
            "PT",
            "Stop",
        ]
        detail_rows = [
            [
                row["horizon"],
                row["short_symbol"],
                row["long_symbol"],
                row["expiration_date"],
                f"{row['spot_at_horizon']:.2f}",
                f"{row['spread_mark_close']:.2f}",
                f"{row['estimated_pnl']:.0f}",
                "yes" if row["touched_short_strike"] else "no",
                "yes" if row["closed_past_short_strike"] else "no",
                "yes" if row["closed_past_breakeven"] else "no",
                row["exit_reason"],
                "yes" if row["estimated_profit_target_hit"] else "no",
                "yes" if row["estimated_stop_hit"] else "no",
            ]
            for row in available_rows
        ]
        print(_format_table(detail_headers, detail_rows))
    else:
        print("Backtest data is not available yet for the stored horizons.")


def run_scanner_backtest(
    *,
    args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
) -> int:
    if args.backtest_latest and args.strategy == "combined":
        raise SystemExit(
            "Backtest latest requires a concrete strategy such as call_credit, put_credit, call_debit, put_debit, long_straddle, long_strangle, or iron_condor"
        )
    if args.backtest_run_id:
        run_payload = history_store.get_run(args.backtest_run_id)
    else:
        if not args.symbol:
            raise SystemExit(
                "Backtest latest requires --symbol or use --backtest-run-id"
            )
        run_payload = history_store.get_latest_run(
            args.symbol.upper(), strategy=args.strategy
        )

    if not run_payload:
        target = args.backtest_run_id or args.symbol.upper()
        raise SystemExit(f"No stored run found for backtest target: {target}")

    candidates = history_store.list_candidates(run_payload["run_id"])
    generated_at = datetime.fromisoformat(
        run_payload["generated_at"].replace("Z", "+00:00")
    )
    run_date = generated_at.astimezone(NEW_YORK).date()
    evaluation_end = max(
        [
            run_date + timedelta(days=3),
            *[
                date.fromisoformat(candidate["expiration_date"])
                for candidate in candidates
            ],
        ]
    )
    bars = client.get_daily_bars(
        run_payload["symbol"],
        start=(run_date - timedelta(days=2)).isoformat(),
        end=evaluation_end.isoformat(),
        stock_feed=args.stock_feed,
    )
    option_symbols = sorted(
        {
            *[candidate["short_symbol"] for candidate in candidates],
            *[candidate["long_symbol"] for candidate in candidates],
        }
    )
    option_bars = client.get_option_bars(
        option_symbols,
        start=run_date.isoformat(),
        end=evaluation_end.isoformat(),
    )
    option_trades = client.get_option_trades(
        option_symbols,
        start=run_date.isoformat(),
        end=evaluation_end.isoformat(),
    )
    merged_option_bars = merge_option_bars_with_trades(
        bars_by_symbol=option_bars,
        trades_by_symbol=option_trades,
    )
    summaries, rows = summarize_market_outcomes(
        run_payload=run_payload,
        candidates=candidates,
        bars=bars,
        option_bars=merged_option_bars,
        profit_target=args.backtest_profit_target,
        stop_multiple=args.backtest_stop_multiple,
    )
    print_scanner_backtest_summary(run_payload, summaries, rows)
    return 0


__all__ = [
    "bars_through_date",
    "latest_bar_on_or_before",
    "mark_spread_on_date",
    "run_scanner_backtest",
    "simulate_exit_until_date",
    "summarize_market_outcomes",
]
