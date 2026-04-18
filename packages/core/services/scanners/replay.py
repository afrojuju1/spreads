from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import Any

from core.backtest import (
    bars_through_date,
    latest_bar_on_or_before,
    mark_structure_on_date as mark_spread_on_date,
    merge_option_bars_with_trades,
    simulate_exit_path as simulate_exit_until_date,
    summarize_market_outcomes as summarize_replay,
)
from core.integrations.alpaca.client import AlpacaClient
from core.services.market_dates import NEW_YORK
from core.services.scanners.output import format_table
from core.storage.run_history_repository import RunHistoryRepository


def print_replay_summary(
    run_payload: dict[str, Any],
    summaries: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    strategy = run_payload.get("strategy") or run_payload["filters"].get(
        "strategy", "call_credit"
    )
    print(
        f"Replay run: {run_payload['run_id']} | {run_payload['symbol']} | "
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
    print(format_table(table_headers, table_rows))
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
        print(format_table(detail_headers, detail_rows))
    else:
        print("Replay data is not available yet for the stored horizons.")


def run_replay(
    *,
    args: argparse.Namespace,
    client: AlpacaClient,
    history_store: RunHistoryRepository,
) -> int:
    if args.replay_latest and args.strategy == "combined":
        raise SystemExit(
            "Replay latest requires a concrete strategy such as call_credit, put_credit, call_debit, put_debit, long_straddle, long_strangle, or iron_condor"
        )
    if args.replay_run_id:
        run_payload = history_store.get_run(args.replay_run_id)
    else:
        if not args.symbol:
            raise SystemExit("Replay latest requires --symbol or use --replay-run-id")
        run_payload = history_store.get_latest_run(
            args.symbol.upper(), strategy=args.strategy
        )

    if not run_payload:
        target = args.replay_run_id or args.symbol.upper()
        raise SystemExit(f"No stored run found for replay target: {target}")

    candidates = history_store.list_candidates(run_payload["run_id"])
    generated_at = datetime.fromisoformat(
        run_payload["generated_at"].replace("Z", "+00:00")
    )
    run_date = generated_at.astimezone(NEW_YORK).date()
    replay_end = max(
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
        end=replay_end.isoformat(),
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
        end=replay_end.isoformat(),
    )
    option_trades = client.get_option_trades(
        option_symbols,
        start=run_date.isoformat(),
        end=replay_end.isoformat(),
    )
    merged_option_bars = merge_option_bars_with_trades(
        bars_by_symbol=option_bars,
        trades_by_symbol=option_trades,
    )
    summaries, rows = summarize_replay(
        run_payload=run_payload,
        candidates=candidates,
        bars=bars,
        option_bars=merged_option_bars,
        profit_target=args.replay_profit_target,
        stop_multiple=args.replay_stop_multiple,
    )
    print_replay_summary(run_payload, summaries, rows)
    return 0


__all__ = [
    "bars_through_date",
    "latest_bar_on_or_before",
    "mark_spread_on_date",
    "run_replay",
    "simulate_exit_until_date",
    "summarize_replay",
]
