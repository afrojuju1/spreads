from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import Any

from core.domain.models import DailyBar
from core.integrations.alpaca.client import AlpacaClient
from core.services.market_dates import NEW_YORK
from core.services.option_structures import candidate_legs, net_premium_kind
from core.services.scanners.config import strategy_option_type
from core.services.scanners.output import format_table
from core.storage.run_history_repository import RunHistoryRepository


def latest_bar_on_or_before(bars: list[DailyBar], target_date: date) -> DailyBar | None:
    eligible = [
        bar
        for bar in bars
        if datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date()
        <= target_date
    ]
    if not eligible:
        return None
    return eligible[-1]


def bars_through_date(bars: list[DailyBar], target_date: date) -> list[DailyBar]:
    return [
        bar
        for bar in bars
        if datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date()
        <= target_date
    ]


def latest_option_bar_on_or_before(
    bars_by_symbol: dict[str, list[DailyBar]],
    symbol: str,
    target_date: date,
) -> DailyBar | None:
    return latest_bar_on_or_before(bars_by_symbol.get(symbol, []), target_date)


def estimate_spread_bar(
    short_bar: DailyBar,
    long_bar: DailyBar,
    *,
    strategy: str,
) -> dict[str, float]:
    if net_premium_kind(strategy) == "debit":
        return {
            "open": max(long_bar.open - short_bar.open, 0.0),
            "high": max(long_bar.high - short_bar.low, 0.0),
            "low": max(long_bar.low - short_bar.high, 0.0),
            "close": max(long_bar.close - short_bar.close, 0.0),
        }
    return {
        "open": max(short_bar.open - long_bar.open, 0.0),
        "high": max(short_bar.high - long_bar.low, 0.0),
        "low": max(short_bar.low - long_bar.high, 0.0),
        "close": max(short_bar.close - long_bar.close, 0.0),
    }


def estimate_structure_bar(
    *,
    legs: list[dict[str, Any]],
    bars_by_symbol: dict[str, DailyBar],
    strategy: str,
) -> dict[str, float] | None:
    premium_kind = net_premium_kind(strategy)
    if premium_kind is None or not legs:
        return None

    open_short = 0.0
    high_short = 0.0
    low_short = 0.0
    close_short = 0.0
    open_long = 0.0
    high_long = 0.0
    low_long = 0.0
    close_long = 0.0

    for leg in legs:
        symbol = str(leg.get("symbol") or "").strip()
        role = str(leg.get("role") or "").strip().lower()
        bar = bars_by_symbol.get(symbol)
        if not symbol or role not in {"short", "long"} or bar is None:
            return None
        if role == "short":
            open_short += bar.open
            high_short += bar.high
            low_short += bar.low
            close_short += bar.close
        else:
            open_long += bar.open
            high_long += bar.high
            low_long += bar.low
            close_long += bar.close

    if premium_kind == "debit":
        return {
            "open": max(open_long - open_short, 0.0),
            "high": max(high_long - low_short, 0.0),
            "low": max(low_long - high_short, 0.0),
            "close": max(close_long - close_short, 0.0),
        }
    return {
        "open": max(open_short - open_long, 0.0),
        "high": max(high_short - low_long, 0.0),
        "low": max(low_short - high_long, 0.0),
        "close": max(close_short - close_long, 0.0),
    }


def option_bar_available_for_target(
    bars_by_symbol: dict[str, list[DailyBar]],
    short_symbol: str,
    long_symbol: str,
    target_date: date,
) -> bool:
    short_bar = latest_option_bar_on_or_before(
        bars_by_symbol, short_symbol, target_date
    )
    long_bar = latest_option_bar_on_or_before(bars_by_symbol, long_symbol, target_date)
    if short_bar is None or long_bar is None:
        return False
    short_date = datetime.fromisoformat(
        short_bar.timestamp.replace("Z", "+00:00")
    ).date()
    long_date = datetime.fromisoformat(long_bar.timestamp.replace("Z", "+00:00")).date()
    return short_date == target_date and long_date == target_date


def option_bars_by_date(bars: list[DailyBar]) -> dict[date, DailyBar]:
    return {
        datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date(): bar
        for bar in bars
    }


def simulate_exit_until_date(
    candidate: dict[str, Any],
    *,
    option_bars: dict[str, list[DailyBar]],
    start_date: date,
    target_date: date,
    profit_target: float,
    stop_multiple: float,
) -> dict[str, Any]:
    legs = candidate_legs(candidate)
    leg_bars = {
        str(leg.get("symbol") or ""): option_bars_by_date(
            option_bars.get(str(leg.get("symbol") or ""), [])
        )
        for leg in legs
        if str(leg.get("symbol") or "").strip()
    }
    entry_credit = candidate["midpoint_credit"]
    premium_kind = net_premium_kind(candidate.get("strategy"))
    if premium_kind == "debit":
        target_mark = entry_credit * (1.0 + profit_target)
        stop_mark = max(entry_credit / max(stop_multiple, 1.0), 0.0)
    else:
        target_mark = max(entry_credit * (1.0 - profit_target), 0.0)
        stop_mark = entry_credit * stop_multiple

    path_date_sets = [set(bars_by_date) for bars_by_date in leg_bars.values()]
    if not path_date_sets:
        return {"status": "pending_option_bars"}
    path_dates = sorted(
        d for d in set.intersection(*path_date_sets) if start_date <= d <= target_date
    )
    if not path_dates:
        return {"status": "pending_option_bars"}

    last_mark = None
    for path_date in path_dates:
        spread_bar = estimate_structure_bar(
            legs=legs,
            bars_by_symbol={
                symbol: bars_by_date[path_date]
                for symbol, bars_by_date in leg_bars.items()
                if path_date in bars_by_date
            },
            strategy=str(candidate.get("strategy") or ""),
        )
        if spread_bar is None:
            return {"status": "pending_option_bars"}
        last_mark = spread_bar["close"]
        if premium_kind == "debit":
            hit_target = spread_bar["high"] >= target_mark
            hit_stop = spread_bar["low"] <= stop_mark
        else:
            hit_target = spread_bar["low"] <= target_mark
            hit_stop = spread_bar["high"] >= stop_mark

        if hit_target and hit_stop:
            return {
                "status": "conflict",
                "exit_date": path_date.isoformat(),
                "exit_reason": "conflict_stop_first",
                "exit_mark": stop_mark,
                "estimated_pnl": (
                    (stop_mark - entry_credit) * 100.0
                    if premium_kind == "debit"
                    else (entry_credit - stop_mark) * 100.0
                ),
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": True,
                "stop_hit": True,
            }
        if hit_target:
            return {
                "status": "exited",
                "exit_date": path_date.isoformat(),
                "exit_reason": "profit_target",
                "exit_mark": target_mark,
                "estimated_pnl": (
                    (target_mark - entry_credit) * 100.0
                    if premium_kind == "debit"
                    else (entry_credit - target_mark) * 100.0
                ),
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": True,
                "stop_hit": False,
            }
        if hit_stop:
            return {
                "status": "exited",
                "exit_date": path_date.isoformat(),
                "exit_reason": "stop",
                "exit_mark": stop_mark,
                "estimated_pnl": (
                    (stop_mark - entry_credit) * 100.0
                    if premium_kind == "debit"
                    else (entry_credit - stop_mark) * 100.0
                ),
                "spread_mark_close": spread_bar["close"],
                "spread_mark_low": spread_bar["low"],
                "spread_mark_high": spread_bar["high"],
                "profit_target_hit": False,
                "stop_hit": True,
            }

    return {
        "status": "open",
        "exit_date": path_dates[-1].isoformat(),
        "exit_reason": "mark",
        "exit_mark": last_mark,
        "estimated_pnl": (
            ((last_mark - entry_credit) * 100.0)
            if premium_kind == "debit"
            else ((entry_credit - last_mark) * 100.0)
        )
        if last_mark is not None
        else None,
        "spread_mark_close": last_mark,
        "spread_mark_low": None,
        "spread_mark_high": None,
        "profit_target_hit": False,
        "stop_hit": False,
    }


def mark_spread_on_date(
    candidate: dict[str, Any],
    *,
    option_bars: dict[str, list[DailyBar]],
    target_date: date,
) -> dict[str, Any]:
    legs = candidate_legs(candidate)
    bars_by_symbol: dict[str, DailyBar] = {}
    for leg in legs:
        symbol = str(leg.get("symbol") or "").strip()
        if not symbol:
            continue
        bar = latest_option_bar_on_or_before(option_bars, symbol, target_date)
        if bar is None:
            return {"status": "pending_option_bars"}
        bars_by_symbol[symbol] = bar
    if not bars_by_symbol:
        return {"status": "pending_option_bars"}

    spread_bar = estimate_structure_bar(
        legs=legs,
        bars_by_symbol=bars_by_symbol,
        strategy=str(candidate.get("strategy") or ""),
    )
    if spread_bar is None:
        return {"status": "pending_option_bars"}
    entry_credit = candidate["midpoint_credit"]
    close_mark = spread_bar["close"]
    premium_kind = net_premium_kind(candidate.get("strategy"))
    return {
        "status": "mark_only",
        "exit_date": target_date.isoformat(),
        "exit_reason": "entry_mark",
        "exit_mark": close_mark,
        "estimated_pnl": (
            (close_mark - entry_credit) * 100.0
            if premium_kind == "debit"
            else (entry_credit - close_mark) * 100.0
        ),
        "spread_mark_close": close_mark,
        "spread_mark_low": None,
        "spread_mark_high": None,
        "profit_target_hit": False,
        "stop_hit": False,
    }


def summarize_replay(
    *,
    run_payload: dict[str, Any],
    candidates: list[dict[str, Any]],
    bars: list[DailyBar],
    option_bars: dict[str, list[DailyBar]],
    profit_target: float,
    stop_multiple: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    generated_at = datetime.fromisoformat(
        run_payload["generated_at"].replace("Z", "+00:00")
    )
    run_date = generated_at.astimezone(NEW_YORK).date()
    strategy = run_payload.get("strategy") or run_payload["filters"].get(
        "strategy", "call_credit"
    )
    option_type = strategy_option_type(strategy)
    latest_available_date = (
        None
        if not bars
        else max(
            datetime.fromisoformat(bar.timestamp.replace("Z", "+00:00")).date()
            for bar in bars
        )
    )
    horizons = [
        ("entry", run_date),
        ("1d", run_date + timedelta(days=1)),
        ("3d", run_date + timedelta(days=3)),
    ]

    summaries: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for label, target_date in horizons:
        horizon_bars = bars_through_date(bars, target_date)
        available_candidates = 0
        touched = 0
        closed_past_short = 0
        closed_past_breakeven = 0
        profit_target_hits = 0
        stop_hits = 0
        conflicts = 0
        total_pnl = 0.0

        for candidate in candidates:
            if latest_available_date is None or latest_available_date < target_date:
                rows.append(
                    {
                        "horizon": label,
                        "short_symbol": candidate["short_symbol"],
                        "long_symbol": candidate["long_symbol"],
                        "expiration_date": candidate["expiration_date"],
                        "status": "pending",
                    }
                )
                continue

            horizon_bar = latest_bar_on_or_before(bars, target_date)
            if horizon_bar is None:
                continue
            if label == "entry":
                replay_path = mark_spread_on_date(
                    candidate,
                    option_bars=option_bars,
                    target_date=target_date,
                )
            else:
                replay_path = simulate_exit_until_date(
                    candidate,
                    option_bars=option_bars,
                    start_date=run_date,
                    target_date=target_date,
                    profit_target=profit_target,
                    stop_multiple=stop_multiple,
                )
            if replay_path["status"] == "pending_option_bars":
                rows.append(
                    {
                        "horizon": label,
                        "short_symbol": candidate["short_symbol"],
                        "long_symbol": candidate["long_symbol"],
                        "expiration_date": candidate["expiration_date"],
                        "status": "pending_option_bars",
                    }
                )
                continue

            available_candidates += 1
            path_bars = horizon_bars
            path_high = (
                max(bar.high for bar in path_bars) if path_bars else horizon_bar.high
            )
            path_low = (
                min(bar.low for bar in path_bars) if path_bars else horizon_bar.low
            )
            if option_type == "put":
                touched_short = path_low <= candidate["short_strike"]
                closed_beyond_short = horizon_bar.close <= candidate["short_strike"]
                closed_beyond_breakeven = horizon_bar.close <= candidate["breakeven"]
            else:
                touched_short = path_high >= candidate["short_strike"]
                closed_beyond_short = horizon_bar.close >= candidate["short_strike"]
                closed_beyond_breakeven = horizon_bar.close >= candidate["breakeven"]
            touched += int(touched_short)
            closed_past_short += int(closed_beyond_short)
            closed_past_breakeven += int(closed_beyond_breakeven)
            profit_target_hits += int(replay_path["profit_target_hit"])
            stop_hits += int(replay_path["stop_hit"])
            conflicts += int(replay_path["status"] == "conflict")
            total_pnl += replay_path["estimated_pnl"] or 0.0
            rows.append(
                {
                    "horizon": label,
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "available",
                    "spot_at_horizon": horizon_bar.close,
                    "path_extreme_to_horizon": path_low
                    if option_type == "put"
                    else path_high,
                    "touched_short_strike": touched_short,
                    "closed_past_short_strike": closed_beyond_short,
                    "closed_past_breakeven": closed_beyond_breakeven,
                    "spread_mark_close": replay_path["spread_mark_close"],
                    "spread_mark_low": replay_path["spread_mark_low"],
                    "spread_mark_high": replay_path["spread_mark_high"],
                    "estimated_pnl": replay_path["estimated_pnl"],
                    "estimated_profit_target_hit": replay_path["profit_target_hit"],
                    "estimated_stop_hit": replay_path["stop_hit"],
                    "exit_reason": replay_path["exit_reason"],
                    "exit_date": replay_path["exit_date"],
                    "replay_status": replay_path["status"],
                }
            )

        total = len(candidates)
        summaries.append(
            {
                "horizon": label,
                "available": available_candidates,
                "pending": total - available_candidates,
                "touch_pct": None
                if available_candidates == 0
                else 100.0 * touched / available_candidates,
                "close_past_short_pct": None
                if available_candidates == 0
                else 100.0 * closed_past_short / available_candidates,
                "close_past_breakeven_pct": None
                if available_candidates == 0
                else 100.0 * closed_past_breakeven / available_candidates,
                "profit_target_hit_pct": None
                if available_candidates == 0
                else 100.0 * profit_target_hits / available_candidates,
                "stop_hit_pct": None
                if available_candidates == 0
                else 100.0 * stop_hits / available_candidates,
                "conflict_pct": None
                if available_candidates == 0
                else 100.0 * conflicts / available_candidates,
                "avg_pnl": None
                if available_candidates == 0
                else total_pnl / available_candidates,
            }
        )

    expiry_available = 0
    expiry_touched = 0
    expiry_closed_past_short = 0
    expiry_closed_past_breakeven = 0
    expiry_profit_targets = 0
    expiry_stop_hits = 0
    expiry_conflicts = 0
    expiry_total_pnl = 0.0
    for candidate in candidates:
        expiry_date = date.fromisoformat(candidate["expiration_date"])
        if latest_available_date is None or latest_available_date < expiry_date:
            rows.append(
                {
                    "horizon": "expiry",
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "pending",
                }
            )
            continue

        horizon_bar = latest_bar_on_or_before(bars, expiry_date)
        if horizon_bar is None:
            continue
        replay_path = simulate_exit_until_date(
            candidate,
            option_bars=option_bars,
            start_date=run_date,
            target_date=expiry_date,
            profit_target=profit_target,
            stop_multiple=stop_multiple,
        )
        if replay_path["status"] == "pending_option_bars":
            rows.append(
                {
                    "horizon": "expiry",
                    "short_symbol": candidate["short_symbol"],
                    "long_symbol": candidate["long_symbol"],
                    "expiration_date": candidate["expiration_date"],
                    "status": "pending_option_bars",
                }
            )
            continue
        expiry_available += 1
        path_bars = bars_through_date(bars, expiry_date)
        path_high = (
            max(bar.high for bar in path_bars) if path_bars else horizon_bar.high
        )
        path_low = min(bar.low for bar in path_bars) if path_bars else horizon_bar.low
        if option_type == "put":
            touched_short = path_low <= candidate["short_strike"]
            closed_beyond_short = horizon_bar.close <= candidate["short_strike"]
            closed_beyond_breakeven = horizon_bar.close <= candidate["breakeven"]
        else:
            touched_short = path_high >= candidate["short_strike"]
            closed_beyond_short = horizon_bar.close >= candidate["short_strike"]
            closed_beyond_breakeven = horizon_bar.close >= candidate["breakeven"]
        expiry_touched += int(touched_short)
        expiry_closed_past_short += int(closed_beyond_short)
        expiry_closed_past_breakeven += int(closed_beyond_breakeven)
        expiry_profit_targets += int(replay_path["profit_target_hit"])
        expiry_stop_hits += int(replay_path["stop_hit"])
        expiry_conflicts += int(replay_path["status"] == "conflict")
        expiry_total_pnl += replay_path["estimated_pnl"] or 0.0
        rows.append(
            {
                "horizon": "expiry",
                "short_symbol": candidate["short_symbol"],
                "long_symbol": candidate["long_symbol"],
                "expiration_date": candidate["expiration_date"],
                "status": "available",
                "spot_at_horizon": horizon_bar.close,
                "path_extreme_to_horizon": path_low
                if option_type == "put"
                else path_high,
                "touched_short_strike": touched_short,
                "closed_past_short_strike": closed_beyond_short,
                "closed_past_breakeven": closed_beyond_breakeven,
                "spread_mark_close": replay_path["spread_mark_close"],
                "spread_mark_low": replay_path["spread_mark_low"],
                "spread_mark_high": replay_path["spread_mark_high"],
                "estimated_pnl": replay_path["estimated_pnl"],
                "estimated_profit_target_hit": replay_path["profit_target_hit"],
                "estimated_stop_hit": replay_path["stop_hit"],
                "exit_reason": replay_path["exit_reason"],
                "exit_date": replay_path["exit_date"],
                "replay_status": replay_path["status"],
            }
        )

    total = len(candidates)
    summaries.append(
        {
            "horizon": "expiry",
            "available": expiry_available,
            "pending": total - expiry_available,
            "touch_pct": None
            if expiry_available == 0
            else 100.0 * expiry_touched / expiry_available,
            "close_past_short_pct": None
            if expiry_available == 0
            else 100.0 * expiry_closed_past_short / expiry_available,
            "close_past_breakeven_pct": None
            if expiry_available == 0
            else 100.0 * expiry_closed_past_breakeven / expiry_available,
            "profit_target_hit_pct": None
            if expiry_available == 0
            else 100.0 * expiry_profit_targets / expiry_available,
            "stop_hit_pct": None
            if expiry_available == 0
            else 100.0 * expiry_stop_hits / expiry_available,
            "conflict_pct": None
            if expiry_available == 0
            else 100.0 * expiry_conflicts / expiry_available,
            "avg_pnl": None
            if expiry_available == 0
            else expiry_total_pnl / expiry_available,
        }
    )

    return summaries, rows


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
    summaries, rows = summarize_replay(
        run_payload=run_payload,
        candidates=candidates,
        bars=bars,
        option_bars=option_bars,
        profit_target=args.replay_profit_target,
        stop_multiple=args.replay_stop_multiple,
    )
    print_replay_summary(run_payload, summaries, rows)
    return 0


__all__ = [
    "mark_spread_on_date",
    "run_replay",
    "summarize_replay",
]
