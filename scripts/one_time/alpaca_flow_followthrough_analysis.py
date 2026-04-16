#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from spreads.services.alpaca import create_alpaca_client_from_env  # noqa: E402
from spreads.services.option_trade_records import (  # noqa: E402
    UOA_ALLOWED_TRADE_CONDITIONS,
    classify_trade_conditions_for_uoa,
)
from spreads.services.scanner import AlpacaClient  # noqa: E402

NEW_YORK = ZoneInfo("America/New_York")
NYSE = mcal.get_calendar("NYSE")
DEFAULT_SYMBOLS = ["SPY", "QQQ"]
DEFAULT_OUTPUT_ROOT = ROOT / "outputs" / "analysis" / "alpaca_flow_followthrough"
OPTION_BATCH_SIZE = 50
SLOT_MINUTES = 5
SLOTS_PER_SESSION = 78


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a one-off Alpaca option-flow follow-through analysis. "
            "The script looks for scoreable premium bursts in 5-minute windows and "
            "measures what the underlying did next."
        )
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated underlying symbols. Default: SPY,QQQ",
    )
    parser.add_argument(
        "--start-date",
        help="Analysis start date in YYYY-MM-DD. If omitted, use the last --sessions NYSE sessions.",
    )
    parser.add_argument(
        "--end-date",
        default="today",
        help="Analysis end date in YYYY-MM-DD using the New York calendar. Default: today",
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=5,
        help="Number of NYSE sessions to analyze when --start-date is omitted. Default: 5",
    )
    parser.add_argument(
        "--baseline-sessions",
        type=int,
        default=20,
        help="Prior same-slot sessions used for burst thresholds. Default: 20",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=21,
        help="Maximum DTE to include when fetching contracts and trades. Default: 21",
    )
    parser.add_argument(
        "--min-scoreable-trades",
        type=int,
        default=3,
        help="Minimum scoreable trades required for a premium burst. Default: 3",
    )
    parser.add_argument(
        "--stock-feed",
        default="sip",
        choices=("sip", "iex", "delayed_sip", "boats", "overnight"),
        help="Alpaca stock feed for minute bars. Default: sip",
    )
    parser.add_argument(
        "--label",
        help="Optional output label. Default: generated timestamp",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help=f"Root directory for outputs. Default: {DEFAULT_OUTPUT_ROOT}",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=OPTION_BATCH_SIZE,
        help=f"Contracts per historical trades request. Default: {OPTION_BATCH_SIZE}",
    )
    return parser.parse_args(argv)


def resolve_end_date(raw: str) -> date:
    if raw == "today":
        return datetime.now(NEW_YORK).date()
    return date.fromisoformat(raw)


def normalize_symbols(raw: str) -> list[str]:
    symbols: list[str] = []
    for item in raw.split(","):
        symbol = item.strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        raise SystemExit("At least one symbol is required")
    return symbols


def choose_analysis_sessions(
    *,
    start_date: date | None,
    end_date: date,
    sessions: int,
    baseline_sessions: int,
) -> tuple[pd.DataFrame, list[date], list[date], dict[date, date | None]]:
    schedule_start = (start_date or (end_date - timedelta(days=max(60, sessions * 3)))) - timedelta(days=90)
    schedule_end = end_date + timedelta(days=10)
    schedule = build_schedule(schedule_start, schedule_end)
    eligible = schedule[schedule["session_date"] <= end_date].copy()
    if eligible.empty:
        raise SystemExit("No NYSE sessions found for the requested end date")

    if start_date is not None:
        analysis_rows = eligible[eligible["session_date"] >= start_date].copy()
    else:
        analysis_rows = eligible.tail(max(sessions, 1)).copy()
    if analysis_rows.empty:
        raise SystemExit("No analysis sessions found for the requested range")

    analysis_dates = analysis_rows["session_date"].tolist()
    first_analysis_date = analysis_dates[0]
    prior_rows = eligible[eligible["session_date"] < first_analysis_date].tail(max(baseline_sessions, 0))
    baseline_dates = prior_rows["session_date"].tolist()
    working_dates = baseline_dates + analysis_dates
    working_schedule = schedule[schedule["session_date"].isin(working_dates)].copy()
    next_by_session = build_next_session_map(schedule)
    return working_schedule, analysis_dates, baseline_dates, next_by_session


def build_schedule(start_date: date, end_date: date) -> pd.DataFrame:
    raw = NYSE.schedule(start_date=start_date.isoformat(), end_date=end_date.isoformat())
    rows: list[dict[str, Any]] = []
    for session_ts, row in raw.iterrows():
        rows.append(
            {
                "session_date": session_ts.date(),
                "market_open": row["market_open"].tz_convert(NEW_YORK),
                "market_close": row["market_close"].tz_convert(NEW_YORK),
            }
        )
    return pd.DataFrame(rows).sort_values("session_date").reset_index(drop=True)


def build_next_session_map(schedule: pd.DataFrame) -> dict[date, date | None]:
    ordered_dates = schedule["session_date"].tolist()
    payload: dict[date, date | None] = {}
    for index, session_date in enumerate(ordered_dates):
        payload[session_date] = None if index + 1 >= len(ordered_dates) else ordered_dates[index + 1]
    return payload


def make_output_dir(root: Path, label: str | None) -> Path:
    resolved_label = label or datetime.now(NEW_YORK).strftime("%Y%m%d_%H%M%S")
    output_dir = root / resolved_label
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def get_json_with_retry(
    client: AlpacaClient,
    *,
    base_url: str,
    path: str,
    params: dict[str, Any],
    attempts: int = 3,
    sleep_seconds: float = 1.0,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return client.get_json(base_url, path, params)
        except Exception as exc:  # pragma: no cover - network path
            last_error = exc
            if attempt >= attempts:
                raise
            print(
                f"Retrying {path} after attempt {attempt} failed: {exc}",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds * attempt)
    if last_error is not None:  # pragma: no cover - safety
        raise last_error
    raise RuntimeError(f"Unexpected retry state for {path}")


def batched(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    return [items[index : index + size] for index in range(0, len(items), size)]


def to_iso_utc(timestamp: pd.Timestamp) -> str:
    return timestamp.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def to_ny_timestamp(value: str | pd.Timestamp | datetime | None) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return stamp.tz_convert(NEW_YORK)


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.2f}%"


def mean_pct(series: pd.Series) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return round(float(cleaned.mean()), 4)


def positive_rate_pct(series: pd.Series) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return round(float((cleaned > 0).mean() * 100.0), 2)


def fetch_contracts(
    client: AlpacaClient,
    *,
    symbols: list[str],
    expiration_start: date,
    expiration_end: date,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for symbol in symbols:
        print(
            f"Fetching contracts for {symbol} from {expiration_start.isoformat()} to {expiration_end.isoformat()}",
            file=sys.stderr,
        )
        for status in ("active", "inactive"):
            page_token: str | None = None
            while True:
                payload = get_json_with_retry(
                    client,
                    base_url=client.trading_base_url,
                    path="/v2/options/contracts",
                    params={
                        "underlying_symbols": symbol,
                        "status": status,
                        "expiration_date_gte": expiration_start.isoformat(),
                        "expiration_date_lte": expiration_end.isoformat(),
                        "limit": 1000,
                        "page_token": page_token,
                    },
                )
                contracts = payload.get("option_contracts", [])
                for item in contracts:
                    option_symbol = str(item.get("symbol") or "").strip()
                    if not option_symbol or option_symbol in seen:
                        continue
                    expiration_value = item.get("expiration_date")
                    option_type = str(item.get("type") or "").strip().lower()
                    strike_price = item.get("strike_price")
                    if not expiration_value or option_type not in {"call", "put"} or strike_price in (None, ""):
                        continue
                    seen.add(option_symbol)
                    rows.append(
                        {
                            "option_symbol": option_symbol,
                            "underlying_symbol": str(item.get("underlying_symbol") or symbol).upper(),
                            "expiration_date": date.fromisoformat(str(expiration_value)),
                            "option_type": option_type,
                            "strike_price": float(strike_price),
                            "open_interest": None if item.get("open_interest") in (None, "") else int(item["open_interest"]),
                            "status": status,
                        }
                    )
                page_token = payload.get("next_page_token") or payload.get("page_token")
                if not page_token:
                    break
    if not rows:
        return pd.DataFrame(
            columns=[
                "option_symbol",
                "underlying_symbol",
                "expiration_date",
                "option_type",
                "strike_price",
                "open_interest",
                "status",
            ]
        )
    frame = pd.DataFrame(rows).drop_duplicates(subset=["option_symbol"]).sort_values(
        ["underlying_symbol", "expiration_date", "option_symbol"]
    )
    return frame.reset_index(drop=True)


def select_session_contracts(
    contracts: pd.DataFrame,
    *,
    symbol: str,
    session_date: date,
    max_dte: int,
) -> pd.DataFrame:
    if contracts.empty:
        return contracts
    max_expiration = session_date + timedelta(days=max_dte)
    subset = contracts[
        (contracts["underlying_symbol"] == symbol)
        & (contracts["expiration_date"] >= session_date)
        & (contracts["expiration_date"] <= max_expiration)
    ].copy()
    return subset.reset_index(drop=True)


def fetch_session_trades(
    client: AlpacaClient,
    *,
    session_contracts: pd.DataFrame,
    session_date: date,
    market_open: pd.Timestamp,
    market_close: pd.Timestamp,
    batch_size: int,
    exclusion_reason_counts: Counter[str],
    progress_label: str,
) -> pd.DataFrame:
    if session_contracts.empty:
        return pd.DataFrame()

    metadata_by_symbol = {
        row.option_symbol: {
            "underlying_symbol": row.underlying_symbol,
            "expiration_date": row.expiration_date,
            "option_type": row.option_type,
            "strike_price": row.strike_price,
            "open_interest": row.open_interest,
        }
        for row in session_contracts.itertuples(index=False)
    }
    rows: list[dict[str, Any]] = []
    contract_symbols = session_contracts["option_symbol"].tolist()
    batches = batched(contract_symbols, batch_size)
    for batch_index, batch in enumerate(batches, start=1):
        page_token: str | None = None
        batch_trade_count = 0
        while True:
            payload = get_json_with_retry(
                client,
                base_url=client.data_base_url,
                path="/v1beta1/options/trades",
                params={
                    "symbols": ",".join(batch),
                    "start": to_iso_utc(market_open),
                    "end": to_iso_utc(market_close),
                    "limit": 10000,
                    "page_token": page_token,
                },
            )
            raw_trades = payload.get("trades", {})
            if isinstance(raw_trades, dict):
                for option_symbol, trades in raw_trades.items():
                    metadata = metadata_by_symbol.get(str(option_symbol))
                    if metadata is None:
                        continue
                    for trade in trades:
                        trade_timestamp = to_ny_timestamp(trade.get("t"))
                        if trade_timestamp is None or trade_timestamp < market_open or trade_timestamp >= market_close:
                            continue
                        included_in_score, exclusion_reason, conditions = classify_trade_conditions_for_uoa(trade.get("c"))
                        if exclusion_reason:
                            exclusion_reason_counts[exclusion_reason] += 1
                        dte = (metadata["expiration_date"] - session_date).days
                        rows.append(
                            {
                                "session_date": session_date,
                                "underlying_symbol": metadata["underlying_symbol"],
                                "option_symbol": str(option_symbol),
                                "option_type": metadata["option_type"],
                                "expiration_date": metadata["expiration_date"],
                                "strike_price": metadata["strike_price"],
                                "open_interest": metadata["open_interest"],
                                "dte": dte,
                                "trade_timestamp": trade_timestamp,
                                "price": float(trade.get("p") or 0.0),
                                "size": int(trade.get("s") or 0),
                                "premium": float(trade.get("p") or 0.0) * int(trade.get("s") or 0) * 100.0,
                                "exchange_code": str(trade.get("x") or "") or None,
                                "conditions": conditions,
                                "included_in_score": included_in_score,
                                "exclusion_reason": exclusion_reason,
                            }
                        )
                        batch_trade_count += 1
            page_token = payload.get("next_page_token") or payload.get("page_token")
            if not page_token:
                break
        print(
            f"{progress_label}: batch {batch_index}/{len(batches)} -> {batch_trade_count} trades",
            file=sys.stderr,
        )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows).sort_values("trade_timestamp").reset_index(drop=True)
    frame["slot_start"] = frame["trade_timestamp"].dt.floor(f"{SLOT_MINUTES}min")
    return frame


def build_base_windows(
    *,
    symbol: str,
    session_date: date,
    market_open: pd.Timestamp,
) -> pd.DataFrame:
    slot_starts = pd.date_range(start=market_open, periods=SLOTS_PER_SESSION, freq=f"{SLOT_MINUTES}min")
    rows: list[dict[str, Any]] = []
    for slot_index, slot_start in enumerate(slot_starts):
        time_bucket = "open" if slot_index < 12 else "close" if slot_index >= 60 else "mid"
        rows.append(
            {
                "symbol": symbol,
                "session_date": session_date,
                "slot_index": slot_index,
                "slot_start": slot_start,
                "slot_time": slot_start.strftime("%H:%M"),
                "time_bucket": time_bucket,
            }
        )
    return pd.DataFrame(rows)


def summarize_session_windows(
    *,
    symbol: str,
    session_date: date,
    market_open: pd.Timestamp,
    trades: pd.DataFrame,
) -> pd.DataFrame:
    base = build_base_windows(symbol=symbol, session_date=session_date, market_open=market_open)
    if trades.empty:
        for column in (
            "raw_trade_count",
            "raw_premium",
            "scoreable_trade_count",
            "scoreable_premium",
            "excluded_trade_count",
            "excluded_premium",
            "distinct_contract_count",
            "largest_contract_share",
            "largest_expiry_share",
            "call_share",
            "put_share",
            "near_dte_share",
            "far_dte_share",
        ):
            base[column] = 0.0 if "premium" in column or "share" in column else 0
        return base

    rows: list[dict[str, Any]] = []
    for slot_start, group in trades.groupby("slot_start", sort=True):
        scoreable = group[group["included_in_score"]].copy()
        scoreable_premium = float(scoreable["premium"].sum())
        call_premium = float(scoreable.loc[scoreable["option_type"] == "call", "premium"].sum())
        put_premium = float(scoreable.loc[scoreable["option_type"] == "put", "premium"].sum())
        near_premium = float(scoreable.loc[scoreable["dte"].between(0, 7), "premium"].sum())
        far_premium = float(scoreable.loc[scoreable["dte"].between(8, 21), "premium"].sum())
        contract_premium = (
            scoreable.groupby("option_symbol")["premium"].sum().sort_values(ascending=False)
            if not scoreable.empty
            else pd.Series(dtype=float)
        )
        expiry_premium = (
            scoreable.groupby("expiration_date")["premium"].sum().sort_values(ascending=False)
            if not scoreable.empty
            else pd.Series(dtype=float)
        )
        rows.append(
            {
                "slot_start": slot_start,
                "raw_trade_count": int(len(group)),
                "raw_premium": round(float(group["premium"].sum()), 4),
                "scoreable_trade_count": int(len(scoreable)),
                "scoreable_premium": round(scoreable_premium, 4),
                "excluded_trade_count": int((~group["included_in_score"]).sum()),
                "excluded_premium": round(float(group.loc[~group["included_in_score"], "premium"].sum()), 4),
                "distinct_contract_count": int(scoreable["option_symbol"].nunique()),
                "largest_contract_share": 0.0
                if scoreable_premium <= 0 or contract_premium.empty
                else round(float(contract_premium.iloc[0] / scoreable_premium), 4),
                "largest_expiry_share": 0.0
                if scoreable_premium <= 0 or expiry_premium.empty
                else round(float(expiry_premium.iloc[0] / scoreable_premium), 4),
                "call_share": 0.0 if scoreable_premium <= 0 else round(call_premium / scoreable_premium, 4),
                "put_share": 0.0 if scoreable_premium <= 0 else round(put_premium / scoreable_premium, 4),
                "near_dte_share": 0.0 if scoreable_premium <= 0 else round(near_premium / scoreable_premium, 4),
                "far_dte_share": 0.0 if scoreable_premium <= 0 else round(far_premium / scoreable_premium, 4),
            }
        )
    aggregated = pd.DataFrame(rows)
    merged = base.merge(aggregated, on="slot_start", how="left")
    int_columns = [
        "raw_trade_count",
        "scoreable_trade_count",
        "excluded_trade_count",
        "distinct_contract_count",
    ]
    float_columns = [
        "raw_premium",
        "scoreable_premium",
        "excluded_premium",
        "largest_contract_share",
        "largest_expiry_share",
        "call_share",
        "put_share",
        "near_dte_share",
        "far_dte_share",
    ]
    for column in int_columns:
        merged[column] = merged[column].fillna(0).astype(int)
    for column in float_columns:
        merged[column] = merged[column].fillna(0.0).astype(float)
    return merged


def add_slot_baselines(
    frame: pd.DataFrame,
    *,
    baseline_sessions: int,
) -> pd.DataFrame:
    ordered = frame.sort_values(["symbol", "slot_index", "session_date"]).copy()
    grouped = ordered.groupby(["symbol", "slot_index"], sort=False)["scoreable_premium"]
    ordered["baseline_sample_size"] = grouped.transform(
        lambda series: series.shift(1).rolling(baseline_sessions, min_periods=baseline_sessions).count()
    )
    ordered["slot_baseline_median_premium"] = grouped.transform(
        lambda series: series.shift(1).rolling(baseline_sessions, min_periods=baseline_sessions).median()
    )
    ordered["slot_baseline_p90_premium"] = grouped.transform(
        lambda series: series.shift(1).rolling(baseline_sessions, min_periods=baseline_sessions).quantile(0.9)
    )
    ordered["premium_vs_slot_median"] = ordered.apply(
        lambda row: None
        if row["slot_baseline_median_premium"] in (None, 0) or pd.isna(row["slot_baseline_median_premium"])
        else round(float(row["scoreable_premium"] / row["slot_baseline_median_premium"]), 4),
        axis=1,
    )
    ordered["premium_vs_slot_p90"] = ordered.apply(
        lambda row: None
        if row["slot_baseline_p90_premium"] in (None, 0) or pd.isna(row["slot_baseline_p90_premium"])
        else round(float(row["scoreable_premium"] / row["slot_baseline_p90_premium"]), 4),
        axis=1,
    )
    return ordered


def classify_event_windows(
    frame: pd.DataFrame,
    *,
    baseline_sessions: int,
    min_scoreable_trades: int,
) -> pd.DataFrame:
    ordered = frame.sort_values(["symbol", "session_date", "slot_index"]).copy()
    threshold = ordered["slot_baseline_p90_premium"].fillna(0.0)
    ordered["premium_burst"] = (
        (ordered["baseline_sample_size"] >= float(baseline_sessions))
        & (ordered["scoreable_trade_count"] >= int(min_scoreable_trades))
        & (ordered["scoreable_premium"] > 0)
        & ((threshold <= 0) | (ordered["scoreable_premium"] >= threshold))
    )
    ordered["concentrated_burst"] = ordered["premium_burst"] & (
        (ordered["largest_contract_share"] >= 0.50) | (ordered["largest_expiry_share"] >= 0.70)
    )
    ordered["repeated_burst"] = ordered["premium_burst"] & ordered.groupby(
        ["symbol", "session_date"], sort=False
    )["premium_burst"].shift(1).fillna(False)
    ordered["dominance"] = "mixed"
    ordered.loc[ordered["call_share"] >= 0.65, "dominance"] = "call_dominant"
    ordered.loc[ordered["put_share"] >= 0.65, "dominance"] = "put_dominant"
    ordered["primary_family"] = ""
    ordered.loc[ordered["premium_burst"], "primary_family"] = "premium_burst"
    ordered.loc[ordered["concentrated_burst"], "primary_family"] = "concentrated_burst"
    ordered.loc[ordered["repeated_burst"], "primary_family"] = "repeated_burst"
    return ordered


def fetch_stock_bars(
    client: AlpacaClient,
    *,
    symbols: list[str],
    sessions: pd.DataFrame,
    stock_feed: str,
) -> dict[tuple[str, date], pd.DataFrame]:
    payload: dict[tuple[str, date], pd.DataFrame] = {}
    for symbol in symbols:
        print(f"Fetching stock bars for {symbol}", file=sys.stderr)
        for row in sessions.itertuples(index=False):
            bars = client.get_intraday_bars(
                symbol,
                start=to_iso_utc(row.market_open),
                end=to_iso_utc(row.market_close),
                stock_feed=stock_feed,
                timeframe="1Min",
            )
            minute_rows: list[dict[str, Any]] = []
            for bar in bars:
                timestamp = to_ny_timestamp(bar.timestamp)
                if timestamp is None:
                    continue
                minute_rows.append(
                    {
                        "timestamp": timestamp,
                        "open": float(bar.open),
                        "high": float(bar.high),
                        "low": float(bar.low),
                        "close": float(bar.close),
                        "volume": int(bar.volume),
                    }
                )
            if not minute_rows:
                payload[(symbol, row.session_date)] = pd.DataFrame()
                continue
            frame = pd.DataFrame(minute_rows).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
            frame = frame.set_index("timestamp")
            payload[(symbol, row.session_date)] = frame
    return payload


def add_forward_labels(
    events: pd.DataFrame,
    *,
    stock_bars: dict[tuple[str, date], pd.DataFrame],
    next_session_by_session: dict[date, date | None],
) -> pd.DataFrame:
    labeled = events.copy()
    label_rows: list[dict[str, Any]] = []
    for row in labeled.itertuples(index=False):
        current_session_bars = stock_bars.get((row.symbol, row.session_date))
        reference_timestamp = row.slot_start + pd.Timedelta(minutes=SLOT_MINUTES - 1)
        label_row: dict[str, Any] = {
            "reference_price": None,
            "forward_5m_return_pct": None,
            "forward_15m_return_pct": None,
            "forward_30m_return_pct": None,
            "forward_60m_return_pct": None,
            "forward_to_close_return_pct": None,
            "forward_next_open_return_pct": None,
            "forward_60m_max_up_pct": None,
            "forward_60m_max_down_pct": None,
        }
        if current_session_bars is None or current_session_bars.empty or reference_timestamp not in current_session_bars.index:
            label_rows.append(label_row)
            continue

        reference_price = float(current_session_bars.loc[reference_timestamp, "close"])
        label_row["reference_price"] = round(reference_price, 4)

        def close_return(minutes_forward: int) -> float | None:
            target_timestamp = reference_timestamp + pd.Timedelta(minutes=minutes_forward)
            if target_timestamp not in current_session_bars.index:
                return None
            target_close = float(current_session_bars.loc[target_timestamp, "close"])
            return round((target_close / reference_price - 1.0) * 100.0, 4)

        label_row["forward_5m_return_pct"] = close_return(5)
        label_row["forward_15m_return_pct"] = close_return(15)
        label_row["forward_30m_return_pct"] = close_return(30)
        label_row["forward_60m_return_pct"] = close_return(60)

        session_close = float(current_session_bars["close"].iloc[-1])
        label_row["forward_to_close_return_pct"] = round((session_close / reference_price - 1.0) * 100.0, 4)

        future_60 = current_session_bars[
            (current_session_bars.index > reference_timestamp)
            & (current_session_bars.index <= reference_timestamp + pd.Timedelta(minutes=60))
        ]
        if not future_60.empty:
            label_row["forward_60m_max_up_pct"] = round((float(future_60["high"].max()) / reference_price - 1.0) * 100.0, 4)
            label_row["forward_60m_max_down_pct"] = round((float(future_60["low"].min()) / reference_price - 1.0) * 100.0, 4)

        next_session = next_session_by_session.get(row.session_date)
        if next_session is not None:
            next_session_bars = stock_bars.get((row.symbol, next_session))
            if next_session_bars is not None and not next_session_bars.empty:
                next_open = float(next_session_bars["open"].iloc[0])
                label_row["forward_next_open_return_pct"] = round((next_open / reference_price - 1.0) * 100.0, 4)

        label_rows.append(label_row)

    labels = pd.DataFrame(label_rows)
    return pd.concat([labeled.reset_index(drop=True), labels], axis=1)


def build_summary_by_symbol(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for symbol, group in events.groupby("symbol", sort=True):
        rows.append(
            {
                "symbol": symbol,
                "events": int(len(group)),
                "concentrated_events": int(group["concentrated_burst"].sum()),
                "repeated_events": int(group["repeated_burst"].sum()),
                "call_dominant_events": int((group["dominance"] == "call_dominant").sum()),
                "put_dominant_events": int((group["dominance"] == "put_dominant").sum()),
                "mixed_events": int((group["dominance"] == "mixed").sum()),
                "avg_scoreable_premium": round(float(group["scoreable_premium"].mean()), 2),
                "avg_premium_vs_slot_p90": mean_pct(group["premium_vs_slot_p90"]),
                "avg_5m_return_pct": mean_pct(group["forward_5m_return_pct"]),
                "avg_15m_return_pct": mean_pct(group["forward_15m_return_pct"]),
                "avg_30m_return_pct": mean_pct(group["forward_30m_return_pct"]),
                "avg_60m_return_pct": mean_pct(group["forward_60m_return_pct"]),
                "avg_to_close_return_pct": mean_pct(group["forward_to_close_return_pct"]),
                "avg_next_open_return_pct": mean_pct(group["forward_next_open_return_pct"]),
                "hit_rate_15m_pct": positive_rate_pct(group["forward_15m_return_pct"]),
                "hit_rate_to_close_pct": positive_rate_pct(group["forward_to_close_return_pct"]),
                "hit_rate_next_open_pct": positive_rate_pct(group["forward_next_open_return_pct"]),
            }
        )
    return pd.DataFrame(rows)


def build_summary_by_bucket(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    grouped = events.groupby(["primary_family", "dominance", "time_bucket"], sort=True)
    for (primary_family, dominance, time_bucket), group in grouped:
        rows.append(
            {
                "primary_family": primary_family,
                "dominance": dominance,
                "time_bucket": time_bucket,
                "events": int(len(group)),
                "avg_scoreable_premium": round(float(group["scoreable_premium"].mean()), 2),
                "avg_15m_return_pct": mean_pct(group["forward_15m_return_pct"]),
                "avg_to_close_return_pct": mean_pct(group["forward_to_close_return_pct"]),
                "avg_next_open_return_pct": mean_pct(group["forward_next_open_return_pct"]),
                "hit_rate_15m_pct": positive_rate_pct(group["forward_15m_return_pct"]),
                "hit_rate_to_close_pct": positive_rate_pct(group["forward_to_close_return_pct"]),
                "hit_rate_next_open_pct": positive_rate_pct(group["forward_next_open_return_pct"]),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["primary_family", "dominance", "time_bucket"]
    ).reset_index(drop=True)


def render_report(
    *,
    output_dir: Path,
    symbols: list[str],
    analysis_dates: list[date],
    baseline_dates: list[date],
    contracts: pd.DataFrame,
    windows: pd.DataFrame,
    events: pd.DataFrame,
    summary_by_symbol: pd.DataFrame,
    summary_by_bucket: pd.DataFrame,
    exclusion_reason_counts: Counter[str],
) -> None:
    lines: list[str] = []
    lines.append("# Alpaca Option Flow Follow-Through")
    lines.append("")
    lines.append(f"- symbols: `{','.join(symbols)}`")
    lines.append(
        f"- analysis sessions: `{analysis_dates[0].isoformat()}` to `{analysis_dates[-1].isoformat()}` ({len(analysis_dates)} sessions)"
    )
    lines.append(f"- baseline sessions: `{len(baseline_dates)}`")
    lines.append(f"- allowed trade conditions: `{','.join(sorted(UOA_ALLOWED_TRADE_CONDITIONS))}`")
    lines.append(f"- tracked contracts: `{len(contracts)}`")
    lines.append(f"- analyzed windows: `{len(windows)}`")
    lines.append(f"- qualifying events: `{len(events)}`")
    lines.append("")
    if exclusion_reason_counts:
        lines.append("## Excluded Trade Reasons")
        lines.append("")
        lines.append("```text")
        for key, value in exclusion_reason_counts.most_common(10):
            lines.append(f"{key}: {value}")
        lines.append("```")
        lines.append("")
    if not summary_by_symbol.empty:
        lines.append("## Summary By Symbol")
        lines.append("")
        lines.append("```text")
        lines.append(summary_by_symbol.to_string(index=False))
        lines.append("```")
        lines.append("")
    if not summary_by_bucket.empty:
        lines.append("## Summary By Bucket")
        lines.append("")
        lines.append("```text")
        lines.append(summary_by_bucket.to_string(index=False))
        lines.append("```")
        lines.append("")
    (output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def write_outputs(
    *,
    output_dir: Path,
    events: pd.DataFrame,
    summary_by_symbol: pd.DataFrame,
    summary_by_bucket: pd.DataFrame,
    meta: dict[str, Any],
) -> None:
    events.to_csv(output_dir / "events.csv", index=False)
    summary_by_symbol.to_csv(output_dir / "summary_by_symbol.csv", index=False)
    summary_by_bucket.to_csv(output_dir / "summary_by_bucket.csv", index=False)
    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    symbols = normalize_symbols(args.symbols)
    end_date = resolve_end_date(args.end_date)
    start_date = None if not args.start_date else date.fromisoformat(args.start_date)
    working_schedule, analysis_dates, baseline_dates, next_session_by_session = choose_analysis_sessions(
        start_date=start_date,
        end_date=end_date,
        sessions=args.sessions,
        baseline_sessions=args.baseline_sessions,
    )
    if not analysis_dates:
        raise SystemExit("No analysis sessions selected")

    output_dir = make_output_dir(Path(args.output_root), args.label)
    print(f"Writing outputs to {output_dir}", file=sys.stderr)

    client = create_alpaca_client_from_env()
    contracts = fetch_contracts(
        client,
        symbols=symbols,
        expiration_start=working_schedule["session_date"].min(),
        expiration_end=analysis_dates[-1] + timedelta(days=args.max_dte),
    )
    if contracts.empty:
        raise SystemExit("No contracts found for the requested inputs")

    exclusion_reason_counts: Counter[str] = Counter()
    window_frames: list[pd.DataFrame] = []
    for row in working_schedule.itertuples(index=False):
        for symbol in symbols:
            session_contracts = select_session_contracts(
                contracts,
                symbol=symbol,
                session_date=row.session_date,
                max_dte=args.max_dte,
            )
            print(
                f"{row.session_date.isoformat()} {symbol}: {len(session_contracts)} contracts",
                file=sys.stderr,
            )
            trades = fetch_session_trades(
                client,
                session_contracts=session_contracts,
                session_date=row.session_date,
                market_open=row.market_open,
                market_close=row.market_close,
                batch_size=args.batch_size,
                exclusion_reason_counts=exclusion_reason_counts,
                progress_label=f"{row.session_date.isoformat()} {symbol}",
            )
            if trades.empty:
                print(f"{row.session_date.isoformat()} {symbol}: 0 trades", file=sys.stderr)
            else:
                print(
                    f"{row.session_date.isoformat()} {symbol}: {len(trades)} raw trades, "
                    f"{int(trades['included_in_score'].sum())} scoreable",
                    file=sys.stderr,
                )
            window_frames.append(
                summarize_session_windows(
                    symbol=symbol,
                    session_date=row.session_date,
                    market_open=row.market_open,
                    trades=trades,
                )
            )

    windows = pd.concat(window_frames, ignore_index=True)
    windows = add_slot_baselines(windows, baseline_sessions=args.baseline_sessions)
    windows = classify_event_windows(
        windows,
        baseline_sessions=args.baseline_sessions,
        min_scoreable_trades=args.min_scoreable_trades,
    )

    analysis_events = windows[
        windows["session_date"].isin(analysis_dates) & windows["premium_burst"]
    ].copy()

    analysis_plus_next = sorted(
        {
            *analysis_dates,
            *[
                next_session
                for session_date in analysis_dates
                if (next_session := next_session_by_session.get(session_date)) is not None
            ],
        }
    )
    stock_sessions = working_schedule[working_schedule["session_date"].isin(analysis_plus_next)].copy()
    stock_bars = fetch_stock_bars(
        client,
        symbols=symbols,
        sessions=stock_sessions,
        stock_feed=args.stock_feed,
    )
    analysis_events = add_forward_labels(
        analysis_events,
        stock_bars=stock_bars,
        next_session_by_session=next_session_by_session,
    )

    selected_columns = [
        "symbol",
        "session_date",
        "slot_time",
        "time_bucket",
        "primary_family",
        "dominance",
        "scoreable_trade_count",
        "scoreable_premium",
        "distinct_contract_count",
        "largest_contract_share",
        "largest_expiry_share",
        "call_share",
        "put_share",
        "near_dte_share",
        "far_dte_share",
        "premium_vs_slot_median",
        "premium_vs_slot_p90",
        "forward_5m_return_pct",
        "forward_15m_return_pct",
        "forward_30m_return_pct",
        "forward_60m_return_pct",
        "forward_to_close_return_pct",
        "forward_next_open_return_pct",
        "forward_60m_max_up_pct",
        "forward_60m_max_down_pct",
        "raw_trade_count",
        "raw_premium",
        "excluded_trade_count",
        "excluded_premium",
    ]
    events = analysis_events[selected_columns].sort_values(
        ["symbol", "session_date", "slot_time"]
    ).reset_index(drop=True)
    summary_by_symbol = build_summary_by_symbol(analysis_events)
    summary_by_bucket = build_summary_by_bucket(analysis_events)

    meta = {
        "symbols": symbols,
        "analysis_start": analysis_dates[0].isoformat(),
        "analysis_end": analysis_dates[-1].isoformat(),
        "analysis_sessions": len(analysis_dates),
        "baseline_sessions": len(baseline_dates),
        "baseline_required": int(args.baseline_sessions),
        "max_dte": int(args.max_dte),
        "allowed_conditions": sorted(UOA_ALLOWED_TRADE_CONDITIONS),
        "contract_count": int(len(contracts)),
        "window_count": int(len(windows)),
        "event_count": int(len(events)),
        "excluded_trade_reasons": dict(exclusion_reason_counts),
        "output_dir": str(output_dir),
    }

    write_outputs(
        output_dir=output_dir,
        events=events,
        summary_by_symbol=summary_by_symbol,
        summary_by_bucket=summary_by_bucket,
        meta=meta,
    )
    render_report(
        output_dir=output_dir,
        symbols=symbols,
        analysis_dates=analysis_dates,
        baseline_dates=baseline_dates,
        contracts=contracts,
        windows=windows,
        events=events,
        summary_by_symbol=summary_by_symbol,
        summary_by_bucket=summary_by_bucket,
        exclusion_reason_counts=exclusion_reason_counts,
    )

    print(f"Wrote {output_dir / 'report.md'}")
    if not summary_by_symbol.empty:
        print(summary_by_symbol.to_string(index=False))
    else:
        print("No qualifying events found in the analysis window.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
