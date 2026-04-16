#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ROOT / "packages"
if str(PACKAGES) not in sys.path:
    sys.path.insert(0, str(PACKAGES))

from core.services.alpaca import create_alpaca_client_from_env  # noqa: E402
from core.services.scanner import AlpacaClient  # noqa: E402

NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_SOURCE_ROOT = ROOT / "outputs" / "analysis" / "alpaca_flow_regime_matrix"
DEFAULT_OUTPUT_ROOT = (
    ROOT / "outputs" / "analysis" / "alpaca_flow_counter_regime_context"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a completed Alpaca flow regime matrix run, attach stock gap / prior-day "
            "context from Alpaca daily bars, and summarize counter-regime flow buckets."
        )
    )
    parser.add_argument(
        "--source-label",
        required=True,
        help="Input regime matrix label under outputs/analysis/alpaca_flow_regime_matrix/",
    )
    parser.add_argument(
        "--source-root",
        default=str(DEFAULT_SOURCE_ROOT),
        help=f"Root directory containing regime matrix outputs. Default: {DEFAULT_SOURCE_ROOT}",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help=f"Root directory for context analysis outputs. Default: {DEFAULT_OUTPUT_ROOT}",
    )
    parser.add_argument(
        "--label",
        help="Optional output label. Default: generated timestamp",
    )
    parser.add_argument(
        "--stock-feed",
        default="sip",
        choices=("sip", "iex", "delayed_sip", "boats", "overnight"),
        help="Alpaca stock feed for daily bars. Default: sip",
    )
    parser.add_argument(
        "--gap-flat-threshold-pct",
        type=float,
        default=0.15,
        help="Absolute gap threshold below which the session is treated as flat. Default: 0.15",
    )
    parser.add_argument(
        "--prior-day-flat-threshold-pct",
        type=float,
        default=0.20,
        help="Absolute prior-day return threshold below which the prior day is treated as flat. Default: 0.20",
    )
    parser.add_argument(
        "--min-events",
        type=int,
        default=3,
        help="Minimum support for highlighted report tables. Default: 3",
    )
    return parser.parse_args(argv)


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


def regime_direction(regime: str) -> int:
    return 1 if regime == "bull" else -1


def mean_value(series: pd.Series) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return round(float(cleaned.mean()), 4)


def positive_rate_pct(series: pd.Series) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return round(float((cleaned > 0).mean() * 100.0), 2)


def bool_rate_pct(series: pd.Series) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return round(float(cleaned.mean() * 100.0), 2)


def opposite_nonzero_sign(left: pd.Series, right: pd.Series) -> pd.Series:
    left_sign = left.apply(
        lambda value: None if pd.isna(value) or value == 0 else value > 0
    )
    right_sign = right.apply(
        lambda value: None if pd.isna(value) or value == 0 else value > 0
    )
    return pd.Series(
        [
            None if a is None or b is None else a != b
            for a, b in zip(left_sign.tolist(), right_sign.tolist(), strict=False)
        ],
        index=left.index,
        dtype="object",
    )


def classify_signed_context(change_pct: float | None, flat_threshold_pct: float) -> str:
    if change_pct is None or pd.isna(change_pct):
        return "unknown"
    if abs(float(change_pct)) < flat_threshold_pct:
        return "flat"
    return "up" if float(change_pct) > 0 else "down"


def classify_regime_relative_context(
    change_pct: float | None, *, regime: str, flat_threshold_pct: float
) -> str:
    if change_pct is None or pd.isna(change_pct):
        return "unknown"
    if abs(float(change_pct)) < flat_threshold_pct:
        return "flat"
    return (
        "with_regime"
        if float(change_pct) * regime_direction(regime) > 0
        else "against_regime"
    )


def load_counter_regime_events(source_root: Path, source_label: str) -> pd.DataFrame:
    path = source_root / source_label / "combined_events.csv"
    if not path.exists():
        raise SystemExit(f"Missing combined events file: {path}")
    frame = pd.read_csv(path)
    required_columns = {
        "symbol",
        "session_date",
        "time_bucket",
        "dominance",
        "scoreable_premium",
        "forward_15m_return_pct",
        "forward_to_close_return_pct",
        "forward_next_open_return_pct",
        "forward_60m_max_up_pct",
        "forward_60m_max_down_pct",
        "regime",
        "alignment",
    }
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise SystemExit(f"{path} is missing required columns: {', '.join(missing)}")
    filtered = frame[frame["alignment"] == "counter_regime"].copy()
    if filtered.empty:
        raise SystemExit("No counter-regime events found in the source label")
    filtered["session_date"] = pd.to_datetime(filtered["session_date"]).dt.date
    filtered["flip_15m_to_close"] = opposite_nonzero_sign(
        filtered["forward_15m_return_pct"], filtered["forward_to_close_return_pct"]
    )
    filtered["flip_to_close_to_next_open"] = opposite_nonzero_sign(
        filtered["forward_to_close_return_pct"],
        filtered["forward_next_open_return_pct"],
    )
    return filtered.reset_index(drop=True)


def fetch_daily_bars(
    client: AlpacaClient,
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
    stock_feed: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        payload = get_json_with_retry(
            client,
            base_url=client.data_base_url,
            path="/v2/stocks/bars",
            params={
                "symbols": ",".join(symbols),
                "timeframe": "1Day",
                "start": f"{start_date.isoformat()}T00:00:00Z",
                "end": f"{end_date.isoformat()}T23:59:59Z",
                "adjustment": "raw",
                "feed": stock_feed,
                "limit": 1000,
                "sort": "asc",
                "page_token": page_token,
            },
        )
        bars_by_symbol = payload.get("bars", {}) if isinstance(payload, dict) else {}
        for symbol, bars in bars_by_symbol.items():
            for bar in bars:
                timestamp = pd.Timestamp(bar.get("t"))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.tz_localize("UTC")
                session_date = timestamp.tz_convert(NEW_YORK).date()
                rows.append(
                    {
                        "symbol": str(symbol).upper(),
                        "session_date": session_date,
                        "open": float(bar.get("o") or 0.0),
                        "high": float(bar.get("h") or 0.0),
                        "low": float(bar.get("l") or 0.0),
                        "close": float(bar.get("c") or 0.0),
                        "volume": int(bar.get("v") or 0),
                    }
                )
        page_token = (
            payload.get("next_page_token") if isinstance(payload, dict) else None
        )
        if not page_token:
            break
    if not rows:
        raise SystemExit("No Alpaca daily bars returned for the requested date range")
    frame = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["symbol", "session_date"])
        .sort_values(["symbol", "session_date"])
    )
    return frame.reset_index(drop=True)


def build_daily_context(
    daily_bars: pd.DataFrame,
    *,
    gap_flat_threshold_pct: float,
    prior_day_flat_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for symbol, group in daily_bars.groupby("symbol", sort=True):
        ordered = group.sort_values("session_date").reset_index(drop=True)
        for index, row in ordered.iterrows():
            prev_row = None if index == 0 else ordered.iloc[index - 1]
            gap_pct = None
            prior_day_return_pct = None
            prior_2day_return_pct = None
            if prev_row is not None and prev_row["close"] > 0:
                gap_pct = round(
                    (float(row["open"]) / float(prev_row["close"]) - 1.0) * 100.0, 4
                )
                if float(prev_row["open"]) > 0:
                    prior_day_return_pct = round(
                        (float(prev_row["close"]) / float(prev_row["open"]) - 1.0)
                        * 100.0,
                        4,
                    )
            if (
                index >= 2
                and float(ordered.iloc[index - 2]["close"]) > 0
                and prev_row is not None
            ):
                prior_2day_return_pct = round(
                    (
                        float(prev_row["close"])
                        / float(ordered.iloc[index - 2]["close"])
                        - 1.0
                    )
                    * 100.0,
                    4,
                )
            rows.append(
                {
                    "symbol": symbol,
                    "session_date": row["session_date"],
                    "session_open": round(float(row["open"]), 4),
                    "session_close": round(float(row["close"]), 4),
                    "gap_pct": gap_pct,
                    "gap_direction": classify_signed_context(
                        gap_pct, gap_flat_threshold_pct
                    ),
                    "prior_day_return_pct": prior_day_return_pct,
                    "prior_day_direction": classify_signed_context(
                        prior_day_return_pct, prior_day_flat_threshold_pct
                    ),
                    "prior_2day_return_pct": prior_2day_return_pct,
                }
            )
    return pd.DataFrame(rows)


def attach_context(
    events: pd.DataFrame,
    context: pd.DataFrame,
    *,
    gap_flat_threshold_pct: float,
    prior_day_flat_threshold_pct: float,
) -> pd.DataFrame:
    merged = events.merge(context, on=["symbol", "session_date"], how="left")
    if merged["gap_pct"].isna().any():
        missing = merged[merged["gap_pct"].isna()][
            ["symbol", "session_date"]
        ].drop_duplicates()
        missing_pairs = ", ".join(
            f"{row.symbol}:{row.session_date.isoformat()}"
            for row in missing.itertuples(index=False)
        )
        raise SystemExit(f"Missing daily context for some events: {missing_pairs}")

    merged["gap_context"] = merged.apply(
        lambda row: classify_regime_relative_context(
            row["gap_pct"],
            regime=str(row["regime"]),
            flat_threshold_pct=gap_flat_threshold_pct,
        ),
        axis=1,
    )
    merged["prior_day_context"] = merged.apply(
        lambda row: classify_regime_relative_context(
            row["prior_day_return_pct"],
            regime=str(row["regime"]),
            flat_threshold_pct=prior_day_flat_threshold_pct,
        ),
        axis=1,
    )
    merged["joint_context"] = merged.apply(
        lambda row: f"gap_{row['gap_context']}__prior_{row['prior_day_context']}",
        axis=1,
    )
    merged["regime_adjusted_15m_pct"] = merged.apply(
        lambda row: None
        if pd.isna(row["forward_15m_return_pct"])
        else round(
            float(row["forward_15m_return_pct"]) * regime_direction(str(row["regime"])),
            4,
        ),
        axis=1,
    )
    merged["regime_adjusted_to_close_pct"] = merged.apply(
        lambda row: None
        if pd.isna(row["forward_to_close_return_pct"])
        else round(
            float(row["forward_to_close_return_pct"])
            * regime_direction(str(row["regime"])),
            4,
        ),
        axis=1,
    )
    merged["regime_adjusted_next_open_pct"] = merged.apply(
        lambda row: None
        if pd.isna(row["forward_next_open_return_pct"])
        else round(
            float(row["forward_next_open_return_pct"])
            * regime_direction(str(row["regime"])),
            4,
        ),
        axis=1,
    )
    return merged


def build_summary(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in frame.groupby(group_columns, sort=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        session_count = int(group["session_date"].nunique())
        row = dict(zip(group_columns, keys, strict=False))
        row.update(
            {
                "events": int(len(group)),
                "sessions": session_count,
                "avg_events_per_session": round(float(len(group) / session_count), 2)
                if session_count > 0
                else None,
                "avg_scoreable_premium": round(
                    float(group["scoreable_premium"].mean()), 2
                ),
                "avg_gap_pct": mean_value(group["gap_pct"]),
                "avg_prior_day_return_pct": mean_value(group["prior_day_return_pct"]),
                "avg_15m_return_pct": mean_value(group["forward_15m_return_pct"]),
                "avg_to_close_return_pct": mean_value(
                    group["forward_to_close_return_pct"]
                ),
                "avg_next_open_return_pct": mean_value(
                    group["forward_next_open_return_pct"]
                ),
                "hit_rate_15m_pct": positive_rate_pct(group["forward_15m_return_pct"]),
                "hit_rate_to_close_pct": positive_rate_pct(
                    group["forward_to_close_return_pct"]
                ),
                "hit_rate_next_open_pct": positive_rate_pct(
                    group["forward_next_open_return_pct"]
                ),
                "flip_15m_to_close_pct": bool_rate_pct(group["flip_15m_to_close"]),
                "flip_to_close_to_next_open_pct": bool_rate_pct(
                    group["flip_to_close_to_next_open"]
                ),
                "regime_adjusted_avg_15m_pct": mean_value(
                    group["regime_adjusted_15m_pct"]
                ),
                "regime_adjusted_avg_to_close_pct": mean_value(
                    group["regime_adjusted_to_close_pct"]
                ),
                "regime_adjusted_avg_next_open_pct": mean_value(
                    group["regime_adjusted_next_open_pct"]
                ),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(group_columns).reset_index(drop=True)


def make_support_table(
    summary: pd.DataFrame,
    *,
    min_events: int,
    sort_columns: list[str],
    ascending: list[bool],
    limit: int | None = None,
) -> pd.DataFrame:
    filtered = summary[summary["events"] >= min_events].copy()
    if filtered.empty:
        return filtered
    ordered = filtered.sort_values(sort_columns, ascending=ascending).reset_index(
        drop=True
    )
    return ordered if limit is None else ordered.head(limit)


def render_table(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["(no rows)"]
    return frame.to_string(index=False).splitlines()


def render_report(
    *,
    output_dir: Path,
    source_label: str,
    events_with_context: pd.DataFrame,
    summary_by_context: pd.DataFrame,
    summary_by_time_bucket: pd.DataFrame,
    best_to_close: pd.DataFrame,
    best_next_open: pd.DataFrame,
    highest_flip: pd.DataFrame,
    gap_flat_threshold_pct: float,
    prior_day_flat_threshold_pct: float,
    min_events: int,
) -> None:
    lines: list[str] = []
    lines.append("# Alpaca Counter-Regime Context Analysis")
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    lines.append(f"- source label: `{source_label}`")
    lines.append(f"- counter-regime events: `{len(events_with_context)}`")
    lines.append(f"- gap flat threshold: `{gap_flat_threshold_pct:.2f}%`")
    lines.append(f"- prior-day flat threshold: `{prior_day_flat_threshold_pct:.2f}%`")
    lines.append("")
    lines.append("## Definitions")
    lines.append("")
    lines.append("- overnight gap: current session open vs prior session close")
    lines.append("- prior-day trend: prior session close vs prior session open")
    lines.append(
        "- with-regime / against-regime are classified relative to the run regime, not raw up/down direction"
    )
    lines.append("")
    lines.append("## Summary By Context")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(summary_by_context))
    lines.append("```")
    lines.append("")
    lines.append(f"## Summary By Time Bucket (support >= {min_events})")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(summary_by_time_bucket))
    lines.append("```")
    lines.append("")
    lines.append(f"## Best Same-Day Buckets (support >= {min_events})")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(best_to_close))
    lines.append("```")
    lines.append("")
    lines.append(f"## Best Next-Open Buckets (support >= {min_events})")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(best_next_open))
    lines.append("```")
    lines.append("")
    lines.append(f"## Highest 15m-To-Close Flip Buckets (support >= {min_events})")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(highest_flip))
    lines.append("```")
    lines.append("")
    (output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def write_outputs(
    *,
    output_dir: Path,
    events_with_context: pd.DataFrame,
    summary_by_context: pd.DataFrame,
    summary_by_time_bucket: pd.DataFrame,
    best_to_close: pd.DataFrame,
    best_next_open: pd.DataFrame,
    highest_flip: pd.DataFrame,
    meta: dict[str, Any],
) -> None:
    events_with_context.to_csv(
        output_dir / "counter_regime_events_with_context.csv", index=False
    )
    summary_by_context.to_csv(output_dir / "summary_by_context.csv", index=False)
    summary_by_time_bucket.to_csv(
        output_dir / "summary_by_time_bucket.csv", index=False
    )
    best_to_close.to_csv(output_dir / "best_to_close_buckets.csv", index=False)
    best_next_open.to_csv(output_dir / "best_next_open_buckets.csv", index=False)
    highest_flip.to_csv(output_dir / "highest_flip_buckets.csv", index=False)
    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_root = Path(args.source_root)
    output_dir = make_output_dir(Path(args.output_root), args.label)
    events = load_counter_regime_events(source_root, args.source_label)

    symbols = sorted(
        events["symbol"].dropna().astype(str).str.upper().unique().tolist()
    )
    min_session = min(events["session_date"])
    max_session = max(events["session_date"])

    client = create_alpaca_client_from_env()
    daily_bars = fetch_daily_bars(
        client,
        symbols=symbols,
        start_date=min_session - timedelta(days=10),
        end_date=max_session + timedelta(days=2),
        stock_feed=args.stock_feed,
    )
    daily_context = build_daily_context(
        daily_bars,
        gap_flat_threshold_pct=args.gap_flat_threshold_pct,
        prior_day_flat_threshold_pct=args.prior_day_flat_threshold_pct,
    )
    events_with_context = attach_context(
        events,
        daily_context,
        gap_flat_threshold_pct=args.gap_flat_threshold_pct,
        prior_day_flat_threshold_pct=args.prior_day_flat_threshold_pct,
    )

    summary_by_context = build_summary(
        events_with_context,
        ["regime", "symbol", "gap_context", "prior_day_context"],
    )
    full_time_bucket_summary = build_summary(
        events_with_context,
        ["regime", "symbol", "time_bucket", "gap_context", "prior_day_context"],
    )
    summary_by_time_bucket = make_support_table(
        full_time_bucket_summary,
        min_events=args.min_events,
        sort_columns=[
            "regime",
            "symbol",
            "time_bucket",
            "gap_context",
            "prior_day_context",
        ],
        ascending=[True, True, True, True, True],
    )
    best_to_close = make_support_table(
        full_time_bucket_summary,
        min_events=args.min_events,
        sort_columns=["regime_adjusted_avg_to_close_pct", "events"],
        ascending=[False, False],
        limit=12,
    )
    best_next_open = make_support_table(
        full_time_bucket_summary,
        min_events=args.min_events,
        sort_columns=["regime_adjusted_avg_next_open_pct", "events"],
        ascending=[False, False],
        limit=12,
    )
    highest_flip = make_support_table(
        full_time_bucket_summary,
        min_events=args.min_events,
        sort_columns=["flip_15m_to_close_pct", "events"],
        ascending=[False, False],
        limit=12,
    )

    meta = {
        "created_at": datetime.now(NEW_YORK).isoformat(),
        "source_label": args.source_label,
        "counter_regime_events": int(len(events_with_context)),
        "gap_flat_threshold_pct": float(args.gap_flat_threshold_pct),
        "prior_day_flat_threshold_pct": float(args.prior_day_flat_threshold_pct),
        "symbols": symbols,
    }
    write_outputs(
        output_dir=output_dir,
        events_with_context=events_with_context,
        summary_by_context=summary_by_context,
        summary_by_time_bucket=summary_by_time_bucket,
        best_to_close=best_to_close,
        best_next_open=best_next_open,
        highest_flip=highest_flip,
        meta=meta,
    )
    render_report(
        output_dir=output_dir,
        source_label=args.source_label,
        events_with_context=events_with_context,
        summary_by_context=summary_by_context,
        summary_by_time_bucket=summary_by_time_bucket,
        best_to_close=best_to_close,
        best_next_open=best_next_open,
        highest_flip=highest_flip,
        gap_flat_threshold_pct=args.gap_flat_threshold_pct,
        prior_day_flat_threshold_pct=args.prior_day_flat_threshold_pct,
        min_events=args.min_events,
    )
    print(f"Wrote {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
