#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_ROOT = ROOT / "outputs" / "analysis" / "alpaca_flow_followthrough"
DEFAULT_OUTPUT_ROOT = ROOT / "outputs" / "analysis" / "alpaca_flow_regime_matrix"
NEW_YORK = ZoneInfo("America/New_York")
BULL_REGIMES = {"bull", "bullish", "up", "uptrend"}
BEAR_REGIMES = {"bear", "bearish", "down", "downtrend"}
REQUIRED_COLUMNS = {
    "symbol",
    "session_date",
    "time_bucket",
    "primary_family",
    "dominance",
    "scoreable_premium",
    "forward_15m_return_pct",
    "forward_to_close_return_pct",
    "forward_next_open_return_pct",
    "forward_60m_max_up_pct",
    "forward_60m_max_down_pct",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read completed Alpaca flow follow-through event files and build a "
            "regime-by-flow matrix across bullish and bearish runs."
        )
    )
    parser.add_argument(
        "--run",
        action="append",
        required=True,
        help=(
            "Run spec in REGIME=LABEL form. Repeat the flag for multiple runs. "
            "Supported regimes: bull, bear."
        ),
    )
    parser.add_argument(
        "--source-root",
        default=str(DEFAULT_SOURCE_ROOT),
        help=f"Root directory containing follow-through run outputs. Default: {DEFAULT_SOURCE_ROOT}",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help=f"Root directory for regime matrix outputs. Default: {DEFAULT_OUTPUT_ROOT}",
    )
    parser.add_argument(
        "--label",
        help="Optional output label. Default: generated timestamp",
    )
    parser.add_argument(
        "--min-events",
        type=int,
        default=5,
        help="Minimum support for report highlight tables. Default: 5",
    )
    return parser.parse_args(argv)


def normalize_regime(raw: str) -> str:
    value = raw.strip().lower()
    if value in BULL_REGIMES:
        return "bull"
    if value in BEAR_REGIMES:
        return "bear"
    raise SystemExit(f"Unsupported regime '{raw}'. Use bull or bear.")


def parse_run_specs(run_specs: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in run_specs:
        if "=" not in item:
            raise SystemExit(f"Invalid --run value '{item}'. Expected REGIME=LABEL.")
        raw_regime, raw_label = item.split("=", 1)
        regime = normalize_regime(raw_regime)
        label = raw_label.strip()
        if not label:
            raise SystemExit(f"Invalid --run value '{item}'. Label is required.")
        if regime in parsed:
            raise SystemExit(f"Duplicate regime '{regime}' in --run arguments.")
        parsed[regime] = label
    if not parsed:
        raise SystemExit("At least one --run is required.")
    return parsed


def make_output_dir(root: Path, label: str | None) -> Path:
    resolved_label = label or datetime.now(NEW_YORK).strftime("%Y%m%d_%H%M%S")
    output_dir = root / resolved_label
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def regime_direction(regime: str) -> int:
    return 1 if regime == "bull" else -1


def classify_alignment(*, regime: str, dominance: str) -> str:
    if dominance == "mixed":
        return "mixed"
    if regime == "bull":
        return "aligned" if dominance == "call_dominant" else "counter_regime"
    return "aligned" if dominance == "put_dominant" else "counter_regime"


def bool_rate_pct(series: pd.Series) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return round(float(cleaned.mean() * 100.0), 2)


def positive_rate_pct(series: pd.Series) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return round(float((cleaned > 0).mean() * 100.0), 2)


def mean_value(series: pd.Series) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    return round(float(cleaned.mean()), 4)


def opposite_nonzero_sign(left: pd.Series, right: pd.Series) -> pd.Series:
    left_sign = left.apply(lambda value: None if pd.isna(value) or value == 0 else value > 0)
    right_sign = right.apply(lambda value: None if pd.isna(value) or value == 0 else value > 0)
    return pd.Series(
        [
            None if a is None or b is None else a != b
            for a, b in zip(left_sign.tolist(), right_sign.tolist(), strict=False)
        ],
        index=left.index,
        dtype="object",
    )


def load_events(source_root: Path, run_specs: dict[str, str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for regime, label in sorted(run_specs.items()):
        path = source_root / label / "events.csv"
        if not path.exists():
            raise SystemExit(f"Missing events file: {path}")
        frame = pd.read_csv(path)
        missing = sorted(REQUIRED_COLUMNS.difference(frame.columns))
        if missing:
            raise SystemExit(f"{path} is missing required columns: {', '.join(missing)}")
        frame = frame.copy()
        frame["regime"] = regime
        frame["regime_direction"] = regime_direction(regime)
        frame["source_label"] = label
        frame["alignment"] = frame["dominance"].apply(lambda value: classify_alignment(regime=regime, dominance=value))
        frame["regime_consistent_15m"] = frame["forward_15m_return_pct"].apply(
            lambda value: None if pd.isna(value) else value * regime_direction(regime) > 0
        )
        frame["regime_consistent_to_close"] = frame["forward_to_close_return_pct"].apply(
            lambda value: None if pd.isna(value) else value * regime_direction(regime) > 0
        )
        frame["regime_consistent_next_open"] = frame["forward_next_open_return_pct"].apply(
            lambda value: None if pd.isna(value) else value * regime_direction(regime) > 0
        )
        frame["flip_15m_to_close"] = opposite_nonzero_sign(
            frame["forward_15m_return_pct"], frame["forward_to_close_return_pct"]
        )
        frame["flip_to_close_to_next_open"] = opposite_nonzero_sign(
            frame["forward_to_close_return_pct"], frame["forward_next_open_return_pct"]
        )
        frames.append(frame)
    if not frames:
        raise SystemExit("No runs loaded")
    combined = pd.concat(frames, ignore_index=True)
    combined["session_date"] = combined["session_date"].astype(str)
    return combined


def build_summary(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in frame.groupby(group_columns, sort=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_columns, keys, strict=False))
        row.update(
            {
                "events": int(len(group)),
                "avg_scoreable_premium": round(float(group["scoreable_premium"].mean()), 2),
                "avg_15m_return_pct": mean_value(group["forward_15m_return_pct"]),
                "avg_to_close_return_pct": mean_value(group["forward_to_close_return_pct"]),
                "avg_next_open_return_pct": mean_value(group["forward_next_open_return_pct"]),
                "avg_60m_max_up_pct": mean_value(group["forward_60m_max_up_pct"]),
                "avg_60m_max_down_pct": mean_value(group["forward_60m_max_down_pct"]),
                "hit_rate_15m_pct": positive_rate_pct(group["forward_15m_return_pct"]),
                "hit_rate_to_close_pct": positive_rate_pct(group["forward_to_close_return_pct"]),
                "hit_rate_next_open_pct": positive_rate_pct(group["forward_next_open_return_pct"]),
                "regime_consistent_15m_pct": bool_rate_pct(group["regime_consistent_15m"]),
                "regime_consistent_to_close_pct": bool_rate_pct(group["regime_consistent_to_close"]),
                "regime_consistent_next_open_pct": bool_rate_pct(group["regime_consistent_next_open"]),
                "flip_15m_to_close_pct": bool_rate_pct(group["flip_15m_to_close"]),
                "flip_to_close_to_next_open_pct": bool_rate_pct(group["flip_to_close_to_next_open"]),
            }
        )
        rows.append(row)
    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary
    if "regime" in summary.columns:
        summary["regime_adjusted_avg_15m_pct"] = summary.apply(
            lambda row: None
            if pd.isna(row["avg_15m_return_pct"])
            else round(row["avg_15m_return_pct"] * regime_direction(str(row["regime"])), 4),
            axis=1,
        )
        summary["regime_adjusted_avg_to_close_pct"] = summary.apply(
            lambda row: None
            if pd.isna(row["avg_to_close_return_pct"])
            else round(row["avg_to_close_return_pct"] * regime_direction(str(row["regime"])), 4),
            axis=1,
        )
        summary["regime_adjusted_avg_next_open_pct"] = summary.apply(
            lambda row: None
            if pd.isna(row["avg_next_open_return_pct"])
            else round(row["avg_next_open_return_pct"] * regime_direction(str(row["regime"])), 4),
            axis=1,
        )
    return summary.sort_values(group_columns).reset_index(drop=True)


def make_support_table(
    summary: pd.DataFrame,
    *,
    min_events: int,
    sort_columns: list[str],
    ascending: list[bool],
) -> pd.DataFrame:
    filtered = summary[summary["events"] >= min_events].copy()
    if filtered.empty:
        return filtered
    return filtered.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)


def render_table(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["(no rows)"]
    return frame.to_string(index=False).splitlines()


def render_report(
    *,
    output_dir: Path,
    run_specs: dict[str, str],
    combined_events: pd.DataFrame,
    summary_by_alignment: pd.DataFrame,
    summary_by_dominance: pd.DataFrame,
    summary_by_time_bucket: pd.DataFrame,
    best_next_open: pd.DataFrame,
    biggest_15m_to_close_flips: pd.DataFrame,
    min_events: int,
) -> None:
    lines: list[str] = []
    lines.append("# Alpaca Flow Regime Matrix")
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    for regime, label in sorted(run_specs.items()):
        lines.append(f"- `{regime}` -> `{label}`")
    lines.append(f"- combined events: `{len(combined_events)}`")
    lines.append(f"- report support threshold: `{min_events}` events")
    lines.append("")
    lines.append("## Summary By Alignment")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(summary_by_alignment))
    lines.append("```")
    lines.append("")
    lines.append("## Summary By Regime / Symbol / Dominance")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(summary_by_dominance))
    lines.append("```")
    lines.append("")
    lines.append(f"## Summary By Time Bucket (support >= {min_events})")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(summary_by_time_bucket))
    lines.append("```")
    lines.append("")
    lines.append(f"## Best Regime-Adjusted Next-Open Buckets (support >= {min_events})")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(best_next_open))
    lines.append("```")
    lines.append("")
    lines.append(f"## Highest 15m-To-Close Flip Buckets (support >= {min_events})")
    lines.append("")
    lines.append("```text")
    lines.extend(render_table(biggest_15m_to_close_flips))
    lines.append("```")
    lines.append("")
    (output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def write_outputs(
    *,
    output_dir: Path,
    combined_events: pd.DataFrame,
    summary_by_alignment: pd.DataFrame,
    summary_by_dominance: pd.DataFrame,
    summary_by_time_bucket: pd.DataFrame,
    best_next_open: pd.DataFrame,
    biggest_15m_to_close_flips: pd.DataFrame,
    meta: dict[str, Any],
) -> None:
    combined_events.to_csv(output_dir / "combined_events.csv", index=False)
    summary_by_alignment.to_csv(output_dir / "summary_by_alignment.csv", index=False)
    summary_by_dominance.to_csv(output_dir / "summary_by_dominance.csv", index=False)
    summary_by_time_bucket.to_csv(output_dir / "summary_by_time_bucket.csv", index=False)
    best_next_open.to_csv(output_dir / "best_next_open_buckets.csv", index=False)
    biggest_15m_to_close_flips.to_csv(output_dir / "largest_15m_to_close_flips.csv", index=False)
    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_specs = parse_run_specs(args.run)
    output_dir = make_output_dir(Path(args.output_root), args.label)
    combined_events = load_events(Path(args.source_root), run_specs)

    summary_by_alignment = build_summary(combined_events, ["regime", "symbol", "alignment"])
    summary_by_dominance = build_summary(combined_events, ["regime", "symbol", "dominance"])
    full_time_bucket_summary = build_summary(combined_events, ["regime", "symbol", "dominance", "time_bucket"])
    summary_by_time_bucket = make_support_table(
        full_time_bucket_summary,
        min_events=args.min_events,
        sort_columns=["regime", "symbol", "dominance", "time_bucket"],
        ascending=[True, True, True, True],
    )
    best_next_open = make_support_table(
        full_time_bucket_summary,
        min_events=args.min_events,
        sort_columns=["regime_adjusted_avg_next_open_pct", "events"],
        ascending=[False, False],
    ).head(12)
    biggest_15m_to_close_flips = make_support_table(
        full_time_bucket_summary,
        min_events=args.min_events,
        sort_columns=["flip_15m_to_close_pct", "events"],
        ascending=[False, False],
    ).head(12)

    meta = {
        "created_at": datetime.now(NEW_YORK).isoformat(),
        "run_specs": run_specs,
        "combined_events": int(len(combined_events)),
        "min_events": int(args.min_events),
    }
    write_outputs(
        output_dir=output_dir,
        combined_events=combined_events,
        summary_by_alignment=summary_by_alignment,
        summary_by_dominance=summary_by_dominance,
        summary_by_time_bucket=summary_by_time_bucket,
        best_next_open=best_next_open,
        biggest_15m_to_close_flips=biggest_15m_to_close_flips,
        meta=meta,
    )
    render_report(
        output_dir=output_dir,
        run_specs=run_specs,
        combined_events=combined_events,
        summary_by_alignment=summary_by_alignment,
        summary_by_dominance=summary_by_dominance,
        summary_by_time_bucket=summary_by_time_bucket,
        best_next_open=best_next_open,
        biggest_15m_to_close_flips=biggest_15m_to_close_flips,
        min_events=args.min_events,
    )
    print(f"Wrote {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
