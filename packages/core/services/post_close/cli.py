from __future__ import annotations

import argparse
from typing import Any

from core.runtime.config import default_database_url


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize persisted intraday collector sessions and replay spread outcomes."
        )
    )
    parser.add_argument(
        "--db",
        default=default_database_url(),
        help=(
            "Database target. Default: SPREADS_DATABASE_URL / DATABASE_URL / local Docker Postgres."
        ),
    )
    parser.add_argument(
        "--date",
        default="today",
        help=(
            "Session date in YYYY-MM-DD format using the New York trading date. Default: today"
        ),
    )
    parser.add_argument(
        "--label",
        default="explore_10_combined_0dte_auto",
        help="Collector label to summarize.",
    )
    parser.add_argument(
        "--replay-profit-target",
        type=float,
        default=0.5,
        help="Profit target used for replay verdicts. Default: 0.5",
    )
    parser.add_argument(
        "--replay-stop-multiple",
        type=float,
        default=2.0,
        help="Stop multiple used for replay verdicts. Default: 2.0",
    )
    return parser.parse_args(argv)


def build_analysis_args(overrides: dict[str, Any] | None = None) -> argparse.Namespace:
    args = parse_args([])
    for key, value in (overrides or {}).items():
        setattr(args, key, value)
    return args
