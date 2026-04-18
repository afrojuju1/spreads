from __future__ import annotations

import argparse
from typing import Any

from core.services.market_dates import resolve_market_date

from .cli import parse_args
from .rendering import render_session_summary_markdown
from .summary import build_session_summary


def run_post_close_analysis(
    args: argparse.Namespace,
    *,
    emit_output: bool = True,
) -> dict[str, Any]:
    session_date = resolve_market_date(args.date)
    summary = build_session_summary(
        db_target=args.db,
        session_date=session_date,
        label=args.label,
        profit_target=args.replay_profit_target,
        stop_multiple=args.replay_stop_multiple,
    )
    report = render_session_summary_markdown(summary)
    if emit_output:
        print(report, end="")
    return {
        "session_date": session_date,
        "label": args.label,
        "summary": summary,
        "report": report,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_post_close_analysis(args, emit_output=True)
    return 0
