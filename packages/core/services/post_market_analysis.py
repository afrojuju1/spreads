from __future__ import annotations

import argparse
from datetime import UTC, datetime
from typing import Any, Mapping

from core.services.market_dates import resolve_market_date
from core.services.analysis.rendering import render_session_summary_markdown
from core.services.analysis.summary import build_session_summary
from core.runtime.config import default_database_url
from core.services.selection_terms import (
    MONITOR_SELECTION_STATE,
    PROMOTABLE_SELECTION_STATE,
    promotable_monitor_pnl_spread,
)
from core.storage.factory import build_post_market_repository


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run persisted post-market analysis for a collector label.")
    parser.add_argument("--db", default=default_database_url(), help="Postgres database URL.")
    parser.add_argument("--date", default="today", help="Trading date in YYYY-MM-DD or 'today'.")
    parser.add_argument("--label", required=True, help="Collector session label.")
    parser.add_argument(
        "--replay-profit-target",
        type=float,
        default=0.5,
        help="Replay profit target as a fraction of collected credit.",
    )
    parser.add_argument(
        "--replay-stop-multiple",
        type=float,
        default=2.0,
        help="Replay stop threshold as a multiple of collected credit.",
    )
    return parser.parse_args(argv)


def _selection_state_pnl(outcomes: Mapping[str, Any], selection_state: str) -> float | None:
    value = outcomes["average_estimated_pnl_by_selection_state"].get(selection_state)
    return None if value is None else float(value)


def _selection_state_outcomes(
    outcomes: Mapping[str, Any], selection_state: str
) -> dict[str, int]:
    return dict(outcomes["outcome_counts_by_selection_state"].get(selection_state) or {})


def _outcome_totals(items: Mapping[str, int]) -> tuple[int, int, int]:
    wins = int(items.get("win", 0))
    losses = int(items.get("loss", 0))
    open_count = int(items.get("still_open", 0))
    return wins, losses, open_count


def _signal_line(row: Mapping[str, Any]) -> str:
    win_rate = row.get("win_rate")
    avg_pnl = row.get("average_estimated_pnl")
    win_rate_text = "n/a" if win_rate is None else f"{float(win_rate) * 100:.0f}%"
    avg_pnl_text = "n/a" if avg_pnl is None else f"{float(avg_pnl):.0f}"
    return (
        f"{row['dimension']}={row['bucket']} | count {row['count']} | "
        f"resolved {row['resolved_count']} | win rate {win_rate_text} | avg pnl {avg_pnl_text}"
    )


def build_post_market_diagnostics(summary: Mapping[str, Any]) -> dict[str, Any]:
    outcomes = summary["outcomes"]
    tuning = summary["tuning"]
    event_overview = summary["event_overview"]
    quote_overview = summary["quote_overview"]

    promotable_count = int(
        outcomes["counts_by_selection_state"].get(PROMOTABLE_SELECTION_STATE, 0)
    )
    monitor_count = int(
        outcomes["counts_by_selection_state"].get(MONITOR_SELECTION_STATE, 0)
    )
    promotable_avg_pnl = _selection_state_pnl(outcomes, PROMOTABLE_SELECTION_STATE)
    monitor_avg_pnl = _selection_state_pnl(outcomes, MONITOR_SELECTION_STATE)
    promotable_wins, promotable_losses, promotable_open = _outcome_totals(
        _selection_state_outcomes(outcomes, PROMOTABLE_SELECTION_STATE)
    )
    monitor_wins, monitor_losses, monitor_open = _outcome_totals(
        _selection_state_outcomes(outcomes, MONITOR_SELECTION_STATE)
    )

    side_flip_count = int(event_overview["by_type"].get("side_flip", 0))
    replacement_count = int(event_overview["by_type"].get("replaced", 0))
    event_count = int(event_overview["event_count"])
    cycle_count = int(summary["cycle_count"])
    tracked_legs = int(quote_overview["tracked_leg_count"])
    quote_events = int(quote_overview["quote_event_count"])
    avg_quote_events_per_leg = 0.0 if tracked_legs == 0 else quote_events / tracked_legs
    churn_ratio = 0.0 if cycle_count == 0 else (side_flip_count + replacement_count) / cycle_count

    strengths: list[dict[str, Any]] = []
    problems: list[dict[str, Any]] = []
    recommendations: list[dict[str, Any]] = []

    strongest = list(tuning.get("strongest_signals") or [])
    weakest = list(tuning.get("weakest_signals") or [])
    provisional_strongest = list(tuning.get("provisional_strongest_signals") or [])

    if strongest:
        strengths.append(
            {
                "title": "Best resolved signal segment",
                "details": _signal_line(strongest[0]),
                "evidence": strongest[0],
            }
        )
    elif provisional_strongest:
        strengths.append(
            {
                "title": "Best provisional signal segment",
                "details": _signal_line(provisional_strongest[0]),
                "evidence": provisional_strongest[0],
            }
        )

    if promotable_avg_pnl is not None and promotable_avg_pnl > 0:
        strengths.append(
            {
                "title": "Promotable ideas finished positive on average",
                "details": f"Promotable ideas averaged {promotable_avg_pnl:.0f} estimated PnL.",
                "evidence": {
                    "promotable_average_estimated_pnl": promotable_avg_pnl,
                    "promotable_count": promotable_count,
                    "promotable_wins": promotable_wins,
                    "promotable_losses": promotable_losses,
                    "promotable_open": promotable_open,
                },
            }
        )

    if avg_quote_events_per_leg >= 5:
        strengths.append(
            {
                "title": "Quote capture was healthy",
                "details": f"Captured {quote_events} quote events across {tracked_legs} tracked legs.",
                "evidence": {
                    "quote_event_count": quote_events,
                    "tracked_leg_count": tracked_legs,
                    "average_quote_events_per_leg": round(avg_quote_events_per_leg, 2),
                },
            }
        )

    if side_flip_count <= 1 and replacement_count <= max(2, cycle_count // 4):
        strengths.append(
            {
                "title": "Promotable set stayed stable",
                "details": f"Only {side_flip_count} side flips and {replacement_count} replacements across {cycle_count} cycles.",
                "evidence": {
                    "side_flip_count": side_flip_count,
                    "replacement_count": replacement_count,
                    "cycle_count": cycle_count,
                },
            }
        )

    if weakest:
        problems.append(
            {
                "title": "Weakest resolved signal segment",
                "details": _signal_line(weakest[0]),
                "evidence": weakest[0],
            }
        )

    if promotable_count == 0:
        problems.append(
            {
                "title": "No promotable ideas",
                "details": f"The label produced {monitor_count} monitor ideas but no promotable ideas.",
                "evidence": {
                    "promotable_count": promotable_count,
                    "monitor_count": monitor_count,
                },
            }
        )
    elif promotable_avg_pnl is not None and promotable_avg_pnl < 0:
        problems.append(
            {
                "title": "Promotable ideas underperformed",
                "details": f"Promotable ideas averaged {promotable_avg_pnl:.0f} estimated PnL.",
                "evidence": {
                    "promotable_average_estimated_pnl": promotable_avg_pnl,
                    "promotable_wins": promotable_wins,
                    "promotable_losses": promotable_losses,
                    "promotable_open": promotable_open,
                },
            }
        )

    if (
        promotable_avg_pnl is not None
        and monitor_avg_pnl is not None
        and monitor_avg_pnl > promotable_avg_pnl + 5
    ):
        problems.append(
            {
                "title": "Monitor ideas outperformed the promotable set",
                "details": f"Monitor ideas averaged {monitor_avg_pnl:.0f} vs promotable {promotable_avg_pnl:.0f}.",
                "evidence": {
                    "promotable_average_estimated_pnl": promotable_avg_pnl,
                    "monitor_average_estimated_pnl": monitor_avg_pnl,
                    "monitor_wins": monitor_wins,
                    "monitor_losses": monitor_losses,
                    "monitor_open": monitor_open,
                },
            }
        )

    if side_flip_count >= 3 or churn_ratio >= 0.3:
        problems.append(
            {
                "title": "Collector churn was elevated",
                "details": f"Observed {side_flip_count} side flips and {replacement_count} replacements across {cycle_count} cycles.",
                "evidence": {
                    "side_flip_count": side_flip_count,
                    "replacement_count": replacement_count,
                    "cycle_count": cycle_count,
                    "churn_ratio": round(churn_ratio, 3),
                },
            }
        )

    if tracked_legs > 0 and avg_quote_events_per_leg < 3:
        problems.append(
            {
                "title": "Quote capture was thin",
                "details": f"Only {avg_quote_events_per_leg:.1f} quote events per tracked leg on average.",
                "evidence": {
                    "quote_event_count": quote_events,
                    "tracked_leg_count": tracked_legs,
                    "average_quote_events_per_leg": round(avg_quote_events_per_leg, 2),
                },
            }
        )

    label = str(summary["label"])
    if (
        promotable_count > 0
        and promotable_avg_pnl is not None
        and promotable_avg_pnl < 0
        and "0dte" in label
    ):
        recommendations.append(
            {
                "code": "raise_0dte_promotable_floor",
                "priority": "high",
                "confidence": "medium",
                "title": "Raise the 0DTE promotable quality floor",
                "action": "Raise the promotable score floor and minimum credit requirements for this 0DTE label.",
                "reason": "Promotable ideas were accepted but finished negative on average.",
                "evidence": {
                    "promotable_average_estimated_pnl": promotable_avg_pnl,
                    "promotable_count": promotable_count,
                    "label": label,
                },
            }
        )

    if side_flip_count >= 3 or churn_ratio >= 0.3:
        recommendations.append(
            {
                "code": "increase_hysteresis",
                "priority": "high",
                "confidence": "medium",
                "title": "Increase promotable-switch hysteresis",
                "action": "Tighten side-switch and same-side replacement thresholds for this label.",
                "reason": "Collector churn was elevated enough to risk noisy promotable-set changes.",
                "evidence": {
                    "side_flip_count": side_flip_count,
                    "replacement_count": replacement_count,
                    "cycle_count": cycle_count,
                    "churn_ratio": round(churn_ratio, 3),
                },
            }
        )

    if (
        promotable_count > 0
        and promotable_avg_pnl is not None
        and monitor_avg_pnl is not None
        and monitor_avg_pnl > promotable_avg_pnl + 5
    ):
        recommendations.append(
            {
                "code": "tighten_monitor_promotion",
                "priority": "high",
                "confidence": "medium",
                "title": "Tighten monitor-to-promotable promotion",
                "action": "Require a larger score gap or stronger directional alignment before promoting monitor ideas into the promotable set.",
                "reason": "Monitor ideas outperformed promotable ideas in the same session.",
                "evidence": {
                    "promotable_average_estimated_pnl": promotable_avg_pnl,
                    "monitor_average_estimated_pnl": monitor_avg_pnl,
                    "promotable_count": promotable_count,
                    "monitor_count": monitor_count,
                },
            }
        )

    if (
        promotable_count <= 1
        and monitor_count >= 5
        and monitor_avg_pnl is not None
        and monitor_avg_pnl > 0
    ):
        recommendations.append(
            {
                "code": "widen_discovery",
                "priority": "medium",
                "confidence": "low",
                "title": "Widen discovery slightly",
                "action": "Relax discovery thresholds modestly while keeping promotable thresholds strict.",
                "reason": "The session found a positive monitor set but surfaced too few promotable ideas.",
                "evidence": {
                    "promotable_count": promotable_count,
                    "monitor_count": monitor_count,
                    "monitor_average_estimated_pnl": monitor_avg_pnl,
                },
            }
        )

    if tracked_legs > 0 and avg_quote_events_per_leg < 3:
        recommendations.append(
            {
                "code": "increase_quote_capture_window",
                "priority": "medium",
                "confidence": "medium",
                "title": "Increase quote capture window",
                "action": "Capture quotes for longer on each cycle so post-market evaluation has better intraday mark coverage.",
                "reason": "Per-leg quote coverage was thinner than expected.",
                "evidence": {
                    "quote_event_count": quote_events,
                    "tracked_leg_count": tracked_legs,
                    "average_quote_events_per_leg": round(avg_quote_events_per_leg, 2),
                },
            }
        )

    recommendation_priority = {"high": 0, "medium": 1, "low": 2}
    recommendations.sort(key=lambda row: (recommendation_priority.get(str(row["priority"]), 9), str(row["code"])))

    verdict = "mixed"
    if promotable_count == 0 and monitor_count == 0:
        verdict = "quiet"
    elif (
        promotable_count > 0
        and promotable_avg_pnl is not None
        and promotable_avg_pnl > 0
        and side_flip_count <= 1
    ):
        verdict = "strong"
    elif (
        promotable_avg_pnl is not None
        and promotable_avg_pnl < 0
        and (promotable_losses > promotable_wins or promotable_wins == 0)
    ):
        verdict = "weak"

    return {
        "overall_verdict": verdict,
        "strengths": strengths[:3],
        "problems": problems[:3],
        "recommendations": recommendations[:3],
        "noise": {
            "event_count": event_count,
            "side_flip_count": side_flip_count,
            "replacement_count": replacement_count,
            "cycle_count": cycle_count,
            "churn_ratio": round(churn_ratio, 3),
        },
        "execution_quality": {
            "quote_event_count": quote_events,
            "tracked_leg_count": tracked_legs,
            "average_quote_events_per_leg": round(avg_quote_events_per_leg, 2),
            "candidate_run_count": int(summary["run_overview"]["candidate_run_count"]),
        },
        "promotable_monitor_pnl_spread": promotable_monitor_pnl_spread(
            outcomes.get("average_estimated_pnl_by_selection_state")
        ),
        "selection_state_performance": {
            PROMOTABLE_SELECTION_STATE: {
                "count": promotable_count,
                "average_estimated_pnl": promotable_avg_pnl,
                "wins": promotable_wins,
                "losses": promotable_losses,
                "still_open": promotable_open,
            },
            MONITOR_SELECTION_STATE: {
                "count": monitor_count,
                "average_estimated_pnl": monitor_avg_pnl,
                "wins": monitor_wins,
                "losses": monitor_losses,
                "still_open": monitor_open,
            },
        },
    }


def render_post_market_markdown(analysis: Mapping[str, Any]) -> str:
    diagnostics = analysis["diagnostics"]
    lines = [
        f"# Post-Market Analysis: {analysis['session_date']}",
        "",
        f"- Label: `{analysis['label']}`",
        f"- Verdict: {diagnostics['overall_verdict']}",
        f"- Generated at: {analysis['generated_at']}",
        "",
        "## Strengths",
        "",
    ]
    strengths = diagnostics["strengths"] or [{"title": "None", "details": "No strengths were identified.", "evidence": {}}]
    for row in strengths:
        lines.append(f"- **{row['title']}**: {row['details']}")
    lines.extend(["", "## Problems", ""])
    problems = diagnostics["problems"] or [{"title": "None", "details": "No material problems were identified.", "evidence": {}}]
    for row in problems:
        lines.append(f"- **{row['title']}**: {row['details']}")
    lines.extend(["", "## Recommendations", ""])
    recommendations = analysis["recommendations"] or [
        {
            "title": "No change",
            "action": "No immediate tuning change is recommended from this session alone.",
            "reason": "Diagnostics did not produce a strong deterministic adjustment.",
            "priority": "low",
        }
    ]
    for row in recommendations:
        lines.append(
            f"- **{row['title']}** ({row['priority']}, {row['confidence'] if 'confidence' in row else 'n/a'}): "
            f"{row['action']} Reason: {row['reason']}"
        )
    lines.extend(
        [
            "",
            "## Supporting Summary",
            "",
            render_session_summary_markdown(analysis["summary"]).strip(),
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def build_post_market_analysis(
    *,
    db_target: str,
    session_date: str,
    label: str,
    profit_target: float,
    stop_multiple: float,
) -> dict[str, Any]:
    generated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    summary = build_session_summary(
        db_target=db_target,
        session_date=session_date,
        label=label,
        profit_target=profit_target,
        stop_multiple=stop_multiple,
    )
    diagnostics = build_post_market_diagnostics(summary)
    analysis = {
        "session_date": session_date,
        "label": label,
        "generated_at": generated_at,
        "summary": summary,
        "diagnostics": diagnostics,
        "recommendations": diagnostics["recommendations"],
    }
    analysis["report"] = render_post_market_markdown(analysis)
    return analysis


def run_post_market_analysis(
    args: argparse.Namespace,
    *,
    emit_output: bool = True,
    analysis_run_id: str | None = None,
    job_run_id: str | None = None,
) -> dict[str, Any]:
    session_date = resolve_market_date(args.date)
    run_id = analysis_run_id or f"post-market:{args.label}:{session_date}"
    repository = build_post_market_repository(args.db)
    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    repository.begin_run(
        analysis_run_id=run_id,
        job_run_id=job_run_id,
        session_date=session_date,
        label=args.label,
        created_at=created_at,
    )
    try:
        analysis = build_post_market_analysis(
            db_target=args.db,
            session_date=session_date,
            label=args.label,
            profit_target=args.replay_profit_target,
            stop_multiple=args.replay_stop_multiple,
        )
        completed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        repository.complete_run(
            analysis_run_id=run_id,
            completed_at=completed_at,
            summary=analysis["summary"],
            diagnostics=analysis["diagnostics"],
            recommendations=analysis["recommendations"],
            report_markdown=analysis["report"],
        )
        if emit_output:
            print(analysis["report"], end="")
        return {
            **analysis,
            "analysis_run_id": run_id,
            "job_run_id": job_run_id,
            "created_at": created_at,
            "completed_at": completed_at,
            "status": "succeeded",
        }
    except Exception as exc:
        repository.fail_run(
            analysis_run_id=run_id,
            completed_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            error_text=str(exc),
        )
        raise
    finally:
        repository.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_post_market_analysis(args, emit_output=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
