from __future__ import annotations

from typing import Any, Mapping

from core.services.selection_terms import (
    PROMOTABLE_SELECTION_STATE,
)


def render_event_summary(event_overview: Mapping[str, Any]) -> list[str]:
    if not event_overview["event_count"]:
        return ["No collector events were persisted for this session."]
    lines = [
        f"- Event count: {event_overview['event_count']}",
        "- By type: "
        + ", ".join(
            f"{event_type}={count}"
            for event_type, count in sorted(event_overview["by_type"].items())
        ),
        "- Most active symbols: "
        + ", ".join(
            f"{symbol}={count}"
            for symbol, count in list(event_overview["most_active_symbols"].items())[
                :5
            ]
        ),
    ]
    recent_side_flips = [
        event
        for event in event_overview["recent_events"]
        if event.get("event_type") == "side_flip"
    ]
    if recent_side_flips:
        lines.append("- Recent side flips:")
        for event in recent_side_flips[-5:]:
            lines.append(f"  - {event['generated_at']} {event['message']}")
    return lines


def render_symbol_summaries(symbol_breakdown: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for summary in symbol_breakdown:
        lines.append(f"### {summary['symbol']} {summary['strategy']}")
        lines.append(
            f"- Runs: {summary['run_count']} | idea runs: {summary['idea_count']} | first: {summary['first_seen']} | last: {summary['last_seen']}"
        )
        best_idea = summary["best_idea"]
        if best_idea is None:
            lines.append("- Best idea: none surfaced in this session")
        else:
            lines.append(
                "- Best idea: "
                f"{best_idea['short_strike']:.2f}/{best_idea['long_strike']:.2f} "
                f"score {best_idea['quality_score']:.1f} mid {best_idea['midpoint_credit']:.2f} "
                f"at {best_idea['generated_at']}"
            )
            lines.append(
                f"- Calendar status: {best_idea['calendar_status']} | average best score: {summary['avg_best_score']:.1f}"
            )
        latest_setup = summary["latest_setup"] or {}
        if latest_setup:
            lines.append(
                "- Latest setup: "
                f"{latest_setup.get('status', 'unknown')} score {latest_setup.get('score', 0):.1f} "
                f"(daily {latest_setup.get('daily_score', 'n/a')}, intraday {latest_setup.get('intraday_score', 'n/a')})"
            )
            reasons = latest_setup.get("reasons") or []
            if reasons:
                lines.append("- Latest setup reasons: " + "; ".join(reasons[:3]))
        quote_coverage = summary.get("quote_coverage")
        if quote_coverage:
            lines.append(
                "- Quote coverage: "
                f"{quote_coverage['quote_events']} quote events across {quote_coverage['unique_legs']} legs "
                f"from {quote_coverage['first_quote_at']} to {quote_coverage['last_quote_at']}"
            )
        lines.append("")
    return lines


def render_leg_summaries(leg_summaries: list[dict[str, Any]]) -> list[str]:
    if not leg_summaries:
        return ["No websocket quote rows were captured for this session."]
    lines = [
        "| Option | Sym | Side | Role | Events | Mid Min | Mid Max | Last Quote |",
        "|---|---|---|---|---:|---:|---:|---|",
    ]
    for item in leg_summaries:
        lines.append(
            f"| {item['option_symbol']} | {item['underlying_symbol']} | {item['strategy']} | {item['leg_role']} | "
            f"{item['event_count']} | {item['midpoint_min']:.2f} | {item['midpoint_max']:.2f} | {item['last_quote_at']} |"
        )
    return lines


def render_outcome_summaries(outcomes: Mapping[str, Any]) -> list[str]:
    ideas = list(outcomes["ideas"])
    if not ideas:
        return ["No persisted promotable or monitor ideas were available for this session."]

    lines = [
        f"- Idea count: {outcomes['idea_count']}",
        "- Selection-state counts: "
        + ", ".join(
            f"{state}={count}"
            for state, count in sorted(outcomes["counts_by_selection_state"].items())
        ),
        "- Average estimated PnL: "
        + ", ".join(
            f"{state}={value:.0f}" if value is not None else f"{state}=n/a"
            for state, value in sorted(
                outcomes["average_estimated_pnl_by_selection_state"].items()
            )
        ),
        "",
    ]
    for idea in ideas[:20]:
        lines.append(
            f"### {idea['underlying_symbol']} {idea['strategy']} {idea['short_symbol']} / {idea['long_symbol']}"
        )
        timing_line = f"- First seen: {idea['first_seen']} | entry seen: {idea['entry_seen']}"
        if (
            idea["selection_state"] == PROMOTABLE_SELECTION_STATE
            and idea["first_monitor_seen"] is not None
        ):
            timing_line += f" | first monitor: {idea['first_monitor_seen']}"
        lines.append(timing_line)
        lines.append(
            f"- Selection state: {idea['selection_state']} | latest score: {idea['latest_score']:.1f} ({idea['score_bucket']})"
        )
        lines.append(
            f"- Outcome: {idea['replay_verdict']} | bucket: {idea['outcome_bucket']} | still in play: {'yes' if idea['still_in_play'] else 'no'}"
        )
        close_pnl = (
            "n/a"
            if idea["estimated_close_pnl"] is None
            else f"{idea['estimated_close_pnl']:.0f}"
        )
        expiry_pnl = (
            "n/a"
            if idea["estimated_expiry_pnl"] is None
            else f"{idea['estimated_expiry_pnl']:.0f}"
        )
        lines.append(
            f"- Estimated PnL: close {close_pnl} | expiry {expiry_pnl} | PT {'yes' if idea['profit_target_hit'] else 'no'} | stop {'yes' if idea['stop_hit'] else 'no'}"
        )
        lines.append("")
    return lines


def render_signal_tuning(tuning: Mapping[str, Any]) -> list[str]:
    if tuning["sample_size"] == 0:
        return ["No ideas were available for signal tuning."]

    lines = [f"- Sample size: {tuning['sample_size']} ideas", ""]

    strongest = tuning["strongest_signals"]
    weakest = tuning["weakest_signals"]
    provisional_strongest = tuning["provisional_strongest_signals"]
    provisional_weakest = tuning["provisional_weakest_signals"]
    if strongest:
        lines.append("- Strongest resolved segments:")
        for row in strongest:
            win_rate = (
                "n/a" if row["win_rate"] is None else f"{row['win_rate'] * 100:.0f}%"
            )
            avg_pnl = (
                "n/a"
                if row["average_estimated_pnl"] is None
                else f"{row['average_estimated_pnl']:.0f}"
            )
            lines.append(
                f"  - {row['dimension']}={row['bucket']} | count {row['count']} | resolved {row['resolved_count']} | win rate {win_rate} | avg pnl {avg_pnl}"
            )
        lines.append("")
    if weakest:
        lines.append("- Weakest resolved segments:")
        for row in weakest:
            win_rate = (
                "n/a" if row["win_rate"] is None else f"{row['win_rate'] * 100:.0f}%"
            )
            avg_pnl = (
                "n/a"
                if row["average_estimated_pnl"] is None
                else f"{row['average_estimated_pnl']:.0f}"
            )
            lines.append(
                f"  - {row['dimension']}={row['bucket']} | count {row['count']} | resolved {row['resolved_count']} | win rate {win_rate} | avg pnl {avg_pnl}"
            )
        lines.append("")
    if provisional_strongest:
        lines.append("- Strongest provisional segments (open-trade PnL only):")
        for row in provisional_strongest:
            avg_pnl = (
                "n/a"
                if row["average_estimated_pnl"] is None
                else f"{row['average_estimated_pnl']:.0f}"
            )
            lines.append(
                f"  - {row['dimension']}={row['bucket']} | count {row['count']} | resolved {row['resolved_count']} | avg pnl {avg_pnl}"
            )
        lines.append("")
    if provisional_weakest:
        lines.append("- Weakest provisional segments (open-trade PnL only):")
        for row in provisional_weakest:
            avg_pnl = (
                "n/a"
                if row["average_estimated_pnl"] is None
                else f"{row['average_estimated_pnl']:.0f}"
            )
            lines.append(
                f"  - {row['dimension']}={row['bucket']} | count {row['count']} | resolved {row['resolved_count']} | avg pnl {avg_pnl}"
            )
        lines.append("")

    dimensions_to_render = (
        ("score_bucket", "By Score Bucket"),
        ("setup_status", "By Setup Status"),
        ("calendar_status", "By Calendar Status"),
        ("session_phase", "By Session Phase"),
        ("vwap_regime", "By VWAP Regime"),
        ("trend_regime", "By Intraday Trend"),
    )
    for key, title in dimensions_to_render:
        rows = tuning["dimensions"].get(key) or []
        lines.append(f"### {title}")
        if not rows:
            lines.append("No data.")
            lines.append("")
            continue
        lines.append("| Bucket | Count | Win | Loss | Open | Win Rate | Avg PnL | Avg Score |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for row in rows:
            win_rate = (
                "n/a" if row["win_rate"] is None else f"{row['win_rate'] * 100:.0f}%"
            )
            avg_pnl = (
                "n/a"
                if row["average_estimated_pnl"] is None
                else f"{row['average_estimated_pnl']:.0f}"
            )
            lines.append(
                f"| {row['bucket']} | {row['count']} | {row['win_count']} | {row['loss_count']} | {row['still_open_count']} | "
                f"{win_rate} | {avg_pnl} | {row['average_latest_score']:.1f} |"
            )
        lines.append("")
    return lines


def render_session_summary_markdown(summary: Mapping[str, Any]) -> str:
    run_overview = summary["run_overview"]
    quote_overview = summary["quote_overview"]
    latest_cycle = summary["latest_cycle"]
    lines = [
        f"# Post-Close Analysis: {summary['session_date']}",
        "",
        f"- Label: `{summary['label']}`",
        f"- Collector cycles: {summary['cycle_count']}",
        f"- Scan runs: {run_overview['run_count']} total, {run_overview['candidate_run_count']} with surfaced ideas",
        f"- Scan window: {run_overview['first_run_at']} -> {run_overview['last_run_at']}",
        f"- Quote events: {quote_overview['quote_event_count']} across {quote_overview['tracked_leg_count']} legs",
        f"- Quote window: {quote_overview['first_quote_at']} -> {quote_overview['last_quote_at']}",
    ]
    if latest_cycle is not None:
        lines.extend(
            [
                f"- Latest cycle: {latest_cycle['generated_at']}",
                f"- Latest promotable/monitor sizes: "
                f"{int(dict(latest_cycle.get('selection_counts') or {}).get('promotable') or 0)}/"
                f"{int(dict(latest_cycle.get('selection_counts') or {}).get('monitor') or 0)}",
            ]
        )
    lines.extend(
        [
            "",
            "## Collector Events",
            "",
            *render_event_summary(summary["event_overview"]),
            "",
            "## Symbol Breakdown",
            "",
            *render_symbol_summaries(summary["symbol_breakdown"]),
            "## Promotable Vs Monitor Outcomes",
            "",
            *render_outcome_summaries(summary["outcomes"]),
            "## Signal Tuning",
            "",
            *render_signal_tuning(summary["tuning"]),
            "## Most Tracked Legs",
            "",
            *render_leg_summaries(summary["leg_summaries"]),
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"
