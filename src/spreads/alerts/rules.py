from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from spreads.storage.collector_repository import CollectorRepository
from spreads.storage.records import CollectorCycleCandidateRecord

ALERT_SCORE_FLOOR = 72.0
SCORE_BREAKOUT_THRESHOLDS = (85.0, 75.0)
SCORE_BREAKOUT_DELTA = 8.0


@dataclass(frozen=True)
class AlertDecision:
    alert_type: str
    dedupe_key: str
    symbol: str
    description: str
    candidate: dict[str, Any]
    dedupe_state: dict[str, Any]


def parse_utc_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def candidate_identity(candidate: dict[str, Any] | CollectorCycleCandidateRecord) -> tuple[str, str, str, str]:
    return (
        str(candidate["strategy"]),
        str(candidate["expiration_date"]),
        str(candidate["short_symbol"]),
        str(candidate["long_symbol"]),
    )


def idea_fragment(candidate: dict[str, Any] | CollectorCycleCandidateRecord) -> str:
    strategy, expiration_date, short_symbol, long_symbol = candidate_identity(candidate)
    return f"{strategy}|{expiration_date}|{short_symbol}|{long_symbol}"


def score_anchor_key(label: str, session_date: str, candidate: dict[str, Any]) -> str:
    return f"{label}|{session_date}|{candidate['underlying_symbol']}|score_anchor|{idea_fragment(candidate)}"


def event_dedupe_key(label: str, session_date: str, symbol: str, alert_type: str, candidate: dict[str, Any]) -> str:
    if alert_type == "new_board_idea":
        return f"{label}|{session_date}|{symbol}|new_board_idea"
    return f"{label}|{session_date}|{symbol}|{alert_type}|{idea_fragment(candidate)}"


def threshold_dedupe_key(label: str, session_date: str, candidate: dict[str, Any], threshold: float) -> str:
    threshold_label = str(int(threshold))
    return f"{label}|{session_date}|{candidate['underlying_symbol']}|score_breakout|{idea_fragment(candidate)}|threshold_{threshold_label}"


def delta_breakout_key(label: str, session_date: str, candidate: dict[str, Any]) -> str:
    return f"{label}|{session_date}|{candidate['underlying_symbol']}|score_breakout_delta|{idea_fragment(candidate)}"


def is_before_current_cycle(
    row: CollectorCycleCandidateRecord,
    *,
    current_cycle_id: str,
    current_generated_at: str,
) -> bool:
    if row["cycle_id"] == current_cycle_id:
        return False
    row_dt = parse_utc_timestamp(row["generated_at"])
    current_dt = parse_utc_timestamp(current_generated_at)
    if row_dt < current_dt:
        return True
    if row_dt > current_dt:
        return False
    return row["cycle_id"] < current_cycle_id


def build_history_indexes(
    collector_store: CollectorRepository,
    *,
    label: str,
    session_date: str,
    current_cycle_id: str,
    current_generated_at: str,
) -> tuple[set[tuple[str, str, str, str]], set[tuple[str, str, str, str]], set[str]]:
    rows = collector_store.list_session_candidates(label=label, session_date=session_date)
    prior_board_identities: set[tuple[str, str, str, str]] = set()
    prior_watchlist_identities: set[tuple[str, str, str, str]] = set()
    prior_board_symbols: set[str] = set()

    for row in rows:
        if not is_before_current_cycle(row, current_cycle_id=current_cycle_id, current_generated_at=current_generated_at):
            continue
        identity = candidate_identity(row)
        if row["bucket"] == "board":
            prior_board_identities.add(identity)
            prior_board_symbols.add(str(row["underlying_symbol"]))
        elif row["bucket"] == "watchlist":
            prior_watchlist_identities.add(identity)

    return prior_board_identities, prior_watchlist_identities, prior_board_symbols


def resolve_event_alert_type(
    event: dict[str, Any],
    *,
    prior_board_identities: set[tuple[str, str, str, str]],
    prior_watchlist_identities: set[tuple[str, str, str, str]],
    prior_board_symbols: set[str],
) -> str | None:
    event_type = str(event["event_type"])
    current = event.get("current")
    if not isinstance(current, dict):
        return None
    identity = candidate_identity(current)
    symbol = str(current["underlying_symbol"])

    if event_type == "new":
        if identity in prior_watchlist_identities and identity not in prior_board_identities:
            return "watchlist_promoted"
        if symbol not in prior_board_symbols:
            return "new_board_idea"
        return None
    if event_type == "side_flip":
        return "side_flip"
    if event_type == "replaced":
        return "board_replaced"
    return None


def should_send_event_alert(alert_type: str, candidate: dict[str, Any]) -> bool:
    if alert_type in {"watchlist_promoted", "side_flip"}:
        return True
    return float(candidate["quality_score"]) >= ALERT_SCORE_FLOOR


def build_event_alert_decisions(
    *,
    label: str,
    session_date: str,
    current_cycle_id: str,
    current_generated_at: str,
    events: list[dict[str, Any]],
    collector_store: CollectorRepository,
    get_alert_state: callable,
) -> list[AlertDecision]:
    prior_board_identities, prior_watchlist_identities, prior_board_symbols = build_history_indexes(
        collector_store,
        label=label,
        session_date=session_date,
        current_cycle_id=current_cycle_id,
        current_generated_at=current_generated_at,
    )
    decisions: list[AlertDecision] = []
    for event in events:
        current = event.get("current")
        if not isinstance(current, dict):
            continue
        alert_type = resolve_event_alert_type(
            event,
            prior_board_identities=prior_board_identities,
            prior_watchlist_identities=prior_watchlist_identities,
            prior_board_symbols=prior_board_symbols,
        )
        if alert_type is None or not should_send_event_alert(alert_type, current):
            continue
        dedupe_key = event_dedupe_key(
            label,
            session_date,
            str(current["underlying_symbol"]),
            alert_type,
            current,
        )
        if get_alert_state(dedupe_key) is not None:
            continue
        decisions.append(
            AlertDecision(
                alert_type=alert_type,
                dedupe_key=dedupe_key,
                symbol=str(current["underlying_symbol"]),
                description=str(event["message"]),
                candidate=current,
                dedupe_state={
                    "score": float(current["quality_score"]),
                    "source_event_type": str(event["event_type"]),
                },
            )
        )
    return decisions


def build_score_breakout_decisions(
    *,
    label: str,
    session_date: str,
    board_candidates: list[dict[str, Any]],
    get_alert_state: callable,
) -> list[AlertDecision]:
    decisions: list[AlertDecision] = []
    for candidate in board_candidates:
        score = float(candidate["quality_score"])
        if score < ALERT_SCORE_FLOOR:
            continue
        symbol = str(candidate["underlying_symbol"])

        threshold_to_alert: float | None = None
        for threshold in SCORE_BREAKOUT_THRESHOLDS:
            if score < threshold:
                continue
            dedupe_key = threshold_dedupe_key(label, session_date, candidate, threshold)
            if get_alert_state(dedupe_key) is None:
                threshold_to_alert = threshold
                break
        if threshold_to_alert is not None:
            decisions.append(
                AlertDecision(
                    alert_type="score_breakout",
                    dedupe_key=threshold_dedupe_key(label, session_date, candidate, threshold_to_alert),
                    symbol=symbol,
                    description=f"{symbol} score breakout to {score:.1f} on {candidate['strategy']} {candidate['short_strike']:.2f}/{candidate['long_strike']:.2f}",
                    candidate=candidate,
                    dedupe_state={
                        "score": score,
                        "threshold": threshold_to_alert,
                        "mode": "threshold_cross",
                    },
                )
            )
            continue

        anchor_state = get_alert_state(score_anchor_key(label, session_date, candidate))
        if anchor_state is None:
            continue
        last_score = anchor_state["state"].get("last_score")
        if last_score is None:
            continue
        if score - float(last_score) < SCORE_BREAKOUT_DELTA:
            continue
        decisions.append(
            AlertDecision(
                alert_type="score_breakout",
                dedupe_key=delta_breakout_key(label, session_date, candidate),
                symbol=symbol,
                description=f"{symbol} score improved from {float(last_score):.1f} to {score:.1f} on {candidate['strategy']} {candidate['short_strike']:.2f}/{candidate['long_strike']:.2f}",
                candidate=candidate,
                dedupe_state={
                    "score": score,
                    "previous_score": float(last_score),
                    "mode": "delta_jump",
                },
            )
        )
    return decisions
