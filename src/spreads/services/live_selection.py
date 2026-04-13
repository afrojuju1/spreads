from __future__ import annotations

from typing import Any

from spreads.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    candidate_requires_favorable_setup,
)

DEFAULT_SELECTION_THRESHOLDS = {
    "promotable_score_floor": 65.0,
    "promotable_strong_score": 82.0,
    "promotable_winner_gap": 6.0,
    "promotable_side_switch_margin": 10.0,
    "promotable_replacement_margin": 5.0,
    "promotable_confirmation_cycles": 2,
    "promotable_hold_tolerance": 3.0,
    "monitor_score_floor": 55.0,
    "monitor_per_symbol": 2,
    "min_promotable_midpoint_credit": None,
    "min_monitor_midpoint_credit": None,
}

PROFILE_SELECTION_THRESHOLDS = {
    "0dte": {
        "promotable_score_floor": 72.0,
        "promotable_strong_score": 86.0,
        "monitor_score_floor": 60.0,
        "min_promotable_midpoint_credit": 0.50,
        "min_monitor_midpoint_credit": 0.35,
    },
    "core": {
        "promotable_winner_gap": 8.0,
        "promotable_replacement_margin": 7.0,
        "promotable_side_switch_margin": 14.0,
        "promotable_confirmation_cycles": 3,
    },
}


def _selection_thresholds(profile: str | None) -> dict[str, Any]:
    normalized = str(profile or "").strip().lower()
    thresholds = dict(DEFAULT_SELECTION_THRESHOLDS)
    thresholds.update(PROFILE_SELECTION_THRESHOLDS.get(normalized, {}))
    return thresholds


def candidate_identity(candidate: dict[str, Any]) -> str:
    return (
        f"{candidate['strategy']}|{candidate['short_symbol']}|{candidate['long_symbol']}"
    )


def summarize_candidate(candidate: dict[str, Any]) -> str:
    return (
        f"{candidate['strategy']} {candidate['short_strike']:.2f}/{candidate['long_strike']:.2f} "
        f"score {candidate['quality_score']:.1f}"
    )


def promotable_candidate_is_eligible(candidate: dict[str, Any]) -> bool:
    if not candidate_requires_favorable_setup(candidate):
        return True
    return str(candidate.get("setup_status") or "").lower() == "favorable" and (
        candidate_has_intraday_setup_context(candidate)
    )


def _meets_midpoint_credit_floor(
    candidate: dict[str, Any],
    minimum_credit: float | None,
) -> bool:
    if minimum_credit is None:
        return True
    return float(candidate.get("midpoint_credit") or 0.0) >= float(minimum_credit)


def read_previous_selection(
    collector_store: Any,
    label: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    latest_cycle = collector_store.get_latest_cycle(label)
    if latest_cycle is None:
        return {}, {}

    promotable_rows = collector_store.list_cycle_candidates(
        latest_cycle["cycle_id"],
        selection_state="promotable",
        eligibility="live",
    )
    previous: dict[str, dict[str, Any]] = {}
    for candidate in promotable_rows:
        payload = dict(candidate.get("candidate") or {})
        symbol = payload.get("underlying_symbol")
        if symbol:
            previous[str(symbol)] = payload

    raw_memory = latest_cycle.get("selection_memory") or {}
    selection_memory = {
        str(symbol): state
        for symbol, state in raw_memory.items()
        if isinstance(symbol, str) and isinstance(state, dict)
    }
    return previous, selection_memory


def _evaluate_pending_candidate(
    *,
    winner: dict[str, Any],
    previous_memory: dict[str, Any],
    confirmation_cycles: int,
) -> tuple[bool, dict[str, Any]]:
    winner_id = candidate_identity(winner)
    pending_id = previous_memory.get("pending_identity")
    pending_count = int(previous_memory.get("pending_count", 0))
    if pending_id == winner_id:
        pending_count += 1
    else:
        pending_count = 1
    next_memory = {
        "pending_identity": winner_id,
        "pending_strategy": winner["strategy"],
        "pending_count": pending_count,
    }
    return pending_count >= max(int(confirmation_cycles), 1), next_memory


def _sort_candidates(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        payloads,
        key=lambda candidate: (
            float(candidate.get("quality_score") or 0.0),
            float(candidate.get("return_on_risk") or 0.0),
            float(candidate.get("midpoint_credit") or 0.0),
            min(
                float(candidate.get("short_open_interest") or 0.0),
                float(candidate.get("long_open_interest") or 0.0),
            ),
        ),
        reverse=True,
    )


def _select_promotable_candidates(
    *,
    symbol_candidates: dict[str, list[dict[str, Any]]],
    previous_promotable: dict[str, dict[str, Any]],
    previous_memory: dict[str, dict[str, Any]],
    top: int,
    thresholds: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    next_memory: dict[str, dict[str, Any]] = {}

    for symbol in sorted(symbol_candidates):
        options = _sort_candidates(list(symbol_candidates.get(symbol) or []))
        viable = [
            candidate
            for candidate in options
            if float(candidate.get("quality_score") or 0.0)
            >= float(thresholds["promotable_score_floor"])
            and promotable_candidate_is_eligible(candidate)
            and _meets_midpoint_credit_floor(
                candidate,
                thresholds.get("min_promotable_midpoint_credit"),
            )
        ]
        winner = viable[0] if viable else None
        runner_up = viable[1] if len(viable) > 1 else None
        winner_gap = None
        if winner is not None and runner_up is not None:
            winner_gap = float(winner["quality_score"]) - float(
                runner_up["quality_score"]
            )

        previous = previous_promotable.get(symbol)
        symbol_memory = dict(previous_memory.get(symbol) or {})
        accepted: dict[str, Any] | None = None
        memory_update: dict[str, Any] = {}

        if previous is None:
            if winner is not None:
                if (
                    float(winner["quality_score"])
                    >= float(thresholds["promotable_strong_score"])
                    or runner_up is None
                    or (
                        winner_gap is not None
                        and winner_gap >= float(thresholds["promotable_winner_gap"])
                    )
                ):
                    accepted = winner
                else:
                    confirmed, memory_update = _evaluate_pending_candidate(
                        winner=winner,
                        previous_memory=symbol_memory,
                        confirmation_cycles=int(
                            thresholds["promotable_confirmation_cycles"]
                        ),
                    )
                    if confirmed:
                        accepted = winner
                        memory_update = {}
            if accepted is not None:
                memory_update.update(
                    {
                        "accepted_identity": candidate_identity(accepted),
                        "accepted_strategy": accepted["strategy"],
                        "accepted_score": float(accepted["quality_score"]),
                    }
                )
                selected.append(accepted)
            next_memory[symbol] = memory_update
            continue

        previous_id = candidate_identity(previous)
        previous_match = next(
            (candidate for candidate in options if candidate_identity(candidate) == previous_id),
            None,
        )
        previous_same_side = next(
            (
                candidate
                for candidate in options
                if candidate["strategy"] == previous["strategy"]
            ),
            None,
        )
        current_anchor = previous_match or previous_same_side
        if current_anchor is not None and not promotable_candidate_is_eligible(
            current_anchor
        ):
            current_anchor = None

        if (
            current_anchor is not None
            and float(current_anchor["quality_score"])
            >= float(thresholds["promotable_score_floor"])
            - float(thresholds["promotable_hold_tolerance"])
        ):
            accepted = current_anchor
        elif winner is not None:
            if winner["strategy"] == previous["strategy"]:
                accepted = winner
            else:
                confirmed, memory_update = _evaluate_pending_candidate(
                    winner=winner,
                    previous_memory=symbol_memory,
                    confirmation_cycles=int(
                        thresholds["promotable_confirmation_cycles"]
                    ),
                )
                if confirmed:
                    accepted = winner
                    memory_update = {}

        if winner is not None and accepted is not None:
            accepted_id = candidate_identity(accepted)
            winner_id = candidate_identity(winner)
            if winner_id != accepted_id:
                same_side = winner["strategy"] == accepted["strategy"]
                score_gap = float(winner["quality_score"]) - float(
                    accepted["quality_score"]
                )
                if same_side:
                    if score_gap >= float(
                        thresholds["promotable_replacement_margin"]
                    ):
                        confirmed, memory_update = _evaluate_pending_candidate(
                            winner=winner,
                            previous_memory=symbol_memory,
                            confirmation_cycles=int(
                                thresholds["promotable_confirmation_cycles"]
                            ),
                        )
                        if confirmed:
                            accepted = winner
                            memory_update = {}
                else:
                    if score_gap >= float(
                        thresholds["promotable_side_switch_margin"]
                    ) and (
                        winner_gap is None
                        or winner_gap >= float(thresholds["promotable_winner_gap"])
                    ):
                        confirmed, memory_update = _evaluate_pending_candidate(
                            winner=winner,
                            previous_memory=symbol_memory,
                            confirmation_cycles=int(
                                thresholds["promotable_confirmation_cycles"]
                            ),
                        )
                        if confirmed:
                            accepted = winner
                            memory_update = {}

        if accepted is not None:
            accepted_score = float(accepted.get("quality_score") or 0.0)
            if accepted_score >= float(thresholds["promotable_score_floor"]):
                memory_update.update(
                    {
                        "accepted_identity": candidate_identity(accepted),
                        "accepted_strategy": accepted["strategy"],
                        "accepted_score": accepted_score,
                    }
                )
                if memory_update.get("pending_identity") == memory_update.get(
                    "accepted_identity"
                ):
                    memory_update.pop("pending_identity", None)
                    memory_update.pop("pending_strategy", None)
                    memory_update.pop("pending_count", None)
                selected.append(accepted)

        next_memory[symbol] = memory_update

    return _sort_candidates(selected)[:top], next_memory


def _select_monitor_candidates(
    *,
    symbol_candidates: dict[str, list[dict[str, Any]]],
    promotable_candidates: list[dict[str, Any]],
    top: int,
    thresholds: dict[str, Any],
) -> list[dict[str, Any]]:
    accepted_ids = {
        candidate_identity(candidate) for candidate in promotable_candidates
    }
    monitor: list[dict[str, Any]] = []

    for symbol in sorted(symbol_candidates):
        kept = 0
        for candidate in _sort_candidates(list(symbol_candidates.get(symbol) or [])):
            if candidate_identity(candidate) in accepted_ids:
                continue
            if float(candidate.get("quality_score") or 0.0) < float(
                thresholds["monitor_score_floor"]
            ):
                continue
            if not _meets_midpoint_credit_floor(
                candidate,
                thresholds.get("min_monitor_midpoint_credit"),
            ):
                continue
            monitor.append(candidate)
            kept += 1
            if kept >= int(thresholds["monitor_per_symbol"]):
                break

    return _sort_candidates(monitor)[:top]


def _selection_row(
    candidate: dict[str, Any],
    *,
    selection_state: str,
    selection_rank: int,
    state_reason: str,
    origin: str,
    eligibility: str,
) -> dict[str, Any]:
    return {
        **dict(candidate),
        "selection_state": selection_state,
        "selection_rank": selection_rank,
        "state_reason": state_reason,
        "origin": origin,
        "eligibility": eligibility,
        "candidate": dict(candidate),
    }


def build_selection_events(
    *,
    label: str,
    cycle_id: str,
    generated_at: str,
    previous_promotable: dict[str, dict[str, Any]],
    current_promotable: dict[str, dict[str, Any]],
    score_delta_threshold: float = 5.0,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    all_symbols = sorted(set(previous_promotable) | set(current_promotable))
    for symbol in all_symbols:
        previous = previous_promotable.get(symbol)
        current = current_promotable.get(symbol)
        if previous is None and current is not None:
            events.append(
                {
                    "generated_at": generated_at,
                    "cycle_id": cycle_id,
                    "label": label,
                    "symbol": symbol,
                    "event_type": "new_promotable",
                    "message": f"{symbol} new promotable idea: {summarize_candidate(current)}",
                    "previous": None,
                    "current": current,
                }
            )
            continue
        if previous is not None and current is None:
            events.append(
                {
                    "generated_at": generated_at,
                    "cycle_id": cycle_id,
                    "label": label,
                    "symbol": symbol,
                    "event_type": "dropped_promotable",
                    "message": f"{symbol} dropped from promotable set: {summarize_candidate(previous)}",
                    "previous": previous,
                    "current": None,
                }
            )
            continue
        if previous is None or current is None:
            continue

        previous_identity = (
            previous["strategy"],
            previous["short_symbol"],
            previous["long_symbol"],
        )
        current_identity = (
            current["strategy"],
            current["short_symbol"],
            current["long_symbol"],
        )
        if previous["strategy"] != current["strategy"]:
            event_type = "promotable_side_flip"
            message = (
                f"{symbol} promotable side flipped: "
                f"{summarize_candidate(previous)} -> {summarize_candidate(current)}"
            )
        elif previous_identity != current_identity:
            event_type = "promotable_replaced"
            message = (
                f"{symbol} promotable idea replaced: "
                f"{summarize_candidate(previous)} -> {summarize_candidate(current)}"
            )
        else:
            score_change = float(current["quality_score"]) - float(
                previous["quality_score"]
            )
            if abs(score_change) < score_delta_threshold:
                continue
            direction = "up" if score_change > 0 else "down"
            event_type = f"promotable_score_{direction}"
            message = (
                f"{symbol} promotable score {direction}: "
                f"{previous['quality_score']:.1f} -> {current['quality_score']:.1f} "
                f"for {summarize_candidate(current)}"
            )

        events.append(
            {
                "generated_at": generated_at,
                "cycle_id": cycle_id,
                "label": label,
                "symbol": symbol,
                "event_type": event_type,
                "message": message,
                "previous": previous,
                "current": current,
            }
        )
    return events


def select_live_opportunities(
    *,
    label: str,
    cycle_id: str,
    generated_at: str,
    symbol_candidates: dict[str, list[dict[str, Any]]],
    previous_promotable: dict[str, dict[str, Any]],
    previous_selection_memory: dict[str, dict[str, Any]],
    top_promotable: int,
    top_monitor: int,
    profile: str | None = None,
    recovered_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    thresholds = _selection_thresholds(profile)
    promotable_candidates, selection_memory = _select_promotable_candidates(
        symbol_candidates=symbol_candidates,
        previous_promotable=previous_promotable,
        previous_memory=previous_selection_memory,
        top=top_promotable,
        thresholds=thresholds,
    )
    monitor_candidates = _select_monitor_candidates(
        symbol_candidates=symbol_candidates,
        promotable_candidates=promotable_candidates,
        top=top_monitor,
        thresholds=thresholds,
    )

    opportunities: list[dict[str, Any]] = []
    for index, candidate in enumerate(promotable_candidates, start=1):
        opportunities.append(
            _selection_row(
                candidate,
                selection_state="promotable",
                selection_rank=index,
                state_reason="selected_promotable",
                origin="live_scan",
                eligibility="live",
            )
        )

    next_rank = len(opportunities) + 1
    for candidate in monitor_candidates:
        opportunities.append(
            _selection_row(
                candidate,
                selection_state="monitor",
                selection_rank=next_rank,
                state_reason="selected_monitor",
                origin="live_scan",
                eligibility="live",
            )
        )
        next_rank += 1

    for candidate in list(recovered_candidates or []):
        opportunities.append(
            _selection_row(
                candidate,
                selection_state="monitor",
                selection_rank=next_rank,
                state_reason="history_recovery",
                origin="history_recovery",
                eligibility="analysis_only",
            )
        )
        next_rank += 1

    current_promotable = {
        str(candidate["underlying_symbol"]): candidate for candidate in promotable_candidates
    }
    events = build_selection_events(
        label=label,
        cycle_id=cycle_id,
        generated_at=generated_at,
        previous_promotable=previous_promotable,
        current_promotable=current_promotable,
    )

    return {
        "opportunities": opportunities,
        "selection_memory": selection_memory,
        "promotable_candidates": promotable_candidates,
        "monitor_candidates": monitor_candidates,
        "events": events,
    }
