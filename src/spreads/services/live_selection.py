from __future__ import annotations

from typing import Any

from spreads.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    candidate_meets_return_on_risk_floor,
    candidate_requires_favorable_setup,
    resolve_deployment_quality_thresholds,
)
from spreads.services.opportunity_scoring import build_candidate_opportunity_score

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
    "min_promotable_return_on_risk": None,
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
    thresholds.update(resolve_deployment_quality_thresholds(normalized))
    return thresholds


def candidate_identity(candidate: dict[str, Any]) -> str:
    return f"{candidate['strategy']}|{candidate['short_symbol']}|{candidate['long_symbol']}"


def summarize_candidate(candidate: dict[str, Any]) -> str:
    return (
        f"{candidate['strategy']} {candidate['short_strike']:.2f}/{candidate['long_strike']:.2f} "
        f"score {_selection_score(candidate):.1f}"
    )


def _selection_score(candidate: dict[str, Any]) -> float:
    promotion_score = candidate.get("promotion_score")
    if promotion_score not in (None, ""):
        return float(promotion_score)
    return float(candidate.get("quality_score") or 0.0)


def _candidate_promotion_floor(
    candidate: dict[str, Any],
    thresholds: dict[str, Any],
) -> float:
    return float(
        (candidate.get("score_thresholds") or {}).get("promotion_floor")
        or thresholds["promotable_score_floor"]
    )


def _candidate_monitor_floor(
    candidate: dict[str, Any],
    thresholds: dict[str, Any],
) -> float:
    return float(
        (candidate.get("score_thresholds") or {}).get("monitor_floor")
        or thresholds["monitor_score_floor"]
    )


def _scored_candidate(
    candidate: dict[str, Any],
    *,
    thresholds: dict[str, Any],
    profile: str | None,
    generated_at: str,
) -> dict[str, Any]:
    scorecard = build_candidate_opportunity_score(
        candidate,
        cycle={
            "generated_at": generated_at,
            "profile": profile,
        },
    )
    execution_blockers: list[str] = []
    if str(scorecard["state"]) == "blocked":
        execution_blockers.extend(list(scorecard["blockers"]))
    if not promotable_candidate_is_eligible(candidate):
        execution_blockers.append("selection_not_live_ready")
    if not _meets_midpoint_credit_floor(
        candidate,
        thresholds.get("min_promotable_midpoint_credit"),
    ):
        execution_blockers.append("midpoint_credit_below_promotable_floor")
    if not candidate_meets_return_on_risk_floor(
        candidate,
        thresholds.get("min_promotable_return_on_risk"),
    ):
        execution_blockers.append("return_on_risk_below_promotable_floor")
    execution_penalty = min(float(len(execution_blockers)) * 12.0, 30.0)
    execution_score = round(
        max(float(scorecard["execution_score"]) - execution_penalty, 0.0),
        1,
    )
    monitor_floor = float(scorecard["monitor_floor"])
    confidence = round(
        max(
            0.0,
            min(
                (execution_score - monitor_floor) / max(100.0 - monitor_floor, 1.0), 1.0
            ),
        ),
        4,
    )
    return {
        **dict(candidate),
        "discovery_score": scorecard["discovery_score"],
        "promotion_score": scorecard["promotion_score"],
        "execution_score": execution_score,
        "confidence": confidence,
        "score_style_profile": scorecard["style_profile"],
        "scoring_state": scorecard["state"],
        "scoring_state_reason": scorecard["state_reason"],
        "scoring_blockers": list(scorecard["blockers"]),
        "execution_blockers": execution_blockers,
        "score_thresholds": {
            "promotion_floor": scorecard["promotion_floor"],
            "monitor_floor": scorecard["monitor_floor"],
        },
        "score_evidence": {
            "setup_score_delta": scorecard["setup_score_delta"],
            "fill_ratio_delta": scorecard["fill_ratio_delta"],
            "calibration_delta": scorecard["calibration_delta"],
            "calibration_breakdown": scorecard["calibration_breakdown"],
            "profile_score_components": scorecard["profile_score_components"],
            "profile_score_evidence": scorecard["profile_score_evidence"],
            "penalty": scorecard["penalty"],
        },
    }


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


def _meets_promotable_thresholds(
    candidate: dict[str, Any],
    thresholds: dict[str, Any],
    *,
    score_floor: float,
) -> bool:
    return (
        str(candidate.get("scoring_state") or "") != "blocked"
        and _selection_score(candidate) >= float(score_floor)
        and promotable_candidate_is_eligible(candidate)
        and _meets_midpoint_credit_floor(
            candidate,
            thresholds.get("min_promotable_midpoint_credit"),
        )
        and candidate_meets_return_on_risk_floor(
            candidate,
            thresholds.get("min_promotable_return_on_risk"),
        )
    )


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
            _selection_score(candidate),
            float(candidate.get("execution_score") or 0.0),
            float(
                candidate.get("discovery_score")
                or candidate.get("quality_score")
                or 0.0
            ),
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
            if _meets_promotable_thresholds(
                candidate,
                thresholds,
                score_floor=_candidate_promotion_floor(candidate, thresholds),
            )
        ]
        winner = viable[0] if viable else None
        runner_up = viable[1] if len(viable) > 1 else None
        winner_gap = None
        if winner is not None and runner_up is not None:
            winner_gap = _selection_score(winner) - _selection_score(runner_up)

        previous = previous_promotable.get(symbol)
        symbol_memory = dict(previous_memory.get(symbol) or {})
        accepted: dict[str, Any] | None = None
        memory_update: dict[str, Any] = {}

        if previous is None:
            if winner is not None:
                if (
                    _selection_score(winner)
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
                        "accepted_score": _selection_score(accepted),
                    }
                )
                selected.append(accepted)
            next_memory[symbol] = memory_update
            continue

        previous_id = candidate_identity(previous)
        previous_match = next(
            (
                candidate
                for candidate in options
                if candidate_identity(candidate) == previous_id
            ),
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

        if current_anchor is not None and _meets_promotable_thresholds(
            current_anchor,
            thresholds,
            score_floor=_candidate_promotion_floor(current_anchor, thresholds)
            - float(thresholds["promotable_hold_tolerance"]),
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
                score_gap = _selection_score(winner) - _selection_score(accepted)
                if same_side:
                    if score_gap >= float(thresholds["promotable_replacement_margin"]):
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
            accepted_score = _selection_score(accepted)
            if accepted_score >= _candidate_promotion_floor(accepted, thresholds):
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
            if str(candidate.get("scoring_state") or "") == "blocked":
                continue
            if _selection_score(candidate) < _candidate_monitor_floor(
                candidate, thresholds
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
            score_change = _selection_score(current) - _selection_score(previous)
            if abs(score_change) < score_delta_threshold:
                continue
            direction = "up" if score_change > 0 else "down"
            event_type = f"promotable_score_{direction}"
            message = (
                f"{symbol} promotable score {direction}: "
                f"{_selection_score(previous):.1f} -> {_selection_score(current):.1f} "
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
    scored_symbol_candidates = {
        symbol: [
            _scored_candidate(
                dict(candidate),
                thresholds=thresholds,
                profile=profile,
                generated_at=generated_at,
            )
            for candidate in rows
        ]
        for symbol, rows in symbol_candidates.items()
    }
    scored_recovered_candidates = [
        _scored_candidate(
            dict(candidate),
            thresholds=thresholds,
            profile=profile,
            generated_at=generated_at,
        )
        for candidate in list(recovered_candidates or [])
    ]
    promotable_candidates, selection_memory = _select_promotable_candidates(
        symbol_candidates=scored_symbol_candidates,
        previous_promotable=previous_promotable,
        previous_memory=previous_selection_memory,
        top=top_promotable,
        thresholds=thresholds,
    )
    monitor_candidates = _select_monitor_candidates(
        symbol_candidates=scored_symbol_candidates,
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

    for candidate in scored_recovered_candidates:
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
        str(candidate["underlying_symbol"]): candidate
        for candidate in promotable_candidates
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
        "symbol_candidates": scored_symbol_candidates,
        "selection_memory": selection_memory,
        "promotable_candidates": promotable_candidates,
        "monitor_candidates": monitor_candidates,
        "events": events,
    }
