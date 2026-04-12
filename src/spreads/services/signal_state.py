from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from spreads.events.bus import publish_global_event_sync
from spreads.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    candidate_requires_favorable_setup,
)
from spreads.storage.signal_repository import SignalRepository

BOARD_STRONG_SCORE = 82.0
WATCHLIST_SCORE_FLOOR = 55.0


def _candidate_identity(candidate: dict[str, Any]) -> str:
    return f"{candidate['strategy']}|{candidate['short_symbol']}|{candidate['long_symbol']}"


def _candidate_with_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    base = dict(candidate)
    payload = base.pop("candidate", None)
    if isinstance(payload, dict):
        return {
            **base,
            **dict(payload),
        }
    return base


def _candidate_blockers(candidate: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if candidate_requires_favorable_setup(candidate):
        if str(candidate.get("setup_status") or "").lower() != "favorable":
            blockers.append("setup_not_favorable")
        if not candidate_has_intraday_setup_context(candidate):
            blockers.append("missing_intraday_context")
    data_status = str(candidate.get("data_status") or "clean").lower()
    if data_status != "clean":
        blockers.append(f"data_{data_status}")
    calendar_status = str(candidate.get("calendar_status") or "clean").lower()
    if calendar_status != "clean":
        blockers.append(f"calendar_{calendar_status}")
    return blockers


def _signal_subject_key(label: str, symbol: str) -> str:
    return f"signal_subject:{label}:{symbol}"


def build_signal_state_id(label: str, symbol: str) -> str:
    return f"signal_state:{label}:{symbol}"


def build_opportunity_id(
    label: str, session_date: str, candidate: dict[str, Any]
) -> str:
    return (
        f"opportunity:{label}:{session_date}:{candidate['underlying_symbol']}:"
        f"{candidate['strategy']}:{candidate['short_symbol']}:{candidate['long_symbol']}"
    )


def _opportunity_side(candidate: dict[str, Any]) -> str | None:
    strategy = str(candidate.get("strategy") or "").lower()
    if strategy == "call_credit":
        return "bearish"
    if strategy == "put_credit":
        return "bullish"
    return None


def _candidate_confidence(candidate: dict[str, Any] | None) -> float | None:
    if candidate is None:
        return None
    try:
        score = float(candidate.get("quality_score"))
    except (TypeError, ValueError):
        return None
    normalized = (score - WATCHLIST_SCORE_FLOOR) / max(
        BOARD_STRONG_SCORE - WATCHLIST_SCORE_FLOOR, 1.0
    )
    return round(max(0.0, min(normalized, 1.0)), 4)


def _expires_at(generated_at: str, profile: str) -> str:
    ttl_minutes = {
        "0dte": 5,
        "micro": 15,
        "weekly": 30,
        "core": 30,
        "swing": 60,
    }.get(str(profile or "").lower(), 30)
    observed_at = datetime.fromisoformat(
        generated_at.replace("Z", "+00:00")
    ).astimezone(UTC)
    return (
        (observed_at + timedelta(minutes=ttl_minutes))
        .isoformat()
        .replace("+00:00", "Z")
    )


def publish_opportunity_event(
    *,
    topic: str,
    opportunity: dict[str, Any],
    session_date: str,
    correlation_id: str,
    causation_id: str | None = None,
    timestamp: str | None = None,
    source: str = "signal_state",
) -> None:
    try:
        publish_global_event_sync(
            topic=topic,
            event_class="opportunity_event",
            entity_type="opportunity",
            entity_id=str(opportunity["opportunity_id"]),
            payload=opportunity,
            timestamp=timestamp or opportunity.get("updated_at"),
            source=source,
            session_date=session_date,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
    except Exception as exc:
        print(f"{topic} publish unavailable: {exc}")


def _publish_signal_state_event(
    *,
    signal_state: dict[str, Any],
    session_date: str,
    correlation_id: str,
    causation_id: str | None = None,
) -> None:
    try:
        publish_global_event_sync(
            topic="signal.state.updated",
            event_class="signal_event",
            entity_type="signal_state",
            entity_id=str(signal_state["signal_state_id"]),
            payload=signal_state,
            timestamp=signal_state.get("updated_at"),
            source="signal_state",
            session_date=session_date,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
    except Exception as exc:
        print(f"signal.state.updated publish unavailable: {exc}")


def _publish_signal_transition_event(
    *,
    transition: dict[str, Any],
    session_date: str,
    correlation_id: str,
    causation_id: str | None = None,
) -> None:
    try:
        publish_global_event_sync(
            topic="signal.transition.recorded",
            event_class="signal_event",
            entity_type="signal_transition",
            entity_id=str(transition["transition_id"]),
            payload=transition,
            timestamp=transition.get("occurred_at"),
            source="signal_state",
            session_date=session_date,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
    except Exception as exc:
        print(f"signal.transition.recorded publish unavailable: {exc}")


def _candidate_evidence(
    *,
    cycle_id: str,
    classification: str,
    candidate: dict[str, Any] | None,
    pending_state: dict[str, Any] | None = None,
    failure_error: str | None = None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "cycle_id": cycle_id,
        "classification": classification,
    }
    if candidate is not None:
        evidence.update(
            {
                "candidate_identity": _candidate_identity(candidate),
                "strategy": candidate.get("strategy"),
                "quality_score": candidate.get("quality_score"),
                "return_on_risk": candidate.get("return_on_risk"),
                "midpoint_credit": candidate.get("midpoint_credit"),
                "setup_status": candidate.get("setup_status"),
                "data_status": candidate.get("data_status"),
                "calendar_status": candidate.get("calendar_status"),
            }
        )
    if pending_state:
        evidence["pending_state"] = dict(pending_state)
    if failure_error:
        evidence["failure_error"] = failure_error
    return evidence


def _risk_hints(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "midpoint_credit": candidate.get("midpoint_credit"),
        "natural_credit": candidate.get("natural_credit"),
        "max_loss": candidate.get("max_loss"),
        "return_on_risk": candidate.get("return_on_risk"),
        "fill_ratio": candidate.get("fill_ratio"),
        "width": candidate.get("width"),
    }


def _execution_shape(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "underlying_symbol": candidate.get("underlying_symbol"),
        "short_symbol": candidate.get("short_symbol"),
        "long_symbol": candidate.get("long_symbol"),
        "order_payload": dict(candidate.get("order_payload") or {}),
    }


def _derive_symbol_slice(
    *,
    label: str,
    session_date: str,
    generated_at: str,
    cycle_id: str,
    default_strategy: str,
    default_profile: str,
    symbol: str,
    raw_candidates: list[dict[str, Any]],
    board_candidate: dict[str, Any] | None,
    watchlist_candidate: dict[str, Any] | None,
    pending_state: dict[str, Any],
    failure_error: str | None,
) -> dict[str, Any]:
    candidate = board_candidate or watchlist_candidate
    classification: str = "none"
    state = "IDLE"
    lifecycle_state: str | None = None
    reason_codes: list[str] = []
    blockers: list[str] = []

    raw_best = raw_candidates[0] if raw_candidates else None
    pending_identity = str(pending_state.get("pending_identity") or "") or None

    if board_candidate is not None:
        candidate = board_candidate
        classification = "board"
        state = "ACTIVE"
        lifecycle_state = "ready"
        reason_codes = ["board_candidate_selected"]
    elif watchlist_candidate is not None:
        candidate = watchlist_candidate
        classification = "watchlist"
        state = "ARMING"
        lifecycle_state = "candidate"
        reason_codes = ["watchlist_candidate_selected"]
        if pending_identity and pending_identity == _candidate_identity(candidate):
            classification = "pending"
            reason_codes.append("pending_board_confirmation")
    elif failure_error:
        classification = "scan_failure"
        state = "BLOCKED"
        reason_codes = ["scan_failure"]
        blockers = ["scan_failure"]
        candidate = None
    elif raw_best is not None:
        raw_quality = float(raw_best.get("quality_score") or 0.0)
        raw_blockers = _candidate_blockers(raw_best)
        raw_identity = _candidate_identity(raw_best)
        if pending_identity and pending_identity == raw_identity:
            candidate = raw_best
            classification = "pending"
            state = "ARMING"
            lifecycle_state = "candidate"
            reason_codes = ["pending_board_confirmation"]
        elif raw_blockers:
            candidate = raw_best
            classification = "blocked"
            state = "BLOCKED"
            lifecycle_state = "blocked"
            reason_codes = list(raw_blockers)
            blockers = list(raw_blockers)
        elif raw_quality >= WATCHLIST_SCORE_FLOOR:
            candidate = raw_best
            classification = "candidate"
            state = "ARMING"
            lifecycle_state = "candidate"
            reason_codes = ["candidate_ready_for_watchlist"]
        else:
            classification = "idle"
            state = "IDLE"
            reason_codes = ["score_below_watchlist_floor"]
            candidate = None
    else:
        classification = "idle"
        state = "IDLE"
        reason_codes = ["no_ranked_candidate"]
        candidate = None

    profile = str((candidate or {}).get("profile") or default_profile)
    strategy_family = str((candidate or {}).get("strategy") or default_strategy)
    confidence = _candidate_confidence(candidate)
    expiry = None if candidate is None else _expires_at(generated_at, profile)
    signal_state_id = build_signal_state_id(label, symbol)
    entity_key = _signal_subject_key(label, symbol)
    evidence = _candidate_evidence(
        cycle_id=cycle_id,
        classification=classification,
        candidate=candidate,
        pending_state=pending_state,
        failure_error=failure_error,
    )

    opportunity: dict[str, Any] | None = None
    if candidate is not None and lifecycle_state is not None:
        opportunity = {
            "opportunity_id": build_opportunity_id(label, session_date, candidate),
            "label": label,
            "session_date": session_date,
            "strategy_family": strategy_family,
            "profile": profile,
            "entity_type": "signal_subject",
            "entity_key": entity_key,
            "underlying_symbol": symbol,
            "side": _opportunity_side(candidate),
            "classification": classification,
            "confidence": confidence,
            "signal_state_ref": signal_state_id,
            "lifecycle_state": lifecycle_state,
            "created_at": generated_at,
            "updated_at": generated_at,
            "expires_at": expiry,
            "reason_codes": list(reason_codes),
            "blockers": list(blockers),
            "execution_shape": _execution_shape(candidate),
            "risk_hints": _risk_hints(candidate),
            "source_cycle_id": cycle_id,
            "source_candidate_id": (
                None
                if candidate.get("candidate_id") in (None, "")
                else int(candidate["candidate_id"])
            ),
            "source_bucket": str(candidate.get("bucket") or classification),
            "candidate_identity": _candidate_identity(candidate),
            "candidate": dict(candidate),
        }

    return {
        "signal_state": {
            "signal_state_id": signal_state_id,
            "label": label,
            "strategy_family": strategy_family,
            "profile": profile,
            "entity_type": "signal_subject",
            "entity_key": entity_key,
            "underlying_symbol": symbol,
            "state": state,
            "confidence": confidence,
            "reason_codes": list(reason_codes),
            "blockers": list(blockers),
            "evidence": evidence,
            "active_cycle_id": cycle_id if candidate is not None else None,
            "active_candidate_id": (
                None
                if candidate is None or candidate.get("candidate_id") in (None, "")
                else int(candidate["candidate_id"])
            ),
            "active_bucket": None
            if candidate is None
            else str(candidate.get("bucket") or classification),
            "opportunity_id": None
            if opportunity is None
            else opportunity["opportunity_id"],
            "session_date": session_date,
            "market_session": "regular",
            "observed_at": generated_at,
            "expires_at": expiry,
        },
        "opportunity": opportunity,
    }


def sync_live_collector_signal_layer(
    *,
    signal_store: SignalRepository,
    label: str,
    session_date: str,
    generated_at: str,
    cycle_id: str,
    strategy: str,
    profile: str,
    symbols: list[str],
    symbol_candidates: dict[str, list[dict[str, Any]]],
    selection_state: dict[str, dict[str, Any]],
    failures: list[dict[str, Any]],
    persisted_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    if not signal_store.schema_ready():
        return {
            "signal_states_upserted": 0,
            "signal_transitions_recorded": 0,
            "opportunities_upserted": 0,
            "opportunities_expired": 0,
        }

    board_by_symbol: dict[str, dict[str, Any]] = {}
    watchlist_by_symbol: dict[str, dict[str, Any]] = {}
    for row in persisted_candidates:
        candidate = _candidate_with_payload(row)
        symbol = str(candidate["underlying_symbol"])
        if str(row.get("bucket")) == "board":
            board_by_symbol[symbol] = candidate
        elif (
            str(row.get("bucket")) == "watchlist" and symbol not in watchlist_by_symbol
        ):
            watchlist_by_symbol[symbol] = candidate

    raw_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for symbol, rows in symbol_candidates.items():
        raw_by_symbol[str(symbol)] = sorted(
            [_candidate_with_payload(row) for row in rows],
            key=lambda candidate: float(candidate.get("quality_score") or 0.0),
            reverse=True,
        )

    failures_by_symbol = {
        str(failure["symbol"]): str(failure["error"])
        for failure in failures
        if failure.get("symbol") and failure.get("error")
    }

    seen_symbols = sorted(
        set(str(symbol).upper() for symbol in symbols)
        | set(board_by_symbol)
        | set(watchlist_by_symbol)
        | set(raw_by_symbol)
        | set(failures_by_symbol)
    )

    signal_states_upserted = 0
    signal_transitions_recorded = 0
    opportunities_upserted = 0
    active_opportunity_ids: list[str] = []

    for symbol in seen_symbols:
        slice_payload = _derive_symbol_slice(
            label=label,
            session_date=session_date,
            generated_at=generated_at,
            cycle_id=cycle_id,
            default_strategy=strategy,
            default_profile=profile,
            symbol=symbol,
            raw_candidates=raw_by_symbol.get(symbol, []),
            board_candidate=board_by_symbol.get(symbol),
            watchlist_candidate=watchlist_by_symbol.get(symbol),
            pending_state=dict(selection_state.get(symbol) or {}),
            failure_error=failures_by_symbol.get(symbol),
        )

        signal_state, transition, changed = signal_store.upsert_signal_state(
            **slice_payload["signal_state"]
        )
        signal_states_upserted += 1
        causation_id = (
            None
            if signal_state.get("active_candidate_id") is None
            else str(signal_state["active_candidate_id"])
        )
        if changed:
            _publish_signal_state_event(
                signal_state=signal_state,
                session_date=session_date,
                correlation_id=cycle_id,
                causation_id=causation_id,
            )
        if transition is not None:
            signal_transitions_recorded += 1
            _publish_signal_transition_event(
                transition=transition,
                session_date=session_date,
                correlation_id=cycle_id,
                causation_id=causation_id,
            )

        opportunity_payload = slice_payload["opportunity"]
        if opportunity_payload is None:
            continue
        opportunity, opportunity_changed = signal_store.upsert_opportunity(
            **opportunity_payload
        )
        opportunities_upserted += 1
        active_opportunity_ids.append(str(opportunity["opportunity_id"]))
        if opportunity_changed:
            publish_opportunity_event(
                topic="opportunity.lifecycle.updated",
                opportunity=opportunity,
                session_date=session_date,
                correlation_id=cycle_id,
                causation_id=causation_id,
                timestamp=generated_at,
            )

    expired_opportunities = signal_store.expire_absent_opportunities(
        label=label,
        session_date=session_date,
        active_opportunity_ids=active_opportunity_ids,
        expired_at=generated_at,
    )
    for opportunity in expired_opportunities:
        publish_opportunity_event(
            topic="opportunity.lifecycle.updated",
            opportunity=opportunity,
            session_date=session_date,
            correlation_id=cycle_id,
            causation_id=None
            if opportunity.get("source_candidate_id") is None
            else str(opportunity["source_candidate_id"]),
            timestamp=generated_at,
        )

    return {
        "signal_states_upserted": signal_states_upserted,
        "signal_transitions_recorded": signal_transitions_recorded,
        "opportunities_upserted": opportunities_upserted,
        "opportunities_expired": len(expired_opportunities),
    }
