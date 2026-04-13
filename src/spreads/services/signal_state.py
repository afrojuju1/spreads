from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from spreads.events.bus import publish_global_event_sync
from spreads.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    candidate_requires_favorable_setup,
)
from spreads.storage.signal_repository import SignalRepository

PROMOTABLE_SCORE_FLOOR = 65.0
MONITOR_SCORE_FLOOR = 55.0
PROMOTABLE_STRONG_SCORE = 82.0


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
    normalized = (score - MONITOR_SCORE_FLOOR) / max(
        PROMOTABLE_STRONG_SCORE - MONITOR_SCORE_FLOOR, 1.0
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
    selection_state: str | None,
    candidate: dict[str, Any] | None,
    origin: str | None = None,
    eligibility: str | None = None,
    failure_error: str | None = None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "cycle_id": cycle_id,
        "selection_state": selection_state,
        "origin": origin,
        "eligibility": eligibility,
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


def _build_opportunity_payload(
    *,
    label: str,
    session_date: str,
    generated_at: str,
    cycle_id: str,
    default_strategy: str,
    default_profile: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    candidate = _candidate_with_payload(row)
    profile = str(candidate.get("profile") or default_profile)
    strategy_family = str(candidate.get("strategy") or default_strategy)
    return {
        "opportunity_id": build_opportunity_id(label, session_date, candidate),
        "label": label,
        "session_date": session_date,
        "strategy_family": strategy_family,
        "profile": profile,
        "entity_type": "signal_subject",
        "entity_key": _signal_subject_key(label, str(candidate["underlying_symbol"])),
        "underlying_symbol": str(candidate["underlying_symbol"]),
        "side": _opportunity_side(candidate),
        "selection_state": str(row["selection_state"]),
        "selection_rank": (
            None
            if row.get("selection_rank") in (None, "")
            else int(row["selection_rank"])
        ),
        "state_reason": str(row.get("state_reason") or "selected"),
        "origin": str(row.get("origin") or "live_scan"),
        "eligibility": str(row.get("eligibility") or "live"),
        "confidence": _candidate_confidence(candidate),
        "signal_state_ref": build_signal_state_id(label, str(candidate["underlying_symbol"])),
        "lifecycle_state": (
            "ready"
            if str(row["selection_state"]) == "promotable"
            else "candidate"
        ),
        "created_at": generated_at,
        "updated_at": generated_at,
        "expires_at": _expires_at(generated_at, profile),
        "reason_codes": [str(row.get("state_reason") or "selected")],
        "blockers": [] if str(row.get("eligibility") or "live") == "live" else ["analysis_only"],
        "execution_shape": _execution_shape(candidate),
        "risk_hints": _risk_hints(candidate),
        "source_cycle_id": cycle_id,
        "source_candidate_id": (
            None
            if row.get("candidate_id") in (None, "")
            else int(row["candidate_id"])
        ),
        "source_selection_state": str(row["selection_state"]),
        "candidate_identity": _candidate_identity(candidate),
        "candidate": dict(candidate),
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
    primary_live_opportunity: dict[str, Any] | None,
    analysis_only_opportunity: dict[str, Any] | None,
    failure_error: str | None,
) -> dict[str, Any]:
    candidate = (
        None
        if primary_live_opportunity is None
        else _candidate_with_payload(primary_live_opportunity)
    )
    selection_state = (
        None
        if primary_live_opportunity is None
        else str(primary_live_opportunity.get("selection_state") or "")
    )
    origin = (
        None if primary_live_opportunity is None else primary_live_opportunity.get("origin")
    )
    eligibility = (
        None
        if primary_live_opportunity is None
        else primary_live_opportunity.get("eligibility")
    )

    state = "IDLE"
    reason_codes: list[str] = []
    blockers: list[str] = []
    active_candidate_id: int | None = None
    active_selection_state: str | None = None
    opportunity_id: str | None = None
    confidence: float | None = None
    expires_at: str | None = None

    if primary_live_opportunity is not None and candidate is not None:
        confidence = _candidate_confidence(candidate)
        active_candidate_id = (
            None
            if primary_live_opportunity.get("candidate_id") in (None, "")
            else int(primary_live_opportunity["candidate_id"])
        )
        active_selection_state = selection_state
        opportunity_id = str(
            primary_live_opportunity.get("opportunity_id")
            or build_opportunity_id(label, session_date, candidate)
        )
        expires_at = _expires_at(
            generated_at, str(candidate.get("profile") or default_profile)
        )
        if selection_state == "promotable":
            state = "ACTIVE"
            reason_codes = ["promotable_selected"]
        else:
            state = "ARMING"
            reason_codes = ["monitor_selected"]
    elif failure_error:
        state = "BLOCKED"
        reason_codes = ["scan_failure"]
        blockers = ["scan_failure"]
    else:
        raw_best = raw_candidates[0] if raw_candidates else None
        if raw_best is not None:
            raw_quality = float(raw_best.get("quality_score") or 0.0)
            raw_blockers = _candidate_blockers(raw_best)
            if raw_blockers:
                state = "BLOCKED"
                reason_codes = list(raw_blockers)
                blockers = list(raw_blockers)
                candidate = raw_best
            elif raw_quality >= MONITOR_SCORE_FLOOR:
                state = "ARMING"
                reason_codes = ["live_candidate_retained"]
                candidate = raw_best
                confidence = _candidate_confidence(candidate)
                expires_at = _expires_at(
                    generated_at, str(candidate.get("profile") or default_profile)
                )
            else:
                reason_codes = ["score_below_monitor_floor"]
        elif analysis_only_opportunity is not None:
            reason_codes = ["analysis_only_recovery"]
            candidate = _candidate_with_payload(analysis_only_opportunity)
        else:
            reason_codes = ["no_live_opportunity"]

    if candidate is not None and confidence is None:
        confidence = _candidate_confidence(candidate)

    evidence = _candidate_evidence(
        cycle_id=cycle_id,
        selection_state=active_selection_state,
        candidate=candidate,
        origin=None if primary_live_opportunity is None else str(origin),
        eligibility=None if primary_live_opportunity is None else str(eligibility),
        failure_error=failure_error,
    )

    return {
        "signal_state": {
            "signal_state_id": build_signal_state_id(label, symbol),
            "label": label,
            "strategy_family": str(
                (candidate or {}).get("strategy") or default_strategy
            ),
            "profile": str((candidate or {}).get("profile") or default_profile),
            "entity_type": "signal_subject",
            "entity_key": _signal_subject_key(label, symbol),
            "underlying_symbol": symbol,
            "state": state,
            "confidence": confidence,
            "reason_codes": list(reason_codes),
            "blockers": list(blockers),
            "evidence": evidence,
            "active_cycle_id": cycle_id if active_candidate_id is not None else None,
            "active_candidate_id": active_candidate_id,
            "active_selection_state": active_selection_state,
            "opportunity_id": opportunity_id,
            "session_date": session_date,
            "market_session": "regular",
            "observed_at": generated_at,
            "expires_at": expires_at,
        }
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
    selection_memory: dict[str, dict[str, Any]],
    failures: list[dict[str, Any]],
    persisted_opportunities: list[dict[str, Any]],
) -> dict[str, Any]:
    del selection_memory
    if not signal_store.schema_ready():
        return {
            "signal_states_upserted": 0,
            "signal_transitions_recorded": 0,
            "opportunities_upserted": 0,
            "opportunities_expired": 0,
        }

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

    live_by_symbol: dict[str, list[dict[str, Any]]] = {}
    analysis_only_by_symbol: dict[str, list[dict[str, Any]]] = {}
    active_opportunity_ids: list[str] = []
    opportunity_rows: list[dict[str, Any]] = []

    for row in persisted_opportunities:
        payload = dict(row)
        candidate = _candidate_with_payload(payload)
        symbol = str(candidate["underlying_symbol"])
        opportunity_payload = _build_opportunity_payload(
            label=label,
            session_date=session_date,
            generated_at=generated_at,
            cycle_id=cycle_id,
            default_strategy=strategy,
            default_profile=profile,
            row=payload,
        )
        opportunity_rows.append(opportunity_payload)
        active_opportunity_ids.append(str(opportunity_payload["opportunity_id"]))
        if str(payload.get("eligibility") or "live") == "live":
            live_by_symbol.setdefault(symbol, []).append(
                {**payload, "opportunity_id": opportunity_payload["opportunity_id"]}
            )
        else:
            analysis_only_by_symbol.setdefault(symbol, []).append(
                {**payload, "opportunity_id": opportunity_payload["opportunity_id"]}
            )

    for rows in live_by_symbol.values():
        rows.sort(
            key=lambda item: (
                0 if str(item.get("selection_state")) == "promotable" else 1,
                int(item.get("selection_rank") or 0),
            )
        )

    signal_states_upserted = 0
    signal_transitions_recorded = 0
    opportunities_upserted = 0

    for opportunity_payload in opportunity_rows:
        opportunity, opportunity_changed = signal_store.upsert_opportunity(
            **opportunity_payload
        )
        opportunities_upserted += 1
        if opportunity_changed:
            publish_opportunity_event(
                topic="opportunity.lifecycle.updated",
                opportunity=opportunity,
                session_date=session_date,
                correlation_id=cycle_id,
                causation_id=(
                    None
                    if opportunity.get("source_candidate_id") is None
                    else str(opportunity["source_candidate_id"])
                ),
                timestamp=generated_at,
            )

    seen_symbols = sorted(
        set(str(symbol).upper() for symbol in symbols)
        | set(raw_by_symbol)
        | set(failures_by_symbol)
        | set(live_by_symbol)
        | set(analysis_only_by_symbol)
    )

    for symbol in seen_symbols:
        live_rows = list(live_by_symbol.get(symbol) or [])
        primary_live = live_rows[0] if live_rows else None
        analysis_only = (
            None
            if not analysis_only_by_symbol.get(symbol)
            else analysis_only_by_symbol[symbol][0]
        )
        slice_payload = _derive_symbol_slice(
            label=label,
            session_date=session_date,
            generated_at=generated_at,
            cycle_id=cycle_id,
            default_strategy=strategy,
            default_profile=profile,
            symbol=symbol,
            raw_candidates=raw_by_symbol.get(symbol, []),
            primary_live_opportunity=primary_live,
            analysis_only_opportunity=analysis_only,
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
            causation_id=(
                None
                if opportunity.get("source_candidate_id") is None
                else str(opportunity["source_candidate_id"])
            ),
            timestamp=generated_at,
        )

    return {
        "signal_states_upserted": signal_states_upserted,
        "signal_transitions_recorded": signal_transitions_recorded,
        "opportunities_upserted": opportunities_upserted,
        "opportunities_expired": len(expired_opportunities),
    }
