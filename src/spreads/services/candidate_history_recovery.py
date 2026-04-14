from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Mapping

from spreads.services.option_structures import candidate_legs, legs_identity_key


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _parse_datetime(value: Any) -> datetime | None:
    text = _as_text(value)
    if text is None:
        return None
    normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _fill_ratio(candidate: Mapping[str, Any]) -> float | None:
    midpoint = _as_float(candidate.get("midpoint_credit"))
    natural = _as_float(candidate.get("natural_credit"))
    if midpoint in (None, 0.0) or natural is None:
        return None
    return round(natural / midpoint, 6)


def _candidate_identity(candidate: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        legs_identity_key(
            strategy=candidate.get("strategy"),
            legs=candidate_legs(candidate),
        ),
        str(candidate.get("underlying_symbol") or ""),
        str(candidate.get("expiration_date") or ""),
    )


def _serialize_recovered_candidate(
    *,
    session_date: str,
    run_payload: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    setup = (
        dict(run_payload.get("setup") or {})
        if isinstance(run_payload.get("setup"), Mapping)
        else {}
    )
    intraday_minutes = int(_as_float(setup.get("source_window_minutes")) or 0)
    expiration_date = str(candidate["expiration_date"])
    days_to_expiration = max(
        (date.fromisoformat(expiration_date) - date.fromisoformat(session_date)).days,
        0,
    )
    return {
        "run_id": str(run_payload["run_id"]),
        "underlying_symbol": str(run_payload["symbol"]),
        "strategy": str(candidate["strategy"]),
        "expiration_date": expiration_date,
        "short_symbol": str(candidate["short_symbol"]),
        "long_symbol": str(candidate["long_symbol"]),
        "short_strike": _as_float(candidate.get("short_strike")),
        "long_strike": _as_float(candidate.get("long_strike")),
        "width": _as_float(candidate.get("width")),
        "midpoint_credit": float(_as_float(candidate.get("midpoint_credit")) or 0.0),
        "natural_credit": _as_float(candidate.get("natural_credit")),
        "breakeven": _as_float(candidate.get("breakeven")),
        "max_profit": _as_float(candidate.get("max_profit")),
        "max_loss": _as_float(candidate.get("max_loss")),
        "quality_score": float(_as_float(candidate.get("quality_score")) or 0.0),
        "return_on_risk": _as_float(candidate.get("return_on_risk")),
        "short_otm_pct": _as_float(candidate.get("short_otm_pct")),
        "calendar_status": _as_text(candidate.get("calendar_status")) or "unknown",
        "setup_status": _as_text(candidate.get("setup_status"))
        or _as_text(run_payload.get("setup_status"))
        or _as_text(setup.get("status"))
        or "unknown",
        "setup_score": _as_float(run_payload.get("setup_score")),
        "profile": _as_text(run_payload.get("profile")) or "unknown",
        "spot_price": _as_float(run_payload.get("spot_price")),
        "expected_move": _as_float(candidate.get("expected_move")),
        "short_vs_expected_move": _as_float(candidate.get("short_vs_expected_move")),
        "fill_ratio": _fill_ratio(candidate),
        "data_status": "clean",
        "days_to_expiration": days_to_expiration,
        "setup_has_intraday_context": bool(
            setup.get("intraday_score") is not None or intraday_minutes > 0
        ),
        "setup_intraday_score": _as_float(setup.get("intraday_score")),
        "setup_intraday_minutes": intraday_minutes,
        "setup_spot_vs_vwap_pct": _as_float(setup.get("spot_vs_vwap_pct")),
        "setup_intraday_return_pct": _as_float(setup.get("intraday_return_pct")),
        "setup_distance_to_session_extreme_pct": _as_float(
            setup.get("distance_to_session_extreme_pct")
        ),
        "setup_opening_range_break_pct": _as_float(
            setup.get("opening_range_break_pct")
        ),
        "setup_latest_close": _as_float(setup.get("latest_close")),
        "setup_vwap": _as_float(setup.get("vwap")),
        "setup_opening_range_high": _as_float(setup.get("opening_range_high")),
        "setup_opening_range_low": _as_float(setup.get("opening_range_low")),
        "selection_source": "session_history_recovery",
        "recovered_from_run_generated_at": run_payload.get("generated_at"),
    }


def recover_session_candidates_from_history(
    *,
    history_store: Any,
    session_date: str,
    session_label: str,
    generated_at: str,
    top: int = 12,
    max_per_strategy: int = 3,
) -> list[dict[str, Any]]:
    cutoff = _parse_datetime(generated_at)
    if cutoff is None:
        return []

    top_runs = history_store.list_session_top_runs(
        session_date=session_date,
        session_label=session_label,
    )
    latest_by_symbol_strategy: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in top_runs:
        row_dt = _parse_datetime(row.get("generated_at"))
        if row_dt is None or row_dt > cutoff:
            continue
        if int(_as_float(row.get("candidate_count")) or 0) <= 0:
            continue
        symbol = _as_text(row.get("symbol"))
        strategy = _as_text(row.get("strategy"))
        if symbol is None or strategy is None:
            continue
        latest_by_symbol_strategy[(symbol, strategy)] = dict(row)

    recovered: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for key in sorted(latest_by_symbol_strategy):
        run_stub = latest_by_symbol_strategy[key]
        run_id = str(run_stub["run_id"])
        run_payload = history_store.get_run(run_id)
        if not isinstance(run_payload, Mapping):
            continue
        candidates = history_store.list_candidates(run_id)
        for candidate in candidates[: max(max_per_strategy, 1)]:
            identity = _candidate_identity(candidate)
            if identity in seen:
                continue
            seen.add(identity)
            recovered.append(
                _serialize_recovered_candidate(
                    session_date=session_date,
                    run_payload=run_payload,
                    candidate=candidate,
                )
            )

    recovered.sort(
        key=lambda item: (
            float(item.get("quality_score") or 0.0),
            float(item.get("return_on_risk") or 0.0),
            float(item.get("midpoint_credit") or 0.0),
        ),
        reverse=True,
    )
    return recovered[:top]


__all__ = ["recover_session_candidates_from_history"]
