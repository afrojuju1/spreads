from __future__ import annotations

from typing import Any, Mapping


DEFAULT_DEPLOYMENT_QUALITY_THRESHOLDS = {
    "min_promotable_return_on_risk": None,
    "min_execution_return_on_risk": None,
}

PROFILE_DEPLOYMENT_QUALITY_THRESHOLDS = {
    "weekly": {
        "min_promotable_return_on_risk": 0.13,
        "min_execution_return_on_risk": 0.13,
    },
    "core": {
        "min_promotable_return_on_risk": 0.16,
        "min_execution_return_on_risk": 0.16,
    },
}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _candidate_nested_payload(candidate: Mapping[str, Any]) -> Mapping[str, Any] | None:
    nested = candidate.get("candidate")
    return nested if isinstance(nested, Mapping) else None


def resolve_candidate_profile(candidate: Mapping[str, Any]) -> str | None:
    for source in (candidate, _candidate_nested_payload(candidate)):
        if source is None:
            continue
        profile = _as_text(source.get("profile"))
        if profile is not None:
            return profile.lower()
    return None


def resolve_candidate_return_on_risk(candidate: Mapping[str, Any]) -> float | None:
    for source in (candidate, _candidate_nested_payload(candidate)):
        if source is None:
            continue
        resolved = _coerce_float(source.get("return_on_risk"))
        if resolved is not None:
            return resolved
    return None


def resolve_deployment_quality_thresholds(
    profile: str | None,
) -> dict[str, float | None]:
    thresholds = dict(DEFAULT_DEPLOYMENT_QUALITY_THRESHOLDS)
    normalized = _as_text(profile)
    if normalized is not None:
        thresholds.update(PROFILE_DEPLOYMENT_QUALITY_THRESHOLDS.get(normalized.lower(), {}))
    return thresholds


def candidate_meets_return_on_risk_floor(
    candidate: Mapping[str, Any],
    minimum_return_on_risk: float | None,
) -> bool:
    if minimum_return_on_risk is None:
        return True
    resolved = resolve_candidate_return_on_risk(candidate)
    if resolved is None:
        return False
    return resolved >= float(minimum_return_on_risk)


def candidate_requires_favorable_setup(candidate: Mapping[str, Any]) -> bool:
    return resolve_candidate_profile(candidate) == "0dte"


def candidate_has_intraday_setup_context(candidate: Mapping[str, Any]) -> bool:
    if bool(candidate.get("setup_has_intraday_context")):
        return True
    score = candidate.get("setup_intraday_score")
    if score not in (None, ""):
        return True
    minutes = candidate.get("setup_intraday_minutes")
    try:
        return int(float(minutes)) > 0
    except (TypeError, ValueError):
        return False


__all__ = [
    "candidate_meets_return_on_risk_floor",
    "candidate_has_intraday_setup_context",
    "candidate_requires_favorable_setup",
    "resolve_candidate_profile",
    "resolve_candidate_return_on_risk",
    "resolve_deployment_quality_thresholds",
]
