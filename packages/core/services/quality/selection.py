from __future__ import annotations


from core.value_coercion import coerce_float, unique_text_list

from .models import (
    EntryFilterRef,
    EntryQualityContext,
    FeatureSnapshot,
    FilterResult,
    FilterResultStatus,
)

from core.services.quality.shared import (
    _candidate,
    _first_reason,
    _result,
)

def _selection_score_ok(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    candidate = _candidate(snapshot)
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            message="No candidate was attached for selection evaluation.",
        )
    scoring_state = str(candidate.get("scoring_state") or "").strip().lower()
    selection_state = str(candidate.get("selection_state") or "").strip().lower()
    metrics = {
        "selection_state": selection_state or None,
        "scoring_state": scoring_state or None,
        "promotion_score": coerce_float(candidate.get("promotion_score")),
        "execution_score": coerce_float(candidate.get("execution_score")),
        "confidence": coerce_float(candidate.get("confidence")),
    }
    if scoring_state == "blocked":
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=_first_reason(candidate.get("scoring_blockers"), default="scoring_blocked"),
            metrics=metrics,
            message="Candidate scoring blocked selection.",
        )
    if selection_state in {"promotable", "monitor"}:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS if selection_state == "promotable" else FilterResultStatus.WATCH,
            reason_codes=(f"selected_{selection_state}",),
            metrics=metrics,
            message="Candidate was selected for live or monitor output.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.WATCH,
        reason_codes=("selection_not_evaluated",),
        metrics=metrics,
        message="Selection scoring has not been evaluated on this snapshot yet.",
    )


def _selection_live_ready(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    del context
    candidate = _candidate(snapshot)
    if not candidate:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("candidate_missing",),
            message="No candidate was attached for live-readiness evaluation.",
        )
    eligibility = str(candidate.get("eligibility") or candidate.get("eligibility_state") or "live").strip().lower()
    selection_state = str(candidate.get("selection_state") or "").strip().lower()
    blockers = []
    for field in ("blockers", "execution_blockers", "scoring_blockers"):
        blockers.extend(unique_text_list(candidate.get(field)))
    blockers = list(dict.fromkeys(blockers))
    metrics = {
        "eligibility": eligibility,
        "selection_state": selection_state or None,
        "blocker_count": len(blockers),
    }
    if eligibility != "live":
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=(eligibility or "analysis_only",),
            metrics=metrics,
            message="Candidate was not eligible for live entry.",
        )
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            message="Candidate had selection or execution blockers.",
        )
    if selection_state == "monitor":
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.WATCH,
            reason_codes=("selected_monitor",),
            metrics=metrics,
            message="Candidate was retained for monitoring, not live entry.",
        )
    if selection_state == "promotable":
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.PASS,
            reason_codes=("selection_live_ready",),
            metrics=metrics,
            message="Candidate was live-ready after selection.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.WATCH,
        reason_codes=("selection_not_evaluated",),
        metrics=metrics,
        message="Live-readiness has not been evaluated on this snapshot yet.",
    )
