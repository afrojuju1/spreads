from __future__ import annotations


from core.value_coercion import as_mapping, unique_text_list

from .models import (
    EntryFilterRef,
    EntryQualityContext,
    FeatureSnapshot,
    FilterResult,
    FilterResultStatus,
)

from core.services.quality.shared import (
    _first_reason,
    _resolved_thresholds,
    _result,
)

def _source_is_fresh(context: EntryQualityContext, snapshot: FeatureSnapshot, filter_ref: EntryFilterRef) -> FilterResult:
    source = as_mapping(snapshot.source)
    thresholds = _resolved_thresholds(
        context,
        filter_ref,
        {"max_age_seconds": source.get("max_age_seconds")},
    )
    blockers = unique_text_list(source.get("blockers"))
    metrics = {
        "ticker_source_kind": source.get("ticker_source_kind"),
        "ticker_source_id": source.get("ticker_source_id"),
        "ticker_source_run_id": source.get("ticker_source_run_id"),
        "resolved_at": source.get("resolved_at"),
        "max_age_seconds": source.get("max_age_seconds"),
        "status": source.get("status"),
    }
    if blockers:
        return _result(
            filter_ref=filter_ref,
            status=FilterResultStatus.BLOCK,
            reason_codes=blockers,
            metrics=metrics,
            thresholds=thresholds,
            message="Ticker source was not usable for entry.",
        )
    return _result(
        filter_ref=filter_ref,
        status=FilterResultStatus.PASS,
        reason_codes=_first_reason(source.get("reason_codes"), default="ticker_source_usable"),
        metrics=metrics,
        thresholds=thresholds,
        message="Ticker source was usable.",
    )
