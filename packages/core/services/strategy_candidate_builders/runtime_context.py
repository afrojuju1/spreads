from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Any

from core.domain.profiles import zero_dte_session_bucket
from core.services.market_dates import NEW_YORK


def coerce_evaluation_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def candidate_reference_datetime(context: Any) -> datetime | None:
    return coerce_evaluation_datetime(getattr(context, "evaluation_timestamp", None))


def candidate_reference_date(context: Any) -> date:
    reference_datetime = candidate_reference_datetime(context)
    if reference_datetime is not None:
        return reference_datetime.astimezone(NEW_YORK).date()
    raw_date = getattr(context, "evaluation_date", None)
    if raw_date not in (None, ""):
        if isinstance(raw_date, date):
            return raw_date
        return date.fromisoformat(str(raw_date))
    return datetime.now(UTC).date()


def candidate_session_bucket(context: Any) -> str | None:
    override = getattr(context, "session_bucket_override", None)
    if override not in (None, ""):
        return str(override)
    reference_datetime = candidate_reference_datetime(context)
    if reference_datetime is not None:
        return zero_dte_session_bucket(reference_datetime)
    return zero_dte_session_bucket()


def apply_candidate_evaluation_context(
    context: Any,
    *,
    evaluation_timestamp: datetime | str | None = None,
    evaluation_date: date | str | None = None,
    session_bucket: str | None = None,
) -> Any:
    if evaluation_timestamp is not None:
        resolved_timestamp = coerce_evaluation_datetime(evaluation_timestamp)
        if resolved_timestamp is not None:
            context.evaluation_timestamp = resolved_timestamp.isoformat()
    if evaluation_date is not None:
        context.evaluation_date = evaluation_date.isoformat() if isinstance(evaluation_date, date) else str(evaluation_date)
    if session_bucket is not None:
        context.session_bucket_override = str(session_bucket)
    return context


def option_expiry_close(expiration_date: str) -> datetime:
    local_close = datetime.combine(
        datetime.fromisoformat(expiration_date).date(),
        time(16, 0),
        tzinfo=NEW_YORK,
    )
    return local_close.astimezone(UTC)


__all__ = [
    "apply_candidate_evaluation_context",
    "candidate_reference_date",
    "candidate_reference_datetime",
    "candidate_session_bucket",
    "coerce_evaluation_datetime",
    "option_expiry_close",
]
