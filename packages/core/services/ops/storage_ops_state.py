from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.services.retention import build_retention_status

from .shared import _attention


def build_storage_ops_state(
    *,
    db_target: str | None = None,
    storage: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_now = now or datetime.now(UTC)
    payload = build_retention_status(db_target=db_target, now=resolved_now)
    summary = dict(payload.get("summary") or {})
    attention: list[dict[str, str]] = []

    missing_current = [str(value) for value in list(summary.get("missing_current_partitions") or [])]
    if missing_current:
        attention.append(
            _attention(
                severity="high",
                code="tick_partition_current_missing",
                message=f"Current-day tick partitions are missing for: {', '.join(missing_current)}.",
            )
        )

    future_short = [str(value) for value in list(summary.get("future_partition_short_tables") or [])]
    if future_short:
        attention.append(
            _attention(
                severity="medium",
                code="tick_partition_future_short",
                message=f"Future tick partition coverage is short for: {', '.join(future_short)}.",
            )
        )

    if summary.get("latest_run_status") == "failed":
        attention.append(
            _attention(
                severity="medium",
                code="retention_latest_run_failed",
                message="The latest tick partition maintenance run failed.",
            )
        )

    return {
        **payload,
        "generated_at": payload.get("generated_at") or resolved_now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "attention": attention,
    }


__all__ = ["build_storage_ops_state"]
